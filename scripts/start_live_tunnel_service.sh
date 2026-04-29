#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
BIN_DIR="${ROOT_DIR}/.bin"
CONFIG_FILE="${ROOT_DIR}/live-config.json"
LOG_DIR="${ROOT_DIR}/.runtime"
TUNNEL_LOG="${LOG_DIR}/cloudflared-service.log"
UVICORN_LOG="${LOG_DIR}/uvicorn-service-fallback.log"
AUTO_PUSH_CONFIG="${PANEL_AUTO_PUSH_CONFIG:-1}"
ALLOW_FALLBACK_BACKEND="${PANEL_SERVICE_START_BACKEND_IF_MISSING:-1}"
PAGES_ORIGIN="${PANEL_PAGES_ORIGIN:-https://darrylclay2005.github.io}"
PAGES_URL="${PANEL_PAGES_PUBLIC_URL:-https://darrylclay2005.github.io/SwarmPanel/}"
MAX_TUNNEL_START_ATTEMPTS="${PANEL_MAX_TUNNEL_START_ATTEMPTS:-12}"

mkdir -p "${BIN_DIR}" "${LOG_DIR}"

write_config() {
  local panel_url="$1"
  cat > "${CONFIG_FILE}" <<EOF
{
  "panel_url": "${panel_url}",
  "updated_at": "$(date -Is)"
}
EOF
}

write_offline_config() {
  cat > "${CONFIG_FILE}" <<EOF
{
  "panel_url": "",
  "updated_at": "$(date -Is)"
}
EOF
}

run_git() {
  if command -v flatpak-spawn >/dev/null 2>&1; then
    flatpak-spawn --host git -C "${ROOT_DIR}" "$@"
  else
    git -C "${ROOT_DIR}" "$@"
  fi
}

publish_config() {
  if [[ "${AUTO_PUSH_CONFIG}" != "1" ]]; then
    echo "PANEL_AUTO_PUSH_CONFIG is disabled; live-config.json was updated locally only."
    return
  fi
  if ! command -v git >/dev/null 2>&1 && ! command -v flatpak-spawn >/dev/null 2>&1; then
    echo "Skipping live-config push because git is unavailable." >&2
    return
  fi
  if ! run_git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Skipping live-config push because ${ROOT_DIR} is not a git work tree." >&2
    return
  fi
  if run_git diff --quiet -- live-config.json; then
    echo "live-config.json already matches the current tunnel URL."
    return
  fi
  run_git add live-config.json
  run_git commit -m "Update live backend URL" -- live-config.json || true
  run_git push origin main || echo "Could not push live-config.json automatically." >&2
}

install_python_deps() {
  if [[ -x "${VENV_DIR}/bin/python" ]] && ! "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
    rm -rf "${VENV_DIR}"
  fi
  if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    python3 -m venv "${VENV_DIR}"
  fi
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements.txt"
}

cloudflared_bin() {
  if command -v cloudflared >/dev/null 2>&1; then
    command -v cloudflared
    return
  fi

  local local_bin="${BIN_DIR}/cloudflared"
  if [[ -x "${local_bin}" ]]; then
    echo "${local_bin}"
    return
  fi

  echo "cloudflared was not found at ${local_bin}. Run scripts/start_live_backend.sh once to download it." >&2
  exit 1
}

backend_ready() {
  curl -fsS --max-time 5 "http://127.0.0.1:${PORT}/api/session" >/dev/null 2>&1
}

start_fallback_backend() {
  if [[ "${ALLOW_FALLBACK_BACKEND}" != "1" ]]; then
    return 1
  fi
  echo "No backend responded on http://127.0.0.1:${PORT}; starting local fallback backend."
  install_python_deps
  export PANEL_PAGES_PUBLIC_URL="${PAGES_URL}"
  export PANEL_CORS_ALLOWED_ORIGINS="${PANEL_CORS_ALLOWED_ORIGINS:-${PAGES_ORIGIN},${PAGES_URL%/},http://127.0.0.1:${PORT},http://localhost:${PORT}}"
  if [[ -z "${PANEL_DB_HOST:-}" || "${PANEL_DB_HOST}" == "host.docker.internal" ]]; then
    export PANEL_DB_HOST="127.0.0.1"
  fi
  cd "${ROOT_DIR}"
  "${VENV_DIR}/bin/python" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" >"${UVICORN_LOG}" 2>&1 &
  UVICORN_PID="$!"
}

cleanup() {
  if [[ -n "${PUBLISHED_PANEL_URL:-}" ]] && grep -Fq "\"panel_url\": \"${PUBLISHED_PANEL_URL}\"" "${CONFIG_FILE}" 2>/dev/null; then
    write_offline_config
    publish_config
  fi
  if [[ -n "${TUNNEL_PID:-}" ]]; then
    kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${UVICORN_PID:-}" ]]; then
    kill "${UVICORN_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

tunnel_retry_delay() {
  local attempt="$1"
  if (( attempt <= 3 )); then
    echo 15
  elif (( attempt <= 6 )); then
    echo 30
  else
    echo 60
  fi
}

log_tunnel_failure_details() {
  echo "Tunnel exited early. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  if grep -Fq 'status_code="429 Too Many Requests"' "${TUNNEL_LOG}" 2>/dev/null; then
    echo "Cloudflare quick tunnel creation is being rate-limited. Waiting before retrying." >&2
  fi
}

CLOUDFLARED="$(cloudflared_bin)"

echo "Waiting for SwarmPanel Docker backend on http://127.0.0.1:${PORT}"
for _ in {1..45}; do
  if backend_ready; then break; fi
  sleep 2
done

if ! backend_ready; then
  start_fallback_backend || true
  for _ in {1..60}; do
    if backend_ready; then break; fi
    if [[ -n "${UVICORN_PID:-}" ]] && ! kill -0 "${UVICORN_PID}" >/dev/null 2>&1; then
      echo "Fallback backend exited early. Last log lines:" >&2
      tail -80 "${UVICORN_LOG}" >&2 || true
      exit 1
    fi
    sleep 1
  done
fi

if ! backend_ready; then
  echo "SwarmPanel backend did not become reachable on port ${PORT}." >&2
  echo "Start Docker or run scripts/start_live_backend.sh ${PORT}." >&2
  exit 1
fi

for ((attempt=1; attempt<=MAX_TUNNEL_START_ATTEMPTS; attempt++)); do
  echo "Opening Cloudflare quick tunnel to http://127.0.0.1:${PORT} (attempt ${attempt}/${MAX_TUNNEL_START_ATTEMPTS})"
  : > "${TUNNEL_LOG}"
  "${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
  TUNNEL_PID="$!"

  PANEL_URL=""
  for _ in {1..120}; do
    if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
      log_tunnel_failure_details
      break
    fi

    PANEL_URL="$(grep -Eo 'https://[-a-zA-Z0-9.]+trycloudflare\.com' "${TUNNEL_LOG}" | tail -1 || true)"
    if [[ -z "${PANEL_URL}" ]]; then
      sleep 1
      continue
    fi

    echo "Waiting for ${PANEL_URL} to answer through Cloudflare..."
    for _ in {1..40}; do
      if curl -fsS --max-time 10 "${PANEL_URL}/api/session" >/dev/null 2>&1; then
        write_config "${PANEL_URL}"
        PUBLISHED_PANEL_URL="${PANEL_URL}"
        publish_config
        echo "Live backend URL: ${PANEL_URL}"
        echo "GitHub Pages frontend: ${PAGES_URL}"
        wait "${TUNNEL_PID}"
        exit $?
      fi
      if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
        log_tunnel_failure_details
        break
      fi
      sleep 1
    done

    if [[ -n "${PANEL_URL}" ]] && kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
      echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
      tail -80 "${TUNNEL_LOG}" >&2 || true
      kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
    fi
    break
  done

  if (( attempt < MAX_TUNNEL_START_ATTEMPTS )); then
    retry_delay="$(tunnel_retry_delay "${attempt}")"
    echo "Retrying Cloudflare quick tunnel startup in ${retry_delay}s..."
    sleep "${retry_delay}"
  fi
done

echo "Exceeded Cloudflare quick tunnel startup retries." >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
