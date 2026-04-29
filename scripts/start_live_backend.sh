#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
BIN_DIR="${ROOT_DIR}/.bin"
CONFIG_FILE="${ROOT_DIR}/live-config.json"
PAGES_ORIGIN="${PANEL_PAGES_ORIGIN:-https://darrylclay2005.github.io}"
PAGES_URL="${PANEL_PAGES_PUBLIC_URL:-https://darrylclay2005.github.io/SwarmPanel/}"
LOG_DIR="${ROOT_DIR}/.runtime"
UVICORN_LOG="${LOG_DIR}/uvicorn.log"
TUNNEL_LOG="${LOG_DIR}/cloudflared.log"
PID_FILE="${LOG_DIR}/live_backend.pid"
AUTO_PUSH_CONFIG="${PANEL_AUTO_PUSH_CONFIG:-1}"

mkdir -p "${BIN_DIR}" "${LOG_DIR}"
echo "$$" > "${PID_FILE}"

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

run_host_git() {
  if command -v flatpak-spawn >/dev/null 2>&1; then
    flatpak-spawn --host git -C "${ROOT_DIR}" "$@"
  else
    git -C "${ROOT_DIR}" "$@"
  fi
}

publish_config() {
  if [[ "${AUTO_PUSH_CONFIG}" != "1" ]]; then
    return
  fi
  if ! command -v git >/dev/null 2>&1 && ! command -v flatpak-spawn >/dev/null 2>&1; then
    echo "Skipping live-config push because git is unavailable." >&2
    return
  fi
  if run_host_git diff --quiet -- live-config.json; then
    return
  fi
  echo "Publishing updated live-config.json to GitHub Pages..."
  run_host_git add live-config.json
  run_host_git commit -m "Update live backend URL" -- live-config.json || true
  run_host_git push origin main || echo "Could not push live-config.json automatically. Push it manually when convenient." >&2
}

install_python_deps() {
  if [[ -x "${VENV_DIR}/bin/python" ]] && ! "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
    echo "Existing virtualenv cannot run pip; rebuilding ${VENV_DIR}..." >&2
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

  local machine
  machine="$(uname -m)"
  local arch="amd64"
  if [[ "${machine}" == "aarch64" || "${machine}" == "arm64" ]]; then
    arch="arm64"
  fi

  local url="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${arch}"
  echo "Downloading cloudflared (${arch})..." >&2
  if command -v curl >/dev/null 2>&1; then
    curl -L --fail "${url}" -o "${local_bin}"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "${local_bin}" "${url}"
  else
    echo "Need curl or wget to download cloudflared." >&2
    exit 1
  fi
  chmod +x "${local_bin}"
  echo "${local_bin}"
}

cleanup() {
  if [[ -n "${PUBLISHED_PANEL_URL:-}" ]] && grep -Fq "\"panel_url\": \"${PUBLISHED_PANEL_URL}\"" "${CONFIG_FILE}" 2>/dev/null; then
    write_offline_config
    publish_config
  fi
  rm -f "${PID_FILE}"
  if [[ -n "${UVICORN_PID:-}" ]]; then
    kill "${UVICORN_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TUNNEL_PID:-}" ]]; then
    kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

install_python_deps
CLOUDFLARED="$(cloudflared_bin)"

export PANEL_PAGES_PUBLIC_URL="${PAGES_URL}"
export PANEL_CORS_ALLOWED_ORIGINS="${PANEL_CORS_ALLOWED_ORIGINS:-${PAGES_ORIGIN},${PAGES_URL%/},http://127.0.0.1:${PORT},http://localhost:${PORT}}"
if [[ -z "${PANEL_DB_HOST:-}" || "${PANEL_DB_HOST}" == "host.docker.internal" ]]; then
  export PANEL_DB_HOST="127.0.0.1"
fi

cd "${ROOT_DIR}"
echo "Starting SwarmPanel backend on http://127.0.0.1:${PORT}"
"${VENV_DIR}/bin/python" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" >"${UVICORN_LOG}" 2>&1 &
UVICORN_PID="$!"

for _ in {1..40}; do
  if curl -fsS "http://127.0.0.1:${PORT}/api/session" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${UVICORN_PID}" >/dev/null 2>&1; then
    echo "Backend exited early. Last log lines:" >&2
    tail -80 "${UVICORN_LOG}" >&2 || true
    exit 1
  fi
  sleep 0.5
done

echo "Opening Cloudflare quick tunnel..."
"${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
TUNNEL_PID="$!"

PANEL_URL=""
for _ in {1..80}; do
  if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
    echo "Tunnel exited early. Last log lines:" >&2
    tail -80 "${TUNNEL_LOG}" >&2 || true
    exit 1
  fi
  PANEL_URL="$(grep -Eo 'https://[-a-zA-Z0-9.]+trycloudflare\.com' "${TUNNEL_LOG}" | tail -1 || true)"
  if [[ -n "${PANEL_URL}" ]]; then
    echo "Waiting for ${PANEL_URL} to answer through Cloudflare..."
    for _ in {1..40}; do
      if curl -fsS --max-time 10 "${PANEL_URL}/api/session" >/dev/null 2>&1; then
        break
      fi
      if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
        echo "Tunnel exited before it became reachable. Last log lines:" >&2
        tail -80 "${TUNNEL_LOG}" >&2 || true
        exit 1
      fi
      sleep 1
    done
    if ! curl -fsS --max-time 10 "${PANEL_URL}/api/session" >/dev/null 2>&1; then
      echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
      tail -80 "${TUNNEL_LOG}" >&2 || true
      exit 1
    fi
    write_config "${PANEL_URL}"
    PUBLISHED_PANEL_URL="${PANEL_URL}"
    publish_config
    echo
    echo "Live backend URL: ${PANEL_URL}"
    echo "Updated ${CONFIG_FILE}"
    echo "GitHub Pages front-end: ${PAGES_URL}"
    echo
    echo "Keep this script running while you want the live site connected."
    wait "${TUNNEL_PID}"
    exit $?
  fi
  sleep 0.5
done

echo "Timed out waiting for Cloudflare tunnel URL. Last log lines:" >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
