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
INSTANCE_LOCK_FILE="${LOG_DIR}/live-manager.lock"
AUTO_PUSH_CONFIG="${PANEL_AUTO_PUSH_CONFIG:-1}"
ALLOW_FALLBACK_BACKEND="${PANEL_SERVICE_START_BACKEND_IF_MISSING:-1}"
TUNNEL_PROVIDER="${PANEL_TUNNEL_PROVIDER:-cloudflare}"
PAGES_ORIGIN="${PANEL_PAGES_ORIGIN:-https://heavenlyxenusvr.github.io}"
PAGES_URL="${PANEL_PAGES_PUBLIC_URL:-https://heavenlyxenusvr.github.io/SwarmPanel/}"
MAX_TUNNEL_START_ATTEMPTS="${PANEL_MAX_TUNNEL_START_ATTEMPTS:-12}"
TUNNEL_READY_ATTEMPTS="${PANEL_TUNNEL_READY_ATTEMPTS:-900}"
QUICK_TUNNEL_URL_ATTEMPTS="${PANEL_QUICK_TUNNEL_URL_ATTEMPTS:-180}"
CLOUDFLARE_PROTOCOL="${PANEL_CLOUDFLARE_PROTOCOL:-quic}"
CLOUDFLARE_TUNNEL_TOKEN="${PANEL_CLOUDFLARE_TUNNEL_TOKEN:-}"
CLOUDFLARE_PUBLIC_URL="${PANEL_CLOUDFLARE_PUBLIC_URL:-}"
GLOBAL_TUNNEL_STATE_DIR="${HOME}/.local/state/cloudflare-quick-tunnels"
GLOBAL_TUNNEL_LOCK_FILE="${GLOBAL_TUNNEL_STATE_DIR}/create.lock"
GLOBAL_TUNNEL_NEXT_ALLOWED_FILE="${GLOBAL_TUNNEL_STATE_DIR}/next-allowed-epoch"
GLOBAL_SUCCESS_COOLDOWN_SECONDS="${PANEL_CLOUDFLARE_SUCCESS_COOLDOWN_SECONDS:-180}"
GLOBAL_FAILURE_COOLDOWN_SECONDS="${PANEL_CLOUDFLARE_FAILURE_COOLDOWN_SECONDS:-300}"
GLOBAL_RATE_LIMIT_COOLDOWN_SECONDS="${PANEL_CLOUDFLARE_RATE_LIMIT_COOLDOWN_SECONDS:-900}"

mkdir -p "${BIN_DIR}" "${LOG_DIR}" "${GLOBAL_TUNNEL_STATE_DIR}"

current_live_url() {
  CONFIG_FILE_PATH="${CONFIG_FILE}" python3 <<'PY'
import json
import os
from pathlib import Path

config_path = Path(os.environ["CONFIG_FILE_PATH"])
try:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
except Exception:
    print("")
else:
    print(str(payload.get("panel_url") or "").strip())
PY
}

acquire_instance_lock() {
  exec {INSTANCE_LOCK_FD}> "${INSTANCE_LOCK_FILE}"
  if flock -n "${INSTANCE_LOCK_FD}"; then
    return 0
  fi

  local existing_url
  existing_url="$(current_live_url)"
  echo "Another SwarmPanel live tunnel manager is already running."
  if [[ -n "${existing_url}" ]]; then
    echo "Current published live URL: ${existing_url}"
  else
    echo "Current published live URL is not available yet. Reuse the running service instead of starting a second tunnel manager."
  fi
  exit 0
}

release_instance_lock() {
  if [[ -n "${INSTANCE_LOCK_FD:-}" ]]; then
    flock -u "${INSTANCE_LOCK_FD}" || true
    eval "exec ${INSTANCE_LOCK_FD}>&-"
    INSTANCE_LOCK_FD=""
  fi
}

flush_local_dns_cache() {
  if command -v resolvectl >/dev/null 2>&1; then
    resolvectl flush-caches >/dev/null 2>&1 || true
  elif command -v systemd-resolve >/dev/null 2>&1; then
    systemd-resolve --flush-caches >/dev/null 2>&1 || true
  fi
}

local_urls_json() {
  PORT="${PORT}" python3 <<'PY'
import json
import os
import socket

port = os.environ["PORT"]
urls = [f"http://127.0.0.1:{port}", f"http://localhost:{port}"]

try:
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.connect(("1.1.1.1", 80))
    urls.append(f"http://{udp.getsockname()[0]}:{port}")
    udp.close()
except Exception:
    pass

try:
    hostname = socket.gethostname()
    for family, *_rest, sockaddr in socket.getaddrinfo(hostname, None, family=socket.AF_INET, type=socket.SOCK_STREAM):
        ip = sockaddr[0]
        if ip.startswith("127."):
            continue
        urls.append(f"http://{ip}:{port}")
except Exception:
    pass

seen = set()
deduped = []
for url in urls:
    if url in seen:
        continue
    seen.add(url)
    deduped.append(url)

print(json.dumps(deduped))
PY
}

write_config() {
  local panel_url="$1"
  local local_urls
  local_urls="$(local_urls_json)"
  cat > "${CONFIG_FILE}" <<EOF
{
  "panel_url": "${panel_url}",
  "status": "live",
  "local_urls": ${local_urls},
  "updated_at": "$(date -Is)"
}
EOF
}

write_offline_config() {
  local local_urls
  local_urls="$(local_urls_json)"
  cat > "${CONFIG_FILE}" <<EOF
{
  "panel_url": "",
  "status": "offline",
  "local_urls": ${local_urls},
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
  if ! GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false run_git ls-remote --exit-code origin HEAD >/dev/null 2>&1; then
    echo "Skipping live-config auto-push because GitHub auth is unavailable; local file was still updated."
    return
  fi
  echo "Publishing updated live-config.json to GitHub Pages..."
  run_git add live-config.json
  run_git commit -m "Update live backend URL" -- live-config.json || true
  GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false run_git push origin main || echo "Could not push live-config.json automatically." >&2
}

publish_offline_config() {
  write_offline_config
  publish_config
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

port_in_use() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :${PORT}" | grep -q ":${PORT}"
    return
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1
    return
  fi
  return 1
}

kill_stale_port() {
  if [[ "${PANEL_KILL_STALE_PORT:-1}" != "1" ]]; then
    return 0
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"${PORT}" | xargs -r kill -TERM || true
  fi
}

start_fallback_backend() {
  if [[ "${ALLOW_FALLBACK_BACKEND}" != "1" ]]; then
    return 1
  fi
  if port_in_use && ! backend_ready; then
    echo "Port ${PORT} is busy but the SwarmPanel backend is not responding; attempting cleanup."
    kill_stale_port
    sleep 1
  fi
  if port_in_use && ! backend_ready; then
    echo "Port ${PORT} is still busy; cannot start fallback backend." >&2
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

release_global_tunnel_slot() {
  if [[ -n "${GLOBAL_TUNNEL_SLOT_FD:-}" ]]; then
    flock -u "${GLOBAL_TUNNEL_SLOT_FD}" || true
    eval "exec ${GLOBAL_TUNNEL_SLOT_FD}>&-"
    GLOBAL_TUNNEL_SLOT_FD=""
  fi
}

cleanup() {
  release_instance_lock
  release_global_tunnel_slot
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

acquire_instance_lock

tunnel_retry_delay() {
  local attempt="$1"
  if (( attempt <= 2 )); then
    echo 30
  elif (( attempt <= 5 )); then
    echo 90
  else
    echo 180
  fi
}

tunnel_was_rate_limited() {
  grep -Fq 'status_code="429 Too Many Requests"' "${TUNNEL_LOG}" 2>/dev/null
}

log_tunnel_failure_details() {
  echo "Tunnel exited early. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  if tunnel_was_rate_limited; then
    echo "Cloudflare quick tunnel creation is being rate-limited. Waiting before retrying." >&2
  fi
}

set_global_tunnel_cooldown() {
  local seconds="$1"
  printf '%s\n' "$(( $(date +%s) + seconds ))" > "${GLOBAL_TUNNEL_NEXT_ALLOWED_FILE}"
}

acquire_global_tunnel_slot() {
  mkdir -p "${GLOBAL_TUNNEL_STATE_DIR}"
  exec {GLOBAL_TUNNEL_SLOT_FD}> "${GLOBAL_TUNNEL_LOCK_FILE}"
  flock "${GLOBAL_TUNNEL_SLOT_FD}"
  local next_allowed=0 now wait_seconds
  if [[ -f "${GLOBAL_TUNNEL_NEXT_ALLOWED_FILE}" ]]; then
    read -r next_allowed < "${GLOBAL_TUNNEL_NEXT_ALLOWED_FILE}" || next_allowed=0
  fi
  now="$(date +%s)"
  if (( next_allowed > now )); then
    wait_seconds="$(( next_allowed - now ))"
    echo "Cloudflare quick tunnel cooldown active; waiting ${wait_seconds}s before requesting a new public URL."
    sleep "${wait_seconds}"
  fi
}

resolve_public_ipv4s() {
  local host="$1"
  if command -v dig >/dev/null 2>&1; then
    dig +short @1.1.1.1 "${host}" A 2>/dev/null | awk 'NF'
    return
  fi
  if command -v getent >/dev/null 2>&1; then
    getent ahostsv4 "${host}" 2>/dev/null | awk '{print $1}' | sort -u
  fi
}

cloudflare_url_ready() {
  local public_url="$1"
  local health_path="$2"
  local host="${public_url#https://}"
  host="${host%%/*}"

  if curl -fsS --max-time 10 "${public_url}${health_path}" >/dev/null 2>&1; then
    return 0
  fi

  local ip
  while IFS= read -r ip; do
    [[ -z "${ip}" ]] && continue
    if curl -gfsS --max-time 10 --resolve "${host}:443:${ip}" "${public_url}${health_path}" >/dev/null 2>&1; then
      return 0
    fi
  done < <(resolve_public_ipv4s "${host}")

  return 1
}

announce_live_url() {
  local panel_url="$1"
  if [[ "${PUBLISHED_PANEL_URL:-}" == "${panel_url}" ]]; then
    return
  fi
  write_config "${panel_url}"
  flush_local_dns_cache
  PUBLISHED_PANEL_URL="${panel_url}"
  publish_config
  echo "Live backend URL: ${panel_url}"
  echo "GitHub Pages frontend: ${PAGES_URL}"
}

wait_for_public_readiness() {
  local panel_url="$1"
  local health_path="$2"
  local ready_attempt
  for ((ready_attempt=1; ready_attempt<=TUNNEL_READY_ATTEMPTS; ready_attempt++)); do
    if cloudflare_url_ready "${panel_url}" "${health_path}"; then
      echo "Cloudflare public URL is answering: ${panel_url}"
      return 0
    fi
    if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
      return 1
    fi
    sleep 1
  done
  echo "Cloudflare has not answered yet for ${panel_url}, but the tunnel is still running. Keeping it alive to give propagation more time."
  return 0
}

start_named_cloudflare_tunnel() {
  local public_url="$1"
  local token="$2"
  local health_path="$3"

  if [[ -z "${token}" || -z "${public_url}" ]]; then
    return 1
  fi

  echo "Starting named Cloudflare tunnel for ${public_url}"
  : > "${TUNNEL_LOG}"
  "${CLOUDFLARED}" tunnel --no-autoupdate run --token "${token}" >"${TUNNEL_LOG}" 2>&1 &
  TUNNEL_PID="$!"

  announce_live_url "${public_url}"
  wait_for_public_readiness "${public_url}" "${health_path}" || {
    echo "Named Cloudflare tunnel exited before it became reachable." >&2
    tail -80 "${TUNNEL_LOG}" >&2 || true
    return 1
  }

  wait "${TUNNEL_PID}"
  exit $?
}

CLOUDFLARED="$(cloudflared_bin)"

echo "Waiting for SwarmPanel backend on http://127.0.0.1:${PORT}"
for _ in {1..45}; do
  if backend_ready; then
    break
  fi
  sleep 2
done

if ! backend_ready; then
  start_fallback_backend || true
  for _ in {1..60}; do
    if backend_ready; then
      break
    fi
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
  publish_offline_config
  exit 1
fi

publish_offline_config

if [[ "${TUNNEL_PROVIDER}" == "cloudflare" || "${TUNNEL_PROVIDER}" == "auto" ]]; then
  if start_named_cloudflare_tunnel "${CLOUDFLARE_PUBLIC_URL}" "${CLOUDFLARE_TUNNEL_TOKEN}" "/api/session"; then
    exit 0
  fi

  for ((attempt=1; attempt<=MAX_TUNNEL_START_ATTEMPTS; attempt++)); do
    acquire_global_tunnel_slot
    echo "Opening Cloudflare quick tunnel to http://127.0.0.1:${PORT} (attempt ${attempt}/${MAX_TUNNEL_START_ATTEMPTS})"
    : > "${TUNNEL_LOG}"
    "${CLOUDFLARED}" tunnel --no-autoupdate --protocol "${CLOUDFLARE_PROTOCOL}" --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
    TUNNEL_PID="$!"

    PANEL_URL=""
    for _ in $(seq 1 "${QUICK_TUNNEL_URL_ATTEMPTS}"); do
      if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
        break
      fi
      PANEL_URL="$(grep -Eo 'https://[-a-zA-Z0-9.]+trycloudflare\.com' "${TUNNEL_LOG}" | tail -1 || true)"
      if [[ -n "${PANEL_URL}" ]]; then
        break
      fi
      sleep 1
    done

    if [[ -z "${PANEL_URL}" ]]; then
      log_tunnel_failure_details
      if tunnel_was_rate_limited; then
        set_global_tunnel_cooldown "${GLOBAL_RATE_LIMIT_COOLDOWN_SECONDS}"
      else
        set_global_tunnel_cooldown "${GLOBAL_FAILURE_COOLDOWN_SECONDS}"
      fi
      release_global_tunnel_slot
    else
      set_global_tunnel_cooldown "${GLOBAL_SUCCESS_COOLDOWN_SECONDS}"
      release_global_tunnel_slot
      announce_live_url "${PANEL_URL}"
      wait_for_public_readiness "${PANEL_URL}" "/api/session" || {
        echo "Cloudflare quick tunnel died before it became reachable." >&2
        tail -80 "${TUNNEL_LOG}" >&2 || true
        publish_offline_config
        if tunnel_was_rate_limited; then
          set_global_tunnel_cooldown "${GLOBAL_RATE_LIMIT_COOLDOWN_SECONDS}"
        else
          set_global_tunnel_cooldown "${GLOBAL_FAILURE_COOLDOWN_SECONDS}"
        fi
        continue
      }

      wait "${TUNNEL_PID}"
      exit $?
    fi

    if (( attempt < MAX_TUNNEL_START_ATTEMPTS )); then
      retry_delay="$(tunnel_retry_delay "${attempt}")"
      echo "Retrying Cloudflare quick tunnel startup in ${retry_delay}s..."
      sleep "${retry_delay}"
    fi
  done
fi

publish_offline_config
echo "Exceeded Cloudflare tunnel startup retries." >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
