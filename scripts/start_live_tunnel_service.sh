#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/.bin"
CONFIG_FILE="${ROOT_DIR}/live-config.json"
LOG_DIR="${ROOT_DIR}/.runtime"
TUNNEL_LOG="${LOG_DIR}/cloudflared-service.log"
AUTO_PUSH_CONFIG="${PANEL_AUTO_PUSH_CONFIG:-1}"

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

run_git() {
  git -C "${ROOT_DIR}" "$@"
}

publish_config() {
  if [[ "${AUTO_PUSH_CONFIG}" != "1" ]]; then
    return
  fi
  if run_git diff --quiet -- live-config.json; then
    return
  fi
  run_git add live-config.json
  run_git commit -m "Update live backend URL" -- live-config.json || true
  run_git push origin main || echo "Could not push live-config.json automatically." >&2
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

CLOUDFLARED="$(cloudflared_bin)"

echo "Waiting for SwarmPanel Docker backend on http://127.0.0.1:${PORT}"
for _ in {1..180}; do
  if curl -fsS --max-time 5 "http://127.0.0.1:${PORT}/api/session" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! curl -fsS --max-time 5 "http://127.0.0.1:${PORT}/api/session" >/dev/null 2>&1; then
  echo "SwarmPanel backend did not become reachable on port ${PORT}." >&2
  exit 1
fi

echo "Opening Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
"${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
TUNNEL_PID="$!"

cleanup() {
  kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

PANEL_URL=""
for _ in {1..120}; do
  if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
    echo "Tunnel exited early. Last log lines:" >&2
    tail -80 "${TUNNEL_LOG}" >&2 || true
    exit 1
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
      publish_config
      echo "Live backend URL: ${PANEL_URL}"
      wait "${TUNNEL_PID}"
      exit $?
    fi
    sleep 1
  done

  echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  exit 1
done

echo "Timed out waiting for Cloudflare tunnel URL." >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
