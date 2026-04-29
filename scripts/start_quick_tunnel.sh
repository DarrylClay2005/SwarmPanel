#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/live-config.json"
LOG_DIR="${ROOT_DIR}/.runtime"
TUNNEL_LOG="${LOG_DIR}/cloudflared-quick.log"
AUTO_PUSH_CONFIG="${PANEL_AUTO_PUSH_CONFIG:-1}"

mkdir -p "${LOG_DIR}"

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
    echo "PANEL_AUTO_PUSH_CONFIG is disabled; live-config.json was updated locally only."
    return
  fi
  if ! command -v git >/dev/null 2>&1 && ! command -v flatpak-spawn >/dev/null 2>&1; then
    echo "Skipping live-config push because git is unavailable." >&2
    return
  fi
  if ! run_host_git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Skipping live-config push because ${ROOT_DIR} is not a git work tree." >&2
    return
  fi
  if run_host_git diff --quiet -- live-config.json; then
    echo "live-config.json already matches the current tunnel URL."
    return
  fi
  echo "Publishing updated live-config.json to GitHub Pages..."
  run_host_git add live-config.json
  run_host_git commit -m "Update live backend URL" -- live-config.json || true
  run_host_git push origin main || echo "Could not push live-config.json automatically. Push it manually when convenient." >&2
}

cleanup() {
  if [[ -n "${PUBLISHED_PANEL_URL:-}" ]] && grep -Fq "\"panel_url\": \"${PUBLISHED_PANEL_URL}\"" "${CONFIG_FILE}" 2>/dev/null; then
    write_offline_config
    publish_config
  fi
  if [[ -n "${TUNNEL_PID:-}" ]]; then
    kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

echo "Opening a public Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
echo "Keep this process running while you want the phone site to stay connected."
echo "When the tunnel URL appears, ${CONFIG_FILE} will be updated automatically."
echo

docker run --rm --network host cloudflare/cloudflared:latest tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
TUNNEL_PID="$!"

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
      PUBLISHED_PANEL_URL="${PANEL_URL}"
      publish_config
      echo
      echo "Updated ${CONFIG_FILE} with ${PANEL_URL}"
      echo
      wait "${TUNNEL_PID}"
      exit $?
    fi
    sleep 1
  done
  echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  exit 1
done

echo "Timed out waiting for Cloudflare tunnel URL. Last log lines:" >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
