#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/live-config.json"

write_config() {
  local panel_url="$1"
  cat > "${CONFIG_FILE}" <<EOF
{
  "panel_url": "${panel_url}",
  "updated_at": "$(date -Is)"
}
EOF
}

echo "Opening a public Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
echo "Keep this process running while you want the phone site to stay connected."
echo "When the tunnel URL appears, ${CONFIG_FILE} will be updated automatically."
echo

docker run --rm --network host cloudflare/cloudflared:latest tunnel --no-autoupdate --url "http://127.0.0.1:${PORT}" 2>&1 \
  | while IFS= read -r line; do
      echo "${line}"
      if [[ "${line}" =~ https://[-a-zA-Z0-9.]+trycloudflare\.com ]]; then
        write_config "${BASH_REMATCH[0]}"
        echo
        echo "Updated ${CONFIG_FILE} with ${BASH_REMATCH[0]}"
        echo
      fi
    done
