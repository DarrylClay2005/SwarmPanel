#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SERVICE_DIR}/swarmpanel-live-backend.service"

mkdir -p "${SERVICE_DIR}"

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=SwarmPanel live backend and GitHub Pages tunnel
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${ROOT_DIR}
Environment=PANEL_SERVICE_START_BACKEND_IF_MISSING=0
Environment=PANEL_PUSH_OFFLINE_CONFIG=0
Environment=PANEL_CLOUDFLARE_PROTOCOL=http2
Environment=PANEL_KILL_STALE_PORT=0
ExecStart=/usr/bin/env bash -lc 'cd "${ROOT_DIR}" && exec bash ./scripts/start_live_tunnel_service.sh 8000'
Restart=always
RestartSec=15
TimeoutStopSec=20

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now swarmpanel-live-backend.service

if command -v loginctl >/dev/null 2>&1; then
  loginctl enable-linger "${USER}" >/dev/null 2>&1 || true
fi

systemctl --user --no-pager status swarmpanel-live-backend.service
