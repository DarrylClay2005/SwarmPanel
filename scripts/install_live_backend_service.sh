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
ExecStart=/usr/bin/env bash -lc 'cd "${ROOT_DIR}" && exec ./scripts/start_live_backend.sh 8787'
ExecStop=/usr/bin/env bash -lc 'cd "${ROOT_DIR}" && ./scripts/stop_live_backend.sh || true'
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
