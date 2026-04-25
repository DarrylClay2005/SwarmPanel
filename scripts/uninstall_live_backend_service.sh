#!/usr/bin/env bash
set -euo pipefail

SERVICE_FILE="${HOME}/.config/systemd/user/swarmpanel-live-backend.service"

systemctl --user disable --now swarmpanel-live-backend.service >/dev/null 2>&1 || true
rm -f "${SERVICE_FILE}"
systemctl --user daemon-reload

echo "Removed swarmpanel-live-backend.service."
