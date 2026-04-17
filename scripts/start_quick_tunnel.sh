#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8000}"

echo "Opening a public Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
echo "Keep this process running while you want the phone site to stay connected."
echo

docker run --rm --network host cloudflare/cloudflared:latest tunnel --no-autoupdate --url "http://127.0.0.1:${PORT}"
