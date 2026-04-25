#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${ROOT_DIR}/.runtime/live_backend.pid"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "No live backend pid file found."
  exit 0
fi

PID="$(cat "${PID_FILE}")"
if [[ -z "${PID}" ]]; then
  rm -f "${PID_FILE}"
  echo "Live backend pid file was empty."
  exit 0
fi

if kill "${PID}" >/dev/null 2>&1; then
  echo "Stopped live backend launcher ${PID}."
else
  echo "Live backend launcher ${PID} was not running."
fi

rm -f "${PID_FILE}"
