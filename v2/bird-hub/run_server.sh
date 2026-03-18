#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${SCRIPT_DIR}"
export PYTHONPATH="${SCRIPT_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

exec "${INSTALL_ROOT}/.venv/bin/gunicorn" \
  --workers 1 \
  --threads 4 \
  --bind "${BIRD_MONITOR_HOST:-0.0.0.0}:${BIRD_MONITOR_PORT:-8080}" \
  "bird_hub.app:create_app()"
