#!/usr/bin/env bash
# Runtime wrapper for Django and Celery. Mutable setup belongs in one-shot
# Compose services, not in every web/worker restart.
set -euo pipefail

BACKEND_DIR="${BACKEND_DIR:-/app}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
RUNTIME_ENV="${DATA_DIR}/.gee_runtime.env"

cd "$BACKEND_DIR"

if [ -f "$RUNTIME_ENV" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$RUNTIME_ENV"
    set +a
fi

runtime_dirs=(
    "$BACKEND_DIR/logs"
    "$BACKEND_DIR/tmp"
    "$BACKEND_DIR/bot_interface/whatsapp_media"
    "$DATA_DIR/activated_locations"
    "$DATA_DIR/excel_files"
    "$DATA_DIR/base_layers/tehsil_watersheds"
)
mkdir -p "${runtime_dirs[@]}"

if [ "$#" -eq 0 ]; then
    set -- gunicorn nrm_app.wsgi:application --bind 0.0.0.0:8000 --workers "${GUNICORN_WORKERS:-2}" --timeout "${GUNICORN_TIMEOUT:-7500}" --graceful-timeout "${GUNICORN_GRACEFUL_TIMEOUT:-120}" --access-logfile - --error-logfile -
fi

exec "$@"
