#!/usr/bin/env bash
# Prepare host-mounted runtime files and directories exactly once per Compose up.
set -euo pipefail
umask 077

BACKEND_DIR="${BACKEND_DIR:-/app}"
APP_ENV_FILE="${APP_ENV_FILE:-$BACKEND_DIR/nrm_app/.env}"
ENV_TEMPLATE="${ENV_TEMPLATE:-$BACKEND_DIR/installation/docker/env.template}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"

upsert_env() {
    local file="$1"
    local key="$2"
    local value="$3"
    [ -n "$value" ] || return 0
    python - "$file" "$key" "$value" <<'PY'
import os
import sys
import tempfile

path, key, value = sys.argv[1:]
if "\n" in value or "\r" in value:
    raise SystemExit(f"Refusing multiline value for {key}")

prefix = f"{key}="
with open(path, encoding="utf-8") as handle:
    lines = handle.readlines()

replacement = f"{prefix}{value}\n"
updated = []
found = False
for line in lines:
    if line.startswith(prefix):
        if not found:
            updated.append(replacement)
            found = True
    else:
        updated.append(line)
if not found:
    updated.append(replacement)

directory = os.path.dirname(path) or "."
fd, temporary_path = tempfile.mkstemp(dir=directory)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.writelines(updated)
    os.chmod(temporary_path, 0o600)
    os.replace(temporary_path, path)
except BaseException:
    try:
        os.unlink(temporary_path)
    except FileNotFoundError:
        pass
    raise
PY
}

if [ ! -f "$APP_ENV_FILE" ]; then
    echo "Creating $APP_ENV_FILE from the Docker template..."
    mkdir -p "$(dirname "$APP_ENV_FILE")"
    cp "$ENV_TEMPLATE" "$APP_ENV_FILE"
fi

if ! grep -q '^SECRET_KEY=..*' "$APP_ENV_FILE" || grep -q '^SECRET_KEY=change-me-in-docker$' "$APP_ENV_FILE"; then
    generated_secret="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"
    upsert_env "$APP_ENV_FILE" "SECRET_KEY" "$generated_secret"
fi

if ! grep -q '^FERNET_KEY=..*' "$APP_ENV_FILE"; then
    generated_fernet_key="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
    upsert_env "$APP_ENV_FILE" "FERNET_KEY" "$generated_fernet_key"
fi

upsert_env "$APP_ENV_FILE" "BACKEND_DIR" "$BACKEND_DIR"
upsert_env "$APP_ENV_FILE" "DATA_DIR" "$DATA_DIR"
upsert_env "$APP_ENV_FILE" "EXCEL_DIR" "$DATA_DIR/excel_files"

# Keep Django's host-mounted environment consistent with the values resolved
# by Compose from .env.core-stack-docker. Container environment variables still
# take precedence, but host-side inspection and management remain unambiguous.
runtime_keys=(
    DB_NAME DB_USER DB_PASSWORD DB_HOST DB_PORT DB_CONN_MAX_AGE
    CELERY_BROKER_URL CELERY_RESULT_BACKEND
    GEOSERVER_URL GEOSERVER_USERNAME GEOSERVER_PASSWORD
    DEBUG ALLOWED_HOSTS LAYER_GENERATION_SYNC_MODE SYNC_LAYER
    STAC_UPLOAD_TO_S3 GPU_AVAILABLE
)
for key in "${runtime_keys[@]}"; do
    upsert_env "$APP_ENV_FILE" "$key" "${!key:-}"
done

chmod 600 "$APP_ENV_FILE"
runtime_dirs=(
    "$BACKEND_DIR/logs"
    "$BACKEND_DIR/tmp"
    "$BACKEND_DIR/data/gee_confs"
    "$BACKEND_DIR/bot_interface/whatsapp_media"
    "$DATA_DIR/activated_locations"
    "$DATA_DIR/excel_files"
    "$DATA_DIR/admin-boundary"
    "$DATA_DIR/base_layers/tehsil_watersheds"
)
mkdir -p "${runtime_dirs[@]}"
touch "$BACKEND_DIR/logs/app.log" "$BACKEND_DIR/logs/nrm_app.log"

echo "Runtime directories and Django environment are ready."
