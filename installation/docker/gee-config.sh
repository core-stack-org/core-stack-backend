#!/usr/bin/env bash
# Point the runtime env at mounted GEE service-account JSON. Import stays manual.
set -euo pipefail

GEE_DIR="${GEE_CONFS_DIR:-/app/data/gee_confs}"
APP_ENV_FILE="${APP_ENV_FILE:-/app/nrm_app/.env}"
MARKER="${DATA_DIR:-/var/tmp/core-stack-data}/.gee_config_checked"

mkdir -p "$GEE_DIR"

jsons=()
while IFS= read -r path; do
    jsons+=("$path")
done < <(find "$GEE_DIR" -maxdepth 2 -type f -name '*.json' 2>/dev/null | sort)

echo "GEE config directory: $GEE_DIR"
if [ "${#jsons[@]}" -eq 0 ]; then
    echo "No GEE JSON found. Copy your service-account file to ./gee_confs/ and restart."
    echo "Expected name (optional): gee-service-account.json"
    mkdir -p "$(dirname "$MARKER")"
    echo "missing" > "$MARKER"
    exit 0
fi

primary="${GEE_SERVICE_ACCOUNT_KEY_PATH:-}"
if [ -z "$primary" ] || [ ! -f "$primary" ]; then
    if [ -f "$GEE_DIR/gee-service-account.json" ]; then
        primary="$GEE_DIR/gee-service-account.json"
    else
        primary="${jsons[0]}"
    fi
fi

echo "Using GEE service account: $primary"
for path in "${jsons[@]}"; do
    echo "  - $path"
done

if [ -f "$APP_ENV_FILE" ]; then
    if grep -q '^GEE_SERVICE_ACCOUNT_KEY_PATH=' "$APP_ENV_FILE"; then
        sed -i "s|^GEE_SERVICE_ACCOUNT_KEY_PATH=.*|GEE_SERVICE_ACCOUNT_KEY_PATH=${primary}|" "$APP_ENV_FILE"
    else
        echo "GEE_SERVICE_ACCOUNT_KEY_PATH=${primary}" >> "$APP_ENV_FILE"
    fi
fi

mkdir -p "$(dirname "$MARKER")"
echo "$primary" > "$MARKER"
echo "GEE path written. Add the account in Django admin if it is not imported yet."
