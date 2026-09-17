#!/usr/bin/env bash
# Point the runtime env at mounted GEE JSON: project id, GCS bucket, key path.
# Django GEEAccount import stays manual (admin). Writes a shared file on the
# data volume so the backend container can pick the values up.
set -euo pipefail

GEE_DIR="${GEE_CONFS_DIR:-/app/data/gee_confs}"
APP_ENV_FILE="${APP_ENV_FILE:-/app/nrm_app/.env}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
MARKER="${DATA_DIR}/.gee_config_checked"
RUNTIME_ENV="${DATA_DIR}/.gee_runtime.env"

read_project_id() {
    local json_path="$1"
    python - "$json_path" <<'PY'
import json, sys
path = sys.argv[1]
try:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
except (OSError, json.JSONDecodeError):
    print("")
    raise SystemExit(0)
print((payload.get("project_id") or "").strip())
PY
}

upsert_env() {
    local file="$1"
    local key="$2"
    local value="$3"
    [ -n "$value" ] || return 0
    mkdir -p "$(dirname "$file")"
    if [ -f "$file" ] && grep -q "^${key}=" "$file"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$file"
    else
        echo "${key}=${value}" >> "$file"
    fi
}

mkdir -p "$GEE_DIR" "$DATA_DIR"

jsons=()
while IFS= read -r path; do
    jsons+=("$path")
done < <(find "$GEE_DIR" -maxdepth 2 -type f -name '*.json' 2>/dev/null | sort)

echo "GEE config directory: $GEE_DIR"
if [ "${#jsons[@]}" -eq 0 ]; then
    echo "No GEE JSON found. Copy your service-account file to ./gee_confs/ and restart."
    echo "Expected name (optional): gee-service-account.json"
    echo "missing" > "$MARKER"
    : > "$RUNTIME_ENV"
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

helper="${GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH:-}"
if [ -z "$helper" ] || [ ! -f "$helper" ]; then
    if [ -f "$GEE_DIR/gee-helper-account.json" ]; then
        helper="$GEE_DIR/gee-helper-account.json"
    else
        helper=""
    fi
fi

echo "Using GEE service account: $primary"
for path in "${jsons[@]}"; do
    echo "  - $path"
done

json_project="$(read_project_id "$primary")"
project="${GEE_STORAGE_PROJECT:-$json_project}"
helper_project=""
if [ -n "$helper" ]; then
    helper_project="$(read_project_id "$helper")"
fi
helper_project="${GEE_STORAGE_PROJECT_HELPER:-${helper_project:-$project}}"
gcs_bucket="${GCS_BUCKET_NAME:-}"

: > "$RUNTIME_ENV"
upsert_env "$RUNTIME_ENV" "GEE_SERVICE_ACCOUNT_KEY_PATH" "$primary"
upsert_env "$RUNTIME_ENV" "GEE_STORAGE_PROJECT" "$project"
upsert_env "$RUNTIME_ENV" "GEE_STORAGE_PROJECT_HELPER" "$helper_project"
upsert_env "$RUNTIME_ENV" "GCS_BUCKET_NAME" "$gcs_bucket"
if [ -n "$helper" ]; then
    upsert_env "$RUNTIME_ENV" "GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH" "$helper"
fi

if [ -f "$APP_ENV_FILE" ]; then
    upsert_env "$APP_ENV_FILE" "GEE_SERVICE_ACCOUNT_KEY_PATH" "$primary"
    upsert_env "$APP_ENV_FILE" "GEE_STORAGE_PROJECT" "$project"
    upsert_env "$APP_ENV_FILE" "GEE_STORAGE_PROJECT_HELPER" "$helper_project"
    upsert_env "$APP_ENV_FILE" "GCS_BUCKET_NAME" "$gcs_bucket"
    if [ -n "$helper" ]; then
        upsert_env "$APP_ENV_FILE" "GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH" "$helper"
    fi
fi

echo "$primary" > "$MARKER"
echo "GEE project: ${project:-<empty>}"
echo "GEE helper project: ${helper_project:-<empty>}"
if [ -n "$gcs_bucket" ]; then
    echo "GCS bucket: $gcs_bucket"
else
    echo "GCS_BUCKET_NAME is not set. Raster publish to GeoServer needs a bucket; set GCS_BUCKET_NAME in the Compose .env."
fi
echo "GEE path written. Add the account in Django admin if it is not imported yet."
