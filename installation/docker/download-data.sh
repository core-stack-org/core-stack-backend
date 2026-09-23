#!/usr/bin/env bash
# Download admin-boundary data and local-compute base layers inside the
# container onto DATA_DIR (same volume used at runtime).
set -euo pipefail

DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
BACKEND_DIR="${BACKEND_DIR:-/app}"
APP_ENV_FILE="${APP_ENV_FILE:-$BACKEND_DIR/nrm_app/.env}"
ENV_TEMPLATE="${ENV_TEMPLATE:-$BACKEND_DIR/installation/docker/env.template}"
ADMIN_DIR="$DATA_DIR/admin-boundary"
ARCHIVE="$DATA_DIR/dataset.7z"
EXTRACT="$DATA_DIR/.admin-boundary-extract"
FILE_ID="${ADMIN_BOUNDARY_GDRIVE_ID:-1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d}"

admin_boundary_ready() {
    [ -f "$ADMIN_DIR/input/soi_tehsil.geojson" ] \
        && find "$ADMIN_DIR/input" -mindepth 2 -name '*.geojson' -print -quit 2>/dev/null | grep -q .
}

normalize_nested_layout() {
    local nested="$ADMIN_DIR/admin-boundary"
    if [ -f "$nested/input/soi_tehsil.geojson" ]; then
        echo "Normalizing nested admin-boundary layout..."
        mkdir -p "$ADMIN_DIR/input" "$ADMIN_DIR/output"
        if [ -d "$nested/input" ]; then
            find "$nested/input" -mindepth 1 -maxdepth 1 -exec mv -t "$ADMIN_DIR/input" {} +
        fi
        if [ -d "$nested/output" ]; then
            find "$nested/output" -mindepth 1 -maxdepth 1 -exec mv -t "$ADMIN_DIR/output" {} +
        fi
        rm -rf "$nested"
    fi
}

place_extraction() {
    local candidate=""
    if [ -d "$EXTRACT/admin-boundary" ]; then
        candidate="$EXTRACT/admin-boundary"
    elif [ -d "$EXTRACT/input" ] || [ -d "$EXTRACT/output" ]; then
        candidate="$EXTRACT"
    else
        candidate="$(find "$EXTRACT" -mindepth 1 -maxdepth 1 -type d | head -n 1 || true)"
    fi

    rm -rf "$ADMIN_DIR"
    mkdir -p "$DATA_DIR"
    if [ -z "$candidate" ]; then
        echo "ERROR: could not detect admin-boundary layout under $EXTRACT"
        return 1
    fi
    if [ "$candidate" = "$EXTRACT" ]; then
        mkdir -p "$ADMIN_DIR"
        find "$candidate" -mindepth 1 -maxdepth 1 -exec mv -t "$ADMIN_DIR" {} +
    else
        mv "$candidate" "$ADMIN_DIR"
    fi
    normalize_nested_layout
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

ensure_django_env() {
    if [ ! -f "$APP_ENV_FILE" ]; then
        if [ ! -f "$ENV_TEMPLATE" ]; then
            echo "ERROR: Django env template missing at $ENV_TEMPLATE"
            return 1
        fi
        echo "Creating ${APP_ENV_FILE} from Docker template..."
        mkdir -p "$(dirname "$APP_ENV_FILE")"
        cp "$ENV_TEMPLATE" "$APP_ENV_FILE"
    fi
    upsert_env "$APP_ENV_FILE" "DATA_DIR" "$DATA_DIR"
    upsert_env "$APP_ENV_FILE" "EXCEL_DIR" "$DATA_DIR/excel_files"
    upsert_env "$APP_ENV_FILE" "S3_ACCESS_KEY" "${S3_ACCESS_KEY:-}"
    upsert_env "$APP_ENV_FILE" "S3_SECRET_KEY" "${S3_SECRET_KEY:-}"
    upsert_env "$APP_ENV_FILE" "S3_REGION" "${S3_REGION:-}"
    upsert_env "$APP_ENV_FILE" "S3_BUCKET" "${S3_BUCKET:-}"
}

download_admin_boundary() {
    mkdir -p "$DATA_DIR"

    if [ "${SKIP_ADMIN_BOUNDARY_DOWNLOAD:-0}" = "1" ]; then
        echo "Skipping admin-boundary download (SKIP_ADMIN_BOUNDARY_DOWNLOAD=1)."
        return 0
    fi

    if [ "${FORCE_DATA_DOWNLOAD:-0}" != "1" ] && admin_boundary_ready; then
        echo "Admin-boundary data already present at $ADMIN_DIR"
        return 0
    fi

    if [ "${FORCE_DATA_DOWNLOAD:-0}" != "1" ]; then
        normalize_nested_layout || true
        if admin_boundary_ready; then
            echo "Admin-boundary data already present at $ADMIN_DIR"
            return 0
        fi
    fi

    echo "Downloading admin-boundary dataset inside Docker (~8GB, first run only)..."
    python -m pip install --quiet gdown
    rm -rf "$EXTRACT" "$ARCHIVE"
    mkdir -p "$EXTRACT"
    gdown "$FILE_ID" -O "$ARCHIVE"
    7z x "$ARCHIVE" -o"$EXTRACT"
    rm -f "$ARCHIVE"
    place_extraction
    rm -rf "$EXTRACT"

    if ! admin_boundary_ready; then
        echo "ERROR: download finished but $ADMIN_DIR/input/soi_tehsil.geojson is missing."
        return 1
    fi

    echo "Admin-boundary data ready at $ADMIN_DIR"
}

run_layer_setup() {
    local command="$1"
    shift
    echo "Running: python manage.py $command --skip-checks $*"
    python manage.py "$command" --skip-checks "$@"
}

download_local_compute_layers() {
    if [ "${SKIP_BASE_LAYER_DOWNLOAD:-0}" = "1" ] || [ "${SKIP_LAYER_SETUP:-0}" = "1" ]; then
        echo "Skipping local compute base-layer downloads."
        return 0
    fi

    if [ ! -f "$BACKEND_DIR/manage.py" ]; then
        echo "ERROR: manage.py not found at $BACKEND_DIR/manage.py"
        return 1
    fi
    if [ ! -f "$BACKEND_DIR/computing/management/commands/local_compute_layer_setup.py" ]; then
        echo "ERROR: local_compute_layer_setup command is missing from the checkout."
        return 1
    fi

    ensure_django_env
    mkdir -p "$DATA_DIR/base_layers" "$DATA_DIR/excel_files"
    cd "$BACKEND_DIR"
    export DATA_DIR

    echo "Downloading local compute layers into $DATA_DIR ..."
    run_layer_setup local_compute_layer_setup terrain mws lulc_v3
    run_layer_setup local_compute_layer_setup static_layers
    run_layer_setup local_compute_layer_setup tehsil_level
    run_layer_setup local_compute_layer_setup --ensure-soi-tehsil
    echo "Local compute layers ready under $DATA_DIR"
}

download_tehsil_watersheds() {
    if [ "${SKIP_TEHSIL_WATERSHEDS:-0}" = "1" ] || [ "${SKIP_LAYER_SETUP:-0}" = "1" ]; then
        echo "Skipping GeoServer tehsil watershed setup."
        return 0
    fi

    ensure_django_env
    mkdir -p "$DATA_DIR/base_layers/tehsil_watersheds"
    cd "$BACKEND_DIR"
    export DATA_DIR

    echo "Downloading active tehsil watershed layers from GeoServer into $DATA_DIR ..."
    echo "This path does not generate or clip watersheds from the local pan-India MWS file."
    if ! python manage.py local_compute_layer_setup --ensure-tehsil-watersheds --geoserver --skip-checks; then
        echo "WARNING: tehsil watershed download from GeoServer failed."
        echo "Local GeoServer may not have mws layers yet; files that already exist were kept."
        return 0
    fi
}

mkdir -p "$DATA_DIR"

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    if [ "${DOWNLOAD_TEHSIL_WATERSHEDS_ONLY:-0}" = "1" ]; then
        download_tehsil_watersheds
        exit 0
    fi

    download_admin_boundary
    download_local_compute_layers
fi
