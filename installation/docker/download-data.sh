#!/usr/bin/env bash
# Download admin-boundary data inside the container (same source as installation/install.sh).
set -euo pipefail

DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
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

mkdir -p "$DATA_DIR"

if [ "${FORCE_DATA_DOWNLOAD:-0}" != "1" ] && admin_boundary_ready; then
    echo "Admin-boundary data already present at $ADMIN_DIR"
    exit 0
fi

if [ "${FORCE_DATA_DOWNLOAD:-0}" != "1" ]; then
    normalize_nested_layout || true
    if admin_boundary_ready; then
        echo "Admin-boundary data already present at $ADMIN_DIR"
        exit 0
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
    exit 1
fi

echo "Admin-boundary data ready at $ADMIN_DIR"
