#!/usr/bin/env bash
# Create every GeoServer workspace from install.sh and upload bundled SLD styles.
set -euo pipefail

GEOSERVER_URL="${GEOSERVER_URL:-http://geoserver:8080/geoserver}"
GEOSERVER_URL="${GEOSERVER_URL%/}"
GEOSERVER_USERNAME="${GEOSERVER_USERNAME:-admin}"
GEOSERVER_PASSWORD="${GEOSERVER_PASSWORD:-geoserver}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACES_FILE="${WORKSPACES_FILE:-${SCRIPT_DIR}/workspaces.txt}"
STYLES_DIR="${STYLES_DIR:-/app/installation/geoserver/styles}"
MARKER="${DATA_DIR:-/var/tmp/core-stack-data}/.geoserver_configured"
TIMEOUT="${GEOSERVER_WAIT_TIMEOUT:-180}"

wait_for_rest() {
    local elapsed=0
    echo "Waiting for GeoServer REST at ${GEOSERVER_URL} (up to ${TIMEOUT}s)..."
    while [ "$elapsed" -lt "$TIMEOUT" ]; do
        local code
        code="$(curl -s -o /dev/null -w '%{http_code}' \
            -u "${GEOSERVER_USERNAME}:${GEOSERVER_PASSWORD}" \
            "${GEOSERVER_URL}/rest/workspaces.json" || true)"
        if [ "$code" = "200" ]; then
            echo "GeoServer REST is ready."
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    echo "ERROR: GeoServer REST did not become ready after ${TIMEOUT}s."
    return 1
}

ensure_workspace() {
    local workspace="$1"
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' \
        -u "${GEOSERVER_USERNAME}:${GEOSERVER_PASSWORD}" \
        "${GEOSERVER_URL}/rest/workspaces/${workspace}.json" || true)"
    if [ "$code" = "200" ]; then
        echo "Workspace '${workspace}' already exists."
        return 0
    fi
    echo "Creating workspace '${workspace}'..."
    code="$(curl -s -o /dev/null -w '%{http_code}' \
        -u "${GEOSERVER_USERNAME}:${GEOSERVER_PASSWORD}" \
        -H "Content-Type: application/json" \
        -X POST \
        -d "{\"workspace\": {\"name\": \"${workspace}\"}}" \
        "${GEOSERVER_URL}/rest/workspaces" || true)"
    if [ "$code" = "201" ] || [ "$code" = "200" ]; then
        echo "Workspace '${workspace}' created."
        return 0
    fi
    echo "ERROR: failed to create workspace '${workspace}' (HTTP ${code})."
    return 1
}

if [ "${FORCE_GEOSERVER_INIT:-0}" != "1" ] && [ -f "$MARKER" ]; then
    echo "GeoServer already configured (${MARKER}). Skipping."
    exit 0
fi

wait_for_rest

failed=()
while IFS= read -r workspace || [ -n "${workspace:-}" ]; do
    workspace="$(echo "$workspace" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [ -n "$workspace" ] || continue
    case "$workspace" in
        \#*) continue ;;
    esac
    if ! ensure_workspace "$workspace"; then
        failed+=("$workspace")
    fi
done < "$WORKSPACES_FILE"

if [ "${#failed[@]}" -gt 0 ]; then
    echo "ERROR: failed workspaces: ${failed[*]}"
    exit 1
fi

if [ -f /app/installation/geoserver_style_bundle.py ]; then
    echo "Syncing bundled GeoServer styles from ${STYLES_DIR}..."
    python /app/installation/geoserver_style_bundle.py sync \
        --url "$GEOSERVER_URL" \
        --username "$GEOSERVER_USERNAME" \
        --password "$GEOSERVER_PASSWORD" \
        --styles-dir "$STYLES_DIR"
fi

mkdir -p "$(dirname "$MARKER")"
date -u +"%Y-%m-%dT%H:%M:%SZ" > "$MARKER"
echo "GeoServer workspaces and styles are ready."
