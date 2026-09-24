#!/usr/bin/env bash
# Run a command as the owner of the mounted checkout instead of root, so
# everything it writes to the host (data/, migrations, static/, logs/, tmp/)
# stays usable from the host shell without sudo, and later image builds can
# read the checkout. Containers start as root; this is the last step before
# the real work. Nothing changes when the checkout is owned by root.
set -euo pipefail

BACKEND_DIR="${BACKEND_DIR:-/app}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"

owner_uid="$(stat -c '%u' "$BACKEND_DIR")"
owner_gid="$(stat -c '%g' "$BACKEND_DIR")"

if [ "$(id -u)" != "0" ] || [ "$owner_uid" = "0" ]; then
    exec "$@"
fi
if ! command -v setpriv >/dev/null 2>&1; then
    echo "WARNING: setpriv not found; running as root, host files will be root-owned." >&2
    exec "$@"
fi

# Directories the backend writes to. Docker creates missing bind-mount
# sources as root, so hand them over before dropping privileges.
writable_dirs=(
    "$DATA_DIR"
    "$BACKEND_DIR/logs"
    "$BACKEND_DIR/tmp"
    "$BACKEND_DIR/static"
    "$BACKEND_DIR/bot_interface/whatsapp_media"
)
mkdir -p "${writable_dirs[@]}"
chown "$owner_uid:$owner_gid" "${writable_dirs[@]}"

# The owner has no /etc/passwd entry in the image, so give libraries that
# look up a home directory or user name something usable.
runtime_home=/tmp/corestack-home
mkdir -p "$runtime_home"
chown "$owner_uid:$owner_gid" "$runtime_home"

exec setpriv --reuid="$owner_uid" --regid="$owner_gid" --clear-groups \
    env HOME="$runtime_home" USER=corestack LOGNAME=corestack "$@"
