#!/usr/bin/env bash
# Mirror installation/install.sh: env, dirs, migrations, seed, superuser, then run Django.
set -euo pipefail

BACKEND_DIR="${BACKEND_DIR:-/app}"
APP_ENV_FILE="${APP_ENV_FILE:-$BACKEND_DIR/nrm_app/.env}"
ENV_TEMPLATE="${ENV_TEMPLATE:-$BACKEND_DIR/installation/docker/env.template}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
SEED_FILE="${SEED_FILE:-$BACKEND_DIR/installation/seed/seed_data.json}"

cd "$BACKEND_DIR"

wait_for_tcp() {
    local host="$1"
    local port="$2"
    local name="$3"
    local timeout="${4:-120}"
    local elapsed=0
    echo "Waiting for ${name} at ${host}:${port}..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if python - "$host" "$port" <<'PY'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
sock = socket.socket()
sock.settimeout(2)
try:
    sock.connect((host, port))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
        then
            echo "${name} is ready."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "ERROR: ${name} did not become ready after ${timeout}s."
    return 1
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

apply_geoserver_and_gee_env() {
    local runtime="${DATA_DIR}/.gee_runtime.env"
    if [ -f "$runtime" ]; then
        set -a
        # shellcheck disable=SC1090
        . "$runtime"
        set +a
    fi
    upsert_env "$APP_ENV_FILE" "GEOSERVER_URL" "${GEOSERVER_URL:-}"
    upsert_env "$APP_ENV_FILE" "GEOSERVER_USERNAME" "${GEOSERVER_USERNAME:-}"
    upsert_env "$APP_ENV_FILE" "GEOSERVER_PASSWORD" "${GEOSERVER_PASSWORD:-}"
    upsert_env "$APP_ENV_FILE" "GEE_STORAGE_PROJECT" "${GEE_STORAGE_PROJECT:-}"
    upsert_env "$APP_ENV_FILE" "GEE_STORAGE_PROJECT_HELPER" "${GEE_STORAGE_PROJECT_HELPER:-}"
    upsert_env "$APP_ENV_FILE" "GCS_BUCKET_NAME" "${GCS_BUCKET_NAME:-}"
    upsert_env "$APP_ENV_FILE" "GEE_SERVICE_ACCOUNT_KEY_PATH" "${GEE_SERVICE_ACCOUNT_KEY_PATH:-}"
    upsert_env "$APP_ENV_FILE" "GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH" "${GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH:-}"
    export GEOSERVER_URL="${GEOSERVER_URL:-}"
    export GEE_STORAGE_PROJECT="${GEE_STORAGE_PROJECT:-}"
    export GEE_STORAGE_PROJECT_HELPER="${GEE_STORAGE_PROJECT_HELPER:-}"
    export GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-}"
}

ensure_env_file() {
    if [ -f "$APP_ENV_FILE" ]; then
        echo "Using existing ${APP_ENV_FILE}"
        return 0
    fi
    echo "Creating ${APP_ENV_FILE} from Docker template..."
    mkdir -p "$(dirname "$APP_ENV_FILE")"
    cp "$ENV_TEMPLATE" "$APP_ENV_FILE"
    if ! grep -q '^FERNET_KEY=.\+' "$APP_ENV_FILE"; then
        local key
        key="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
        if grep -q '^FERNET_KEY=' "$APP_ENV_FILE"; then
            sed -i "s|^FERNET_KEY=.*|FERNET_KEY=${key}|" "$APP_ENV_FILE"
        else
            echo "FERNET_KEY=${key}" >> "$APP_ENV_FILE"
        fi
    fi
    if grep -q '^SECRET_KEY=change-me-in-docker$' "$APP_ENV_FILE"; then
        local secret
        secret="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"
        sed -i "s|^SECRET_KEY=change-me-in-docker$|SECRET_KEY=${secret}|" "$APP_ENV_FILE"
    fi
}

ensure_dirs() {
    mkdir -p \
        "$BACKEND_DIR/logs" \
        "$BACKEND_DIR/tmp" \
        "$BACKEND_DIR/data/fc_to_shape" \
        "$BACKEND_DIR/data/gee_confs" \
        "$BACKEND_DIR/bot_interface/whatsapp_media" \
        "$DATA_DIR" \
        "$DATA_DIR/activated_locations" \
        "$DATA_DIR/excel_files" \
        "$DATA_DIR/admin-boundary" \
        "$DATA_DIR/gee_confs"
    touch "$BACKEND_DIR/logs/app.log" "$BACKEND_DIR/logs/nrm_app.log"
}

maybe_download_admin_boundary() {
    if [ "${DOWNLOAD_ADMIN_BOUNDARY:-1}" != "1" ]; then
        echo "Skipping admin-boundary download (DOWNLOAD_ADMIN_BOUNDARY=${DOWNLOAD_ADMIN_BOUNDARY})."
        return 0
    fi
    if [ -f /opt/corestack-scripts/download-data.sh ]; then
        bash /opt/corestack-scripts/download-data.sh
    elif [ -f /usr/local/bin/download-data.sh ]; then
        bash /usr/local/bin/download-data.sh
    else
        bash "$BACKEND_DIR/installation/docker/download-data.sh"
    fi
}

run_database() {
    echo "Collecting static files..."
    python manage.py collectstatic --noinput --clear --skip-checks

    echo "Building Django database (makemigrations + migrate)..."
    python manage.py makemigrations --skip-checks
    python manage.py migrate --fake-initial --skip-checks

    if [ -f "$SEED_FILE" ]; then
        echo "Loading seed data..."
        python manage.py loaddata --skip-checks "$SEED_FILE" || true
        python manage.py seed_default_plantation --skip-checks || true
    fi

    echo "Ensuring installer superuser..."
    python manage.py shell <<'PY'
import random
from django.contrib.auth import get_user_model

User = get_user_model()
installer_user = (
    User.objects.filter(username__startswith="test_user_", is_superuser=True)
    .order_by("id")
    .first()
)
if installer_user is None:
    while True:
        username = f"test_user_{random.randint(0, 9999):04d}"
        if not User.objects.filter(username=username).exists():
            break
    installer_user = User.objects.create_superuser(
        username=username,
        email="",
        password="test_change_me",
    )
    print(f"created|{installer_user.username}")
else:
    installer_user.set_password("test_change_me")
    installer_user.is_active = True
    installer_user.is_staff = True
    installer_user.is_superuser = True
    installer_user.save()
    print(f"updated|{installer_user.username}")
PY
}

wait_for_tcp "${DB_HOST:-postgres}" "${DB_PORT:-5432}" "PostgreSQL"
if [ -n "${RABBITMQ_HOST:-}" ] && [ "${SKIP_RABBITMQ:-1}" != "1" ]; then
    wait_for_tcp "$RABBITMQ_HOST" "5672" "RabbitMQ"
fi
ensure_env_file
apply_geoserver_and_gee_env
ensure_dirs

if [ "${SKIP_DB_SETUP:-0}" != "1" ]; then
    maybe_download_admin_boundary
    run_database
    echo "Django is ready. Superuser password is test_change_me"
    echo "Mount GEE JSON under /app/data/gee_confs (./gee_confs on the host)."
fi

if [ "$#" -gt 0 ]; then
    exec "$@"
fi
exec python manage.py runserver 0.0.0.0:8000 --skip-checks
