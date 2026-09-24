#!/usr/bin/env bash
# Generate installation-local migrations, apply them, and perform other
# idempotent database/static setup.
set -euo pipefail

BACKEND_DIR="${BACKEND_DIR:-/app}"
DATA_DIR="${DATA_DIR:-/var/tmp/core-stack-data}"
SEED_FILE="${SEED_FILE:-$BACKEND_DIR/installation/seed/seed_data.json}"
SEED_MARKER="$DATA_DIR/.database_seeded"

cd "$BACKEND_DIR"

ensure_local_migration_packages() {
    find . -maxdepth 2 -name "apps.py" -type f -print0 |
        while IFS= read -r -d '' apps_file; do
            migration_dir="$(dirname "$apps_file")/migrations"
            mkdir -p "$migration_dir"
            touch "$migration_dir/__init__.py"
        done
}

reset_local_migrations() {
    echo "Resetting installation-local Django migrations..."
    find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
    find . -path "*/migrations/*.pyc" -delete
}

if [ "${RESET_LOCAL_MIGRATIONS:-0}" = "1" ]; then
    reset_local_migrations
fi

ensure_local_migration_packages

echo "Generating installation-local Django migrations..."
python manage.py makemigrations --skip-checks

echo "Reviewing the Django migration plan..."
python manage.py migrate --plan --skip-checks

echo "Applying Django migrations (including restored initial tables)..."
python manage.py migrate --fake-initial --noinput --skip-checks

echo "Collecting static files..."
python manage.py collectstatic --noinput --clear --skip-checks

if [ "${SKIP_SEED_DATA:-0}" = "1" ]; then
    echo "Skipping seed data (SKIP_SEED_DATA=1)."
elif [ "${FORCE_SEED_DATA:-0}" = "1" ] || [ ! -f "$SEED_MARKER" ]; then
    if [ -f "$SEED_FILE" ]; then
        echo "Loading seed data..."
        python manage.py loaddata --skip-checks "$SEED_FILE"
        python manage.py seed_default_plantation --skip-checks
    fi
    date -u +"%Y-%m-%dT%H:%M:%SZ" > "$SEED_MARKER"
else
    echo "Seed data already loaded; use FORCE_SEED_DATA=1 to load it again."
fi

if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ]; then
    if [ -z "${DJANGO_SUPERUSER_PASSWORD:-}" ]; then
        echo "ERROR: DJANGO_SUPERUSER_PASSWORD is required when DJANGO_SUPERUSER_USERNAME is set."
        exit 1
    fi
    echo "Ensuring the requested Django superuser exists..."
    python manage.py shell <<'PY'
import os
from django.contrib.auth import get_user_model

User = get_user_model()
username = os.environ["DJANGO_SUPERUSER_USERNAME"]
defaults = {
    "email": os.environ.get("DJANGO_SUPERUSER_EMAIL", ""),
    "is_active": True,
    "is_staff": True,
    "is_superuser": True,
}
user, created = User.objects.get_or_create(username=username, defaults=defaults)
if created:
    user.set_password(os.environ["DJANGO_SUPERUSER_PASSWORD"])
    user.save(update_fields=["password"])
    print(f"Created superuser {username}.")
else:
    print(f"Superuser {username} already exists; password was not changed.")
PY
fi

echo "Database and static setup completed."
