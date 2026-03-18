#!/bin/bash
# CoRE Stack backend installer.
#
# Rerun behavior:
# - The installer checks whether expensive steps appear to be already completed.
# - If a completed step is detected, the script asks whether you want to redo it.
# - Reply `y` to rerun it, `n` or press Enter to skip it.
# - If you paste a file path at a redo prompt for a file-driven step such as GEE setup,
#   the installer treats that as new input instead of discarding it.
# - If there is no reply within 30 seconds, the installer proceeds with that step anyway.
# - Completion is tracked with a mix of live checks and local state markers in
#   `.installation_state/` so repeat runs stay interactive instead of blindly redoing work.

set -e

# === CONFIGURATION ===
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="corestackenv"
INSTALL_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONDA_ENV_YAML="$INSTALL_SCRIPT_DIR/environment.yml"
BACKEND_DIR="$(cd "$INSTALL_SCRIPT_DIR/.." && pwd)"
INSTALL_INVOCATION_DIR="$PWD"
POSTGRES_USER="corestack_admin"
POSTGRES_DB="corestack_db"
POSTGRES_PASSWORD="corestack@123"
SHELL_RC="$HOME/.bashrc"  # Change to .zshrc if using zsh
INSTALL_STATE_DIR="$BACKEND_DIR/.installation_state"
STEP_PROMPT_TIMEOUT=30
LAST_PROMPT_RESPONSE=""

# === ENV FILE CONFIGURATION ===
# Map script config variables to .env variable names
# These will override blank values in the generated .env
ENV_DB_NAME="$POSTGRES_DB"
ENV_DB_USER="$POSTGRES_USER"
ENV_DB_PASSWORD="$POSTGRES_PASSWORD"
ENV_DEPLOYMENT_DIR="$BACKEND_DIR"
ENV_TMP_LOCATION="$BACKEND_DIR/tmp"
APP_ENV_FILE="$BACKEND_DIR/nrm_app/.env"
LEGACY_ROOT_ENV_FILE="$BACKEND_DIR/.env"

# === FUNCTIONS ===

function step_marker_path() {
    local step_name="$1"
    echo "$INSTALL_STATE_DIR/${step_name}.done"
}

function mark_step_complete() {
    local step_name="$1"
    mkdir -p "$INSTALL_STATE_DIR"
    date -u +"%Y-%m-%dT%H:%M:%SZ" > "$(step_marker_path "$step_name")"
}

function clear_step_marker() {
    local step_name="$1"
    rm -f "$(step_marker_path "$step_name")"
}

function is_step_marked_complete() {
    local step_name="$1"
    [ -f "$(step_marker_path "$step_name")" ]
}

function prompt_redo_completed_step() {
    local step_label="$1"
    local allow_path_input="${2:-0}"
    local answer

    LAST_PROMPT_RESPONSE=""
    echo ""
    echo "Step already looks complete: $step_label"
    echo "Redo it? [y/N]"
    echo "If no response is received in ${STEP_PROMPT_TIMEOUT} seconds, this step will run again."

    if [ ! -t 0 ]; then
        echo "No interactive terminal detected. Re-running: $step_label"
        return 0
    fi

    if read -r -t "$STEP_PROMPT_TIMEOUT" -p "> " answer; then
        LAST_PROMPT_RESPONSE="$answer"
        case "${answer,,}" in
            y|yes)
                return 0
                ;;
            n|no|"")
                return 1
                ;;
            *)
                if [ "$allow_path_input" = "1" ] && looks_like_user_path_input "$answer"; then
                    return 0
                fi
                echo "Unrecognized response. Skipping: $step_label"
                return 1
                ;;
        esac
    fi

    echo ""
    echo "Timed out after ${STEP_PROMPT_TIMEOUT} seconds. Re-running: $step_label"
    return 0
}

function looks_like_user_path_input() {
    local candidate="$1"

    candidate=$(printf '%s' "$candidate" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    if [ -z "$candidate" ]; then
        return 1
    fi

    if [[ "$candidate" == \"*\" && "$candidate" == *\" ]]; then
        candidate="${candidate:1:${#candidate}-2}"
    elif [[ "$candidate" == \'*\' && "$candidate" == *\' ]]; then
        candidate="${candidate:1:${#candidate}-2}"
    fi

    [[ "$candidate" =~ ^[A-Za-z]:[\\/].* ]] && return 0
    [[ "$candidate" == *"/"* ]] && return 0
    [[ "$candidate" == *"\\"* ]] && return 0
    [[ "$candidate" == ./* ]] && return 0
    [[ "$candidate" == ../* ]] && return 0
    [[ "$candidate" == "~"* ]] && return 0
    [[ "$candidate" == *.json ]] && return 0

    return 1
}

function drain_tty_input() {
    if [ ! -t 0 ]; then
        return
    fi

    while IFS= read -r -t 0.05 -n 1 _discarded_char; do
        :
    done
}

function should_run_step() {
    local step_key="$1"
    local step_label="$2"
    local detector="${3:-}"

    if { [ -n "$detector" ] && "$detector"; } || is_step_marked_complete "$step_key"; then
        if prompt_redo_completed_step "$step_label"; then
            clear_step_marker "$step_key"
            return 0
        fi
        echo "Skipping: $step_label"
        return 1
    fi

    return 0
}

function conda_env_exists() {
    ensure_conda
    conda env list | sed 's/^[* ]*//' | awk '{print $1}' | grep -qx "$CONDA_ENV_NAME"
}

function env_files_exist() {
    [ -f "$APP_ENV_FILE" ]
}

function static_files_exist() {
    [ -d "$BACKEND_DIR/static" ] && find "$BACKEND_DIR/static" -mindepth 1 -print -quit 2>/dev/null | grep -q .
}

function unzip_installed() {
    command -v unzip &> /dev/null
}

function django_migrations_applied() {
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    cd "$BACKEND_DIR"
    local migration_count
    migration_count=$(python manage.py shell <<'PY' 2>/dev/null | tail -n 1
from django.db import connection

count = 0
try:
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM django_migrations")
    count = cursor.fetchone()[0] or 0
except Exception:
    count = 0

print(count)
PY
)
    [ "${migration_count:-0}" -gt 0 ]
}

function seed_data_loaded() {
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    cd "$BACKEND_DIR"
    local has_seed_data
    has_seed_data=$(python manage.py shell -c "from geoadmin.models import StateSOI; print(1 if StateSOI.objects.exists() else 0)" 2>/dev/null | tail -n 1)
    [ "${has_seed_data:-0}" = "1" ]
}

function superuser_exists() {
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    cd "$BACKEND_DIR"
    local has_superuser
    has_superuser=$(python manage.py shell -c "from django.contrib.auth import get_user_model; User = get_user_model(); print(1 if User.objects.filter(is_superuser=True).exists() else 0)" 2>/dev/null | tail -n 1)
    [ "${has_superuser:-0}" = "1" ]
}

function gee_configuration_present() {
    local ENV_FILE="$APP_ENV_FILE"
    [ -f "$ENV_FILE" ] && grep -Eq '^GEE_DEFAULT_ACCOUNT_ID="?[0-9]+' "$ENV_FILE"
}

function directory_has_contents() {
    local directory_path="$1"
    [ -d "$directory_path" ] && [ "$(ls -A "$directory_path" 2>/dev/null)" ]
}

function admin_boundary_data_present() {
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    directory_has_contents "$admin_boundary_dir/input"
}

function nested_admin_boundary_data_present() {
    local nested_admin_boundary_dir="$BACKEND_DIR/data/admin-boundary/admin-boundary"
    directory_has_contents "$nested_admin_boundary_dir/input"
}

function normalize_existing_admin_boundary_data() {
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    local nested_admin_boundary_dir="$admin_boundary_dir/admin-boundary"

    if admin_boundary_data_present; then
        mark_step_complete "admin_boundary_data"
        return 0
    fi

    if ! nested_admin_boundary_data_present; then
        return 1
    fi

    echo "Found admin boundary data in nested extracted layout. Normalizing it to $admin_boundary_dir ..."
    mkdir -p "$admin_boundary_dir/input" "$admin_boundary_dir/output"

    if [ -d "$nested_admin_boundary_dir/input" ] && [ ! "$(ls -A "$admin_boundary_dir/input" 2>/dev/null)" ]; then
        mv "$nested_admin_boundary_dir/input"/* "$admin_boundary_dir/input/"
    fi

    if [ -d "$nested_admin_boundary_dir/output" ] && [ ! "$(ls -A "$admin_boundary_dir/output" 2>/dev/null)" ]; then
        mv "$nested_admin_boundary_dir/output"/* "$admin_boundary_dir/output/" 2>/dev/null || true
    fi

    rmdir "$nested_admin_boundary_dir/output" 2>/dev/null || true
    rmdir "$nested_admin_boundary_dir/input" 2>/dev/null || true
    rmdir "$nested_admin_boundary_dir" 2>/dev/null || true

    if admin_boundary_data_present; then
        echo "Admin boundary data is ready at $admin_boundary_dir"
        mark_step_complete "admin_boundary_data"
        return 0
    fi

    echo "Admin boundary data was detected, but automatic normalization did not finish cleanly."
    return 1
}

function install_miniconda() {
    if command -v conda &> /dev/null; then
        echo "Conda already available ($(conda --version)). Skipping Miniconda install."
        return
    fi
    if [ -d "$MINICONDA_DIR" ]; then
        echo "Miniconda found at $MINICONDA_DIR but not on PATH. Sourcing it..."
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
        return
    fi
    echo "Installing Miniconda..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    bash miniconda.sh -b -p "$MINICONDA_DIR"
    rm miniconda.sh
    echo "Miniconda installed."
    echo "Adding Conda to your shell profile ($SHELL_RC)..."
    {
        echo "# >>> conda initialize >>>"
        echo "source \"$MINICONDA_DIR/etc/profile.d/conda.sh\""
        echo "# <<< conda initialize <<<"
    } >> "$SHELL_RC"
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
}

function ensure_conda() {
    if ! command -v conda &> /dev/null; then
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    fi
    if ! command -v conda &> /dev/null; then
        echo "Conda still not found. Exiting."
        exit 1
    fi
}

function setup_conda_env() {
    ensure_conda
    echo "Setting up conda environment '$CONDA_ENV_NAME'..."
    conda env remove -n "$CONDA_ENV_NAME" -y || true
    conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"
    echo "Conda environment ready."
    mark_step_complete "conda_env"
}

function install_postgres() {
    if command -v psql &> /dev/null; then
        echo "PostgreSQL already installed ($(psql --version)). Skipping install."
    else
        echo "Installing PostgreSQL..."
        sudo apt-get update
        sudo apt-get install -y postgresql postgresql-contrib postgis libpq-dev
    fi
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
    echo "Setting up PostgreSQL user/database..."
    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname = '$POSTGRES_USER'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres psql -c "ALTER USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"
    sudo -u postgres psql -c "ALTER USER $POSTGRES_USER WITH SUPERUSER;"
    echo "PostgreSQL ready."
}

function install_rabbitmq() {
    if command -v rabbitmqctl &> /dev/null; then
        echo "RabbitMQ already installed. Skipping install."
    else
        echo "Installing RabbitMQ..."
        sudo apt-get install -y rabbitmq-server
    fi
    sudo systemctl start rabbitmq-server
    sudo systemctl enable rabbitmq-server
    echo "RabbitMQ ready."
}

function collect_static_files() {
    echo "Collecting static files..."
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    cd "$BACKEND_DIR"
    python manage.py collectstatic --noinput --skip-checks
    echo "Static files collected."
    mark_step_complete "collectstatic"
}

reset_django_migrations() {

    echo "Resetting Django migrations..."

    cd "$BACKEND_DIR"

    # Remove old migration files
    find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
    find . -path "*/migrations/*.pyc" -delete

    # Recreate migrations folders
    find . -maxdepth 2 -name "apps.py" -type f | while IFS= read -r f; do
        d=$(dirname "$f")
        mkdir -p "$d/migrations"
        touch "$d/migrations/__init__.py"
    done

    echo "Migrations cleaned."

}

run_django_migrations() {

    echo "Running Django migrations..."

    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    python manage.py makemigrations --skip-checks
    python manage.py migrate --plan --skip-checks
    python manage.py migrate --fake-initial --skip-checks
    mark_step_complete "django_migrations"

}

function generate_env_file() {

    echo "Generating .env file from settings.py..."

    local SETTINGS_FILE="$BACKEND_DIR/nrm_app/settings.py"
    local ENV_FILE="$APP_ENV_FILE"

    if [ ! -f "$SETTINGS_FILE" ]; then
        echo "ERROR: settings.py not found at $SETTINGS_FILE"
        return 1
    fi

    # Extract env("VAR_NAME") patterns
    local env_vars
    env_vars=$(grep -oE 'env\.[a-z]*\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$SETTINGS_FILE" 2>/dev/null | \
               sed -E 's/env\.[a-z]*\s*\(\s*"([^"]+)"/\1/' | \
               sort -u)

    # Extract env("VAR_NAME") simple calls
    local env_vars_simple
    env_vars_simple=$(grep -oE 'env\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$SETTINGS_FILE" 2>/dev/null | \
                      sed -E 's/env\s*\(\s*"([^"]+)"/\1/' | \
                      sort -u)

    # Combine variables
    local all_vars
    all_vars=$(echo -e "${env_vars}\n${env_vars_simple}" | sort -u | grep -v '^$')

    if [ ! -f "$ENV_FILE" ] && [ -f "$LEGACY_ROOT_ENV_FILE" ]; then
        echo "Migrating existing root .env to $APP_ENV_FILE ..."
        mkdir -p "$(dirname "$ENV_FILE")"
        cp "$LEGACY_ROOT_ENV_FILE" "$ENV_FILE"
    fi

    # Create .env if it does not exist
    if [ ! -f "$ENV_FILE" ]; then

        echo "Creating new .env file..."

        echo "# Auto-generated .env file" > "$ENV_FILE"
        echo "# Generated on $(date)" >> "$ENV_FILE"
        echo "" >> "$ENV_FILE"

        # Required Django variables
        echo "SECRET_KEY=$(openssl rand -base64 32)" >> "$ENV_FILE"
        echo "DEBUG=True" >> "$ENV_FILE"
        echo "" >> "$ENV_FILE"

    else
        echo "Existing .env file found. Updating missing variables..."
    fi

    # Read existing variables
    local existing_vars
    existing_vars=$(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$ENV_FILE" | cut -d'=' -f1 | sort -u)

while IFS= read -r var_name; do
    if [ -n "$var_name" ] && [ "$var_name" != "SECRET_KEY" ] && [ "$var_name" != "DEBUG" ]; then
        if ! echo "$existing_vars" | grep -q "^${var_name}$"; then

            if [ "$var_name" = "WHATSAPP_MEDIA_PATH" ]; then
                echo "WHATSAPP_MEDIA_PATH=$BACKEND_DIR/bot_interface/whatsapp_media" >> "$ENV_FILE"
                mkdir -p "$BACKEND_DIR/bot_interface/whatsapp_media"

            elif [ "$var_name" = "EXCEL_DIR" ]; then
                echo "EXCEL_DIR=$BACKEND_DIR/data/excel_files" >> "$ENV_FILE"
                mkdir -p "$BACKEND_DIR/excel_files"

            else
                echo "${var_name}=\"\"" >> "$ENV_FILE"
            fi

        fi
    fi
done <<< "$all_vars"
if ! grep -q "^EXCEL_DIR=" "$ENV_FILE"; then
    echo "EXCEL_DIR=$BACKEND_DIR/data/excel_files" >> "$ENV_FILE"
    mkdir -p "$BACKEND_DIR/data/excel_files"
fi
    # Apply database overrides
    apply_env_overrides "$ENV_FILE"

    # Fix permissions
    chown $USER:$USER "$ENV_FILE"
    chmod 640 "$ENV_FILE"

    echo "Total variables in .env: $(grep -c '^[A-Za-z_]' "$ENV_FILE")"
    echo ".env ready at $ENV_FILE"

    # Generate FERNET_KEY only once so reruns don't invalidate existing encrypted data.
    if grep -q '^FERNET_KEY=' "$ENV_FILE"; then
        echo "FERNET_KEY already present. Keeping the existing value."
    else
        local FERNET_KEY
        FERNET_KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64 | tr +/ -_)
        echo "FERNET_KEY=$FERNET_KEY" >> "$ENV_FILE"
        echo "FERNET_KEY generated and added to .env"
    fi

    echo ".env ready at $APP_ENV_FILE"

    mark_step_complete "env_file"

}

function apply_env_overrides() {
    local ENV_FILE="$1"
    
    # Override with values from script configuration
    # DB settings mapping - only override if value is blank or variable was just added
    if [ -n "$ENV_DB_NAME" ]; then
        sed -i "s|^DB_NAME=\"\"|DB_NAME=\"$ENV_DB_NAME\"|" "$ENV_FILE"
    fi
    
    if [ -n "$ENV_DB_USER" ]; then
        sed -i "s|^DB_USER=\"\"|DB_USER=\"$ENV_DB_USER\"|" "$ENV_FILE"
    fi
    
    if [ -n "$ENV_DB_PASSWORD" ]; then
        sed -i "s|^DB_PASSWORD=\"\"|DB_PASSWORD=\"$ENV_DB_PASSWORD\"|" "$ENV_FILE"
    fi

    if [ -n "$ENV_DEPLOYMENT_DIR" ]; then
        sed -i "s|^DEPLOYMENT_DIR=\"\"|DEPLOYMENT_DIR=\"$ENV_DEPLOYMENT_DIR\"|" "$ENV_FILE"
    fi

    if [ -n "$ENV_TMP_LOCATION" ]; then
        sed -i "s|^TMP_LOCATION=\"\"|TMP_LOCATION=\"$ENV_TMP_LOCATION\"|" "$ENV_FILE"
    fi
}

function set_env_value() {
    local ENV_FILE="$1"
    local KEY="$2"
    local VALUE="$3"

    if grep -q "^${KEY}=" "$ENV_FILE"; then
        sed -i "s|^${KEY}=.*|${KEY}=\"$VALUE\"|" "$ENV_FILE"
    else
        echo "${KEY}=\"$VALUE\"" >> "$ENV_FILE"
    fi
}

function normalize_user_path() {
    local raw_path="$1"
    local normalized_path
    local drive_letter
    local remainder

    normalized_path=$(printf '%s' "$raw_path" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

    if [[ "$normalized_path" == \"*\" && "$normalized_path" == *\" ]]; then
        normalized_path="${normalized_path:1:${#normalized_path}-2}"
    elif [[ "$normalized_path" == \'*\' && "$normalized_path" == *\' ]]; then
        normalized_path="${normalized_path:1:${#normalized_path}-2}"
    fi

    normalized_path="${normalized_path//\\//}"

    if [[ "$normalized_path" == "~"* ]]; then
        normalized_path="${HOME}${normalized_path:1}"
    fi

    if [[ "$normalized_path" =~ ^([A-Za-z]):/(.*)$ ]]; then
        drive_letter="${BASH_REMATCH[1],,}"
        remainder="${BASH_REMATCH[2]}"
        normalized_path="/mnt/${drive_letter}/${remainder}"
    elif [[ "$normalized_path" != /* ]]; then
        normalized_path="$INSTALL_INVOCATION_DIR/$normalized_path"
    fi

    if command -v realpath >/dev/null 2>&1; then
        normalized_path=$(realpath -m "$normalized_path")
    fi

    printf '%s\n' "$normalized_path"
}

function auto_configure_gee_account_ids() {
    local ENV_FILE="$APP_ENV_FILE"

    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    local first_account_id
    local helper_account_id

    first_account_id=$(python manage.py shell -c "from gee_computing.models import GEEAccount; account = GEEAccount.objects.order_by('id').first(); print(account.id if account else '')" 2>/dev/null | tail -n 1)
    helper_account_id=$(python manage.py shell -c "from gee_computing.models import GEEAccount; account = GEEAccount.objects.exclude(helper_account=None).order_by('id').first(); print(account.helper_account_id if account and account.helper_account_id else '')" 2>/dev/null | tail -n 1)

    if [ -n "$first_account_id" ] && grep -q '^GEE_DEFAULT_ACCOUNT_ID=""' "$ENV_FILE"; then
        sed -i "s|^GEE_DEFAULT_ACCOUNT_ID=\"\"|GEE_DEFAULT_ACCOUNT_ID=\"$first_account_id\"|" "$ENV_FILE"
        echo "Auto-configured GEE_DEFAULT_ACCOUNT_ID=$first_account_id"
    fi

    if [ -n "$helper_account_id" ] && grep -q '^GEE_HELPER_ACCOUNT_ID=""' "$ENV_FILE"; then
        sed -i "s|^GEE_HELPER_ACCOUNT_ID=\"\"|GEE_HELPER_ACCOUNT_ID=\"$helper_account_id\"|" "$ENV_FILE"
        echo "Auto-configured GEE_HELPER_ACCOUNT_ID=$helper_account_id"
    fi

}

function configure_paths() {
    local GEE_JSON_PATH_INPUT="$1"
    local ENV_FILE="$APP_ENV_FILE"
    local ACCOUNT_NAME="${2:-local-gee-account}"
    local normalized_gee_json_path

    normalized_gee_json_path=$(normalize_user_path "$GEE_JSON_PATH_INPUT")

    if [ "$normalized_gee_json_path" != "$GEE_JSON_PATH_INPUT" ]; then
        echo "Resolved GEE credentials path to: $normalized_gee_json_path"
    fi

    GEE_JSON_PATH_INPUT="$normalized_gee_json_path"

    if [ ! -f "$GEE_JSON_PATH_INPUT" ]; then
        echo "GEE credentials file not found: $GEE_JSON_PATH_INPUT"
        return 1
    fi

    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    local import_result
    import_result=$(GEE_JSON_PATH="$GEE_JSON_PATH_INPUT" GEE_ACCOUNT_NAME="$ACCOUNT_NAME" PYTHONPATH="$BACKEND_DIR" python - <<'PY'
import os
import json

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

import django

django.setup()

from utilities.gee_utils import copy_gee_credentials_into_repo, upsert_gee_account_from_json

staged_credentials = copy_gee_credentials_into_repo(
    credentials_path=os.environ["GEE_JSON_PATH"],
)

account = upsert_gee_account_from_json(
    credentials_path=staged_credentials["absolute_path"],
    account_name=os.environ["GEE_ACCOUNT_NAME"],
)
print(
    json.dumps(
        {
            "account_id": account.id,
            "relative_path": staged_credentials["relative_path"],
            "absolute_path": staged_credentials["absolute_path"],
        }
    )
)
PY
)

    local account_id
    account_id=$(echo "$import_result" | tail -n 1 | python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("account_id",""))' 2>/dev/null | tr -d '[:space:]')
    local staged_relative_path
    staged_relative_path=$(echo "$import_result" | tail -n 1 | python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("relative_path",""))' 2>/dev/null | tr -d '\r')

    if [ -z "$account_id" ]; then
        echo "Unable to create or update the GEE account from the provided JSON."
        return 1
    fi
    if [ -z "$staged_relative_path" ]; then
        echo "Unable to determine the staged repo path for the GEE credentials."
        return 1
    fi

    set_env_value "$ENV_FILE" "GEE_DEFAULT_ACCOUNT_ID" "$account_id"
    set_env_value "$ENV_FILE" "GEE_HELPER_ACCOUNT_ID" "$account_id"
    set_env_value "$ENV_FILE" "GEE_SERVICE_ACCOUNT_KEY_PATH" "$staged_relative_path"
    set_env_value "$ENV_FILE" "GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH" "$staged_relative_path"
    POST_INSTALL_REQUIRE_GEE=1
    echo "Configured GEE account id=$account_id using staged credentials at $staged_relative_path"
    mark_step_complete "gee_configuration"
}

function optional_configure_gee_account() {
    local had_existing_gee_configuration=0
    local GEE_JSON_PATH_INPUT=""

    if gee_configuration_present || is_step_marked_complete "gee_configuration"; then
        had_existing_gee_configuration=1
        if ! prompt_redo_completed_step "Google Earth Engine configuration" 1; then
            if looks_like_user_path_input "$LAST_PROMPT_RESPONSE"; then
                GEE_JSON_PATH_INPUT="$LAST_PROMPT_RESPONSE"
                echo "Treating the pasted response as a GEE JSON path."
                clear_step_marker "gee_configuration"
            else
                POST_INSTALL_REQUIRE_GEE=1
                echo "Keeping the existing GEE configuration."
                return
            fi
        else
            clear_step_marker "gee_configuration"
            if looks_like_user_path_input "$LAST_PROMPT_RESPONSE"; then
                GEE_JSON_PATH_INPUT="$LAST_PROMPT_RESPONSE"
                echo "Treating the pasted response as a GEE JSON path."
            fi
        fi
    fi

    if [ -z "$GEE_JSON_PATH_INPUT" ]; then
        echo ""
        echo "Optional: configure Google Earth Engine now."
        echo "If your organization shared a service-account JSON, enter its full path below."
        echo "Windows paths like C:\\Users\\name\\Downloads\\file.json and relative paths like .\\file.json are accepted."
        echo "Common places: ~/Downloads, a mounted team drive, or the folder your admin shared with you."
        if [ ! -t 0 ]; then
            GEE_JSON_PATH_INPUT=""
        elif ! read -r -t "$STEP_PROMPT_TIMEOUT" -p "GEE JSON path [leave blank to skip]: " GEE_JSON_PATH_INPUT; then
            echo ""
            drain_tty_input
            if [ "$had_existing_gee_configuration" -eq 1 ]; then
                POST_INSTALL_REQUIRE_GEE=1
                echo "No GEE JSON path received in ${STEP_PROMPT_TIMEOUT} seconds. Keeping the existing GEE configuration."
                return
            fi
            echo "No GEE JSON path received in ${STEP_PROMPT_TIMEOUT} seconds. Skipping optional GEE setup."
            GEE_JSON_PATH_INPUT=""
        fi
    fi

    if [ -z "$GEE_JSON_PATH_INPUT" ]; then
        if [ "$had_existing_gee_configuration" -eq 1 ]; then
            POST_INSTALL_REQUIRE_GEE=1
            echo "Keeping the existing GEE configuration."
            return
        fi
        POST_INSTALL_REQUIRE_GEE=0
        echo "Skipping optional GEE setup for now."
        return
    fi

    if configure_paths "$GEE_JSON_PATH_INPUT"; then
        echo "GEE credentials imported. The final initialisation test will now prove a live GEE call too."
    else
        POST_INSTALL_REQUIRE_GEE=0
        echo "GEE setup did not complete. Continuing with the core initialisation test only."
    fi
}

function run_post_install_initialisation_check() {
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    echo ""
    echo "Running internal API initialisation test..."
    echo "This validation runs Django in-process, creates a JWT Bearer token automatically,"
    echo "and forces Celery eager mode for the checked task. You do not need runserver"
    echo "or a separate Celery worker for this installer-time verification."

    if [ "${POST_INSTALL_REQUIRE_GEE:-0}" -eq 1 ]; then
        INITIALISATION_ARGS=(--require-gee)
    else
        INITIALISATION_ARGS=()
    fi

    if python computing/misc/internal_api_initialisation_test.py "${INITIALISATION_ARGS[@]}"; then
        POST_INSTALL_INITIALISATION_FAILED=0
        echo "Internal API initialisation test passed."
    else
        POST_INSTALL_INITIALISATION_FAILED=1
        echo "Internal API initialisation test found issues. Review the output above before using the APIs."
    fi
}

create_django_superuser() {

    echo ""
    echo "Create Django superuser"
    
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    python manage.py createsuperuser --skip-checks
    mark_step_complete "superuser"

}

function ensure_dirs() {
    mkdir -p "$BACKEND_DIR/logs"
    touch "$BACKEND_DIR/logs/app.log" "$BACKEND_DIR/logs/nrm_app.log"
    mkdir -p "$BACKEND_DIR/data/activated_locations"
    mkdir -p "$BACKEND_DIR/tmp"
    mkdir -p "$INSTALL_STATE_DIR"
    echo "Required directories ready."
}

function load_seed_data() {
    local seed_file="$BACKEND_DIR/installation/seed/seed_data.json"
    if [ ! -f "$seed_file" ]; then
        echo "No seed data found at $seed_file. Skipping."
        return
    fi
    echo "Loading seed data..."
    cd "$BACKEND_DIR"
    conda run -n "$CONDA_ENV_NAME" python manage.py loaddata --skip-checks "$seed_file"
    echo "Seed data loaded."
    mark_step_complete "seed_data"
}

function download_admin_boundary_data() {
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    mkdir -p "$BACKEND_DIR/data"
    echo "Downloading admin boundary data (~8GB, this may take a while)..."
    rm -rf "$admin_boundary_dir"
    rm -f "$BACKEND_DIR/dataset.7z"
    pip install gdown
    sudo apt-get install -y p7zip-full
    local fileid="1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d"
    (
        cd "$BACKEND_DIR"
        gdown "$fileid" -O dataset.7z
        7z x dataset.7z -o"data/admin-boundary"
        rm dataset.7z
        mkdir -p "$admin_boundary_dir/input" "$admin_boundary_dir/output"
        echo "Admin boundary data extracted to $admin_boundary_dir"
    ) &
    GDOWN_PID=$!
    echo "Download started in background (PID: $GDOWN_PID)"
}

function main() {
    if should_run_step "unzip_install" "unzip installation" unzip_installed; then
        sudo apt-get install -y unzip
        mark_step_complete "unzip_install"
    fi

    ensure_dirs
    install_miniconda
    ensure_conda
    install_postgres
    install_rabbitmq

    if should_run_step "conda_env" "conda environment setup" conda_env_exists; then
        setup_conda_env
    fi

    if should_run_step "env_file" ".env generation" env_files_exist; then
        generate_env_file
    fi

    if should_run_step "collectstatic" "collectstatic" static_files_exist; then
        collect_static_files
    fi

    if should_run_step "django_migrations" "Django migration reset and migration apply" django_migrations_applied; then
        reset_django_migrations
        run_django_migrations
    fi

    if should_run_step "seed_data" "seed data loading" seed_data_loaded; then
        load_seed_data
    fi

    if should_run_step "superuser" "Django superuser creation" superuser_exists; then
        create_django_superuser
    fi

    auto_configure_gee_account_ids
    optional_configure_gee_account
    run_post_install_initialisation_check

    echo ""
    echo "=============================================="
    echo "  Core installation complete!"
    echo "=============================================="
    echo ""
    echo "Activate env: conda activate $CONDA_ENV_NAME"
    echo ""
    echo "IMPORTANT: Review and update the .env file at $BACKEND_DIR/nrm_app/.env"
    echo "   with your actual credentials before running in production."
    echo ""

    if normalize_existing_admin_boundary_data; then
        echo "Existing admin boundary data detected. Skipping download prompt."
        echo ""
        if [ "${POST_INSTALL_INITIALISATION_FAILED:-0}" -eq 1 ]; then
            echo "All done, but post-install validation found issues that still need attention."
        else
            echo "All done! Setup is fully complete."
        fi
        return
    fi

    echo "=============================================="
    echo "  Admin boundary data (~8GB) is required."
    echo "=============================================="
    echo ""
    echo "1) Download now (will take a while)"
    echo "2) Skip (I will download it manually later)"
    echo ""
    if [ ! -t 0 ]; then
        admin_boundary_choice="1"
    elif ! read -r -t "$STEP_PROMPT_TIMEOUT" -p "Enter choice [1/2]: " admin_boundary_choice; then
        echo ""
        echo "No response received in ${STEP_PROMPT_TIMEOUT} seconds. Proceeding with download."
        admin_boundary_choice="1"
    fi

    case "$admin_boundary_choice" in
        1)
            if should_run_step "admin_boundary_data" "admin boundary data download" admin_boundary_data_present; then
                echo ""
                echo "Downloading admin boundary data. Please be patient..."
                echo ""
                download_admin_boundary_data
                if [ -n "$GDOWN_PID" ]; then
                    wait "$GDOWN_PID"
                    echo ""
                    echo "Admin boundary data download and extraction complete."
                    mark_step_complete "admin_boundary_data"
                fi
            fi
            ;;
        *)
            echo ""
            echo "Skipped. To download later, run from the repo root:"
            echo "  pip install gdown && sudo apt-get install -y p7zip-full"
            echo "  gdown 1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d -O dataset.7z"
            echo "  7z x dataset.7z -o\"data/admin-boundary\""
            echo "  rm dataset.7z"
            ;;
    esac

    echo ""
    if [ "${POST_INSTALL_INITIALISATION_FAILED:-0}" -eq 1 ]; then
        echo "All done, but post-install validation found issues that still need attention."
    else
        echo "All done! Setup is fully complete."
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
