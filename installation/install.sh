#!/bin/bash
# CoRE Stack backend installer.

set -euo pipefail

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
SHELL_RC="$HOME/.bashrc"
INSTALL_STATE_DIR="$BACKEND_DIR/.installation_state"
APP_ENV_FILE="$BACKEND_DIR/nrm_app/.env"
LEGACY_ROOT_ENV_FILE="$BACKEND_DIR/.env"
DEFAULT_GEE_ACCOUNT_NAME="local-gee-account"
POST_INSTALL_REQUIRE_GEE=0
POST_INSTALL_INITIALISATION_FAILED=0
GEE_JSON_PATH_ARG=""
PUBLIC_API_X_API_KEY_ARG=""
PUBLIC_API_BASE_URL_ARG=""
GEOSERVER_URL_ARG=""
GEOSERVER_USERNAME_ARG=""
GEOSERVER_PASSWORD_ARG=""
STEP_START_FROM=""
LIST_STEPS_ONLY=0
DEFAULT_PUBLIC_API_BASE_URL="https://geoserver.core-stack.org/api/v1"
DEFAULT_PUBLIC_API_SAMPLE_STATE="assam"
DEFAULT_PUBLIC_API_SAMPLE_DISTRICT="cachar"
DEFAULT_PUBLIC_API_SAMPLE_TEHSIL="lakhipur"
declare -a ONLY_STEPS=()
declare -a SKIP_STEPS=()
declare -a OPTIONAL_INPUT_KEYS=(
    "gee_json"
    "public_api_key"
    "public_api_base_url"
    "geoserver_url"
    "geoserver_username"
    "geoserver_password"
)
declare -A OPTIONAL_INPUT_VALUES=()

# === ENV FILE CONFIGURATION ===
ENV_DB_NAME="$POSTGRES_DB"
ENV_DB_USER="$POSTGRES_USER"
ENV_DB_PASSWORD="$POSTGRES_PASSWORD"
ENV_DEPLOYMENT_DIR='$BACKEND_DIR'
ENV_TMP_LOCATION='$BACKEND_DIR/tmp'

STEP_ORDER=(
    "unzip_install"
    "miniconda"
    "postgres"
    "rabbitmq"
    "conda_env"
    "env_file"
    "collectstatic"
    "django_migrations"
    "seed_data"
    "superuser"
    "gee_configuration"
    "admin_boundary_data"
    "initialisation_check"
    "public_api_check"
)

function step_label() {
    case "$1" in
        unzip_install) echo "Install unzip" ;;
        miniconda) echo "Install Miniconda" ;;
        postgres) echo "Install PostgreSQL" ;;
        rabbitmq) echo "Install RabbitMQ" ;;
        conda_env) echo "Set up conda environment" ;;
        env_file) echo "Generate/update .env" ;;
        collectstatic) echo "Collect static files" ;;
        django_migrations) echo "Run Django migrations" ;;
        seed_data) echo "Load seed data" ;;
        superuser) echo "Ensure test superuser" ;;
        gee_configuration) echo "Configure Google Earth Engine" ;;
        admin_boundary_data) echo "Download admin-boundary data" ;;
        initialisation_check) echo "Run internal API initialisation check" ;;
        public_api_check) echo "Run public API smoke test" ;;
        *) echo "$1" ;;
    esac
}

function print_usage() {
    cat <<EOF
Usage: ./installation/install.sh [options]

Options:
  --from STEP           Run from STEP through the remaining installer steps.
  --only A,B,C          Run only the listed comma-separated steps.
  --skip A,B,C          Skip the listed comma-separated steps.
  --input KEY=VALUE     Provide an optional installer input.
  --gee-json PATH       Import this GEE service-account JSON without prompting.
  --list-steps          Print the available installer steps and exit.
  -h, --help            Show this help text.

Steps:
$(for step in "${STEP_ORDER[@]}"; do printf '  %-22s %s\n' "$step" "$(step_label "$step")"; done)
EOF
}

function print_available_steps() {
    local step
    for step in "${STEP_ORDER[@]}"; do
        printf '%-22s %s\n' "$step" "$(step_label "$step")"
    done
}

function trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s\n' "$value"
}

function is_known_step() {
    local candidate="$1"
    local step
    for step in "${STEP_ORDER[@]}"; do
        if [ "$step" = "$candidate" ]; then
            return 0
        fi
    done
    return 1
}

function array_contains() {
    local needle="$1"
    shift
    local item
    for item in "$@"; do
        if [ "$item" = "$needle" ]; then
            return 0
        fi
    done
    return 1
}

function parse_step_csv() {
    local csv="$1"
    local -n output_ref="$2"
    local raw_steps=()
    local step=""

    IFS=',' read -r -a raw_steps <<< "$csv"
    output_ref=()
    for step in "${raw_steps[@]}"; do
        step="$(trim "$step")"
        if [ -z "$step" ]; then
            continue
        fi
        if ! is_known_step "$step"; then
            echo "Unknown step: $step"
            echo ""
            print_usage
            exit 1
        fi
        output_ref+=("$step")
    done
}

function optional_input_description() {
    case "$1" in
        gee_json) echo "Path to a GEE service-account JSON file" ;;
        public_api_key) echo "X-API-Key used by public API helper scripts and smoke tests" ;;
        public_api_base_url) echo "Base URL for public APIs, for example https://geoserver.core-stack.org/api/v1" ;;
        geoserver_url) echo "GeoServer base URL used for publish/download validation, for example https://host/geoserver" ;;
        geoserver_username) echo "GeoServer REST username used by internal publish flows" ;;
        geoserver_password) echo "GeoServer REST password used by internal publish flows" ;;
        *) echo "" ;;
    esac
}

function optional_input_example() {
    case "$1" in
        gee_json) echo "gee_json=/full/path/to/service-account.json" ;;
        public_api_key) echo "public_api_key=your-public-api-key" ;;
        public_api_base_url) echo "public_api_base_url=https://geoserver.core-stack.org/api/v1" ;;
        geoserver_url) echo "geoserver_url=https://host/geoserver" ;;
        geoserver_username) echo "geoserver_username=admin" ;;
        geoserver_password) echo "geoserver_password=your-password" ;;
        *) echo "" ;;
    esac
}

function normalize_optional_input_key() {
    local candidate="$1"

    candidate="$(trim "$candidate")"
    candidate="${candidate##--}"
    if [[ "$candidate" =~ ^-[[:space:]]*(.+)$ ]]; then
        candidate="${BASH_REMATCH[1]}"
    fi
    candidate="${candidate//-/_}"

    case "$candidate" in
        gee_json)
            echo "gee_json"
            ;;
        public_api_key)
            echo "public_api_key"
            ;;
        public_api_base_url)
            echo "public_api_base_url"
            ;;
        geoserver_url)
            echo "geoserver_url"
            ;;
        geoserver_username)
            echo "geoserver_username"
            ;;
        geoserver_password)
            echo "geoserver_password"
            ;;
        *)
            echo "$candidate"
            ;;
    esac
}

function is_supported_optional_input() {
    local candidate="$1"
    array_contains "$candidate" "${OPTIONAL_INPUT_KEYS[@]}"
}

function set_optional_input_value() {
    local key="$1"
    local value="$2"

    key="$(normalize_optional_input_key "$key")"
    value="$(trim "$value")"
    value="$(strip_wrapping_quotes "$value")"

    if ! is_supported_optional_input "$key"; then
        echo "Unknown optional input key: $key"
        echo "Supported optional inputs:"
        local supported_key
        for supported_key in "${OPTIONAL_INPUT_KEYS[@]}"; do
            echo "  - $supported_key: $(optional_input_description "$supported_key")"
        done
        exit 1
    fi

    OPTIONAL_INPUT_VALUES["$key"]="$value"
    case "$key" in
        gee_json)
            GEE_JSON_PATH_ARG="$value"
            ;;
        public_api_key)
            PUBLIC_API_X_API_KEY_ARG="$value"
            ;;
        public_api_base_url)
            PUBLIC_API_BASE_URL_ARG="$value"
            ;;
        geoserver_url)
            GEOSERVER_URL_ARG="$value"
            ;;
        geoserver_username)
            GEOSERVER_USERNAME_ARG="$value"
            ;;
        geoserver_password)
            GEOSERVER_PASSWORD_ARG="$value"
            ;;
    esac
}

function parse_optional_input_entry() {
    local entry="$1"
    local key=""
    local value=""

    entry="$(trim "$entry")"
    [ -n "$entry" ] || return 0

    if [[ "$entry" == *=* ]]; then
        key="${entry%%=*}"
        value="${entry#*=}"
    elif [[ "$entry" =~ ^-[[:space:]]+([A-Za-z0-9_-]+)[[:space:]]+(.+)$ ]]; then
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
    elif [[ "$entry" =~ ^--?([A-Za-z0-9_-]+)[[:space:]]+(.+)$ ]]; then
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
    else
        echo "Invalid optional input: $entry"
        echo "Expected KEY=VALUE or CLI-style input, for example:"
        echo "  gee_json=/full/path/to/service-account.json"
        echo "  --gee-json /full/path/to/service-account.json"
        echo "  - gee-json \"Y:\\path\\to\\service-account.json\""
        exit 1
    fi

    key="$(trim "$key")"
    value="$(trim "$value")"

    if [ -z "$value" ]; then
        echo "Optional input '$key' requires a value."
        exit 1
    fi

    set_optional_input_value "$key" "$value"
}

function parse_optional_input_list() {
    local raw_input="$1"
    local assignments=()
    local assignment=""

    IFS=',' read -r -a assignments <<< "$raw_input"
    for assignment in "${assignments[@]}"; do
        parse_optional_input_entry "$assignment"
    done
}

function parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --from)
                shift
                [ "$#" -gt 0 ] || { echo "--from requires a step name."; exit 1; }
                STEP_START_FROM="$(trim "$1")"
                is_known_step "$STEP_START_FROM" || { echo "Unknown step: $STEP_START_FROM"; exit 1; }
                ;;
            --only)
                shift
                [ "$#" -gt 0 ] || { echo "--only requires a comma-separated step list."; exit 1; }
                parse_step_csv "$1" ONLY_STEPS
                ;;
            --skip)
                shift
                [ "$#" -gt 0 ] || { echo "--skip requires a comma-separated step list."; exit 1; }
                parse_step_csv "$1" SKIP_STEPS
                ;;
            --input)
                shift
                [ "$#" -gt 0 ] || { echo "--input requires KEY=VALUE."; exit 1; }
                parse_optional_input_entry "$1"
                ;;
            --gee-json)
                shift
                [ "$#" -gt 0 ] || { echo "--gee-json requires a file path."; exit 1; }
                set_optional_input_value "gee_json" "$1"
                ;;
            --list-steps)
                LIST_STEPS_ONLY=1
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo ""
                print_usage
                exit 1
                ;;
        esac
        shift
    done

    if [ -n "$STEP_START_FROM" ] && [ "${#ONLY_STEPS[@]}" -gt 0 ]; then
        echo "Use either --from or --only, not both."
        exit 1
    fi
}

function print_optional_input_catalog() {
    local key=""

    echo "Optional installer inputs available now or later via CLI:"
    for key in "${OPTIONAL_INPUT_KEYS[@]}"; do
        echo "  - $key: $(optional_input_description "$key")"
        echo "    Example: $(optional_input_example "$key")"
    done
    echo "CLI shortcuts:"
    echo "  --input gee_json=/full/path/to/service-account.json"
    echo "  --gee-json /full/path/to/service-account.json"
    echo "  --input public_api_key=your-public-api-key"
    echo "  --input public_api_base_url=https://geoserver.core-stack.org/api/v1"
    echo "  --input geoserver_url=https://host/geoserver"
    echo "  --input geoserver_username=admin"
    echo "  --input geoserver_password=your-password"
    echo "You can enter KEY=VALUE pairs, CLI-style values like --gee-json /path,"
    echo "or multiple comma-separated entries in one line."
}

function prompt_for_optional_inputs() {
    local optional_input_line=""

    if [ ! -t 0 ]; then
        return 0
    fi

    if [ "${#OPTIONAL_INPUT_VALUES[@]}" -gt 0 ]; then
        return 0
    fi

    echo ""
    print_optional_input_catalog
    read -r -p "Optional inputs [press Enter to continue]: " optional_input_line

    if [ -n "$optional_input_line" ]; then
        parse_optional_input_list "$optional_input_line"
    fi
}

function print_optional_input_summary() {
    local key=""
    local value=""
    local display_value=""

    echo "Optional inputs:"
    if [ "${#OPTIONAL_INPUT_VALUES[@]}" -eq 0 ]; then
        echo "  - none"
        return
    fi

    for key in "${OPTIONAL_INPUT_KEYS[@]}"; do
        if [ -v OPTIONAL_INPUT_VALUES["$key"] ]; then
            value="${OPTIONAL_INPUT_VALUES["$key"]}"
            display_value="$value"
            if [ "$key" = "public_api_key" ] && [ "${#value}" -gt 8 ]; then
                display_value="${value:0:4}...${value: -4}"
            fi
            echo "  - $key=$display_value"
        fi
    done
}

function step_index() {
    local target="$1"
    local index=0
    local step
    for step in "${STEP_ORDER[@]}"; do
        if [ "$step" = "$target" ]; then
            echo "$index"
            return 0
        fi
        index=$((index + 1))
    done
    echo "-1"
    return 1
}

function should_execute_step() {
    local step="$1"

    if array_contains "$step" "${SKIP_STEPS[@]}"; then
        return 1
    fi

    if [ "${#ONLY_STEPS[@]}" -gt 0 ]; then
        array_contains "$step" "${ONLY_STEPS[@]}"
        return $?
    fi

    if [ -n "$STEP_START_FROM" ]; then
        [ "$(step_index "$step")" -ge "$(step_index "$STEP_START_FROM")" ]
        return $?
    fi

    return 0
}

function step_is_forced() {
    local step="$1"

    if [ "${#ONLY_STEPS[@]}" -gt 0 ]; then
        array_contains "$step" "${ONLY_STEPS[@]}"
        return $?
    fi

    if [ -n "$STEP_START_FROM" ]; then
        [ "$(step_index "$step")" -ge "$(step_index "$STEP_START_FROM")" ]
        return $?
    fi

    return 1
}

function step_marker_path() {
    local step_name="$1"
    echo "$INSTALL_STATE_DIR/${step_name}.done"
}

function mark_step_complete() {
    local step_name="$1"
    mkdir -p "$INSTALL_STATE_DIR"
    date -u +"%Y-%m-%dT%H:%M:%SZ" > "$(step_marker_path "$step_name")"
}

function ensure_conda() {
    if command -v conda >/dev/null 2>&1; then
        return 0
    fi

    if [ -f "$MINICONDA_DIR/etc/profile.d/conda.sh" ]; then
        # shellcheck disable=SC1091
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    fi

    if ! command -v conda >/dev/null 2>&1; then
        echo "Conda was not found. Expected it at $MINICONDA_DIR."
        exit 1
    fi
}

function activate_conda_env() {
    ensure_conda
    export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
    export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
    export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
    export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
    export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"
    export BLIS_NUM_THREADS="${BLIS_NUM_THREADS:-1}"
    export GOTO_NUM_THREADS="${GOTO_NUM_THREADS:-1}"
    # shellcheck disable=SC1091
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
}

function conda_env_exists() {
    ensure_conda
    conda env list | sed 's/^[* ]*//' | awk '{print $1}' | grep -qx "$CONDA_ENV_NAME"
}

function gee_configuration_present() {
    [ -f "$APP_ENV_FILE" ] && grep -Eq '^GEE_DEFAULT_ACCOUNT_ID="?([0-9]+)' "$APP_ENV_FILE"
}

function directory_has_contents() {
    local directory_path="$1"
    [ -d "$directory_path" ] && find "$directory_path" -mindepth 1 -print -quit 2>/dev/null | grep -q .
}

function admin_boundary_data_present() {
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    [ -f "$admin_boundary_dir/input/soi_tehsil.geojson" ] && \
        find "$admin_boundary_dir/input" -mindepth 2 -name '*.geojson' -print -quit 2>/dev/null | grep -q .
}

function nested_admin_boundary_data_present() {
    local nested_admin_boundary_dir="$BACKEND_DIR/data/admin-boundary/admin-boundary"
    [ -f "$nested_admin_boundary_dir/input/soi_tehsil.geojson" ] && \
        find "$nested_admin_boundary_dir/input" -mindepth 2 -name '*.geojson' -print -quit 2>/dev/null | grep -q .
}

function move_directory_contents() {
    local source_dir="$1"
    local destination_dir="$2"
    local items=()

    mkdir -p "$destination_dir"
    shopt -s dotglob nullglob
    items=("$source_dir"/*)
    if [ "${#items[@]}" -gt 0 ]; then
        mv "${items[@]}" "$destination_dir/"
    fi
    shopt -u dotglob nullglob
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

    echo "Found admin-boundary data in a nested extracted layout. Normalizing it to $admin_boundary_dir ..."
    mkdir -p "$admin_boundary_dir/input" "$admin_boundary_dir/output"

    if [ -d "$nested_admin_boundary_dir/input" ]; then
        move_directory_contents "$nested_admin_boundary_dir/input" "$admin_boundary_dir/input"
    fi

    if [ -d "$nested_admin_boundary_dir/output" ]; then
        move_directory_contents "$nested_admin_boundary_dir/output" "$admin_boundary_dir/output"
    fi

    rmdir "$nested_admin_boundary_dir/output" 2>/dev/null || true
    rmdir "$nested_admin_boundary_dir/input" 2>/dev/null || true
    rmdir "$nested_admin_boundary_dir" 2>/dev/null || true

    if admin_boundary_data_present; then
        echo "Admin-boundary data is ready at $admin_boundary_dir"
        mark_step_complete "admin_boundary_data"
        return 0
    fi

    echo "Admin-boundary data was detected, but automatic normalization did not finish cleanly."
    return 1
}

function finalize_admin_boundary_extraction() {
    local extracted_root="$1"
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    local candidate_dir=""
    local first_child=""

    if [ -d "$extracted_root/admin-boundary" ]; then
        candidate_dir="$extracted_root/admin-boundary"
    elif [ -d "$extracted_root/input" ] || [ -d "$extracted_root/output" ]; then
        candidate_dir="$extracted_root"
    else
        first_child="$(find "$extracted_root" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
        if [ -n "$first_child" ] && { [ -d "$first_child/input" ] || [ -d "$first_child/output" ]; }; then
            candidate_dir="$first_child"
        fi
    fi

    if [ -z "$candidate_dir" ]; then
        echo "Unable to detect the extracted admin-boundary layout under $extracted_root"
        return 1
    fi

    rm -rf "$admin_boundary_dir"
    mkdir -p "$(dirname "$admin_boundary_dir")"

    if [ "$candidate_dir" = "$extracted_root" ]; then
        mkdir -p "$admin_boundary_dir"
        move_directory_contents "$candidate_dir" "$admin_boundary_dir"
    else
        mv "$candidate_dir" "$admin_boundary_dir"
    fi

    normalize_existing_admin_boundary_data
}

function install_miniconda() {
    if command -v conda >/dev/null 2>&1; then
        echo "Conda already available ($(conda --version))."
        mark_step_complete "miniconda"
        return
    fi

    if [ -d "$MINICONDA_DIR" ]; then
        echo "Miniconda found at $MINICONDA_DIR. Sourcing it..."
        # shellcheck disable=SC1091
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
        mark_step_complete "miniconda"
        return
    fi

    echo "Installing Miniconda..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "$BACKEND_DIR/miniconda.sh"
    bash "$BACKEND_DIR/miniconda.sh" -b -p "$MINICONDA_DIR"
    rm -f "$BACKEND_DIR/miniconda.sh"

    if ! grep -qs 'miniconda3/etc/profile.d/conda.sh' "$SHELL_RC" 2>/dev/null; then
        {
            echo ""
            echo "# >>> conda initialize >>>"
            echo "source \"$MINICONDA_DIR/etc/profile.d/conda.sh\""
            echo "# <<< conda initialize <<<"
        } >> "$SHELL_RC"
    fi

    # shellcheck disable=SC1091
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    echo "Miniconda installed."
    mark_step_complete "miniconda"
}

function setup_conda_env() {
    local force="${1:-0}"

    ensure_conda
    if conda_env_exists && [ "$force" -ne 1 ]; then
        echo "Conda environment '$CONDA_ENV_NAME' already exists. Keeping it."
        mark_step_complete "conda_env"
        return
    fi

    echo "Setting up conda environment '$CONDA_ENV_NAME'..."
    conda env remove -n "$CONDA_ENV_NAME" -y >/dev/null 2>&1 || true
    conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"
    echo "Conda environment ready."
    mark_step_complete "conda_env"
}

function install_postgres() {
    if command -v psql >/dev/null 2>&1; then
        echo "PostgreSQL already installed ($(psql --version))."
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
    mark_step_complete "postgres"
}

function install_rabbitmq() {
    if command -v rabbitmqctl >/dev/null 2>&1; then
        echo "RabbitMQ already installed."
    else
        echo "Installing RabbitMQ..."
        sudo apt-get install -y rabbitmq-server
    fi

    sudo systemctl start rabbitmq-server
    sudo systemctl enable rabbitmq-server
    echo "RabbitMQ ready."
    mark_step_complete "rabbitmq"
}

function apply_env_overrides() {
    local env_file="$1"

    if [ -n "$ENV_DB_NAME" ]; then
        sed -i "s|^DB_NAME=\"\"|DB_NAME=\"$ENV_DB_NAME\"|" "$env_file"
    fi
    if [ -n "$ENV_DB_USER" ]; then
        sed -i "s|^DB_USER=\"\"|DB_USER=\"$ENV_DB_USER\"|" "$env_file"
    fi
    if [ -n "$ENV_DB_PASSWORD" ]; then
        sed -i "s|^DB_PASSWORD=\"\"|DB_PASSWORD=\"$ENV_DB_PASSWORD\"|" "$env_file"
    fi
    if [ -n "$ENV_DEPLOYMENT_DIR" ]; then
        sed -i "s|^DEPLOYMENT_DIR=\"\"|DEPLOYMENT_DIR=\"$ENV_DEPLOYMENT_DIR\"|" "$env_file"
    fi
    if [ -n "$ENV_TMP_LOCATION" ]; then
        sed -i "s|^TMP_LOCATION=\"\"|TMP_LOCATION=\"$ENV_TMP_LOCATION\"|" "$env_file"
    fi
}

function set_env_value() {
    local env_file="$1"
    local key="$2"
    local value="$3"

    if grep -q "^${key}=" "$env_file"; then
        sed -i "s|^${key}=.*|${key}=\"$value\"|" "$env_file"
    else
        echo "${key}=\"$value\"" >> "$env_file"
    fi
}

function normalize_public_api_base_url() {
    local base_url="$1"

    base_url="$(trim "$base_url")"
    base_url="$(strip_wrapping_quotes "$base_url")"
    base_url="${base_url%/}"

    if [ -n "$base_url" ] && [[ "$base_url" != */api/v1 ]]; then
        base_url="$base_url/api/v1"
    fi

    printf '%s\n' "$base_url"
}

function strip_wrapping_quotes() {
    local value="$1"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
        value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
        value="${value:1:${#value}-2}"
    fi
    printf '%s\n' "$value"
}

function current_env_value() {
    local env_file="$1"
    local key="$2"
    local value=""

    if [ ! -f "$env_file" ]; then
        return 0
    fi

    value=$(grep -E "^${key}=" "$env_file" | tail -n 1 | cut -d'=' -f2- || true)
    strip_wrapping_quotes "$value"
}

function maybe_set_installer_managed_path_value() {
    local env_file="$1"
    local key="$2"
    local legacy_absolute="$3"
    local legacy_relative="$4"
    local managed_value="$5"
    local current_value=""

    current_value="$(current_env_value "$env_file" "$key")"

    if [ -z "$current_value" ] || [ "$current_value" = "$legacy_absolute" ] || [ "$current_value" = "$legacy_relative" ] || [ "$current_value" = "$managed_value" ]; then
        set_env_value "$env_file" "$key" "$managed_value"
    fi
}

function generate_fernet_key() {
    activate_conda_env
    python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode("utf-8"))
PY
}

function fernet_key_is_valid() {
    local candidate="$1"

    if [ -z "$candidate" ]; then
        return 1
    fi

    activate_conda_env
    python - "$candidate" <<'PY'
import sys
from cryptography.fernet import Fernet

try:
    Fernet(sys.argv[1].encode("utf-8"))
except Exception:
    raise SystemExit(1)
raise SystemExit(0)
PY
}

function generate_env_file() {
    local settings_file="$BACKEND_DIR/nrm_app/settings.py"
    local env_file="$APP_ENV_FILE"
    local env_vars=""
    local env_vars_simple=""
    local all_vars=""
    local existing_vars=""
    local var_name=""
    local current_fernet_key=""
    local fernet_key=""

    echo "Generating .env file from settings.py..."

    if [ ! -f "$settings_file" ]; then
        echo "ERROR: settings.py not found at $settings_file"
        return 1
    fi

    env_vars=$(grep -oE 'env\.[a-z]*\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$settings_file" 2>/dev/null | \
        sed -E 's/env\.[a-z]*\s*\(\s*"([^"]+)"/\1/' | sort -u)
    env_vars_simple=$(grep -oE 'env\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$settings_file" 2>/dev/null | \
        sed -E 's/env\s*\(\s*"([^"]+)"/\1/' | sort -u)
    all_vars=$(printf '%s\n%s\n' "$env_vars" "$env_vars_simple" | sort -u | grep -v '^$' || true)

    if [ ! -f "$env_file" ] && [ -f "$LEGACY_ROOT_ENV_FILE" ]; then
        echo "Migrating existing root .env to $APP_ENV_FILE ..."
        mkdir -p "$(dirname "$env_file")"
        cp "$LEGACY_ROOT_ENV_FILE" "$env_file"
    fi

    if [ ! -f "$env_file" ]; then
        echo "Creating new .env file..."
        mkdir -p "$(dirname "$env_file")"
        {
            echo "# Auto-generated .env file"
            echo "# Generated on $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
            echo ""
            echo "SECRET_KEY=$(openssl rand -base64 32)"
            echo "DEBUG=True"
            echo ""
        } > "$env_file"
    else
        echo "Existing .env file found. Updating missing variables..."
    fi

    existing_vars=$(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$env_file" | cut -d'=' -f1 | sort -u || true)

    while IFS= read -r var_name; do
        if [ -z "$var_name" ] || [ "$var_name" = "SECRET_KEY" ] || [ "$var_name" = "DEBUG" ]; then
            continue
        fi

        if echo "$existing_vars" | grep -qx "$var_name"; then
            continue
        fi

        case "$var_name" in
            WHATSAPP_MEDIA_PATH)
                echo 'WHATSAPP_MEDIA_PATH=$BACKEND_DIR/bot_interface/whatsapp_media' >> "$env_file"
                mkdir -p "$BACKEND_DIR/bot_interface/whatsapp_media"
                ;;
            EXCEL_DIR)
                echo 'EXCEL_DIR=$BACKEND_DIR/data/excel_files' >> "$env_file"
                mkdir -p "$BACKEND_DIR/data/excel_files"
                ;;
            EXCEL_PATH)
                echo 'EXCEL_PATH=$BACKEND_DIR' >> "$env_file"
                ;;
            *)
                echo "${var_name}=\"\"" >> "$env_file"
                ;;
        esac
    done <<< "$all_vars"

    if ! grep -q '^BACKEND_DIR=' "$env_file"; then
        echo 'BACKEND_DIR=.' >> "$env_file"
    fi

    if ! grep -q '^EXCEL_DIR=' "$env_file"; then
        echo 'EXCEL_DIR=$BACKEND_DIR/data/excel_files' >> "$env_file"
        mkdir -p "$BACKEND_DIR/data/excel_files"
    fi

    if ! grep -q '^WHATSAPP_MEDIA_PATH=' "$env_file"; then
        echo 'WHATSAPP_MEDIA_PATH=$BACKEND_DIR/bot_interface/whatsapp_media' >> "$env_file"
        mkdir -p "$BACKEND_DIR/bot_interface/whatsapp_media"
    fi

    if ! grep -q '^EXCEL_PATH=' "$env_file"; then
        echo 'EXCEL_PATH=$BACKEND_DIR' >> "$env_file"
    fi

    apply_env_overrides "$env_file"
    maybe_set_installer_managed_path_value "$env_file" "BACKEND_DIR" "$BACKEND_DIR" "." "."
    maybe_set_installer_managed_path_value "$env_file" "DEPLOYMENT_DIR" "$BACKEND_DIR" "." '$BACKEND_DIR'
    maybe_set_installer_managed_path_value "$env_file" "TMP_LOCATION" "$BACKEND_DIR/tmp" "tmp" '$BACKEND_DIR/tmp'
    maybe_set_installer_managed_path_value "$env_file" "WHATSAPP_MEDIA_PATH" "$BACKEND_DIR/bot_interface/whatsapp_media" "bot_interface/whatsapp_media" '$BACKEND_DIR/bot_interface/whatsapp_media'
    maybe_set_installer_managed_path_value "$env_file" "EXCEL_DIR" "$BACKEND_DIR/data/excel_files" "data/excel_files" '$BACKEND_DIR/data/excel_files'
    maybe_set_installer_managed_path_value "$env_file" "EXCEL_PATH" "$BACKEND_DIR" "." '$BACKEND_DIR'
    set_env_value "$env_file" "PUBLIC_API_BASE_URL" "$(normalize_public_api_base_url "${PUBLIC_API_BASE_URL_ARG:-$(current_env_value "$env_file" "PUBLIC_API_BASE_URL")}")"
    if [ -z "$(current_env_value "$env_file" "PUBLIC_API_BASE_URL")" ]; then
        set_env_value "$env_file" "PUBLIC_API_BASE_URL" "$DEFAULT_PUBLIC_API_BASE_URL"
    fi
    if [ -n "$PUBLIC_API_X_API_KEY_ARG" ]; then
        set_env_value "$env_file" "PUBLIC_API_X_API_KEY" "$PUBLIC_API_X_API_KEY_ARG"
    elif ! grep -q '^PUBLIC_API_X_API_KEY=' "$env_file"; then
        echo 'PUBLIC_API_X_API_KEY=""' >> "$env_file"
    fi
    if [ -n "$GEOSERVER_URL_ARG" ]; then
        set_env_value "$env_file" "GEOSERVER_URL" "$GEOSERVER_URL_ARG"
    fi
    if [ -n "$GEOSERVER_USERNAME_ARG" ]; then
        set_env_value "$env_file" "GEOSERVER_USERNAME" "$GEOSERVER_USERNAME_ARG"
    fi
    if [ -n "$GEOSERVER_PASSWORD_ARG" ]; then
        set_env_value "$env_file" "GEOSERVER_PASSWORD" "$GEOSERVER_PASSWORD_ARG"
    fi

    current_fernet_key="$(current_env_value "$env_file" "FERNET_KEY")"
    if fernet_key_is_valid "$current_fernet_key"; then
        echo "FERNET_KEY already present and valid. Keeping the existing value."
    else
        fernet_key="$(generate_fernet_key)"
        set_env_value "$env_file" "FERNET_KEY" "$fernet_key"
        echo "FERNET_KEY generated and added to .env"
    fi

    chown "$USER:$USER" "$env_file" 2>/dev/null || true
    chmod 640 "$env_file"

    echo "Total variables in .env: $(grep -c '^[A-Za-z_]' "$env_file")"
    echo ".env ready at $env_file"
    mark_step_complete "env_file"
}

function collect_static_files() {
    echo "Collecting static files..."
    activate_conda_env
    cd "$BACKEND_DIR"
    python manage.py collectstatic --noinput --clear --skip-checks
    echo "Static files collected."
    mark_step_complete "collectstatic"
}

function run_django_migrations() {
    echo "Running Django migrations..."
    activate_conda_env
    cd "$BACKEND_DIR"
    python manage.py makemigrations --skip-checks
    python manage.py migrate --fake-initial --skip-checks
    echo "Django migrations complete."
    mark_step_complete "django_migrations"
}

function seed_data_loaded() {
    activate_conda_env
    cd "$BACKEND_DIR"
    local has_seed_data
    has_seed_data=$(python manage.py shell -c "from geoadmin.models import StateSOI; print(1 if StateSOI.objects.exists() else 0)" 2>/dev/null | tail -n 1)
    [ "${has_seed_data:-0}" = "1" ]
}

function load_seed_data() {
    local force="${1:-0}"
    local seed_file="$BACKEND_DIR/installation/seed/seed_data.json"

    if [ ! -f "$seed_file" ]; then
        echo "No seed data found at $seed_file. Skipping."
        return
    fi

    if [ "$force" -ne 1 ] && seed_data_loaded; then
        echo "Seed data already looks loaded. Keeping the existing database contents."
        mark_step_complete "seed_data"
        return
    fi

    echo "Loading seed data..."
    activate_conda_env
    cd "$BACKEND_DIR"
    python manage.py loaddata --skip-checks "$seed_file"
    echo "Seed data loaded."
    mark_step_complete "seed_data"
}

function normalize_user_path() {
    local raw_path="$1"
    local normalized_path=""
    local drive_letter=""
    local remainder=""

    normalized_path="$(trim "$raw_path")"
    normalized_path="$(strip_wrapping_quotes "$normalized_path")"
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
        normalized_path="$(realpath -m "$normalized_path")"
    fi

    printf '%s\n' "$normalized_path"
}

function looks_like_user_path_input() {
    local candidate="$1"
    candidate="$(trim "$candidate")"
    candidate="$(strip_wrapping_quotes "$candidate")"

    [ -n "$candidate" ] || return 1

    [[ "$candidate" =~ ^[A-Za-z]:[\\/].* ]] && return 0
    [[ "$candidate" == *"/"* ]] && return 0
    [[ "$candidate" == *"\\"* ]] && return 0
    [[ "$candidate" == ./* ]] && return 0
    [[ "$candidate" == ../* ]] && return 0
    [[ "$candidate" == "~"* ]] && return 0
    [[ "$candidate" == *.json ]] && return 0

    return 1
}

function auto_configure_gee_account_ids() {
    local env_file="$APP_ENV_FILE"
    local first_account_id=""
    local helper_account_id=""

    [ -f "$env_file" ] || return 0

    activate_conda_env
    cd "$BACKEND_DIR"

    first_account_id=$(python manage.py shell -c "from gee_computing.models import GEEAccount; account = GEEAccount.objects.order_by('id').first(); print(account.id if account else '')" 2>/dev/null | tail -n 1)
    helper_account_id=$(python manage.py shell -c "from gee_computing.models import GEEAccount; account = GEEAccount.objects.exclude(helper_account=None).order_by('id').first(); print(account.helper_account_id if account and account.helper_account_id else '')" 2>/dev/null | tail -n 1)

    if [ -n "$first_account_id" ] && grep -q '^GEE_DEFAULT_ACCOUNT_ID=""' "$env_file"; then
        sed -i "s|^GEE_DEFAULT_ACCOUNT_ID=\"\"|GEE_DEFAULT_ACCOUNT_ID=\"$first_account_id\"|" "$env_file"
        echo "Auto-configured GEE_DEFAULT_ACCOUNT_ID=$first_account_id"
    fi

    if [ -n "$helper_account_id" ] && grep -q '^GEE_HELPER_ACCOUNT_ID=""' "$env_file"; then
        sed -i "s|^GEE_HELPER_ACCOUNT_ID=\"\"|GEE_HELPER_ACCOUNT_ID=\"$helper_account_id\"|" "$env_file"
        echo "Auto-configured GEE_HELPER_ACCOUNT_ID=$helper_account_id"
    fi
}

function configure_paths() {
    local gee_json_path_input="$1"
    local env_file="$APP_ENV_FILE"
    local account_name="${2:-$DEFAULT_GEE_ACCOUNT_NAME}"
    local normalized_gee_json_path=""
    local import_result=""
    local account_id=""
    local staged_relative_path=""

    normalized_gee_json_path="$(normalize_user_path "$gee_json_path_input")"

    if [ "$normalized_gee_json_path" != "$gee_json_path_input" ]; then
        echo "Resolved GEE credentials path to: $normalized_gee_json_path"
    fi

    if [ ! -f "$normalized_gee_json_path" ]; then
        echo "GEE credentials file not found: $normalized_gee_json_path"
        return 1
    fi

    activate_conda_env
    cd "$BACKEND_DIR"

    import_result=$(GEE_JSON_PATH="$normalized_gee_json_path" GEE_ACCOUNT_NAME="$account_name" PYTHONPATH="$BACKEND_DIR" python - <<'PY'
import json
import os

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
        }
    )
)
PY
)

    account_id=$(echo "$import_result" | tail -n 1 | python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("account_id",""))' 2>/dev/null | tr -d '[:space:]')
    staged_relative_path=$(echo "$import_result" | tail -n 1 | python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("relative_path",""))' 2>/dev/null | tr -d '\r')

    if [ -z "$account_id" ] || [ -z "$staged_relative_path" ]; then
        echo "Unable to create or update the GEE account from the provided JSON."
        return 1
    fi

    set_env_value "$env_file" "GEE_DEFAULT_ACCOUNT_ID" "$account_id"
    set_env_value "$env_file" "GEE_HELPER_ACCOUNT_ID" "$account_id"
    set_env_value "$env_file" "GEE_SERVICE_ACCOUNT_KEY_PATH" "$staged_relative_path"
    set_env_value "$env_file" "GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH" "$staged_relative_path"
    POST_INSTALL_REQUIRE_GEE=1
    echo "Configured GEE account id=$account_id using staged credentials at $staged_relative_path"
    mark_step_complete "gee_configuration"
}

function optional_configure_gee_account() {
    local force="${1:-0}"
    local gee_json_path_input=""
    local had_existing_gee_configuration=0

    auto_configure_gee_account_ids
    if gee_configuration_present; then
        had_existing_gee_configuration=1
        POST_INSTALL_REQUIRE_GEE=1
    fi

    if [ -n "$GEE_JSON_PATH_ARG" ]; then
        gee_json_path_input="$GEE_JSON_PATH_ARG"
    elif [ "$force" -ne 1 ] && [ "$had_existing_gee_configuration" -eq 1 ]; then
        echo "Existing Google Earth Engine configuration detected. Keeping it."
        mark_step_complete "gee_configuration"
        return
    elif [ ! -t 0 ]; then
        if [ "$had_existing_gee_configuration" -eq 1 ]; then
            echo "No interactive terminal detected. Keeping the existing GEE configuration."
            mark_step_complete "gee_configuration"
            return
        fi
        echo "No interactive terminal detected. Skipping optional GEE setup."
        POST_INSTALL_REQUIRE_GEE=0
        return
    else
        echo ""
        echo "Optional: configure Google Earth Engine now."
        echo "If your organization shared a service-account JSON, enter its full path below."
        echo "Windows paths like C:\\Users\\name\\Downloads\\file.json and relative paths like .\\file.json are accepted."
        echo "Press Enter to skip."
        read -r -p "GEE JSON path: " gee_json_path_input
    fi

    if [ -z "$gee_json_path_input" ]; then
        if [ "$had_existing_gee_configuration" -eq 1 ]; then
            echo "Keeping the existing GEE configuration."
            POST_INSTALL_REQUIRE_GEE=1
            mark_step_complete "gee_configuration"
            return
        fi

        echo "Skipping optional GEE setup for now."
        POST_INSTALL_REQUIRE_GEE=0
        return
    fi

    if configure_paths "$gee_json_path_input"; then
        echo "GEE credentials imported. The final initialisation test will validate the live GEE path too."
    else
        POST_INSTALL_REQUIRE_GEE="$had_existing_gee_configuration"
        echo "GEE setup did not complete. Continuing with the core initialisation test only."
    fi
}

function ensure_django_superuser() {
    local result=""
    local username=""
    local action=""

    echo "Ensuring installer test superuser exists..."
    activate_conda_env
    cd "$BACKEND_DIR"

    result=$(python manage.py shell <<'PY'
import random
from django.contrib.auth import get_user_model

User = get_user_model()
installer_user = User.objects.filter(username__startswith="test_user_", is_superuser=True).order_by("id").first()

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
    installer_user.is_active = True
    installer_user.is_staff = True
    installer_user.save(update_fields=["is_active", "is_staff"])
    print(f"{installer_user.username}|created")
else:
    installer_user.is_active = True
    installer_user.is_staff = True
    installer_user.is_superuser = True
    installer_user.set_password("test_change_me")
    installer_user.save(update_fields=["is_active", "is_staff", "is_superuser", "password"])
    print(f"{installer_user.username}|updated")
PY
)

    username="${result%%|*}"
    action="${result##*|}"
    echo "Installer test superuser $action: username=$username password=test_change_me"
    mark_step_complete "superuser"
}

function public_api_configuration_present() {
    local api_key=""
    local base_url=""

    api_key="$(current_env_value "$APP_ENV_FILE" "PUBLIC_API_X_API_KEY")"
    base_url="$(current_env_value "$APP_ENV_FILE" "PUBLIC_API_BASE_URL")"

    [ -n "$api_key" ] && [ -n "$base_url" ]
}

function persist_optional_inputs_to_env() {
    local base_url=""

    [ -f "$APP_ENV_FILE" ] || return 0

    if [ -n "$PUBLIC_API_X_API_KEY_ARG" ]; then
        set_env_value "$APP_ENV_FILE" "PUBLIC_API_X_API_KEY" "$PUBLIC_API_X_API_KEY_ARG"
    fi

    base_url="$PUBLIC_API_BASE_URL_ARG"
    if [ -z "$base_url" ]; then
        base_url="$(current_env_value "$APP_ENV_FILE" "PUBLIC_API_BASE_URL")"
    fi
    if [ -n "$base_url" ]; then
        set_env_value "$APP_ENV_FILE" "PUBLIC_API_BASE_URL" "$(normalize_public_api_base_url "$base_url")"
    fi

    if [ -n "$GEOSERVER_URL_ARG" ]; then
        set_env_value "$APP_ENV_FILE" "GEOSERVER_URL" "$GEOSERVER_URL_ARG"
    fi
    if [ -n "$GEOSERVER_USERNAME_ARG" ]; then
        set_env_value "$APP_ENV_FILE" "GEOSERVER_USERNAME" "$GEOSERVER_USERNAME_ARG"
    fi
    if [ -n "$GEOSERVER_PASSWORD_ARG" ]; then
        set_env_value "$APP_ENV_FILE" "GEOSERVER_PASSWORD" "$GEOSERVER_PASSWORD_ARG"
    fi
}

function ensure_dirs() {
    mkdir -p "$BACKEND_DIR/logs"
    touch "$BACKEND_DIR/logs/app.log" "$BACKEND_DIR/logs/nrm_app.log"
    mkdir -p "$BACKEND_DIR/data/activated_locations"
    mkdir -p "$BACKEND_DIR/data/excel_files"
    mkdir -p "$BACKEND_DIR/tmp"
    mkdir -p "$INSTALL_STATE_DIR"
    echo "Required directories ready."
}

function install_gdown_if_missing() {
    activate_conda_env
    if python -m pip show gdown >/dev/null 2>&1; then
        return
    fi
    echo "Installing gdown into $CONDA_ENV_NAME ..."
    python -m pip install gdown
}

function download_admin_boundary_data() {
    local force="${1:-0}"
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    local archive_path="$BACKEND_DIR/dataset.7z"
    local extraction_root="$BACKEND_DIR/data/.admin-boundary-extract"
    local fileid="1VqIhB6HrKFDkDnlk1vedcEHhh5fk4f1d"

    if [ "$force" -ne 1 ] && normalize_existing_admin_boundary_data; then
        echo "Existing admin-boundary data detected. Keeping it."
        return
    fi

    echo "Downloading admin-boundary data (~8GB, this may take a while)..."
    rm -rf "$admin_boundary_dir" "$extraction_root"
    rm -f "$archive_path"
    mkdir -p "$BACKEND_DIR/data" "$extraction_root"

    install_gdown_if_missing
    sudo apt-get install -y p7zip-full

    cd "$BACKEND_DIR"
    gdown "$fileid" -O "$archive_path"
    7z x "$archive_path" -o"$extraction_root"
    rm -f "$archive_path"

    finalize_admin_boundary_extraction "$extraction_root"
    rm -rf "$extraction_root"

    echo "Admin-boundary data extracted to $admin_boundary_dir"
    mark_step_complete "admin_boundary_data"
}

function run_post_install_initialisation_check() {
    local initialisation_args=()

    normalize_existing_admin_boundary_data >/dev/null 2>&1 || true
    persist_optional_inputs_to_env

    activate_conda_env
    cd "$BACKEND_DIR"

    echo ""
    echo "Running internal API initialisation test..."
    echo "This validation runs Django in-process, creates a JWT Bearer token automatically,"
    echo "and forces Celery eager mode for the checked task. You do not need runserver"
    echo "or a separate Celery worker for this installer-time verification."

    if [ "${POST_INSTALL_REQUIRE_GEE:-0}" -eq 1 ]; then
        initialisation_args=(--require-gee)
    fi

    if python computing/misc/internal_api_initialisation_test.py "${initialisation_args[@]}"; then
        POST_INSTALL_INITIALISATION_FAILED=0
        echo "Internal API initialisation test passed."
    else
        POST_INSTALL_INITIALISATION_FAILED=1
        echo "Internal API initialisation test found issues. Review the output above before using the APIs."
    fi
    mark_step_complete "initialisation_check"
}

function run_public_api_smoke_test() {
    local api_key=""
    local base_url=""
    local smoke_test_args=()

    if [ ! -f "$APP_ENV_FILE" ]; then
        if [ -z "$PUBLIC_API_X_API_KEY_ARG" ]; then
            echo "Skipping public API smoke test because $APP_ENV_FILE does not exist yet."
            mark_step_complete "public_api_check"
            return
        fi
    else
        persist_optional_inputs_to_env
    fi

    api_key="$PUBLIC_API_X_API_KEY_ARG"
    if [ -z "$api_key" ] && [ -f "$APP_ENV_FILE" ]; then
        api_key="$(current_env_value "$APP_ENV_FILE" "PUBLIC_API_X_API_KEY")"
    fi
    base_url="$PUBLIC_API_BASE_URL_ARG"
    if [ -z "$base_url" ] && [ -f "$APP_ENV_FILE" ]; then
        base_url="$(current_env_value "$APP_ENV_FILE" "PUBLIC_API_BASE_URL")"
    fi

    if [ -z "$api_key" ]; then
        echo "Skipping public API smoke test because PUBLIC_API_X_API_KEY is not configured."
        echo "Provide it up front with --input public_api_key=... or update $APP_ENV_FILE later."
        mark_step_complete "public_api_check"
        return
    fi

    if [ -z "$base_url" ]; then
        base_url="$DEFAULT_PUBLIC_API_BASE_URL"
        if [ -f "$APP_ENV_FILE" ]; then
            set_env_value "$APP_ENV_FILE" "PUBLIC_API_BASE_URL" "$base_url"
        fi
    fi

    echo ""
    echo "Running public API smoke test..."
    echo "Sample location: $DEFAULT_PUBLIC_API_SAMPLE_STATE / $DEFAULT_PUBLIC_API_SAMPLE_DISTRICT / $DEFAULT_PUBLIC_API_SAMPLE_TEHSIL"

    activate_conda_env
    cd "$BACKEND_DIR"

    if [ -f "$APP_ENV_FILE" ]; then
        smoke_test_args+=(--env-file "$APP_ENV_FILE")
    fi
    smoke_test_args+=(--api-key "$api_key" --base-url "$base_url")

    if python installation/public_api_client.py \
        "${smoke_test_args[@]}" \
        smoke-test \
        --state "$DEFAULT_PUBLIC_API_SAMPLE_STATE" \
        --district "$DEFAULT_PUBLIC_API_SAMPLE_DISTRICT" \
        --tehsil "$DEFAULT_PUBLIC_API_SAMPLE_TEHSIL"; then
        echo "Public API smoke test passed."
    else
        POST_INSTALL_INITIALISATION_FAILED=1
        echo "Public API smoke test found issues. Review the output above before sharing public API instructions."
    fi

    mark_step_complete "public_api_check"
}

function run_step() {
    local step="$1"
    local force="$2"

    echo ""
    echo "=============================================="
    echo "  $(step_label "$step")"
    echo "=============================================="

    case "$step" in
        unzip_install)
            if command -v unzip >/dev/null 2>&1; then
                echo "unzip already installed."
            else
                sudo apt-get install -y unzip
            fi
            mark_step_complete "unzip_install"
            ;;
        miniconda)
            install_miniconda
            ;;
        postgres)
            install_postgres
            ;;
        rabbitmq)
            install_rabbitmq
            ;;
        conda_env)
            setup_conda_env "$force"
            ;;
        env_file)
            generate_env_file
            ;;
        collectstatic)
            collect_static_files
            ;;
        django_migrations)
            run_django_migrations
            ;;
        seed_data)
            load_seed_data "$force"
            ;;
        superuser)
            ensure_django_superuser
            ;;
        gee_configuration)
            optional_configure_gee_account "$force"
            ;;
        admin_boundary_data)
            download_admin_boundary_data "$force"
            ;;
        initialisation_check)
            run_post_install_initialisation_check
            ;;
        public_api_check)
            run_public_api_smoke_test
            ;;
        *)
            echo "Unknown step: $step"
            exit 1
            ;;
    esac
}

function print_selection_summary() {
    local selected_steps=()
    local step=""

    for step in "${STEP_ORDER[@]}"; do
        if should_execute_step "$step"; then
            selected_steps+=("$step")
        fi
    done

    echo "Selected steps:"
    for step in "${selected_steps[@]}"; do
        echo "  - $step"
    done
}

function main() {
    local step=""
    local force=0

    parse_args "$@"

    if [ "$LIST_STEPS_ONLY" -eq 1 ]; then
        print_available_steps
        return 0
    fi

    prompt_for_optional_inputs
    ensure_dirs
    print_selection_summary
    print_optional_input_summary

    for step in "${STEP_ORDER[@]}"; do
        if ! should_execute_step "$step"; then
            continue
        fi

        force=0
        if step_is_forced "$step"; then
            force=1
        fi

        run_step "$step" "$force"
    done

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

    if [ "${POST_INSTALL_INITIALISATION_FAILED:-0}" -eq 1 ]; then
        echo "All done, but post-install validation found issues that still need attention."
    else
        echo "All done! Setup is fully complete."
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
