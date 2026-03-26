#!/bin/bash
# script to install

set -e

# === CONFIGURATION ===
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="corestackenv"
CONDA_ENV_YAML="environment.yml"
BACKEND_GIT_REPO="https://github.com/core-stack-org/core-stack-backend.git"
BACKEND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
POSTGRES_USER="corestack_admin"
POSTGRES_DB="corestack_db"
POSTGRES_PASSWORD="corestack@123"
APACHE_CONF="/etc/apache2/sites-available/corestack.conf"
SHELL_RC="$HOME/.bashrc"  # Change to .zshrc if using zsh

# === ENV FILE CONFIGURATION ===
# Map script config variables to .env variable names
# These will override blank values in the generated .env
ENV_DB_NAME="$POSTGRES_DB"
ENV_DB_USER="$POSTGRES_USER"
ENV_DB_PASSWORD="$POSTGRES_PASSWORD"

# === FUNCTIONS ===

function install_miniconda() {
    if command -v conda &> /dev/null; then
        MINICONDA_DIR="$(conda info --base)"
        echo "Conda already available ($(conda --version)) at $MINICONDA_DIR. Skipping Miniconda install."
        return
    fi
    if [ -d "$MINICONDA_DIR" ]; then
        echo "Miniconda found at $MINICONDA_DIR but not on PATH. Sourcing it..."
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
        MINICONDA_DIR="$(conda info --base)"
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
        source "$MINICONDA_DIR/etc/profile.d/conda.sh" 2>/dev/null || true
    fi
    if ! command -v conda &> /dev/null; then
        echo "Conda still not found. Exiting."
        exit 1
    fi
    MINICONDA_DIR="$(conda info --base)"
}

function setup_conda_env() {
    ensure_conda
    echo "Setting up conda environment '$CONDA_ENV_NAME'..."
    conda env remove -n "$CONDA_ENV_NAME" -y || true
    conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"
    echo "Conda environment ready."
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

function install_apache() {
    echo "Installing Apache and mod_wsgi..."
    sudo apt-get update
    sudo apt-get install -y apache2 libapache2-mod-wsgi-py3
    sudo a2enmod wsgi
    sudo systemctl restart apache2
    echo "Apache installed."
}

function clone_backend() {
    if [ -d "$BACKEND_DIR/.git" ]; then
        echo "Backend exists as git repo. Pulling latest changes..."
        sudo git -C "$BACKEND_DIR" pull
    else
        if [ -d "$BACKEND_DIR" ]; then
            echo "Backend exists but not a git repo. Skipping clone."
        else
            echo "Cloning backend..."
            sudo mkdir -p /var/www/data
            sudo git clone "$BACKEND_GIT_REPO" "$BACKEND_DIR"
        fi
    fi
    sudo chown -R www-data:www-data $BACKEND_DIR
    sudo chmod -R 755 $BACKEND_DIR
}

function setup_logs_dir() {
    echo "Setting up logs directory..."
    sudo mkdir -p $BACKEND_DIR/logs
    sudo touch $BACKEND_DIR/logs/nrm_app.log
    sudo chmod -R 755 $BACKEND_DIR/logs
    echo "✅ Logs directory ready."
}

function run_manage_command() {
    # Run any Django manage.py command as www-data
    local cmd="$1"
    bash -c "
    source $MINICONDA_DIR/etc/profile.d/conda.sh
    conda activate $CONDA_ENV_NAME
    cd $BACKEND_DIR
    python manage.py $cmd --skip-checks
    "
}

function collect_static_files() {
    echo "Collecting static files..."
    run_manage_command "collectstatic --noinput"
    echo "Static files collected."
}

function run_migrations() {
    echo "Running Django migrations..."
    run_manage_command "migrate --noinput"
    echo "Migrations applied."
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

}

function configure_apache() {
    echo "Configuring Apache..."

    sudo bash -c "cat > $APACHE_CONF" <<EOL
<VirtualHost *:80>
    ServerName localhost

    WSGIDaemonProcess corestack python-home=$MINICONDA_DIR/envs/$CONDA_ENV_NAME python-path=$BACKEND_DIR
    WSGIProcessGroup corestack
    WSGIScriptAlias / $BACKEND_DIR/nrm_app/wsgi.py

    # Django specific
    WSGIApplicationGroup %{GLOBAL}
    WSGIPassAuthorization On

    <Directory $BACKEND_DIR/nrm_app>
        <Files wsgi.py>
            Require all granted
        </Files>
    </Directory>

    Alias /static $BACKEND_DIR/static
    <Directory $BACKEND_DIR/static>
        Require all granted
    </Directory>

    Alias /media $BACKEND_DIR/media
    <Directory $BACKEND_DIR/media>
        Require all granted
    </Directory>

    ErrorLog \${APACHE_LOG_DIR}/corestack_error.log
    CustomLog \${APACHE_LOG_DIR}/corestack_access.log combined
</VirtualHost>
EOL

    sudo a2ensite corestack.conf
    sudo systemctl reload apache2
    echo "Apache configured."
}

function generate_env_file() {

    echo "Generating .env file from settings.py..."

    local SETTINGS_FILE="$BACKEND_DIR/nrm_app/settings.py"
    local ENV_FILE="$BACKEND_DIR/.env"

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

    # Generate and append FERNET_KEY
    local FERNET_KEY
    FERNET_KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64 | tr +/ -_)
    if grep -q '^FERNET_KEY=' "$ENV_FILE"; then
        sed -i "s|^FERNET_KEY=.*|FERNET_KEY=$FERNET_KEY|" "$ENV_FILE"
    else
        echo "FERNET_KEY=$FERNET_KEY" >> "$ENV_FILE"
    fi
    echo "FERNET_KEY generated and added to .env"

    # Copy .env to nrm_app directory
    cp "$ENV_FILE" "$BACKEND_DIR/nrm_app/.env"
    echo ".env copied to $BACKEND_DIR/nrm_app/.env"


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
}

create_django_superuser() {

    echo ""
    echo "Create Django superuser"
    
    source "$MINICONDA_DIR/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"

    cd "$BACKEND_DIR"

    python manage.py createsuperuser --skip-checks

}

install_geoserver_on_tomcat() {

    echo "Installing Java..."
    sudo apt-get update
    sudo apt-get install -y openjdk-17-jdk

    echo "Installing Tomcat..."
    sudo apt-get install -y tomcat10 tomcat10-admin

    echo "Starting Tomcat..."
    sudo systemctl enable tomcat10
    sudo systemctl start tomcat10

    echo "Downloading GeoServer..."

    GEOSERVER_VERSION="2.24.2"
    TMP_DIR="/tmp/geoserver_install"

    mkdir -p "$TMP_DIR"
    cd "$TMP_DIR"

    wget https://sourceforge.net/projects/geoserver/files/GeoServer/${GEOSERVER_VERSION}/geoserver-${GEOSERVER_VERSION}-war.zip

    unzip geoserver-${GEOSERVER_VERSION}-war.zip

    echo "Deploying GeoServer to Tomcat..."

    sudo cp geoserver.war /var/lib/tomcat10/webapps/

    echo "Restarting Tomcat..."
    sudo systemctl restart tomcat10

    echo "GeoServer installation complete."

    echo "Access GeoServer at:"
    echo "http://localhost:8080/geoserver"

    echo "Default credentials:"
    echo "Username: admin"
    echo "Password: geoserver"

}

function ensure_dirs() {
    mkdir -p "$BACKEND_DIR/logs"
    touch "$BACKEND_DIR/logs/app.log" "$BACKEND_DIR/logs/nrm_app.log"
    mkdir -p "$BACKEND_DIR/data/activated_locations"
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
}

function download_admin_boundary_data() {
    local admin_boundary_dir="$BACKEND_DIR/data/admin-boundary"
    if [ -d "$admin_boundary_dir/input" ] && [ "$(ls -A "$admin_boundary_dir/input" 2>/dev/null)" ]; then
        echo "Admin boundary data already exists at $admin_boundary_dir. Skipping download."
        return
    fi
    mkdir -p "$BACKEND_DIR/data"
    echo "Downloading admin boundary data (~8GB, this may take a while)..."
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

# === MAIN ===
sudo apt-get install -y unzip
ensure_dirs
install_miniconda
ensure_conda
install_postgres
install_rabbitmq
setup_conda_env
generate_env_file
collect_static_files
reset_django_migrations
run_django_migrations
load_seed_data
create_django_superuser
#install_geoserver_on_tomcat

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
echo "=============================================="
echo "  Admin boundary data (~8GB) is required."
echo "=============================================="
echo ""
echo "1) Download now (will take a while)"
echo "2) Skip (I will download it manually later)"
echo ""
read -p "Enter choice [1/2]: " admin_boundary_choice

case "$admin_boundary_choice" in
    1)
        echo ""
        echo "Downloading admin boundary data. Please be patient..."
        echo ""
        download_admin_boundary_data
        if [ -n "$GDOWN_PID" ]; then
            wait "$GDOWN_PID"
            echo ""
            echo "Admin boundary data download and extraction complete."
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
echo "All done! Setup is fully complete."
