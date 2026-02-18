#script to install

#!/bin/bash

set -e

# === CONFIGURATION ===
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="corestack-backend"
CONDA_ENV_YAML="environment.yml"
BACKEND_GIT_REPO="https://github.com/core-stack-org/core-stack-backend.git"
BACKEND_DIR="/var/www/data/corestack"
POSTGRES_USER="nrm"
POSTGRES_DB="nrm"
POSTGRES_PASSWORD="nrm@123"
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
    if [ -d "$MINICONDA_DIR" ]; then
        echo "Miniconda already installed at $MINICONDA_DIR"
    else
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
    fi
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
}

function install_postgres() {
    echo "Installing PostgreSQL..."
    sudo apt-get update
    sudo apt-get install -y postgresql postgresql-contrib libpq-dev
    echo "Setting up PostgreSQL user/database..."
    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname = '$POSTGRES_USER'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"
    echo "PostgreSQL ready."
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
    sudo chown -R www-data:www-data $BACKEND_DIR/logs
    sudo chmod -R 755 $BACKEND_DIR/logs
    echo "✅ Logs directory ready."
}

function run_manage_command() {
    # Run any Django manage.py command as www-data
    local cmd="$1"
    sudo -u www-data bash -c "
    source $MINICONDA_DIR/etc/profile.d/conda.sh
    conda activate $CONDA_ENV_NAME
    cd $BACKEND_DIR
    python manage.py $cmd
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
    local ENV_FILE="$BACKEND_DIR/nrm_app/.env"
    
    if [ ! -f "$SETTINGS_FILE" ]; then
        echo "ERROR: settings.py not found at $SETTINGS_FILE"
        return 1
    fi
    
    # Extract all env() calls from settings.py
    # Pattern matches: env("VAR_NAME") or env.str("VAR_NAME") or env.bool("VAR_NAME", default=...) etc.
    # Also matches: env("VAR_NAME", default=...)
    
    echo "# Auto-generated .env file from settings.py" > "$ENV_FILE"
    echo "# Generated on $(date)" >> "$ENV_FILE"
    echo "" >> "$ENV_FILE"
    
    # Extract variable names using grep and sed
    # This regex matches env("VAR_NAME") patterns and captures VAR_NAME
    local env_vars
    env_vars=$(grep -oE 'env\.[a-z]*\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$SETTINGS_FILE" 2>/dev/null | \
               sed -E 's/env\.[a-z]*\s*\(\s*"([^"]+)"/\1/' | \
               sort -u)
    
    # Also capture simple env("VAR_NAME") calls
    local env_vars_simple
    env_vars_simple=$(grep -oE 'env\s*\(\s*"[A-Za-z_][A-Za-z0-9_]*"' "$SETTINGS_FILE" 2>/dev/null | \
                      sed -E 's/env\s*\(\s*"([^"]+)"/\1/' | \
                      sort -u)
    
    # Combine and deduplicate
    local all_vars
    all_vars=$(echo -e "${env_vars}\n${env_vars_simple}" | sort -u | grep -v '^$')
    
    # Write each variable with blank value to .env
    while IFS= read -r var_name; do
        if [ -n "$var_name" ]; then
            echo "${var_name}=\"\"" >> "$ENV_FILE"
        fi
    done <<< "$all_vars"
    
    echo "" >> "$ENV_FILE"
    echo "# === CONFIGURATION OVERRIDES ===" >> "$ENV_FILE"
    
    # Override with values from script configuration
    # DB settings mapping
    if [ -n "$ENV_DB_NAME" ]; then
        sed -i "s|^DB_NAME=\"\"|DB_NAME=\"$ENV_DB_NAME\"|" "$ENV_FILE"
        echo "# DB_NAME overridden from POSTGRES_DB config" >> "$ENV_FILE"
    fi
    
    if [ -n "$ENV_DB_USER" ]; then
        sed -i "s|^DB_USER=\"\"|DB_USER=\"$ENV_DB_USER\"|" "$ENV_FILE"
        echo "# DB_USER overridden from POSTGRES_USER config" >> "$ENV_FILE"
    fi
    
    if [ -n "$ENV_DB_PASSWORD" ]; then
        sed -i "s|^DB_PASSWORD=\"\"|DB_PASSWORD=\"$ENV_DB_PASSWORD\"|" "$ENV_FILE"
        echo "# DB_PASSWORD overridden from POSTGRES_PASSWORD config" >> "$ENV_FILE"
    fi
    
    # Set proper permissions
    sudo chown www-data:www-data "$ENV_FILE"
    sudo chmod 640 "$ENV_FILE"
    
    echo "✅ .env file generated at $ENV_FILE"
    echo "   Total variables extracted: $(grep -c '^[A-Za-z_]' "$ENV_FILE")"
}

# === MAIN ===
install_miniconda
ensure_conda
install_postgres
install_apache
setup_conda_env
clone_backend
setup_logs_dir
generate_env_file
collect_static_files
run_migrations
configure_apache

echo ""
echo "Deployment complete!"
echo "Visit: http://localhost"
echo "Activate env: conda activate $CONDA_ENV_NAME"
echo "Apache serves /, /static, and /media automatically."
echo ""
echo "⚠️  IMPORTANT: Review and update the .env file at $BACKEND_DIR/nrm_app/.env"
echo "   with your actual credentials before running in production."
