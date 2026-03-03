#!/bin/bash

set -e

# ==============================
# CONFIGURATION
# ==============================

TEST_MODE=true

MINICONDA_DIR="/opt/miniconda3"
CONDA_ENV_NAME="corestack-backend"

BACKEND_GIT_REPO="https://github.com/core-stack-org/core-stack-backend.git"
BACKEND_DIR="/var/www/data/corestack"

CONDA_ENV_YAML="$BACKEND_DIR/installation/environment.yml"

POSTGRES_USER="nrm"
POSTGRES_DB="nrm"
POSTGRES_PASSWORD="nrm@123"

APACHE_CONF="/etc/apache2/sites-available/corestack.conf"

# ==============================
# SYSTEM PACKAGES
# ==============================

function install_system_packages() {
    echo "Installing system packages..."
    sudo apt-get update
    sudo apt-get install -y \
        wget curl git acl \
        build-essential \
        apache2 libapache2-mod-wsgi-py3 \
        postgresql postgresql-contrib libpq-dev
}

# ==============================
# MINICONDA
# ==============================

function install_miniconda() {
    if [ -d "$MINICONDA_DIR" ]; then
        echo "Miniconda already installed."
        return
    fi

    echo "Installing Miniconda in /opt..."

    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    sudo bash miniconda.sh -b -p "$MINICONDA_DIR"
    rm miniconda.sh

    sudo chmod -R 755 "$MINICONDA_DIR"

    echo "Miniconda installed."
}

# ==============================
# CLONE BACKEND (TEST SAFE)
# ==============================

function clone_backend() {

    if [ -d "$BACKEND_DIR" ]; then
        echo "Backend already exists. Skipping git clone (TEST MODE)."
        return
    fi

    echo "Cloning backend (first run only)..."

    sudo mkdir -p /var/www/data
    sudo git clone "$BACKEND_GIT_REPO" "$BACKEND_DIR"

    # Give YOU ownership in test mode
    sudo chown -R $USER:$USER "$BACKEND_DIR"
    sudo chmod -R 755 "$BACKEND_DIR"
}

# ==============================
# CONDA ENVIRONMENT
# ==============================

function setup_conda_env() {

    echo "Setting up Conda environment..."

    if [ ! -f "$CONDA_ENV_YAML" ]; then
        echo "ERROR: environment.yml not found at:"
        echo "$CONDA_ENV_YAML"
        exit 1
    fi

    source "$MINICONDA_DIR/etc/profile.d/conda.sh"

    # Remove only environment (not miniconda)
    conda env remove -n "$CONDA_ENV_NAME" -y || true

    conda clean --all -y || true

    conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"

    echo "Conda environment ready."
}

# ==============================
# POSTGRESQL
# ==============================

function setup_postgres() {
    echo "Configuring PostgreSQL..."

    sudo systemctl start postgresql || true

    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='$POSTGRES_USER'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';"

    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='$POSTGRES_DB'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"

    echo "PostgreSQL ready."
}

# ==============================
# LOGS DIRECTORY
# ==============================

function setup_logs_dir() {
    echo "Creating logs directory..."
    mkdir -p "$BACKEND_DIR/logs"
    touch "$BACKEND_DIR/logs/nrm_app.log"
    echo "Logs directory ready."
}

# ==============================
# DJANGO COMMANDS
# ==============================

function run_manage_command() {
    local cmd="$1"

    "$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python" \
        "$BACKEND_DIR/manage.py" $cmd
}

function collect_static_files() {
    echo "Collecting static files..."
    run_manage_command "collectstatic --noinput"
}

function run_migrations() {
    echo "Running migrations..."
    run_manage_command "migrate --noinput"
}

# ==============================
# APACHE CONFIG (Optional)
# ==============================

function configure_apache() {

    echo "Configuring Apache..."

    sudo bash -c "cat > $APACHE_CONF" <<EOF
<VirtualHost *:80>
    ServerName localhost

    WSGIDaemonProcess corestack \
        python-home=$MINICONDA_DIR/envs/$CONDA_ENV_NAME \
        python-path=$BACKEND_DIR

    WSGIProcessGroup corestack
    WSGIScriptAlias / $BACKEND_DIR/nrm_app/wsgi.py

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
</VirtualHost>
EOF

    sudo a2enmod wsgi
    sudo a2ensite corestack.conf
    sudo systemctl restart apache2
}

# ==============================
# MAIN EXECUTION
# ==============================

install_system_packages
install_miniconda
clone_backend
setup_conda_env
setup_postgres
setup_logs_dir
collect_static_files
run_migrations

echo ""
echo "======================================"
echo "✅ TEST MODE Deployment Complete!"
echo "You can now run:"
echo ""
echo "$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python $BACKEND_DIR/manage.py runserver"
echo ""
echo "Or open: http://localhost"
echo "======================================"