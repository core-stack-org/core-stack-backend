#!/bin/bash

set -e

# ==========================================
# CONFIGURATION
# ==========================================
MINICONDA_DIR="$HOME/miniconda3"
CONDA_ENV_NAME="corestack-backend"
CONDA_ENV_YAML="environment.yml"
BACKEND_GIT_REPO="https://github.com/core-stack-org/core-stack-backend.git"

# Detect script location and set BACKEND_DIR dynamically
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# If script is in installation/ directory, BACKEND_DIR is the parent directory
if [ "$(basename "$SCRIPT_DIR")" = "installation" ]; then
    BACKEND_DIR="$(dirname "$SCRIPT_DIR")"
else
    # Fallback to default if not in installation/ directory
    BACKEND_DIR="/var/www/data/corestack"
fi

# Verify BACKEND_DIR contains manage.py
if [ ! -f "$BACKEND_DIR/manage.py" ]; then
    echo "Warning: manage.py not found in $BACKEND_DIR"
    echo "Using default backend directory: /var/www/data/corestack"
    BACKEND_DIR="/var/www/data/corestack"
fi

POSTGRES_USER="nrm"
POSTGRES_DB="nrm"
POSTGRES_PASSWORD="nrm@123"
APACHE_CONF="/etc/apache2/sites-available/corestack.conf"
SHELL_RC="$HOME/.bashrc"  # Change to .zshrc if using zsh

# ==========================================
# FUNCTIONS
# ==========================================

function install_miniconda() {
    if [ -d "$MINICONDA_DIR" ]; then
        echo "✅ Miniconda already installed at $MINICONDA_DIR"
    else
        echo "Installing Miniconda..."
        wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
        bash miniconda.sh -b -p "$MINICONDA_DIR"
        rm miniconda.sh
        echo "✅ Miniconda installed."
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
        echo "❌ Conda still not found. Exiting."
        exit 1
    fi
}

function setup_conda_env() {
    ensure_conda
    echo "Checking conda environment '$CONDA_ENV_NAME'..."
    
    if conda env list | grep -q "^${CONDA_ENV_NAME} "; then
        echo "✅ Conda environment '$CONDA_ENV_NAME' already exists."
        read -p "Do you want to recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Removing existing environment..."
            conda env remove -n "$CONDA_ENV_NAME" -y
            echo "Creating new environment..."
            conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"
        else
            echo "Keeping existing environment."
        fi
    else
        echo "Creating new environment..."
        conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"
    fi
    echo "✅ Conda environment ready."
}

function install_postgres() {
    if command -v psql &> /dev/null && systemctl is-active --quiet postgresql 2>/dev/null; then
        echo "✅ PostgreSQL already installed and running."
    else
        echo "Installing PostgreSQL..."
        sudo apt-get update
        sudo apt-get install -y postgresql postgresql-contrib libpq-dev
        echo "✅ PostgreSQL installed."
    fi
    echo "Setting up PostgreSQL user/database..."
    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname = '$POSTGRES_USER'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 || \
        sudo -u postgres psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"
    echo "✅ PostgreSQL ready."
}

function install_apache() {
    if command -v apache2 &> /dev/null && systemctl is-active --quiet apache2 2>/dev/null; then
        echo "✅ Apache already installed and running."
    else
        echo "Installing Apache and mod_wsgi..."
        sudo apt-get update
        sudo apt-get install -y apache2 libapache2-mod-wsgi-py3
        sudo a2enmod wsgi
        sudo systemctl restart apache2
        echo "✅ Apache installed."
    fi
}

function clone_backend() {
    if [ -d "$BACKEND_DIR/.git" ]; then
        echo "Backend exists as git repo at $BACKEND_DIR."
        # Handle git ownership issue for newer git versions
        git config --global --add safe.directory "$BACKEND_DIR" 2>/dev/null || true
        
        # Check for local changes
        if git -C "$BACKEND_DIR" status --porcelain | grep -q .; then
            echo "⚠️  You have local changes in $BACKEND_DIR"
            echo ""
            echo "Local changes:"
            git -C "$BACKEND_DIR" status --short
            echo ""
            echo "Choose an option:"
            echo "1) Stash local changes, pull, then restore stash (recommended)"
            echo "2) Discard local changes and pull"
            echo "3) Skip pull and keep local changes"
            echo "4) Abort installation"
            echo ""
            read -p "Enter your choice (1-4): " -n 1 -r
            echo
            
            case $REPLY in
                1)
                    echo "Stashing local changes..."
                    git -C "$BACKEND_DIR" stash push -m "temp stash before pulling on $(date)"
                    echo "Pulling latest changes..."
                    git -C "$BACKEND_DIR" pull
                    echo "Restoring stashed changes..."
                    git -C "$BACKEND_DIR" stash pop
                    echo "✅ Local changes restored"
                    ;;
                2)
                    echo "Discarding local changes..."
                    git -C "$BACKEND_DIR" reset --hard HEAD
                    git -C "$BACKEND_DIR" clean -fd
                    echo "Pulling latest changes..."
                    git -C "$BACKEND_DIR" pull
                    echo "✅ Latest changes pulled"
                    ;;
                3)
                    echo "Skipping pull. Keeping local changes."
                    ;;
                4)
                    echo "Aborting installation."
                    exit 1
                    ;;
                *)
                    echo "❌ Invalid choice. Aborting installation."
                    exit 1
                    ;;
            esac
        else
            echo "No local changes. Pulling latest changes..."
            git -C "$BACKEND_DIR" pull
        fi
    else
        if [ -d "$BACKEND_DIR" ]; then
            echo "Backend exists at $BACKEND_DIR but not a git repo. Skipping clone."
        else
            echo "Cloning backend..."
            mkdir -p "$(dirname "$BACKEND_DIR")"
            git clone "$BACKEND_GIT_REPO" "$BACKEND_DIR"
        fi
    fi
    # Only set ownership if BACKEND_DIR is in /var/www/data (production deployment)
    if [[ "$BACKEND_DIR" == /var/www/data/* ]]; then
        sudo chown -R www-data:www-data $BACKEND_DIR
        sudo chmod -R 755 $BACKEND_DIR
    fi
}

function setup_logs_dir() {
    echo "Setting up logs directory..."
    mkdir -p $BACKEND_DIR/logs
    touch $BACKEND_DIR/logs/nrm_app.log
    # Only set ownership if BACKEND_DIR is in /var/www/data (production deployment)
    if [[ "$BACKEND_DIR" == /var/www/data/* ]]; then
        sudo chown -R www-data:www-data $BACKEND_DIR/logs
        sudo chmod -R 755 $BACKEND_DIR/logs
    else
        chmod -R 755 $BACKEND_DIR/logs
    fi
    echo "✅ Logs directory ready."
}

function run_manage_command() {
    # Run any Django manage.py command as current user (not www-data for local development)
    local cmd="$1"
    bash -c "
    source $MINICONDA_DIR/etc/profile.d/conda.sh
    conda activate $CONDA_ENV_NAME
    cd $BACKEND_DIR
    python manage.py $cmd
    "
}

function collect_static_files() {
    echo "Collecting static files..."
    run_manage_command "collectstatic --noinput"
    echo "✅ Static files collected."
}

function setup_migrations() {
    echo ""
    echo "=========================================="
    echo "Migration Setup Options"
    echo "=========================================="
    echo "1) Fully automated (recommended for new installations)"
    echo "2) Manual (use individual scripts)"
    echo "3) Skip (for existing setups)"
    echo ""
    read -p "Enter your choice (1-3): " -n 1 -r
    echo
    
    case $REPLY in
        1)
            echo "Setting up migrations automatically..."
            # Ensure conda.sh is readable by current user
            sudo chmod +r $MINICONDA_DIR/etc/profile.d/conda.sh 2>/dev/null || true
            # Run setup_migrations.sh to generate migrations
            echo ""
            echo "Step 1: Generating migrations..."
            bash -c "
            source $MINICONDA_DIR/etc/profile.d/conda.sh
            conda activate $CONDA_ENV_NAME
            cd $BACKEND_DIR
            bash $SCRIPT_DIR/setup_migrations.sh
            "
            # Run apply_migrations.sh to apply migrations
            echo ""
            echo "Step 2: Applying migrations to database..."
            bash -c "
            source $MINICONDA_DIR/etc/profile.d/conda.sh
            conda activate $CONDA_ENV_NAME
            cd $BACKEND_DIR
            bash $SCRIPT_DIR/apply_migrations.sh
            "
            # Run verify_migrations.sh to verify
            echo ""
            echo "Step 3: Verifying migrations..."
            bash -c "
            source $MINICONDA_DIR/etc/profile.d/conda.sh
            conda activate $CONDA_ENV_NAME
            cd $BACKEND_DIR
            bash $SCRIPT_DIR/verify_migrations.sh
            "
            echo ""
            echo "✅ Migrations generated, applied, and verified successfully!"
            echo ""
            echo "Next steps:"
            echo "  1. Create a superuser: python manage.py createsuperuser"
            echo "  2. Start the server: python manage.py runserver"
            echo ""
            ;;
        2)
            echo "Manual migration setup selected."
            echo "Please run the following scripts in order:"
            echo ""
            echo "Step 1: Generate migrations"
            echo "  cd $BACKEND_DIR && bash $SCRIPT_DIR/setup_migrations.sh"
            echo ""
            echo "Step 2: Apply migrations"
            echo "  cd $BACKEND_DIR && bash $SCRIPT_DIR/apply_migrations.sh"
            echo ""
            echo "Step 3: Verify migrations"
            echo "  cd $BACKEND_DIR && bash $SCRIPT_DIR/verify_migrations.sh"
            echo ""
            echo "Step 4: Create superuser (optional but recommended)"
            echo "  cd $BACKEND_DIR && python manage.py createsuperuser"
            echo ""
            read -p "Press Enter to continue after completing manual migrations..."
            echo ""
            ;;
        3)
            echo "Skipping migration setup."
            ;;
        *)
            echo "❌ Invalid choice. Skipping migration setup."
            ;;
    esac
}

function configure_apache() {
    # Only configure Apache if BACKEND_DIR is in /var/www/data (production deployment)
    if [[ "$BACKEND_DIR" != /var/www/data/* ]]; then
        echo "Skipping Apache configuration (not in production deployment mode)."
        echo "Backend directory: $BACKEND_DIR"
        echo "To configure Apache for production, set BACKEND_DIR to /var/www/data/corestack"
        return
    fi

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
    echo "✅ Apache configured."
}

# ==========================================
# MAIN INSTALLATION FLOW
# ==========================================

echo ""
echo "=========================================="
echo "Core Stack Backend Installation"
echo "=========================================="
echo ""

install_miniconda
ensure_conda
install_postgres
install_apache
setup_conda_env
clone_backend
setup_logs_dir
setup_migrations
collect_static_files
configure_apache

echo ""
echo "=========================================="
echo "✅ Deployment complete!"
echo "=========================================="
echo ""
echo "Backend directory: $BACKEND_DIR"
echo ""
if [[ "$BACKEND_DIR" == /var/www/data/* ]]; then
    echo "Visit: http://localhost"
    echo ""
    echo "Apache serves /, /static, and /media automatically."
else
    echo "To start the development server:"
    echo "  cd $BACKEND_DIR"
    echo "  conda activate $CONDA_ENV_NAME"
    echo "  python manage.py runserver"
    echo ""
    echo "Then visit: http://localhost:8000"
fi
echo ""
echo "To activate environment:"
echo "  conda activate $CONDA_ENV_NAME"
echo ""
