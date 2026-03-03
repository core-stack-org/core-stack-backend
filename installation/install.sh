#!/bin/bash
set -e

# FORCE USE OF /opt CONDA ONLY
export PATH="/opt/miniconda3/bin:$PATH"

# ==============================
# CONFIGURATION
# ==============================

MINICONDA_DIR="/opt/miniconda3"
CONDA_ENV_NAME="corestack-backend"

BACKEND_GIT_REPO="https://github.com/core-stack-org/core-stack-backend.git"
BACKEND_DIR="/var/www/data/corestack"

CONDA_ENV_YAML="$BACKEND_DIR/installation/environment.yml"

POSTGRES_USER="nrm"
POSTGRES_DB="nrm"
POSTGRES_PASSWORD="nrm@123"

# ==============================
# CLEAN OLD HOME CONDA (CRITICAL FIX)
# ==============================

echo "Removing any old home conda environments..."
rm -rf ~/.conda || true
rm -rf ~/miniconda3 || true

# ==============================
# INSTALL SYSTEM PACKAGES
# ==============================

sudo apt-get update
sudo apt-get install -y wget curl git build-essential \
    postgresql postgresql-contrib libpq-dev

# ==============================
# INSTALL MINICONDA (IF NEEDED)
# ==============================

if [ ! -d "$MINICONDA_DIR" ]; then
    echo "Installing Miniconda in /opt..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    sudo bash miniconda.sh -b -p "$MINICONDA_DIR"
    rm miniconda.sh
    sudo chmod -R 755 "$MINICONDA_DIR"
fi

# ==============================
# CLONE BACKEND (NO OVERWRITE)
# ==============================

if [ ! -d "$BACKEND_DIR" ]; then
    sudo mkdir -p /var/www/data
    sudo git clone "$BACKEND_GIT_REPO" "$BACKEND_DIR"
    sudo chown -R $USER:$USER "$BACKEND_DIR"

    cd "$BACKEND_DIR"
    git checkout features/installation
fi

# ==============================
# REMOVE OLD ENV (CRITICAL)
# ==============================

rm -rf "$MINICONDA_DIR/envs/$CONDA_ENV_NAME" || true

# ==============================
# CREATE CONDA ENV
# ==============================

if [ ! -f "$CONDA_ENV_YAML" ]; then
    echo "ERROR: environment.yml not found!"
    exit 1
fi

conda clean --all -y || true

conda env create -f "$CONDA_ENV_YAML" -n "$CONDA_ENV_NAME"

# ==============================
# VERIFY CELERY VERSION
# ==============================

"$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/pip" show celery

# ==============================
# DONE
# ==============================

echo ""
echo "======================================"
echo "✅ TEST MODE Deployment Complete!"
echo ""
echo "Run:"
echo "$MINICONDA_DIR/envs/$CONDA_ENV_NAME/bin/python $BACKEND_DIR/manage.py runserver"
echo "======================================"