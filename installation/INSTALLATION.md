# CoRE Stack Backend - Installation Guide

This document provides comprehensive installation instructions for the CoRE Stack Backend application using a hybrid approach: manual prerequisites, automated installation script, and manual post-installation configuration.

> **Having issues?** Check the [Troubleshooting Guide](TROUBLESHOOTING.md) for solutions to common problems, or post an issue at [GitHub Issues](https://github.com/core-stack-org/core-stack-backend/issues)

## Table of Contents

- [Quick Start Overview](#quick-start-overview)
- [Prerequisites](#prerequisites)
- [System Requirements](#system-requirements)
- [Step 1: Manual Prerequisites](#step-1-manual-prerequisites)
- [Step 2: Automated Installation](#step-2-automated-installation)
- [Step 3: Manual Post-Installation](#step-3-manual-post-installation)
- [Running the Application](#running-the-application)
- [Installation Summary](#installation-summary)

---

## Quick Start Overview

The installation follows a three-phase approach:

| Phase | Description | Time |
|-------|-------------|------|
| **Phase 1** | Manual prerequisites (RabbitMQ, Git) | ~5 min |
| **Phase 2** | Automated installation script | ~15-20 min |
| **Phase 3** | Manual post-installation (superuser, env vars) | ~10 min |

---

## Prerequisites

Before installing the CoRE Stack Backend, ensure you have:

1. **Ubuntu 24.04 (Noble)** or compatible Linux distribution
2. **sudo** access for installing system packages
3. **Internet connection** for downloading dependencies
4. **Git** installed on your system

---

## System Requirements

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 4 GB | 8+ GB |
| Disk Space | 20 GB | 50+ GB |
| PostgreSQL | Version 15+ | Version 16 |

### Software Requirements

| Software | Version | Notes |
|----------|---------|-------|
| Python | 3.10 | Via Miniconda |
| PostgreSQL | 15+ | With PostGIS support |
| RabbitMQ | 3.12+ | For Celery task queue |
| Apache | 2.4+ | With mod_wsgi |
| Miniconda | Latest | Python environment manager |

---

## Step 1: Manual Prerequisites

These steps must be completed manually before running the automated installation script.

### 1.1 Update System and Install Essential Packages

```bash
# Update package list
sudo apt update

# Install essential packages
sudo apt install -y git wget curl build-essential libpq-dev
```

### 1.2 Install RabbitMQ (Required for Celery)

RabbitMQ is required for Celery task processing and is **not installed by the automated script**:

```bash
# Install RabbitMQ
sudo apt install -y rabbitmq-server

# Start and enable RabbitMQ service
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server

# Verify RabbitMQ is running
sudo systemctl status rabbitmq-server
```

### 1.3 Clone the Repository (Optional - for local development)

If you want to review or modify the installation scripts before running:

```bash
# Navigate to your workspace
cd /path/to/your/workspace

# Clone the backend repository
git clone https://github.com/core-stack-org/core-stack-backend.git

# Navigate into the project directory
cd core-stack-backend
```

---

## Step 2: Automated Installation

Once the prerequisites are ready, run the automated installation script.

### 2.1 Navigate to Installation Directory

```bash
cd /path/to/core-stack-backend/installation
```

### 2.2 Configure Installation (Optional)

You can modify the configuration variables at the top of [`install.sh`](install.sh) before running:

```bash
# Edit the script to customize configuration
nano install.sh
```

**Configurable Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `MINICONDA_DIR` | `$HOME/miniconda3` | Miniconda installation path |
| `CONDA_ENV_NAME` | `corestack-backend` | Conda environment name |
| `BACKEND_DIR` | `/var/www/data/corestack` | Backend deployment directory |
| `POSTGRES_USER` | `nrm` | PostgreSQL username |
| `POSTGRES_DB` | `nrm` | PostgreSQL database name |
| `POSTGRES_PASSWORD` | `nrm@123` | PostgreSQL password |

### 2.3 Run the Installation Script

```bash
# Make the script executable
chmod +x install.sh

# Run the installation script
./install.sh
```

### 2.4 What the Script Automates

The installation script automatically performs the following:

| Step | Description |
|------|-------------|
| Miniconda | Downloads and installs Miniconda |
| Conda Environment | Creates environment from `environment.yml` |
| PostgreSQL | Installs PostgreSQL, creates user and database |
| Apache | Installs Apache with mod_wsgi |
| Backend Clone | Clones repository to `/var/www/data/corestack` |
| Logs Directory | Creates and configures logs directory |
| .env File | Generates `.env` file from `settings.py` |
| Static Files | Runs `collectstatic` |
| Migrations | Runs database migrations |
| Apache Config | Configures Apache virtual host |

### 2.5 Script Output

After successful installation, you'll see:

```
Deployment complete!
Visit: http://localhost
Activate env: conda activate corestack-backend
Apache serves /, /static, and /media automatically.

⚠️  IMPORTANT: Review and update the .env file at /var/www/data/corestack/nrm_app/.env
   with your actual credentials before running in production.
```

---

## Step 3: Manual Post-Installation

These steps must be completed manually after the automated installation.

### 3.1 Configure Environment Variables

The installation script generates a `.env` file with blank values for most variables. You **must** configure these manually:

```bash
# Edit the .env file
sudo nano /var/www/data/corestack/nrm_app/.env
```

**Critical Variables to Configure:**

```env
# Django Settings (REQUIRED)
SECRET_KEY=your-secret-key-here
DEBUG=False

# ODK Credentials (If required)
ODK_USERNAME=your-odk-username
ODK_PASSWORD=your-odk-password
ODK_USER_EMAIL_SYNC=your-email@example.com
ODK_USER_PASSWORD_SYNC=your-sync-password

# Email Settings (If required)
EMAIL_HOST_USER=your-email@example.com
EMAIL_HOST_PASSWORD=your-email-password

# GeoServer Settings (If required)
GEOSERVER_URL=https://geoserver.example.com/geoserver
GEOSERVER_USERNAME=admin
GEOSERVER_PASSWORD=your-geoserver-password

# Google Earth Engine
GEE_SERVICE_ACCOUNT_KEY_PATH=path/to/service-account-key.json
GEE_HELPER_SERVICE_ACCOUNT_KEY_PATH=path/to/helper-key.json
GEE_DATASETS_SERVICE_ACCOUNT_KEY_PATH=path/to/datasets-key.json

# S3 Settings (Not essential for local storage)
S3_BUCKET=your-bucket-name
S3_REGION=ap-south-1
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key

# DPR S3 Settings (Not essential for local storage)
DPR_S3_BUCKET=your-dpr-bucket
DPR_S3_FOLDER=your-folder
DPR_S3_ACCESS_KEY=your-access-key
DPR_S3_SECRET_KEY=your-secret-key
DPR_S3_REGION=ap-south-1

# Other Services (As per need)
AUTH_TOKEN_360=your-360dialog-token
FERNET_KEY=your-fernet-key
TMP_LOCATION=/tmp
DEPLOYMENT_DIR=/var/www/data/corestack
```

**Generate a Secret Key:**

```bash
# Generate a random secret key
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```

**Generate a Fernet Key:**

```bash
# Generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3.2 Create Superuser

Create an admin user for the Django admin interface:

```bash
# Activate conda environment
conda activate corestack-backend

# Navigate to project directory
cd /var/www/data/corestack

# Create superuser
python manage.py createsuperuser
```

### 3.3 Configure GEE Service Account Keys

Place your Google Earth Engine service account JSON keys:

```bash
# Create directory for GEE keys
sudo mkdir -p /var/www/data/corestack/data/gee_confs

# Copy your service account keys
sudo cp ~/path/to/your-gee-key.json /var/www/data/corestack/data/gee_confs/

# Set permissions
sudo chown -R www-data:www-data /var/www/data/corestack/data/gee_confs
sudo chmod 750 /var/www/data/corestack/data/gee_confs/*.json
```

### 3.4 Restart Services

After configuring environment variables:

```bash
# Restart Apache to pick up new environment
sudo systemctl restart apache2

# Verify Apache is running
sudo systemctl status apache2
```

### 3.5 Database Restore (Optional)

If you have a backup SQL file to restore:

```bash
# Restore database from SQL file
psql -h localhost -U nrm -d nrm -f /path/to/backup.sql
```

---

## Running the Application

### Production Mode (Apache)

After installation, the application is served by Apache at `http://localhost/`.

```bash
# Check Apache status
sudo systemctl status apache2

# View error logs
sudo tail -f /var/log/apache2/corestack_error.log

# View access logs
sudo tail -f /var/log/apache2/corestack_access.log
```

### Development Mode

For development, you can run the Django development server:

```bash
# Activate environment
conda activate corestack-backend

# Navigate to project
cd /var/www/data/corestack

# Run development server
python manage.py runserver 0.0.0.0:8000
```

The server will be available at `http://127.0.0.1:8000/`

### Running Celery Worker

For asynchronous task processing:

```bash
# Activate environment
conda activate corestack-backend

# Navigate to project
cd /var/www/data/corestack

# Start Celery worker
celery -A nrm_app worker -l info -Q nrm
```

### Running Both Services (Development)

For development, you'll need two terminal sessions:

**Terminal 1 - Django Server:**
```bash
conda activate corestack-backend
cd /var/www/data/corestack
python manage.py runserver
```

**Terminal 2 - Celery Worker:**
```bash
conda activate corestack-backend
cd /var/www/data/corestack
celery -A nrm_app worker -l info -Q nrm
```

---

## Installation Summary

### Automated Steps (install.sh)

- [x] Install Miniconda
- [x] Create Conda environment
- [x] Install PostgreSQL
- [x] Install Apache with mod_wsgi
- [x] Clone backend repository
- [x] Setup logs directory
- [x] Generate .env file template
- [x] Collect static files
- [x] Run database migrations
- [x] Configure Apache virtual host

### Manual Steps (Required)

- [ ] Install RabbitMQ
- [ ] Configure environment variables in `.env`
- [ ] Create Django superuser
- [ ] Configure GEE service account keys
- [ ] Restart Apache after configuration

---

## Additional Resources

- [Troubleshooting Guide](TROUBLESHOOTING.md) - Solutions to common installation issues
- [GitHub Issues](https://github.com/core-stack-org/core-stack-backend/issues) - Report bugs or request help
- [Django Documentation](https://docs.djangoproject.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Celery Documentation](https://docs.celeryproject.org/)
- [Conda Documentation](https://docs.conda.io/)
- [Apache mod_wsgi Documentation](https://modwsgi.readthedocs.io/)

---

> **Need help?** If you encounter any issues during installation or to start with this project, please refer to the [Troubleshooting Guide](TROUBLESHOOTING.md) or post an issue at [GitHub Issues](https://github.com/core-stack-org/core-stack-backend/issues)
