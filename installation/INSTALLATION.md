# CoRE Stack Backend - Installation Guide

This document provides comprehensive installation instructions for the CoRE Stack Backend application. It is based on the actual installation process documented in the setup log.

## Table of Contents

- [Prerequisites](#prerequisites)
- [System Requirements](#system-requirements)
- [Installation Steps](#installation-steps)
- [Configuration](#configuration)
- [Post-Installation Setup](#post-installation-setup)
- [Running the Application](#running-the-application)

---

## Prerequisites

Before installing the CoRE Stack Backend, ensure you have:

1. **Ubuntu 24.04 (Noble)** or compatible Linux distribution
2. **sudo** access for installing system packages
3. **Internet connection** for downloading dependencies
4. **Git** installed on your system

### Required System Packages

The following system packages are required:

```bash
# Update package list
sudo apt update

# Install essential packages
sudo apt install -y \
    git \
    wget \
    curl \
    build-essential \
    libpq-dev

# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Install Apache and mod_wsgi
sudo apt install -y apache2 libapache2-mod-wsgi-py3

# Install RabbitMQ (required for Celery)
sudo apt install -y rabbitmq-server
```

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

## Installation Steps

### Step 1: Clone the Repository

```bash
# Navigate to your workspace
cd /mnt/y/core-stack-org

# Clone the backend repository
git clone https://github.com/core-stack-org/core-stack-backend.git

# Navigate into the project directory
cd core-stack-backend
```

### Step 2: Install Miniconda

If Miniconda is not already installed:

```bash
# Download Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh

# Install Miniconda
bash miniconda.sh -b -p $HOME/miniconda3

# Add to PATH
export PATH="$HOME/miniconda3/bin:$PATH"

# Initialize conda for bash
source $HOME/miniconda3/etc/profile.d/conda.sh

# Add to .bashrc permanently
echo "source $HOME/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc
```

### Step 3: Set Up Conda Environment

```bash
# Navigate to installation directory
cd installation

# Create the conda environment from environment.yml
conda env create -f environment.yml -n corestack-backend

# Activate the environment
conda activate corestack-backend
```

**Note:** The environment creation may take 10-15 minutes as it installs many dependencies.

### Step 4: Configure PostgreSQL

```bash
# Start PostgreSQL service
sudo service postgresql start

# Switch to postgres user
sudo -u postgres psql

# Create database user
CREATE USER corestack_user WITH PASSWORD 'ProUser#8487';

# Create database
CREATE DATABASE corestack OWNER corestack_user;

# Grant privileges
GRANT ALL PRIVILEGES ON DATABASE corestack TO corestack_user;

# Exit psql
\q
```

**Important:** Ensure PostgreSQL is running before attempting to connect:

```bash
# Check PostgreSQL status
sudo service postgresql status

# If not running, start it
sudo service postgresql start
```

### Step 5: Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Copy from example
cp .env.example nrm/.env

# Edit the .env file with your settings
nano nrm/.env
```

Required environment variables:

```env
# Django Settings
SECRET_KEY=your-secret-key-here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1

# Database Settings
DB_NAME=corestack
DB_USER=corestack_user
DB_PASSWORD=yourpassword
DB_HOST=localhost
DB_PORT=5432

# RabbitMQ Settings
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
```

### Step 6: Create Log Directory

```bash
# Navigate to project root
cd /mnt/y/core-stack-org/core-stack-backend

# Create logs directory
mkdir -p logs

# Create log files
touch logs/app.log
touch logs/nrm_app.log

# Set permissions (if running as www-data)
sudo chown -R www-data:www-data logs
```

### Step 7: Run Database Migrations

```bash
# Activate conda environment
conda activate corestack-backend

# Navigate to project root
cd /mnt/y/core-stack-org/core-stack-backend

# Run migrations
python manage.py migrate
```

### Step 8: Start RabbitMQ (Required for Celery)

```bash
# Start RabbitMQ service
sudo service rabbitmq-server start

# Verify RabbitMQ is running
sudo service rabbitmq-server status
```

---

## Configuration

### Apache Configuration (Production)

For production deployments, configure Apache:

```apache
<VirtualHost *:80>
    ServerName localhost

    WSGIDaemonProcess corestack python-home=/home/user/miniconda3/envs/corestack-backend python-path=/var/www/data/corestack
    WSGIProcessGroup corestack
    WSGIScriptAlias / /var/www/data/corestack/nrm_app/wsgi.py

    WSGIPassAuthorization On

    <Directory /var/www/data/corestack/nrm_app>
        <Files wsgi.py>
            Require all granted
        </Files>
    </Directory>

    Alias /static /var/www/data/corestack/static
    <Directory /var/www/data/corestack/static>
        Require all granted
    </Directory>

    ErrorLog ${APACHE_LOG_DIR}/corestack_error.log
    CustomLog ${APACHE_LOG_DIR}/corestack_access.log combined
</VirtualHost>
```

### Database Restore (Optional)

If you have a backup SQL file:

```bash
# Restore database from SQL file
psql -h localhost -U corestack_user -d corestack -f /path/to/backup.sql

# Note: Use -h localhost for TCP connection instead of socket
```

---

## Post-Installation Setup

### Create Superuser

```bash
# Activate environment
conda activate corestack-backend

# Navigate to project
cd /mnt/y/core-stack-org/core-stack-backend

# Create superuser
python manage.py createsuperuser
```

### Collect Static Files

```bash
python manage.py collectstatic --noinput
```

---

## Running the Application

### Development Mode

```bash
# Activate environment
conda activate corestack-backend

# Navigate to project
cd /mnt/y/core-stack-org/core-stack-backend

# Run development server
python manage.py runserver
```

The server will be available at `http://127.0.0.1:8000/`

### Running Celery Worker

For asynchronous task processing:

```bash
# Activate environment
conda activate corestack-backend

# Navigate to project
cd /mnt/y/core-stack-org/core-stack-backend

# Start Celery worker
celery -A nrm_app worker -l info -Q nrm
```

### Running Both Services

For development, you'll need two terminal sessions:

**Terminal 1 - Django Server:**
```bash
conda activate corestack-backend
cd /mnt/y/core-stack-org/core-stack-backend
python manage.py runserver
```

**Terminal 2 - Celery Worker:**
```bash
conda activate corestack-backend
cd /mnt/y/core-stack-org/core-stack-backend
celery -A nrm_app worker -l info -Q nrm
```

---

## Common Issues and Solutions

### Issue 1: PostgreSQL Connection Error

**Error:** `connection to server on socket "/tmp/.s.PGSQL.5432" failed`

**Solution:**
```bash
# Ensure PostgreSQL is running
sudo service postgresql start

# Use TCP connection instead of socket
psql -h localhost -U username -d database
```

### Issue 2: Missing SECRET_KEY

**Error:** `django.core.exceptions.ImproperlyConfigured: Set the SECRET_KEY environment variable`

**Solution:** Ensure `.env` file is created with `SECRET_KEY` defined.

### Issue 3: Missing Log Files

**Error:** `FileNotFoundError: /mnt/y/core-stack-org/core-stack-backend/logs/app.log`

**Solution:**
```bash
mkdir -p logs
touch logs/app.log
touch logs/nrm_app.log
```

### Issue 4: Git Merge Conflicts

**Error:** `error: Your local changes to the following files would be overwritten by merge`

**Solution:**
```bash
# Stash local changes
git stash

# Pull latest
git pull origin main

# Apply stashed changes (if needed)
git stash pop
```

### Issue 5: Celery Cannot Connect to RabbitMQ

**Error:** `Cannot connect to amqp://guest:**@127.0.0.1:5672//: Connection refused`

**Solution:**
```bash
# Start RabbitMQ
sudo service rabbitmq-server start

# Verify it's running
sudo service rabbitmq-server status
```

---

## Additional Resources

- [Django Documentation](https://docs.djangoproject.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Celery Documentation](https://docs.celeryproject.org/)
- [Conda Documentation](https://docs.conda.io/)

