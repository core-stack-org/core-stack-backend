# CoRE Stack Backend - Troubleshooting Guide

This document contains solutions to common issues encountered during the installation and operation of the CoRE Stack Backend application.

> **Need help?** If you encounter issues not covered here, you can:
> - Review the [Installation Guide](INSTALLATION.md)
> - Post an issue at [GitHub Issues](https://github.com/core-stack-org/core-stack-backend/issues)

## Table of Contents

- [Phase 1: Prerequisites Issues](#phase-1-prerequisites-issues)
- [Phase 2: Installation Script Issues](#phase-2-installation-script-issues)
- [Phase 3: Post-Installation Issues](#phase-3-post-installation-issues)
- [Running the Application](#running-the-application)

---

## Phase 1: Prerequisites Issues

### System Packages

**Error:** Package not found or installation fails

```bash
# Update package lists
sudo apt update

# Install essential packages
sudo apt install -y git wget curl build-essential libpq-dev
```

### RabbitMQ Installation

**Error:** `rabbitmq-server: unrecognized service`

**Solution:**
```bash
# Install RabbitMQ
sudo apt update
sudo apt install -y rabbitmq-server

# Start and enable
sudo systemctl start rabbitmq-server
sudo systemctl enable rabbitmq-server

# Verify
sudo systemctl status rabbitmq-server
```

---

## Phase 2: Installation Script Issues

### Miniconda Installation

**Error:** `conda: command not found`

**Solution:**
```bash
# Source conda initialization
source $HOME/miniconda3/etc/profile.d/conda.sh

# Add to PATH if needed
export PATH="$HOME/miniconda3/bin:$PATH"

# Add permanently to .bashrc
echo "source $HOME/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc
source ~/.bashrc
```

### Conda Environment Issues

**Error:** `Could not find conda environment: corestack-backend`

**Solution:**
```bash
# Initialize conda for current session
source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate corestack-backend
```

**Error:** Conda environment creation fails or takes too long

**Solution:**
```bash
# Remove existing environment and recreate
conda env remove -n corestack-backend -y
conda env create -f installation/environment.yml -n corestack-backend

# If still failing, try with --offline flag
conda env create -f installation/environment.yml -n corestack-backend --offline
```

**Error:** `ModuleNotFoundError: No module named 'some_module'`

**Solution:**
```bash
# Activate environment
conda activate corestack-backend

# Install pip dependencies manually
pip install -r installation/requirements.txt
```

### PostgreSQL Issues

**Error:** `connection to server on socket "/tmp/.s.PGSQL.5432" failed`

**Solution:**
```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Start PostgreSQL
sudo systemctl start postgresql

# Use TCP connection instead of socket
psql -h localhost -U nrm -d nrm
```

**Error:** `ERROR: role "nrm" already exists`

**Solution:**
```bash
sudo -u postgres psql
-- Grant access to existing user
GRANT USAGE, CREATE ON SCHEMA public TO nrm;
ALTER DATABASE nrm OWNER TO nrm;
\q
```

**Error:** `permission denied for database "nrm"`

**Solution:**
```bash
sudo -u postgres psql
GRANT ALL PRIVILEGES ON DATABASE nrm TO nrm;
GRANT ALL ON SCHEMA public TO nrm;
\q
```

### Apache Issues

**Error:** `Job for apache2.service failed`

**Solution:**
```bash
# Check Apache error logs
sudo tail -f /var/log/apache2/error.log

# Test configuration
sudo apache2ctl configtest

# Enable mod_wsgi
sudo a2enmod wsgi

# Restart Apache
sudo systemctl restart apache2
```

**Error:** 403 Forbidden or permission errors

**Solution:**
```bash
# Fix ownership
sudo chown -R www-data:www-data /var/www/data/corestack

# Fix permissions
sudo chmod -R 755 /var/www/data/corestack
sudo chmod 640 /var/www/data/corestack/nrm_app/.env
```

---

## Phase 3: Post-Installation Issues

### Environment Variables

**Error:** `django.core.exceptions.ImproperlyConfigured: Set the SECRET_KEY environment variable`

**Solution:**
```bash
# Edit .env file
sudo nano /var/www/data/corestack/nrm_app/.env

# Add required variables:
SECRET_KEY=your-secret-key-here
DEBUG=False
```

**Generate a Secret Key:**
```bash
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```

**Error:** Permission denied when accessing `.env` file

**Solution:**
```bash
# Fix permissions
sudo chown www-data:www-data /var/www/data/corestack/nrm_app/.env
sudo chmod 640 /var/www/data/corestack/nrm_app/.env
```

### Log Files

**Error:** `FileNotFoundError: logs/app.log`

**Solution:**
```bash
# Create logs directory
sudo mkdir -p /var/www/data/corestack/logs

# Create log files
sudo touch /var/www/data/corestack/logs/app.log
sudo touch /var/www/data/corestack/logs/nrm_app.log

# Set ownership
sudo chown -R www-data:www-data /var/www/data/corestack/logs
sudo chmod -R 755 /var/www/data/corestack/logs
```

### Database Migrations

**Error:** `relation "users_user" does not exist` or `You have unapplied migration(s)`

**Solution:**
```bash
# Activate environment
conda activate corestack-backend
cd /var/www/data/corestack

# Show migration status
python manage.py showmigrations

# Apply all migrations
python manage.py migrate

# If tables exist but migrations not tracked
python manage.py migrate --fake-initial
```

**Error:** `psycopg.errors.UndefinedTable: relation "django_session" does not exist`

**Solution:**
```bash
# Run migrations
python manage.py migrate

# If specific app migrations are missing
python manage.py migrate sessions
```

### GEE Service Account Keys

**Error:** `FileNotFoundError: GEE service account key not found`

**Solution:**
```bash
# Create directory for GEE keys
sudo mkdir -p /var/www/data/corestack/data/gee_confs

# Copy your service account keys
sudo cp ~/path/to/your-gee-key.json /var/www/data/corestack/data/gee_confs/

# Set permissions
sudo chown -R www-data:www-data /var/www/data/corestack/data/gee_confs
sudo chmod 750 /var/www/data/corestack/data/gee_confs/*.json
```

---

## Running the Application

### Celery Worker Issues

**Error:** `Cannot connect to amqp://guest:**@127.0.0.1:5672//: Connection refused`

**Solution:**
```bash
# Start RabbitMQ
sudo systemctl start rabbitmq-server

# Verify it's running
sudo systemctl status rabbitmq-server
```

**Error:** `celery: command not found`

**Solution:**
```bash
# Ensure conda environment is activated
conda activate corestack-backend

# Start worker with full path
python -m celery -A nrm_app worker -l info -Q nrm
```

**Celery tasks not processing:**

Checklist:
1. Is RabbitMQ running? `sudo systemctl status rabbitmq-server`
2. Is Celery worker running? Check process list
3. Are there error logs? Check `celery -A nrm_app worker -l debug`

### Development Server Issues

**Error:** Port 8000 already in use

```bash
# Find process using port 8000
lsof -i :8000

# Kill if needed
kill -9 <PID>
```

**Error:** Server won't start

```bash
# Test database connection
python manage.py dbshell

# Collect static files
python manage.py collectstatic --noinput
```

### Git Issues

**Error:** `Your local changes would be overwritten by merge`

**Option 1: Stash changes**
```bash
git stash
git pull origin main
git stash pop
```

**Option 2: Commit changes**
```bash
git add -A
git commit -m "Your commit message"
git pull origin main
```

**Option 3: Discard changes**
```bash
git checkout -- <file-path>
git pull origin main
```

---

## Additional Help

If you encounter issues not covered here:

1. Review the [Installation Guide](INSTALLATION.md)
2. Search or post at [GitHub Issues](https://github.com/core-stack-org/core-stack-backend/issues)
3. Check Django logs: `/var/www/data/corestack/logs/app.log`
4. Check Apache error logs: `/var/log/apache2/error.log`
5. Check Celery logs in terminal output
6. Learn basics of Django’s architecture by watching this brief [Harvard CS50 lecture on Django](https://www.youtube.com/watch?v=w8q0C-C1js4).