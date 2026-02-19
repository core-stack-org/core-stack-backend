# CoRE Stack Backend - Troubleshooting Guide

This document contains solutions to common issues encountered during the installation and operation of the CoRE Stack Backend application.

## Table of Contents

- [Database Issues](#database-issues)
- [Environment Issues](#environment-issues)
- [Service Issues](#service-issues)
- [Git Issues](#git-issues)
- [Permission Issues](#permission-issues)
- [Application Errors](#application-errors)

---

## Database Issues

### PostgreSQL Connection Refused

**Symptom:**
```
psql: error: connection to server on socket "/tmp/.s.PGSQL.5432" failed: No such file or directory
```

**Causes:**
1. PostgreSQL service is not running
2. PostgreSQL is not listening on the expected port

**Solutions:**

```bash
# Check PostgreSQL status
sudo service postgresql status

# Start PostgreSQL
sudo service postgresql start

# If issues persist, use TCP connection
psql -h localhost -U username -d database
```

### Database Migration Errors

**Symptom:**
```
django.db.utils.ProgrammingError: relation "users_user" does not exist
```

**Causes:**
1. Database not properly initialized
2. Migrations not applied

**Solutions:**

```bash
# Drop and recreate database (if needed)
sudo -u postgres psql
DROP DATABASE corestack;
CREATE DATABASE corestack;
\q

# Run migrations
python manage.py migrate
```

### PostgreSQL User Already Exists

**Symptom:**
```
ERROR: role "corestack_user" already exists
```

**Solution:**
```bash
sudo -u postgres psql
-- Grant access to existing user
GRANT USAGE, CREATE ON SCHEMA public TO corestack_user;
ALTER DATABASE corestack OWNER TO corestack_user;
\q
```

---

## Environment Issues

### Conda Environment Activation Fails

**Symptom:**
```
conda: command not found
```

**Solution:**
```bash
# Source conda initialization
source $HOME/miniconda3/etc/profile.d/conda.sh

# Add to PATH if needed
export PATH="$HOME/miniconda3/bin:$PATH"

# Add permanently to .bashrc
echo "source $HOME/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc
```

### Conda Environment Creation Fails

**Symptom:**
```
Solving environment: done
# ... long package list ...
```

**Solutions:**

```bash
# Remove existing environment and recreate
conda env remove -n corestack-backend -y
conda env create -f installation/environment.yml -n corestack-backend

# If still failing, try with --offline flag
conda env create -f installation/environment.yml -n corestack-backend --offline
```

### Missing pip Dependencies

**Symptom:**
```
ModuleNotFoundError: No module named 'some_module'
```

**Solution:**
```bash
# Activate environment
conda activate corestack-backend

# Install pip dependencies manually
pip install -r installation/requirements.txt
```

---

## Service Issues

### RabbitMQ Connection Refused

**Symptom:**
```
[ERROR] Cannot connect to amqp://guest:**@127.0.0.1:5672//: [Errno 111] Connection refused
```

**Causes:**
1. RabbitMQ service not running
2. RabbitMQ not installed

**Solutions:**

```bash
# Install RabbitMQ
sudo apt update
sudo apt install -y rabbitmq-server

# Start RabbitMQ
sudo service rabbitmq-server start

# Check status
sudo service rabbitmq-server status

# Enable on startup
sudo systemctl enable rabbitmq-server
```

### Apache Not Starting

**Symptom:**
```
Job for apache2.service failed because the control process exited with error code
```

**Solutions:**

```bash
# Check Apache error logs
sudo tail -f /var/log/apache2/error.log

# Test configuration
sudo apache2ctl configtest

# Start Apache
sudo service apache2 start

# Enable mod_wsgi
sudo a2enmod wsgi
```

---

## Git Issues

### Local Changes Overwritten by Merge

**Symptom:**
```
error: Your local changes to the following files would be overwritten by merge:
        projects/views.py
        users/permissions.py
Please commit your changes or stash them before you merge.
```

**Solutions:**

Option 1: Stash changes
```bash
git stash
git pull origin main
git stash pop
```

Option 2: Commit changes
```bash
git add -A
git commit -m "Your commit message"
git pull origin main
```

Option 3: Discard changes (if not needed)
```bash
git checkout -- projects/views.py users/permissions.py
git pull origin main
```

---

## Permission Issues

### Permission Denied for Logs Directory

**Symptom:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/mnt/y/core-stack-org/core-stack-backend/logs/app.log'
```

**Solution:**
```bash
# Create logs directory
mkdir -p logs

# Create log files
touch logs/app.log
touch logs/nrm_app.log

# Set ownership (if using www-data)
sudo chown -R www-data:www-data logs
sudo chmod -R 755 logs
```

### PostgreSQL Permission Denied

**Symptom:**
```
permission denied for database "corestack"
```

**Solution:**
```bash
sudo -u postgres psql
GRANT ALL PRIVILEGES ON DATABASE corestack TO corestack_user;
GRANT ALL ON SCHEMA public TO corestack_user;
\q
```

---

## Application Errors

### Missing SECRET_KEY

**Symptom:**
```
django.core.exceptions.ImproperlyConfigured: Set the SECRET_KEY environment variable
```

**Solution:**
```bash
# Create .env file
cp .env.example .env

# Edit with your secret key
nano .env

# Add: SECRET_KEY=your-secret-key-here
```

### Missing ALLOWED_HOSTS

**Symptom:**
```
django.core.exceptions.ImproperlyConfigured: ALLOWED_HOSTS is empty
```

**Solution:**
In `.env`:
```env
ALLOWED_HOSTS=localhost,127.0.0.1,your-domain.com
```

### Session Table Does Not Exist

**Symptom:**
```
psycopg.errors.UndefinedTable: relation "django_session" does not exist
```

**Solution:**
```bash
# Run migrations
python manage.py migrate

# If specific app migrations are missing
python manage.py migrate sessions
```

### Django Migration Issues

**Symptom:**
```
You have 33 unapplied migration(s)
```

**Solution:**
```bash
# Show migration status
python manage.py showmigrations

# Apply all migrations
python manage.py migrate

# If tables exist but migrations not tracked
python manage.py migrate --fake-initial
```

---

## Celery Worker Issues

### Celery Worker Not Starting

**Symptom:**
```
celery: command not found
```

**Solution:**
```bash
# Ensure conda environment is activated
conda activate corestack-backend

# Install celery
pip install celery

# Start worker with full path
python -m celery -A nrm_app worker -l info -Q nrm
```

### Celery Tasks Not Processing

**Checklist:**
1. Is RabbitMQ running? `sudo service rabbitmq-server status`
2. Is Celery worker running? Check process list
3. Are there error logs? Check `celery -A nrm_app worker -l debug`

---

## Development Server Issues

### Server Won't Start

**Common Causes and Solutions:**

1. **Port in use:**
   ```bash
   # Find process using port 8000
   lsof -i :8000
   
   # Kill if needed
   kill -9 <PID>
   ```

2. **Database connection:**
   ```bash
   # Test database connection
   python manage.py dbshell
   ```

3. **Static files:**
   ```bash
   # Collect static files
   python manage.py collectstatic --noinput
   ```

---

## Additional Help

If you encounter issues not covered here:

1. Check Django logs in `logs/app.log`
2. Check Apache error logs: `/var/log/apache2/error.log`
3. Check Celery logs in terminal output
4. Review the [Installation Guide](INSTALLATION.md)
5. Search existing GitHub issues

