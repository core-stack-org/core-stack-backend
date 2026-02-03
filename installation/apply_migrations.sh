#!/bin/bash

# Core Stack Backend - Migration Application Script
# This script applies all database migrations

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Application"
echo "=========================================="
echo ""

# Activate conda environment if not already active
if [ -z "$CONDA_DEFAULT_ENV" ] || [ "$CONDA_DEFAULT_ENV" != "corestack-backend" ]; then
    echo "Activating conda environment..."
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate corestack-backend
fi

# Check if Django is available
if ! python -c "import django" 2>/dev/null; then
    echo "Error: Django is not installed or conda environment is not active"
    exit 1
fi

# Check if database exists
echo "Checking database connection..."
if ! python manage.py check --database default 2>/dev/null; then
    echo "Error: Cannot connect to database. Please check your .env file."
    exit 1
fi

echo "Applying migrations..."
echo ""

# Apply all migrations
python manage.py migrate

echo ""
echo "=========================================="
echo "Migration application complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Verify migrations with: python manage.py showmigrations"
echo "2. Create superuser with: python manage.py createsuperuser"
echo "3. Start development server with: python manage.py runserver"
echo ""
