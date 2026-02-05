#!/bin/bash

# Core Stack Backend - Migration Application Script
# This script applies all database migrations

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Application"
echo "=========================================="
echo ""

# Detect script location and set BACKEND_DIR dynamically
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# If script is in installation/ directory, BACKEND_DIR is the parent directory
if [ "$(basename "$SCRIPT_DIR")" = "installation" ]; then
    BACKEND_DIR="$(dirname "$SCRIPT_DIR")"
else
    # Fallback: assume we're already in the backend directory
    BACKEND_DIR="$(pwd)"
fi

# Verify BACKEND_DIR contains manage.py
if [ ! -f "$BACKEND_DIR/manage.py" ]; then
    echo "Error: manage.py not found in $BACKEND_DIR"
    echo "Please run this script from the installation/ directory or from the backend root directory."
    exit 1
fi

echo "Backend directory: $BACKEND_DIR"
echo ""

# Activate conda environment if not already active
if [ -z "$CONDA_DEFAULT_ENV" ] || [ "$CONDA_DEFAULT_ENV" != "corestack-backend" ]; then
    echo "Activating conda environment..."
    # Try multiple possible conda locations
    if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
        source "$HOME/miniconda3/etc/profile.d/conda.sh"
    elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
        source "$HOME/anaconda3/etc/profile.d/conda.sh"
    elif [ -f "/opt/miniconda3/etc/profile.d/conda.sh" ]; then
        source "/opt/miniconda3/etc/profile.d/conda.sh"
    else
        echo "Error: Conda not found. Please install Miniconda/Anaconda first."
        exit 1
    fi
    conda activate corestack-backend
fi

# Check if Django is available
if ! python -c "import django" 2>/dev/null; then
    echo "Error: Django is not installed or conda environment is not active"
    exit 1
fi

# Pre-flight check: Run Django system check to catch configuration errors early
echo "Pre-flight check: Running Django system check..."
if ! python manage.py check --deploy 2>&1; then
    echo ""
    echo "❌ Django system check failed! Please fix the configuration errors above before proceeding."
    echo ""
    echo "Common issues:"
    echo "  - Invalid CORS origins (should not contain paths)"
    echo "  - Missing environment variables"
    echo "  - Database connection issues"
    echo ""
    exit 1
fi
echo "✅ Django system check passed"
echo ""

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
