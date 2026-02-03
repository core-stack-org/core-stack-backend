#!/bin/bash

# Core Stack Backend - Migration Verification Script
# This script verifies that all migrations have been applied

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Verification"
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

echo "Checking migration status..."
echo ""

# Show migration status
python manage.py showmigrations

echo ""
echo "=========================================="
echo "Migration verification complete!"
echo "=========================================="
echo ""

# Check for unapplied migrations
UNAPPLIED=$(python manage.py showmigrations 2>&1 | grep -c "^\[ \]" || true)

if [ "$UNAPPLIED" -gt 0 ]; then
    echo "Warning: There are $UNAPPLIED unapplied migrations."
    echo "Run 'python manage.py migrate' to apply them."
    exit 1
else
    echo "Success: All migrations have been applied!"
    exit 0
fi
