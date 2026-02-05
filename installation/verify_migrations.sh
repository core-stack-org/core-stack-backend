#!/bin/bash

# Core Stack Backend - Migration Verification Script
# This script verifies that all migrations have been applied

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Verification"
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
