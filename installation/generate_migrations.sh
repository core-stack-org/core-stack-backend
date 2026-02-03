#!/bin/bash

# Core Stack Backend - Migration Generation Script
# This script generates all database migrations in the correct order

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Generation"
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

echo "Generating migrations in dependency order..."
echo ""

# Phase 1: Generate migrations for geoadmin (required by projects)
echo "Phase 1: Generating geoadmin migrations..."
python manage.py makemigrations geoadmin || echo "Warning: geoadmin migrations may have issues"
echo ""

# Phase 2: Generate migrations for other apps (in dependency order)
echo "Phase 2: Generating migrations for other apps..."

# Apps that don't depend on users or projects
APPS_PHASE_2=(
    "computing"
    "dpr"
    "gee_computing"
    "moderation"
    "plans"
    "plantations"
    "public_api"
    "public_dataservice"
    "stats_generator"
    "waterrejuvenation"
)

for app in "${APPS_PHASE_2[@]}"; do
    echo "  Generating migrations for $app..."
    python manage.py makemigrations "$app" || echo "  Warning: Failed to generate migrations for $app"
done

echo ""

# Phase 3: Generate migrations for apps that depend on users
echo "Phase 3: Generating migrations for apps that depend on users..."

APPS_PHASE_3=(
    "community_engagement"
    "bot_interface"
    "apiadmin"
    "app_controller"
)

for app in "${APPS_PHASE_3[@]}"; do
    echo "  Generating migrations for $app..."
    python manage.py makemigrations "$app" || echo "  Warning: Failed to generate migrations for $app"
done

echo ""
echo "=========================================="
echo "Migration generation complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review the generated migration files"
echo "2. Run: bash installation/apply_migrations.sh"
echo "3. Verify with: python manage.py showmigrations"
echo ""
