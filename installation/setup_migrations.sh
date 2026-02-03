#!/bin/bash

# Core Stack Backend - Complete Migration Setup Script
# This script handles the complete migration setup process including
# resolving circular dependencies for users, organization, and projects

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Setup"
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

echo "Setting up migrations for Core Stack Backend..."
echo ""

# Step 1: Check if migrations already exist for core apps
echo "Step 1: Checking existing migrations..."
if [ -d "organization/migrations" ] && [ -f "organization/migrations/0001_initial.py" ]; then
    echo "  ✓ Organization migrations exist"
else
    echo "  ✗ Organization migrations missing"
    echo "  Generating organization migrations..."
    python manage.py makemigrations organization
fi

if [ -d "projects/migrations" ] && [ -f "projects/migrations/0001_initial.py" ]; then
    echo "  ✓ Projects migrations exist"
else
    echo "  ✗ Projects migrations missing"
    echo "  Generating projects migrations..."
    python manage.py makemigrations projects
fi

if [ -d "users/migrations" ] && [ -f "users/migrations/0001_initial.py" ]; then
    echo "  ✓ Users migrations exist"
else
    echo "  ✗ Users migrations missing"
    echo "  Generating users migrations..."
    python manage.py makemigrations users
fi

echo ""

# Step 2: Generate migrations for geoadmin (required by projects)
echo "Step 2: Generating geoadmin migrations..."
if [ -d "geoadmin/migrations" ] && [ -f "geoadmin/migrations/0001_initial.py" ]; then
    echo "  ✓ Geoadmin migrations exist"
else
    echo "  Generating geoadmin migrations..."
    python manage.py makemigrations geoadmin || echo "  Warning: geoadmin migrations may have issues"
fi

echo ""

# Step 3: Generate migrations for other apps
echo "Step 3: Generating migrations for other apps..."

APPS=(
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
    "community_engagement"
    "bot_interface"
    "apiadmin"
    "app_controller"
)

for app in "${APPS[@]}"; do
    if [ -d "$app/migrations" ] && [ -f "$app/migrations/0001_initial.py" ]; then
        echo "  ✓ $app migrations exist"
    else
        echo "  Generating migrations for $app..."
        python manage.py makemigrations "$app" || echo "  Warning: Failed to generate migrations for $app"
    fi
done

echo ""
echo "=========================================="
echo "Migration setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review the generated migration files"
echo "2. Run: bash installation/apply_migrations.sh"
echo "3. Verify with: bash installation/verify_migrations.sh"
echo ""
