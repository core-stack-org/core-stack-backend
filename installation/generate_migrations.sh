#!/bin/bash

# Core Stack Backend - Migration Generation Script
# This script generates all database migrations in the correct order

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Generation"
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

echo "Generating migrations in dependency order..."
echo ""


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

# ============================================================================
# MIGRATION GENERATION PHASES
# ============================================================================
# The key to avoiding CircularDependencyError is to generate migrations in the
# correct order. Django detects circular dependencies when loading ALL apps
# at once, but if we generate them phase by phase, each phase only loads
# the migrations from previous phases.
# ============================================================================

# Phase 1: Generate migrations for foundation apps (no circular dependencies)
# Order: geoadmin → organization → users
# These apps must be generated first as other apps depend on them
echo "=========================================="
echo "Phase 1: Generating foundation app migrations"
echo "=========================================="
echo "  (These apps have no dependencies on other project apps)"
echo ""

echo "  1/3: geoadmin (geographic data models, API keys)"
python manage.py makemigrations geoadmin || echo "  Warning: geoadmin migrations may have issues"
echo ""

echo "  2/3: organization (organization model)"
python manage.py makemigrations organization || echo "  Warning: organization migrations may have issues"
echo ""

echo "  3/3: users (user model, user-project groups)"
python manage.py makemigrations users || echo "  Warning: users migrations may have issues"
echo ""

# Phase 2: Generate migrations for projects (depends on geoadmin, organization, users)
echo "=========================================="
echo "Phase 2: Generating projects migrations"
echo "=========================================="
echo "  (Projects depends on geoadmin, organization, and users)"
echo ""

python manage.py makemigrations projects || echo "  Warning: projects migrations may have issues"
echo ""

# Phase 3: Generate migrations for apps that depend on projects and users
echo "=========================================="
echo "Phase 3: Generating dependent app migrations"
echo "=========================================="
echo "  (These apps depend on users and/or projects)"
echo ""

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

# Phase 4: Generate migrations for other apps (no user/project dependencies)
echo "=========================================="
echo "Phase 4: Generating other app migrations"
echo "=========================================="
echo "  (These apps have no user/project dependencies)"
echo ""

APPS_PHASE_4=(
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

for app in "${APPS_PHASE_4[@]}"; do
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
echo "3. Verify with: bash installation/verify_migrations.sh"
echo ""
