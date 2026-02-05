#!/bin/bash

# Core Stack Backend - Complete Migration Setup Script
# This script handles the complete migration setup process including
# resolving circular dependencies for users, organization, and projects

set -e  # Exit on error

echo "=========================================="
echo "Core Stack Backend - Migration Setup"
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

echo "Setting up migrations for Core Stack Backend..."
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
# MIGRATION SETUP PHASES
# ============================================================================
# The key to avoiding CircularDependencyError is to generate migrations in the
# correct order. Django detects circular dependencies when loading ALL apps
# at once, but if we generate them phase by phase, each phase only loads
# the migrations from previous phases.
#
# Correct Order:
#   Phase 1: geoadmin (no project dependencies)
#   Phase 2: organization (no project dependencies)
#   Phase 3: users (depends on organization)
#   Phase 4: projects (depends on geoadmin, organization, users)
#   Phase 5: Other apps
# ============================================================================

echo "=========================================="
echo "Phase 1: Foundation Apps (No Dependencies)"
echo "=========================================="
echo ""

# Step 1a: Check/generate geoadmin migrations
echo "Step 1a: Geoadmin migrations..."
if [ -d "geoadmin/migrations" ] && [ -f "geoadmin/migrations/0001_initial.py" ]; then
    echo "  ✓ Geoadmin migrations exist"
else
    echo "  ✗ Geoadmin migrations missing"
    echo "  Generating geoadmin migrations..."
    python manage.py makemigrations geoadmin || echo "  Warning: geoadmin migrations may have issues"
fi
echo ""

# Step 1b: Check/generate organization migrations
echo "Step 1b: Organization migrations..."
if [ -d "organization/migrations" ] && [ -f "organization/migrations/0001_initial.py" ]; then
    echo "  ✓ Organization migrations exist"
else
    echo "  ✗ Organization migrations missing"
    echo "  Generating organization migrations..."
    python manage.py makemigrations organization || echo "  Warning: organization migrations may have issues"
fi
echo ""

# Step 1c: Check/generate users migrations
echo "Step 1c: Users migrations..."
if [ -d "users/migrations" ] && [ -f "users/migrations/0001_initial.py" ]; then
    echo "  ✓ Users migrations exist"
else
    echo "  ✗ Users migrations missing"
    echo "  Generating users migrations..."
    python manage.py makemigrations users || echo "  Warning: users migrations may have issues"
fi
echo ""

echo "=========================================="
echo "Phase 2: Projects App"
echo "=========================================="
echo ""

# Step 2: Check/generate projects migrations
echo "Step 2: Projects migrations..."
if [ -d "projects/migrations" ] && [ -f "projects/migrations/0001_initial.py" ]; then
    echo "  ✓ Projects migrations exist"
else
    echo "  ✗ Projects migrations missing"
    echo "  Generating projects migrations..."
    python manage.py makemigrations projects || echo "  Warning: projects migrations may have issues"
fi
echo ""

echo "=========================================="
echo "Phase 3: Apps Dependent on Users/Projects"
echo "=========================================="
echo ""

APPS_PHASE_3=(
    "community_engagement"
    "bot_interface"
    "apiadmin"
    "app_controller"
)

for app in "${APPS_PHASE_3[@]}"; do
    if [ -d "$app/migrations" ] && [ -f "$app/migrations/0001_initial.py" ]; then
        echo "  ✓ $app migrations exist"
    else
        echo "  ✗ $app migrations missing"
        echo "  Generating migrations for $app..."
        python manage.py makemigrations "$app" || echo "  Warning: Failed to generate migrations for $app"
    fi
done
echo ""

echo "=========================================="
echo "Phase 4: Other Apps"
echo "=========================================="
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
    if [ -d "$app/migrations" ] && [ -f "$app/migrations/0001_initial.py" ]; then
        echo "  ✓ $app migrations exist"
    else
        echo "  ✗ $app migrations missing"
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
