# Migration Solution Summary

## Overview

This document provides a comprehensive summary of the database migration solution implemented for the Core Stack Backend project. It addresses the migration issues that prevented open source users from setting up the application from scratch.

## Problem Statement

The Core Stack Backend project had incomplete database migrations, causing the following issues:

1. **Initial Migration Error**: `relation "users_user" does not exist`
   - The `admin.0001_initial` migration tried to reference the `users_user` table before it was created
   - This occurred because the project uses a custom user model (`AUTH_USER_MODEL = "users.User"`)

2. **Circular Dependencies**: Multiple models had circular foreign key relationships:
   - `users.User` ↔ `organization.Organization`
   - `users.UserProjectGroup` ↔ `projects.Project`

3. **Missing Migrations**: Many apps had models but no migrations

## Solution Implemented

### Phase 1: Resolve Circular Dependencies

Created a phased migration approach that breaks circular dependencies:

#### Organization App
- **`organization/migrations/0001_initial.py`**: Creates Organization table without `created_by` field
- **`organization/migrations/0002_add_created_by.py`**: Adds `created_by` field after users table exists

#### Projects App
- **`projects/migrations/0001_initial.py`**: Creates Project table without user fields
- **`projects/migrations/0002_add_user_fields.py`**: Adds user fields after users table exists

#### Users App
- **`users/migrations/0001_initial.py`**: Creates User and UserProjectGroup tables

### Phase 2: Create Migration Scripts

Created automated scripts to manage the migration process:

#### `installation/setup_migrations.sh` (Recommended)
Complete migration setup script that:
1. Checks for existing migrations for core apps
2. Generates missing migrations for core apps
3. Generates migrations for geoadmin (required by projects)
4. Generates migrations for all other apps

#### `installation/generate_migrations.sh`
Generates migrations for all apps in dependency order:
1. Generates migrations for geoadmin
2. Generates migrations for apps that don't depend on users or projects
3. Generates migrations for apps that depend on users

#### `installation/apply_migrations.sh`
Applies all migrations to the database:
1. Checks database connection
2. Applies all pending migrations
3. Reports completion status

#### `installation/verify_migrations.sh`
Verifies that all migrations have been applied:
1. Shows migration status for all apps
2. Checks for unapplied migrations
3. Reports success or failure

### Phase 3: Update Installation Process

Updated `installation/install.sh` to include migration process:
1. Added `setup_migrations()` function to generate migrations
2. Integrated migration setup into the main installation flow
3. Ensures migrations are generated before being applied

### Phase 4: Create Documentation

Created comprehensive documentation:

#### `[Migration Setup](./30-database-migration-setup-for-community-users.md)`
GitHub Issue documenting:
- Problem description
- Root causes
- Proposed solution
- Implementation plan
- Acceptance criteria

#### `[Migration Guide](./31-migration-guide-for-oss-community-users.md)`
Complete migration guide covering:
- Prerequisites
- Quick start instructions
- Manual migration process
- Understanding migration structure
- Troubleshooting
- Advanced topics
- Best practices

#### `../installation/README.md`
Scripts documentation covering:
- Overview of all scripts
- Usage instructions
- Migration order
- Circular dependencies explanation
- Troubleshooting
- Integration with installation script

## Migration Order

The migrations are applied in this order to resolve circular dependencies:

1. **contenttypes** - Django's content types framework
2. **auth** - Django's authentication system
3. **organization.0001** - Organization model (without user references)
4. **geoadmin** - Geographic administrative boundaries
5. **projects.0001** - Project model (without user references)
6. **users.0001** - User model and UserProjectGroup
7. **organization.0002** - Add user references to Organization
8. **projects.0002** - Add user references to Projects
9. **admin** - Django admin
10. **sessions** - Django sessions
11. **rest_framework_api_key** - API key management
12. **token_blacklist** - JWT token blacklist
13. **Other apps** - All other project apps

## Usage

### Quick Start

For a complete migration setup, run:

```bash
# 1. Generate all migrations
bash installation/setup_migrations.sh

# 2. Apply migrations to database
bash installation/apply_migrations.sh

# 3. Verify migrations were applied
bash installation/verify_migrations.sh
```

### Using Installation Script

The migration process is integrated with the main installation script:

```bash
cd installation
bash install.sh
```

This will automatically:
1. Generate all migrations
2. Apply migrations to the database
3. Verify migrations were applied successfully

### Manual Process

If you prefer to run migrations manually:

```bash
# Activate conda environment
conda activate corestack-backend

# Generate migrations for specific app
python manage.py makemigrations <app_name>

# Apply all migrations
python manage.py migrate

# Verify migration status
python manage.py showmigrations
```

## Files Created/Modified

### Migration Files
- `organization/migrations/0001_initial.py` - Created
- `organization/migrations/0002_add_created_by.py` - Created
- `projects/migrations/0001_initial.py` - Created
- `projects/migrations/0002_add_user_fields.py` - Created
- `users/migrations/0001_initial.py` - Created

### Migration Automation Scripts
- `installation/setup_migrations.sh` - Created
- `installation/generate_migrations.sh` - Created
- `installation/apply_migrations.sh` - Created
- `installation/verify_migrations.sh` - Created
- `installation/README.md` - Created

### Installation Files
- `installation/install.sh` - Modified (added migration setup)

## Testing

### Test Plan

1. **Fresh Database Test**
   - Create a new database
   - Run `bash installation/setup_migrations.sh`
   - Run `bash installation/apply_migrations.sh`
   - Run `bash installation/verify_migrations.sh`
   - Verify all tables are created correctly

2. **Existing Database Test**
   - Test on an existing database (if applicable)
   - Verify migrations don't break existing data

3. **Installation Script Test**
   - Run `bash installation/install.sh`
   - Verify migrations are generated and applied correctly

4. **Verification Test**
   - Run `python manage.py showmigrations`
   - Verify all migrations show as applied `[X]`

### Expected Results

- All migrations are generated successfully
- All migrations are applied without errors
- No circular dependency errors
- All tables are created correctly
- All relationships work correctly

## Benefits

1. **Automated Process**: Migration process is now fully automated
2. **Clear Documentation**: Comprehensive documentation for open source users
3. **Error Handling**: Scripts handle common errors gracefully
4. **Verification**: Built-in verification to ensure migrations are applied correctly
5. **Integration**: Seamlessly integrated with installation script
6. **Maintainability**: Easy to update and maintain

## Future Improvements

1. **Data Migrations**: Add support for data migrations if needed
2. **Rollback Support**: Add rollback functionality
3. **Automated Testing**: Create automated tests for migration process
4. **Performance Optimization**: Optimize migration generation for large projects
5. **Backup Integration**: Add automatic database backup before migrations

## Support

For issues or questions:
1. Check the troubleshooting section in the migration guide
2. Review the GitHub issue documentation
3. Create a new issue with: [Issue Template](../.github/ISSUE_TEMPLATE/c4gt_community.yml)
   - Error message
   - Steps to reproduce
   - Environment details
   - Migration status (`python manage.py showmigrations`)
