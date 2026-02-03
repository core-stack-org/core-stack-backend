# Migration Scripts

This directory contains automated scripts for managing database migrations in the Core Stack Backend project.

## Overview

The migration scripts handle the complete database migration process, including:
- Generating migrations for all apps
- Resolving circular dependencies
- Applying migrations to the database
- Verifying migration status

## Scripts
### `setup_migrations.sh` (Recommended)

Complete migration setup script that handles the entire process.

**Usage:**
```bash
bash installation/setup_migrations.sh
```

**What it does:**
1. Checks for existing migrations for core apps (organization, projects, users)
2. Generates missing migrations for core apps
3. Generates migrations for geoadmin (required by projects)
4. Generates migrations for all other apps

**When to use:**
- First-time setup
- After adding new models
- After modifying existing models

### `generate_migrations.sh`

Generates migrations for all apps in dependency order.

**Usage:**
```bash
bash installation/generate_migrations.sh
```

**What it does:**
1. Generates migrations for geoadmin
2. Generates migrations for apps that don't depend on users or projects
3. Generates migrations for apps that depend on users

**When to use:**
- When you want to generate migrations without applying them
- When you want to review migrations before applying

### `apply_migrations.sh`

Applies all migrations to the database.

**Usage:**
```bash
bash installation/apply_migrations.sh
```

**What it does:**
1. Checks database connection
2. Applies all pending migrations
3. Reports completion status

**When to use:**
- After generating migrations
- After pulling changes from git
- When deploying to production

### `verify_migrations.sh`

Verifies that all migrations have been applied.

**Usage:**
```bash
bash installation/verify_migrations.sh
```

**What it does:**
1. Shows migration status for all apps
2. Checks for unapplied migrations
3. Reports success or failure

**When to use:**
- After applying migrations
- Before deploying to production
- When troubleshooting migration issues

## Quick Start

For a complete migration setup, run:

```bash
# 1. Generate all migrations
bash installation/setup_migrations.sh

# 2. Apply migrations to database
bash installation/apply_migrations.sh

# 3. Verify migrations were applied
bash installation/verify_migrations.sh
```

## Manual Migration Process

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

## Circular Dependencies

The project has circular foreign key relationships between:
- `users.User` ↔ `organization.Organization`
- `users.UserProjectGroup` ↔ `projects.Project`

To resolve this, migrations are split into multiple files:

### Organization
- `0001_initial.py` - Creates Organization table without `created_by` field
- `0002_add_created_by.py` - Adds `created_by` field after users table exists

### Projects
- `0001_initial.py` - Creates Project table without user fields
- `0002_add_user_fields.py` - Adds user fields after users table exists

### Users
- `0001_initial.py` - Creates User and UserProjectGroup tables

## Troubleshooting

### Error: "relation does not exist"

**Problem:** A migration is trying to reference a table that doesn't exist yet.

**Solution:** This usually indicates a circular dependency issue. Run `bash installation/setup_migrations.sh` to ensure migrations are generated in the correct order.

### Error: "CircularDependencyError"

**Problem:** Two or more migrations depend on each other.

**Solution:** The migration scripts handle this automatically by splitting migrations into multiple files. If you encounter this error, ensure you're using the latest version of the scripts.

### Error: "ProgrammingError: relation already exists"

**Problem:** A table already exists in the database.

**Solution:** This can happen if you're running migrations on an existing database. You may need to:
1. Drop the table manually: `DROP TABLE table_name;`
2. Or use `python manage.py migrate --fake` to mark the migration as applied

### Error: "ModuleNotFoundError: No module named 'django'"

**Problem:** Django is not installed or the conda environment is not activated.

**Solution:** Activate the conda environment:
```bash
conda activate corestack-backend
```

## Integration with Installation Script

The migration scripts are integrated with the main installation script (`installation/install.sh`).

The installation process:
1. Installs dependencies (Miniconda, PostgreSQL, Apache)
2. Sets up conda environment
3. Clones the repository
4. **Generates migrations** (via `setup_migrations.sh`)
5. Collects static files
6. **Applies migrations** (via `run_migrations`)
7. Configures Apache

## Best Practices

1. **Always backup your database** before running migrations on production
2. **Test migrations** on a staging environment before applying to production
3. **Review migration files** before applying them to understand what changes will be made
4. **Use version control** for migration files to track changes
5. **Run verification** after applying migrations to ensure success

## Support

For more information, see:
- [Migration Strategy](../documentations/32-recommended-migration-solution.md)
- [Django Migrations Documentation](https://docs.djangoproject.com/en/stable/topics/migrations/)
