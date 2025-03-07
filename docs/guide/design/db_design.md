# Database Design

This document outlines the database design for the Core Stack Backend, including the main tables, relationships, and user role management.

## Tables and Relationships

### Organization

The Organization table serves as the top-level entity in the system hierarchy.

**Fields:**
- `id` (UUID): Primary key
- `name` (CharField): Organization name
- `description` (TextField): Organization description
- `created_at` (DateTimeField): Creation timestamp
- `created_by` (CharField): User who created the organization
- `updated_at` (DateTimeField): Last update timestamp
- `updated_by` (CharField): User who last updated the organization

**Relationships:**
- One-to-many with User (an organization has many users)
- One-to-many with Project (an organization has many projects)

### User

The User table extends Django's AbstractUser and manages authentication and permissions.

**Fields:**
- `id` (AutoField): Primary key
- `organization` (ForeignKey): Reference to Organization
- `contact_number` (CharField): User's contact number
- `is_superadmin` (BooleanField): Flag for super admin privileges
- Standard Django AbstractUser fields (username, email, password, etc.)

**Relationships:**
- Many-to-one with Organization (many users belong to one organization)
- Many-to-many with Project through UserProjectGroup (users can have roles in multiple projects)

### UserProjectGroup

This table manages user roles within specific projects.

**Fields:**
- `id` (AutoField): Primary key
- `user` (ForeignKey): Reference to User
- `project` (ForeignKey): Reference to Project
- `group` (ForeignKey): Reference to Django's Group model (represents the role)

**Relationships:**
- Many-to-one with User
- Many-to-one with Project
- Many-to-one with Group (Django's built-in group model for permissions)

### Project

The Project table represents a distinct project within an organization.

**Fields:**
- `id` (AutoField): Primary key
- `name` (CharField): Project name
- `organization` (ForeignKey): Reference to Organization
- `description` (TextField): Project description
- `geojson_path` (CharField): Path to the GeoJSON file for the project
- `created_at` (DateTimeField): Creation timestamp
- `created_by` (ForeignKey): User who created the project
- `updated_at` (DateTimeField): Last update timestamp
- `updated_by` (ForeignKey): User who last updated the project

**Relationships:**
- Many-to-one with Organization
- One-to-many with ProjectApp (a project can have multiple apps enabled)
- Many-to-many with User through UserProjectGroup

### ProjectApp

This table manages which applications are enabled for a project.

**Fields:**
- `id` (AutoField): Primary key
- `project` (ForeignKey): Reference to Project
- `app_type` (CharField): Type of app (e.g., 'plantation', 'watershed')
- `enabled` (BooleanField): Whether the app is enabled

**Relationships:**
- Many-to-one with Project
- One-to-many with KMLFile (for plantation app)
- One-to-many with Plan (for watershed app)

### KMLFile (Plantations)

This table stores KML files uploaded for plantation projects.

**Fields:**
- `id` (AutoField): Primary key
- `project_app` (ForeignKey): Reference to ProjectApp
- `name` (CharField): File name
- `file` (FileField): Uploaded KML file
- `kml_hash` (CharField): SHA-256 hash of the file content (for deduplication)
- `uploaded_by` (ForeignKey): User who uploaded the file
- `created_at` (DateTimeField): Upload timestamp

**Relationships:**
- Many-to-one with ProjectApp

### Plan (Watershed Planning)

This table stores watershed planning data.

**Fields:**
- `id` (AutoField): Primary key
- `plan` (CharField): Plan name
- `project_app` (ForeignKey): Reference to ProjectApp
- `organization` (ForeignKey): Reference to Organization
- `state` (ForeignKey): Reference to State
- `district` (ForeignKey): Reference to District
- `block` (ForeignKey): Reference to Block
- `village_name` (CharField): Village name
- `gram_panchayat` (CharField): Gram Panchayat name
- `created_by` (ForeignKey): User who created the plan
- `created_at` (DateTimeField): Creation timestamp
- `updated_at` (DateTimeField): Last update timestamp
- `updated_by` (ForeignKey): User who last updated the plan

**Relationships:**
- Many-to-one with ProjectApp
- Many-to-one with Organization
- Many-to-one with State, District, and Block (geographical hierarchy)

## User Roles and Permissions

### User Creation and Authentication

1. **User Registration**:
   - Users can register through the `/api/v1/auth/register/` endpoint
   - Required fields include username, email, password, and optional organization ID
   - Upon registration, a JWT token is issued for authentication

2. **User Login**:
   - Users login through the `/api/v1/auth/login/` endpoint
   - Successful login returns a JWT access token and refresh token

3. **User Logout**:
   - Users logout through the `/api/v1/auth/logout/` endpoint
   - The refresh token is blacklisted to prevent reuse

### User Roles

The system implements a hierarchical role-based access control system:

1. **Super Admin**:
   - Created by setting `is_superadmin=True` on a User account
   - Has full access to all organizations, projects, and system features
   - Can create and manage organizations
   - Can assign organization admins

2. **Organization Admin**:
   - Assigned to the "Organization Admin" Django Group
   - Can manage users within their organization
   - Can create and manage projects within their organization
   - Can assign project roles to users within their organization

3. **Project-specific Roles**:
   - Managed through the UserProjectGroup table
   - Users can have different roles in different projects
   - Permissions are defined at the project level through Django's Group model
   - Common project roles might include:
     - Project Manager: Can manage all aspects of a specific project
     - Data Entry: Can upload data but not modify project settings
     - Viewer: Can only view project data

### Permission Checking

1. **Organization-level Permissions**:
   - Super admins have access to all organizations
   - Users can only access their assigned organization

2. **Project-level Permissions**:
   - Checked through the `has_project_permission` method on the User model
   - Super admins have access to all projects
   - Organization admins have access to all projects in their organization
   - Other users have access based on their project roles

3. **Feature-specific Permissions**:
   - Controlled by Django's permission system
   - Each app type (plantation, watershed) has its own set of permissions
   - Permissions are assigned to Groups, which are then assigned to users in the context of specific projects

## Key Relationships

1. **Organization → Projects**:
   - An organization can have multiple projects
   - Projects belong to exactly one organization

2. **Project → ProjectApp**:
   - A project can have multiple app types enabled (plantation, watershed)
   - Each app type is represented by a ProjectApp record

3. **User → Organization**:
   - A user belongs to one organization
   - An organization can have multiple users

4. **User → Project**:
   - Users are assigned to projects through the UserProjectGroup table
   - A user can have different roles in different projects

5. **ProjectApp → Data (KML Files, Plans)**:
   - Each ProjectApp can have its own data
   - Plantation apps have KML files
   - Watershed apps have plans

