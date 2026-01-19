# API Documentation

This document provides a comprehensive overview of the Core Stack Backend API endpoints, their functionality, and usage.

## Authentication Endpoints

### Authentication Flow
The API uses JWT (JSON Web Tokens) for authentication. Here's how the authentication flow works:

1. **Registration/Login**: 
   - When a user registers or logs in, they receive both an access token and a refresh token.

2. **Using Access Tokens**:
   - The access token must be included in the Authorization header for all authenticated API requests:
   ```
   Authorization: Bearer <your_access_token>
   ```
   - Access tokens are valid for 2 days.

3. **Token Refresh**:
   - When an access token expires, use the refresh token to obtain a new one.
   - Send a POST request to the token refresh endpoint with the refresh token.
   - You'll receive a new access token (and potentially a new refresh token).
   - Refresh tokens are valid for 14 days.

4. **Logout**:
   - When logging out, send the refresh token to the logout endpoint to invalidate it.
   - This prevents the refresh token from being used to obtain new access tokens.

### User Registration
- **URL**: `/api/v1/auth/register/`
- **Method**: POST
- **Description**: Register a new user in the system
- **Request Body**:
  ```json
  {
    "username": "user123",
    "email": "user@example.com",
    "password": "securepassword",
    "password_confirm": "securepassword",
    "first_name": "John",
    "last_name": "Doe",
    "contact_number": "1234567890",
    "organization": "optional-organization-uuid"
  }
  ```
- **Response**: Returns the created user object along with access and refresh tokens
- **Notes**: 
  - Organization ID is optional
  - If provided, the user will be associated with that organization

### Get Available Organizations for Registration
- **URL**: `/api/v1/auth/register/available_organizations/`
- **Method**: GET
- **Description**: Get a list of organizations that users can select during registration
- **Authentication**: Not required
- **Response**: List of active organizations with their IDs and names

### User Login
- **URL**: `/api/v1/auth/login/`
- **Method**: POST
- **Description**: Authenticate a user and obtain JWT tokens
- **Request Body**:
  ```json
  {
    "username": "user123",
    "password": "securepassword"
  }
  ```
- **Response**: Returns access token, refresh token, and user details
- **Notes**: The access token must be included in the Authorization header for subsequent API calls

### Token Refresh
- **URL**: `/api/v1/auth/token/refresh/`
- **Method**: POST
- **Description**: Obtain a new access token using a valid refresh token
- **Request Body**:
  ```json
  {
    "refresh": "your-refresh-token"
  }
  ```
- **Response**: Returns a new access token and, if configured, a new refresh token
- **Notes**: 
  - Use this endpoint when your access token expires but your refresh token is still valid
  - With the current configuration, refresh tokens are valid for 14 days while access tokens are valid for 2 days
  - The system is configured with token rotation, meaning you'll receive a new refresh token with each refresh
  - The old refresh token is automatically blacklisted after use

### User Logout
- **URL**: `/api/v1/auth/logout/`
- **Method**: POST
- **Description**: Logout a user by blacklisting their refresh token
- **Request Body**:
  ```json
  {
    "refresh_token": "your-refresh-token"
  }
  ```
- **Response**: Success message
- **Authentication**: Required
- **Notes**: This invalidates the refresh token, preventing it from being used to obtain new access tokens

### Change Password
- **URL**: `/api/v1/users/change_password/`
- **Method**: POST
- **Description**: Change the authenticated user's password
- **Request Body**:
  ```json
  {
    "old_password": "current-password",
    "new_password": "new-secure-password",
    "new_password_confirm": "new-secure-password"
  }
  ```
- **Response**: Success message
- **Authentication**: Required
- **Notes**: 
  - The user must provide their current password correctly
  - The new password must meet the system's password requirements
  - After password change, all refresh tokens are invalidated (user will be logged out from all devices)
  - The user will need to log in again with the new password

## User Management Endpoints

### List Users
- **URL**: `/api/v1/users/`
- **Method**: GET
- **Description**: List users based on permissions
- **Authentication**: Required
- **Notes**:
  - Super admins see all users
  - Organization admins see users in their organization
  - Regular users see only themselves

### Create User
- **URL**: `/api/v1/users/`
- **Method**: POST
- **Description**: Create a new user (admin function)
- **Request Body**:
  ```json
  {
    "username": "newuser",
    "email": "newuser@example.com",
    "password": "securepassword",
    "organization": "organization-id",
    "is_superadmin": false
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin or organization admin
- **Notes**: Organization admins can only create users in their own organization

### Get User Details
- **URL**: `/api/v1/users/{user_id}/`
- **Method**: GET
- **Description**: Get details of a specific user
- **Authentication**: Required
- **Permissions**: 
  - Users can view their own details
  - Organization admins can view users in their organization
  - Super admins can view any user

### Update User
- **URL**: `/api/v1/users/{user_id}/`
- **Method**: PUT/PATCH
- **Description**: Update user details
- **Authentication**: Required
- **Permissions**: 
  - Users can update their own details
  - Organization admins can update users in their organization
  - Super admins can update any user
- **Notes**: Certain fields (is_superadmin, is_staff) can only be modified by super admins

### Set User Organization
- **URL**: `/api/v1/users/{user_id}/set_organization/`
- **Method**: PUT
- **Description**: Assign a user to an organization
- **Request Body**:
  ```json
  {
    "organization_id": "organization-uuid"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin or organization admin of the target organization

### Set User Group
- **URL**: `/api/v1/users/{user_id}/set_group/`
- **Method**: PUT
- **Description**: Assign a user to a group/role (e.g., Organization Admin)
- **Request Body**:
  ```json
  {
    "group_id": "group-id"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin only
- **Notes**: 
  - For Organization Admin group, the user must already be assigned to an organization
  - Special validation is performed for Organization Admin assignments

### Remove User from Group
- **URL**: `/api/v1/users/{user_id}/remove_group/`
- **Method**: PUT
- **Description**: Remove a user from a group/role
- **Request Body**:
  ```json
  {
    "group_id": "group-id"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin only

### Get My Projects
- **URL**: `/api/v1/users/my_projects/`
- **Method**: GET
- **Description**: Get all projects the current user is assigned to with their roles
- **Authentication**: Required
- **Response Body**:
  ```json
  [
    {
      "project": {
        "id": 1,
        "name": "Project Name",
        "description": "Project Description",
        "app_type": "plantation",
        "enabled": true,
        "organization": "organization-uuid",
        "organization_name": "Organization Name"
      },
      "role": {
        "id": 2,
        "name": "Project Manager"
      }
    }
  ]
  ```
- **Notes**: This endpoint allows users to see all projects they have been assigned to along with their role in each project

## Organization Endpoints

### List Organizations
- **URL**: `/api/v1/organizations/`
- **Method**: GET
- **Description**: List organizations based on permissions
- **Authentication**: Required
- **Notes**:
  - Super admins see all organizations
  - Other users see only their organization

### Create Organization
- **URL**: `/api/v1/organizations/`
- **Method**: POST
- **Description**: Create a new organization
- **Request Body**:
  ```json
  {
    "name": "Organization Name",
    "description": "Organization Description"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin only

### Get Organization Details
- **URL**: `/api/v1/organizations/{organization_id}/`
- **Method**: GET
- **Description**: Get details of a specific organization
- **Authentication**: Required
- **Permissions**: 
  - Super admins can view any organization
  - Users can view their own organization

### Update Organization
- **URL**: `/api/v1/organizations/{organization_id}/`
- **Method**: PUT/PATCH
- **Description**: Update organization details
- **Authentication**: Required
- **Permissions**: 
  - Super admins can update any organization
  - Organization admins can update their own organization

## Project Endpoints

### List Projects
- **URL**: `/api/v1/projects/`
- **Method**: GET
- **Description**: List projects based on permissions
- **Authentication**: Required
- **Notes**:
  - Super admins see all projects
  - Organization admins see all projects in their organization
  - Other users see projects they have access to

### Create Project
- **URL**: `/api/v1/projects/`
- **Method**: POST
- **Description**: Create a new project
- **Authentication**: Required
- **Permissions**: Super admin or organization admin

#### For Regular Users (Organization Members)

Regular users create projects under their own organization automatically.

**Request Body**:

```json
{
  "name": "Project Name",
  "description": "Project Description",
  "state_soi": 1,
  "district_soi": 10,
  "tehsil_soi": 100,
  "app_type": "plantation",
  "enabled": true,
  "created_by": "user-id",
  "updated_by": "user-id"
}
```

#### For Superadmins

Superadmins must specify the organization ID since they can create projects for any organization.

**Request Body**:

```json
{
  "name": "Project Name",
  "description": "Project Description",
  "organization": "org-id",
  "state_soi": 1,
  "district_soi": 10,
  "tehsil_soi": 100,
  "app_type": "plantation",
  "enabled": true,
  "created_by": "user-id",
  "updated_by": "user-id"
}
```

**Notes**:

- For regular users, the organization is automatically set to the user's organization
- For superadmins, the organization field is required and must be provided
- `state_soi` is the State SOI ID from geoadmin
- `district_soi` is the District SOI ID from geoadmin (optional)
- `tehsil_soi` is the Tehsil SOI ID from geoadmin (optional)
- Valid app_type values include 'plantation', 'watershed', etc. (as defined in AppType choices)
- The enabled field defaults to true if not specified

### Get Project Details
- **URL**: `/api/v1/projects/{project_id}/`
- **Method**: GET
- **Description**: Get details of a specific project
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Update Project
- **URL**: `/api/v1/projects/{project_id}/`
- **Method**: PUT/PATCH
- **Description**: Update project details
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager

### Delete Project
- **URL**: `/api/v1/projects/{project_id}/`
- **Method**: DELETE
- **Description**: Delete a project
- **Authentication**: Required
- **Permissions**: Super admin or organization admin

### Enable Project
- **URL**: `/api/v1/projects/{project_id}/enable/`
- **Method**: POST
- **Description**: Enable a project (sets enabled=True)
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or user with project access
- **Response**: Returns the updated project object
- **Notes**: 
  - Updates the `updated_by` field to the current user
  - Updates the `updated_at` timestamp

### Disable Project
- **URL**: `/api/v1/projects/{project_id}/disable/`
- **Method**: POST
- **Description**: Disable a project (sets enabled=False)
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or user with project access
- **Response**: Returns the updated project object
- **Notes**: 
  - Updates the `updated_by` field to the current user
  - Updates the `updated_at` timestamp
  - Disabled projects are not included in project listings by default

## Project App Type Management

### Update Project App Type
- **URL**: `/api/v1/projects/{project_id}/update_app_type/`
- **Method**: PATCH
- **Description**: Update a project's app type and enabled status
- **Request Body**:
  ```json
  {
    "app_type": "plantation",
    "enabled": true
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager
- **Notes**: 
  - Valid app types include 'plantation', 'watershed', etc. (as defined in AppType choices)
  - The enabled field controls whether the app functionality is active for the project

## Project User Management Endpoints

### List Project Users
- **URL**: `/api/v1/projects/{project_id}/users/`
- **Method**: GET
- **Description**: List users assigned to a project with their roles
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Assign User to Project
- **URL**: `/api/v1/projects/{project_id}/users/`
- **Method**: POST
- **Description**: Assign a user to a project with a specific role
- **Request Body**:
  ```json
  {
    "user": "user-id",
    "group": "group-id"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager
- **Notes**: The group ID represents the role (e.g., Project Manager, Data Entry, Viewer)

### Update User Project Role
- **URL**: `/api/v1/projects/{project_id}/users/{user_project_id}/`
- **Method**: PUT/PATCH
- **Description**: Update a user's role in a project
- **Request Body**:
  ```json
  {
    "group": "new-group-id"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager

### Remove User from Project
- **URL**: `/api/v1/projects/{project_id}/users/{user_project_id}/`
- **Method**: DELETE
- **Description**: Remove a user from a project (removes their role/group assignment)
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager
- **Notes**: The `user_project_id` is the ID of the user-project assignment record, not the user ID. To find this ID, first list all users for the project using the List Project Users endpoint.

## Plantation App Endpoints

### List KML Files
- **URL**: `/api/v1/projects/{project_id}/plantation/kml/`
- **Method**: GET
- **Description**: List KML files uploaded for a plantation project
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Upload KML File
- **URL**: `/api/v1/projects/{project_id}/plantation/kml/`
- **Method**: POST
- **Description**: Upload a new KML file for a plantation project
- **Request Body**: Multipart form data with 'file' and optional 'name'
- **Authentication**: Required
- **Permissions**: User must have upload permission for the project
- **Notes**:
  - Only .kml files are accepted
  - Duplicate files (same hash) are rejected
  - The file is automatically converted to GeoJSON
  - The project's merged GeoJSON file is updated

### Get KML File Details
- **URL**: `/api/v1/projects/{project_id}/plantation/kml/{kml_id}/`
- **Method**: GET
- **Description**: Get details of a specific KML file
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Delete KML File
- **URL**: `/api/v1/projects/{project_id}/plantation/kml/{kml_id}/`
- **Method**: DELETE
- **Description**: Delete a KML file
- **Authentication**: Required
- **Permissions**: User must have delete permission for the project
- **Notes**: The project's merged GeoJSON file is updated after deletion

## Watershed Planning Endpoints

### List Watershed Plans (Project Level)
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/`
- **Method**: GET
- **Description**: List watershed plans for a specific project
- **Authentication**: Required
- **Permissions**:
    - Superadmins: Can access any project
    - Org Admins: Can access projects in their organization
    - App Users: Can access assigned projects only

### List Watershed Plans (Organization Level)

- **URL**: `/api/v1/organizations/{organization_id}/watershed/plans/`
- **Method**: GET
- **Description**: List all watershed plans for a specific organization
- **Authentication**: Required
- **Permissions**: Superadmins only

### List Watershed Plans (Global Level)

- **URL**: `/api/v1/watershed/plans/`
- **Method**: GET
- **Description**: List all watershed plans across all organizations and projects
- **Authentication**: Required
- **Permissions**: Superadmins only
- **Query Parameters**:
    - `tehsil`: Filter plans by tehsil ID (e.g., `?tehsil=123`)
    - `district`: Filter plans by district ID (e.g., `?district=456`)
    - `state`: Filter plans by state ID (e.g., `?state=789`)

### Create Watershed Plan
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/`
- **Method**: POST
- **Description**: Create a new watershed plan for a specific project
- **Authentication**: Required
- **Permissions**:
    - Superadmins: Can create plans in any project
    - Org Admins: Can create plans in their organization's projects
    - Project Users: Must have 'add_watershed' permission for the project

#### Required Fields:

- `plan` (string): Name of the watershed plan
- `state_soi` (integer): State SOI ID from geoadmin
- `district_soi` (integer): District SOI ID from geoadmin
- `tehsil_soi` (integer): Tehsil SOI ID from geoadmin
- `village_name` (string): Name of the village
- `gram_panchayat` (string): Name of the gram panchayat
- `facilitator_name` (string): Name of the plan facilitator

#### Optional Fields:

- `enabled` (boolean): Whether the plan is enabled (default: true)
- `is_completed` (boolean): Whether the plan is completed (default: false)
- `is_dpr_generated` (boolean): Whether DPR is generated (default: false)
- `is_dpr_reviewed` (boolean): Whether DPR is reviewed (default: false)
- `is_dpr_approved` (boolean): Whether DPR is approved (default: false)
- `latitude` (decimal): Latitude coordinate (optional)
- `longitude` (decimal): Longitude coordinate (optional)

#### Auto-set Fields:

- `project`: Set automatically from URL parameter
- `organization`: Set automatically from project's organization
- `created_by`: Set automatically from authenticated user
- `created_at`, `updated_at`: Set automatically by system

#### Request Examples:

**Minimal Plan Creation:**

```json
{
  "plan": "Basic Watershed Plan 2025",
  "state_soi": 1,
  "district_soi": 10,
  "tehsil_soi": 100,
  "village_name": "Example Village",
  "gram_panchayat": "Example GP",
  "facilitator_name": "John Doe"
}
```

**Complete Plan Creation:**

```json
{
  "plan": "Comprehensive Watershed Management Plan 2025",
  "state_soi": 1,
  "district_soi": 10,
  "tehsil_soi": 100,
  "village_name": "Hauz Khas Village",
  "gram_panchayat": "Hauz Khas Gram Panchayat",
  "facilitator_name": "Dr. Rajesh Kumar",
  "enabled": true,
  "is_completed": false,
  "is_dpr_generated": false,
  "is_dpr_reviewed": false,
  "is_dpr_approved": false,
  "latitude": 28.5494,
  "longitude": 77.1960
}
```

#### Response:

```json
{
  "plan_data": {
    "id": 296,
    "plan": "Comprehensive Watershed Management Plan 2025",
    "facilitator_name": "Dr. Rajesh Kumar",
    "village_name": "Hauz Khas Village",
    "gram_panchayat": "Hauz Khas Gram Panchayat",
    "created_at": "2025-01-18T10:30:00.000000+05:30",
    "updated_at": "2025-01-18T10:30:00.000000+05:30",
    "enabled": true,
    "is_completed": false,
    "is_dpr_generated": false,
    "is_dpr_reviewed": false,
    "is_dpr_approved": false,
    "latitude": 28.5494,
    "longitude": 77.1960,
    "project": 10,
    "project_name": "Delhi Watershed Project",
    "organization": "2e4fed85-39d2-4691-a7dd-f5cf70a78ec6",
    "organization_name": "Delhi Development Authority",
    "state_soi": 1,
    "district_soi": 10,
    "tehsil_soi": 100,
    "created_by": 1,
    "created_by_name": "John Doe",
    "updated_by": null
  },
  "message": "Successfully created the watershed plan, Comprehensive Watershed Management Plan 2025"
}
```
- **Permissions**: User must have create permission for the project

### Get Watershed Plan Details
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/{plan_id}/`
- **Method**: GET
- **Description**: Get details of a specific watershed plan
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Update Watershed Plan
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/{plan_id}/`
- **Method**: PUT/PATCH
- **Description**: Update an existing watershed plan
- **Authentication**: Required
- **Permissions**:
    - Superadmins: Can update any plan in any project
    - Org Admins: Can update plans in their organization's projects
    - Project Users: Must have 'change_watershed' permission for the project

#### Updatable Fields:

- `plan` (string): Name of the watershed plan
- `state_soi` (integer): State SOI ID from geoadmin
- `district_soi` (integer): District SOI ID from geoadmin
- `tehsil_soi` (integer): Tehsil SOI ID from geoadmin
- `village_name` (string): Name of the village
- `gram_panchayat` (string): Name of the gram panchayat
- `facilitator_name` (string): Name of the plan facilitator
- `enabled` (boolean): Whether the plan is enabled
- `is_completed` (boolean): Whether the plan is completed
- `is_dpr_generated` (boolean): Whether DPR is generated
- `is_dpr_reviewed` (boolean): Whether DPR is reviewed
- `is_dpr_approved` (boolean): Whether DPR is approved
- `latitude` (decimal): Latitude coordinate
- `longitude` (decimal): Longitude coordinate

#### Non-updatable Fields:

- `project`: Cannot be changed after creation
- `organization`: Cannot be changed after creation
- `created_by`, `created_at`: Cannot be modified
- `updated_by`, `updated_at`: Set automatically by system

#### Update Methods:

**PATCH (Partial Update)** - Update only specific fields:

```json
{
  "is_completed": true,
  "is_dpr_generated": true,
  "facilitator_name": "Updated Facilitator Name"
}
```

**PUT (Full Update)** - Provide all required fields:

```json
{
  "plan": "Updated Watershed Management Plan 2025",
  "state_soi": 1,
  "district_soi": 10,
  "tehsil_soi": 100,
  "village_name": "Updated Village Name",
  "gram_panchayat": "Updated GP Name",
  "facilitator_name": "Dr. Updated Facilitator",
  "enabled": true,
  "is_completed": true,
  "is_dpr_generated": true,
  "is_dpr_reviewed": false,
  "is_dpr_approved": false
}
```

#### Response:

```json
{
  "plan_data": {
    "id": 295,
    "plan": "Updated Watershed Management Plan 2025",
    "facilitator_name": "Dr. Updated Facilitator",
    "village_name": "Updated Village Name",
    "gram_panchayat": "Updated GP Name",
    "created_at": "2025-01-18T00:52:51.479180+05:30",
    "updated_at": "2025-01-18T15:30:00.000000+05:30",
    "enabled": true,
    "is_completed": true,
    "is_dpr_generated": true,
    "is_dpr_reviewed": false,
    "is_dpr_approved": false,
    "latitude": null,
    "longitude": null,
    "project": 10,
    "project_name": "Delhi Watershed Project",
    "organization": "2e4fed85-39d2-4691-a7dd-f5cf70a78ec6",
    "organization_name": "Delhi Development Authority",
    "state_soi": 1,
    "district_soi": 10,
    "tehsil_soi": 100,
    "created_by": 1,
    "created_by_name": "John Doe",
    "updated_by": 2
  },
  "message": "Successfully updated the watershed plan: Updated Watershed Management Plan 2025"
}
```

### Delete Watershed Plan
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/{plan_id}/`
- **Method**: DELETE
- **Description**: Delete a watershed plan
- **Authentication**: Required
- **Permissions**: User must have delete permission for the project

## Legacy Plan Endpoints

These endpoints are maintained for backward compatibility:

### Get Plans
- **URL**: `/api/v1/get_plans/`
- **Method**: GET
- **Description**: Get all plans

### Add Plan
- **URL**: `/api/v1/add_plan/`
- **Method**: POST
- **Description**: Add a new plan

### Add Resources
- **URL**: `/api/v1/add_resources/`
- **Method**: POST
- **Description**: Add resources to a plan

### Add Works
- **URL**: `/api/v1/add_works/`
- **Method**: POST
- **Description**: Add works to a plan

### Sync Offline Data
- **URL**: `/api/v1/sync_offline_data/{resource_type}/`
- **Method**: POST
- **Description**: Synchronize offline data for a specific resource type

## API Usage Examples

### User Registration and Login Flow

1. Register a new user:
   ```bash
   curl -X POST http://api.example.com/api/v1/auth/register/ \
     -H "Content-Type: application/json" \
     -d '{"username": "newuser", "email": "newuser@example.com", "password": "securepassword"}'
   ```

2. Login with the new user:
   ```bash
   curl -X POST http://api.example.com/api/v1/auth/login/ \
     -H "Content-Type: application/json" \
     -d '{"username": "newuser", "password": "securepassword"}'
   ```

3. Refresh your access token when it expires:
   ```bash
   curl -X POST http://api.example.com/api/v1/auth/token/refresh/ \
     -H "Content-Type: application/json" \
     -d '{"refresh": "your-refresh-token"}'
   ```

4. Use the access token for authenticated requests:
   ```bash
   curl -X GET http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}"
   ```

### KML File Upload Process

1. Create a project (if not already created):

   **For Regular Users:**
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Plantation Project", 
       "description": "A new plantation project", 
       "state_soi": 1,
       "district_soi": 10,
       "tehsil_soi": 100,
       "app_type": "plantation"
     }'
   ```

   **For Superadmins:**
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Plantation Project", 
       "description": "A new plantation project", 
       "organization": "organization-id",
       "state_soi": 1,
       "district_soi": 10,
       "tehsil_soi": 100,
       "app_type": "plantation"
     }'
   ```

2. Enable the plantation app for the project:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/apps/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"app_type": "plantation", "enabled": true}'
   ```

3. Upload a KML file:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/plantation/kml/ \
     -H "Authorization: Bearer {access_token}" \
     -F "file=@/path/to/plantation.kml" \
     -F "name=Plantation Boundaries"
   ```

4. View the uploaded KML files:
   ```bash
   curl -X GET http://api.example.com/api/v1/projects/{project_id}/plantation/kml/ \
     -H "Authorization: Bearer {access_token}"
   ```

### Managing Project Status

1. Enable a project:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/enable/ \
     -H "Authorization: Bearer {access_token}"
   ```

2. Disable a project:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/disable/ \
     -H "Authorization: Bearer {access_token}"
   ```

### Creating a Watershed Plan

1. Create a project (if not already created):

   **For Regular Users:**
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Watershed Project", 
       "description": "A new watershed project", 
       "state_soi": 1,
       "app_type": "watershed"
     }'
   ```

   **For Superadmins:**
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Watershed Project", 
       "description": "A new watershed project", 
       "organization": "organization-id",
       "state_soi": 1,
       "app_type": "watershed"
     }'
   ```

2. Enable the watershed app for the project:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/apps/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"app_type": "watershed", "enabled": true}'
   ```

3. Create a watershed plan:
    3. Create a watershed plan for the project:
       ```bash
       # Minimal required fields
       curl -X POST http://api.example.com/api/v1/projects/{project_id}/watershed/plans/ \
         -H "Authorization: Bearer {access_token}" \
         -H "Content-Type: application/json" \
         -d '{
           "plan": "Basic Watershed Plan 2025",
           "state_soi": 1,
           "district_soi": 10,
           "tehsil_soi": 100,
           "village_name": "Example Village",
           "gram_panchayat": "Example GP",
           "facilitator_name": "John Doe"
         }'
    
       # Complete plan with all optional fields
       curl -X POST http://api.example.com/api/v1/projects/{project_id}/watershed/plans/ \
         -H "Authorization: Bearer {access_token}" \
         -H "Content-Type: application/json" \
         -d '{
           "plan": "Comprehensive Watershed Management Plan 2025",
           "state_soi": 1,
           "district_soi": 10,
           "tehsil_soi": 100,
           "village_name": "Hauz Khas Village",
           "gram_panchayat": "Hauz Khas Gram Panchayat",
           "facilitator_name": "Dr. Rajesh Kumar",
           "enabled": true,
           "is_completed": false,
           "is_dpr_generated": false,
           "is_dpr_reviewed": false,
           "is_dpr_approved": false,
           "latitude": 28.5494,
           "longitude": 77.1960
         }'
       ```

4. View the created watershed plans:
   ```bash
   # View plans for a specific project
   curl -X GET http://api.example.com/api/v1/projects/{project_id}/watershed/plans/ \
     -H "Authorization: Bearer {access_token}"
   
   # View all plans for an organization (superadmin only)
   curl -X GET http://api.example.com/api/v1/organizations/{organization_id}/watershed/plans/ \
     -H "Authorization: Bearer {access_token}"
   
   # View all plans globally (superadmin only)
   curl -X GET http://api.example.com/api/v1/watershed/plans/ \
     -H "Authorization: Bearer {access_token}"
   
   # View plans filtered by tehsil (superadmin only)
   curl -X GET http://api.example.com/api/v1/watershed/plans/?tehsil=100 \
     -H "Authorization: Bearer {access_token}"
   ```

5. Update a watershed plan:
   ```bash
   # Partial update - only update specific fields
   curl -X PATCH http://api.example.com/api/v1/projects/{project_id}/watershed/plans/{plan_id}/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "is_completed": true,
       "is_dpr_generated": true,
       "facilitator_name": "Updated Facilitator"
     }'
   
   # Full update - provide all fields
   curl -X PUT http://api.example.com/api/v1/projects/{project_id}/watershed/plans/{plan_id}/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "plan": "Updated Watershed Plan 2025",
       "state_soi": 1,
       "district_soi": 10,
       "tehsil_soi": 100,
       "village_name": "Updated Village",
       "gram_panchayat": "Updated GP",
       "facilitator_name": "New Facilitator",
       "enabled": true,
       "is_completed": true,
       "is_dpr_generated": true,
       "is_dpr_reviewed": false,
       "is_dpr_approved": false
     }'
   ```

## Superadmin Watershed Plan Access Patterns

Superadmins have multiple ways to access watershed plans depending on their context and needs:

### Use Cases

1. **Global Overview**: Get all plans across all organizations
   ```
   GET /api/v1/watershed/plans/
   ```

2. **Organization Focus**: Get all plans for a specific organization
   ```
   GET /api/v1/organizations/{organization_id}/watershed/plans/
   ```

3. **Project Specific**: Get plans for a specific project
   ```
   GET /api/v1/projects/{project_id}/watershed/plans/
   ```

4. **Geographical Filtering**: Filter plans by location (useful when working from partner locations)
   ```
   GET /api/v1/watershed/plans/?tehsil=100
   GET /api/v1/watershed/plans/?district=10
   GET /api/v1/watershed/plans/?state=1
   ```

### Superadmin vs Organization Admin Access

- **Superadmins**: Can access any endpoint and see plans from any organization/project
- **Organization Admins**: Limited to their organization's plans through existing project-level endpoints
- **Regular Users**: Limited to plans from projects they're assigned to

## API Security

1. **Authentication**: All API endpoints (except registration and login) require JWT authentication.
2. **Authorization**: Permissions are checked at multiple levels:
   - Organization level
   - Project level
   - Feature-specific permissions
3. **Token Expiration**: Access tokens expire after 2 days
4. **Token Refresh**: Refresh tokens (valid for 14 days) can be used to obtain new access tokens
5. **Token Blacklisting**: Refresh tokens are blacklisted on logout or when used for refresh
6. **Token Rotation**: When refreshing a token, a new refresh token is issued and the old one is blacklisted

## Permission Structure

Below are ASCII diagrams showing the permission hierarchy and capabilities for different user roles in the system:

```
+----------------+     +----------------+     +----------------+
|   Superadmin   |     |    Org Admin   |     | Project Manager|
+----------------+     +----------------+     +----------------+
| - Access all   |     | - Access org   |     | - Access       |
|   organizations|     |   projects     |     |   assigned     |
| - Access all   |     | - Create/edit  |     |   projects     |
|   projects     |     |   projects     |     | - Manage       |
| - Create/edit  |     | - Manage users |     |   project      |
|   organizations|     |   in org       |     |   users        |
| - Manage all   |     | - Assign users |     | - Create/edit  |
|   users        |     |   to projects  |     |   project data |
| - Assign any   |     | - Cannot access|     | - Cannot       |
|   role         |     |   other orgs   |     |   create       |
+----------------+     +----------------+     |   projects     |
                                              +----------------+
                                                      |
                                                      v
                                              +----------------+
                                              |    App User    |
                                              +----------------+
                                              | - View assigned|
                                              |   projects     |
                                              | - Enter data   |
                                              |   based on     |
                                              |   permissions  |
                                              | - Cannot       |
                                              |   manage users |
                                              | - Cannot       |
                                              |   create/edit  |
                                              |   projects     |
                                              +----------------+
```

### Permission Comparison Table

| Capability                   | Superadmin | Org Admin | Project Manager | App User |
|------------------------------|------------|-----------|-----------------|----------|
| Access all organizations     | ✓          | ✗         | ✗               | ✗        |
| Access organization projects | ✓          | ✓         | ✗               | ✗        |
| Access assigned projects     | ✓          | ✓         | ✓               | ✓        |
| Create organizations         | ✓          | ✗         | ✗               | ✗        |
| Create projects              | ✓          | ✓         | ✗               | ✗        |
| Edit project details         | ✓          | ✓         | ✓               | ✗        |
| Manage all users             | ✓          | ✗         | ✗               | ✗        |
| Manage org users             | ✓          | ✓         | ✗               | ✗        |
| Manage project users         | ✓          | ✓         | ✓               | ✗        |
| Upload project data          | ✓          | ✓         | ✓               | ✓        |
| Delete project data          | ✓          | ✓         | ✓               | ✗        |
| Assign superadmin role       | ✓          | ✗         | ✗               | ✗        |
| Assign org admin role        | ✓          | ✗         | ✗               | ✗        |
| Assign project roles         | ✓          | ✓         | ✓               | ✗        |
| View global watershed plans  | ✓          | ✗         | ✗               | ✗        |
| View org watershed plans     | ✓          | ✗         | ✗               | ✗        |
| Filter plans by geography    | ✓          | ✗         | ✗               | ✗        |
