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

### Forgot Password
- **URL**: `/api/v1/auth/forgot-password/`
- **Method**: POST
- **Description**: Request a password reset link. The client sends the username; if the account has an email on file the reset link is sent immediately. If not, the API responds asking for an email, and the client re-submits with both username and email.
- **Authentication**: Not required
- **Step 1 — Request Body**:
  ```json
  {
    "username": "user123"
  }
  ```
- **Step 1 — Success Response** (`200 OK`): Reset link sent to the email on file.
- **Step 1 — No email on file** (`400 Bad Request`):
  ```json
  {
    "detail": "No email address on file for this account. Please provide an email to receive the reset link.",
    "email_required": true
  }
  ```
- **Step 2 (if `email_required`)** — Re-submit with email:
  ```json
  {
    "username": "user123",
    "email": "user@example.com"
  }
  ```
  The provided email is saved to the user's profile and the reset link is sent to it.
- **Notes**:
  - The reset link is valid for 3 days (`PASSWORD_RESET_TIMEOUT`)
  - `username` is always required
  - `email` is only needed when the account has no email on file
  - The provided email is persisted on the user profile so future resets work directly

### Reset Password (via link)
- **URL**: `/api/v1/auth/reset-password/<uidb64>/<token>/`
- **Methods**: GET, POST
- **Description**: Backend-served HTML page for resetting a password using the link from the forgot-password email
- **Authentication**: Not required (token-based validation)
- **GET**: Renders a form to enter a new password
- **POST**: Validates the token and sets the new password
- **Notes**:
  - The token is single-use — once the password is reset, the same link cannot be reused
  - All existing JWT sessions are invalidated after a successful reset
  - Password must be at least 8 characters

### Admin Reset Password
- **URL**: `/api/v1/users/{user_id}/reset_password/`
- **Method**: POST
- **Description**: Allows an org admin or superadmin to reset another user's password (useful when the user has no email)
- **Request Body**:
  ```json
  {
    "new_password": "new-secure-password"
  }
  ```
- **Response**: Success message
- **Authentication**: Required
- **Permissions**:
  - Superadmins can reset any user's password
  - Organization admins can only reset passwords for users in their organization
- **Notes**:
  - The new password must meet the system's password validation requirements
  - All of the target user's JWT sessions are invalidated after reset

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
- **Query Parameters**:
  - `organization` (optional, superadmin only): Filter projects by organization ID
- **Notes**:
  - Super admins see all projects (can filter by organization using `?organization=<org_id>`)
  - Organization admins see all projects in their organization
  - Other users see projects they have access to

**Example (Superadmin filtering by organization)**:
```
GET /api/v1/projects/?organization=5
```

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

### List Disabled Projects
- **URL**: `/api/v1/projects/disabled/`
- **Method**: GET
- **Description**: Retrieve all disabled projects based on user permissions
- **Authentication**: Required
- **Permissions**: 
  - Super admins: Can see all disabled projects across all organizations
  - Organization users: Can see only their organization's disabled projects
  - Users without organization: Receive empty result
- **Response**: Returns an array of disabled project objects
- **Response Body**:
  ```json
  [
    {
      "id": 1,
      "name": "Disabled Project Name",
      "description": "Project Description",
      "app_type": "plantation",
      "enabled": false,
      "organization": "organization-uuid",
      "organization_name": "Organization Name",
      "state_soi": 1,
      "district_soi": 10,
      "tehsil_soi": 100,
      "created_at": "2025-01-15T10:30:00.000000+05:30",
      "updated_at": "2025-01-20T15:45:00.000000+05:30",
      "created_by": 1,
      "updated_by": 2
    }
  ]
  ```
- **Notes**: 
  - This endpoint provides visibility into projects that have been disabled
  - Useful for auditing and re-enabling previously disabled projects

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
- **Query Parameters**:
    - `filter_test_plan=true` (optional): Exclude plans whose name contains "test" or "demo"
- **Notes**:
    - Test/demo plans are included by default. Pass `?filter_test_plan=true` to exclude them.
    - Users in the **Test Plan Reviewer** group see only test/demo plans, regardless of this parameter.

### List Watershed Plans (Organization Level)

- **URL**: `/api/v1/organizations/{organization_id}/watershed/plans/`
- **Method**: GET
- **Description**: List all watershed plans for a specific organization
- **Authentication**: Required
- **Permissions**: Superadmins only
- **Query Parameters**:
    - `filter_test_plan=true` (optional): Exclude plans whose name contains "test" or "demo"
- **Notes**: Test/demo plans are included by default. Pass `?filter_test_plan=true` to exclude them.

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
    - `filter_test_plan=true` (optional): Exclude plans whose name contains "test" or "demo"
- **Notes**: Test/demo plans are included by default. Pass `?filter_test_plan=true` to exclude them.

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

### Meta Stats (Global Level)
- **URL**: `/api/v1/watershed/plans/meta-stats/`
- **Method**: GET
- **Description**: Get comprehensive statistics about all watershed plans globally. Excludes test/demo plans. Only accessible to superadmins and API key users.
- **Authentication**: Required (JWT or API Key)
- **Permissions**: Superadmins and API key users only
- **Query Parameters**:
    - `organization` (optional): Filter by organization ID
    - `project` (optional): Filter by project ID
    - `state` (optional): Filter by state SOI ID
    - `district` (optional): Filter by district SOI ID
    - `tehsil` (optional): Filter by tehsil SOI ID
- **Response**:
  ```json
  {
      "summary": {
          "total_plans": 500,
          "completed_plans": 300,
          "in_progress_plans": 200,
          "dpr_generated": 150,
          "dpr_reviewed": 80,
          "pending_dpr_generation": 150,
          "pending_dpr_review": 70
      },
      "demand_overview": {
          "community_demands": 320,
          "individual_demands": 215
      },
      "commons_connect_operational": {
          "active_tehsils": 25,
          "active_districts": 10,
          "active_states": 5
      },
      "landscape_stewards": {
          "total_stewards": 120,
          "gender_breakdown": {
              "male": 85,
              "female": 32,
              "other": 3
          },
          "by_organization": [
              {"organization_id": 1, "organization_name": "Org X", "steward_count": 40}
          ]
      },
      "completion_rate": 60.0,
      "dpr_generation_rate": 30.0,
      "organization_breakdown": [
          {
              "organization_id": 1,
              "organization_name": "Org X",
              "total_plans": 200,
              "completed_plans": 120,
              "dpr_generated": 60,
              "dpr_reviewed": 30
          }
      ],
      "state_breakdown": [
          {
              "state_id": 1,
              "state_name": "Bihar",
              "total_plans": 150,
              "completed_plans": 90,
              "dpr_generated": 45,
              "centroid": {"lat": 25.0961, "lon": 85.3131}
          }
      ],
      "filters_applied": {
          "organization_id": null,
          "project_id": null,
          "state_id": null,
          "district_id": null,
          "tehsil_id": null
      }
  }
  ```
- **Notes**:
    - `demand_overview`: counts Community Demands and Individual Demands across all NRM maintenance (Section E) and NRM works (Section F) records for the filtered plans
    - `landscape_stewards.total_stewards`: only counts facilitators who are **App User** group members and do **not** belong to the CFPT organization
    - `landscape_stewards.gender_breakdown`: male/female/other counts from the User table for the active stewards; users without a gender set are excluded from all buckets
    - `by_organization` in `landscape_stewards` is omitted when `?organization` filter is applied
    - `organization_breakdown` is only present when no `?organization` filter is applied
    - `state_breakdown` is present when no tehsil or district filter is applied
    - `district_breakdown` is present when a state or district filter is applied (but not tehsil)
    - `tehsil_breakdown` is present when any of state, district, or tehsil filter is applied
    - All filters also scope `demand_overview` and `landscape_stewards` counts

### Steward Meta Stats (Global Level)
- **URL**: `/api/v1/watershed/plans/steward-meta-stats/`
- **Method**: GET
- **Description**: Get comprehensive statistics about landscape stewards (facilitators) across all watershed plans. Excludes test/demo plans and test facilitator names.
- **Authentication**: Required (JWT or API Key)
- **Permissions**: Superadmins and API key users only
- **Query Parameters**:
    - `organization` (optional): Filter by organization ID
    - `project` (optional): Filter by project ID
    - `state` (optional): Filter by state SOI ID
    - `district` (optional): Filter by district SOI ID
    - `tehsil` (optional): Filter by tehsil SOI ID
- **Response**:
  ```json
  {
      "total_stewards": 150,
      "plans_per_steward": {
          "avg": 4.2,
          "min": 1,
          "max": 18
      },
      "avg_completion_rate": 62.5,
      "dpr_stats": {
          "total_dpr_generated": 80,
          "total_dpr_reviewed": 45,
          "pending_dpr_generation": 35,
          "pending_dpr_review": 20
      },
      "active_stewards": 95,
      "inactive_stewards": 55,
      "top_stewards": [
          {
              "facilitator_name": "John Doe",
              "plan_count": 18,
              "completed_count": 12,
              "villages": ["Village A", "Village B"]
          }
      ],
      "by_organization": [
          {"organization_id": 1, "organization_name": "Org X", "steward_count": 40}
      ],
      "state_level": [
          {"state_id": 1, "state_name": "Bihar", "steward_count": 50}
      ],
      "district_level": [
          {"district_id": 1, "district_name": "Nalanda", "state_name": "Bihar", "steward_count": 30}
      ],
      "tehsil_level": [
          {"tehsil_id": 1, "tehsil_name": "Hilsa", "district_name": "Nalanda", "steward_count": 15}
      ],
      "village_level": [
          {
              "village_name": "XYZ",
              "tehsil_name": "Hilsa",
              "district_name": "Nalanda",
              "state_name": "Bihar",
              "steward_count": 5
          }
      ],
      "filters_applied": {
          "organization_id": null,
          "project_id": null,
          "state_id": null,
          "district_id": null,
          "tehsil_id": null
      }
  }
  ```
- **Notes**:
    - Steward counts are distinct facilitator names at each geographic level
    - `active_stewards`: stewards with at least one in-progress plan
    - `inactive_stewards`: stewards whose all plans are completed
    - `avg_completion_rate`: average percentage of completed plans per steward
    - `top_stewards`: top 10 stewards ranked by plan count, with their villages
    - Village name is resolved from `village_name` field; if blank, extracted from plan name (e.g., "Plan Villagename" yields "Villagename")

### Steward Meta Stats (Project Level)
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/steward-meta-stats/`
- **Method**: GET
- **Description**: Get steward statistics scoped to a specific project (or the user's accessible plans)
- **Authentication**: Required
- **Permissions**:
    - Superadmins: Full access
    - Org Admins: Access to their organization's projects
    - Project Users: Access to assigned projects only
- **Query Parameters**:
    - `state` (optional): Filter by state SOI ID
    - `district` (optional): Filter by district SOI ID
    - `tehsil` (optional): Filter by tehsil SOI ID
- **Response**: Same structure as Global Level (see above), with `filters_applied` containing `project_id` instead of `organization_id`

### Steward Listing (Global Level)
- **URL**: `/api/v1/watershed/plans/steward-listing/`
- **Method**: GET
- **Description**: List all stewards with their individual plans, villages, organization, and projects. Unlike `steward-meta-stats` which returns aggregates, this returns the full per-steward breakdown.
- **Authentication**: Required (JWT or API Key)
- **Permissions**: Superadmins and API key users only
- **Query Parameters**:
    - `organization` (optional): Filter by organization ID
    - `project` (optional): Filter by project ID
    - `state` (optional): Filter by state SOI ID
    - `district` (optional): Filter by district SOI ID
    - `tehsil` (optional): Filter by tehsil SOI ID
- **Response**:
  ```json
  {
      "organization": {"id": "2e4fed85-39d2-4691-a7dd-f5cf70a78ec6", "name": "Org X"},
      "total_stewards": 150,
      "working_states": [
          {"id": 3, "name": "Bihar"},
          {"id": 7, "name": "Uttar Pradesh"}
      ],
      "stewards": [
          {
              "facilitator_name": "John Doe",
              "plan_count": 3,
              "completed_count": 2,
              "organization": {"id": "2e4fed85-39d2-4691-a7dd-f5cf70a78ec6", "name": "Org X"},
              "projects": [
                  {"id": 10, "name": "Delhi Watershed Project"},
                  {"id": 15, "name": "Bihar Watershed Project"}
              ],
              "states": [
                  {"id": 3, "name": "Bihar"}
              ],
              "villages": ["Village A", "Village B"],
              "plans": [
                  {"id": 1, "plan": "Plan Village A", "is_completed": true, "village_name": "Village A"},
                  {"id": 2, "plan": "Plan Village B", "is_completed": true, "village_name": "Village B"},
                  {"id": 5, "plan": "Plan Village A Phase 2", "is_completed": false, "village_name": "Village A"}
              ]
          }
      ],
      "filters_applied": {}
  }
  ```
- **Notes**:
    - Top-level `organization` (`id` and `name`) is only present when `?organization=<id>` filter is applied
    - Top-level `working_states`: all distinct states (from `state_soi`) covered by any steward in the filtered context, sorted by name
    - Per-steward `organization`: the organization the steward belongs to, derived from their plans (single object)
    - Per-steward `projects`: all distinct projects the steward has plans in
    - Per-steward `states`: all distinct states that steward has plans in

### Steward Listing (Project Level)
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/steward-listing/`
- **Method**: GET
- **Description**: List all stewards and their plans scoped to a specific project (or the user's accessible plans)
- **Authentication**: Required
- **Permissions**:
    - Superadmins: Full access
    - Org Admins: Access to their organization's projects
    - Project Users: Access to assigned projects only
- **Query Parameters**:
    - `state` (optional): Filter by state SOI ID
    - `district` (optional): Filter by district SOI ID
    - `tehsil` (optional): Filter by tehsil SOI ID
- **Response**: Same structure as Global Level (see above), with `filters_applied` containing `project_id` instead of `organization_id`

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

3. List all disabled projects:
   ```bash
   curl -X GET http://api.example.com/api/v1/projects/disabled/ \
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

### Password Reset Flow

1. Request a password reset:
   ```bash
   curl -X POST http://api.example.com/api/v1/auth/forgot-password/ \
     -H "Content-Type: application/json" \
     -d '{"username": "user123"}'
   ```

2. If the API responds with `"email_required": true`, re-submit with an email:
   ```bash
   curl -X POST http://api.example.com/api/v1/auth/forgot-password/ \
     -H "Content-Type: application/json" \
     -d '{"username": "user123", "email": "user@example.com"}'
   ```

3. The user receives an email with a reset link and opens it in their browser to set a new password.

3. For users without email, an admin can reset the password directly:
   ```bash
   curl -X POST http://api.example.com/api/v1/users/{user_id}/reset_password/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"new_password": "new-secure-password"}'
   ```

## API Security

1. **Authentication**: All API endpoints (except registration, login, and forgot-password) require JWT authentication.
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
| Reset other user's password  | ✓          | ✓ (org)   | ✗               | ✗        |
| View global watershed plans  | ✓          | ✗         | ✗               | ✗        |
| View org watershed plans     | ✓          | ✗         | ✗               | ✗        |
| Filter plans by geography    | ✓          | ✗         | ✗               | ✗        |

## DPR (Detailed Project Report) Data API

These endpoints expose the data that powers each section of the DPR document for a given watershed plan. Most endpoints are read-only (`GET`), with the exception of the demand status update endpoint (`PATCH`).

**Base URL pattern**: `/api/v1/dpr_data/{plan_id}/`

**Authentication**: Every endpoint accepts **either** of the following:

| Method | Header | Example |
|--------|--------|---------|
| JWT Bearer token | `Authorization: Bearer <token>` | Obtained from `/api/v1/auth/token/` |
| API Key | `X-API-Key: <key>` | Issued from the admin panel (`UserAPIKey`) |

If neither credential is provided, or if the credential is invalid, the endpoint returns `401 Unauthorized`.

**Pagination**: Endpoints that return lists support `PageNumberPagination`.
- Default page size: `50`
- Override with `?page_size=<n>` (max `200`)
- Response shape for paginated endpoints:
  ```json
  {
    "count": 87,
    "next": "...?page=2",
    "previous": null,
    "results": [...]
  }
  ```

---

### Summary

- **URL**: `/api/v1/dpr_data/{plan_id}/summary/`
- **Method**: GET
- **Description**: Returns record counts for every section in one lightweight call. Use this to render skeleton UIs or know what data to expect before fetching sections.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Response**:
  ```json
  {
    "plan_id": 42,
    "plan_name": "Watershed Plan Jaunpur",
    "village_name": "Rampur",
    "sections": {
      "settlements": 12,
      "crops": 34,
      "wells": 87,
      "waterbodies": 45,
      "maintenance": { "gw": 23, "agri": 15, "swb": 8, "swb_rs": 4 },
      "nrm_works": { "recharge": 30, "irrigation": 22 },
      "livelihood": 18,
      "agrohorticulture": 7
    }
  }
  ```

---

### Section A – Team Details

- **URL**: `/api/v1/dpr_data/{plan_id}/team-details/`
- **Method**: GET
- **Description**: Returns plan team metadata (organization, project, facilitator).
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Response**:
  ```json
  {
    "organization": "CoRE Stack Foundation",
    "project": "Jaunpur Watershed",
    "plan": "Rampur Watershed Plan 2024",
    "facilitator": "Dr. Rajesh Kumar",
    "process": "PRA, Gram Sabha, Transect Walk, GIS Mapping"
  }
  ```

---

### Section B – Village Brief

- **URL**: `/api/v1/dpr_data/{plan_id}/village-brief/`
- **Method**: GET
- **Description**: Returns village-level geography and settlement count.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Response**:
  ```json
  {
    "village_name": "Rampur",
    "gram_panchayat": "Rampur GP",
    "tehsil": "Badlapur",
    "district": "Jaunpur",
    "state": "Uttar Pradesh",
    "total_settlements": 12,
    "latitude": 25.12345678,
    "longitude": 82.56789012
  }
  ```

---

### Section C – Settlements (Socio-Economic + MGNREGA)

- **URL**: `/api/v1/dpr_data/{plan_id}/settlements/`
- **Method**: GET
- **Description**: Returns one record per settlement with household demographics, caste profile, and MGNREGA data.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "settlement_id": "s_abc123",
    "settlement_name": "Rampur Tola",
    "number_of_households": 120,
    "settlement_type": "Mixed Caste Group",
    "caste_group_detail": "SC, ST, OBC",
    "caste_counts": { "sc": "30", "st": "20", "obc": "50", "general": "20" },
    "marginal_farmers": "45",
    "nrega_job_applied": 80,
    "nrega_job_card": 75,
    "nrega_work_days": 1200,
    "nrega_past_work": "Road construction\n\nWell digging",
    "nrega_demand": "Pond desilting",
    "nrega_issues": "Delayed payments",
    "latitude": 25.1234,
    "longitude": 82.5678
  }
  ```

---

### Section C – Crops

- **URL**: `/api/v1/dpr_data/{plan_id}/crops/`
- **Method**: GET
- **Description**: Returns cropping pattern data per settlement. Acreage values are converted from hectares (form input) to acres.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "crop_grid_id": "cg_xyz456",
    "beneficiary_settlement": "Rampur Tola",
    "irrigation_source": "Rainfed",
    "land_classification": "Upland",
    "kharif_crops": "Paddy Maize",
    "kharif_acres": 12.3553,
    "rabi_crops": "Wheat",
    "rabi_acres": 8.6487,
    "zaid_crops": null,
    "zaid_acres": null,
    "cropping_intensity": "Single crop"
  }
  ```

---

### Section C – Livestock

- **URL**: `/api/v1/dpr_data/{plan_id}/livestock/`
- **Method**: GET
- **Description**: Returns livestock census per settlement (from `ODK_settlement.livestock_census`).
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "settlement_id": "s_abc123",
    "settlement_name": "Rampur Tola",
    "goats": "45",
    "sheep": null,
    "cattle": "30",
    "piggery": null,
    "poultry": "120"
  }
  ```

---

### Section D – Wells

- **URL**: `/api/v1/dpr_data/{plan_id}/wells/`
- **Method**: GET
- **Description**: Returns individual well records with ownership, usage, and maintenance demand details extracted from `data_well` JSON.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "well_id": "well_abc123",
    "beneficiary_settlement": "Rampur Tola",
    "well_type": "Open Well",
    "owner": "Community",
    "beneficiary_name": "Ravi Kumar",
    "beneficiary_father_name": "Shyam Kumar",
    "water_availability": "Year Round",
    "households_benefitted": 15,
    "caste_uses": "SC",
    "well_usage": "Irrigation",
    "need_maintenance": "Yes",
    "repair_activities": "Desilting",
    "latitude": 25.1234,
    "longitude": 82.5678
  }
  ```

---

### Section D – Waterbodies

- **URL**: `/api/v1/dpr_data/{plan_id}/waterbodies/`
- **Method**: GET
- **Description**: Returns individual water structure records with ownership, usage, and repair activity details extracted from `data_waterbody` JSON.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "waterbody_id": "wb_def789",
    "beneficiary_settlement": "Rampur Tola",
    "owner": "Community",
    "beneficiary_name": "Sita Devi",
    "beneficiary_father_name": "Ram Prasad",
    "who_manages": "Gram Panchayat",
    "caste_who_uses": "All castes",
    "households_benefitted": 80,
    "water_structure_type": "Check dam",
    "usage": "Irrigation",
    "need_maintenance": "Yes",
    "repair_activities": "Desilting",
    "latitude": 25.1111,
    "longitude": 82.5555
  }
  ```

---

### Section E – Maintenance

- **URL**: `/api/v1/dpr_data/{plan_id}/maintenance/`
- **Method**: GET
- **Description**: Returns proposed maintenance work records. Filter by asset type using the `?type=` query parameter.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Query Parameters**:
  - `type` (required): One of `gw` (groundwater/recharge structures), `agri` (irrigation structures), `swb` (surface water bodies), `swb_rs` (remote-sensed surface water bodies). Defaults to `gw`.
- **Response** (`results` item):
  ```json
  {
    "id": 1,
    "demand_type": "Community Demand",
    "beneficiary_settlement": "Rampur Tola",
    "beneficiary_name": "Mohan Lal",
    "beneficiary_father_name": "Hari Lal",
    "structure_type": "Check dam",
    "repair_activities": "Desilting",
    "latitude": 25.1234,
    "longitude": 82.5678
  }
  ```
- **Examples**:
  ```
  GET /api/v1/dpr_data/42/maintenance/?type=gw      # Recharge structures
  GET /api/v1/dpr_data/42/maintenance/?type=agri     # Irrigation structures
  GET /api/v1/dpr_data/42/maintenance/?type=swb      # Surface water bodies
  GET /api/v1/dpr_data/42/maintenance/?type=swb_rs   # Remote-sensed SWBs
  ```

---

### Section F – NRM Works

- **URL**: `/api/v1/dpr_data/{plan_id}/nrm-works/`
- **Method**: GET
- **Description**: Returns proposed new NRM works combining both recharge structures (`ODK_groundwater`) and irrigation works (`ODK_agri`) in a single list. Each record includes a `work_category` discriminator.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "work_category": "Recharge Structure",
    "demand_type": "Community Demand",
    "work_demand": "Check dam",
    "beneficiary_settlement": "Rampur Tola",
    "beneficiary_name": "Geeta Devi",
    "gender": "Female",
    "beneficiary_father_name": "Ram Babu",
    "latitude": 25.1234,
    "longitude": 82.5678
  }
  ```
- `work_category` values: `"Recharge Structure"` or `"Irrigation Work"`

---

### Section G – Livelihood

- **URL**: `/api/v1/dpr_data/{plan_id}/livelihood/`
- **Method**: GET
- **Description**: Returns proposed livelihood works spanning livestock, fisheries, plantations, kitchen gardens, and agrohorticulture. Each record includes a `livelihood_work` discriminator.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Paginated**: Yes
- **Response** (`results` item):
  ```json
  {
    "livelihood_work": "Plantations",
    "demand_type": "Individual Demand",
    "work_demand": "Mango",
    "beneficiary_settlement": "Rampur Tola",
    "beneficiary_name": "Sunita Devi",
    "gender": "Female",
    "beneficiary_father_name": "Babu Lal",
    "total_acres": "2.5",
    "latitude": 25.1234,
    "longitude": 82.5678
  }
  ```
- `livelihood_work` values: `"Livestock"`, `"Fisheries"`, `"Plantations"`, `"Kitchen Garden"`

---

### DPR Data API – Quick Reference

All endpoints accept `Authorization: Bearer <token>` **or** `X-API-Key: <key>`.

| Endpoint | Paginated | Data Source |
|---|---|---|
| `GET dpr_data/{id}/summary/` | No | All ODK models (counts only) |
| `GET dpr_data/{id}/team-details/` | No | `PlanApp` |
| `GET dpr_data/{id}/village-brief/` | No | `PlanApp` + `ODK_settlement` |
| `GET dpr_data/{id}/settlements/` | Yes | `ODK_settlement` |
| `GET dpr_data/{id}/crops/` | Yes | `ODK_crop` |
| `GET dpr_data/{id}/livestock/` | Yes | `ODK_settlement.livestock_census` |
| `GET dpr_data/{id}/wells/` | Yes | `ODK_well` |
| `GET dpr_data/{id}/waterbodies/` | Yes | `ODK_waterbody` |
| `GET dpr_data/{id}/maintenance/?type=gw\|agri\|swb\|swb_rs` | Yes | `GW_maintenance`, `Agri_maintenance`, `SWB_maintenance`, `SWB_RS_maintenance` |
| `GET dpr_data/{id}/nrm-works/` | Yes | `ODK_groundwater` + `ODK_agri` |
| `GET dpr_data/{id}/livelihood/` | Yes | `ODK_livelihood` + `ODK_agrohorticulture` |
| `GET dpr_data/{id}/status-tracking/` | No | All resource + demand models (counts by status) |
| `PATCH dpr_data/{id}/demand-status/` | No | Any resource/demand model (single record update) |
| `GET dpr_data/{id}/report-status/` | No | `DPR_Report` (current workflow status) |
| `PATCH dpr_data/{id}/report-status/` | No | `DPR_Report` (update workflow status) |

---

### Status Tracking

- **URL**: `/api/v1/dpr_data/{plan_id}/status-tracking/`
- **Method**: GET
- **Description**: Returns aggregate counts of resources and demands grouped by `DEMAND_STATUS_CHOICES`. The "Submitted" status is split into two sub-sections: **Resources Submitted** (settlements, wells, waterbodies, crops) and **Demands Submitted** (groundwater, agri, livelihood, agrohorticulture, maintenance records).
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Response**:
  ```json
  {
    "statuses": [
      {
        "key": "SUBMITTED",
        "label": "Submitted",
        "sub_sections": [
          { "key": "RESOURCES_SUBMITTED", "label": "Resources Submitted", "count": 25 },
          { "key": "DEMANDS_SUBMITTED", "label": "Demands Submitted", "count": 10 }
        ]
      },
      { "key": "APPROVED", "label": "Approved", "count": 15 },
      { "key": "REJECTED", "label": "Rejected", "count": 3 }
    ]
  }
  ```
- **Notes**:
  - PENDING and REVERTED statuses are excluded from the response.
  - **Resources**: `ODK_settlement`, `ODK_well`, `ODK_waterbody`, `ODK_crop`
  - **Demands**: `ODK_groundwater`, `ODK_agri`, `ODK_livelihood`, `ODK_agrohorticulture`, `GW_maintenance`, `SWB_RS_maintenance`, `SWB_maintenance`, `Agri_maintenance`
  - Approved and Rejected counts span both groups.

---

### Update Demand Status

- **URL**: `/api/v1/dpr_data/{plan_id}/demand-status/`
- **Method**: PATCH
- **Description**: Updates the demand status on a single resource or demand record. The frontend renders a dropdown per record populated with `DEMAND_STATUS_CHOICES`; on selection change, this endpoint is called.
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`
- **Request Body**:
  ```json
  {
    "resource_type": "settlement",
    "resource_id": "SET_001",
    "status": "APPROVED"
  }
  ```
- **Required Fields**:
  - `resource_type` (string): One of `settlement`, `well`, `waterbody`, `groundwater`, `agri`, `crop`, `livelihood`, `agrohorticulture`, `gw_maintenance`, `swb_rs_maintenance`, `swb_maintenance`, `agri_maintenance`
  - `resource_id` (string): Primary key of the record
  - `status` (string): One of `PENDING`, `SUBMITTED`, `APPROVED`, `REVERTED`, `REJECTED`
- **Response** (`200 OK`):
  ```json
  {
    "resource_type": "settlement",
    "resource_id": "SET_001",
    "status": "APPROVED"
  }
  ```
- **Error Responses**:
  - `400 Bad Request` — missing fields, invalid `resource_type`, invalid `status`, or resource not found
  - `404 Not Found` — plan does not exist

---

### DPR Report Workflow Status

- **URL**: `/api/v1/dpr_data/{plan_id}/report-status/`
- **Methods**: GET, PATCH
- **Description**: Read or update the workflow status of the `DPR_Report` for a plan. The frontend uses this to render a status toggle (e.g. Submitted → Approved / Rejected).
- **Authentication**: `Authorization: Bearer <token>` or `X-API-Key: <key>`

#### GET — Read current status

**Response** (`200 OK`):
```json
{
  "dpr_report_id": 12,
  "plan_id": 42,
  "status": "SUBMITTED",
  "submitted_breakdown": {
    "resources_submitted": 25,
    "demands_submitted": 10
  },
  "dpr_report_s3_url": "https://...",
  "dpr_generated_at": "2026-04-01T10:00:00Z",
  "last_updated_at": "2026-04-05T14:30:00Z",
  "last_updated_by": 7
}
```

- `submitted_breakdown` is always present regardless of current status — it shows how many resource and demand records are in `SUBMITTED` state across all ODK models for this plan.

**Error**: `404 Not Found` — DPR report has not been generated for this plan yet.

#### PATCH — Update status

All fields are optional but at least one must be provided.

| Field | Type | Description |
|---|---|---|
| `status` | string | Updates `DPR_Report.status`. Allowed: `SUBMITTED`, `APPROVED`, `REJECTED` |
| `resources_submitted` | string | Bulk-sets the demand status on **all resource records** (settlements, wells, waterbodies, crops) for this plan |
| `demands_submitted` | string | Bulk-sets the demand status on **all demand records** (groundwater, agri, livelihood, agrohorticulture, all maintenance) for this plan |

`resources_submitted` and `demands_submitted` accept any value from `DEMAND_STATUS_CHOICES`: `PENDING`, `SUBMITTED`, `APPROVED`, `REVERTED`, `REJECTED`.

**Examples**:

Toggle all resources to Submitted:
```json
{ "resources_submitted": "SUBMITTED" }
```

Toggle all demands to Submitted:
```json
{ "demands_submitted": "SUBMITTED" }
```

Mark both groups and set DPR status in one call:
```json
{
  "status": "SUBMITTED",
  "resources_submitted": "SUBMITTED",
  "demands_submitted": "SUBMITTED"
}
```

Approve the DPR without touching individual records:
```json
{ "status": "APPROVED" }
```

**Response** (`200 OK`) — always includes the updated `submitted_breakdown` counts:
```json
{
  "dpr_report_id": 12,
  "plan_id": 42,
  "status": "SUBMITTED",
  "submitted_breakdown": {
    "resources_submitted": 25,
    "demands_submitted": 10
  },
  "last_updated_at": "2026-04-06T09:15:00Z",
  "last_updated_by": 7
}
```

**Error Responses**:
- `400 Bad Request` — no fields provided, or invalid value for any field
- `404 Not Found` — plan or DPR report not found

---

## STAC Endpoints

### Get STAC Catalog (Query Param Style)

- **URL**: `/api/v1/get_stac_catalog/`
- **Method**: GET
- **Description**: Returns the STAC catalog or collection JSON at any level of the hierarchy via query parameters. With no params returns the root catalog; each additional param narrows the scope one level deeper.
- **Authentication**: Required
- **Query Parameters** (all optional, progressively narrow scope):
  - `state` (string): State name — returns the state collection
  - `district` (string): Requires `state` — returns the district collection
  - `block` (string): Requires `state` + `district` — returns the block collection
- **Content-Type**: `application/geo+json`

| Params supplied | File returned |
|---|---|
| _(none)_ | `catalog.json` (root) |
| `state` | `tehsil_wise/{state}/collection.json` |
| `state` + `district` | `tehsil_wise/{state}/{district}/collection.json` |
| `state` + `district` + `block` | `tehsil_wise/{state}/{district}/{block}/collection.json` |

- **Examples**:
  ```
  GET /api/v1/get_stac_catalog/
  GET /api/v1/get_stac_catalog/?state=bihar
  GET /api/v1/get_stac_catalog/?state=bihar&district=nalanda
  GET /api/v1/get_stac_catalog/?state=bihar&district=nalanda&block=hilsa
  ```
- **Error**: `404` if the catalog at the requested scope has not been generated yet
- **Notes**: Input names are case-insensitive and normalised using `sanitize_text` to match how paths were written during generation.

---

### Get Root Catalog

- **URL**: `/api/v1/stac/`
- **Method**: GET
- **Description**: Returns the root STAC catalog JSON. Entry point for navigating the full catalog hierarchy.
- **Authentication**: Required
- **Response** (`200 OK`): Root `catalog.json` as JSON
- **Error**: `404` if no catalog has been generated yet

---

### Get State Collection

- **URL**: `/api/v1/stac/{state}/`
- **Method**: GET
- **Description**: Returns the STAC collection for a given state.
- **Authentication**: Required
- **URL Parameters**:
  - `state` (string): State name (case-insensitive, e.g. `bihar`)
- **Response** (`200 OK`): State-level `collection.json` as JSON
- **Error**: `404` if the state collection does not exist

---

### Get District Collection

- **URL**: `/api/v1/stac/{state}/{district}/`
- **Method**: GET
- **Description**: Returns the STAC collection for a given district within a state.
- **Authentication**: Required
- **URL Parameters**:
  - `state` (string): State name
  - `district` (string): District name (case-insensitive)
- **Response** (`200 OK`): District-level `collection.json` as JSON
- **Error**: `404` if the district collection does not exist

---

### Get Block Collection

- **URL**: `/api/v1/stac/{state}/{district}/{block}/`
- **Method**: GET
- **Description**: Returns the STAC collection for a given block, including links to all items generated for that block.
- **Authentication**: Required
- **URL Parameters**:
  - `state` (string): State name
  - `district` (string): District name
  - `block` (string): Block name (case-insensitive)
- **Response** (`200 OK`): Block-level `collection.json` as JSON
- **Error**: `404` if the block collection does not exist

---

### Get Item

- **URL**: `/api/v1/stac/{state}/{district}/{block}/items/{item_id}/`
- **Method**: GET
- **Description**: Returns a single STAC item (layer) within a block collection. The `item_id` follows the pattern `{state}_{district}_{block}_{layer_name}` (with optional `_{start_year}` suffix for time-series layers).
- **Authentication**: Required
- **URL Parameters**:
  - `state`, `district`, `block` (string): Geographic hierarchy (case-insensitive)
  - `item_id` (string): Item identifier, e.g. `bihar_nalanda_hilsa_ndvi_2023`
- **Response** (`200 OK`): STAC item JSON with assets (data, thumbnail, style)
- **Error**: `404` if the item does not exist

---

### Catalog Hierarchy

```
GET /api/v1/stac/                                          root catalog
GET /api/v1/stac/{state}/                                  state collection
GET /api/v1/stac/{state}/{district}/                       district collection
GET /api/v1/stac/{state}/{district}/{block}/               block collection (lists items)
GET /api/v1/stac/{state}/{district}/{block}/items/{id}/    single item
```

All names are case-insensitive. Collections are only available after `generate_stac_collection` has been run for at least one layer in that geography.

---

### Generate STAC Collection

- **URL**: `/api/v1/generate_stac_collection/`
- **Method**: POST
- **Description**: Asynchronously generates a STAC (SpatioTemporal Asset Catalog) collection for a given geographic boundary and layer. The task is dispatched to the `nrm` Celery queue and returns immediately.
- **Authentication**: Required
- **Request Body**:
  ```json
  {
    "state": "Bihar",
    "district": "Nalanda",
    "block": "Hilsa",
    "layer_name": "ndvi",
    "layer_type": "raster",
    "start_year": "2023",
    "upload_to_s3": false,
    "overwrite": false
  }
  ```
- **Required Fields**:
  - `state` (string): State name
  - `district` (string): District name
  - `block` (string): Block name
  - `layer_name` (string): Name of the layer for which the STAC collection is generated
  - `layer_type` (string): Type of layer — must be `"raster"` or `"vector"`
- **Optional Fields**:
  - `start_year` (string, default: `""`): Starting year for the collection
  - `upload_to_s3` (boolean, default: `false`): Whether to upload the generated collection to S3
  - `overwrite` (boolean, default: `false`): Whether to overwrite an existing collection
- **Success Response** (`200 OK`):
  ```json
  {
    "Success": "STAC collection generation initiated"
  }
  ```
- **Error Responses**:
  - `400 Bad Request` — missing required fields:
    ```json
    { "error": "state, district, block, layer_name, and layer_type are required" }
    ```
  - `400 Bad Request` — invalid `layer_type`:
    ```json
    { "error": "layer_type must be 'raster' or 'vector'" }
    ```
  - `500 Internal Server Error`:
    ```json
    { "Exception": "<error message>" }
    ```
- **Notes**:
  - The generation runs asynchronously; a `200` response only confirms the task was enqueued.
  - Use `upload_to_s3: true` to persist the output to S3 after generation.
  - Use `overwrite: true` to regenerate and replace an existing collection.
