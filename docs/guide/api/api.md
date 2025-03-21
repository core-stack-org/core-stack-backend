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
- **Request Body**:
  ```json
  {
    "name": "Project Name",
    "description": "Project Description",
    "state": "state-id",
    "app_type": "plantation",
    "enabled": true,
    "created_by": "user-id",
    "updated_by": "user-id"
  }
  ```
- **Authentication**: Required
- **Permissions**: Super admin or organization admin
- **Notes**: 
  - The organization is automatically set to the user's organization
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
- **Description**: Remove a user from a project
- **Authentication**: Required
- **Permissions**: Super admin, organization admin, or project manager

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

### List Watershed Plans
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/`
- **Method**: GET
- **Description**: List watershed plans for a project
- **Authentication**: Required
- **Permissions**: User must have access to the project

### Create Watershed Plan
- **URL**: `/api/v1/projects/{project_id}/watershed/plans/`
- **Method**: POST
- **Description**: Create a new watershed plan
- **Request Body**:
  ```json
  {
    "plan": "Plan Name",
    "state": "state-id",
    "district": "district-id",
    "block": "block-id",
    "village_name": "Village Name",
    "gram_panchayat": "Gram Panchayat Name"
  }
  ```
- **Authentication**: Required
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
- **Description**: Update a watershed plan
- **Authentication**: Required
- **Permissions**: User must have update permission for the project

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
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"name": "Plantation Project", "description": "A new plantation project", "organization": "organization-id"}'
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

### Creating a Watershed Plan

1. Create a project (if not already created):
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"name": "Watershed Project", "description": "A new watershed project", "organization": "organization-id"}'
   ```

2. Enable the watershed app for the project:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/apps/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{"app_type": "watershed", "enabled": true}'
   ```

3. Create a watershed plan:
   ```bash
   curl -X POST http://api.example.com/api/v1/projects/{project_id}/watershed/plans/ \
     -H "Authorization: Bearer {access_token}" \
     -H "Content-Type: application/json" \
     -d '{
       "plan": "Watershed Plan 2023",
       "state": "state-id",
       "district": "district-id",
       "block": "block-id",
       "village_name": "Example Village",
       "gram_panchayat": "Example GP"
     }'
   ```

4. View the created watershed plans:
   ```bash
   curl -X GET http://api.example.com/api/v1/projects/{project_id}/watershed/plans/ \
     -H "Authorization: Bearer {access_token}"
   ```

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
