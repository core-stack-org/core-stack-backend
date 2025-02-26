# User Hierarchy and Permission System

This document outlines the user hierarchy, permission system, and creation flows in the Core Stack Backend.

## User Roles Hierarchy

The system has the following user roles, in descending order of permissions:

1. **Superuser** - Django's built-in administrator role
2. **Superadmin** - Application-level administrator role
3. **Organization Admin** - Administrator for a specific organization
4. **Project User** - Regular user with specific project-level permissions

## Role Creation and Management

### Superuser Creation

Superusers are Django's built-in administrator role and can only be created through the command line:

```bash
python manage.py createsuperuser
```

This creates a user with `is_superuser=True` and `is_staff=True`, giving them access to the Django admin interface and all permissions.

**Note:** A superuser is not automatically a superadmin. The `is_superadmin` flag must be set separately.

### Superadmin Creation

Superadmins are application-level administrators who can manage organizations and users across the entire system.

**To create the first superadmin:**

1. First, create an organization (required as the User model requires an organization):
   ```python
   from organization.models import Organization
   org = Organization.objects.create(name="Admin Organization", description="Organization for superadmin")
   ```

2. Create a superuser via the command line:
   ```bash
   python manage.py createsuperuser
   ```

3. Set the `is_superadmin` flag for this user:
   ```python
   from users.models import User
   user = User.objects.get(username='your_superuser_username')
   user.is_superadmin = True
   user.save()
   ```

**To create additional superadmins:**

Only existing superadmins can create new superadmins through the API:

```http
PUT/PATCH /api/v1/users/{user_id}/
```

With the request body:
```json
{
  "is_superadmin": true
}
```

### Organization Admin Creation

Organization Admins are users who have management permissions for a specific organization. To create an Organization Admin:

1. First, create a regular user and assign them to an organization:
   ```http
   POST /api/v1/auth/register/
   ```
   
   With the request body:
   ```json
   {
     "username": "orgadmin",
     "email": "orgadmin@example.com",
     "password": "securepassword",
     "password_confirm": "securepassword",
     "first_name": "Admin",
     "last_name": "User",
     "contact_number": "1234567890",
     "organization": "organization-uuid"
   }
   ```

2. Assign the user to an organization (must be done by a superadmin):
   ```http
   PUT /api/v1/users/{user_id}/set_organization/
   ```
   
   With the request body:
   ```json
   {
     "organization_id": "organization-uuid"
   }
   ```

3. Add the user to the "Organization Admin" group using the API:
   ```http
   PUT /api/v1/users/{user_id}/set_group/
   ```
   
   With the request body:
   ```json
   {
     "group_id": "organization-admin-group-id"
   }
   ```
   
   Note: You'll need to know the ID of the "Organization Admin" group. You can get this by listing all groups:
   ```http
   GET /api/v1/groups/
   ```

   Alternatively, this can still be done through the Django shell or admin interface as described below.

4. Alternative methods for adding a user to the "Organization Admin" group:
   
   Using the Django shell:
   ```python
   python manage.py shell
   
   from django.contrib.auth.models import Group
   from users.models import User
   
   # Get or create the Organization Admin group
   org_admin_group, created = Group.objects.get_or_create(name='Organization Admin')
   
   # Get the user
   user = User.objects.get(username='orgadmin')
   
   # Add the user to the group
   user.groups.add(org_admin_group)
   user.save()
   ```

   Or through the Django admin interface:
   - Go to `http://your-server/admin/`
   - Log in with superuser credentials
   - Navigate to Users
   - Find the user you want to make an organization admin
   - In the Groups field, add the "Organization Admin" group
   - Save the changes

### Organization Creation

Organizations can only be created by superadmins:

```http
POST /api/v1/organizations/
```

With the request body:
```json
{
  "name": "Organization Name",
  "description": "Organization Description"
}
```

### Project Creation

Projects can be created by:
- Superadmins (for any organization)
- Organization Admins (for their organization only)

```http
POST /api/v1/projects/
```

With the request body:
```json
{
  "name": "Project Name",
  "description": "Project Description",
  "organization": "organization-uuid"
}
```

### User Registration

Users can register themselves through the API:

```http
POST /api/v1/auth/register/
```

With the request body:
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

The `organization` field is optional. If provided, the user will be assigned to that organization upon registration.

Users can get a list of available organizations to select from during registration:

```http
GET /api/v1/auth/register/available_organizations/
```

This endpoint returns a list of active organizations with their IDs and names, allowing users to select which organization they want to join during registration.

## Permission Comparison

| Action | Superuser | Superadmin | Org Admin | Regular User |
|--------|-----------|------------|-----------|--------------|
| Access Django Admin | ✅ | ❌ (unless also a superuser) | ❌ | ❌ |
| Create Organizations | ✅ | ✅ | ❌ | ❌ |
| Manage All Organizations | ✅ | ✅ | ❌ | ❌ |
| Manage Own Organization | ✅ | ✅ | ✅ | ❌ |
| Create Projects | ✅ | ✅ | ✅ (own org only) | ❌ |
| Create Users | ✅ | ✅ | ✅ (own org only) | ❌ |
| Make User a Superadmin | ✅ | ✅ | ❌ | ❌ |
| View All Users | ✅ | ✅ | ❌ | ❌ |
| View Org Users | ✅ | ✅ | ✅ | ❌ |
| View Own Profile | ✅ | ✅ | ✅ | ✅ |

## Frontend Implementation Flow

To implement a smooth user experience in the frontend:

1. **Initial Setup Flow**:
   - The first user must be created as a superuser via command line
   - This superuser must then be made a superadmin via command line or Django admin
   - The superadmin can then create organizations and other superadmins through the UI

2. **New System Setup**:
   - Superadmin creates an organization
   - Superadmin creates an organization admin
   - Organization admin can then create projects and users

3. **User Registration Flow**:
   - If allowing self-registration, implement an approval process where:
     - Users register
     - Superadmins or Org Admins approve and assign them to organizations
     - Users are then assigned to projects with appropriate roles

4. **Permission Checks**:
   - Frontend should check user roles and only show relevant UI elements
   - For superadmins: show organization management
   - For org admins: show user and project management for their organization
   - For regular users: show only projects they have access to

5. **Error Handling**:
   - Implement clear error messages for permission-related issues
   - Provide guidance when users attempt actions they don't have permission for

6. **Role Management UI**:
   - Create interfaces for superadmins to manage roles
   - Allow organization admins to manage project roles
   - Implement confirmation dialogs for sensitive permission changes

### Organization Admin Management

For managing organization admins in the frontend:

1. **Creating Organization Admins:**
   - Superadmin creates a user and assigns them to an organization
   - Superadmin then uses the `set_group` API endpoint to assign the "Organization Admin" group
   - The frontend should provide a user-friendly interface for this process

2. **Organization Admin Dashboard:**
   - Show organization-specific management options
   - Allow management of users within their organization
   - Provide project creation and management capabilities

3. **Permission Checks:**
   - Frontend should verify if the logged-in user has the "Organization Admin" group
   - Show/hide UI elements based on this group membership
   - Handle API responses that might deny access due to insufficient permissions

4. **Group Management Interface:**
   - Provide an interface for superadmins to view and manage user groups
   - Include confirmation dialogs when changing user roles
   - Show clear feedback on successful role assignments

## Common Scenarios

1. **Setting up a new organization**:
   - Superadmin creates the organization
   - Superadmin creates or assigns an organization admin
   - Organization admin sets up projects and invites users

2. **Adding a new superadmin**:
   - Existing superadmin navigates to user management
   - Edits a user and sets the superadmin flag
   - New superadmin now has system-wide permissions

3. **User joining an organization**:
   - User registers through the registration form
   - Superadmin or organization admin assigns them to an organization
   - User is then assigned to specific projects with appropriate roles
