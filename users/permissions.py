from rest_framework import permissions
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from projects.models import Project
from plantations.models import KMLFile
from plans.models import Plan


class IsOrganizationMember(permissions.BasePermission):
    """
    Permission check for organization membership.
    """
    def has_permission(self, request, view):
        # Allow authenticated users only
        if not request.user or not request.user.is_authenticated:
            return False
        
        # Super admins have full access
        if request.user.is_superadmin or request.user.is_superuser:
            return True
        
        # Check if user belongs to an organization
        return request.user.organization is not None
    
    def has_object_permission(self, request, view, obj):
        # Super admins have full access
        if request.user.is_superadmin or request.user.is_superuser:
            return True
        
        # Check if user belongs to the same organization as the object
        if hasattr(obj, 'organization'):
            return obj.organization == request.user.organization
        
        # If object has project field, check that project's organization
        if hasattr(obj, 'project') and hasattr(obj.project, 'organization'):
            return obj.project.organization == request.user.organization
        
        return False


class HasProjectPermission(permissions.BasePermission):
    """
    Permission check for project-level permissions.
    """
    def has_permission(self, request, view):
        # Allow authenticated users only
        if not request.user or not request.user.is_authenticated:
            return False
        
        # Super admins have full access
        if request.user.is_superadmin or request.user.is_superuser:
            return True
        
        # Check method-specific permissions
        project_id = view.kwargs.get('project_pk')
        if project_id:
            method = request.method
            permission_codename = self._get_permission_codename(method, view)
            
            return request.user.has_project_permission(
                project_id=project_id,
                codename=permission_codename
            )
        
        return False
    
    def has_object_permission(self, request, view, obj):
        # Super admins have full access
        if request.user.is_superadmin or request.user.is_superuser:
            return True
        
        # Get project from the object
        project = None
        if hasattr(obj, 'project'):
            project = obj.project
        elif hasattr(obj, 'project_app') and hasattr(obj.project_app, 'project'):
            project = obj.project_app.project
        
        if project:
            method = request.method
            permission_codename = self._get_permission_codename(method, view)
            
            return request.user.has_project_permission(
                project=project,
                codename=permission_codename
            )
        
        return False
    
    def _get_permission_codename(self, method, view):
        """Map HTTP methods to permission codenames."""
        app_type = None
        if hasattr(view, 'app_type'):
            app_type = view.app_type
        else:
            # Try to infer from the viewset's model or queryset
            if hasattr(view, 'queryset') and view.queryset is not None:
                model = view.queryset.model
                if hasattr(model, 'app_type'):
                    app_type = model.app_type
        
        method_map = {
            'GET': 'view',
            'HEAD': 'view',
            'OPTIONS': 'view',
            'POST': 'add',
            'PUT': 'change',
            'PATCH': 'change',
            'DELETE': 'delete',
        }
        
        action = method_map.get(method, 'view')
        
        if app_type:
            return f"{action}_{app_type}"
        
        # Default to project-level permission if app_type not found
        return f"{action}_project"


def create_app_permissions():
    """
    Create custom permissions for different app types and actions.
    """
    # Get content types for the models
    project_content_type = ContentType.objects.get_for_model(Project)
    kml_content_type = ContentType.objects.get_for_model(KMLFile)
    plan_content_type = ContentType.objects.get_for_model(Plan)
    
    # Create permissions for plantation app
    for action in ['view', 'add', 'change', 'delete']:
        Permission.objects.get_or_create(
            codename=f'{action}_plantation',
            name=f'Can {action} plantation data',
            content_type=project_content_type,
        )
    
    # Create permissions for watershed app
    for action in ['view', 'add', 'change', 'delete']:
        Permission.objects.get_or_create(
            codename=f'{action}_watershed',
            name=f'Can {action} watershed planning data',
            content_type=project_content_type,
        )

def create_default_groups():
    """
    Create default groups with assigned permissions.
    """
    # Project Admin group
    admin_group, created = Group.objects.get_or_create(name='Project Admin')
    if created:
        # Add all permissions
        for app_type in ['plantation', 'watershed']:
            for action in ['view', 'add', 'change', 'delete']:
                perm = Permission.objects.get(codename=f'{action}_{app_type}')
                admin_group.permissions.add(perm)
    
    # Project Editor group
    editor_group, created = Group.objects.get_or_create(name='Project Editor')
    if created:
        # Add view, add, change permissions
        for app_type in ['plantation', 'watershed']:
            for action in ['view', 'add', 'change']:
                perm = Permission.objects.get(codename=f'{action}_{app_type}')
                editor_group.permissions.add(perm)
    
    # Project Viewer group
    viewer_group, created = Group.objects.get_or_create(name='Project Viewer')
    if created:
        # Add only view permissions
        for app_type in ['plantation', 'watershed']:
            perm = Permission.objects.get(codename=f'view_{app_type}')
            viewer_group.permissions.add(perm)