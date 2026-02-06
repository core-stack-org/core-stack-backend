from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from rest_framework import permissions

from plans.models import Plan
from plantations.models import KMLFile
from projects.models import Project


class IsOrganizationMember(permissions.BasePermission):
    """
    Permission check for organization membership.
    """

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        return request.user.organization is not None

    def has_object_permission(self, request, view, obj):
        if request.user.is_superadmin or request.user.is_superuser:
            return True

        if (
            request.method in permissions.SAFE_METHODS
            and request.user.groups.filter(name="Test Plan Reviewer").exists()
        ):
            return True

        if hasattr(obj, "organization"):
            return obj.organization == request.user.organization

        if hasattr(obj, "project") and hasattr(obj.project, "organization"):
            return obj.project.organization == request.user.organization

        return False


class HasProjectPermission(permissions.BasePermission):
    """
    Permission check for project-level permissions.
    """

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        project_id = view.kwargs.get("project_pk")
        if not project_id:
            return False

        if request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            try:
                project = Project.objects.get(id=project_id)
                return project.organization == request.user.organization
            except Project.DoesNotExist:
                return False

        method = request.method
        permission_codename = self._get_permission_codename(method, view)

        return request.user.has_project_permission(
            project_id=project_id, codename=permission_codename
        )

    def has_object_permission(self, request, view, obj):
        if request.user.is_superadmin or request.user.is_superuser:
            return True

        project = None
        if hasattr(obj, "project"):
            project = obj.project
        elif hasattr(obj, "project_app") and hasattr(obj.project_app, "project"):
            project = obj.project_app.project

        if not project:
            return False

        if request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            return project.organization == request.user.organization

        method = request.method
        permission_codename = self._get_permission_codename(method, view)

        return request.user.has_project_permission(
            project=project, codename=permission_codename
        )

    def _get_permission_codename(self, method, view):
        """Map HTTP methods to permission codenames."""
        app_type = None
        if hasattr(view, "app_type"):
            app_type = view.app_type
        else:
            if hasattr(view, "queryset") and view.queryset is not None:
                model = view.queryset.model
                if hasattr(model, "app_type"):
                    app_type = model.app_type

        method_map = {
            "GET": "view",
            "HEAD": "view",
            "OPTIONS": "view",
            "POST": "add",
            "PUT": "change",
            "PATCH": "change",
            "DELETE": "delete",
        }

        action = method_map.get(method, "view")

        if app_type:
            return f"{action}_{app_type}"

        return f"{action}_project"


def create_app_permissions():
    """
    Create custom permissions for different app types and actions.
    """
    project_content_type = ContentType.objects.get_for_model(Project)
    kml_content_type = ContentType.objects.get_for_model(KMLFile)
    plan_content_type = ContentType.objects.get_for_model(Plan)

    for action in ["view", "add", "change", "delete"]:
        Permission.objects.get_or_create(
            codename=f"{action}_plantation",
            name=f"Can {action} plantation data",
            content_type=project_content_type,
        )

    for action in ["view", "add", "change", "delete"]:
        Permission.objects.get_or_create(
            codename=f"{action}_watershed",
            name=f"Can {action} watershed planning data",
            content_type=project_content_type,
        )


def create_default_groups():
    """
    Create default groups with assigned permissions.
    """
    admin_group, created = Group.objects.get_or_create(name="Project Manager")
    if created:
        for app_type in ["plantation", "watershed"]:
            for action in ["view", "add", "change", "delete"]:
                perm = Permission.objects.get(codename=f"{action}_{app_type}")
                admin_group.permissions.add(perm)

    editor_group, created = Group.objects.get_or_create(name="App User")
    if created:
        for app_type in ["plantation", "watershed"]:
            for action in ["view", "add", "change"]:
                perm = Permission.objects.get(codename=f"{action}_{app_type}")
                editor_group.permissions.add(perm)

    viewer_group, created = Group.objects.get_or_create(name="Analyst")
    if created:
        for app_type in ["plantation", "watershed"]:
            perm = Permission.objects.get(codename=f"view_{app_type}")
            viewer_group.permissions.add(perm)
