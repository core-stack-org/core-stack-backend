# plans/views.py
from rest_framework import permissions, status, viewsets
from rest_framework.response import Response

from projects.models import AppType, Project

from .models import PlanApp
from .serializers import PlanAppListSerializer, PlanCreateSerializer, PlanSerializer


class PlanPermission(permissions.BasePermission):
    """
    Custom permission for PlanApp:
    - All authenticated users can view plans
    - Only superadmins, org admins, administrators, and project managers can create/edit plans
    - Plans must be enabled to be visible
    """

    schema = None

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        if request.method in permissions.SAFE_METHODS:
            return True

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

        if request.method == "POST":
            return request.user.has_project_permission(
                project_id=project_id, codename="add_watershed"
            )
        elif request.method in ["PUT", "PATCH"]:
            return request.user.has_project_permission(
                project_id=project_id, codename="change_watershed"
            )
        elif request.method == "DELETE":
            return request.user.has_project_permission(
                project_id=project_id, codename="delete_watershed"
            )

        return False

    def has_object_permission(self, request, view, obj):
        if hasattr(obj, "enabled") and not obj.enabled:
            return False

        if request.method in permissions.SAFE_METHODS:
            return True

        if request.user.is_superadmin or request.user.is_superuser:
            return True

        project = None
        if hasattr(obj, "project"):
            project = obj.project

        if not project:
            return False

        if request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            return project.organization == request.user.organization

        if request.method in ["PUT", "PATCH"]:
            return request.user.has_project_permission(
                project=project, codename="change_watershed"
            )
        elif request.method == "DELETE":
            return request.user.has_project_permission(
                project=project, codename="delete_watershed"
            )

        return False


class PlanViewSet(viewsets.ModelViewSet):
    """
    ViewSet for watershed planning operations
    """

    serializer_class = PlanSerializer
    permission_classes = [permissions.IsAuthenticated, PlanPermission]
    schema = None
    app_type = AppType.WATERSHED

    def get_queryset(self):
        """
        Filter plans by project
        Superadmins: can see all the plans from all the projects from all the organizations
        Org Admins: can see all plans from all the projects for an organization
        App Users: can see all the plans from a project they are associated with
        """
        if self.request.user.is_superuser or self.request.user.is_superadmin:
            return PlanApp.objects.filter(enabled=True)

        if self.request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            return PlanApp.objects.filter(
                organization=self.request.user.organization, enabled=True
            )

        project_id = self.kwargs.get("project_pk")
        if project_id:
            try:
                project = Project.objects.get(
                    id=project_id, app_type=AppType.WATERSHED, enabled=True
                )
                return PlanApp.objects.filter(project=project)
            except Project.DoesNotExist:
                return PlanApp.objects.none()
        return PlanApp.objects.none()

    def get_serializer_class(self):
        """
        Use different serializers based on the action
        """
        if self.action in ["create"]:
            return PlanCreateSerializer
        elif self.action in ["list", "retrieve"]:
            return PlanAppListSerializer
        return PlanSerializer

    def create(self, request, *args, **kwargs):
        """
        Create a new watershed plan
        """
        project_id = self.kwargs.get("project_pk")
        if not project_id:
            return Response(
                {"message": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get project and check if it's a watershed project and enabled
        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.WATERSHED, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"message": "Watershed Planning is not enabled for this project."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Add project, organization, and created_by
        plan = serializer.save(
            project=project, organization=project.organization, created_by=request.user
        )

        # Use the full serializer for response with success message
        response_data = {
            "plan_data": PlanAppListSerializer(plan).data,
            "message": f"Successfully created the watershed plan,{plan.plan}",
        }

        return Response(response_data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        """
        Update a watershed plan
        """
        partial = kwargs.pop("partial", False)
        instance = self.get_object()

        # Use PlanCreateSerializer for validation
        create_serializer = PlanCreateSerializer(
            instance, data=request.data, partial=partial
        )
        create_serializer.is_valid(raise_exception=True)

        # Save the instance with validated data
        updated_instance = create_serializer.save()

        # Use PlanSerializer for response
        return Response(PlanSerializer(updated_instance).data)

    def perform_destroy(self, instance):
        """
        Delete a watershed plan
        """
        instance.delete()
