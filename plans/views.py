# plans/views.py
from rest_framework import permissions, status, viewsets
from rest_framework.response import Response

from organization.models import Organization
from projects.models import AppType, Project

from .models import PlanApp
from .serializers import (
    PlanAppSerializer,
    PlanCreateSerializer,
    PlanSerializer,
    PlanUpdateSerializer,
)


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


class SuperAdminPlanPermission(permissions.BasePermission):
    """
    Custom permission for superadmin only plan endpoints
    """

    schema = None

    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False

        return request.user.is_superadmin or request.user.is_superuser

    def has_object_permission(self, request, view, obj):
        if hasattr(obj, "enabled") and not obj.enabled:
            return False

        return request.user.is_superadmin or request.user.is_superuser


class GlobalPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for global watershed planning operations
    Allows superadmin to view all plans across all organizations and projects
    URL: /api/v1/watershed/plans/
    """

    schema = None

    serializer_class = PlanAppSerializer
    permission_classes = [permissions.IsAuthenticated, SuperAdminPlanPermission]

    def get_queryset(self):
        """
        Return all plans for superadmins
        """
        if not (self.request.user.is_superadmin or self.request.user.is_superuser):
            return PlanApp.objects.none()

        queryset = PlanApp.objects.filter(enabled=True)
        block_id = self.request.query_params.get("block", None)
        district_id = self.request.query_params.get("district", None)
        state_id = self.request.query_params.get("state", None)

        if block_id:
            queryset = queryset.filter(block=block_id)
        elif district_id:
            queryset = queryset.filter(district=district_id)
        elif state_id:
            queryset = queryset.filter(state=state_id)
        return queryset.order_by("-created_at")


class OrganizationPlanViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for organization level watershed planning ops
    Allows superadmins to view plans for a specific organization
    URL: /api/v1/organization/{organization_id}/watershed/plans/
    """

    schema = None

    serializer_class = PlanAppSerializer
    permissions_classes = [permissions.IsAuthenticated, SuperAdminPlanPermission]

    def get_queryset(self):
        """
        Filter plans by organizations for superadmins
        """
        if not (self.request.user.is_superadmin or self.request.user.is_superuser):
            return PlanApp.objects.none()

        organization_id = self.kwargs.get("organization_pk")
        if organization_id:
            try:
                organization = Organization.objects.get(pk=organization_id)
                return PlanApp.objects.filter(
                    organization=organization, enabled=True
                ).order_by("-created_at")
            except Organization.DoesNotExist:
                return PlanApp.objects.none()

        return PlanApp.objects.none()


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
        project_id = self.kwargs.get("project_pk")

        if self.request.user.is_superuser or self.request.user.is_superadmin:
            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    base_queryset = PlanApp.objects.filter(
                        project=project, enabled=True
                    )
                except Project.DoesNotExist:
                    return PlanApp.objects.none()
            else:
                base_queryset = PlanApp.objects.filter(enabled=True)

        elif self.request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists():
            base_queryset = PlanApp.objects.filter(
                organization=self.request.user.organization, enabled=True
            )

            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    if project.organization == self.request.user.organization:
                        base_queryset = base_queryset.filter(project=project)
                    else:
                        return PlanApp.objects.none()
                except Project.DoesNotExist:
                    return PlanApp.objects.none()

        else:
            # regular user
            if project_id:
                try:
                    project = Project.objects.get(
                        id=project_id, app_type=AppType.WATERSHED, enabled=True
                    )
                    base_queryset = PlanApp.objects.filter(
                        project=project, enabled=True
                    )
                except Project.DoesNotExist:
                    return PlanApp.objects.none()
            else:
                return PlanApp.objects.none()

        block_id = self.request.query_params.get("block_id", None)
        if block_id:
            base_queryset = base_queryset.filter(block=block_id)

        return base_queryset

    def get_serializer_class(self):
        """
        Use different serializers based on the action
        """
        if self.action in ["create"]:
            return PlanCreateSerializer
        elif self.action in ["update", "partial_update"]:
            return PlanUpdateSerializer
        elif self.action in ["list", "retrieve"]:
            return PlanAppSerializer
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

        plan = serializer.save(
            project=project, organization=project.organization, created_by=request.user
        )

        response_data = {
            "plan_data": PlanAppSerializer(plan).data,
            "message": f"Successfully created the watershed plan,{plan.plan}",
        }

        return Response(response_data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        """
        Update a watershed plan
        """
        partial = kwargs.pop("partial", False)
        instance = self.get_object()

        update_serializer = PlanUpdateSerializer(
            instance, data=request.data, partial=partial, context={"request": request}
        )
        update_serializer.is_valid(raise_exception=True)

        updated_instance = update_serializer.save()

        response_data = {
            "plan_data": PlanAppSerializer(updated_instance).data,
            "message": f"Successfully updated the watershed plan,{updated_instance.plan}",
        }

        return Response(response_data, status=status.HTTP_200_OK)

    def perform_destroy(self, instance):
        """
        Delete a watershed plan
        """
        instance.delete()
