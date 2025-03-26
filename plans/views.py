# plans/views.py
from django.shortcuts import get_object_or_404
from rest_framework import viewsets, permissions, status
from rest_framework.response import Response
from projects.models import Project, AppType
from users.permissions import IsOrganizationMember, HasProjectPermission
from .models import Plan
from .serializers import PlanSerializer, PlanCreateSerializer


class PlanViewSet(viewsets.ModelViewSet):
    """
    ViewSet for watershed planning operations
    """

    serializer_class = PlanSerializer
    permission_classes = [permissions.IsAuthenticated, HasProjectPermission]
    # For the HasProjectPermission to work correctly
    app_type = AppType.WATERSHED

    def get_queryset(self):
        """
        Filter plans by project
        """
        project_id = self.kwargs.get("project_pk")
        if project_id:
            # Get the watershed project
            try:
                project = Project.objects.get(
                    id=project_id, app_type=AppType.WATERSHED, enabled=True
                )
                return Plan.objects.filter(project=project)
            except Project.DoesNotExist:
                return Plan.objects.none()
        return Plan.objects.none()

    def get_serializer_class(self):
        """
        Use different serializers based on the action
        """
        if self.action in ["create"]:
            return PlanCreateSerializer
        return PlanSerializer

    def create(self, request, *args, **kwargs):
        """
        Create a new watershed plan
        """
        project_id = self.kwargs.get("project_pk")
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get project and check if it's a watershed project and enabled
        try:
            project = Project.objects.get(
                id=project_id, app_type=AppType.WATERSHED, enabled=True
            )
        except Project.DoesNotExist:
            return Response(
                {"detail": "Watershed planning is not enabled for this project."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Add project, organization, and created_by
        plan = serializer.save(
            project=project, organization=project.organization, created_by=request.user
        )

        # Use the full serializer for response
        return Response(PlanSerializer(plan).data, status=status.HTTP_201_CREATED)

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
