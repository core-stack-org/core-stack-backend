# projects/views.py
from rest_framework import viewsets, permissions, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from .models import Project, ProjectApp, AppType
from .serializers import (
    ProjectSerializer,
    ProjectDetailSerializer,
    ProjectAppSerializer,
    AppTypeSerializer,
    ProjectAppUpdateSerializer,
)
from users.permissions import IsOrganizationMember, HasProjectPermission
from users.serializers import UserProjectGroup, UserProjectGroupSerializer


class ProjectViewSet(viewsets.ModelViewSet):
    serializer_class = ProjectSerializer
    permission_classes = [permissions.IsAuthenticated, IsOrganizationMember]

    def get_queryset(self):
        user = self.request.user

        # Super admins can see all projects
        if user.is_superadmin or user.is_superuser:
            return Project.objects.all()

        # Organization users can see their organization's projects
        if user.organization:
            return Project.objects.filter(organization=user.organization)

        # Users without organization see nothing
        return Project.objects.none()

    def get_serializer_class(self):
        if self.action == "retrieve":
            return ProjectDetailSerializer
        return ProjectSerializer

    def perform_create(self, serializer):
        # Ensure the project is created within the user's organization
        organization = self.request.user.organization
        serializer.save(organization=organization)

    @action(detail=True, methods=["get"])
    def apps(self, request, pk=None):
        project = self.get_object()
        project_apps = ProjectApp.objects.filter(project=project)
        serializer = ProjectAppSerializer(project_apps, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=["post"])
    def enable_app(self, request, pk=None):
        project = self.get_object()
        serializer = AppTypeSerializer(data=request.data)

        if serializer.is_valid():
            app_type = serializer.validated_data["app_type"]
            enabled = serializer.validated_data["enabled"]

            project_app, created = ProjectApp.objects.update_or_create(
                project=project, app_type=app_type, defaults={"enabled": enabled}
            )

            return Response(ProjectAppSerializer(project_app).data)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=["get"])
    def users(self, request, pk=None):
        project = self.get_object()
        user_roles = UserProjectGroup.objects.filter(project=project)
        serializer = UserProjectGroupSerializer(user_roles, many=True)
        return Response(serializer.data)


class ProjectAppViewSet(viewsets.ModelViewSet):
    serializer_class = ProjectAppSerializer
    permission_classes = [permissions.IsAuthenticated, HasProjectPermission]

    def get_queryset(self):
        return ProjectApp.objects.filter(project_id=self.kwargs.get("project_pk"))

    def get_serializer_class(self):
        if self.action in ["update", "partial_update", "create"]:
            return ProjectAppUpdateSerializer
        return ProjectAppSerializer

    def perform_create(self, serializer):
        project = get_object_or_404(Project, pk=self.kwargs.get("project_pk"))
        serializer.save(project=project)
