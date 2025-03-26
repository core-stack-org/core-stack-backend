# projects/views.py
from rest_framework import viewsets, permissions, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from .models import Project, AppType
from .serializers import (
    ProjectSerializer,
    ProjectDetailSerializer,
    AppTypeSerializer,
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

    @action(detail=True, methods=["patch"])
    def update_app_type(self, request, pk=None):
        project = self.get_object()
        serializer = AppTypeSerializer(data=request.data)

        if serializer.is_valid():
            app_type = serializer.validated_data["app_type"]
            enabled = serializer.validated_data["enabled"]

            project.app_type = app_type
            project.enabled = enabled
            project.save()

            return Response(ProjectSerializer(project).data)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=["get"])
    def users(self, request, pk=None):
        project = self.get_object()
        user_roles = UserProjectGroup.objects.filter(project=project)
        serializer = UserProjectGroupSerializer(user_roles, many=True)
        return Response(serializer.data)
