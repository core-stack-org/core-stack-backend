from rest_framework import permissions, serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from users.permissions import IsOrganizationMember
from users.serializers import UserProjectGroup, UserProjectGroupSerializer

from .models import Project
from .serializers import (
    AppTypeSerializer,
    ProjectDetailSerializer,
    ProjectSerializer,
)


class ProjectViewSet(viewsets.ModelViewSet):
    serializer_class = ProjectSerializer
    permission_classes = [permissions.IsAuthenticated, IsOrganizationMember]
    schema = None

    def get_queryset(self):
        user = self.request.user

        if user.is_superadmin or user.is_superuser:
            return Project.objects.all()

        if user.organization:
            return Project.objects.filter(organization=user.organization)

        return Project.objects.none()

    def get_serializer_class(self):
        if self.action == "retrieve":
            return ProjectDetailSerializer
        return ProjectSerializer

    def perform_create(self, serializer):
        user = self.request.user

        if user.is_superadmin or user.is_superuser:
            if (
                "organization" not in serializer.validated_data
                or not serializer.validated_data["organization"]
            ):
                raise serializers.ValidationError(
                    {
                        "organization": "Organization ID is required for superadmin users."
                    }
                )
            serializer.save()
        else:
            organization = user.organization
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
