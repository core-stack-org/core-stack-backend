from django.shortcuts import render
from rest_framework import viewsets, permissions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import Organization
from .serializers import OrganizationSerializer
from users.models import User
from users.serializers import UserSerializer


class IsSuperAdmin(permissions.BasePermission):
    """
    Permission class that allows only superadmins to access the endpoint.
    """

    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.is_superuser


class OrganizationViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows organizations to be viewed or edited.
    """

    queryset = Organization.objects.all()
    serializer_class = OrganizationSerializer
    permission_classes = [IsSuperAdmin]

    def get_permissions(self):
        """
        Returns the permission objects that should be applied to the request.
        """
        if self.action in ["create", "delete"]:
            permission_classes = [
                IsSuperAdmin
            ]  # only superadmins can create or delete organizations

        elif self.action in ["retrieve", "update", "partial_update", "list"]:
            permission_classes = [
                permissions.IsAuthenticated
            ]  # only authenticated users can retrieve, update, or partial_update an organization

        else:
            permission_classes = [permissions.IsAuthenticated]

        return [permission() for permission in permission_classes]

    def perform_create(self, serializer):
        serializer.save(created_by=self.request.user, updated_by=self.request.user)

    def check_organization_permissions(self, organization):
        """
        Check if the user is allowed to access the organization.
        """
        user = self.request.user
        return (
            user.is_superadmin
            or user == organization.created_by
            or user.organization == organization
        )

    def retrieve(self, request, *args, **kwargs):
        """
        Get specific organization
        """
        instance = self.get_object()
        if not self.check_organization_permissions(instance):
            return Response(
                {"message": "You don't have permission to access this organization."},
                status=status.HTTP_403_BAD_REQUEST,
            )

        serializer = self.get_serializer(instance)
        return Response(
            {"message": "success", "data": serializer.data}, status=status.HTTP_200_OK
        )

    def update(self, request, *args, **kwargs):
        """
        Update an organization
        """
        instance = self.get_object()
        if not self.check_organization_permissions(instance):
            return Response(
                {"message": "You don't have permission to access this organization."},
                status=status.HTTP_403_BAD_REQUEST,
            )

        partial = kwargs.pop("partial", False)
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        return Response(
            {"message": "success", "data": serializer.data}, status=status.HTTP_200_OK
        )

    @action(detail=True, methods=["get"])
    def users(self, request, pk=None):
        """
        Lists users associated with the organization
        """

        organization = self.get_object()

        if not self.check_organization_permissions(organization):
            return Response(
                {
                    "detail": "You do not have permission to view users in this organization."
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        users = User.objects.filter(organization=organization)
        serializer = UserSerializer(users, many=True)
        return Response(
            {"message": "success", "data": serializer.data}, status=status.HTTP_200_OK
        )
