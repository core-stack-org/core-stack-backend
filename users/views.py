from rest_framework import status, viewsets, mixins, generics, permissions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.authentication import JWTAuthentication
from django.contrib.auth.models import Group
from .models import User, UserProjectGroup
from .serializers import (
    UserSerializer,
    UserRegistrationSerializer,
    GroupSerializer,
    UserProjectGroupSerializer,
    PasswordChangeSerializer,
)
from projects.models import Project
from projects.serializers import ProjectSerializer
from organization.models import Organization
from organization.serializers import OrganizationSerializer
import logging

logger = logging.getLogger(__name__)


class RegisterView(viewsets.GenericViewSet, generics.CreateAPIView):
    """API endpoint for user registration."""

    queryset = User.objects.all()
    serializer_class = UserRegistrationSerializer
    permission_classes = [permissions.AllowAny]

    @action(detail=False, methods=["get"])
    def available_organizations(self, request):
        """Get list of organizations that users can register for."""

        # Get all organizations
        organizations = Organization.objects.all()

        # Return organization name and ID
        organization_data = [
            {"id": str(org.id), "name": org.name} for org in organizations
        ]

        return Response(organization_data)

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()

        # Generate tokens for the new user
        refresh = RefreshToken.for_user(user)

        # Return user info and tokens
        return Response(
            {
                "user": UserSerializer(user).data,
                "token": str(refresh.access_token),
                "refresh_token": str(refresh),
            },
            status=status.HTTP_201_CREATED,
        )


class LoginView(TokenObtainPairView):
    """
    API endpoint for user login.
    Extends SimpleJWT's TokenObtainPairView to customize the response.
    """

    def post(self, request, *args, **kwargs):
        # Call parent class method to validate credentials and get tokens
        response = super().post(request, *args, **kwargs)

        # Get the user from the validated data
        token = response.data.get("access")
        jwt_auth = JWTAuthentication()
        validated_token = jwt_auth.get_validated_token(token)
        user = jwt_auth.get_user(validated_token)

        # Add user info to response
        response.data["user"] = UserSerializer(user).data

        return response


class LogoutView(generics.GenericAPIView):
    """API endpoint for user logout - invalidates refresh token."""

    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        try:
            # Get the refresh token from request
            refresh_token = request.data.get("refresh_token")

            # Blacklist the token
            token = RefreshToken(refresh_token)
            token.blacklist()

            return Response({"success": True}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"success": False, "error": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class IsSuperAdminOrOrgAdmin(permissions.BasePermission):
    """Permission to allow only superadmins or organization admins."""

    def has_permission(self, request, view):
        return request.user.is_authenticated and (
            request.user.is_superadmin
            or request.user.groups.filter(
                name__in=["Organization Admin", "Org Admin", "Administrator"]
            ).exists()
        )


class UserViewSet(viewsets.ModelViewSet):
    """API endpoint for user management."""

    queryset = User.objects.all()
    serializer_class = UserSerializer

    def get_permissions(self):
        """Set custom permissions based on action."""
        if self.action in ["list", "create"]:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        elif self.action in ["retrieve", "update", "partial_update", "my_projects"]:
            # Users can view/edit their own profile and see their projects
            permission_classes = [permissions.IsAuthenticated]
        else:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        return [permission() for permission in permission_classes]

    def get_queryset(self):
        """Filter users based on permissions."""
        user = self.request.user

        # Superadmins can see all users
        if user.is_superadmin:
            return User.objects.all()

        # Organization admins can see users in their organization
        if (
            user.organization
            and user.groups.filter(
                name__in=["Organization Admin", "Org Admin", "Administrator"]
            ).exists()
        ):
            return User.objects.filter(organization=user.organization)

        # Regular users can only see themselves
        return User.objects.filter(id=user.id)

    def retrieve(self, request, *args, **kwargs):
        """Get user details."""
        instance = self.get_object()

        # Allow users to view their own details
        if request.user.id != instance.id and not (
            request.user.is_superadmin
            or (
                request.user.organization == instance.organization
                and request.user.groups.filter(name="Organization Admin").exists()
            )
        ):
            return Response(
                {"detail": "You do not have permission to view this user."},
                status=status.HTTP_403_FORBIDDEN,
            )

        serializer = self.get_serializer(instance)
        return Response(serializer.data)

    def update(self, request, *args, **kwargs):
        """Update user profile."""
        instance = self.get_object()

        # Allow users to update their own profile
        if request.user.id != instance.id and not (
            request.user.is_superadmin
            or (
                request.user.organization == instance.organization
                and request.user.groups.filter(name="Organization Admin").exists()
            )
        ):
            return Response(
                {"detail": "You do not have permission to update this user."},
                status=status.HTTP_403_FORBIDDEN,
            )

        # Prevent changing specific fields unless superadmin
        if not request.user.is_superadmin:
            # Remove is_superadmin and is_staff from the data if present
            mutable_data = request.data.copy()
            mutable_data.pop("is_superadmin", None)
            mutable_data.pop("is_staff", None)
            mutable_data.pop("is_active", None)

            # Only org admins can change organization
            if not request.user.groups.filter(name="Organization Admin").exists():
                mutable_data.pop("organization", None)

            partial = kwargs.pop("partial", False)
            serializer = self.get_serializer(
                instance, data=mutable_data, partial=partial
            )
        else:
            partial = kwargs.pop("partial", False)
            serializer = self.get_serializer(
                instance, data=request.data, partial=partial
            )

        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        return Response(serializer.data)

    @action(detail=False, methods=["post"])
    def change_password(self, request):
        """Change the user's password."""
        serializer = PasswordChangeSerializer(
            data=request.data, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()

        # After password change, invalidate all refresh tokens
        # This is a security measure to log out the user from all devices
        RefreshToken.for_user(request.user)

        return Response(
            {
                "detail": "Password changed successfully. Please login again with your new password."
            },
            status=status.HTTP_200_OK,
        )

    @action(detail=True, methods=["put"])
    def set_organization(self, request, pk=None):
        """Assign user to an organization."""
        user = self.get_object()
        organization_id = request.data.get("organization_id")

        if not (
            request.user.is_superadmin
            or (
                request.user.groups.filter(name="Organization Admin").exists()
                and request.user.organization.id == organization_id
            )
        ):
            return Response(
                {
                    "detail": "You do not have permission to assign users to this organization."
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        # Update user's organization
        from organization.models import Organization

        try:
            organization = Organization.objects.get(id=organization_id)
            user.organization = organization
            user.save()

            return Response(UserSerializer(user).data)
        except Organization.DoesNotExist:
            return Response(
                {"detail": "Organization not found."}, status=status.HTTP_404_NOT_FOUND
            )

    @action(detail=True, methods=["put"])
    def set_group(self, request, pk=None):
        """Assign a user to a group (role)."""
        user = self.get_object()
        group_id = request.data.get("group_id")

        # Check if the requester is a superadmin or an organization admin
        is_superadmin = request.user.is_superadmin
        is_org_admin = request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists()

        if not (is_superadmin or is_org_admin):
            return Response(
                {"detail": "You do not have permission to assign users to groups."},
                status=status.HTTP_403_FORBIDDEN,
            )

        # Organization admins can only assign users from their own organization
        if is_org_admin and not is_superadmin:
            # Check if the user belongs to the same organization as the requester
            if user.organization != request.user.organization:
                return Response(
                    {
                        "detail": "You can only assign users from your organization to groups."
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        # Get the group
        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."}, status=status.HTTP_404_NOT_FOUND
            )

        # Special handling for Organization Admin group
        if group.name == "Organization Admin":
            # Ensure the user has an organization assigned
            if not user.organization:
                return Response(
                    {
                        "detail": "User must be assigned to an organization before being made an Organization Admin."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # If the user is being assigned to an organization different from the requester's
            # organization, only superadmins can do this
            if (
                request.user.organization != user.organization
                and not request.user.is_superadmin
            ):
                return Response(
                    {
                        "detail": "You can only assign users in your organization as Organization Admins."
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        # Add the user to the group
        user.groups.add(group)

        return Response(
            {
                "message": f"User {user.username} successfully added to group {group.name}",
                "user": UserSerializer(user).data,
            }
        )

    @action(detail=True, methods=["put"])
    def remove_group(self, request, pk=None):
        """Remove a user from a group (role)."""
        user = self.get_object()
        group_id = request.data.get("group_id")

        # Only superadmins can remove users from groups
        if not request.user.is_superadmin:
            return Response(
                {"detail": "You do not have permission to remove users from groups."},
                status=status.HTTP_403_FORBIDDEN,
            )

        # Get the group
        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."}, status=status.HTTP_404_NOT_FOUND
            )

        # Remove the user from the group
        user.groups.remove(group)

        return Response(
            {
                "message": f"User {user.username} successfully removed from group {group.name}",
                "user": UserSerializer(user).data,
            }
        )

    @action(detail=False, methods=["get"])
    def my_projects(self, request):
        """Get all projects the current user is assigned to with their roles."""
        user = request.user
        user_project_groups = UserProjectGroup.objects.filter(user=user).select_related(
            "project", "group"
        )

        projects_data = []
        for upg in user_project_groups:
            projects_data.append(
                {
                    "project": {
                        "id": upg.project.id,
                        "name": upg.project.name,
                        "description": upg.project.description,
                        "app_type": upg.project.app_type,
                        "enabled": upg.project.enabled,
                        "organization": str(upg.project.organization.id)
                        if upg.project.organization
                        else None,
                        "organization_name": upg.project.organization.name
                        if upg.project.organization
                        else None,
                    },
                    "role": {"id": upg.group.id, "name": upg.group.name},
                }
            )

        return Response(projects_data)


class GroupViewSet(viewsets.ReadOnlyModelViewSet):
    """API endpoint for group management (read-only)."""

    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class UserProjectGroupViewSet(viewsets.ModelViewSet):
    """API endpoint for managing user-project assignments."""

    serializer_class = UserProjectGroupSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        project_id = self.kwargs.get("project_pk")
        if project_id:
            return UserProjectGroup.objects.filter(project_id=project_id)
        return UserProjectGroup.objects.none()

    def get_serializer_class(self):
        if self.action == "list":
            return UserProjectGroupSerializer
        return UserProjectGroupSerializer

    def create(self, request, *args, **kwargs):
        project_id = self.kwargs.get("project_pk")
        if not project_id:
            return Response(
                {"detail": "Project ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Add project_id to request data
        mutable_data = request.data.copy()
        mutable_data["project_id"] = project_id

        # Handle both user/user_id and group/group_id field names
        user_id = mutable_data.get("user_id") or mutable_data.get("user")
        group_id = mutable_data.get("group_id") or mutable_data.get("group")

        if not user_id:
            return Response(
                {"detail": "User ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not group_id:
            return Response(
                {"detail": "Group ID is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Ensure the data has the fields the serializer expects
        mutable_data["user_id"] = user_id
        mutable_data["group_id"] = group_id

        serializer = self.get_serializer(data=mutable_data)
        serializer.is_valid(raise_exception=True)

        # Verify the user belongs to the same organization as the project
        project = Project.objects.get(id=project_id)
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response(
                {"detail": f"User with ID {user_id} does not exist."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if user.organization != project.organization:
            return Response(
                {"detail": "User must belong to the same organization as the project."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if the user already has a role in this project
        existing = UserProjectGroup.objects.filter(
            user=user, project_id=project_id
        ).first()
        if existing:
            # Update existing role
            existing.group_id = group_id
            existing.save()

            # Also update the user's global group
            try:
                group = Group.objects.get(id=group_id)
                # Remove user from all groups they might be in
                # Uncomment this if you want to remove from all other groups
                # user.groups.clear()
                # Add user to the selected group
                user.groups.add(group)
            except Group.DoesNotExist:
                # Log this error but don't fail the request
                logger.error(
                    f"Failed to add user {user.id} to global group {group_id}: Group does not exist"
                )

            return Response(
                UserProjectGroupSerializer(existing).data, status=status.HTTP_200_OK
            )

        # Create new role assignment
        self.perform_create(serializer)

        # Also add the user to the global group
        try:
            group = Group.objects.get(id=group_id)
            # Remove user from all groups they might be in
            # Uncomment this if you want to remove from all other groups
            # user.groups.clear()
            # Add user to the selected group
            user.groups.add(group)
        except Group.DoesNotExist:
            # Log this error but don't fail the request
            logger.error(
                f"Failed to add user {user.id} to global group {group_id}: Group does not exist"
            )

        headers = self.get_success_headers(serializer.data)
        return Response(
            serializer.data, status=status.HTTP_201_CREATED, headers=headers
        )
