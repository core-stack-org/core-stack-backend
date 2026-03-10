import logging

from django.conf import settings
from django.contrib.auth.models import Group
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from django.shortcuts import render
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from django.views import View
from rest_framework import generics, permissions, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.token_blacklist.models import OutstandingToken
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from organization.models import Organization
from projects.models import Project
from utilities.mailutils import send_email

from .models import User, UserProjectGroup
from .serializers import (
    AdminResetPasswordSerializer,
    ForgotPasswordSerializer,
    GroupSerializer,
    PasswordChangeSerializer,
    UserProjectGroupSerializer,
    UserRegistrationSerializer,
    UserSerializer,
)

logger = logging.getLogger(__name__)


class RegisterView(viewsets.GenericViewSet, generics.CreateAPIView):
    """API endpoint for user registration."""

    queryset = User.objects.all()
    serializer_class = UserRegistrationSerializer
    permission_classes = [permissions.AllowAny]
    schema = None

    @action(detail=False, methods=["get"])
    def available_organizations(self, request):
        """Get list of organizations that users can register for."""
        app_type = request.query_params.get("app_type", None)
        if not app_type:
            organizations = Organization.objects.all().order_by("name")
        else:
            organizations = (
                Organization.objects.filter(
                    projects__app_type=app_type, projects__enabled=True
                )
                .distinct()
                .order_by("name")
            )

        organization_data = [
            {"id": str(org.id), "name": org.name} for org in organizations
        ]

        return Response(organization_data)

    @action(detail=False, methods=["get"])
    def available_organizations_by_app_type(self, request):
        """Get list of organizations that users can register for."""

        app_type = request.query_params.get("app_type")

        organizations = Organization.objects.all()

        if app_type:
            organizations = organizations.filter(app_type=app_type)

        organization_data = [
            {"id": str(org.id), "name": org.name} for org in organizations
        ]

        return Response(organization_data)

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()

        refresh = RefreshToken.for_user(user)

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

    schema = None

    def post(self, request, *args, **kwargs):
        # Call parent class method to validate credentials and get tokens
        response = super().post(request, *args, **kwargs)

        token = response.data.get("access")
        jwt_auth = JWTAuthentication()
        validated_token = jwt_auth.get_validated_token(token)
        user = jwt_auth.get_user(validated_token)

        response.data["user"] = UserSerializer(user).data

        return response


class LogoutView(generics.GenericAPIView):
    """API endpoint for user logout - invalidates refresh token."""

    permission_classes = [permissions.IsAuthenticated]
    schema = None

    def post(self, request):
        try:
            refresh_token = request.data.get("refresh_token")

            token = RefreshToken(refresh_token)
            token.blacklist()

            return Response({"success": True}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"success": False, "error": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class IsSuperAdminOrOrgAdmin(permissions.BasePermission):
    """Permission to allow only superadmins or organization admins."""

    schema = None

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
    schema = None

    def get_permissions(self):
        """Set custom permissions based on action."""
        if self.action in ["list", "create"]:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        elif self.action in ["retrieve", "update", "partial_update", "my_projects"]:
            permission_classes = [permissions.IsAuthenticated]
        else:
            permission_classes = [IsSuperAdminOrOrgAdmin]
        return [permission() for permission in permission_classes]

    def get_queryset(self):
        """Filter users based on permissions."""
        user = self.request.user

        if user.is_superadmin:
            return User.objects.all()

        if (
            user.organization
            and user.groups.filter(
                name__in=["Organization Admin", "Org Admin", "Administrator"]
            ).exists()
        ):
            return User.objects.filter(organization=user.organization)

        return User.objects.filter(id=user.id)

    def retrieve(self, request, *args, **kwargs):
        """Get user details."""
        instance = self.get_object()

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

    @action(detail=True, methods=["post"], permission_classes=[IsSuperAdminOrOrgAdmin])
    def reset_password(self, request, pk=None):
        target_user = self.get_object()

        if not request.user.is_superadmin:
            if target_user.organization != request.user.organization:
                return Response(
                    {"detail": "You can only reset passwords for users in your organization."},
                    status=status.HTTP_403_FORBIDDEN,
                )

        serializer = AdminResetPasswordSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        target_user.set_password(serializer.validated_data["new_password"])
        target_user.save()

        OutstandingToken.objects.filter(user=target_user).delete()

        return Response(
            {"detail": f"Password for {target_user.username} has been reset successfully."},
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

        is_superadmin = request.user.is_superadmin
        is_org_admin = request.user.groups.filter(
            name__in=["Organization Admin", "Org Admin", "Administrator"]
        ).exists()

        if not (is_superadmin or is_org_admin):
            return Response(
                {"detail": "You do not have permission to assign users to groups."},
                status=status.HTTP_403_FORBIDDEN,
            )

        if is_org_admin and not is_superadmin:
            if user.organization != request.user.organization:
                return Response(
                    {
                        "detail": "You can only assign users from your organization to groups."
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."}, status=status.HTTP_404_NOT_FOUND
            )

        # TODO: add other types as well
        if group.name in ["Organization Admin", "Org Admin", "Administrator"]:
            if not user.organization:
                return Response(
                    {
                        "detail": "User must be assigned to an organization before being made an Organization Admin."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

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

        try:
            group = Group.objects.get(id=group_id)
        except Group.DoesNotExist:
            return Response(
                {"detail": "Group not found."}, status=status.HTTP_404_NOT_FOUND
            )

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
                        "organization": (
                            str(upg.project.organization.id)
                            if upg.project.organization
                            else None
                        ),
                        "organization_name": (
                            upg.project.organization.name
                            if upg.project.organization
                            else None
                        ),
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
    schema = None


class UserProjectGroupViewSet(viewsets.ModelViewSet):
    """API endpoint for managing user-project assignments."""

    serializer_class = UserProjectGroupSerializer
    permission_classes = [permissions.IsAuthenticated]
    schema = None

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

        mutable_data = request.data.copy()
        mutable_data["project_id"] = project_id

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


password_reset_token_generator = PasswordResetTokenGenerator()


class ForgotPasswordView(APIView):
    permission_classes = [permissions.AllowAny]
    schema = None

    GENERIC_MSG = "If a matching account exists and has an email on file, a password reset link has been sent."

    def post(self, request):
        serializer = ForgotPasswordSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        username = serializer.validated_data["username"]
        provided_email = serializer.validated_data.get("email")

        user = User.objects.filter(username=username).first()
        if user is None:
            return Response({"detail": self.GENERIC_MSG}, status=status.HTTP_200_OK)

        target_email = user.email
        if not target_email:
            if not provided_email:
                return Response(
                    {"detail": "No email address on file for this account. Please provide an email to receive the reset link.",
                     "email_required": True},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            user.email = provided_email
            user.save(update_fields=["email"])
            target_email = provided_email

        token = password_reset_token_generator.make_token(user)
        uidb64 = urlsafe_base64_encode(force_bytes(user.pk))
        reset_url = request.build_absolute_uri(
            f"/api/v1/auth/reset-password/{uidb64}/{token}/"
        )

        html_body = render(
            request, "password_reset_email.html",
            {"user": user, "reset_url": reset_url},
        ).content.decode()

        send_email(
            subject="Password Reset Request - CoRE Stack",
            text_body=f"Hi {user.first_name or user.username},\n\nClick the link to reset your password:\n{reset_url}\n\nIf you did not request this, ignore this email.",
            to_emails=[target_email],
            html_body=html_body,
        )

        return Response({"detail": self.GENERIC_MSG}, status=status.HTTP_200_OK)


class ResetPasswordView(View):

    def _get_user(self, uidb64):
        try:
            uid = force_str(urlsafe_base64_decode(uidb64))
            return User.objects.get(pk=uid)
        except (TypeError, ValueError, OverflowError, User.DoesNotExist):
            return None

    def get(self, request, uidb64, token):
        user = self._get_user(uidb64)
        if user is None or not password_reset_token_generator.check_token(user, token):
            return render(request, "password_reset_invalid.html", status=400)
        return render(request, "password_reset.html", {"uidb64": uidb64, "token": token})

    def post(self, request, uidb64, token):
        user = self._get_user(uidb64)
        if user is None or not password_reset_token_generator.check_token(user, token):
            return render(request, "password_reset_invalid.html", status=400)

        new_password = request.POST.get("new_password", "")
        confirm_password = request.POST.get("confirm_password", "")

        if not new_password or len(new_password) < 8:
            return render(request, "password_reset.html", {
                "uidb64": uidb64, "token": token,
                "error": "Password must be at least 8 characters long.",
            })

        if new_password != confirm_password:
            return render(request, "password_reset.html", {
                "uidb64": uidb64, "token": token,
                "error": "Passwords do not match.",
            })

        user.set_password(new_password)
        user.save()

        OutstandingToken.objects.filter(user=user).delete()

        return render(request, "password_reset_done.html")
