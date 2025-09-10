from django.contrib.auth.models import Group
from django.contrib.auth.password_validation import validate_password
from rest_framework import serializers

from projects.models import Project

from .models import User, UserProjectGroup


class UserSerializer(serializers.ModelSerializer):
    """Serializer for user details."""

    organization_name = serializers.CharField(
        source="organization.name", read_only=True
    )
    groups = serializers.SerializerMethodField()
    project_details = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "email",
            "first_name",
            "last_name",
            "contact_number",
            "organization",
            "organization_name",
            "is_active",
            "groups",
            "project_details",
            "is_superadmin",
        ]
        read_only_fields = ["id", "is_active"]

    def get_groups(self, obj):
        """Get simplified groups list."""
        return [{"id": group.id, "name": group.name} for group in obj.groups.all()]

    def get_project_details(self, obj):
        """Get project-specific roles for the user."""
        project_details = UserProjectGroup.objects.filter(user=obj).select_related(
            "project", "group"
        )
        return [
            {
                "project_id": role.project.id,
                "project_name": role.project.name,
            }
            for role in project_details
        ]


class UserRegistrationSerializer(serializers.ModelSerializer):
    """Serializer for user registration."""

    password = serializers.CharField(
        write_only=True, required=True, validators=[validate_password]
    )
    password_confirm = serializers.CharField(write_only=True, required=True)
    organization = serializers.CharField(required=False, write_only=True)  # Changed from UUIDField to CharField

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "email",
            "password",
            "password_confirm",
            "first_name",
            "last_name",
            "contact_number",
            "organization",
        ]
        read_only_fields = ["id"]

    def validate(self, attrs):
        # Check that passwords match
        if attrs["password"] != attrs["password_confirm"]:
            raise serializers.ValidationError(
                {"password": "Password fields didn't match."}
            )

        # Handle organization validation and creation
        organization_input = attrs.get("organization")
        
        if organization_input:
            from organization.models import Organization
            import uuid

            # Check if the input is a valid UUID
            try:
                uuid.UUID(organization_input)
                is_uuid = True
            except (ValueError, TypeError):
                is_uuid = False

            if is_uuid:
                # Handle as UUID - find existing organization
                try:
                    organization = Organization.objects.get(id=organization_input)
                    attrs["organization_obj"] = organization
                except Organization.DoesNotExist:
                    raise serializers.ValidationError(
                        {"organization": "Organization not found."}
                    )
            else:
                # Handle as organization name - create if doesn't exist
                try:
                    organization, created = Organization.objects.get_or_create(
                        name__iexact=organization_input,
                        defaults={'name': organization_input}
                    )
                    attrs["organization_obj"] = organization
                except Exception as e:
                    raise serializers.ValidationError(
                        {"organization": f"Failed to create/find organization: {str(e)}"}
                    )

        return attrs

    def create(self, validated_data):
        # Remove fields that aren't part of the User model
        validated_data.pop("password_confirm")

        # Handle organization separately
        organization = None
        if "organization_obj" in validated_data:
            organization = validated_data.pop("organization_obj")
        if "organization" in validated_data:
            validated_data.pop("organization")

        # Create the user with a hashed password
        user = User.objects.create_user(**validated_data)

        # Assign organization if provided
        if organization:
            user.organization = organization
            user.save()

        return user



class GroupSerializer(serializers.ModelSerializer):
    """Serializer for Django's Group model."""

    permissions = serializers.SerializerMethodField()

    class Meta:
        model = Group
        fields = ["id", "name", "permissions"]

    def get_permissions(self, obj):
        """Get simplified permissions list."""
        return [
            {"id": perm.id, "codename": perm.codename, "name": perm.name}
            for perm in obj.permissions.all()
        ]


class UserProjectGroupSerializer(serializers.ModelSerializer):
    """Serializer for user project role assignments."""

    user_id = serializers.PrimaryKeyRelatedField(
        source="user", queryset=User.objects.all()
    )
    username = serializers.CharField(source="user.username", read_only=True)
    group_id = serializers.PrimaryKeyRelatedField(
        source="group", queryset=Group.objects.all()
    )
    group_name = serializers.CharField(source="group.name", read_only=True)
    project_id = serializers.PrimaryKeyRelatedField(
        source="project", queryset=Project.objects.all(), required=False
    )

    class Meta:
        model = UserProjectGroup
        fields = ["id", "user_id", "username", "group_id", "group_name", "project_id"]


class PasswordChangeSerializer(serializers.Serializer):
    """Serializer for changing user password."""

    old_password = serializers.CharField(required=True, write_only=True)
    new_password = serializers.CharField(
        required=True, write_only=True, validators=[validate_password]
    )
    new_password_confirm = serializers.CharField(required=True, write_only=True)

    def validate_old_password(self, value):
        user = self.context["request"].user
        if not user.check_password(value):
            raise serializers.ValidationError("Current password is incorrect.")
        return value

    def validate(self, attrs):
        if attrs["new_password"] != attrs["new_password_confirm"]:
            raise serializers.ValidationError(
                {"new_password": "New password fields didn't match."}
            )
        return attrs

    def save(self, **kwargs):
        user = self.context["request"].user
        user.set_password(self.validated_data["new_password"])
        user.save()
        return user
