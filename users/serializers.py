from rest_framework import serializers
from django.contrib.auth.models import Group
from django.contrib.auth.password_validation import validate_password
from .models import User, UserProjectGroup
from projects.models import Project


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
    organization = serializers.UUIDField(required=False, write_only=True)

    class Meta:
        model = User
        fields = [
            "username",
            "email",
            "password",
            "password_confirm",
            "first_name",
            "last_name",
            "contact_number",
            "organization",
        ]

    def validate(self, attrs):
        # Check that passwords match
        if attrs["password"] != attrs["password_confirm"]:
            raise serializers.ValidationError(
                {"password": "Password fields didn't match."}
            )

        # Validate organization if provided
        organization_id = attrs.get("organization")
        if organization_id:
            from organization.models import Organization

            try:
                organization = Organization.objects.get(id=organization_id)
                # Store the organization object for use in create
                attrs["organization_obj"] = organization
            except Organization.DoesNotExist:
                raise serializers.ValidationError(
                    {"organization": "Organization not found."}
                )

        return attrs

    def create(self, validated_data):
        # Remove password_confirm as it's not part of the User model
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
