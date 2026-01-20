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
            "age",
            "education_qualification",
            "gender",
            "profile_picture",
            "account_type",
        ]
        read_only_fields = ["id", "is_active"]

    def get_groups(self, obj):
        """Get simplified groups list."""
        return [{"id": group.id, "name": group.name} for group in obj.groups.all()]

    def get_project_details(self, obj):
        """Get project-specific roles for the user."""
        if (
            obj.groups.filter(
                name__in=["Organization Admin", "Org Admin", "Administrator"]
            ).exists()
            and obj.organization
        ):
            projects = Project.objects.filter(organization=obj.organization)
            return [
                {
                    "project_id": project.id,
                    "project_name": project.name,
                }
                for project in projects
            ]

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
    organization = serializers.CharField(
        required=False, write_only=True
    )  # Changed from UUIDField to CharField

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
            "age",
            "education_qualification",
            "gender",
            "profile_picture",
            "account_type",
        ]
        read_only_fields = ["id"]

    def validate(self, attrs):
        if attrs["password"] != attrs["password_confirm"]:
            raise serializers.ValidationError(
                {"password": "Password fields didn't match."}
            )

        organization_input = attrs.get("organization")
        if organization_input:
            import uuid

            from organization.models import Organization

            try:
                uuid.UUID(organization_input)
                is_uuid = True
            except (ValueError, TypeError):
                is_uuid = False

            if is_uuid:
                try:
                    organization = Organization.objects.get(id=organization_input)
                    attrs["organization_obj"] = organization
                except Organization.DoesNotExist:
                    raise serializers.ValidationError(
                        {"organization": "Organization not found."}
                    )
            else:
                try:
                    organization = Organization.objects.filter(
                        name__iexact=organization_input
                    ).first()

                    if organization:
                        attrs["organization_obj"] = organization
                    else:
                        attrs["_new_org_name"] = organization_input
                except Exception as e:
                    raise serializers.ValidationError(
                        {
                            "organization": f"Failed to create/find organization: {str(e)}"
                        }
                    )

        return attrs

    def create(self, validated_data):
        from organization.models import Organization

        # pop custom fields
        org_obj = validated_data.pop("organization_obj", None)
        new_org_name = validated_data.pop("_new_org_name", None)
        validated_data.pop("password_confirm")

        # Create the user first
        user = User.objects.create_user(
            username=validated_data["username"],
            email=validated_data["email"],
            password=validated_data["password"],
            first_name=validated_data.get("first_name", ""),
            last_name=validated_data.get("last_name", ""),
            contact_number=validated_data.get("contact_number", ""),
            age=validated_data.get("age"),
            education_qualification=validated_data.get("education_qualification", ""),
            gender=validated_data.get("gender", ""),
            profile_picture=validated_data.get("profile_picture"),
            account_type=validated_data.get("account_type"),
        )

        if org_obj:
            user.organization = org_obj
        elif new_org_name:
            org_obj = Organization.objects.create(
                name=new_org_name,
                created_by=user,  # <-- here we set created_by to the same new user
            )
            user.organization = org_obj
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
