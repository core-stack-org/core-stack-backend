from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

from .models import User, UserProjectGroup


@admin.register(User)
class CustomUserAdmin(UserAdmin):
    """Custom admin for the User model."""

    list_display = (
        "username",
        "first_name",
        "last_name",
        "email",
        "organization",
        "age",
        "education_qualification",
        "gender",
        "get_groups",
        "is_superadmin",
        "is_superuser",
    )
    list_filter = (
        "is_superadmin",
        "is_staff",
        "is_active",
        "groups",
        "organization",
        "gender",
    )
    search_fields = ("username", "email", "first_name", "last_name")
    autocomplete_fields = ("organization",)

    fieldsets = (
        (None, {"fields": ("username", "password")}),
        (
            "Personal info",
            {
                "fields": (
                    "first_name",
                    "last_name",
                    "email",
                    "contact_number",
                    "age",
                    "education_qualification",
                    "gender",
                    "profile_picture",
                )
            },
        ),
        (
            "Permissions",
            {
                "fields": (
                    "is_active",
                    "is_staff",
                    "is_superuser",
                    "is_superadmin",
                    "groups",
                    "user_permissions",
                )
            },
        ),
        ("Organization", {"fields": ("organization",)}),
        ("Important dates", {"fields": ("last_login", "date_joined")}),
    )

    def get_groups(self, obj):
        """Return a comma-separated list of groups the user belongs to."""
        if obj.groups.exists():
            return ", ".join([group.name for group in obj.groups.all()])
        return "No groups"

    get_groups.short_description = "Groups"


@admin.register(UserProjectGroup)
class UserProjectGroupAdmin(admin.ModelAdmin):
    """Admin for the UserProjectGroup model."""

    list_display = (
        "user",
        "get_user_first_name",
        "get_user_last_name",
        "get_user_organization",
        "project",
        "group",
    )
    list_filter = ("group", "user__organization", "project")
    search_fields = (
        "user__username",
        "user__first_name",
        "user__last_name",
        "user__organization__name",
        "project__name",
        "group__name",
    )

    def get_user_first_name(self, obj):
        """Return the user's first name."""
        return obj.user.first_name or "-"

    get_user_first_name.short_description = "First Name"

    def get_user_last_name(self, obj):
        """Return the user's last name."""
        return obj.user.last_name or "-"

    get_user_last_name.short_description = "Last Name"

    def get_user_organization(self, obj):
        """Return the user's organization name."""
        return obj.user.organization.name if obj.user.organization else "-"

    get_user_organization.short_description = "Organization"
