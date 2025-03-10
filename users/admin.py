from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import User, UserProjectGroup


@admin.register(User)
class CustomUserAdmin(UserAdmin):
    """Custom admin for the User model."""

    list_display = (
        "username",
        "email",
        "first_name",
        "last_name",
        "organization",
        "is_superadmin",
        "is_staff",
    )
    list_filter = ("is_superadmin", "is_staff", "is_active", "organization")
    search_fields = ("username", "email", "first_name", "last_name")

    fieldsets = (
        (None, {"fields": ("username", "password")}),
        (
            "Personal info",
            {"fields": ("first_name", "last_name", "email", "contact_number")},
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


@admin.register(UserProjectGroup)
class UserProjectGroupAdmin(admin.ModelAdmin):
    """Admin for the UserProjectGroup model."""

    list_display = ("user", "project", "group")
    list_filter = ("group", "project")
    search_fields = ("user__username", "project__name", "group__name")
