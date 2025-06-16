# projects/admin.py
from django.contrib import admin
from .models import Project


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "organization",
        "app_type",
        "enabled",
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
    )
    list_filter = ("organization", "app_type", "enabled", "created_at")
    search_fields = ("name", "description", "organization__name")

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "name",
                    "organization",
                    "description",
                    "geojson_path",
                    "state",
                    'district',
                    'block',
                    "app_type",
                    "enabled",
                    "created_by",
                    "updated_by",
                )
            },
        ),
        (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )
    readonly_fields = ("created_at", "updated_at")
