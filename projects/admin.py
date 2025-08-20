# projects/admin.py
from django.contrib import admin

from .models import Project


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "organization",
        "district",
        "app_type",
        "enabled",
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
    )
    list_filter = ("organization", "app_type", "enabled", "state", "created_at")
    search_fields = ("name", "description", "organization__name")

    fieldsets = (
        (
            "Basic Information",
            {
                "fields": (
                    "name",
                    "organization",
                    "description",
                    "app_type",
                    "enabled",
                )
            },
        ),
        (
            "Geographical Information",
            {
                "fields": (
                    "state",
                    "district",
                    "block",
                    "state_soi",
                    "district_soi",
                    "tehsil_soi",
                    "geojson_path",
                )
            },
        ),
        (
            "User Information",
            {
                "fields": (
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
