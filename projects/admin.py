# projects/admin.py
from django.contrib import admin

from .models import Project


@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "get_organization_name",
        "state",
        "district",
        "block",
        "app_type",
        "enabled",
    )
    list_filter = ("organization", "app_type", "enabled", "state", "created_at")
    search_fields = (
        "name",
        "description",
        "organization__name",
        "created_by__first_name",
        "created_by__last_name",
        "updated_by__first_name",
        "updated_by__last_name",
    )
    autocomplete_fields = ("organization", "state_soi", "district_soi", "tehsil_soi")

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

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.select_related(
            "organization",
            "created_by",
            "updated_by",
            "created_by__organization",
            "updated_by__organization",
            "district",
            "state",
        )

    def get_organization_name(self, obj):
        return obj.organization.name if obj.organization else "-"

    get_organization_name.short_description = "Organization"
    get_organization_name.admin_order_field = "organization__name"

    def get_created_by_full_name(self, obj):
        if obj.created_by:
            full_name = (
                f"{obj.created_by.first_name} {obj.created_by.last_name}".strip()
            )
            org_name = (
                obj.created_by.organization.name
                if obj.created_by.organization
                else "No Org"
            )
            return (
                f"{full_name} ({org_name})"
                if full_name
                else f"{obj.created_by.username} ({org_name})"
            )
        return "-"

    get_created_by_full_name.short_description = "Created By"
    get_created_by_full_name.admin_order_field = "created_by__first_name"

    def get_updated_by_full_name(self, obj):
        if obj.updated_by:
            full_name = (
                f"{obj.updated_by.first_name} {obj.updated_by.last_name}".strip()
            )
            org_name = (
                obj.updated_by.organization.name
                if obj.updated_by.organization
                else "No Org"
            )
            return (
                f"{full_name} ({org_name})"
                if full_name
                else f"{obj.updated_by.username} ({org_name})"
            )
        return "-"

    get_updated_by_full_name.short_description = "Updated By"
    get_updated_by_full_name.admin_order_field = "updated_by__first_name"
