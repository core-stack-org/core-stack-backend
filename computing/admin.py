from django.contrib import admin
from .models import *


class IsGeneratedLocallyFilter(admin.SimpleListFilter):
    title = "is generated locally"
    parameter_name = "is_generated_locally"

    def lookups(self, request, model_admin):
        return [
            ("true", "Yes"),
            ("false", "No"),
        ]

    def queryset(self, request, queryset):
        if self.value() == "true":
            return queryset.filter(misc__is_generated_locally=True)
        if self.value() == "false":
            return queryset.exclude(misc__is_generated_locally=True)
        return queryset


@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    readonly_fields = ("created_at", "updated_at")
    search_fields = ("layer_name",)
    list_display = ["state", "layer_name", "dataset", "layer_version", "misc"]
    list_filter = [
        "is_stac_specs_generated",
        "is_sync_to_geoserver",
        "layer_version",
        "dataset",
        IsGeneratedLocallyFilter,
    ]


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    list_display = ["name", "layer_type", "workspace"]
    list_filter = ["layer_type"]


@admin.register(LayerMapping)
class LayerMappingAdmin(admin.ModelAdmin):
    readonly_fields = ("created_at", "updated_at")
    search_fields = (
        "layer_name",
        "db_dataset_name",
        "ee_layer_name",
        "geoserver_layer_name",
        "display_name",
    )
    list_display = [
        "layer_name",
        "layer_type",
        "db_dataset_name",
        "ee_layer_name",
        "geoserver_workspace_name",
        "geoserver_layer_name",
        "auto_stac",
    ]
    list_filter = ["layer_type", "auto_stac", "geoserver_workspace_name", "theme"]
    list_editable = ["auto_stac"]
    fieldsets = (
        (
            "Identity",
            {
                "fields": (
                    "display_name",
                    "layer_type",
                    "layer_name",
                    "theme",
                )
            },
        ),
        (
            "Source / GeoServer",
            {
                "fields": (
                    "db_dataset_name",
                    "ee_layer_name",
                    "geoserver_workspace_name",
                    "geoserver_layer_name",
                    "spatial_resolution_in_meters",
                )
            },
        ),
        (
            "STAC trigger",
            {
                "fields": (
                    "auto_stac",
                    "start_year",
                    "end_year",
                    "style_file_url",
                )
            },
        ),
        ("Audit", {"fields": ("created_at", "updated_at")}),
    )
