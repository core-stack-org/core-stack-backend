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
class LayerAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    list_display = ["name", "layer_type", "workspace"]
    list_filter = ["layer_type"]
