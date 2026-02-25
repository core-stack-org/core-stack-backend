from django.contrib import admin
from .models import *
from .drought.models import DroughtAlert

# Register your models here.


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
    ]


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    list_display = ["name", "layer_type", "workspace"]
    list_filter = ["layer_type"]


@admin.register(DroughtAlert)
class DroughtAlertAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "severity",
        "alert_type",
        "aoi_name",
        "state",
        "district",
        "alert_date",
        "is_active",
    ]
    list_filter = ["severity", "alert_type", "is_active", "state"]
    search_fields = ["aoi_name", "state", "district", "block"]
    readonly_fields = ("created_at", "updated_at")
    ordering = ["-alert_date", "-created_at"]
