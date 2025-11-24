from django.contrib import admin
from .models import *

# Register your models here.


@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    readonly_fields = ("created_at", "updated_at")
    search_fields = ("layer_name",)
    list_display = ["misc", "state", "dataset", "layer_name", "layer_version"]
    list_filter = [
        "is_stac_specs_generated",
        "is_sync_to_geoserver",
        "layer_version",
        "dataset",
    ]


@admin.register(Dataset)
class LayerAdmin(admin.ModelAdmin):
    search_fields = ("name",)
    list_display = ["layer_type", "name", "workspace"]
    list_filter = ["layer_type"]
