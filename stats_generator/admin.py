from django.contrib import admin
from .models import LayerInfo


@admin.register(LayerInfo)
class LayerInfoAdmin(admin.ModelAdmin):
    # Fields to display in the list view
    list_display = (
        "layer_name",
        "layer_type",
        "workspace",
        "excel_to_be_generated",
        "start_year",
        "end_year",
        "created_at",
        "updated_at",
    )

    # Fields to filter by in the admin panel
    list_filter = ("layer_type", "excel_to_be_generated", "start_year", "end_year", "created_at")

    # Fields that can be searched in the admin panel
    search_fields = ("layer_name", "layer_desc", "workspace")

    # Fieldsets to organize the form view
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "layer_name",
                    "layer_type",
                    "workspace",
                    "layer_desc",
                    "excel_to_be_generated",
                    "start_year",
                    "end_year",
                    "style_name",
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

    # Fields to be read-only
    readonly_fields = ("created_at", "updated_at")
