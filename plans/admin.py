# plans/admin.py
from django.contrib import admin
from .models import Plan, PlanApp


@admin.register(PlanApp)
class PlanAppAdmin(admin.ModelAdmin):
    list_display = (
        "plan",
        "organization",
        "state",
        "district",
        "village_name",
        "created_by",
        "created_at",
    )
    list_filter = (
        "organization",
        "state",
        "district",
        "project_app__project",
        "created_at",
    )
    search_fields = (
        "plan",
        "state",
        "district",
        "block",
        "village_name",
        "gram_panchayat",
        "created_by__username",
    )
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        (None, {"fields": ("plan", "project_app", "organization")}),
        (
            "Location Information",
            {
                "fields": (
                    "state",
                    "district",
                    "block",
                    "village_name",
                    "gram_panchayat",
                )
            },
        ),
        (
            "Metadata",
            {
                "fields": ("created_by", "created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )


admin.site.register(Plan)
