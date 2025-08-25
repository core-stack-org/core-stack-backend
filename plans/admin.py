# plans/admin.py
from django.contrib import admin

from .models import Plan, PlanApp


@admin.register(PlanApp)
class PlanAppAdmin(admin.ModelAdmin):
    list_display = (
        "plan",
        "organization",
        "project",
        "state",
        "district",
        "block",
        "village_name",
        "facilitator_name",
        "created_by",
        "created_at",
        "enabled",
        "is_completed",
        "is_dpr_generated",
        "is_dpr_reviewed",
        "is_dpr_approved",
    )
    list_filter = (
        "organization",
        "project",
        "state",
        "district",
        "block",
        "created_by",
        "created_at",
        "enabled",
        "is_completed",
        "is_dpr_generated",
        "is_dpr_reviewed",
        "is_dpr_approved",
    )
    search_fields = (
        "plan",
        "organization",
        "project",
        "state",
        "district",
        "block",
        "village_name",
        "gram_panchayat",
        "facilitator_name",
        "created_by__username",
    )
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        (None, {"fields": ("plan", "project", "organization")}),
        (
            "Location Information",
            {
                "fields": (
                    "state",
                    "district",
                    "block",
                    "village_name",
                    "gram_panchayat",
                    "facilitator_name",
                    "latitude",
                    "longitude",
                )
            },
        ),
        (
            "Status Information",
            {
                "fields": (
                    "enabled",
                    "is_completed",
                    "is_dpr_generated",
                    "is_dpr_reviewed",
                    "is_dpr_approved",
                )
            },
        ),
        (
            "Metadata",
            {
                "fields": ("created_by", "updated_by", "created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )


admin.site.register(Plan)
