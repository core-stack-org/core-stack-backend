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
        "state_soi",
        "district_soi",
        "tehsil_soi",
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
        "organization__name",
        "project__name",
        "state__state_name",
        "district__district_name",
        "block__block_name",
        "state_soi__state_name",
        "district_soi__district_name",
        "tehsil_soi__tehsil_name",
        "village_name",
        "gram_panchayat",
        "facilitator_name",
        "created_by__username",
    )
    readonly_fields = ("created_by", "created_at", "updated_by", "updated_at")
    autocomplete_fields = (
        "state_soi",
        "district_soi",
        "tehsil_soi",
        "state",
        "district",
        "block",
        "project",
        "organization",
    )

    fieldsets = (
        (None, {"fields": ("plan", "project", "organization")}),
        (
            "Location Information",
            {
                "fields": (
                    "state",
                    "district",
                    "block",
                    "state_soi",
                    "district_soi",
                    "tehsil_soi",
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

    def save_model(self, request, obj, form, change):
        if not change:
            obj.created_by = request.user
        obj.updated_by = request.user
        super().save_model(request, obj, form, change)


@admin.register(Plan)
class PlanAdmin(admin.ModelAdmin):
    list_display = (
        "plan",
        "plan_id",
        "facilitator_name",
        "village_name",
        "gram_panchayat",
        "state",
        "district",
        "block",
    )
    list_filter = ("facilitator_name", "village_name", "state", "district", "block")
    search_fields = ("facilitator_name", "plan", "village_name")
