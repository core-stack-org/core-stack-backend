# plans/admin.py
import csv

from django.contrib import admin
from django.http import HttpResponse
from django.utils import timezone

from .models import ODKSyncLog, Plan, PlanApp


def export_as_csv(fields, filename_prefix):
    def action(modeladmin, request, queryset):
        response = HttpResponse(content_type="text/csv")
        timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
        response["Content-Disposition"] = f'attachment; filename="{filename_prefix}_{timestamp}.csv"'
        writer = csv.writer(response)
        writer.writerow(fields)
        for obj in queryset.values_list(*fields):
            writer.writerow(obj)
        return response

    action.short_description = "Export selected as CSV"
    action.__name__ = f"export_{filename_prefix}_as_csv"
    return action


@admin.register(ODKSyncLog)
class ODKSyncLogAdmin(admin.ModelAdmin):
    list_display = ("id", "category", "sync_type", "status", "odk_url", "created_at")
    list_filter = ("category", "sync_type", "status", "created_at")
    search_fields = ("sync_type", "odk_url", "error_details")
    readonly_fields = (
        "category",
        "sync_type",
        "xml_content",
        "odk_url",
        "status",
        "odk_response",
        "error_details",
        "created_at",
    )
    ordering = ("-created_at",)
    actions = [
        export_as_csv(
            ("id", "category", "sync_type", "status", "odk_url", "created_at"),
            "odk_sync_logs",
        )
    ]

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(PlanApp)
class PlanAppAdmin(admin.ModelAdmin):
    actions = [
        export_as_csv(
            (
                "id",
                "plan",
                "organization__name",
                "project__name",
                "state_soi__state_name",
                "district_soi__district_name",
                "tehsil_soi__tehsil_name",
                "village_name",
                "gram_panchayat",
                "facilitator_name",
                "created_by__username",
                "created_at",
                "enabled",
                "is_completed",
                "is_dpr_generated",
                "is_dpr_reviewed",
                "is_dpr_approved",
            ),
            "plan_apps",
        )
    ]
    list_display = (
        "id",
        "plan",
        "organization",
        "project",
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
        "state_soi",
        "district_soi",
        "tehsil_soi",
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
        "project",
        "organization",
    )

    fieldsets = (
        (None, {"fields": ("plan", "project", "organization")}),
        (
            "Location Information",
            {
                "fields": (
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