from django.contrib import admin

from .models import SyncMetadata


@admin.register(SyncMetadata)
class SyncMetadataAdmin(admin.ModelAdmin):
    list_display = ["sync_type", "last_synced_at", "baseline_date"]
    list_filter = ["sync_type"]
    search_fields = ["sync_type"]
    readonly_fields = ["sync_type", "last_synced_at", "baseline_date"]

    fieldsets = (
        (
            None,
            {
                "fields": ("sync_type", "last_synced_at", "baseline_date"),
            },
        ),
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
