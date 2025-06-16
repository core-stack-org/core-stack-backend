from django.contrib import admin
from .models import Community, Community_user_mapping, Item_category


admin.site.register(Item_category)

@admin.register(Community)
class CommunityAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "project",
    )
    list_filter = ("project",)
    search_fields = ("project",)

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "project",
                )
            },
        ),
    )


@admin.register(Community_user_mapping)
class CommunityUserMappingAdmin(admin.ModelAdmin):
    list_display = (
        "community",
        "user",
        "is_last_accessed_community",
        "created_at",
        "updated_at"
    )
    list_filter = ("community", "created_at")
    search_fields = ("community__project__name", "user__username")
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "community",
                    "user",
                    "is_last_accessed_community",
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