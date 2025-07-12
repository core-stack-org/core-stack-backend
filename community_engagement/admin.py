from django.contrib import admin
from .models import Location, Community, Community_user_mapping, Item_category


admin.site.register(Item_category)


class LocationInline(admin.TabularInline):
    model = Community.locations.through
    extra = 1
    autocomplete_fields = ("location",)


@admin.register(Location)
class LocationAdmin(admin.ModelAdmin):
    """Make it easy to see — and filter by — the scope of each Location row."""

    list_display  = ("id", "level", "state", "district", "block")
    list_filter   = ("level", "state")            # quick dropdowns on right sidebar
    search_fields = (
        "state__name",
        "district__name",
        "block__name",
    )


@admin.register(Community)
class CommunityAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "project",
        "bot",
        "locations_display",            # ← new column
    )
    inlines        = [LocationInline]
    exclude        = ("locations",)
    list_filter    = ("project",)
    search_fields  = ("project__name",)

    @admin.display(description="Locations")
    def locations_display(self, obj):
        return ", ".join(str(loc) for loc in obj.locations.all())

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.prefetch_related(
            "locations__state",
            "locations__district",
            "locations__block",
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