from django.contrib import admin

from .models import *

# Register your models here.


@admin.register(State)
class StateAdmin(admin.ModelAdmin):
    search_fields = ("state_name",)


@admin.register(StateSOI)
class StateSOIAdmin(admin.ModelAdmin):
    search_fields = ("state_name",)


admin.site.register(State)


@admin.register(StateSOI)
class StateSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "state_name", "active_status")
    search_fields = ("state_name",)


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    search_fields = ("district_name",)


@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    search_fields = ("block_name",)


@admin.register(DistrictSOI)
class DistrictSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "district_name", "state", "active_status")
    search_fields = ("district_name",)
    list_filter = ("state__state_name",)


@admin.register(TehsilSOI)
class TehsilSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "tehsil_name", "district", "get_state", "active_status")
    search_fields = ("tehsil_name",)
    list_filter = (
        "district__state__state_name",
        "district__district_name",
        "active_status",
    )

    def get_state(self, obj):
        return obj.district.state.state_name

    get_state.short_description = "State"
