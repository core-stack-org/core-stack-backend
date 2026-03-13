from django.contrib import admin

from .models import Block, District, DistrictSOI, State, StateSOI, TehsilSOI


@admin.register(State)
class StateAdmin(admin.ModelAdmin):
    list_display = ("state_census_code", "state_name", "active_status")
    search_fields = ("state_name", "state_census_code")
    list_filter = ("active_status",)


@admin.register(StateSOI)
class StateSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "state_name", "active_status")
    search_fields = ("state_name",)
    list_filter = ("active_status",)


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "district_census_code",
        "district_name",
        "state",
        "active_status",
    )
    search_fields = ("district_name", "district_census_code")
    list_filter = ("state", "active_status")
    autocomplete_fields = ("state",)


@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "block_census_code",
        "block_name",
        "district",
        "active_status",
    )
    search_fields = ("block_name", "block_census_code")
    list_filter = ("district__state", "active_status")
    autocomplete_fields = ("district",)


@admin.register(TehsilSOI)
class TehsilSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "tehsil_name", "district", "active_status")
    search_fields = ("tehsil_name",)
    list_filter = ("district__state", "active_status")
    autocomplete_fields = ("district",)


@admin.register(DistrictSOI)
class DistrictSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "district_name", "state", "active_status")
    search_fields = ("district_name",)
    list_filter = ("state", "active_status")
    autocomplete_fields = ("state",)
