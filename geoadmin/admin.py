from django.contrib import admin

from .models import Block, District, DistrictSOI, State, StateSOI, TehsilSOI

@admin.register(StateSOI)
class StateSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "state_name", "active_status")
    search_fields = ("state_name",)
    list_filter = ("active_status",)

@admin.register(DistrictSOI)
class DistrictSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "district_name", "state", "active_status")
    search_fields = ("district_name",)
    list_filter = ("state", "active_status")
    autocomplete_fields = ("state",)
    
@admin.register(TehsilSOI)
class TehsilSOIAdmin(admin.ModelAdmin):
    list_display = ("id", "tehsil_name", "district", "active_status")
    search_fields = ("tehsil_name",)
    list_filter = ("district__state", "active_status")
    autocomplete_fields = ("district",)



