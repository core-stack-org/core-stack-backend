from django.contrib import admin

from .models import *

# Register your models here.


@admin.register(State)
class StateAdmin(admin.ModelAdmin):
    search_fields = ("state_name",)


@admin.register(StateSOI)
class StateSOIAdmin(admin.ModelAdmin):
    search_fields = ("state_name",)


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    search_fields = ("district_name",)


@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    search_fields = ("block_name",)


@admin.register(TehsilSOI)
class TehsilSOIAdmin(admin.ModelAdmin):
    search_fields = ("tehsil_name",)


@admin.register(DistrictSOI)
class DistrictSOIAdmin(admin.ModelAdmin):
    search_fields = ("district_name",)
