from django.contrib import admin

from .models import State, District, Block

# Register your models here.
admin.site.register(State)


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    search_fields = ("district_name",)
    

@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    search_fields = ("block_name",)