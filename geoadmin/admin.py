from django.contrib import admin

from .models import State, District, Block, State_SOI, District_SOI, Block_SOI

# Register your models here.
admin.site.register(State)


@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    search_fields = ("district_name",)
    

@admin.register(Block)
class BlockAdmin(admin.ModelAdmin):
    search_fields = ("block_name",) 
admin.site.register(State_SOI)
admin.site.register(District_SOI)
admin.site.register(Block_SOI)
