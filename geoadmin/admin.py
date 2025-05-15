from django.contrib import admin

from .models import State, District, Block, State_SOI, District_SOI, Block_SOI

# Register your models here.
admin.site.register(State)
admin.site.register(District)
admin.site.register(Block)
admin.site.register(State_SOI)
admin.site.register(District_SOI)
admin.site.register(Block_SOI)
