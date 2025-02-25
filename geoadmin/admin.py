from django.contrib import admin

from .models import State, District, Block

# Register your models here.
admin.site.register(State)
admin.site.register(District)
admin.site.register(Block)
