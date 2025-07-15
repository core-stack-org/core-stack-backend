from django.contrib import admin
from .models import *
# Register your models here.

@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    readonly_fields = ('created_at', 'updated_at')
    search_fields = ("layer_name",)

@admin.register(Dataset)
class LayerAdmin(admin.ModelAdmin):
    search_fields = ("name",)


