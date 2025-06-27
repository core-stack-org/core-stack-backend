from django.contrib import admin
from .models import *
# Register your models here.

admin.site.register(Dataset)

@admin.register(Layer)
class LayerAdmin(admin.ModelAdmin):
    readonly_fields = ('created_at', 'updated_at')

