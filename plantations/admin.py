# plantations/admin.py
from django.contrib import admin
from .models import KMLFile

@admin.register(KMLFile)
class KMLFileAdmin(admin.ModelAdmin):
    list_display = ('name', 'project_app', 'uploaded_by', 'created_at')
    list_filter = ('project_app__project', 'created_at')
    search_fields = ('name', 'project_app__project__name')
    readonly_fields = ('kml_hash', 'created_at')
    
    fieldsets = (
        (None, {
            'fields': ('name', 'project_app', 'file', 'uploaded_by')
        }),
        ('Technical Details', {
            'fields': ('kml_hash', 'created_at'),
            'classes': ('collapse',),
        }),
    )