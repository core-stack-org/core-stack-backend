# projects/admin.py
from django.contrib import admin
from .models import Project, ProjectApp

class ProjectAppInline(admin.TabularInline):
    model = ProjectApp
    extra = 1

@admin.register(Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name', 'organization', 'created_at', 'updated_at', "created_by", "updated_by")
    list_filter = ('organization', 'created_at')
    search_fields = ('name', 'description', 'organization__name')
    inlines = [ProjectAppInline]
    
    fieldsets = (
        (None, {
            'fields': ('name', 'organization', 'description', 'geojson_path', "created_by", "updated_by")
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',),
        }),
    )
    readonly_fields = ('created_at', 'updated_at')

@admin.register(ProjectApp)
class ProjectAppAdmin(admin.ModelAdmin):
    list_display = ('project', 'app_type', 'enabled')
    list_filter = ('app_type', 'enabled', 'project__organization')
    search_fields = ('project__name',)