from django.contrib import admin

from .models import Endpoint, StatusCheck


@admin.register(Endpoint)
class EndpointAdmin(admin.ModelAdmin):
    list_display = ("name", "url", "is_active", "created_at")
    list_filter = ("is_active",)
    search_fields = ("name", "url")


@admin.register(StatusCheck)
class StatusCheckAdmin(admin.ModelAdmin):
    list_display = ("endpoint", "is_up", "status_code", "response_time_ms", "checked_at")
    list_filter = ("is_up", "endpoint")
    readonly_fields = ("endpoint", "status_code", "response_time_ms", "is_up", "error", "checked_at")
    ordering = ("-checked_at",)
