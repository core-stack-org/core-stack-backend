from django.contrib import admin
from .models import GEEAccount


@admin.register(GEEAccount)
class GEEAccountAdmin(admin.ModelAdmin):
    readonly_fields = ("name", "account_email")
    search_fields = ("name", "account_email")
    list_display = ["name", "account_email", "helper_account"]
    list_filter = ["account_email"]
