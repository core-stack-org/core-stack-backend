from django.contrib import admin
from .models import Organization
from users.models import User


class OrganizationAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "description",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
    )
    list_filter = ("created_at", "updated_at")
    search_fields = ("name", "description", "created_by", "updated_by")
    readonly_fields = ("created_at", "updated_at")

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if obj is None:  # Creating a new organization
            superadmins = User.objects.filter(is_superadmin=True).values_list(
                "username", flat=True
            )
            form.base_fields[
                "created_by"
            ].help_text = f"Superadmins: {', '.join(superadmins)}"
        return form

    def save_model(self, request, obj, form, change):
        if not change:  # Creating a new organization
            obj.created_by = request.user.username
        obj.updated_by = request.user.username
        super().save_model(request, obj, form, change)


admin.site.register(Organization, OrganizationAdmin)
