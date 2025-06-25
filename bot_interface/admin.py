from django.contrib import admin
import bot_interface.models

# Register your models here.

admin.site.register(bot_interface.models.Bot)
admin.site.register(bot_interface.models.SMJ)