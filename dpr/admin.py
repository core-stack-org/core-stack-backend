from django.contrib import admin

# Register your models here.
from .models import *

admin.site.register(ODK_settlement)
admin.site.register(ODK_well)