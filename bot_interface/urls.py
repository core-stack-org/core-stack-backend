from django.urls import path
from . import api

urlpatterns = [
    path("webhook", api.whatsapp_webhook, name="whatsapp_webhook"),
]
