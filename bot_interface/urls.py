from django.urls import path
from . import api

urlpatterns = [
    path("verify_webhook", api.whatsapp_webhook, name="whatsapp_webhook"),
]
