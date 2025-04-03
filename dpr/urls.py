from django.urls import path

from . import api

urlpatterns = [
    path("generate_dpr/", api.generate_dpr, name="generate_dpr"),
    path("generate_mws_report/", api.generate_mws_report, name="generate_mws_report"),
]
