from django.urls import path

from . import api

urlpatterns = [
    path("generate_dpr/", api.generate_dpr, name="generate_dpr"),
    path("generate_mws_report/", api.generate_mws_report, name="generate_mws_report"),
    path("generate_resource_report/", api.generate_resource_report, name="generate_resource_report"),
    path("download_mws_report/", api.download_mws_report, name="download_mws_report"),
    path("generate_tehsil_report/", api.generate_tehsil_report, name="generate_tehsil_report")
]
