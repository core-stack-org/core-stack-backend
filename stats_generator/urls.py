from django.urls import path

from . import api

urlpatterns = [
    path("download_excel_layer/", api.generate_excel_file_layer, name="generate_excel_file_layer"),
    path("download_kyl_data/", api.generate_kyl_data_excel, name="generate_kyl_data_excel"),
    path("download_kyl_village_data/", api.generate_kyl_village_data, name="generate_kyl_village_data"),
]