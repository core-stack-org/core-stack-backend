from django.urls import path

from . import api

urlpatterns = [
    path(
        "download_excel_layer/",
        api.download_stats_excel_file,
        name="download_excel_layer",
    ),
    path(
        "generate_stats_excel_file/",
        api.generate_stats_excel_file_data,
        name="generate_stats_excel_file",
    ),
    path(
        "add_new_layer_data_to_excel/",
        api.add_sheets_in_stats_excel,
        name="add_new_layer_data_to_excel",
    ),
    path(
        "download_kyl_data/",
        api.generate_mws_data_for_kyl,
        name="generate_kyl_data_excel",
    ),
    path(
        "download_kyl_village_data/",
        api.generate_village_data_for_kyl,
        name="generate_kyl_village_data",
    ),
]
