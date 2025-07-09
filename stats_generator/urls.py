from django.urls import path

from . import api

urlpatterns = [
    path("download_excel_layer/", api.generate_excel_file_layer, name="generate_excel_file_layer"),
    path("download_kyl_data/", api.generate_kyl_data_excel, name="generate_kyl_data_excel"),
    path("download_kyl_village_data/", api.generate_kyl_village_data, name="generate_kyl_village_data"),
    path("get_admin_details_by_lat_lon/", api.get_admin_details_by_lat_lon, name="get_admin_details_by_lat_lon"),
    path("get_mws_id_by_lat_lon/", api.get_mws_by_lat_lon, name="get-mws-id-by-lat-lon"),
    path("get_tehsil_data/", api.generate_tehsil_data, name="get-tehsil-data"),
    path("get_mws_data/", api.get_mws_json_by_stats_excel, name="get-mws-data-by-excel"),
    path("get_mws_kyl_indicator/", api.get_mws_json_by_kyl_indicator, name="get-mws-kyl-indicator"),
    path("get_generated_layer_urls/", api.get_generated_layer_urls, name="get-generated-layer-urls"),
]