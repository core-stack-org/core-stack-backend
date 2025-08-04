from django.urls import path

from . import api

urlpatterns = [
    path("get_admin_details_by_latlon/", api.get_admin_details_by_lat_lon, name="get_admin_details_by_lat_lon"),
    path("get_mwsid_by_latlon/", api.get_mws_by_lat_lon, name="get-mws-id-by-lat-lon"),
    path("get_tehsil_data/", api.generate_tehsil_data, name="get-tehsil-data"),
    path("get_mws_data/", api.get_mws_json_by_stats_excel, name="get-mws-data-by-excel"),
    path("get_mws_kyl_indicators/", api.get_mws_json_by_kyl_indicator, name="get-mws-kyl-indicator"),
    path("get_generated_layer_urls/", api.get_generated_layer_urls, name="get-generated-layer-urls"),
]