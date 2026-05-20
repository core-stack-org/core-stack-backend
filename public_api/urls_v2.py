from django.urls import path
from . import api

urlpatterns = [
    path(
        "get_admin_details_by_latlon/",
        api.get_admin_details_by_lat_lon,
        name="get_admin_details_by_lat_lon_v2",
    ),
    path(
        "get_mwsid_by_latlon/",
        api.get_mws_by_lat_lon,
        name="get_mwsid_by_latlon_v2",
    ),
    path("get_tehsil_data/", api.generate_tehsil_data, name="get_tehsil_data_v2"),
    path("get_mws_data/", api.get_mws_data_v2, name="get-mws-data-v2"),
    path(
        "get_mws_kyl_indicators/",
        api.get_mws_json_by_kyl_indicator,
        name="get_mws_kyl_indicators_v2",
    ),
    path(
        "get_generated_layer_urls/",
        api.get_generated_layer_urls,
        name="get_generated_layer_urls_v2",
    ),
    path("get_mws_report/", api.get_mws_report_urls, name="get_mws_report_urls_v2"),
    path("get_mws_geometries/", api.get_mws_geometries, name="get_mws_geometries_v2"),
    path(
        "get_village_geometries/",
        api.get_village_geometries_api,
        name="get_village_geometries_v2",
    ),
    path(
        "get_active_locations/",
        api.generate_active_locations,
        name="get_active_locations_v2",
    ),
]
