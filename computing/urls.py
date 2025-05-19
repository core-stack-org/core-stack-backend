from django.urls import path
from . import api

urlpatterns = [
    path("create_workspace/", api.create_workspace, name="create_workspace"),
    path(
        "generate_block_layer/",
        api.generate_admin_boundary,
        name="generate_block_layer",
    ),
    path("delete_layer/", api.delete_layer, name="delete_layer"),
    path("upload_kml/", api.upload_kml, name="upload_kml"),
    path("generate_mws_layer/", api.generate_mws_layer, name="generate_mws_layer"),
    path("lulc_v3/", api.lulc_v3_river_basin, name="lulc_v3"),
    path("lulc_vector/", api.lulc_vector, name="lulc_vector"),
    path("lulc_v4/", api.lulc_v4, name="lulc_v4"),
    path("get_gee_layer/", api.get_gee_layer, name="get_gee_layer"),
    path("generate_ci_layer/", api.generate_ci_layer, name="generate_ci_layer"),
    path("generate_swb/", api.generate_swb, name="generate_swb"),
    path("generate_well_depth/", api.generate_well_depth, name="generate_well_depth"),
    path(
        "generate_drought_layer/",
        api.generate_drought_layer,
        name="generate_drought_layer",
    ),
    path(
        "generate_terrain_descriptor/",
        api.generate_terrain_descriptor,
        name="generate_terrain_descriptor",
    ),
    path(
        "generate_terrain_raster/",
        api.generate_terrain_raster,
        name="generate_terrain_raster",
    ),
    path(
        "terrain_lulc_slope_cluster/",
        api.terrain_lulc_slope_cluster,
        name="terrain_lulc_slope_cluster",
    ),
    path(
        "terrain_lulc_plain_cluster/",
        api.terrain_lulc_plain_cluster,
        name="terrain_lulc_plain_cluster",
    ),
    path("generate_clart/", api.generate_clart, name="generate_clart"),
    path("change_detection/", api.change_detection, name="change_detection"),
    path(
        "change_detection_vector/",
        api.change_detection_vector,
        name="change_detection_vector",
    ),
    path("crop_grid/", api.crop_grid, name="crop_grid"),
    path("tree_health_raster/", api.tree_health_raster, name="tree_health_raster"),
    path("tree_health_vector/", api.tree_health_vector, name="tree_health_vector"),
    path("stream_order_vector/", api.stream_order_vector, name="stream_order_vector"),
    path(
        "mws_drought_causality/",
        api.mws_drought_causality,
        name="mws_drought_causality",
    ),
    path("gee_task_status/", api.gee_task_status, name="gee_task_status"),
    path(
        "generate_nrega_layer/", api.generate_nrega_layer, name="generate_nrega_layer"
    ),
    path(
        "generate_drainage_layer/",
        api.generate_drainage_layer,
        name="generate_drainage_layer",
    ),
    path(
        "generate_drainage_density/",
        api.generate_drainage_density,
        name="generate_drainage_density",
    ),
    path("generate_lithology/", api.generate_lithology, name="generate_lithology"),
    path(
        "plantation_site_suitability/",
        api.plantation_site_suitability,
        name="plantation_site_suitability",
    ),
    path(
        "restoration_opportunity/",
        api.restoration_opportunity,
        name="restoration_opportunity",
    ),
    path("aquifer_vector/", api.aquifer_vector, name="aquifer_vector"),
    path("soge_vector/", api.soge_vector, name="soge_vector"),
    path("fes_clart_layer/", api.fes_clart_upload_layer, name="fes_clart_layer"),
]
