import json
from datetime import datetime

import ee

from computing.utils import (
    sync_fc_to_geoserver,
    calculate_precipitation_season,
    get_season_key,
    get_agri_year_key,
    update_dashboard_geojson,
    sync_project_fc_to_geoserver,
)
from constants.pan_india_path import CATCHMENT_AREA, STREAM_ORDER_RASTER
from geoadmin.models import (
    District,
    State_Disritct_Block_Properties,
    StateSOI,
    DistrictSOI,
    TehsilSOI,
)
from nrm_app.celery import app
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    check_task_status,
)
from computing.utils import sync_project_fc_to_geoserver, sync_fc_to_geoserver

from waterrejuvenation.utils import (
    calculate_zoi_area,
    wait_for_task_completion,
    delete_asset_on_GEE,
)
from computing.surface_water_bodies.swb import sync_asset_to_db_and_geoserver


def generate_zoi1(
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
):
    print("insdie zoi")
    ee_initialize(gee_account_id)
    description = "swb4_" + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    if roi:
        print("inside roi")
        print(roi)
        roi = ee.FeatureCollection(roi)
    else:
        roi = ee.FeatureCollection(asset_id)
    description_zoi = "zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )
    delete_asset_on_GEE(asset_id_zoi)
    start_date = "2017-07-01"
    end_date = "2025-06-30"
    zoi_fc = roi.map(compute_zoi)
    zoi_fc = ee.FeatureCollection(zoi_fc)
    zoi_rings = zoi_fc.filter(ee.Filter.gt("zoi_wb", 0)).map(create_ring)
    if not is_gee_asset_exists(asset_id_zoi):
        zoi_task = export_vector_asset_to_gee(zoi_rings, description_zoi, asset_id_zoi)
        check_task_status([zoi_task])
        make_asset_public(asset_id_zoi)
    if state and district and block:
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        print(layer_name)
        layer_at_geoserver = sync_asset_to_db_and_geoserver(
            asset_id_zoi,
            layer_name,
            asset_suffix,
            start_date,
            end_date,
            state,
            district,
            block,
        )
    else:
        proj_obj = Project.objects.get(pk=proj_id)
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        print(layer_name)
        sync_project_fc_to_geoserver(zoi_rings, proj_obj.name, layer_name, "zoi_layers")


def compute_zoi(feature):

    area_of_wb = ee.Number(feature.get("area_ored"))  # assumes area field exists

    # logistic_weight
    def logistic_weight(x, x0=0.2, k=50):
        return ee.Number(1).divide(
            ee.Number(1).add((ee.Number(-k).multiply(x.subtract(x0))).exp())
        )

    # y_small_bodies
    def y_small_bodies(area):
        return ee.Number(126.84).multiply(area.add(0.05).log()).add(383.57)

    # y_large_bodies
    def y_large_bodies(area):
        return ee.Number(140).multiply(area.add(0.05).log()).add(500)

    s = logistic_weight(area_of_wb)

    zoi = (
        (ee.Number(1).subtract(s))
        .multiply(y_small_bodies(area_of_wb))
        .add(s.multiply(y_large_bodies(area_of_wb)).round())
    )

    return feature.set("zoi_wb", zoi)


def create_ring(feature):
    geom = feature.geometry()  # can be point or polygon
    zoi = ee.Number(feature.get("zoi_wb"))
    uid = feature.get("UID")

    # Make circle buffer from centroid
    centroid = geom.centroid()
    circle = centroid.buffer(zoi)

    zoi_area = calculate_zoi_area(zoi)

    return ee.Feature(circle).set(
        {
            "zoi": zoi,
            "UID": uid,
            "zoi_area": zoi_area,
        }
    )
