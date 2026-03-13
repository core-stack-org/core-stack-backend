from computing.surface_water_bodies.swb import sync_asset_to_db_and_geoserver
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import valid_gee_text, get_gee_dir_path, make_asset_public
from projects.models import Project
from computing.utils import sync_project_fc_to_geoserver, sync_fc_to_geoserver
import ee

from waterrejuvenation.utils import delete_asset_on_GEE


def generate_zoi_ci(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
    roi=None,
):
    from computing.cropping_intensity.cropping_intensity import (
        generate_cropping_intensity,
    )

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

    description_zoi = "zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )

    description_ci = "zoi_cropping_intensity_" + asset_suffix
    asset_id_ci = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_ci
    )
    delete_asset_on_GEE(asset_id_ci)
    if roi:
        roi = ee.FeatureCollection(roi)
    else:
        roi = ee.FeatureCollection(asset_id_zoi)
    generate_cropping_intensity(
        roi_path=roi,
        zoi_ci_asset=asset_id_ci,
        asset_folder_list=asset_folder_list,
        asset_suffix=asset_suffix,
        app_type=app_type,
        start_year=2017,
        end_year=2024,
        gee_account_id=gee_account_id,
    )
    start_date = "2017-07-01"
    end_date = "2025-06-30"
    description_zoi_ci = f"cropping_intensity_zoi_{asset_suffix}"

    asset_id_zoi_ci = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi_ci
    )
    if state and district and block:
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        layer_at_geoserver = sync_asset_to_db_and_geoserver(
            asset_id_zoi_ci,
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
        fc = ee.FeatureCollection(asset_id_zoi_ci)
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        sync_project_fc_to_geoserver(fc, proj_obj.name, layer_name, "zoi_layers")
