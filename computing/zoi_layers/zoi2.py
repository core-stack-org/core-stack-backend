from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import valid_gee_text, get_gee_dir_path, make_asset_public

import ee


def generate_zoi_ci(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
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
    roi = ee.FeatureCollection(asset_id_zoi)
    generate_cropping_intensity(
        roi_path=roi,
        zoi_ci_asset=asset_id_ci,
        asset_folder_list=asset_folder_list,
        asset_suffix=asset_suffix,
        app_type=app_type,
        start_year=2017,
        end_year=2023,
        gee_account_id=gee_account_id,
    )
