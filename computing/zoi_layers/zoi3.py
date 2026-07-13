from computing.surface_water_bodies.swb import sync_asset_to_db_and_geoserver
from computing.utils import sync_project_fc_to_geoserver, sync_fc_to_geoserver
from nrm_app.celery import app
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
)
from waterrejuvenation.utils import wait_for_task_completion, resolve_zoi_ndvi_input
import ee


def get_ndvi_for_zoi(
    state=None,
    district=None,
    block=None,
    zoi_roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
):
    print("started generating ndvi")
    if not start_date or not end_date or start_year is None or end_year is None:
        raise ValueError(
            "start_date, end_date, start_year, and end_year are required for ZOI NDVI."
        )
    ee_initialize(gee_account_id)
    from waterrejuvenation.utils import get_ndvi_data

    zoi_collections = resolve_zoi_ndvi_input(
        asset_folder_list, app_type, asset_suffix, zoi_roi=zoi_roi
    )

    description_ndvi = asset_suffix
    ndvi_asset_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_ndvi
    )

    fc = get_ndvi_data(
        zoi_collections, start_year, end_year, description_ndvi, ndvi_asset_path
    )
    task = ee.batch.Export.table.toAsset(
        collection=fc, description=description_ndvi, assetId=ndvi_asset_path
    )
    task.start()
    wait_for_task_completion(task)
    if state and district and block:
        layer_name = f"waterbodies_zoi_{asset_suffix}"
        layer_at_geoserver = sync_asset_to_db_and_geoserver(
            ndvi_asset_path,
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
        sync_project_fc_to_geoserver(fc, proj_obj.name, layer_name, "zoi_layers")
