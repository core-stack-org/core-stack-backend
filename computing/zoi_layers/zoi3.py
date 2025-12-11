from computing.surface_water_bodies.swb import sync_asset_to_db_and_geoserver
from computing.utils import sync_project_fc_to_geoserver, sync_fc_to_geoserver
from nrm_app.celery import app
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import ee_initialize, valid_gee_text, get_gee_dir_path
from waterrejuvenation.utils import wait_for_task_completion
import ee


def get_ndvi_for_zoi(
    state=None,
    district=None,
    block=None,
    zoi_roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year="2017-23",
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
):
    print("started generating ndvi")
    ee_initialize(gee_account_id)
    from waterrejuvenation.utils import get_ndvi_data

    if not proj_id:
        description_zoi = (
            "cropping_intensity_zoi_" + asset_suffix 
        )
        asset_id_zoi = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description_zoi
        )
    else:

        description_zoi = (
            "cropping_intensity_zoi_" + asset_suffix 
        )
        asset_id_zoi = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description_zoi
        )

    description_ndvi = "ndvi_" + asset_suffix
    ndvi_asset_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_ndvi
    )

    zoi_collections = ee.FeatureCollection(asset_id_zoi)
    fc = get_ndvi_data(zoi_collections, 2017, 2024, description_ndvi, ndvi_asset_path)
    task = ee.batch.Export.table.toAsset(
        collection=fc, description=description_ndvi, assetId=ndvi_asset_path
    )
    task.start()
    wait_for_task_completion(task)
    start_date = "30-06-2017"
    end_date = "01-07-2024"
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
