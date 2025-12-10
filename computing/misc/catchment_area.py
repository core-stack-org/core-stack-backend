import ee
from nrm_app.celery import app
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from constants.pan_india_urls import CATCHMETN_AREA


@app.task(bind=True)
def generate_catchment_area_singleflow(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    proj_id=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder=None,
    app_type="MWS",
):
    ee_initialize(gee_account_id)
    if state and district and block:
        description = (
            "catchment_area_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_raster"
        )
        asset_id = get_gee_asset_path(state, district, block) + description
        roi_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

    else:
        roi_boundary = ee.FeatureCollection(roi_path)
        description = "catchment_area_" + asset_suffix +'_raster'

        asset_id = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

    catchment_area_raster = ee.Image(CATCHMETN_AREA)
    raster = catchment_area_raster.clip(roi_boundary.geometry())

    # Generate raster Layer
    layer_status = catchment_area_raster_generation(
        asset_id=asset_id,
        state=state,
        district=district,
        block=block,
        description=description,
        roi=roi_boundary,
        raster=raster,
        proj_id=proj_id,
    )

    return layer_status


def catchment_area_raster_generation(
    raster,
    roi,
    proj_id=None,
    state=None,
    district=None,
    block=None,
    description=None,
    asset_id=None,
):
    workspacename = "catchment_area_singleflow"
    if proj_id:
        proj_obj = Project.objects.get(pk=proj_id)

    if not is_gee_asset_exists(asset_id):
        task_id = export_raster_asset_to_gee(
            image=raster,
            description=description,
            asset_id=asset_id,
            scale=30,
            region=roi.geometry(),
        )
        catchment_area_task_id_list = check_task_status([task_id])
        print("catchmenta area task_id list", catchment_area_task_id_list)

    layer_id = None
    layer_at_geoserver = False
    if is_gee_asset_exists(asset_id):
        """Sync image to google cloud storage and then to geoserver"""
        image = ee.Image(asset_id)
        task_id = sync_raster_to_gcs(image, 30, description)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=description,
                asset_id=asset_id,
                dataset_name="Catchment Area",
            )
        make_asset_public(asset_id)
        res = sync_raster_gcs_to_geoserver(
            workspacename,
            description,
            description,
            "catchment_area_singleflow",
        )
        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
