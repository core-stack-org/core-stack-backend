import ee

from computing.utils import sync_layer_to_geoserver, sync_fc_to_geoserver, sync_project_fc_to_geoserver
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    make_asset_public, get_gee_dir_path,
)
from nrm_app.celery import app


@app.task(bind=True)
def clip_drainage_lines(
    self,
    state=None,
    district=None,
    block=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type = "MWS",
    workspace_name = "drainage",
    proj_id = 'None'
):
    ee_initialize()
    if proj_id:
        proj_obj = Project.objects.get(pk = proj_id)
    pan_india_drainage = ee.FeatureCollection(
        "projects/ee-corestackdev/assets/datasets/drainage-line/pan_india_drainage_lines"
    )
    if state and district and block:
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        description = f"drainage_lines_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        asset_id = get_gee_asset_path(state, district, block) + description
        geoserver_layer_name = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())

    else:
        roi = ee.FeatureCollection(roi_path)
        description = (
                "drainage_lines"
                + asset_suffix
                )
        asset_id = (
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + description
        )
        geoserver_layer_name = f"{app_type}_drainage_line_{proj_obj.organization.name}_{proj_obj.name}_{proj_id}"
    clipped_drainage = pan_india_drainage.filterBounds(roi.geometry())

    try:
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": clipped_drainage,
                "description": description,
                "assetId": asset_id,
            }
        )
        task.start()
        print("Successfully started the drainage task", task.status())

        task_id_list = check_task_status([task.status()["id"]])
        print("task_id_list", task_id_list)

        make_asset_public(asset_id)
    except Exception as e:
        print(f"Error occurred in running drainage task: {e}")

    try:
        # Load feature collection from Earth Engine
        fc = ee.FeatureCollection(asset_id)
        if state and district and block:
            res = sync_fc_to_geoserver(
                fc,
                state,
                geoserver_layer_name,
                workspace,
            )
        else:
            res = sync_project_fc_to_geoserver(fc, proj_obj.name, geoserver_layer_name, workspace_name )
        print("Drainage line synced to geoserver:", res)
    except Exception as e:
        print("Exception in syncing Drainage line to geoserver", e)
