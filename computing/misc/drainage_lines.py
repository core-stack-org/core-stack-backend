import ee

from computing.utils import sync_layer_to_geoserver, sync_fc_to_geoserver
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    make_asset_public,
)
from utilities.constants import (
    GEE_DATASET_PATH
)
from nrm_app.celery import app


@app.task(bind=True)
def clip_drainage_lines(
    self,
    state,
    district,
    block,
):
    ee_initialize()
    pan_india_drainage = ee.FeatureCollection(
        GEE_DATASET_PATH + "/drainage-line/pan_india_drainage_lines"
    )
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    clipped_drainage = pan_india_drainage.filterBounds(roi.geometry())

    description = f"drainage_lines_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    asset_id = get_gee_asset_path(state, district, block) + description
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
        res = sync_fc_to_geoserver(
            fc,
            state,
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()),
            "drainage",
        )
        print("Drainage line synced to geoserver:", res)
    except Exception as e:
        print("Exception in syncing Drainage line to geoserver", e)
