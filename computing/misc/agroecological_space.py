import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

from utilities.constants import GEE_EXT_DATASET_PATH
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    check_task_status,
    make_asset_public,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    get_gee_asset_path,
)
from nrm_app.celery import app


@app.task(bind=True)
def generate_agroecological_data(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)

    roi_asset_id = (
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    pan_india_asset_id = f"{GEE_EXT_DATASET_PATH}/Agroecological_space_pan_india"

    description = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_agroecological"
    asset_id = get_gee_asset_path(state, district, block) + description

    roi = ee.FeatureCollection(roi_asset_id)

    pan_india_data = ee.FeatureCollection(pan_india_asset_id)
    clipped_data = pan_india_data.filterBounds(roi.geometry())

    spatial_filter = ee.Filter.intersects(
        leftField=".geo", rightField=".geo", maxError=1
    )

    # Join the clipped data with ROI features to get uid
    join = ee.Join.saveFirst(matchKey="roi_match")
    joined_data = join.apply(clipped_data, roi, spatial_filter)

    # Extract uid from matched ROI feature and add to clipped feature
    def add_uid(feature):
        feature = ee.Feature(feature)
        roi_match = ee.Feature(feature.get("roi_match"))
        uid = roi_match.get("uid")
        return feature.set("uid", uid).set("roi_match", None)

    clipped_data_with_uid = joined_data.map(add_uid)
    task = export_vector_asset_to_gee(clipped_data_with_uid, description, asset_id)

    task_id_list = check_task_status([task])
    print(f"Task completed. Task IDs: {task_id_list}")

    layer_id = None
    layer_at_geoserver = False

    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="agroecological",
        )
        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(
            fc,
            state,
            description,
            "agroecological",
        )

        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True
    return layer_at_geoserver
