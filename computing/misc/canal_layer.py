import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS, CANAL_PAN_INDIA_ASSET
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from nrm_app.celery import app


# Celery task to generate overall tree change vector
@app.task(bind=True)
def canal_vector(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    # Initialize Earth Engine
    ee_initialize(gee_account_id)

    print(f"Inside process canal_vector for {state} - {district} - {block}")

    # Prepare ROI and asset path
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        # Load ROI from GEE
        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{asset_suffix}_uid"
        )

    # Create asset name
    description = f"{asset_suffix}_canal_vector"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Create asset if not exists
    if not is_gee_asset_exists(asset_id):
        roi = ee.FeatureCollection(roi)

        pan_india_data = ee.FeatureCollection(CANAL_PAN_INDIA_ASSET)
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

    # Publish and sync if asset exists
    if is_gee_asset_exists(asset_id):

        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Canal Vector",
        )

        layer_at_geoserver = False

        merged_fc = ee.FeatureCollection(asset_id)

        # Sync to GeoServer
        sync_res = sync_fc_to_geoserver(merged_fc, state, description, "canal")

        # Update sync status
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

        return layer_at_geoserver

    return None
