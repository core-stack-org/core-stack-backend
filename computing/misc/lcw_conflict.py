import ee
from computing.utils import (
    geoserver_sync_succeeded,
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

from utilities.constants import LCW_PAN_INDIA_DATASET
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    wait_for_gee_task,
    make_asset_public,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    get_gee_asset_path,
    create_gee_directory,
)
from utilities.layer_generation_logging import (
    log_task_failure,
    log_task_step,
    task_location_context,
)
from nrm_app.celery import app

TASK_NAME = "generate_lcw_conflict_data"


@app.task(bind=True)
def generate_lcw_conflict_data(self, state, district, block, gee_account_id):
    """
    It will generate the lcw layer for given location at tehsil level
    """
    ctx = task_location_context(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
    )
    log_task_step(TASK_NAME, "start", **ctx)
    try:
        log_task_step(TASK_NAME, "ee_initialize", **ctx)
        ee_initialize(gee_account_id)

        roi_asset_id = (
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        pan_india_asset_id = LCW_PAN_INDIA_DATASET

        description = (
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_lcw_conflict"
        )
        asset_id = get_gee_asset_path(state, district, block) + description
        log_task_step(
            TASK_NAME,
            "asset_ids_resolved",
            roi_asset_id=roi_asset_id,
            pan_india_asset_id=pan_india_asset_id,
            asset_id=asset_id,
            **ctx,
        )

        if not is_gee_asset_exists(roi_asset_id):
            raise RuntimeError(
                f"MWS filtered layer required before LCW generation: "
                f"{roi_asset_id} not found. Run MWS layer generation first."
            )
        if not is_gee_asset_exists(pan_india_asset_id):
            raise RuntimeError(
                f"LCW pan-India dataset not found: {pan_india_asset_id}"
            )

        roi = ee.FeatureCollection(roi_asset_id)
        pan_india_data = ee.FeatureCollection(pan_india_asset_id)
        clipped_data = pan_india_data.filterBounds(roi.geometry())

        spatial_filter = ee.Filter.intersects(
            leftField=".geo", rightField=".geo", maxError=1
        )

        join = ee.Join.saveFirst(matchKey="roi_match")
        joined_data = join.apply(clipped_data, roi, spatial_filter)

        def add_uid(feature):
            feature = ee.Feature(feature)
            roi_match = ee.Feature(feature.get("roi_match"))
            uid = roi_match.get("uid")
            return feature.set("uid", uid).set("roi_match", None)

        clipped_data_with_uid = joined_data.map(add_uid)
        feature_count = clipped_data_with_uid.size().getInfo()
        log_task_step(TASK_NAME, "clip_complete", feature_count=feature_count, **ctx)
        if feature_count == 0:
            log_task_step(
                TASK_NAME,
                "no_lcw_conflict_features",
                message="No LCW conflict areas in this block; skipping export",
                **ctx,
            )
            return True

        log_task_step(TASK_NAME, "create_gee_directory", **ctx)
        create_gee_directory(state, district, block)

        if not is_gee_asset_exists(asset_id):
            log_task_step(TASK_NAME, "export_to_gee_start", **ctx)
            task_id = export_vector_asset_to_gee(
                clipped_data_with_uid, description, asset_id
            )
            if not task_id:
                raise RuntimeError(
                    f"Failed to start GEE export for LCW layer at {asset_id}"
                )
            log_task_step(TASK_NAME, "wait_for_gee_export", task_id=task_id, **ctx)
            wait_for_gee_task(task_id)
        else:
            log_task_step(TASK_NAME, "gee_asset_exists", asset_id=asset_id, **ctx)

        if not is_gee_asset_exists(asset_id):
            raise RuntimeError(
                f"LCW GEE asset was not created at {asset_id}. "
                "Check Earth Engine export logs for this block."
            )

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="LCW Conflict",
        )
        make_asset_public(asset_id)
        log_task_step(TASK_NAME, "layer_saved_to_db", layer_id=layer_id, **ctx)

        fc = ee.FeatureCollection(asset_id)
        log_task_step(TASK_NAME, "sync_fc_to_geoserver", workspace="lcw", **ctx)
        res = sync_fc_to_geoserver(
            fc,
            state,
            description,
            "lcw",
        )
        log_task_step(
            TASK_NAME,
            "geoserver_response",
            status_code=res.get("status_code") if isinstance(res, dict) else res,
            response=res,
            **ctx,
        )

        layer_at_geoserver = False
        if geoserver_sync_succeeded(res) and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

        if not layer_at_geoserver:
            raise RuntimeError(
                f"LCW layer GeoServer sync failed for workspace=lcw "
                f"layer_name={description}: {res!r}"
            )

        log_task_step(
            TASK_NAME, "complete", layer_at_geoserver=layer_at_geoserver, **ctx
        )
        return layer_at_geoserver
    except Exception as exc:
        log_task_failure(TASK_NAME, exc, **ctx)
        raise
