import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
)
from nrm_app.celery import app


@app.task(bind=True)
def tree_health_overall_change_vector(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)
    print("Inside process tree_health_overall_change_vector")
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    description = (
        "tree_health_overall_change_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
    )
    task_list = [
        generate_vector(roi, state, district, block),
    ]

    task_id_list = check_task_status(task_list)
    print(task_id_list)

    asset_id = get_gee_asset_path(state, district, block) + description
    # layer_id = None
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            f"tree_health_overall_change_vector_{district.lower()}_{block.lower()}",
            asset_id,
            "Tree Overall Change Vector",
        )
    try:
        layer_at_geoserver = False
        merged_fc = ee.FeatureCollection(asset_id)
        sync_res = sync_fc_to_geoserver(
            merged_fc, state, description, "tree_overall_ch"
        )
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True

    except Exception as e:
        print(f"Error syncing combined data to GeoServer: {e}")
        raise
    return layer_at_geoserver


def generate_vector(roi, state, district, block):

    args = [
        {"value": 0, "label": "Deforestation"},
        {"value": 1, "label": "Degradation"},
        {"value": 2, "label": "No_Change"},
        {"value": 3, "label": "Improvement"},
        {"value": 4, "label": "Afforestation"},
        {"value": 5, "label": "Partially_Degraded"},
        {"value": 6, "label": "Missing Data"},
    ]

    raster = ee.Image(
        get_gee_asset_path(state, district, block)
        + "tree_health_overall_change_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )  # Change detection raster layer

    fc = roi
    for arg in args:
        raster = raster.select(["constant"])
        mask = raster.eq(ee.Number(arg["value"]))
        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc, reducer=ee.Reducer.sum(), scale=25, crs=raster.projection()
        )

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            feature = feature.set(arg["label"], value)
            return feature

        fc = fc.map(process_feature)

    fc = ee.FeatureCollection(fc)

    description = (
        "tree_health_overall_change_vector_"
        + valid_gee_text(district)
        + "_"
        + valid_gee_text(block)
    )
    task = export_vector_asset_to_gee(
        fc, description, get_gee_asset_path(state, district, block) + description
    )
    return task
