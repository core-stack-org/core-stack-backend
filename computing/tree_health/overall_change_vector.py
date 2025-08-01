import ee
from computing.utils import sync_layer_to_geoserver, save_layer_info_to_db
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
def tree_health_overall_change_vector(self, state, district, block):
    ee_initialize()
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
        overall_vector(roi, state, district, block),
    ]

    print(task_list)
    task_id_list = check_task_status(task_list)
    print(
        "Tree health overall change vector task completed - task_id_list:", task_id_list
    )
    asset_id = get_gee_asset_path(state, district, block) + description
    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            f"tree_health_overall_change_vector_{district.title()}_{block.title()}",
            asset_id,
            "Tree Overall Change Vector",
        )
        make_asset_public(asset_id)
    sync_change_to_geoserver(block, district, state)


def overall_vector(roi, state, district, block):
    args = [
        {"value": 0, "label": "Deforestation"},
        {"value": 1, "label": "Degradation"},
        {"value": 2, "label": "No_Change"},
        {"value": 3, "label": "Improvement"},
        {"value": 4, "label": "Afforestation"},
        {"value": 5, "label": "Partially_Degraded"},
        {"value": 6, "label": "Missing Data"},
    ]  # Classes in afforestation raster layer

    return generate_vector(roi, args, state, district, block)


def generate_vector(roi, args, state, district, block):
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


def sync_change_to_geoserver(block, district, state):
    asset_id = (
        get_gee_asset_path(state, district, block)
        + "tree_health_overall_change_vector_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    fc = ee.FeatureCollection(asset_id).getInfo()
    fc = {"features": fc["features"], "type": fc["type"]}
    res = sync_layer_to_geoserver(
        state,
        fc,
        "tree_health_overall_change_vector_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower()),
        "tree_overall_ch",
    )
    if res["status_code"] == 201:
        save_layer_info_to_db(
            state,
            district,
            block,
            f"tree_health_overall_change_vector_{district.lower()}_{block.lower()}",
            asset_id,
            "Tree Overall Change Vector",
            sync_to_geoserver=True,
        )
