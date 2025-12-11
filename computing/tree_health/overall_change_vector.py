import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from nrm_app.celery import app


@app.task(bind=True)
def tree_health_overall_change_vector(
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
    ee_initialize(gee_account_id)
    print("Inside process tree_health_overall_change_vector")
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{asset_suffix}_uid"
        )

    description = f"overall_change_vector_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"

    asset_id, task_id = overall_change_vector(
        roi, asset_folder_list, asset_suffix, app_type
    )
    task_id_list = check_task_status([task_id])
    print("task_id_list ", task_id_list)

    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            description,
            asset_id,
            "Tree Overall Change Vector",
        )
        layer_at_geoserver = False
        merged_fc = ee.FeatureCollection(asset_id)
        sync_res = sync_fc_to_geoserver(
            merged_fc, state, description, "tree_overall_ch"
        )
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True

        return layer_at_geoserver
    return None


def overall_change_vector(roi, asset_folder_list, asset_suffix, app_type):

    args = [
        {"value": -2, "label": "Deforestation"},
        {"value": -1, "label": "Degradation"},
        {"value": 0, "label": "No_Change"},
        {"value": 1, "label": "Improvement"},
        {"value": 2, "label": "Afforestation"},
        {"value": [3, 4], "label": "Partially_Degraded"},
        {"value": 5, "label": "Missing Data"},
    ]

    raster = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"overall_change_raster_{asset_suffix}"
    )

    fc = roi
    for arg in args:
        raster = raster.select(["constant"])
        if isinstance(arg["value"], list) and len(arg["value"]) > 1:
            ored_str = "raster.eq(ee.Number(" + str(arg["value"][0]) + "))"
            for i in range(1, len(arg["value"])):
                ored_str = (
                    ored_str + ".Or(raster.eq(ee.Number(" + str(arg["value"][i]) + ")))"
                )
            print(ored_str)
            mask = eval(ored_str)
        else:
            mask = raster.eq(ee.Number(arg["value"]))

        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc, reducer=ee.Reducer.sum(), scale=25, crs=raster.projection()
        )

        def remove_property(feat, prop):
            properties = feat.propertyNames()
            select_properties = properties.filter(ee.Filter.neq("item", prop))
            return feat.select(select_properties)

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            feature = feature.set(arg["label"], value)
            feature = remove_property(feature, "sum")
            return feature

        fc = fc.map(process_feature)

    fc = ee.FeatureCollection(fc)

    description = f"overall_change_vector_{asset_suffix}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    task_id = export_vector_asset_to_gee(fc, description, asset_id)
    return asset_id, task_id
