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


# Celery task to generate overall tree change vector
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
    # Initialize Earth Engine
    ee_initialize(gee_account_id)

    print("Inside process tree_health_overall_change_vector")

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
    description = f"overall_change_vector_{asset_suffix}"

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Create asset if not exists
    if not is_gee_asset_exists(asset_id):
        fc = overall_change_vector(roi, asset_folder_list, asset_suffix, app_type)

        task_id = export_vector_asset_to_gee(fc, description, asset_id)
        check_task_status([task_id])

    # Publish and sync if asset exists
    if is_gee_asset_exists(asset_id):

        make_asset_public(asset_id)

        # Save layer info in DB
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

        # Sync to GeoServer
        sync_res = sync_fc_to_geoserver(
            merged_fc, state, description, "tree_overall_ch"
        )

        # Update sync status
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

        return layer_at_geoserver

    return None


def overall_change_vector(roi, asset_folder_list, asset_suffix, app_type):
    """Create vector showing overall tree change categories."""

    # Change categories
    args = [
        {"value": -2, "label": "Deforestation"},
        {"value": -1, "label": "Degradation"},
        {"value": 0, "label": "No_Change"},
        {"value": 1, "label": "Improvement"},
        {"value": 2, "label": "Afforestation"},
        {"value": [3, 4], "label": "Partially_Degraded"},
        {"value": 5, "label": "Missing Data"},
    ]

    # Load overall change raster
    raster = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"overall_change_raster_{asset_suffix}"
    )

    fc = roi

    # Calculate area for each change class
    for arg in args:

        raster = raster.select(["constant"])

        # Handle single or multiple values
        if isinstance(arg["value"], list) and len(arg["value"]) > 1:
            ored_str = "raster.eq(ee.Number(" + str(arg["value"][0]) + "))"
            for i in range(1, len(arg["value"])):
                ored_str += ".Or(raster.eq(ee.Number(" + str(arg["value"][i]) + ")))"
            mask = eval(ored_str)
        else:
            mask = raster.eq(ee.Number(arg["value"]))

        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        # Sum area per polygon
        fc = forest_area.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.sum(),
            scale=25,
            crs="EPSG:4326",
        )

        # Remove unwanted property
        def remove_property(feat, prop):
            properties = feat.propertyNames()
            select_properties = properties.filter(ee.Filter.neq("item", prop))
            return feat.select(select_properties)

        # Convert m² to hectares and set label
        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            feature = feature.set(arg["label"], value)
            feature = remove_property(feature, "sum")
            return feature

        fc = fc.map(process_feature)

    return ee.FeatureCollection(fc)
