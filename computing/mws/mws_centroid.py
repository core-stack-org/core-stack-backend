import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

from utilities.constants import GEE_DATASET_PATH
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
def generate_mws_centroid_data(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)

    roi_asset_id = (
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    description = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_mws_centroid"
    asset_id = get_gee_asset_path(state, district, block) + description

    if not is_gee_asset_exists(asset_id):
        roi = ee.FeatureCollection(roi_asset_id)

        # Function to convert polygon to point at centroid, preserving properties
        def polygon_to_centroid(feature):
            # Get the centroid of the polygon
            centroid = feature.geometry().centroid()
            coords = centroid.coordinates()
            lon = coords.get(0)
            lat = coords.get(1)

            properties = feature.toDictionary()
            properties = properties.set("centroid_lat", lat)
            properties = properties.set("centroid_lon", lon)

            return ee.Feature(centroid, properties)

        centroids = roi.map(polygon_to_centroid)
        task = export_vector_asset_to_gee(centroids, description, asset_id)

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
            dataset_name="Mws Centroid",
        )
        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id)
        res = sync_fc_to_geoserver(
            fc,
            state,
            description,
            "mws_centroid",
        )

        if res and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
            layer_at_geoserver = True

    return layer_at_geoserver
