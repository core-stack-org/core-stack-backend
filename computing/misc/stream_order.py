import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
)


@app.task(bind=True)
def generate_stream_order(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)
    description = (
        "stream_order_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )

    roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

    stream_order_raster = ee.Image(
        "projects/corestack-datasets/assets/datasets/Stream_Order_Raster_India"
    )
    raster = stream_order_raster.clip(roi.geometry())

    # Generate Vector Layer
    stream_order_raster_generation(state, district, block, description, roi, raster)
    args = [
            {"value": 1, "label": "1"},
            {"value": 2, "label": "2"},
            {"value": 3, "label": "3"},
            {"value": 4, "label": "4"},
            {"value": 5, "label": "5"},
            {"value": 6, "label": "6"},
            {"value": 7, "label": "7"},
            {"value": 8, "label": "8"},
            {"value": 9, "label": "9"},
            {"value": 10, "label": "10"},
            {"value": 11, "label": "11"},
        ]

    fc = calculate_pixel_area(args, roi, raster)
    # Generate Raster Layer
    stream_order_vector_generation(state, district, block, description, fc)


def stream_order_raster_generation(state, district, block, description, roi, raster):
    raster_asset_id = get_gee_asset_path(state, district, block) + description + "_raster"
    if not is_gee_asset_exists(raster_asset_id):
        try:
            task_id = export_raster_asset_to_gee(
                image=raster,
                description=description + "_raster",
                asset_id=raster_asset_id,
                scale=30,
                region=roi.geometry(),
            )
            stream_order_task_id_list = check_task_status([task_id])
            print("steam order task_id list", stream_order_task_id_list)

            """ Sync image to google cloud storage and then to geoserver"""
            image = ee.Image(raster_asset_id)
            task_id = sync_raster_to_gcs(image, 30, description + "_raster")

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to gcs ", task_id_list)

            save_layer_info_to_db(state, district, block, layer_name=description + "_raster", asset_id=raster_asset_id, dataset_name="Stream Order",)
            make_asset_public(raster_asset_id)

            res = sync_raster_gcs_to_geoserver("stream_order", description + "_raster", description + "_raster", "stream_order")

        except Exception as e:
            print(f"Error occurred in running stream order: {e}")


def stream_order_vector_generation(state, district, block, description, fc):
    vector_asset_id = get_gee_asset_path(state, district, block) + description + "_vector"
    task = export_vector_asset_to_gee(fc, description  + "_vector", vector_asset_id)
    check_task_status([task])
    if is_gee_asset_exists(vector_asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description + "_vector",
            asset_id=vector_asset_id,
            dataset_name="Stream Order",
        )
        make_asset_public(vector_asset_id)

        # Sync to geoserver
        fc = ee.FeatureCollection(vector_asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        res = sync_layer_to_geoserver(state, fc, description + "_vector", "stream_order")
        print(res)
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")


def calculate_pixel_area(class_labels, fc, raster):
    for arg in class_labels:
        raster = raster.select(["b1"])
        mask = raster.eq(ee.Number(arg["value"]))
        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        fc = forest_area.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.sum(),
            scale=10,
            crs=raster.projection(),
        )

        def process_feature(feature):
            value = feature.get("sum")
            value = ee.Number(value).multiply(0.0001)
            feature = feature.set(arg["label"], value)
            return feature

        fc = fc.map(process_feature)
    fc = ee.FeatureCollection(fc)
    return fc
