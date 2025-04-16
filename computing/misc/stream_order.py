import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)


@app.task(bind=True)
def generate_stream_order_vector(self, state, district, block):
    ee_initialize()
    description = (
        "stream_order_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )

    if not is_gee_asset_exists(
        get_gee_asset_path(state, district, block) + description
    ):
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        raster = ee.Image("projects/ee-ankit-mcs/assets/Stream_Order_Raster_India")
        raster = raster.clip(roi.geometry())

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

        task = ee.batch.Export.table.toAsset(
            **{
                "collection": fc,
                "description": description,
                "assetId": get_gee_asset_path(state, district, block) + description,
            }
        )
        task.start()
        check_task_status([task.status()["id"]])

    # Sync to geoserver
    fc = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + description
    ).getInfo()
    fc = {"features": fc["features"], "type": fc["type"]}
    res = sync_layer_to_geoserver(
        state,
        fc,
        "stream_order_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower()),
        "stream_order",
    )
    print(res)


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
