import ee
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    export_vector_asset_to_gee,
)
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
)
from utilities.constants import (
    GEE_DATASET_PATH
)


@app.task(bind=True)
def generate_restoration_opportunity(self, state, district, block):
    ee_initialize()
    roi = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    description = (
        "restoration_" + valid_gee_text(district) + "_" + valid_gee_text(block)
    )

    raster_asset_id = clip_raster(roi, state, district, block, description)

    args = [
        {"value": 0, "label": "Protection"},
        {"value": 1, "label": "Wide-scale Restoration"},
        {"value": 2, "label": "Mosaic Restoration"},
        {"value": 3, "label": "Excluded Areas"},
    ]

    return generate_vector(
        roi, raster_asset_id, args, state, district, block, description + "_vector"
    )


def clip_raster(roi, state, district, block, description):
    asset_id = get_gee_asset_path(state, district, block) + description + "_raster"
    if is_gee_asset_exists(asset_id):
        return asset_id

    restoration_raster = ee.Image(
        GEE_DATASET_PATH + "/WRI/LandscapeRestorationOpportunities"
    )

    clipped_raster = restoration_raster.clip(roi.geometry())
    task_id = export_raster_asset_to_gee(
        image=clipped_raster,
        description=description + "_raster",
        asset_id=asset_id,
        scale=60,
        region=roi.geometry(),
    )
    check_task_status([task_id])

    image = ee.Image(asset_id)
    task_id = sync_raster_to_gcs(image, 60, description + "_raster")
    check_task_status([task_id])
    sync_raster_gcs_to_geoserver(
        "restoration",
        description + "_raster",
        description + "_raster",
        "restoration_style",
    )

    return asset_id


def generate_vector(roi, raster_asset_id, args, state, district, block, description):
    raster = ee.Image(raster_asset_id)
    fc = roi
    for arg in args:
        raster = raster.select(["b1"])
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
            collection=fc, reducer=ee.Reducer.sum(), scale=10, crs=raster.projection()
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
    task_id = export_vector_asset_to_gee(
        fc,
        description,
        asset_id=get_gee_asset_path(state, district, block) + description,
    )
    check_task_status([task_id])

    fc = ee.FeatureCollection(fc).getInfo()
    fc = {"features": fc["features"], "type": fc["type"]}
    return sync_layer_to_geoserver(state, fc, description, "restoration")
