from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
)
import ee

from .terrain_utils import generate_terrain_classified_raster


@app.task(bind=True)
def terrain_raster(self, state, district, block):
    print("Inside terrain_raster")
    ee_initialize()
    description = (
        "terrain_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if not is_gee_asset_exists(asset_id):
        roi_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        mwsheds_lf_rasters = ee.ImageCollection(
            roi_boundary.map(generate_terrain_classified_raster)
        )
        mwsheds_lf_raster = mwsheds_lf_rasters.mosaic()
        image_export_task = ee.batch.Export.image.toAsset(
            image=mwsheds_lf_raster.clip(roi_boundary.geometry()),
            description=description,
            assetId=asset_id,
            pyramidingPolicy={"predicted_label": "mode"},
            scale=30,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        image_export_task.start()
        print("Successfully started the terrain_raster", image_export_task.status())

        task_id_list = check_task_status([image_export_task.status()["id"]])
        print("terrain_raster task_id_list", task_id_list)

    """ Sync image to google cloud storage and then to geoserver"""
    layer_name = (
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_terrain_raster"
    )
    task_id = sync_raster_to_gcs(ee.Image(asset_id), 30, layer_name)

    task_id_list = check_task_status([task_id])
    print("task_id_list sync to gcs ", task_id_list)

    sync_raster_gcs_to_geoserver("terrain", layer_name, layer_name, "terrain_raster")
