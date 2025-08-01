from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
)
import ee

from .terrain_utils import generate_terrain_classified_raster
from computing.utils import save_layer_info_to_db


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

        task_id = export_raster_asset_to_gee(
            image=mwsheds_lf_raster.clip(roi_boundary.geometry()),
            description=description,
            asset_id=asset_id,
            scale=30,
            region=roi_boundary.geometry(),
        )
        task_id_list = check_task_status([task_id])
        print("terrain_raster task_id_list", task_id_list)

    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            f"{district.title()}_{block.title()}_terrain_raster",
            asset_id,
            "Terrain Raster",
        )
        make_asset_public(asset_id)

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

        save_layer_info_to_db(
            state, district, block, layer_name, asset_id, "Terrain Raster"
        )

        res = sync_raster_gcs_to_geoserver(
            "terrain", layer_name, layer_name, "terrain_raster"
        )
        if res:
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name,
                asset_id,
                "Terrain Raster",
                sync_to_geoserver=True,
            )
