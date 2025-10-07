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
)
import ee

from .terrain_utils import generate_terrain_classified_raster


# @app.task(bind=True)
def terrain_raster(objectid):
    print("Inside terrain_raster")

    ee_initialize(5)
    basin = ee.FeatureCollection(
        "projects/corestack-datasets/assets/datasets/CWC_sub_basin"
    ).filter(ee.Filter.eq("objectid", objectid))

    basin_name = valid_gee_text(basin.first().get("ba_name").getInfo())
    description = str(objectid) + "_" + basin_name + "_terrain_raster"
    print(description)
    asset_id = "projects/ee-shivprakash/assets/famdem_subbasin/" + description

    mwses_uid_fc = ee.FeatureCollection(
        "projects/corestack-datasets/assets/datasets/India_mws_uid_area_gt_500"
    )
    roi_boundary = mwses_uid_fc.filterBounds(basin.geometry())

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
