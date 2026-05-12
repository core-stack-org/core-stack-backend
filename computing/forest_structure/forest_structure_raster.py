import ee
from nrm_app.celery import app
from utilities.constants import GEE_PATHS, FOREST_STRUCTURE_RASTER
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    check_task_status,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from computing.STAC_specs import generate_STAC_layerwise


# Celery task to generate forest structure raster (annual NDVI-based classification)
@app.task(bind=True)
def forest_structure_raster(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year=None,
    end_year=None,
    app_type="MWS",
    gee_account_id=None,
):
    """
    Generate annual forest structure raster at 30m resolution.
    
    Forest structure classes:
    - Dense: NDVI > 0.6 (dark green)
    - Open: 0.3 < NDVI <= 0.6 (light green)
    - Scrub: NDVI <= 0.3 (yellow)
    
    Args:
        state, district, block: Administrative boundaries
        roi: Region of interest (FeatureCollection)
        start_year, end_year: Time period for annual classification
        app_type: 'MWS' or 'AoI'
        gee_account_id: Google Earth Engine account ID
    
    Returns:
        Dictionary with status and asset path
    """
    print("Inside process Forest Structure raster")

    try:
        # Initialize Earth Engine
        ee_initialize(gee_account_id)

        # Prepare ROI and asset folder path
        if state and district and block:
            asset_suffix = (
                valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
            )
            asset_folder_list = [state, district, block]

            # Load ROI FeatureCollection
            roi = ee.FeatureCollection(
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
            )

        # Convert roi to geometry if it's a FeatureCollection
        roi_geom = roi.geometry() if isinstance(roi, ee.FeatureCollection) else roi

        # Ensure start_year and end_year are defined
        start_year = start_year or 2020
        end_year = end_year or ee.Date.now().get("year").getInfo()

        # Generate annual forest structure rasters
        forest_structure_classes = []

        for year in range(start_year, end_year + 1):
            # Filter satellite imagery for the year
            s2_collection = (
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(roi_geom)
                .filterDate(f"{year}-01-01", f"{year}-12-31")
                .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
            )

            # If Sentinel-2 data is not available, use Landsat-8/9
            if s2_collection.size().getInfo() == 0:
                s2_collection = (
                    ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
                    .filterBounds(roi_geom)
                    .filterDate(f"{year}-01-01", f"{year}-12-31")
                    .filter(ee.Filter.lt("CLOUD_COVER", 20))
                )

            # Compute median composite for the year
            composite = s2_collection.median().clip(roi_geom)

            # Compute NDVI
            ndvi = composite.normalizedDifference(["B8", "B4"]).rename("NDVI")

            # Classification thresholds for forest structure
            forest_dense = ndvi.gt(0.6).rename("dense")
            forest_open = ndvi.gt(0.3).And(ndvi.lte(0.6)).rename("open")
            forest_scrub = ndvi.lte(0.3).rename("scrub")

            # Combine into single classification raster
            # 0: no forest/scrub, 1: open, 2: dense
            classification = (
                forest_dense.multiply(2)
                .add(forest_open.multiply(1))
                .rename("forest_structure")
            )

            # Add year as a band for temporal tracking
            annual_class = classification.addBands(
                ee.Image(year).rename("year")
            ).addBands(ndvi)

            forest_structure_classes.append(annual_class)

        # Combine all years into a single ImageCollection
        forest_structure_collection = ee.ImageCollection(forest_structure_classes)

        # Get asset path
        asset_path = get_gee_asset_path(
            asset_folder_list,
            asset_name=f"forest_structure_{asset_suffix}",
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )

        # Check if asset already exists
        if is_gee_asset_exists(asset_path):
            print(f"Asset {asset_path} already exists, skipping...")
            return {
                "status": "Asset already exists",
                "asset_path": asset_path,
            }

        # Export raster to GEE asset
        export_task = export_raster_asset_to_gee(
            forest_structure_collection.first(),
            asset_path,
            roi_geom,
            description=f"Forest Structure {state} {district} {block}",
        )

        # Save layer info to database
        task_info = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=f"forest_structure_{asset_suffix}",
            algorithm="NDVI_threshold_classification",
            dataset_name="Forest Structure Estimation",
            gee_asset_path=asset_path,
        )

        print(f"Forest structure raster export task initiated: {export_task}")
        return {
            "status": "Task initiated",
            "task_id": export_task,
            "asset_path": asset_path,
        }

    except Exception as e:
        print(f"Exception in forest_structure_raster: {e}")
        raise
