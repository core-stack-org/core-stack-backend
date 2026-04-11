"""
Grasslands Health Monitoring (Field Level @30m)

Computes grassland health indicators using NDVI from Sentinel-2/Landsat
at 30m resolution. Generates seasonal/annual composites, classifies
health zones (healthy/moderate/degraded), vectorizes at field level,
and publishes as GEE assets.

Health classification:
    - Healthy:    NDVI > 0.6
    - Moderate:   0.3 <= NDVI <= 0.6
    - Degraded:   NDVI < 0.3

Reference:
    - Global short vegetation height: landcarbonlab.org
    - GPP-based grasslands health: PMC12356185
"""

import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    check_task_status,
    export_raster_asset_to_gee,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from utilities.constants import GEE_PATHS
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

HEALTHY_THRESHOLD = 0.6
MODERATE_THRESHOLD = 0.3

CLASS_HEALTHY = 3
CLASS_MODERATE = 2
CLASS_DEGRADED = 1


def _get_sentinel2_ndvi(start_date, end_date, roi):
    """Compute median NDVI from Sentinel-2 for the given period."""
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
    )

    def mask_clouds(img):
        qa = img.select("QA60")
        mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
        return img.updateMask(mask)

    def compute_ndvi(img):
        return img.normalizedDifference(["B8", "B4"]).rename("ndvi")

    ndvi = s2.map(mask_clouds).map(compute_ndvi).median().clip(roi)
    return ndvi


def _get_landsat_ndvi(start_date, end_date, roi):
    """Fallback: compute median NDVI from Landsat 8/9."""
    l8 = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterDate(start_date, end_date)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUD_COVER", 30))
    )
    l9 = (
        ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
        .filterDate(start_date, end_date)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUD_COVER", 30))
    )

    def compute_ndvi(img):
        return img.normalizedDifference(["SR_B5", "SR_B4"]).rename("ndvi")

    ndvi = l8.merge(l9).map(compute_ndvi).median().clip(roi)
    return ndvi


def _classify_health(ndvi_image):
    """Classify NDVI into grassland health zones."""
    healthy = ndvi_image.gte(HEALTHY_THRESHOLD).multiply(CLASS_HEALTHY)
    moderate = (
        ndvi_image.gte(MODERATE_THRESHOLD)
        .And(ndvi_image.lt(HEALTHY_THRESHOLD))
        .multiply(CLASS_MODERATE)
    )
    degraded = ndvi_image.lt(MODERATE_THRESHOLD).And(ndvi_image.gt(0)).multiply(CLASS_DEGRADED)

    return healthy.add(moderate).add(degraded).rename("health_class").toUint8()


def _vectorize_health(classified, ndvi, roi, scale=30):
    """Convert health raster to field-level polygons."""
    vectors = classified.reduceToVectors(
        geometry=roi,
        scale=scale,
        geometryType="polygon",
        eightConnected=True,
        labelProperty="health_class",
        maxPixels=1e10,
    )

    def add_attributes(feature):
        cls = feature.get("health_class")
        label = (
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_HEALTHY), "healthy",
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_MODERATE), "moderate", "degraded"))
        )
        area = feature.geometry().area().divide(10000)
        return feature.set({"health_label": label, "area_ha": area})

    vectors = vectors.map(add_attributes)

    # add mean NDVI per polygon
    vectors = ndvi.reduceRegions(
        collection=vectors, reducer=ee.Reducer.mean().combine(
            ee.Reducer.minMax(), sharedInputs=True
        ), scale=scale
    )

    return vectors


@app.task(bind=True)
def compute_grassland_health(
    self,
    state=None,
    district=None,
    block=None,
    year=2024,
    season="annual",
    gee_account_id=None,
    roi_path=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Compute grassland health for an AoI/MWS.

    Args:
        season: "annual", "kharif" (Jun-Oct), "rabi" (Nov-Mar), "summer" (Apr-May)
    """
    ee_initialize(gee_account_id)

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        roi_path = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + f"filtered_mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_uid"
        )

    roi = ee.FeatureCollection(roi_path)
    roi_geom = roi.geometry()

    # date range based on season
    date_ranges = {
        "annual": (f"{year}-01-01", f"{year}-12-31"),
        "kharif": (f"{year}-06-01", f"{year}-10-31"),
        "rabi": (f"{year}-11-01", f"{year + 1}-03-31"),
        "summer": (f"{year}-04-01", f"{year}-05-31"),
    }
    start_date, end_date = date_ranges.get(season, date_ranges["annual"])

    raster_name = f"grassland_health_{season}_{year}_{asset_suffix}"
    vector_name = f"grassland_health_vec_{season}_{year}_{asset_suffix}"

    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    # compute NDVI
    print(f"Computing NDVI for {season} {year}...")
    ndvi = _get_sentinel2_ndvi(start_date, end_date, roi_geom)

    # classify
    classified = _classify_health(ndvi)

    # export raster
    if not is_gee_asset_exists(raster_id):
        print(f"Exporting raster: {raster_id}")
        task_id = export_raster_asset_to_gee(
            classified, raster_name, raster_id, scale=30, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # vectorize + export
    if not is_gee_asset_exists(vector_id):
        print("Vectorizing health zones...")
        vectors = _vectorize_health(classified, ndvi, roi_geom)

        task_id = export_vector_asset_to_gee(vectors, vector_name, vector_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_id)

    # save metadata
    layer_name = f"{asset_suffix}_grassland_health_{season}_{year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        dataset_name="Grassland Health",
        metadata={
            "indicator_type": "NDVI",
            "year": year,
            "season": season,
            "resolution": "30m",
            "source": "Sentinel-2 SR Harmonized",
            "classes": {"3": "healthy", "2": "moderate", "1": "degraded"},
            "processing_date": str(ee.Date(ee.Algorithms.Date("now")).getInfo()),
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
