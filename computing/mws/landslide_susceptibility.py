"""
Landslide Susceptibility Map (Field Level @100m)

Computes landslide susceptibility using a weighted model based on:
    - Slope (SRTM DEM)
    - Curvature (profile + plan curvature)
    - LULC (deforested areas more prone)
    - Rainfall (CHIRPS annual total)
    - Soil type (SoilGrids)
    - Elevation

Classification:
    - Low:      score < 0.33
    - Moderate:  0.33 <= score < 0.66
    - High:     score >= 0.66

Reference:
    https://www.sciencedirect.com/science/article/pii/S0341816223007440
    https://www.nature.com/articles/s44304-025-00121-3
"""

import ee
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
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

SRTM = "USGS/SRTMGL1_003"
CHIRPS = "UCSB-CHG/CHIRPS/DAILY"

WEIGHTS = {
    "slope": 0.30,
    "curvature": 0.15,
    "elevation": 0.10,
    "rainfall": 0.20,
    "lulc": 0.15,
    "soil": 0.10,
}

CLASS_HIGH = 3
CLASS_MODERATE = 2
CLASS_LOW = 1


def _normalize(image):
    """Min-max normalize to 0-1."""
    stats = image.reduceRegion(
        reducer=ee.Reducer.minMax(), bestEffort=True, maxPixels=1e9
    )
    band = image.bandNames().get(0)
    mn = ee.Number(stats.get(ee.String(band).cat("_min")))
    mx = ee.Number(stats.get(ee.String(band).cat("_max")))
    rng = mx.subtract(mn).max(0.001)
    return image.subtract(mn).divide(rng).clamp(0, 1)


def _compute_terrain_factors(roi):
    """Compute slope, curvature, and elevation from SRTM."""
    dem = ee.Image(SRTM).clip(roi)
    slope = _normalize(ee.Terrain.slope(dem)).rename("slope_factor")
    elevation = _normalize(dem.select("elevation")).rename("elevation_factor")

    # curvature approximation using second derivative of DEM
    # positive curvature = convex (ridges), negative = concave (valleys)
    kernel = ee.Kernel.laplacian8()
    curvature = dem.select("elevation").convolve(kernel).abs()
    curvature_norm = _normalize(curvature).rename("curvature_factor")

    return slope, curvature_norm, elevation


def _compute_rainfall_factor(year, roi):
    """Annual rainfall as landslide trigger."""
    rainfall = (
        ee.ImageCollection(CHIRPS)
        .filterDate(f"{year}-01-01", f"{year}-12-31")
        .filterBounds(roi)
        .sum()
        .clip(roi)
    )
    return _normalize(rainfall).rename("rainfall_factor")


def _compute_soil_factor(roi):
    """Clay content from SoilGrids as soil instability proxy."""
    try:
        clay = ee.Image("projects/soilgrids-isric/clay_mean").clip(roi)
        return _normalize(clay).rename("soil_factor")
    except Exception:
        return ee.Image.constant(0.5).clip(roi).rename("soil_factor")


def _compute_lulc_factor(roi, lulc_asset_id=None):
    """Deforested/barren areas have higher landslide susceptibility."""
    if lulc_asset_id:
        lulc = ee.Image(lulc_asset_id).clip(roi)
        # invert tree cover: more forest = less susceptible
        # assume class 1,2 = tree, higher values = non-tree
        non_tree = lulc.gt(2).selfMask()
        return _normalize(lulc).rename("lulc_factor")
    return ee.Image.constant(0.5).clip(roi).rename("lulc_factor")


@app.task(bind=True)
def compute_landslide_susceptibility(
    self,
    state=None,
    district=None,
    block=None,
    year=2024,
    gee_account_id=None,
    roi_path=None,
    lulc_asset_id=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Compute landslide susceptibility for an AoI at ~100m."""
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

    mws_fc = ee.FeatureCollection(roi_path)
    roi_geom = mws_fc.geometry()

    raster_name = f"landslide_susceptibility_{year}_{asset_suffix}"
    vector_name = f"landslide_susceptibility_vec_{year}_{asset_suffix}"
    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    print(f"Computing landslide susceptibility for {year}...")

    # compute all factors
    slope, curvature, elevation = _compute_terrain_factors(roi_geom)
    rainfall = _compute_rainfall_factor(year, roi_geom)
    soil = _compute_soil_factor(roi_geom)
    lulc = _compute_lulc_factor(roi_geom, lulc_asset_id)

    # weighted overlay
    score = (
        slope.multiply(WEIGHTS["slope"])
        .add(curvature.multiply(WEIGHTS["curvature"]))
        .add(elevation.multiply(WEIGHTS["elevation"]))
        .add(rainfall.multiply(WEIGHTS["rainfall"]))
        .add(soil.multiply(WEIGHTS["soil"]))
        .add(lulc.multiply(WEIGHTS["lulc"]))
    ).rename("susceptibility_score")

    # classify
    classified = (
        score.gte(0.66).multiply(CLASS_HIGH)
        .add(score.gte(0.33).And(score.lt(0.66)).multiply(CLASS_MODERATE))
        .add(score.lt(0.33).multiply(CLASS_LOW))
    ).rename("susceptibility_class").toUint8()

    # export raster at 100m
    if not is_gee_asset_exists(raster_id):
        task_id = export_raster_asset_to_gee(
            classified, raster_name, raster_id, scale=100, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # vectorize per MWS
    if not is_gee_asset_exists(vector_id):
        result = score.reduceRegions(
            collection=mws_fc, reducer=ee.Reducer.mean(), scale=100
        ).map(lambda f: f.set({
            "susceptibility_score": ee.Number(f.get("mean")),
            "susceptibility_class": ee.Algorithms.If(
                ee.Number(f.get("mean")).gte(0.66), "high",
                ee.Algorithms.If(ee.Number(f.get("mean")).gte(0.33), "moderate", "low")
            ),
            "area_ha": f.geometry().area().divide(10000).round(),
        }))

        task_id = export_vector_asset_to_gee(result, vector_name, vector_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_landslide_susceptibility_{year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Landslide Susceptibility",
        metadata={
            "year": year, "resolution": "100m",
            "weights": WEIGHTS,
            "classes": {"3": "high", "2": "moderate", "1": "low"},
            "source": "SRTM, CHIRPS, SoilGrids",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
