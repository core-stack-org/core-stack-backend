"""
Forest Structure Estimation (Field Level @30m)

Classifies forest cover into dense, open, and scrub using NDVI and
texture metrics from Sentinel-2 / Landsat imagery. Produces annual
raster and vector outputs at 30m resolution.

Classification thresholds (based on FSI conventions):
    - Dense forest:  NDVI > 0.6
    - Open forest:   0.3 < NDVI <= 0.6
    - Scrub:         NDVI <= 0.3

Reference:
    - https://www.mdpi.com/2072-4292/13/24/5105
    - https://www.cse.iitd.ernet.in/~aseth/forest-health-ictd2024.pdf
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
    get_layer_object,
)
from computing.STAC_specs import generate_STAC_layerwise

# NDVI thresholds for forest structure classification
DENSE_THRESHOLD = 0.6
OPEN_THRESHOLD = 0.3

# Class values in raster output
CLASS_DENSE = 3
CLASS_OPEN = 2
CLASS_SCRUB = 1
CLASS_NON_FOREST = 0


def _get_cloud_masked_composite(year, roi):
    """Build a cloud-free annual composite from Sentinel-2 + Landsat."""
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    # Sentinel-2 SR
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start, end)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
    )

    def mask_s2_clouds(image):
        qa = image.select("QA60")
        cloud_mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
        return image.updateMask(cloud_mask)

    s2_masked = s2.map(mask_s2_clouds)

    # Compute NDVI from Sentinel-2 bands
    def add_ndvi_s2(image):
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("ndvi")
        return image.addBands(ndvi)

    s2_ndvi = s2_masked.map(add_ndvi_s2)

    # Fallback to Landsat 8/9 if Sentinel-2 coverage is sparse
    l8 = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterDate(start, end)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUD_COVER", 30))
    )
    l9 = (
        ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
        .filterDate(start, end)
        .filterBounds(roi)
        .filter(ee.Filter.lt("CLOUD_COVER", 30))
    )

    def add_ndvi_landsat(image):
        ndvi = image.normalizedDifference(["SR_B5", "SR_B4"]).rename("ndvi")
        return image.addBands(ndvi)

    landsat = l8.merge(l9).map(add_ndvi_landsat)

    # Merge and take median NDVI
    combined = s2_ndvi.select("ndvi").merge(landsat.select("ndvi"))
    median_ndvi = combined.median().clip(roi)

    return median_ndvi


def _classify_forest_structure(ndvi_image):
    """Classify NDVI image into forest structure classes."""
    dense = ndvi_image.gt(DENSE_THRESHOLD).multiply(CLASS_DENSE)
    open_forest = (
        ndvi_image.gt(OPEN_THRESHOLD)
        .And(ndvi_image.lte(DENSE_THRESHOLD))
        .multiply(CLASS_OPEN)
    )
    scrub = ndvi_image.lte(OPEN_THRESHOLD).And(ndvi_image.gt(0.1)).multiply(CLASS_SCRUB)

    classified = dense.add(open_forest).add(scrub).rename("forest_structure")
    return classified.toUint8()


def _vectorize_structure(classified_image, roi, scale=30):
    """Convert classified raster to vector polygons with attributes."""
    vectors = classified_image.reduceToVectors(
        geometry=roi,
        scale=scale,
        geometryType="polygon",
        eightConnected=True,
        labelProperty="structure_class",
        maxPixels=1e10,
    )

    # Map class numbers to labels
    def add_label(feature):
        cls = feature.get("structure_class")
        label = (
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_DENSE), "dense",
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_OPEN), "open",
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_SCRUB), "scrub", "non_forest")))
        )
        area = feature.geometry().area().divide(10000)  # hectares
        return feature.set({"structure_label": label, "area_ha": area})

    return vectors.map(add_label)


@app.task(bind=True)
def compute_forest_structure(
    self,
    state=None,
    district=None,
    block=None,
    year=2024,
    gee_account_id=None,
    roi_path=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Compute annual forest structure map for a tehsil/AoI.

    Produces both raster (30m classification) and vector (MWS-level polygons)
    outputs as GEE assets.
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

    raster_name = f"forest_structure_{year}_{asset_suffix}"
    vector_name = f"forest_structure_vec_{year}_{asset_suffix}"

    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_asset_id = gee_dir + raster_name
    vector_asset_id = gee_dir + vector_name

    # Step 1: compute NDVI composite
    print(f"Computing NDVI composite for {year}...")
    ndvi = _get_cloud_masked_composite(year, roi_geom)

    # Step 2: classify
    print("Classifying forest structure...")
    classified = _classify_forest_structure(ndvi)

    # Step 3: export raster
    if not is_gee_asset_exists(raster_asset_id):
        print(f"Exporting raster: {raster_asset_id}")
        task_id = export_raster_asset_to_gee(
            classified, raster_name, raster_asset_id, scale=30, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_asset_id)
    else:
        print(f"Raster already exists: {raster_asset_id}")

    # Step 4: vectorize and export
    if not is_gee_asset_exists(vector_asset_id):
        print("Vectorizing...")
        vectors = _vectorize_structure(classified, roi_geom)

        # Add mean NDVI per polygon
        vectors = ndvi.reduceRegions(
            collection=vectors, reducer=ee.Reducer.mean(), scale=30
        ).map(lambda f: f.set("mean_ndvi", f.get("mean")))

        print(f"Exporting vector: {vector_asset_id}")
        task_id = export_vector_asset_to_gee(vectors, vector_name, vector_asset_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_asset_id)
    else:
        print(f"Vector already exists: {vector_asset_id}")

    # Step 5: sync and save
    layer_name = f"{asset_suffix}_forest_structure_{year}"
    sync_fc_to_geoserver(vector_asset_id, layer_name)
    save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        dataset_name="Forest Structure",
        metadata={
            "year": year,
            "resolution": "30m",
            "classes": {"3": "dense", "2": "open", "1": "scrub", "0": "non_forest"},
            "thresholds": {"dense": f"NDVI>{DENSE_THRESHOLD}", "open": f"{OPEN_THRESHOLD}<NDVI<={DENSE_THRESHOLD}"},
            "source": "Sentinel-2 + Landsat 8/9",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
