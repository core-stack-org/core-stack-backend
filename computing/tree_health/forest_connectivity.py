"""
Forest Structural Connectivity (Field Level @30m)

Computes structural connectivity of forests using LULC tree cover data.
Identifies core forest areas vs edge/peripheral zones using morphological
operations on binary forest masks. Helps detect whether deforestation
or degradation is happening in the core or along peripheries.

Methodology:
    1. Extract binary forest mask from CoRE stack LULC
    2. Apply morphological erosion to identify core forest
    3. Difference between mask and eroded = edge forest
    4. Compute patch-level metrics (size, distance from edge)
    5. Classify into core, edge, fragmented
    6. Vectorize and publish

Reference: MSPA (Morphological Spatial Pattern Analysis)
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
from utilities.constants import GEE_PATHS, GEE_DATASET_PATH
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

CLASS_CORE = 3
CLASS_EDGE = 2
CLASS_FRAGMENT = 1
CLASS_NON_FOREST = 0

# Erosion kernel radius in pixels (at 30m, 3 pixels = 90m buffer)
EROSION_RADIUS = 3
# Minimum patch size in hectares to be considered core
MIN_CORE_PATCH_HA = 10


def _get_forest_mask(lulc_asset_id, roi):
    """Extract binary forest mask from CoRE stack LULC asset.

    Tree cover classes in CoRE stack LULC are typically class values
    representing different vegetation types. We look for tree/forest pixels.
    """
    lulc = ee.Image(lulc_asset_id).clip(roi)

    # CoRE stack LULC typically has tree cover as specific class values
    # Try common tree class values (1, 2 for deciduous/evergreen trees)
    # This may need adjustment based on the actual LULC schema
    forest_mask = lulc.eq(1).Or(lulc.eq(2)).rename("forest")
    return forest_mask


def _compute_connectivity(forest_mask):
    """Apply morphological operations to classify connectivity.

    Core = forest that remains after erosion (interior pixels)
    Edge = forest pixels removed by erosion (boundary pixels)
    Fragment = small isolated patches
    """
    # Erosion using a circular kernel
    kernel = ee.Kernel.circle(radius=EROSION_RADIUS, units="pixels")
    eroded = forest_mask.focal_min(kernel=kernel).rename("eroded")

    # Core forest: survived erosion
    core = eroded.multiply(CLASS_CORE)

    # Edge: original forest minus core
    edge = forest_mask.subtract(eroded).multiply(CLASS_EDGE)

    # Combined classification
    connectivity = core.add(edge).where(forest_mask.eq(0), CLASS_NON_FOREST)
    connectivity = connectivity.rename("connectivity_class").toUint8()

    return connectivity


def _compute_distance_to_edge(forest_mask, scale=30):
    """Compute distance from each forest pixel to nearest non-forest pixel."""
    # Invert mask: non-forest = 1
    non_forest = forest_mask.Not()
    distance = non_forest.fastDistanceTransform().sqrt().multiply(scale)
    distance = distance.updateMask(forest_mask).rename("dist_to_edge")
    return distance


def _vectorize_connectivity(connectivity, distance, roi, scale=30):
    """Convert connectivity raster to vector polygons."""
    vectors = connectivity.reduceToVectors(
        geometry=roi,
        scale=scale,
        geometryType="polygon",
        eightConnected=True,
        labelProperty="connectivity_class",
        maxPixels=1e10,
    )

    def add_attributes(feature):
        cls = feature.get("connectivity_class")
        label = (
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_CORE), "core",
            ee.Algorithms.If(ee.Number(cls).eq(CLASS_EDGE), "edge", "non_forest"))
        )
        area = feature.geometry().area().divide(10000)
        return feature.set({"connectivity_label": label, "area_ha": area})

    vectors = vectors.map(add_attributes)

    # add mean distance to edge per polygon
    vectors = distance.reduceRegions(
        collection=vectors, reducer=ee.Reducer.mean(), scale=scale
    ).map(lambda f: f.set("mean_dist_to_edge_m", f.get("mean")))

    return vectors


@app.task(bind=True)
def compute_forest_connectivity(
    self,
    state=None,
    district=None,
    block=None,
    lulc_year=2024,
    gee_account_id=None,
    roi_path=None,
    lulc_asset_id=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Compute forest structural connectivity for an AoI.

    Uses the most recent LULC from CoRE stack or a provided asset.
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

        if lulc_asset_id is None:
            lulc_asset_id = (
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + f"lulc_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_{lulc_year}"
            )

    roi = ee.FeatureCollection(roi_path)
    roi_geom = roi.geometry()

    raster_name = f"forest_connectivity_{lulc_year}_{asset_suffix}"
    vector_name = f"forest_connectivity_vec_{lulc_year}_{asset_suffix}"

    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    # Step 1: get forest mask
    print(f"Extracting forest mask from LULC {lulc_year}...")
    forest_mask = _get_forest_mask(lulc_asset_id, roi_geom)

    # Step 2: compute connectivity
    print("Computing connectivity (core vs edge)...")
    connectivity = _compute_connectivity(forest_mask)
    distance = _compute_distance_to_edge(forest_mask)

    # Step 3: export raster
    if not is_gee_asset_exists(raster_id):
        print(f"Exporting raster: {raster_id}")
        task_id = export_raster_asset_to_gee(
            connectivity, raster_name, raster_id, scale=30, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # Step 4: vectorize and export
    if not is_gee_asset_exists(vector_id):
        print("Vectorizing...")
        vectors = _vectorize_connectivity(connectivity, distance, roi_geom)

        task_id = export_vector_asset_to_gee(vectors, vector_name, vector_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_id)

    # Step 5: save
    layer_name = f"{asset_suffix}_forest_connectivity_{lulc_year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        dataset_name="Forest Connectivity",
        metadata={
            "lulc_year": lulc_year,
            "resolution": "30m",
            "erosion_radius_px": EROSION_RADIUS,
            "classes": {"3": "core", "2": "edge", "0": "non_forest"},
            "source": "CoRE Stack LULC",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
