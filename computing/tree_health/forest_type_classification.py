"""
Forest Type Classification — Native vs Planted (Field Level @30m)

Differentiates native forests from planted forests by intersecting
the CoRE Stack LULC tree cover with the published pan-India natural
forests dataset from Chowdhury et al. (2025).

Instead of training a new classifier, we use the existing published
natural forests map and overlay it on CoRE stack tree cover pixels
to produce per-pixel and per-MWS statistics.

Reference:
    Chowdhury et al. (2025) - Nature Scientific Data
    https://www.nature.com/articles/s41597-025-06097-z
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
from utilities.constants import GEE_PATHS, GEE_DATASET_PATH
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

# Published natural forest dataset (update path once ingested into CoRE GEE)
# This should point to the Chowdhury et al. natural forests raster asset
NATURAL_FOREST_ASSET = GEE_DATASET_PATH + "/natural_forests_india"

CLASS_NATIVE = 2
CLASS_PLANTED = 1
CLASS_NON_FOREST = 0


def _classify_forest_type(lulc_asset_id, natural_forest_asset, roi):
    """Intersect LULC tree cover with natural forests map.

    Pixels that are tree cover in LULC AND natural forest = native
    Pixels that are tree cover in LULC BUT NOT natural forest = planted
    """
    lulc = ee.Image(lulc_asset_id).clip(roi)

    # tree cover mask from LULC (class 1 and 2 are typically tree classes)
    tree_mask = lulc.eq(1).Or(lulc.eq(2))

    # natural forest mask
    try:
        natural = ee.Image(natural_forest_asset).clip(roi)
        natural_mask = natural.gt(0)
    except Exception:
        # fallback: if asset doesn't exist yet, use a placeholder
        # this will be updated once the natural forests asset is ingested
        print(f"Warning: natural forest asset not found at {natural_forest_asset}")
        print("Using tree cover height as proxy (taller trees = likely native)")
        # use canopy height > 10m as proxy for native forest
        canopy = ee.Image("users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1")
        natural_mask = canopy.gt(10).clip(roi)

    native = tree_mask.And(natural_mask).multiply(CLASS_NATIVE)
    planted = tree_mask.And(natural_mask.Not()).multiply(CLASS_PLANTED)

    classified = native.add(planted).rename("forest_type").toUint8()
    return classified, tree_mask


def _vectorize_forest_type(classified, roi, mws_fc, scale=30):
    """Compute per-MWS forest type statistics using reduceRegions.

    For each MWS polygon, computes:
        - native_area_ha
        - planted_area_ha
        - native_pct (% of tree cover that is native)
    """
    # pixel area in hectares
    pixel_area = ee.Image.pixelArea().divide(10000)

    native_area = pixel_area.updateMask(classified.eq(CLASS_NATIVE))
    planted_area = pixel_area.updateMask(classified.eq(CLASS_PLANTED))

    # reduce to MWS boundaries
    native_stats = native_area.reduceRegions(
        collection=mws_fc,
        reducer=ee.Reducer.sum(),
        scale=scale,
    ).map(lambda f: f.set("native_area_ha", ee.Number(f.get("sum")).round()))

    planted_stats = planted_area.reduceRegions(
        collection=native_stats,
        reducer=ee.Reducer.sum(),
        scale=scale,
    ).map(lambda f: f.set("planted_area_ha", ee.Number(f.get("sum")).round()))

    # compute percentage
    def add_pct(feature):
        native = ee.Number(feature.get("native_area_ha"))
        planted = ee.Number(feature.get("planted_area_ha"))
        total = native.add(planted)
        pct = ee.Algorithms.If(total.gt(0), native.divide(total).multiply(100).round(), 0)
        return feature.set({"native_pct": pct, "total_forest_ha": total})

    result = planted_stats.map(add_pct)
    return result


@app.task(bind=True)
def compute_forest_type(
    self,
    state=None,
    district=None,
    block=None,
    lulc_year=2024,
    gee_account_id=None,
    roi_path=None,
    lulc_asset_id=None,
    natural_forest_asset=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Classify forest as native vs planted for a tehsil."""
    ee_initialize(gee_account_id)

    if natural_forest_asset is None:
        natural_forest_asset = NATURAL_FOREST_ASSET

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

    mws_fc = ee.FeatureCollection(roi_path)
    roi_geom = mws_fc.geometry()

    raster_name = f"forest_type_{lulc_year}_{asset_suffix}"
    vector_name = f"forest_type_vec_{lulc_year}_{asset_suffix}"

    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    # classify
    print(f"Classifying forest type for {lulc_year}...")
    classified, tree_mask = _classify_forest_type(
        lulc_asset_id, natural_forest_asset, roi_geom
    )

    # export raster
    if not is_gee_asset_exists(raster_id):
        task_id = export_raster_asset_to_gee(
            classified, raster_name, raster_id, scale=30, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # vectorize per MWS
    if not is_gee_asset_exists(vector_id):
        print("Computing per-MWS stats...")
        vectors = _vectorize_forest_type(classified, roi_geom, mws_fc)

        task_id = export_vector_asset_to_gee(vectors, vector_name, vector_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_forest_type_{lulc_year}"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Forest Type Classification",
        metadata={
            "lulc_year": lulc_year, "resolution": "30m",
            "classes": {"2": "native", "1": "planted", "0": "non_forest"},
            "reference": "Chowdhury et al. 2025 (Nature Sci Data)",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
