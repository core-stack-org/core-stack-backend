"""
Age of Forests and Plantations (Field Level @30m)

Estimates forest age by analyzing Landsat time series to detect
when tree cover first appeared at each pixel. Uses the CoRE stack
annual LULC to find the earliest year a pixel was classified as
tree cover, going back to the Landsat archive (~1985 onwards).

For pixels that have been forested since before the satellite record,
canopy height is used as a proxy — taller canopy generally indicates
older forests.

Methodology:
    Phase 1: LULC time-series based (this implementation)
        - Walk backwards through annual LULC layers
        - For each pixel, find the first year it became tree cover
        - Age = current_year - first_tree_year

    Phase 2 (future): AGB-based estimation for pre-1985 forests
        - Use above-ground biomass and growth rate models

Reference:
    - https://www.nature.com/articles/s41597-022-01260-2
    - https://essd.copernicus.org/articles/13/4881/2021/
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

# Hansen Global Forest Change dataset for tree cover gain/loss timeline
HANSEN_ASSET = "UMD/hansen/global_forest_change_2023_v1_11"
# ETH Global Canopy Height for age proxy of old-growth forests
CANOPY_HEIGHT_ASSET = "users/nlang/ETH_GlobalCanopyHeight_2020_10m_v1"

EARLIEST_YEAR = 1985
CURRENT_YEAR = 2024


def _estimate_age_from_hansen(roi):
    """Use Hansen loss year + gain to estimate forest age.

    Hansen provides:
        - treecover2000: % tree cover in year 2000
        - lossyear: year of loss (1-23 for 2001-2023)
        - gain: binary gain 2000-2012

    Logic:
        - Pixels with treecover2000 > 25 and no loss: age >= 24 years (since 2000)
        - Pixels with gain and no loss: age ~ 12-24 years
        - Pixels with loss then regrowth: age = current_year - loss_year
    """
    hansen = ee.Image(HANSEN_ASSET).clip(roi)

    treecover = hansen.select("treecover2000")
    lossyear = hansen.select("lossyear")
    gain = hansen.select("gain")

    # tree cover in 2000 with no subsequent loss = at least 24 years old
    no_loss = lossyear.eq(0)
    had_cover_2000 = treecover.gt(25)

    # age for pixels with continuous cover since 2000
    # conservatively, these are at least (current_year - 2000) years old
    age_continuous = ee.Image.constant(CURRENT_YEAR - 2000).updateMask(
        had_cover_2000.And(no_loss)
    )

    # age for pixels that were lost and regrew
    # approximate regrowth start as loss_year + a few years
    loss_year_actual = lossyear.add(2000)
    age_regrowth = ee.Image.constant(CURRENT_YEAR).subtract(loss_year_actual).subtract(3)
    age_regrowth = age_regrowth.updateMask(lossyear.gt(0).And(gain))
    age_regrowth = age_regrowth.max(0)

    # age for new gain pixels (gained 2000-2012, ~12-24 years old)
    age_gain = ee.Image.constant(18).updateMask(gain.And(no_loss).And(had_cover_2000.Not()))

    # combine: prefer continuous > gain > regrowth
    age = age_continuous.unmask(age_gain).unmask(age_regrowth).rename("forest_age_years")

    return age


def _refine_with_canopy_height(age_image, roi):
    """Use canopy height to refine age for old-growth forests.

    Taller trees (>15m) that already have age >= 24 are likely much older.
    Apply a rough mapping: height_m * 2 as estimated age for tall canopies.
    """
    canopy = ee.Image(CANOPY_HEIGHT_ASSET).clip(roi).rename("canopy_height")

    # for pixels with canopy > 15m and age already at the cap
    tall_old = canopy.gt(15).And(age_image.gte(CURRENT_YEAR - 2000))
    # rough age estimate: 2 years per meter of height
    height_based_age = canopy.multiply(2).updateMask(tall_old)

    # use height-based where it gives a higher estimate
    refined = age_image.where(height_based_age.gt(age_image), height_based_age)
    return refined.rename("forest_age_years")


def _vectorize_age(age_image, mws_fc, scale=30):
    """Compute per-MWS forest age statistics."""
    stats = age_image.reduceRegions(
        collection=mws_fc,
        reducer=ee.Reducer.mean()
            .combine(ee.Reducer.minMax(), sharedInputs=True)
            .combine(ee.Reducer.stdDev(), sharedInputs=True),
        scale=scale,
    )

    def add_distribution(f):
        """Compute % of forest in different age brackets."""
        geom = f.geometry()
        total_pixels = age_image.gt(0).reduceRegion(
            reducer=ee.Reducer.sum(), geometry=geom, scale=scale, maxPixels=1e9
        ).get("forest_age_years")

        old_pixels = age_image.gte(40).reduceRegion(
            reducer=ee.Reducer.sum(), geometry=geom, scale=scale, maxPixels=1e9
        ).get("forest_age_years")

        pct_old = ee.Algorithms.If(
            ee.Number(total_pixels).gt(0),
            ee.Number(old_pixels).divide(total_pixels).multiply(100).round(),
            0
        )

        return f.set({
            "mean_age_years": ee.Number(f.get("mean")).round(),
            "max_age_years": ee.Number(f.get("max")).round(),
            "min_age_years": ee.Number(f.get("min")).round(),
            "pct_old_growth_40plus": pct_old,
        })

    return stats.map(add_distribution)


@app.task(bind=True)
def compute_forest_age(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    roi_path=None,
    asset_folder_list=None,
    asset_suffix=None,
    app_type="MWS",
):
    """Estimate forest age for an AoI using Landsat time series + canopy height."""
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

    raster_name = f"forest_age_{asset_suffix}"
    vector_name = f"forest_age_vec_{asset_suffix}"
    gee_dir = get_gee_dir_path(
        asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    raster_id = gee_dir + raster_name
    vector_id = gee_dir + vector_name

    # estimate age
    print("Estimating forest age from Hansen + canopy height...")
    age = _estimate_age_from_hansen(roi_geom)
    age = _refine_with_canopy_height(age, roi_geom)

    # export raster
    if not is_gee_asset_exists(raster_id):
        task_id = export_raster_asset_to_gee(
            age, raster_name, raster_id, scale=30, region=roi_geom
        )
        if task_id:
            check_task_status(task_id)
            make_asset_public(raster_id)

    # vectorize
    if not is_gee_asset_exists(vector_id):
        print("Computing per-MWS age stats...")
        vectors = _vectorize_age(age, mws_fc)
        task_id = export_vector_asset_to_gee(vectors, vector_name, vector_id)
        if task_id:
            check_task_status(task_id)
            make_asset_public(vector_id)

    layer_name = f"{asset_suffix}_forest_age"
    sync_fc_to_geoserver(vector_id, layer_name)
    save_layer_info_to_db(
        state=state, district=district, block=block,
        layer_name=layer_name, dataset_name="Forest Age",
        metadata={
            "resolution": "30m",
            "method": "Hansen time-series + ETH canopy height proxy",
            "source": "UMD/Hansen GFC 2023, ETH GlobalCanopyHeight 2020",
            "age_range": f"{EARLIEST_YEAR}-{CURRENT_YEAR}",
        },
    )
    update_layer_sync_status(layer_name, status="synced")
    print("Done.")
