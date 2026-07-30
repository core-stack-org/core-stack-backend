"""Historical VCI climatology (plan.md Step 3 / Script 03a, Parts A-D).

VCI = (NDVI - NDVI_p05) / (NDVI_p95 - NDVI_p05) x 100, from MODIS MOD13A1
(16-day NDVI composites, 500m, 2000-present), using the same agricultural
mask built in Step 2. 5th/95th percentiles (not literal min/max) reduce
sensitivity to cloud-contamination outliers in the 25-year record.

Unlike MAI, the full historical VCI timeseries (not just the climatology)
is needed here: the future operational script looks up "what was VCI at
this pixel, this phenological day, in this specific historical year" for
the analog-year comparison - the same reason SPI-1/SPEI-3 needed their
full timeseries uploaded, not just params.

All GEE-side (bulk ImageCollection percentile/mean/std reduction) -
nothing touches local disk. Phenology curve fitting (Script 03b) is
separate, local work, added in a later pass.
"""

from datetime import datetime

import ee

from utilities.gee_utils import ee_initialize, check_task_status, is_gee_asset_exists
from computing.farm_stress.gee_upload import ensure_farm_stress_folders
from computing.farm_stress.mai_climatology import AGRI_MASK_ASSET_ID
from computing.farm_stress.config import (
    MODIS_NDVI_COLLECTION,
    INDIA_BBOX_COORDS,
    EXPORT_SCALE_M,
    FARM_STRESS_ASSET_ROOT,
)

NDVI_MINMAX_FOLDER = f"{FARM_STRESS_ASSET_ROOT}/ndvi_minmax"
VCI_TIMESERIES_FOLDER = f"{FARM_STRESS_ASSET_ROOT}/vci_timeseries"
VCI_CLIMATOLOGY_FOLDER = f"{FARM_STRESS_ASSET_ROOT}/vci_climatology"

N_16DAY_PERIODS = 23


def build_ndvi_collection(region, agri_mask):
    """NDVI collection: scaled (x0.0001), quality-masked (SummaryQA <= 1,
    good+marginal only), agri-masked, tagged with period_of_year (0-22,
    the fixed 16-day period-of-year).
    """
    col = (
        ee.ImageCollection(MODIS_NDVI_COLLECTION)
        .filterDate("2000-01-01", "2026-01-01")
        .filterBounds(region)
        .select(["NDVI", "SummaryQA"])
    )

    def prep(img):
        ndvi = img.select("NDVI").multiply(0.0001)
        qa = img.select("SummaryQA")
        ndvi = ndvi.updateMask(qa.lte(1)).updateMask(agri_mask).rename("ndvi")

        # Same property-preservation care as MAI: arithmetic/masking ops
        # don't carry system:time_start through, so it must be re-attached
        # explicitly alongside period_of_year.
        time_start = img.get("system:time_start")
        doy = ee.Date(time_start).getRelative("day", "year").add(1)
        period_of_year = doy.subtract(1).divide(16).floor()
        return ndvi.set("period_of_year", period_of_year, "system:time_start", time_start)

    return col.map(prep)


def compute_ndvi_percentiles(ndvi_col, period):
    """5th/95th percentile NDVI across all historical years at one fixed
    16-day period-of-year."""
    period_col = ndvi_col.filter(ee.Filter.eq("period_of_year", period))
    percentiles = period_col.reduce(ee.Reducer.percentile([5, 95]))
    return percentiles.rename(["ndvi_p05", "ndvi_p95"])


def build_percentiles_collection(ndvi_col):
    """All 23 periods' percentile images, tagged with period_of_year, as
    one small ImageCollection - used to look up the right period's
    percentiles for each individual composite when computing VCI.
    """
    images = [
        compute_ndvi_percentiles(ndvi_col, period).set("period_of_year", period)
        for period in range(N_16DAY_PERIODS)
    ]
    return ee.ImageCollection.fromImages(images)


def build_vci_collection(ndvi_col, percentiles_col):
    """VCI for every historical composite: (ndvi - p05)/(p95-p05) x 100,
    clipped [0,100], masked where (p95-p05) < 0.05 (near-constant NDVI ->
    VCI undefined/meaningless there).
    """
    def to_vci(img):
        period = img.get("period_of_year")
        percentiles = ee.Image(
            percentiles_col.filter(ee.Filter.eq("period_of_year", period)).first()
        )
        p05 = percentiles.select("ndvi_p05")
        p95 = percentiles.select("ndvi_p95")
        spread = p95.subtract(p05)

        vci = img.subtract(p05).divide(spread).multiply(100).clamp(0, 100)
        vci = vci.updateMask(spread.gte(0.05)).rename("vci")
        return vci.set("period_of_year", period, "system:time_start", img.get("system:time_start"))

    return ndvi_col.map(to_vci)


def export_ndvi_percentiles(gee_account_id, overwrite=False, poll_seconds=30):
    """Export the 23 NDVI percentile (p05/p95) assets."""
    ee_initialize(gee_account_id)
    ensure_farm_stress_folders(gee_account_id, subfolders=["ndvi_minmax"])

    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    agri_mask = ee.Image(AGRI_MASK_ASSET_ID)
    ndvi_col = build_ndvi_collection(region, agri_mask)

    pending, skipped = [], []
    for period in range(N_16DAY_PERIODS):
        asset_id = f"{NDVI_MINMAX_FOLDER}/period_{period:02d}"
        if not overwrite and is_gee_asset_exists(asset_id):
            skipped.append(asset_id)
            continue

        percentiles = compute_ndvi_percentiles(ndvi_col, period)
        task = ee.batch.Export.image.toAsset(
            image=percentiles,
            description=f"ndvi_minmax_period_{period:02d}",
            assetId=asset_id,
            scale=EXPORT_SCALE_M,
            region=region,
            crs="EPSG:4326",
            maxPixels=1e13,
        )
        task.start()
        task_id = task.status()["id"]
        print(f"Submitted {asset_id} (task {task_id})")
        pending.append((asset_id, task_id))

    print(f"Submitted {len(pending)}, skipped {len(skipped)}. Waiting for the batch...")
    check_task_status([t for _, t in pending], sleep_time=poll_seconds)
    print(f"Done. Exported {len(pending)}, skipped {len(skipped)}.")
    return {"exported": [a for a, _ in pending], "skipped": skipped}


def export_vci_timeseries(gee_account_id, overwrite=False, poll_seconds=30):
    """Export VCI for every historical composite (~575, one per 16-day
    period, 2000-present) as individually queryable GEE assets - needed
    for the operational analog-year lookup (see module docstring).
    """
    ee_initialize(gee_account_id)
    ensure_farm_stress_folders(gee_account_id, subfolders=["vci_timeseries"])

    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    agri_mask = ee.Image(AGRI_MASK_ASSET_ID)
    ndvi_col = build_ndvi_collection(region, agri_mask)
    percentiles_col = build_percentiles_collection(ndvi_col)
    vci_col = build_vci_collection(ndvi_col, percentiles_col)

    # Export.image.toAsset needs a concrete Python asset_id per image, so
    # every composite's timestamp is fetched client-side in one call
    # rather than looping through the lazy collection blindly.
    time_starts = ndvi_col.aggregate_array("system:time_start").getInfo()
    print(f"{len(time_starts)} composites to process")

    pending, skipped = [], []
    for i, ts in enumerate(time_starts, start=1):
        label = datetime.utcfromtimestamp(ts / 1000).strftime("%Y%m%d")
        asset_id = f"{VCI_TIMESERIES_FOLDER}/vci_{label}"
        if not overwrite and is_gee_asset_exists(asset_id):
            skipped.append(asset_id)
            continue

        img = ee.Image(vci_col.filter(ee.Filter.eq("system:time_start", ts)).first())
        task = ee.batch.Export.image.toAsset(
            image=img,
            description=f"vci_{label}",
            assetId=asset_id,
            scale=EXPORT_SCALE_M,
            region=region,
            crs="EPSG:4326",
            maxPixels=1e13,
        )
        task.start()
        task_id = task.status()["id"]
        print(f"[{i}/{len(time_starts)}] submitted {asset_id} (task {task_id})")
        pending.append((asset_id, task_id))

    print(f"Submitted {len(pending)}, skipped {len(skipped)}. Waiting for the batch...")
    check_task_status([t for _, t in pending], sleep_time=poll_seconds)
    print(f"Done. Exported {len(pending)}, skipped {len(skipped)}.")
    return {"exported": [a for a, _ in pending], "skipped": skipped}
