"""Historical MAI climatology (plan.md Step 2 / Script 02).

MAI = AET/PET, from MODIS MOD16A2GF (the same gap-filled collection
already used for SPEI-3's PET input). Stays at MODIS's native 500m
resolution (unlike SPI-1/SPEI-3, computed at GSMaP's coarser 11km), so
every output here is a full-India 500m raster - exported directly as a
GEE asset (Export.image.toAsset), never touching local disk, since this
step is pure bulk ImageCollection aggregation (masking, division,
mean/std reduction) with no local scipy fitting involved.

Only the agricultural mask (1 asset) and the climatology (46 assets, one
per fixed 8-day period-of-year, mean+std) are exported - not the full
~1,150-composite historical MAI timeseries, which nothing in the
currently-planned operational pipeline looks up individually (unlike
SPI-1/SPEI-3's per-year analog similarity lookups).
"""

import ee

from utilities.gee_utils import ee_initialize, check_task_status, is_gee_asset_exists
from computing.farm_stress.gee_upload import ensure_farm_stress_folders
from computing.farm_stress.config import (
    MODIS_LC_COLLECTION,
    MODIS_ET_COLLECTION,
    AGRI_LC_CLASSES,
    INDIA_BBOX_COORDS,
    EXPORT_SCALE_M,
    FARM_STRESS_ASSET_ROOT,
)

AGRI_MASK_ASSET_ID = f"{FARM_STRESS_ASSET_ROOT}/agri_mask_500m"
MAI_CLIMATOLOGY_FOLDER = f"{FARM_STRESS_ASSET_ROOT}/mai_climatology"


def build_agri_mask(region):
    """Binary agricultural mask: pixel = 1 if classified Cropland/Cropland-
    mosaic (IGBP 12/14) in at least 10 of the 20 years 2001-2020 - a
    majority vote to stabilise against year-to-year land-cover noise.
    """
    lc_col = (
        ee.ImageCollection(MODIS_LC_COLLECTION)
        .filterDate("2001-01-01", "2021-01-01")
        .select("LC_Type1")
    )

    def is_agri_year(img):
        lc = img.select("LC_Type1")
        return lc.eq(AGRI_LC_CLASSES[0]).Or(lc.eq(AGRI_LC_CLASSES[1])).rename("is_agri")

    agri_years_count = lc_col.map(is_agri_year).sum()
    return agri_years_count.gte(10).rename("agri_mask").clip(region)


def build_mai_collection(region, agri_mask):
    """Per-composite MAI = AET/PET (scaled, clipped [0,1], masked to
    agricultural pixels), tagged with period_index (0-45, the fixed 8-day
    period-of-year), for every MOD16A2GF composite 2000-2025.
    """
    col = (
        ee.ImageCollection(MODIS_ET_COLLECTION)
        .filterDate("2000-01-01", "2026-01-01")
        .filterBounds(region)
        .select(["ET", "PET"])
    )

    def to_mai(img):
        et = img.select("ET").multiply(0.1)
        pet = img.select("PET").multiply(0.1)
        mai = et.divide(pet).clamp(0, 1).updateMask(pet.gt(0)).rename("mai")
        mai = mai.updateMask(agri_mask)

        # .select/.multiply/.divide/.updateMask/.rename don't carry the
        # original image's properties through (same issue hit with PET
        # proration in Step 1) - system:time_start has to be re-attached
        # explicitly, or filterDate() on the resulting collection silently
        # matches nothing.
        time_start = img.get("system:time_start")
        doy = ee.Date(time_start).getRelative("day", "year").add(1)
        period_index = doy.subtract(1).divide(8).floor()
        return mai.set("period_index", period_index, "system:time_start", time_start)

    return col.map(to_mai)


def export_mai_climatology(gee_account_id, overwrite=False, poll_seconds=30):
    """Build the agricultural mask and the 46-period MAI climatology
    (mean + std per 8-day period-of-year), exporting both as GEE assets.
    Pure GEE-side computation - nothing touches local disk.

    Safe to interrupt and re-run: assets already on GEE are skipped
    unless overwrite=True.
    """
    ee_initialize(gee_account_id)
    ensure_farm_stress_folders(gee_account_id, subfolders=["mai_climatology"])

    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    agri_mask = build_agri_mask(region)
    mai_col = build_mai_collection(region, agri_mask)

    pending = []
    skipped = []

    if not overwrite and is_gee_asset_exists(AGRI_MASK_ASSET_ID):
        skipped.append(AGRI_MASK_ASSET_ID)
    else:
        mask_task = ee.batch.Export.image.toAsset(
            image=agri_mask,
            description="agri_mask_500m",
            assetId=AGRI_MASK_ASSET_ID,
            scale=EXPORT_SCALE_M,
            region=region,
            crs="EPSG:4326",
            maxPixels=1e13,
        )
        mask_task.start()
        task_id = mask_task.status()["id"]
        print(f"Submitted agri mask -> {AGRI_MASK_ASSET_ID} (task {task_id})")
        pending.append((AGRI_MASK_ASSET_ID, task_id))

    for period in range(46):
        asset_id = f"{MAI_CLIMATOLOGY_FOLDER}/period_{period:02d}"
        if not overwrite and is_gee_asset_exists(asset_id):
            skipped.append(asset_id)
            continue

        period_col = mai_col.filter(ee.Filter.eq("period_index", period)).select("mai")
        mean_img = period_col.mean().rename("mai_mean")
        std_img = period_col.reduce(ee.Reducer.stdDev()).rename("mai_std")
        combined = mean_img.addBands(std_img)

        task = ee.batch.Export.image.toAsset(
            image=combined,
            description=f"mai_climatology_period_{period:02d}",
            assetId=asset_id,
            scale=EXPORT_SCALE_M,
            region=region,
            crs="EPSG:4326",
            maxPixels=1e13,
        )
        task.start()
        task_id = task.status()["id"]
        print(f"Submitted period {period:02d} -> {asset_id} (task {task_id})")
        pending.append((asset_id, task_id))

    print(
        f"Submitted {len(pending)} export task(s), skipped {len(skipped)} already on GEE. "
        "Waiting for the batch to finish..."
    )
    check_task_status([task_id for _, task_id in pending], sleep_time=poll_seconds)

    print(f"Done. Exported {len(pending)}, skipped {len(skipped)}.")
    return {"exported": [a for a, _ in pending], "skipped": skipped}
