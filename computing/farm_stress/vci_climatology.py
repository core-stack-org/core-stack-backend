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

Parts A-C (percentiles, VCI timeseries) are GEE-side bulk ImageCollection
work - nothing touches local disk. Part D (the 23-period mean/std
climatology) is local instead: the full 26-year VCI archive was exported
to GEE assets, then pulled off via Drive (manually, not this repo's GCS
path, since these are full-India 500m rasters) and merged into local
COGs - see LOCAL_DIR_VCI_RASTERS. With the whole archive already on
disk, computing the climatology locally with numpy avoids yet another
round of GEE asset exports/quota, same reasoning as SPI-1/SPEI-3's local
fitting. Phenology curve fitting (Script 03b) is separate, also local,
added in a later pass.
"""

import os
from datetime import datetime

import ee
import numpy as np
import rasterio

from utilities.gee_utils import ee_initialize, check_task_status, is_gee_asset_exists
from computing.farm_stress.gee_upload import ensure_farm_stress_folders
from computing.farm_stress.mai_climatology import AGRI_MASK_ASSET_ID
from computing.farm_stress.config import (
    MODIS_NDVI_COLLECTION,
    INDIA_BBOX_COORDS,
    EXPORT_SCALE_M,
    FARM_STRESS_ASSET_ROOT,
    LOCAL_DIR_VCI_RASTERS,
    LOCAL_DIR_VCI_CLIMATOLOGY,
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
    """Export VCI as one multi-band asset PER YEAR (up to 23 bands, named
    period_00..period_22, one per 16-day period-of-year present that
    year) rather than one asset per individual composite - ~26 export
    tasks instead of ~575, to stay within GEE's per-account processing
    quota. Still gives the operational analog-year lookup everything it
    needs (any historical year's VCI at any phenological period), just
    organised as one image per year instead of one per composite.
    """
    ee_initialize(gee_account_id)
    ensure_farm_stress_folders(gee_account_id, subfolders=["vci_timeseries"])

    region = ee.Geometry.Rectangle(INDIA_BBOX_COORDS)
    agri_mask = ee.Image(AGRI_MASK_ASSET_ID)
    ndvi_col = build_ndvi_collection(region, agri_mask)
    percentiles_col = build_percentiles_collection(ndvi_col)
    vci_col = build_vci_collection(ndvi_col, percentiles_col)

    # Two aligned aggregate_array calls on the same (unfiltered/unsorted
    # in between) collection return values in the same underlying image
    # order, so zipping them client-side is safe.
    time_starts = ndvi_col.aggregate_array("system:time_start").getInfo()
    periods = ndvi_col.aggregate_array("period_of_year").getInfo()

    by_year = {}
    for ts, period in zip(time_starts, periods):
        year = datetime.utcfromtimestamp(ts / 1000).year
        by_year.setdefault(year, []).append((int(period), ts))

    print(f"{len(time_starts)} composites across {len(by_year)} years")

    pending, skipped = [], []
    for year in sorted(by_year):
        asset_id = f"{VCI_TIMESERIES_FOLDER}/vci_{year}"
        if not overwrite and is_gee_asset_exists(asset_id):
            skipped.append(asset_id)
            continue

        band_images = [
            ee.Image(vci_col.filter(ee.Filter.eq("system:time_start", ts)).first()).rename(
                f"period_{period:02d}"
            )
            for period, ts in sorted(by_year[year])
        ]
        combined = ee.Image.cat(band_images)

        task = ee.batch.Export.image.toAsset(
            image=combined,
            description=f"vci_{year}",
            assetId=asset_id,
            scale=EXPORT_SCALE_M,
            region=region,
            crs="EPSG:4326",
            maxPixels=1e13,
        )
        task.start()
        task_id = task.status()["id"]
        print(f"Submitted {asset_id} ({len(band_images)} bands, task {task_id})")
        pending.append((asset_id, task_id))

    print(f"Submitted {len(pending)}, skipped {len(skipped)}. Waiting for the batch...")
    check_task_status([t for _, t in pending], sleep_time=poll_seconds)
    print(f"Done. Exported {len(pending)}, skipped {len(skipped)}.")
    return {"exported": [a for a, _ in pending], "skipped": skipped}


def _get_year_band_periods(gee_account_id, years):
    """Authoritative band-order -> period_of_year mapping per year, read
    directly from the still-live vci_timeseries GEE assets' band names.

    Band descriptions weren't preserved through the manual Drive-export /
    merge / COG-conversion round-trip, so the local COGs' bands are
    anonymous by the time they reach this repo. Rather than assume which
    periods are missing for a partial year (e.g. year 2000, whose MODIS
    NDVI collection only starts 2000-02-18), this asks GEE directly - a
    cheap metadata-only call, not a recomputation.
    """
    ee_initialize(gee_account_id)
    mapping = {}
    for year in years:
        asset_id = f"{VCI_TIMESERIES_FOLDER}/vci_{year}"
        names = ee.Image(asset_id).bandNames().getInfo()
        mapping[year] = [int(name.split("_")[1]) for name in names]
    return mapping


def compute_vci_climatology_local(
    gee_account_id,
    start_year=2000,
    end_year=2025,
    rasters_dir=LOCAL_DIR_VCI_RASTERS,
    output_dir=LOCAL_DIR_VCI_CLIMATOLOGY,
    overwrite=False,
):
    """Compute the 23-period VCI climatology (mean, std, and a valid-year
    count for QA) directly from the locally downloaded yearly VCI COGs,
    one output raster per 16-day period-of-year.

    std uses ddof=1 (sample std), matching ee.Reducer.stdDev() and the
    convention already used elsewhere in this pipeline (e.g. spi_fit.py's
    gamma fitting). A pixel/period needs at least 2 valid years to get a
    std (else NaN); mean needs at least 1.

    Safe to interrupt and re-run: periods already on disk are skipped
    unless overwrite=True.
    """
    years = [y for y in range(start_year, end_year + 1)]
    available = {
        y: f"{rasters_dir}/vci_{y}_cog.tif" for y in years if os.path.exists(f"{rasters_dir}/vci_{y}_cog.tif")
    }
    missing_years = [y for y in years if y not in available]
    if missing_years:
        print(f"Warning: missing local rasters for years {missing_years}, proceeding without them")

    print(f"Fetching band-order metadata from GEE for {len(available)} year(s) ...")
    year_periods = _get_year_band_periods(gee_account_id, sorted(available))

    to_compute = [
        p
        for p in range(N_16DAY_PERIODS)
        if overwrite or not os.path.exists(f"{output_dir}/vci_climatology_period_{p:02d}.tif")
    ]
    skipped = [
        f"{output_dir}/vci_climatology_period_{p:02d}.tif" for p in range(N_16DAY_PERIODS) if p not in to_compute
    ]
    os.makedirs(output_dir, exist_ok=True)

    if not to_compute:
        print("All periods already computed. Nothing to do.")
        return {"computed": [], "skipped": skipped, "missing_years": missing_years}

    # These COGs are INTERLEAVE=PIXEL (confirmed via gdalinfo), so a
    # single-band read still has to decompress every band's data at each
    # tile - there's no cheap "just read one band" path. Looping
    # period-outer/year-inner (as an earlier version of this function did)
    # therefore re-decompressed every file once per period (23x redundant
    # work) and effectively hung. Looping year-outer/period-inner instead
    # opens and decompresses each file exactly once, reading all its bands
    # in a single call and accumulating running sum/sum-of-squares/count
    # per period - a single streaming pass, not N_periods passes.
    rows = cols = None
    profile = None
    sum_ = {}
    sumsq = {}
    count = {}

    print(f"Streaming {len(available)} year(s), one pass each ...")
    for i, (year, path) in enumerate(sorted(available.items()), start=1):
        with rasterio.open(path) as src:
            if profile is None:
                profile = src.profile
                rows, cols = src.height, src.width
                for p in to_compute:
                    sum_[p] = np.zeros((rows, cols), dtype=np.float32)
                    sumsq[p] = np.zeros((rows, cols), dtype=np.float32)
                    count[p] = np.zeros((rows, cols), dtype=np.int32)
            # float32: halves memory vs the source float64, plenty of
            # precision for VCI's 0-100 range.
            arr = src.read().astype(np.float32)  # (n_bands, rows, cols) - one decompression pass

        periods_in_year = year_periods[year]
        for band_index, period in enumerate(periods_in_year):
            if period not in to_compute:
                continue
            band = arr[band_index]
            valid = ~np.isnan(band)
            sum_[period][valid] += band[valid]
            sumsq[period][valid] += band[valid] ** 2
            count[period][valid] += 1
        del arr
        print(f"  [{i}/{len(available)}] {year} done")

    computed = []
    for period in to_compute:
        n = count[period]
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = sum_[period] / n
            # sample variance from sum/sumsq (Var = (sumsq - sum^2/n)/(n-1))
            var = (sumsq[period] - (sum_[period] ** 2) / n) / np.maximum(n - 1, 1)
        mean = np.where(n >= 1, mean, np.nan).astype(np.float64)
        std = np.where(n >= 2, np.sqrt(np.maximum(var, 0)), np.nan).astype(np.float64)

        out_path = f"{output_dir}/vci_climatology_period_{period:02d}.tif"
        out_profile = profile.copy()
        out_profile.update(count=3, dtype="float64", nodata=np.nan)
        with rasterio.open(out_path, "w", **out_profile) as dst:
            dst.write(mean, 1)
            dst.write(std, 2)
            dst.write(n.astype(np.float64), 3)

        print(f"period_{period:02d}: max {n.max()} year(s) contributed -> {out_path}")
        computed.append(out_path)

    print(f"Done. Computed {len(computed)}, skipped {len(skipped)}.")
    return {"computed": computed, "skipped": skipped, "missing_years": missing_years}
