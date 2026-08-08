"""
Phase 3 — Intersect AET & PET rasters with farm boundary polygons.

Reads locally stored COG (Cloud Optimized GeoTIFF) rasters for AET and PET,
runs zonal statistics against each farm polygon, computes MAI (Moisture
Adequacy Index = AET/PET), and produces three parquets per the core-lens schema:

    farm_static.parquet    — one row per farm (geometry + static properties)
    farm_annual.parquet    — one row per farm per year (annual ET metrics)
    farm_monthly.parquet   — one row per farm per month (date, AET, PET, MAI)

Data sources:
    Local COG rasters at LOCAL_ET_RASTERS_PATH:
        merge_AET_<aez>_<year>_cog.tif   (13 bands: b1-b12 monthly mm/day, b13 annual)
        merge_PET_<aez>_<year>_cog.tif   (same structure)
    Resolution: 30 metres | NoData: -9999 | CRS: EPSG:4326

Water stress methodology (aligned with Shuvam Chakraborty / ET Applications):
    MAI = AET / PET  (ratio, per pixel, only where both AET & PET are valid and PET > 0)
    Moderate kharif stress : mean kharif MAI <= 0.50
    Severe kharif stress   : mean kharif MAI <= 0.25
    Kharif months          : July, August, September, October

Missing data protocol (mirrors Shuvam's divide_where_valid approach):
    - Pixel-level : MAI = NaN if AET is NaN, PET is NaN, or PET = 0
    - Farm-level  : column = NaN if the farm has zero valid pixels for that band
    - Annual MAI  : mean of all valid monthly MAI values (NaN months excluded)
    - No imputation is performed on missing farms or missing months.
"""

import logging
import os
from datetime import date

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rasterio.features
import rasterio.windows

from utilities.constants import FARM_BOUNDARIES_PATH, LOCAL_ET_RASTERS_PATH

logger = logging.getLogger(__name__)

# AEZ zone mapping — extend as more zones are added
AEZ_ZONE_MAP = {
    "rajasthan":   4,
    "gujarat":     4,
    "punjab":      4,
    "haryana":     4,
    "maharashtra": 6,
    "madhya pradesh": 6,
    "jharkhand":   7,
    "bihar":       7,
    "uttar pradesh": 5,
}

AET_NODATA = -9999

# MAI thresholds — aligned with Shuvam Chakraborty / ET Applications (GEE pipeline)
# Moderate stress : MAI <= 0.50  (farm is water-stressed but not severely)
# Severe stress   : MAI <= 0.25  (farm is severely water-stressed)
MAI_MODERATE_THRESHOLD = 0.50
MAI_SEVERE_THRESHOLD   = 0.25

# Kept for backward compatibility
KHARIF_WATER_STRESS_MAI_THRESHOLD = MAI_MODERATE_THRESHOLD

KHARIF_MONTH_NAMES = ["jul", "aug", "sep", "oct"]

# Calendar month names (used for column naming)
MONTH_NAMES = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]

# Rasters use crop-year band ordering: band 1 = July, band 2 = August,
# ..., band 6 = December (year Y), band 7 = January, ..., band 12 = June (year Y+1).
# This list maps band index 0..11 to the correct calendar month number 1..12.
CROP_YEAR_BAND_TO_MONTH = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]

# Reverse: calendar month number -> column index in MONTH_NAMES
_MONTH_NUM_TO_NAME = {
    1: "jan", 2: "feb", 3: "mar",  4: "apr",  5: "may",  6: "jun",
    7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec",
}

CRS = "EPSG:4326"


# ── path helpers ───────────────────────────────────────────────────────────────

def _block_dir(state, district, block):
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block)

def _farm_parquet_path(state, district, block):
    return os.path.join(_block_dir(state, district, block), "farm_boundaries.parquet")

def _static_parquet_path(state, district, block):
    return os.path.join(_block_dir(state, district, block), "farm_static.parquet")

def _annual_parquet_path(state, district, block):
    return os.path.join(_block_dir(state, district, block), "farm_annual.parquet")

def _monthly_parquet_path(state, district, block):
    return os.path.join(_block_dir(state, district, block), "farm_monthly.parquet")

def _local_aet_path(aez, year):
    return os.path.join(LOCAL_ET_RASTERS_PATH, f"merge_AET_{aez}_{year}_cog.tif")

def _local_pet_path(aez, year):
    return os.path.join(LOCAL_ET_RASTERS_PATH, f"merge_PET_{aez}_{year}_cog.tif")

def _get_aez_zone(state):
    zone = AEZ_ZONE_MAP.get(state)
    if zone is None:
        raise ValueError(
            f"No AEZ zone mapping for state '{state}'. Known: {list(AEZ_ZONE_MAP.keys())}"
        )
    return zone


# ── local raster reading ───────────────────────────────────────────────────────

def _read_raster_clipped(raster_path, bbox):
    """
    Read a local COG raster clipped to bbox using windowed reading.
    Converts the raster's nodata value (-9999 per spec, confirmed via src.nodata)
    to NaN immediately so all downstream code works cleanly with NaN semantics.

    Returns
    -------
    data      : np.ndarray  shape (bands, height, width), float32
    transform : affine transform for the clipped window
    """
    minx, miny, maxx, maxy = bbox
    with rasterio.open(raster_path) as src:
        window    = rasterio.windows.from_bounds(minx, miny, maxx, maxy, src.transform)
        transform = rasterio.windows.transform(window, src.transform)
        data      = src.read(window=window).astype("float32")

        # Resolve nodata: use raster metadata first, fall back to AET_NODATA (-9999)
        nodata_val = float(src.nodata) if src.nodata is not None else float(AET_NODATA)
        n_nodata   = int(np.sum(data == nodata_val))
        if n_nodata > 0:
            logger.debug("%s: masking %d nodata pixels (value=%.0f)",
                         os.path.basename(raster_path), n_nodata, nodata_val)

        # Convert nodata sentinel AND any residual AET_NODATA values to NaN
        data[data == nodata_val] = np.nan
        data[data <= float(AET_NODATA)] = np.nan   # belt-and-suspenders for -9999 variants

    return data, transform


# ── zonal statistics ───────────────────────────────────────────────────────────

def _rasterize_farms(gdf, transform, out_shape):
    shapes = (
        (geom, idx)
        for idx, geom in enumerate(gdf.geometry, start=1)
    )
    return rasterio.features.rasterize(
        shapes,
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype="int32",
        all_touched=True,
    )


def _extract_band_means(labels, band_data, num_farms):
    """
    Compute per-farm mean of band_data, ignoring NaN/Inf and negative values.
    Nodata pixels are already NaN at this point (converted in _read_raster_clipped).
    Farms with zero valid pixels get NaN (not imputed).
    """
    # Valid = finite, non-negative (AET and PET are always >= 0 physically)
    valid        = np.isfinite(band_data) & (band_data >= 0)
    valid_labels = labels[valid]
    valid_values = band_data[valid]

    sums   = np.bincount(valid_labels, weights=valid_values, minlength=num_farms + 1)
    counts = np.bincount(valid_labels,                       minlength=num_farms + 1)

    with np.errstate(invalid="ignore", divide="ignore"):
        means = sums[1:] / counts[1:]

    means[counts[1:] == 0] = np.nan   # farms with no valid pixels → NaN
    return means


# ── temporal gap-filling ───────────────────────────────────────────────────────
# Mirrors Shuvam Chakraborty's fill_monthly_collection() in ET_Applications/helper.py.
# Crop year: July (agri-month 1) → June (agri-month 12).
# Rules:
#   July  (agri_month 1)  → neighbour: August only  (no backward crossing crop-year start)
#   June  (agri_month 12) → neighbour: May only     (no forward crossing crop-year end)
#   All others            → previous and next calendar month (±1 month)
# NaN farm-months with no valid neighbour remain NaN.

# Calendar-month index (0=Jan … 11=Dec) → list of neighbour indices
_GAP_FILL_NEIGHBOURS: dict = {
    0:  [11, 1],   # Jan: Dec, Feb
    1:  [0,  2],   # Feb: Jan, Mar
    2:  [1,  3],   # Mar: Feb, Apr
    3:  [2,  4],   # Apr: Mar, May
    4:  [3,  5],   # May: Apr, Jun
    5:  [4],       # Jun: May only  (crop-year end  — no forward crossing)
    6:  [7],       # Jul: Aug only  (crop-year start — no backward crossing)
    7:  [6,  8],   # Aug: Jul, Sep
    8:  [7,  9],   # Sep: Aug, Oct
    9:  [8, 10],   # Oct: Sep, Nov
    10: [9, 11],   # Nov: Oct, Dec
    11: [10, 0],   # Dec: Nov, Jan
}


def _gap_fill_monthly_farms(monthly_matrix: np.ndarray) -> np.ndarray:
    """
    Gap-fill a (n_farms × 12) monthly matrix following Shuvam's crop-year rules.

    Parameters
    ----------
    monthly_matrix : np.ndarray, shape (n_farms, 12)
        Columns are calendar months Jan–Dec (indices 0–11).
        NaN = missing / nodata.

    Returns
    -------
    filled : np.ndarray, same shape.
        NaN cells replaced with the nanmean of valid neighbours.
        Cells with no valid neighbour remain NaN.
    """
    filled = monthly_matrix.copy()

    for m, neighbours in _GAP_FILL_NEIGHBOURS.items():
        missing = np.isnan(filled[:, m])
        if not missing.any():
            continue
        neighbour_vals = np.stack([filled[:, n] for n in neighbours], axis=1)
        fill_vals      = np.nanmean(neighbour_vals, axis=1)
        can_fill       = missing & np.isfinite(fill_vals)
        filled[can_fill, m] = fill_vals[can_fill]

    return filled


def _run_zonal_stats(gdf, aet_data, aet_transform, pet_data=None, pet_transform=None):
    """
    Compute per-farm monthly AET, PET, MAI from pre-loaded raster arrays.
    Adds wide-format columns (aet_jan..aet_dec, pet_jan..pet_dec, mai_jan..mai_dec,
    aet_annual, pet_annual, mai_annual, kharif_mai, kharif_water_stress) to gdf.
    """
    num_farms = len(gdf)
    out_shape  = (aet_data.shape[1], aet_data.shape[2])

    logger.info("Rasterising %d farm polygons (%d×%d grid)...", num_farms, *out_shape)
    labels = _rasterize_farms(gdf, aet_transform, out_shape)
    logger.info(
        "%d/%d farms have at least one pixel.",
        np.unique(labels[labels > 0]).size, num_farms,
    )

    # AET monthly — map each crop-year band to its correct calendar month column
    # Band 0=Jul, 1=Aug, ..., 5=Dec, 6=Jan, ..., 11=Jun  (CROP_YEAR_BAND_TO_MONTH)
    aet_monthly_cols = [f"aet_{m}" for m in MONTH_NAMES]   # ordered jan..dec
    num_aet_bands = aet_data.shape[0]
    for band_idx in range(min(num_aet_bands, 12)):
        cal_month = CROP_YEAR_BAND_TO_MONTH[band_idx]
        col = f"aet_{_MONTH_NUM_TO_NAME[cal_month]}"
        gdf[col] = np.round(_extract_band_means(labels, aet_data[band_idx], num_farms), 4)

    # ── Temporal gap-fill AET — matrix in calendar order (jan=col0..dec=col11) ──
    aet_matrix  = gdf[aet_monthly_cols].values.astype("float64")
    aet_filled  = _gap_fill_monthly_farms(aet_matrix)
    n_filled_aet = int(np.sum(np.isnan(aet_matrix) & np.isfinite(aet_filled)))
    logger.info("Gap-fill AET: filled %d farm-month NaN values.", n_filled_aet)
    for i, col in enumerate(aet_monthly_cols):
        gdf[col] = np.round(aet_filled[:, i], 4)

    if num_aet_bands >= 13:
        gdf["aet_annual"] = np.round(_extract_band_means(labels, aet_data[12], num_farms), 4)
    else:
        gdf["aet_annual"] = gdf[aet_monthly_cols].mean(axis=1).round(4)

    # PET monthly
    pet_monthly_cols = [f"pet_{m}" for m in MONTH_NAMES]
    if pet_data is not None:
        pet_out_shape = (pet_data.shape[1], pet_data.shape[2])
        pet_labels = (
            _rasterize_farms(gdf, pet_transform, pet_out_shape)
            if (pet_out_shape != out_shape or pet_transform != aet_transform)
            else labels
        )
        num_pet_bands = pet_data.shape[0]
        for band_idx in range(min(num_pet_bands, 12)):
            cal_month = CROP_YEAR_BAND_TO_MONTH[band_idx]
            col = f"pet_{_MONTH_NUM_TO_NAME[cal_month]}"
            gdf[col] = np.round(_extract_band_means(pet_labels, pet_data[band_idx], num_farms), 4)

        # ── Temporal gap-fill PET ─────────────────────────────────────────────
        pet_matrix  = gdf[pet_monthly_cols].values.astype("float64")
        pet_filled  = _gap_fill_monthly_farms(pet_matrix)
        n_filled_pet = int(np.sum(np.isnan(pet_matrix) & np.isfinite(pet_filled)))
        logger.info("Gap-fill PET: filled %d farm-month NaN values.", n_filled_pet)
        for i, col in enumerate(pet_monthly_cols):
            gdf[col] = np.round(pet_filled[:, i], 4)

        if num_pet_bands >= 13:
            gdf["pet_annual"] = np.round(_extract_band_means(pet_labels, pet_data[12], num_farms), 4)
        else:
            gdf["pet_annual"] = gdf[pet_monthly_cols].mean(axis=1).round(4)

    # MAI + water stress
    if len(pet_monthly_cols) == 12:
        mai_monthly_cols = []
        for month in MONTH_NAMES:
            col = f"mai_{month}"
            aet_vals = gdf[f"aet_{month}"].values
            pet_vals = gdf[f"pet_{month}"].values
            with np.errstate(invalid="ignore", divide="ignore"):
                v = aet_vals / pet_vals
            # MAI is physically bounded to [0, 1]: AET cannot exceed PET.
            # Values > 1 indicate raster misalignment or model artifacts → cap at 1.
            n_invalid = int(np.sum(np.isfinite(v) & (v > 1)))
            if n_invalid > 0:
                logger.warning(
                    "MAI[%s]: %d farms have MAI > 1 (raster artifact) — capped at 1.0",
                    month, n_invalid,
                )
            # Set to NaN where not finite, cap valid values to [0, 1]
            v = np.where(np.isfinite(v), np.clip(v, 0.0, 1.0), np.nan)
            gdf[col] = np.round(v, 4)
            mai_monthly_cols.append(col)

        gdf["mai_annual"] = gdf[mai_monthly_cols].mean(axis=1).round(4)

        kharif_cols = [f"mai_{m}" for m in KHARIF_MONTH_NAMES]
        kharif_df   = gdf[kharif_cols]
        gdf["kharif_mai"] = kharif_df.mean(axis=1).round(4)

        # Moderate stress: any kharif month with MAI <= 0.50
        gdf["kharif_water_stress"] = (
            (kharif_df <= MAI_MODERATE_THRESHOLD) & kharif_df.notna()
        ).any(axis=1)

        # Severe stress: any kharif month with MAI <= 0.25
        gdf["kharif_severe_stress"] = (
            (kharif_df <= MAI_SEVERE_THRESHOLD) & kharif_df.notna()
        ).any(axis=1)

        n_nan   = int(gdf["mai_annual"].isna().sum())
        n_valid = int(gdf["mai_annual"].notna().sum())
        logger.info(
            "MAI complete: avg_annual=%.4f | kharif_stress=%d | severe=%d | nan_farms=%d | valid_farms=%d",
            gdf["mai_annual"].mean() if n_valid > 0 else float("nan"),
            int(gdf["kharif_water_stress"].sum()),
            int(gdf["kharif_severe_stress"].sum()),
            n_nan, n_valid,
        )
    else:
        logger.warning("PET not available — MAI not computed.")

    return gdf


# ── output writers ─────────────────────────────────────────────────────────────

def _save_static_parquet(gdf, state, district, block):
    """
    Write farm_static.parquet — one row per farm, geometry + static properties.
    Skips if file already exists (static data never changes).
    """
    out_path = _static_parquet_path(state, district, block)
    if os.path.exists(out_path):
        logger.info("farm_static.parquet already exists — skipping.")
        return out_path

    keep = ["farm_id", "farm_uid", "cell_token", "alu_type",
            "class_confidence", "capture_date", "geometry"]
    static = gdf[[c for c in keep if c in gdf.columns]].copy()
    static.insert(0, "state",    state)
    static.insert(0, "district", district)
    static.insert(0, "tehsil",   block)
    if "area_m2" in gdf.columns:
        static["area_in_ha"] = (gdf["area_m2"] / 10_000).round(4)

    # Bounding box struct
    static["bbox"] = static.geometry.apply(
        lambda g: {
            "xmin": round(g.bounds[0], 6), "ymin": round(g.bounds[1], 6),
            "xmax": round(g.bounds[2], 6), "ymax": round(g.bounds[3], 6),
        }
    )

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    static.to_parquet(out_path, index=False)
    logger.info("farm_static.parquet saved → %s  (%d farms)", out_path, len(static))
    return out_path


def _save_annual_parquet(gdf, state, district, block, year):
    """
    Append one year of annual ET metrics to farm_annual.parquet.
    Replaces any existing rows for the same year (idempotent).
    """
    out_path = _annual_parquet_path(state, district, block)

    keep = ["farm_id", "aet_annual", "pet_annual",
            "mai_annual", "kharif_mai", "kharif_water_stress", "kharif_severe_stress"]
    annual = gdf[[c for c in keep if c in gdf.columns]].copy()
    annual["tehsil"]   = block
    annual["district"] = district
    annual["state"]    = state
    annual["year"]     = int(year)
    if "area_m2" in gdf.columns:
        annual["area_in_ha"] = (gdf["area_m2"] / 10_000).round(4)

    col_order = ["farm_id", "tehsil", "district", "state", "area_in_ha", "year",
                 "aet_annual", "pet_annual", "mai_annual", "kharif_mai", 
                 "kharif_water_stress", "kharif_severe_stress"]
    annual = annual[[c for c in col_order if c in annual.columns]]

    if os.path.exists(out_path):
        existing = pd.read_parquet(out_path)
        existing = existing[existing["year"] != int(year)]
        combined = pd.concat([existing, annual], ignore_index=True)
    else:
        combined = annual

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    combined.to_parquet(out_path, index=False)
    logger.info(
        "farm_annual.parquet updated → %s  (%d total rows)", out_path, len(combined)
    )
    return out_path


def _save_monthly_parquet(gdf, state, district, block, year):
    """
    Melt monthly AET/PET/MAI wide columns into long format and
    append to farm_monthly.parquet.  One row per farm per month.
    """
    out_path = _monthly_parquet_path(state, district, block)

    farm_ids  = gdf["farm_id"].values if "farm_id" in gdf.columns else np.arange(len(gdf))
    area_vals = (gdf["area_m2"] / 10_000).round(4).values if "area_m2" in gdf.columns else np.full(len(gdf), np.nan)

    rows = []
    for month in MONTH_NAMES:   # month = 'jan','feb',...'dec' (calendar order)
        month_num = list(_MONTH_NUM_TO_NAME.keys())[
            list(_MONTH_NUM_TO_NAME.values()).index(month)
        ]  # calendar month number 1-12
        # Months Jul-Dec belong to `year`; Jan-Jun belong to the next calendar year
        # (crop year starting July spans two calendar years)
        cal_year = int(year) if month_num >= 7 else int(year) + 1
        rows.append(pd.DataFrame({
            "farm_id":   farm_ids,
            "tehsil":    block,
            "district":  district,
            "state":     state,
            "area_in_ha": area_vals,
            "year":      int(year),
            "date":      date(cal_year, month_num, 1),
            "aet":       gdf[f"aet_{month}"].values if f"aet_{month}" in gdf.columns else np.nan,
            "pet":       gdf[f"pet_{month}"].values if f"pet_{month}" in gdf.columns else np.nan,
            "mai":       gdf[f"mai_{month}"].values if f"mai_{month}" in gdf.columns else np.nan,
        }))

    monthly = pd.concat(rows, ignore_index=True)
    monthly["date"] = pd.to_datetime(monthly["date"])

    if os.path.exists(out_path):
        existing = pd.read_parquet(out_path)
        existing = existing[existing["year"] != int(year)]
        combined = pd.concat([existing, monthly], ignore_index=True)
    else:
        combined = monthly

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    combined.to_parquet(out_path, index=False)
    logger.info(
        "farm_monthly.parquet updated → %s  (%d total rows)", out_path, len(combined)
    )
    return out_path


# ── main entry point ───────────────────────────────────────────────────────────

def intersect_et_with_farms(
    state: str,
    district: str,
    block: str,
    year: int = 2018,
    overwrite: bool = False,
) -> dict:
    """
    Phase 3: intersect local AET/PET COG rasters with farm polygons.

    Reads local rasters from LOCAL_ET_RASTERS_PATH, runs vectorised zonal
    statistics, computes MAI, and writes/updates the three core-lens parquets:
        farm_static.parquet, farm_annual.parquet, farm_monthly.parquet

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    year : int
        Year of ET data to process (e.g. 2018).
    overwrite : bool
        Re-process even if this year's data already exists.

    Returns
    -------
    dict  summary with paths and key statistics.
    """
    # Skip if year already in annual parquet
    annual_path = _annual_parquet_path(state, district, block)
    if not overwrite and os.path.exists(annual_path):
        existing = pd.read_parquet(annual_path)
        if "year" in existing.columns and int(year) in existing["year"].values:
            logger.info("Year %d already processed — skipping Phase 3.", year)
            return {"skipped": True, "year": year, "path": annual_path}

    logger.info(
        "Phase 3 — ET intersection: %s/%s/%s  year=%d", state, district, block, year
    )

    # 1. Load farm boundaries
    farm_path = _farm_parquet_path(state, district, block)
    if not os.path.exists(farm_path):
        raise FileNotFoundError(
            f"Farm boundaries parquet not found at {farm_path}. "
            "Run Phases 1 & 2 first."
        )
    gdf = gpd.read_parquet(farm_path)
    logger.info("Loaded %d farm polygons.", len(gdf))

    bbox = gdf.total_bounds   # (minx, miny, maxx, maxy)
    aez  = _get_aez_zone(state)

    # 2. Load local AET raster
    aet_path = _local_aet_path(aez, year)
    if not os.path.exists(aet_path):
        raise FileNotFoundError(
            f"Local AET raster not found: {aet_path}\n"
            f"Place the file at: {LOCAL_ET_RASTERS_PATH}/merge_AET_{aez}_{year}_cog.tif"
        )
    logger.info("Reading local AET raster: %s", aet_path)
    aet_data, aet_transform = _read_raster_clipped(aet_path, bbox)
    logger.info("AET loaded: %d bands, shape=%s", aet_data.shape[0], aet_data.shape[1:])

    # 3. Load local PET raster (optional)
    pet_data, pet_transform = None, None
    pet_path = _local_pet_path(aez, year)
    if os.path.exists(pet_path):
        logger.info("Reading local PET raster: %s", pet_path)
        pet_data, pet_transform = _read_raster_clipped(pet_path, bbox)
        logger.info("PET loaded: %d bands, shape=%s", pet_data.shape[0], pet_data.shape[1:])
    else:
        logger.warning(
            "Local PET raster not found at %s — MAI will not be computed.", pet_path
        )

    # 4. Zonal statistics
    gdf = _run_zonal_stats(gdf, aet_data, aet_transform, pet_data, pet_transform)

    # 5. Write 3-file schema
    static_path  = _save_static_parquet(gdf, state, district, block)
    annual_path  = _save_annual_parquet(gdf, state, district, block, year)
    monthly_path = _save_monthly_parquet(gdf, state, district, block, year)

    summary = {
        "state": state, "district": district, "block": block, "year": year,
        "farm_count": len(gdf),
        "paths": {
            "static":  static_path,
            "annual":  annual_path,
            "monthly": monthly_path,
        },
    }
    if "aet_annual" in gdf.columns and gdf["aet_annual"].notna().any():
        summary["avg_aet_annual"] = round(float(gdf["aet_annual"].mean()), 4)
    if "mai_annual" in gdf.columns and gdf["mai_annual"].notna().any():
        summary["avg_mai_annual"] = round(float(gdf["mai_annual"].mean()), 4)
    if "kharif_water_stress" in gdf.columns:
        summary["kharif_stress_farms"] = int(gdf["kharif_water_stress"].sum())

    logger.info("Phase 3 complete: %s", summary)
    return summary


# ── multi-year analysis ────────────────────────────────────────────────────────

def compute_multi_year_water_stress(
    state: str,
    district: str,
    block: str,
    start_year: int = 2017,
    end_year: int = 2024,
) -> dict:
    """
    Run Phase 3 for each year in [start_year, end_year] and compute
    cross-year frequency and intensity indicators:

        kharif_water_stress_years  — number of years with kharif stress
        return_period_years        — N / stress_years  (NaN if 0 stress years)
        water_stress_intensity_mai — mean kharif MAI over stress years

    The annual parquet is updated with per-year rows.
    A separate farm_water_stress_summary.parquet is written with the
    cross-year indicators appended to the static columns.

    Returns
    -------
    dict  summary with paths and aggregate statistics.
    """
    logger.info(
        "Multi-year water stress: %s/%s/%s  %d–%d",
        state, district, block, start_year, end_year,
    )

    farm_path = _farm_parquet_path(state, district, block)
    if not os.path.exists(farm_path):
        raise FileNotFoundError(f"Farm parquet not found: {farm_path}")

    base_gdf  = gpd.read_parquet(farm_path)
    num_farms = len(base_gdf)
    bbox      = base_gdf.total_bounds
    aez       = _get_aez_zone(state)
    years     = list(range(start_year, end_year + 1))

    kharif_stress_count    = np.zeros(num_farms, dtype=int)
    kharif_mai_sum_stress  = np.zeros(num_farms, dtype=float)
    years_processed        = 0

    for year in years:
        logger.info("── Processing year %d ──", year)

        aet_path = _local_aet_path(aez, year)
        pet_path = _local_pet_path(aez, year)

        if not os.path.exists(aet_path):
            logger.warning("AET raster missing for year %d — skipping.", year)
            continue

        try:
            aet_data, aet_transform = _read_raster_clipped(aet_path, bbox)
            pet_data, pet_transform = (
                _read_raster_clipped(pet_path, bbox)
                if os.path.exists(pet_path)
                else (None, None)
            )
        except Exception as exc:
            logger.warning("Year %d: raster read failed — %s", year, exc)
            continue

        year_gdf = base_gdf.copy()
        year_gdf = _run_zonal_stats(year_gdf, aet_data, aet_transform, pet_data, pet_transform)

        if "kharif_water_stress" not in year_gdf.columns:
            logger.warning("Year %d: MAI not computed (PET missing?). Skipping.", year)
            continue

        # Save this year into annual + monthly parquets
        _save_annual_parquet(year_gdf, state, district, block, year)
        _save_monthly_parquet(year_gdf, state, district, block, year)

        years_processed += 1
        is_stress          = year_gdf["kharif_water_stress"].values.astype(bool)
        kharif_mai_values  = year_gdf["kharif_mai"].values
        kharif_stress_count += is_stress.astype(int)
        stress_mask = is_stress & np.isfinite(kharif_mai_values)
        kharif_mai_sum_stress[stress_mask] += kharif_mai_values[stress_mask]

    # Cross-year indicators
    result_gdf = base_gdf.copy()
    result_gdf["total_years"]              = years_processed
    result_gdf["kharif_water_stress_years"] = kharif_stress_count

    with np.errstate(invalid="ignore", divide="ignore"):
        rp = years_processed / kharif_stress_count.astype(float)
    rp[kharif_stress_count == 0] = np.nan
    result_gdf["return_period_years"] = np.round(rp, 2)

    with np.errstate(invalid="ignore", divide="ignore"):
        intensity = kharif_mai_sum_stress / kharif_stress_count.astype(float)
    intensity[kharif_stress_count == 0] = np.nan
    result_gdf["water_stress_intensity_mai"] = np.round(intensity, 4)

    # Save summary parquet
    out_path = os.path.join(
        _block_dir(state, district, block), "farm_water_stress_summary.parquet"
    )
    # keep static cols + cross-year indicators, no geometry duplication
    summary_cols = [c for c in result_gdf.columns if not c.startswith(("aet_", "pet_", "mai_"))]
    result_gdf[summary_cols].to_parquet(out_path, index=False)
    logger.info("Water stress summary parquet saved → %s", out_path)

    valid = result_gdf["return_period_years"].notna().sum()
    summary = {
        "state": state, "district": district, "block": block,
        "years_range": f"{start_year}–{end_year}",
        "years_processed": years_processed,
        "farm_count": num_farms,
        "farms_with_any_stress": int((kharif_stress_count > 0).sum()),
        "avg_return_period": round(float(result_gdf["return_period_years"].mean()), 2) if valid > 0 else None,
        "avg_stress_intensity_mai": round(float(result_gdf["water_stress_intensity_mai"].mean()), 4) if valid > 0 else None,
        "path": out_path,
    }
    logger.info("Multi-year analysis complete: %s", summary)
    return summary
