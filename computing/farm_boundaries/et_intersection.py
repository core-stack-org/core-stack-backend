"""
Phase 3 — Intersect AET & PET rasters with farm boundary polygons.

Downloads the AET (Actual Evapotranspiration) and PET (Potential
Evapotranspiration) rasters from Google Earth Engine for the tehsil's
bounding box, runs zonal statistics against each farm polygon, computes
MAI (Moisture Adequacy Index = AET/PET), and produces an enhanced
GeoParquet with per-farm monthly AET, PET, MAI values and water stress
indicators.

Data sources:
    AET asset:  projects/corestack-datasets-alpha/assets/datasets/
                et_downscale/aet_aez_<zone>_<year>
    PET asset:  projects/core-stack-dev-3-helper/assets/
                et_downscale/pet_aez_<zone>_<year>
    13 bands:   b1–b12 (monthly in mm/day), b13 (annual)
    Resolution: 30 meters
    NoData:     -9999

Water stress methodology (Drought Manual 2016 / Shivani-Shuvam):
    MAI thresholds:  76–100% no stress, 51–75% mild, 26–50% moderate, 0–25% severe
    Kharif water stress: MAI in moderate/severe range (MAI ≤ 50%) during Jul–Oct
    Frequency:       Return period = N / #kharif_water_stress_years
    Intensity:       Mean MAI over Kharif months in water stress years
    Note: MAI-based classification indicates crop water stress, not drought.
          Drought requires additional indicators (SPI/SPEI, VCI) per GoI definition.

Output:
    data/farm_boundaries/<state>/<district>/<block>/farm_boundaries_et.parquet

Usage (standalone):
    from computing.farm_boundaries.et_intersection import intersect_et_with_farms
    result = intersect_et_with_farms("rajasthan", "jaipur", "sanganer", year=2017)
"""

import logging
import os
import tempfile
import zipfile

import ee
import geopandas as gpd
import numpy as np
import rasterio
import rasterio.features
import requests

from utilities.constants import FARM_BOUNDARIES_PATH

logger = logging.getLogger(__name__)

# ── GEE asset path template ──────────────────────────────────────────────────
AET_ASSET_TEMPLATE = (
    "projects/corestack-datasets-alpha/assets/datasets/"
    "et_downscale/aet_aez_{aez}_{year}"
)
PET_ASSET_TEMPLATE = (
    "projects/core-stack-dev-3-helper/assets/"
    "et_downscale/pet_aez_{aez}_{year}"
)

# AEZ zone mapping — which zone covers which region
# (extend this as more zones become available)
AEZ_ZONE_MAP = {
    "rajasthan": 2,
    "gujarat": 2,
    "punjab": 2,
    "haryana": 2,
}

# Default GEE project for authentication
DEFAULT_GEE_PROJECT = "stackd-conversion123"

# Nodata value used in the AET/PET rasters
AET_NODATA = -9999

# MAI (Moisture Adequacy Index) thresholds — Drought Manual 2016, Table 3.5
# MAI = (AET / PET), expressed as ratio (manual uses percentage)
#   76–100%  (0.76–1.00) → No drought
#   51–75%   (0.51–0.75) → Mild drought
#   26–50%   (0.26–0.50) → Moderate drought
#    0–25%   (0.00–0.25) → Severe drought
MAI_NO_DROUGHT_THRESHOLD = 0.76   # MAI ≥ 0.76 → no drought
MAI_MILD_THRESHOLD = 0.51         # 0.51 ≤ MAI < 0.76 → mild drought
MAI_MODERATE_THRESHOLD = 0.26     # 0.26 ≤ MAI < 0.51 → moderate drought
# Below 0.26 → severe drought

# Kharif water stress: MAI ∈ {moderate, severe} means MAI ≤ 0.50
# Note: This indicates crop water stress, not drought (per professor's clarification)
KHARIF_WATER_STRESS_MAI_THRESHOLD = 0.50

# Kharif season months (July–October), 0-indexed for list access
KHARIF_MONTH_INDICES = [6, 7, 8, 9]  # Jul=6, Aug=7, Sep=8, Oct=9
KHARIF_MONTH_NAMES = ["jul", "aug", "sep", "oct"]

# Month names for column labeling
MONTH_NAMES = [
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
]

CRS = "EPSG:4326"
OUTPUT_PARQUET_NAME = "farm_boundaries_et.parquet"


# ── helpers ───────────────────────────────────────────────────────────────────


def _farm_parquet_path(state: str, district: str, block: str) -> str:
    return os.path.join(
        FARM_BOUNDARIES_PATH, state, district, block, "farm_boundaries.parquet"
    )


def _output_parquet_path(state: str, district: str, block: str) -> str:
    return os.path.join(
        FARM_BOUNDARIES_PATH, state, district, block, OUTPUT_PARQUET_NAME
    )


def _output_tiff_path(state: str, district: str, block: str, year: int, raster_type: str = "aet") -> str:
    return os.path.join(
        FARM_BOUNDARIES_PATH, state, district, block, f"{raster_type}_{year}.tif"
    )


def _get_aez_zone(state: str) -> int:
    """Determine the AEZ zone for a given state."""
    zone = AEZ_ZONE_MAP.get(state)
    if zone is None:
        raise ValueError(
            f"No AEZ zone mapping for state '{state}'. "
            f"Known states: {list(AEZ_ZONE_MAP.keys())}"
        )
    return zone


# ── GEE raster download ──────────────────────────────────────────────────────


def _initialize_gee(gee_project: str):
    """Initialize Google Earth Engine with the given project."""
    try:
        ee.Initialize(project=gee_project)
        logger.info("GEE initialized with project: %s", gee_project)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to initialize GEE with project '{gee_project}'. "
            f"Run 'earthengine authenticate' first. Error: {exc}"
        ) from exc


def _download_single_band(img, band_name, region, tmp_dir, band_idx):
    """Download a single band from GEE as a GeoTIFF file."""
    single = img.select(band_name)
    url = single.getDownloadURL({
        "scale": 30,
        "crs": CRS,
        "region": region,
        "format": "GEO_TIFF",
    })

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    tmp_file = os.path.join(tmp_dir, f"band_{band_idx}.tif")

    # GEE may return a zip or raw tiff
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=tmp_dir) as tmp:
        tmp.write(response.content)
        tmp_path = tmp.name

    if zipfile.is_zipfile(tmp_path):
        with zipfile.ZipFile(tmp_path, "r") as zf:
            tif_files = [f for f in zf.namelist() if f.endswith(".tif")]
            if tif_files:
                zf.extract(tif_files[0], tmp_dir)
                extracted = os.path.join(tmp_dir, tif_files[0])
                os.rename(extracted, tmp_file)
        os.remove(tmp_path)
    else:
        os.rename(tmp_path, tmp_file)

    return tmp_file


def _download_gee_raster(
    asset_path: str,
    tiff_path: str,
    bbox: tuple,
    gee_project: str,
    label: str = "raster",
) -> str:
    """
    Generic: download any GEE image asset to a local multi-band GeoTIFF.

    Downloads each band individually to stay under the 50MB GEE download
    limit, then merges them into a single multi-band GeoTIFF locally.

    Parameters
    ----------
    asset_path : str
        Full GEE asset path (e.g. projects/.../aet_aez_2_2017).
    tiff_path : str
        Local path to save the merged multi-band GeoTIFF.
    bbox : tuple
        (minx, miny, maxx, maxy) in EPSG:4326.
    gee_project : str
        GEE cloud project ID for authentication.
    label : str
        Human-readable label for log messages (e.g. 'AET', 'PET').

    Returns
    -------
    str
        Path to the downloaded multi-band GeoTIFF file.
    """
    # Skip download if already cached
    if os.path.exists(tiff_path):
        logger.info("%s raster already cached at %s — skipping download.", label, tiff_path)
        return tiff_path

    _initialize_gee(gee_project)
    logger.info("Loading GEE asset: %s", asset_path)

    img = ee.Image(asset_path)

    minx, miny, maxx, maxy = bbox
    region = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

    band_names = img.bandNames().getInfo()
    logger.info(
        "Downloading %s: %d bands (bbox=%.3f,%.3f,%.3f,%.3f, 30m)",
        label, len(band_names), minx, miny, maxx, maxy,
    )

    os.makedirs(os.path.dirname(tiff_path), exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp_dir:
        band_files = []
        for idx, band_name in enumerate(band_names, 1):
            logger.info("  [%s] Downloading band %d/%d: %s", label, idx, len(band_names), band_name)
            band_file = _download_single_band(
                img, band_name, region, tmp_dir, idx,
            )
            band_files.append(band_file)

        logger.info("Merging %d bands into single GeoTIFF...", len(band_files))

        with rasterio.open(band_files[0]) as src0:
            meta = src0.meta.copy()
            meta.update(count=len(band_files), dtype="float32")

            with rasterio.open(tiff_path, "w", **meta) as dst:
                for band_idx, band_file in enumerate(band_files, 1):
                    with rasterio.open(band_file) as src:
                        dst.write(src.read(1).astype("float32"), band_idx)

    with rasterio.open(tiff_path) as src:
        logger.info(
            "%s raster saved: %s — %d bands, shape=%s, crs=%s",
            label, tiff_path, src.count, src.shape, src.crs,
        )

    return tiff_path


def _download_aet_raster(state, district, block, year, bbox, gee_project):
    """Download AET raster from GEE for the given tehsil and year."""
    aez = _get_aez_zone(state)
    asset_path = AET_ASSET_TEMPLATE.format(aez=aez, year=year)
    tiff_path = _output_tiff_path(state, district, block, year, "aet")
    return _download_gee_raster(asset_path, tiff_path, bbox, gee_project, label="AET")


def _download_pet_raster(state, district, block, year, bbox, gee_project):
    """Download PET raster from GEE for the given tehsil and year."""
    aez = _get_aez_zone(state)
    asset_path = PET_ASSET_TEMPLATE.format(aez=aez, year=year)
    tiff_path = _output_tiff_path(state, district, block, year, "pet")
    return _download_gee_raster(asset_path, tiff_path, bbox, gee_project, label="PET")


# ── zonal statistics ──────────────────────────────────────────────────────────


def _rasterize_farms(gdf, transform, out_shape):
    """
    Burn all farm polygons into a single labelled raster.

    Each pixel gets the 1-based index of the farm polygon it falls within.
    Pixels that don't overlap any farm get 0.

    Returns
    -------
    np.ndarray
        Integer array of shape *out_shape* with farm labels (0 = background).
    """
    shapes = (
        (geom, idx)
        for idx, geom in enumerate(gdf.geometry, start=1)
    )
    labels = rasterio.features.rasterize(
        shapes,
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype="int32",
        all_touched=True,  # include pixels that even partially overlap
    )
    return labels


def _extract_band_means(labels, band_data, num_farms):
    """
    Given a labelled raster and a band's pixel values, compute the
    mean value per farm label using vectorised NumPy operations.

    Parameters
    ----------
    labels : np.ndarray (int32)
        Farm-label raster (0 = background, 1..N = farm index).
    band_data : np.ndarray (float32)
        AET pixel values for one band.
    num_farms : int
        Total number of farms (N).

    Returns
    -------
    np.ndarray of shape (num_farms,)
        Mean AET for each farm.  NaN where no valid pixels exist.
    """
    # Mask invalid AET values
    valid = (
        ~np.isnan(band_data) &
        ~np.isinf(band_data) &
        (band_data > AET_NODATA) &
        (band_data >= 0)
    )

    # Only work with valid pixels
    valid_labels = labels[valid]
    valid_values = band_data[valid]

    # Accumulate sums and counts per label using np.bincount
    # Labels are 1-based (farm 0 doesn't exist), so index 0 = background
    sums = np.bincount(valid_labels, weights=valid_values, minlength=num_farms + 1)
    counts = np.bincount(valid_labels, minlength=num_farms + 1)

    # Compute mean (farms are at indices 1..N)
    with np.errstate(invalid="ignore", divide="ignore"):
        means = sums[1:] / counts[1:]

    # Farms with zero valid pixels → NaN
    means[counts[1:] == 0] = np.nan

    return means


def _run_zonal_stats(
    gdf: gpd.GeoDataFrame, aet_tiff_path: str, pet_tiff_path: str = None,
) -> gpd.GeoDataFrame:
    """
    Extract per-farm monthly AET (and optionally PET) values using vectorised
    rasterisation, then compute MAI and water stress indicators.

    Approach:
      1. Rasterise ALL farm polygons into a single labelled grid at the
         same resolution as the raster image (30 m).
      2. For each band, compute the mean of all pixels that fall within
         each farm polygon using fast NumPy bincount operations.
      3. If PET is available, compute MAI = AET / PET per month and
         classify water stress using Drought Manual 2016 thresholds.

    New columns added to gdf:
        aet_jan..aet_dec, aet_annual             (AET monthly + annual)
        pet_jan..pet_dec, pet_annual              (PET monthly + annual, if available)
        mai_jan..mai_dec, mai_annual              (MAI = AET/PET, if PET available)
        water_stress_months                       (count of stressed months)
        kharif_water_stress                       (bool: does this farm have Kharif water stress?)
        kharif_mai                                (mean MAI over Kharif months)
    """
    logger.info("Running vectorised zonal statistics for %d farms...", len(gdf))

    # ── AET processing ──────────────────────────────────────────────────
    with rasterio.open(aet_tiff_path) as src:
        num_bands = src.count
        transform = src.transform
        out_shape = (src.height, src.width)
        logger.info(
            "AET raster: %d bands, shape=%s, crs=%s",
            num_bands, out_shape, src.crs,
        )

        # Step 1: Rasterise all farm polygons at once
        logger.info(
            "Rasterising %d farm polygons into a labelled grid (%d×%d)...",
            len(gdf), out_shape[0], out_shape[1],
        )
        labels = _rasterize_farms(gdf, transform, out_shape)
        labelled_count = np.unique(labels[labels > 0]).size
        logger.info(
            "Labelled grid ready: %d/%d farms have at least one pixel.",
            labelled_count, len(gdf),
        )

        # Step 2: Extract mean AET per farm for each band
        aet_monthly_cols = []
        num_farms = len(gdf)

        for band_idx in range(1, min(num_bands, 12) + 1):
            col_name = f"aet_{MONTH_NAMES[band_idx - 1]}"
            logger.info("  Processing AET band %d/%d → %s", band_idx, num_bands, col_name)

            band_data = src.read(band_idx).astype("float32")
            means = _extract_band_means(labels, band_data, num_farms)

            gdf[col_name] = np.round(means, 4)
            aet_monthly_cols.append(col_name)

        # Band 13 = annual average (if present)
        if num_bands >= 13:
            logger.info("  Processing AET band 13/%d → aet_annual", num_bands)
            band_data = src.read(13).astype("float32")
            means = _extract_band_means(labels, band_data, num_farms)
            gdf["aet_annual"] = np.round(means, 4)
        else:
            gdf["aet_annual"] = gdf[aet_monthly_cols].mean(axis=1).round(4)

    # ── PET processing (if available) ────────────────────────────────────
    pet_monthly_cols = []
    has_pet = pet_tiff_path is not None and os.path.exists(pet_tiff_path)

    if has_pet:
        logger.info("Processing PET raster: %s", pet_tiff_path)
        with rasterio.open(pet_tiff_path) as pet_src:
            pet_bands = pet_src.count
            pet_transform = pet_src.transform
            pet_shape = (pet_src.height, pet_src.width)

            # Re-rasterise farms if PET raster has different grid
            if pet_shape != out_shape or pet_transform != transform:
                logger.info("PET grid differs from AET — re-rasterising farms...")
                pet_labels = _rasterize_farms(gdf, pet_transform, pet_shape)
            else:
                pet_labels = labels

            for band_idx in range(1, min(pet_bands, 12) + 1):
                col_name = f"pet_{MONTH_NAMES[band_idx - 1]}"
                logger.info("  Processing PET band %d/%d → %s", band_idx, pet_bands, col_name)

                band_data = pet_src.read(band_idx).astype("float32")
                means = _extract_band_means(pet_labels, band_data, num_farms)

                gdf[col_name] = np.round(means, 4)
                pet_monthly_cols.append(col_name)

            if pet_bands >= 13:
                logger.info("  Processing PET band 13/%d → pet_annual", pet_bands)
                band_data = pet_src.read(13).astype("float32")
                means = _extract_band_means(pet_labels, band_data, num_farms)
                gdf["pet_annual"] = np.round(means, 4)
            else:
                gdf["pet_annual"] = gdf[pet_monthly_cols].mean(axis=1).round(4)
    else:
        logger.info("No PET raster available — skipping MAI computation.")

    # ── MAI and water stress indicators ──────────────────────────────────
    if has_pet and len(pet_monthly_cols) == 12:
        logger.info("Computing MAI (AET/PET) and water stress indicators...")

        # Compute monthly MAI = AET / PET
        mai_monthly_cols = []
        for i, month in enumerate(MONTH_NAMES):
            aet_col = f"aet_{month}"
            pet_col = f"pet_{month}"
            mai_col = f"mai_{month}"

            # Safe division: NaN where PET is 0 or missing
            with np.errstate(invalid="ignore", divide="ignore"):
                mai_values = gdf[aet_col].values / gdf[pet_col].values
            # Replace inf/nan from division by zero
            mai_values = np.where(np.isfinite(mai_values), mai_values, np.nan)

            gdf[mai_col] = np.round(mai_values, 4)
            mai_monthly_cols.append(mai_col)

        # MAI annual = mean of monthly MAIs
        gdf["mai_annual"] = gdf[mai_monthly_cols].mean(axis=1).round(4)

        # Water stress classification per month using Drought Manual 2016, Table 3.5
        # Count months in moderate or severe crop water stress (MAI ≤ 0.50)
        mai_df = gdf[mai_monthly_cols]
        gdf["water_stress_months"] = (
            (mai_df <= KHARIF_WATER_STRESS_MAI_THRESHOLD) & (mai_df.notna())
        ).sum(axis=1).astype(int)

        # Kharif water stress indicator
        # A farm has Kharif water stress if MAI ∈ {moderate, severe}
        # during any Kharif month (Jul, Aug, Sep, Oct)
        # Moderate + Severe = MAI ≤ 50% (ratio ≤ 0.50)
        # Note: This is crop water stress, not drought (per GoI definition)
        kharif_mai_cols = [f"mai_{m}" for m in KHARIF_MONTH_NAMES]
        kharif_mai_df = gdf[kharif_mai_cols]

        # Kharif water stress: any Kharif month has MAI ≤ 0.50 (moderate/severe)
        gdf["kharif_water_stress"] = (
            (kharif_mai_df <= KHARIF_WATER_STRESS_MAI_THRESHOLD) & (kharif_mai_df.notna())
        ).any(axis=1)

        # Mean MAI over Kharif months (for intensity computation in multi-year)
        gdf["kharif_mai"] = kharif_mai_df.mean(axis=1).round(4)

        # Summary logging
        valid = gdf["mai_annual"].notna().sum()
        stress_count = gdf["kharif_water_stress"].sum()
        logger.info(
            "MAI stats complete: %d/%d farms with valid data, "
            "avg MAI=%.3f, Kharif water stress farms=%d, avg stress months=%.1f",
            valid, len(gdf),
            gdf["mai_annual"].mean() if valid > 0 else 0,
            stress_count,
            gdf["water_stress_months"].mean(),
        )
    else:
        # Fallback: no PET available, use simple AET threshold
        STRESS_THRESHOLD = 0.5  # mm/day — placeholder
        monthly_df = gdf[aet_monthly_cols]
        gdf["water_stress_months"] = (
            (monthly_df < STRESS_THRESHOLD) & (monthly_df.notna())
        ).sum(axis=1).astype(int)

        valid = gdf["aet_annual"].notna().sum()
        logger.info(
            "Zonal stats complete (AET only, no PET): %d/%d farms with valid data, "
            "avg annual AET=%.3f mm/day, avg stress months=%.1f",
            valid, len(gdf),
            gdf["aet_annual"].mean() if valid > 0 else 0,
            gdf["water_stress_months"].mean(),
        )

    return gdf


# ── public entry point ────────────────────────────────────────────────────────


def intersect_et_with_farms(
    state: str,
    district: str,
    block: str,
    year: int = 2017,
    gee_project: str = DEFAULT_GEE_PROJECT,
    overwrite: bool = False,
) -> dict:
    """
    Phase 3 pipeline: intersect AET & PET raster data with farm polygons.

    Downloads the AET and PET rasters from Google Earth Engine (clipped to
    the tehsil bounding box), runs zonal statistics for each farm polygon,
    computes MAI (AET/PET), and saves an enhanced GeoParquet with monthly
    AET, PET, MAI values and water stress indicators.

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    year : int
        Year of ET data to use (2017–2024).
    gee_project : str
        GEE cloud project ID for authentication.
    overwrite : bool
        If False and output parquet exists, skip processing.

    Returns
    -------
    dict
        Summary with output path and statistics.
    """
    out_path = _output_parquet_path(state, district, block)

    if not overwrite and os.path.exists(out_path):
        logger.info("ET parquet already exists at %s — skipping Phase 3.", out_path)
        return {"path": out_path, "skipped": True}

    logger.info(
        "Phase 3 — ET intersection for %s/%s/%s (year=%d)",
        state, district, block, year,
    )

    # 1. Load farm boundaries ------------------------------------------------
    farm_path = _farm_parquet_path(state, district, block)
    if not os.path.exists(farm_path):
        raise FileNotFoundError(
            f"Farm boundaries parquet not found at {farm_path}. "
            "Run Phases 1 & 2 first."
        )

    gdf = gpd.read_parquet(farm_path)
    logger.info("Loaded %d farm polygons from %s", len(gdf), farm_path)

    # 2. Download AET raster from GEE ----------------------------------------
    bbox = gdf.total_bounds  # (minx, miny, maxx, maxy)
    logger.info("Farm bounding box: %s", bbox)

    aet_tiff_path = _download_aet_raster(
        state, district, block, year, bbox, gee_project,
    )

    # 3. Download PET raster from GEE ----------------------------------------
    try:
        pet_tiff_path = _download_pet_raster(
            state, district, block, year, bbox, gee_project,
        )
        logger.info("PET raster downloaded successfully.")
    except Exception as exc:
        logger.warning(
            "PET download failed (%s). Proceeding with AET only.", exc
        )
        pet_tiff_path = None

    # 4. Run zonal statistics (AET + PET + MAI) ------------------------------
    gdf = _run_zonal_stats(gdf, aet_tiff_path, pet_tiff_path)

    # 5. Save enhanced parquet -----------------------------------------------
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    gdf.to_parquet(out_path, index=False)
    logger.info("Enhanced parquet saved → %s", out_path)

    # 6. Summary statistics --------------------------------------------------
    summary = {
        "state": state,
        "district": district,
        "block": block,
        "year": year,
        "farm_count": len(gdf),
        "columns": list(gdf.columns),
        "avg_aet_annual": round(gdf["aet_annual"].mean(), 4) if gdf["aet_annual"].notna().any() else None,
        "avg_water_stress_months": round(gdf["water_stress_months"].mean(), 1),
        "has_pet": pet_tiff_path is not None,
        "path": out_path,
    }

    if "mai_annual" in gdf.columns:
        summary["avg_mai_annual"] = round(gdf["mai_annual"].mean(), 4) if gdf["mai_annual"].notna().any() else None
        summary["kharif_water_stress_farms"] = int(gdf["kharif_water_stress"].sum()) if "kharif_water_stress" in gdf.columns else 0

    logger.info("Phase 3 complete: %s", summary)
    return summary


# ── multi-year water stress indicators ────────────────────────────────────────


def compute_multi_year_water_stress(
    state: str,
    district: str,
    block: str,
    start_year: int = 2017,
    end_year: int = 2024,
    gee_project: str = DEFAULT_GEE_PROJECT,
) -> dict:
    """
    Compute cross-year water stress indicators (frequency & intensity)
    as prescribed by the Drought Manual 2016 and approved by the professor.
    Note: MAI-based classification indicates crop water stress, not drought.
    Drought requires SPI/SPEI + VCI in addition to MAI (per GoI definition).

    For each year in [start_year, end_year]:
      - Downloads AET & PET rasters
      - Computes per-farm monthly MAI = AET / PET
      - Classifies Kharif water stress years (any Kharif month MAI ≤ 0.50)

    Then across all years:
      - Frequency:  Return period = N / #kharif_water_stress_years  (NA if 0)
      - Intensity:  Mean MAI over Kharif months in water stress years,
                    averaged across all water stress years

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    start_year, end_year : int
        Year range (inclusive) for multi-year analysis.
    gee_project : str
        GEE cloud project ID for authentication.

    Returns
    -------
    dict
        Summary with output path, indicators, and statistics.
    """
    out_path = os.path.join(
        FARM_BOUNDARIES_PATH, state, district, block,
        "farm_boundaries_water_stress.parquet",
    )

    logger.info(
        "Multi-year water stress analysis for %s/%s/%s (%d–%d)",
        state, district, block, start_year, end_year,
    )

    # 1. Load farm boundaries ------------------------------------------------
    farm_path = _farm_parquet_path(state, district, block)
    if not os.path.exists(farm_path):
        raise FileNotFoundError(
            f"Farm boundaries parquet not found at {farm_path}. "
            "Run Phases 1 & 2 first."
        )

    base_gdf = gpd.read_parquet(farm_path)
    num_farms = len(base_gdf)
    logger.info("Loaded %d farm polygons.", num_farms)

    bbox = base_gdf.total_bounds
    years = list(range(start_year, end_year + 1))
    N = len(years)

    # Per-farm accumulators: track Kharif water stress years and their MAI
    kharif_stress_count = np.zeros(num_farms, dtype=int)
    kharif_mai_sum_stress = np.zeros(num_farms, dtype=float)
    years_processed = 0

    # 2. Process each year ---------------------------------------------------
    for year in years:
        logger.info("── Processing year %d ──", year)
        try:
            aet_tiff = _download_aet_raster(
                state, district, block, year, bbox, gee_project,
            )
            pet_tiff = _download_pet_raster(
                state, district, block, year, bbox, gee_project,
            )
        except Exception as exc:
            logger.warning("Skipping year %d — download failed: %s", year, exc)
            continue

        # Run zonal stats on a copy (we only need MAI columns)
        year_gdf = base_gdf.copy()
        year_gdf = _run_zonal_stats(year_gdf, aet_tiff, pet_tiff)

        if "kharif_water_stress" not in year_gdf.columns:
            logger.warning("Year %d: MAI not computed (PET missing?). Skipping.", year)
            continue

        years_processed += 1

        # Accumulate water stress counts and intensity values
        is_drought = year_gdf["kharif_water_stress"].values.astype(bool)
        kharif_mai_values = year_gdf["kharif_mai"].values

        kharif_stress_count += is_drought.astype(int)

        # Add Kharif MAI only for water stress years (for intensity averaging)
        stress_mask = is_drought & np.isfinite(kharif_mai_values)
        kharif_mai_sum_stress[stress_mask] += kharif_mai_values[stress_mask]

    # 3. Compute cross-year indicators ---------------------------------------
    result_gdf = base_gdf.copy()

    result_gdf["total_years"] = years_processed
    result_gdf["kharif_water_stress_years"] = kharif_stress_count

    # Frequency: Return period = N / #kharif_water_stress_years
    with np.errstate(invalid="ignore", divide="ignore"):
        return_period = years_processed / kharif_stress_count.astype(float)
    return_period[kharif_stress_count == 0] = np.nan  # NA for zero water stress years
    result_gdf["return_period_years"] = np.round(return_period, 2)

    # Intensity: Mean MAI over Kharif months in water stress years
    with np.errstate(invalid="ignore", divide="ignore"):
        stress_intensity = kharif_mai_sum_stress / kharif_stress_count.astype(float)
    stress_intensity[kharif_stress_count == 0] = np.nan
    result_gdf["water_stress_intensity_mai"] = np.round(stress_intensity, 4)

    # 4. Save water stress parquet -------------------------------------------
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    result_gdf.to_parquet(out_path, index=False)
    logger.info("Multi-year water stress parquet saved → %s", out_path)

    # 5. Summary -------------------------------------------------------------
    valid = result_gdf["return_period_years"].notna().sum()
    summary = {
        "state": state,
        "district": district,
        "block": block,
        "years_range": f"{start_year}–{end_year}",
        "years_processed": years_processed,
        "farm_count": num_farms,
        "farms_with_water_stress": int((kharif_stress_count > 0).sum()),
        "avg_return_period": round(result_gdf["return_period_years"].mean(), 2) if valid > 0 else None,
        "avg_water_stress_intensity": round(result_gdf["water_stress_intensity_mai"].mean(), 4) if valid > 0 else None,
        "columns": list(result_gdf.columns),
        "path": out_path,
    }
    logger.info("Multi-year water stress analysis complete: %s", summary)
    return summary
