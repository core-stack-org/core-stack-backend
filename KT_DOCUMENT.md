# Farm ET Pipeline — Knowledge Transfer Document

## Overview

This document covers the **Farm-level ET (Evapotranspiration) Pipeline** — the system that intersects AET/PET rasters with farm boundary polygons to compute **Moisture Adequacy Index (MAI)** and kharif water stress indicators per farm.

The pipeline has been validated against Shuvam Chakraborty's GEE-based drought analysis rasters. This document explains the architecture, data flow, recent bug fixes, and how to run/extend the pipeline.

---

## Table of Contents

1. [Architecture & Data Flow](#1-architecture--data-flow)
2. [Directory Structure](#2-directory-structure)
3. [Raster Band Ordering (Critical)](#3-raster-band-ordering-critical)
4. [Pipeline Execution](#4-pipeline-execution)
5. [Key Functions in `et_intersection.py`](#5-key-functions-in-et_intersectionpy)
6. [Missing Data Handling & Gap-Fill](#6-missing-data-handling--gap-fill)
7. [Output Schema](#7-output-schema)
8. [Validation Against Shuvam's Rasters](#8-validation-against-shuvams-rasters)
9. [Recent Bug Fixes & Changes](#9-recent-bug-fixes--changes)
10. [How to Extend to New Regions/Years](#10-how-to-extend-to-new-regionsyears)
11. [Known Limitations](#11-known-limitations)

---

## 1. Architecture & Data Flow

```
Input Rasters (COG GeoTIFF)                   Farm Boundaries (GeoParquet)
  merge_AET_<aez>_<year>_cog.tif                farm_boundaries.parquet
  merge_PET_<aez>_<year>_cog.tif                  ├── farm_id
     ├── 13 bands (crop-year order)               ├── geometry (polygon)
     ├── Band 1-12: monthly mm/day                └── area_m2
     ├── Band 13: annual
     └── NoData: -9999
                │                                          │
                └───────────────┬───────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  et_intersection.py   │
                    │                       │
                    │  1. Read rasters      │
                    │  2. Mask nodata→NaN   │
                    │  3. Zonal stats       │
                    │  4. Gap-fill AET/PET  │
                    │  5. Compute MAI       │
                    │  6. Kharif stress     │
                    └───────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                  ▼
      farm_static.parquet  farm_annual.parquet  farm_monthly.parquet
      (geometry, area)     (MAI, stress/yr)     (AET, PET, MAI/month)
```

---

## 2. Directory Structure

```
core-stack-backend/
├── computing/farm_boundaries/
│   ├── et_intersection.py      ← Main pipeline (Phase 3)
│   ├── fetch_raw.py            ← Phase 1: download farm boundaries
│   ├── convert.py              ← Phase 2: convert to GeoParquet
│   └── farm_boundary.py        ← API / orchestrator
│
├── data/
│   ├── et_rasters/
│   │   ├── merge_AET_4_2018_cog.tif   (17.6 GB, AEZ zone 4, year 2018)
│   │   ├── merge_PET_4_2018_cog.tif   (12.7 GB)
│   │   ├── shuvam_mai_sanganer_2018.tif   (validation raster)
│   │   └── shuvam_mai_dudu_2018.tif       (validation raster)
│   │
│   └── farm_boundaries/rajasthan/jaipur/
│       ├── sanganer/
│       │   ├── farm_boundaries.parquet   (94,214 farms)
│       │   ├── farm_static.parquet       (geometry + area)
│       │   ├── farm_annual.parquet       (annual MAI, kharif stress)
│       │   ├── farm_monthly.parquet      (monthly AET, PET, MAI)
│       │   └── mai_tally_2018.parquet    (validation output)
│       └── dudu/
│           └── ... (same structure, 92,394 farms)
│
├── compare_mai_tally.py        ← Validation script vs Shuvam
├── plot_tally_timeseries.py    ← Time-series comparison plots
└── regen_all_phase3.py         ← Re-run Phase 3 for all tehsils
```

---

## 3. Raster Band Ordering (Critical)

**The AET/PET rasters use crop-year band ordering, NOT calendar-year.**

```
Band  1 = July       (year Y)
Band  2 = August     (year Y)
Band  3 = September  (year Y)
Band  4 = October    (year Y)
Band  5 = November   (year Y)
Band  6 = December   (year Y)
Band  7 = January    (year Y+1)
Band  8 = February   (year Y+1)
Band  9 = March      (year Y+1)
Band 10 = April      (year Y+1)
Band 11 = May        (year Y+1)
Band 12 = June       (year Y+1)
Band 13 = Annual mean
```

This is encoded in the code as:
```python
CROP_YEAR_BAND_TO_MONTH = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]
```

Where index `i` maps raster band `i` (0-indexed) to calendar month number. For example, band index 0 → month 7 (July).

**Why this matters:** If you assume band 1 = January (calendar order), every monthly column will have the wrong month's data, and kharif stress will be computed from Oct–Jan instead of Jul–Oct.

---

## 4. Pipeline Execution

### Running for a specific tehsil

```python
from computing.farm_boundaries.et_intersection import intersect_et_with_farms

result = intersect_et_with_farms(
    state="rajasthan",
    district="jaipur",
    block="sanganer",      # tehsil name
    year=2018,
    overwrite=True          # set True to regenerate
)
```

### Re-running for all tehsils

```bash
python regen_all_phase3.py
```

### Prerequisites
- Conda environment: `corestackenv`
- `.env` file in project root with `LOCAL_ET_RASTERS_PATH` set
- Farm boundary parquets already generated (Phases 1 & 2)
- AET/PET rasters downloaded to `data/et_rasters/`

---

## 5. Key Functions in `et_intersection.py`

### `_read_raster_clipped(raster_path, bbox)`
- Reads a COG raster using windowed reading (only loads the bbox region, not the full 17 GB file)
- Converts nodata sentinel values to NaN using the raster's metadata (`src.nodata`), with a fallback to -9999
- Returns `(data, transform)` where `data` is shape `(bands, height, width)` as float32

### `_extract_band_means(labels, band_data, num_farms)`
- Computes per-farm mean of one raster band using vectorised `np.bincount`
- Ignores NaN, Inf, and negative values
- Farms with zero valid pixels → NaN (not imputed)

### `_gap_fill_monthly_farms(monthly_matrix)`
- Temporal interpolation on a `(n_farms, 12)` matrix in calendar order (col 0 = Jan, col 11 = Dec)
- Rules mirror Shuvam's `fill_monthly_collection()` — see Section 6

### `_run_zonal_stats(gdf, aet_data, aet_transform, pet_data, pet_transform)`
- Core function that:
  1. Rasterizes farm polygons onto the raster grid
  2. Extracts per-farm monthly AET using `CROP_YEAR_BAND_TO_MONTH` mapping
  3. Gap-fills AET, then PET
  4. Computes MAI = AET/PET (capped to [0, 1])
  5. Computes kharif stress flags from Jul–Oct MAI values

### `_save_monthly_parquet(gdf, state, district, block, year)`
- Converts wide-format columns to long format (one row per farm per month)
- Assigns correct calendar dates: Jul–Dec → year Y, Jan–Jun → year Y+1

---

## 6. Missing Data Handling & Gap-Fill

### Nodata at Read Time
- Raster nodata value (typically -9999) is converted to NaN immediately when the raster is read
- Any residual values ≤ -9999 are also masked (belt-and-suspenders)

### Temporal Gap-Fill Rules
Applied at the **farm level** on both AET and PET separately, before computing MAI.

The rules respect the crop-year boundary (July → June) and mirror Shuvam's GEE logic:

| Month | Fill source | Rationale |
|---|---|---|
| **July** | August only | Crop-year start — no backward crossing to previous June |
| **June** | May only | Crop-year end — no forward crossing to next July |
| **All others** | Mean of ±1 neighbouring month | Standard temporal interpolation |
| **No valid neighbour** | Stays NaN | Cannot impute without any reference |

This is implemented in `_GAP_FILL_NEIGHBOURS` dict and `_gap_fill_monthly_farms()`.

### MAI Capping
After computing MAI = AET/PET:
- Values > 1.0 are capped to 1.0 (AET cannot physically exceed PET; values > 1 are raster artifacts)
- Non-finite values → NaN
- A log warning is emitted when farms have MAI > 1

---

## 7. Output Schema

### `farm_static.parquet`
| Column | Type | Description |
|---|---|---|
| farm_id | string | Unique farm identifier |
| geometry | geometry | Farm polygon |
| area_m2 | float | Farm area in square metres |
| bbox | dict | Bounding box {xmin, ymin, xmax, ymax} |

### `farm_annual.parquet`
| Column | Type | Description |
|---|---|---|
| farm_id | string | Unique farm identifier |
| tehsil, district, state | string | Administrative hierarchy |
| area_in_ha | float | Farm area in hectares |
| year | int | Crop year (e.g. 2018 = Jul 2018 – Jun 2019) |
| aet_annual | float | Annual mean AET (mm/day) |
| pet_annual | float | Annual mean PET (mm/day) |
| mai_annual | float | Annual mean MAI [0, 1] |
| kharif_mai | float | Mean MAI for Jul–Oct |
| kharif_water_stress | bool | True if any kharif month MAI ≤ 0.50 |
| kharif_severe_stress | bool | True if any kharif month MAI ≤ 0.25 |

### `farm_monthly.parquet`
| Column | Type | Description |
|---|---|---|
| farm_id | string | Unique farm identifier |
| tehsil, district, state | string | Administrative hierarchy |
| year | int | Crop year |
| date | datetime | Calendar date (e.g. 2018-07-01 for July) |
| aet | float | Monthly AET (mm/day) |
| pet | float | Monthly PET (mm/day) |
| mai | float | Monthly MAI [0, 1] |

**Note:** For crop year 2018, `date` spans 2018-07-01 to 2019-06-01.

---

## 8. Validation Against Shuvam's Rasters

### Running the tally
```bash
python compare_mai_tally.py
```

This reads Shuvam's exported MAI GeoTIFFs (`shuvam_mai_sanganer_2018.tif`, `shuvam_mai_dudu_2018.tif`) and computes per-farm zonal means, then compares against our pipeline's output.

### Current tally results (2018)

| Metric | Sanganer (63,934 farms) | Dudu (80,273 farms) |
|---|---|---|
| Annual MAI — mean abs diff | 0.030 | 0.016 |
| Kharif MAI — mean abs diff | 0.036 | 0.019 |
| Farms within 0.01 (annual) | 30.8% | 47.7% |
| Farms within 0.01 (kharif) | 27.4% | 37.8% |

### Sources of remaining difference
1. **Annual definition:** Shuvam's annual = crop-year mean (Jul–Jun); ours = calendar-year mean (Jan–Dec)
2. **Gap-fill granularity:** Shuvam applies gap-fill at the pixel level in GEE; we apply it at the farm level after zonal stats
3. **Spatial coverage:** Shuvam's raster clips to tehsil boundary in GEE; our rasters extend slightly beyond, so NaN counts differ

### Generating time-series plots
```bash
python plot_tally_timeseries.py
```
Output: `data/tally_plots/mai_timeseries_tally_sanganer_2018.png` and `gap_fill_and_tally_demo_2018.png`

---

## 9. Recent Bug Fixes & Changes

### Bug Fix 1: Band-to-Month Mapping (Critical)
**Problem:** The pipeline assumed raster band 1 = January (calendar-year order). The actual rasters use crop-year order where band 1 = July.

**Impact:** Every monthly column (`aet_jan`, `mai_jul`, etc.) had the wrong month's data. Kharif stress was being computed from Oct–Jan instead of Jul–Oct.

**Fix:** Added `CROP_YEAR_BAND_TO_MONTH` mapping array. Each raster band index is now explicitly mapped to its correct calendar month before assigning to the column.

**File:** `et_intersection.py`, lines 78–87, 267–274, 299–302

---

### Bug Fix 2: Monthly Parquet Date Assignment
**Problem:** Monthly parquet assigned `date = 2018-01-01` for band 1, even though band 1 is actually July. All 12 months were dated within the same calendar year.

**Impact:** Any downstream consumer of `farm_monthly.parquet` filtering by date would get wrong data.

**Fix:** Jan–Jun months are now assigned to `year + 1`. For crop year 2018: Jul 2018 → Dec 2018 + Jan 2019 → Jun 2019.

**File:** `et_intersection.py`, lines 453–467

---

### Bug Fix 3: Nodata Masking at Read Time
**Problem:** `_read_raster_clipped()` was reading raw raster values without checking `src.nodata`. Nodata pixels (value -9999) were treated as valid data, contaminating zonal statistics (e.g., pulling farm means to large negative values).

**Impact:** Farm-level AET/PET means were incorrect for farms near raster boundaries.

**Fix:** Now reads `src.nodata` from raster metadata and converts all matching pixels to NaN. Also applies a belt-and-suspenders check for any value ≤ -9999.

**File:** `et_intersection.py`, lines 126–154

---

### New Feature: Temporal Gap-Fill
**What:** Added `_gap_fill_monthly_farms()` function that fills missing monthly values using neighbouring months, following Shuvam's crop-year boundary rules.

**Why:** Some farms have NaN for individual months due to cloud cover or missing MODIS composites. Without gap-fill, these NaNs propagate to annual and kharif means, artificially reducing coverage.

**File:** `et_intersection.py`, lines 195–248

---

### New Feature: MAI Capping to [0, 1]
**What:** MAI values > 1.0 are capped to 1.0 with a log warning.

**Why:** AET cannot physically exceed PET, but raster misalignment or model artifacts can produce MAI > 1. Previously these were left as-is, distorting farm-level statistics.

**File:** `et_intersection.py`, lines 326–336

---

## 10. How to Extend to New Regions/Years

### Adding a new tehsil
1. Run Phase 1 (`fetch_raw.py`) and Phase 2 (`convert.py`) to generate `farm_boundaries.parquet`
2. Ensure the AET/PET rasters for the correct AEZ zone exist in `data/et_rasters/`
3. Check `AEZ_ZONE_MAP` in `et_intersection.py` — add the state if it's not mapped
4. Run `intersect_et_with_farms(state, district, tehsil, year)`

### Adding a new year
1. Obtain the AET/PET rasters: `merge_AET_<aez>_<year>_cog.tif` and `merge_PET_<aez>_<year>_cog.tif`
2. Place them in `data/et_rasters/`
3. Run `intersect_et_with_farms(state, district, tehsil, year=<new_year>)`
4. The pipeline appends to existing parquets (idempotent — re-running the same year replaces old data)

### Adding a new AEZ zone
1. Add the state→zone mapping in `AEZ_ZONE_MAP`
2. Ensure rasters follow the naming convention `merge_AET_<zone>_<year>_cog.tif`

---

## 11. Known Limitations

1. **Gap-fill granularity:** We gap-fill at the farm level (after zonal stats), not at the pixel level like Shuvam's GEE pipeline. This means if an entire farm is NaN for a month, we interpolate using neighbouring months' farm means, not neighbouring months' pixel values.

2. **Annual MAI definition:** Our `mai_annual` is the mean of 12 monthly MAI values (Jan–Dec calendar order). Shuvam's is crop-year (Jul–Jun). For apples-to-apples comparison, use `kharif_mai` which covers the same Jul–Oct period in both.

3. **Raster coverage at edges:** Our rasters may not cover all farms at tehsil edges. Farms outside the raster extent get NaN. Shuvam's GEE pipeline clips exactly to the tehsil boundary, so he has different NaN counts.

4. **Large raster files:** The AET/PET rasters are 12–17 GB each. Windowed COG reading keeps memory manageable, but initial processing for a tehsil takes 30–60 seconds.
