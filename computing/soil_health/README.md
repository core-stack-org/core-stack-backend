# Soil Health Pipeline

This pipeline generates nutrient-level soil statistics for each polygon in a selected ROI (state/district/block watershed or user-provided geometry). It reads the soil-health raster layers, masks them to valid LULC classes, and writes a vector output with summary statistics for each nutrient and ROI feature.

## Scope

The current implementation is designed to:

- clip soil-health rasters to the chosen ROI geometry
- prepare a stable LULC reference using the latest 3 years of LULC rasters
- apply nutrient-specific LULC masking before summarising the soil values
- compute per-geometry nutrient statistics (count, mean, and percentile distribution)
- export both local raster tiles and a final vector summary GeoPackage
- optionally publish the outputs to GeoServer and save layer metadata

---

## Data sources and inputs

The pipeline depends on the following files and derived products:

- `data/base_layers/soil_health/soil_health_N.tif`
- `data/base_layers/soil_health/soil_health_K.tif`
- `data/base_layers/soil_health/soil_health_P.tif`
- `data/base_layers/soil_health/soil_health_OC.tif`
- `data/base_layers/soil_health/soil_health_OC_OLM.tif`
- Latest LULC rasters under the configured `LULC_BASE_DIR`, selected as the newest 3 available rasters (`lulc_v3_*.tif`)
- ROI geometry from either:
  - precomputed state/district/block watershed boundary, or
  - a user-supplied vector file and `asset_suffix`

---

## Core method

### 1. Prepare ROI and reference grid

The workflow begins by loading the ROI geometry and validating it. The soil raster is opened and the ROI is reprojected to the raster CRS when needed.

The code uses the nitrogen raster (`soil_health_N.tif`) as the reference grid for the LULC alignment step because it is the first raster clipped to the ROI. The reference metadata is later reused for all nutrient rasters that are reprojected to the same grid.

### 2. Build a three-year LULC mode image

The pipeline searches for the latest LULC rasters and reprojects each one to the reference soil grid using the same transform and CRS. The generated arrays are combined with the local `compute_mode_lulc_array(...)` function to produce a 3-year modal LULC dataset.

This is intended to smooth year-to-year variation and create a stable land-cover context before soil-health filtering.

### 3. Apply nutrient-specific LULC masking

The pipeline masks each soil raster to selected LULC classes before calculating statistics. The mask rules are:

| Nutrient | LULC classes used |
|---|---|
| `N` | 8, 9, 10, 11 |
| `K` | 8, 9, 10, 11 |
| `P` | 8, 9, 10, 11 |
| `OC` | 8, 9, 10, 11 |
| `OC_OLM` | 6, 12 |

These classes are chosen to keep the summary focused on meaningful agricultural and vegetated areas, while excluding pixels outside the intended land-use context.

For each nutrient, each raster is clipped to the ROI, then pixels are retained only where:

- the LULC mode falls in the allowed class set for that nutrient, and
- the soil pixel is not nodata

### 4. Compute per-feature statistics

For each geometry in the ROI, the code:

- masks the clipped nutrient raster to the polygon boundary
- removes nodata and non-finite pixels
- calculates the pixel count
- computes the mean soil value
- computes percentiles at 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, and 95%

The percentiles are written as columns like `N_p05`, `N_p10`, ..., `N_p95`.

### 5. Compute LULC area summaries

The pipeline also estimates area by LULC class group using the modal LULC raster:

- `crop_cover_area`: total area of LULC classes 8, 9, 10, 11
- `tree_shrub_area`: total area of LULC classes 6 and 12

Area is calculated using pixel row-wise geodesic area in square metres and converted to hectares by dividing by 10,000.

---

## Output products

The pipeline writes two main outputs:

1. Raster layers per nutrient:
   - `..._soil_health_raster_N`
   - `..._soil_health_raster_K`
   - `..._soil_health_raster_P`
   - `..._soil_health_raster_OC`
   - `..._soil_health_raster_OC_OLM`

2. One vector summary layer:
   - `..._soil_health_vector`

The vector layer contains the original ROI attributes plus the computed nutrient and land-cover summary fields.

---

## Output column description

The final vector contains one set of fields for each nutrient and two area summary fields.

### Nutrient fields

For each nutrient in `N`, `K`, `P`, `OC`, and `OC_OLM`, the code creates the following columns:

- `{nutrient}_count`  
  Number of valid raster pixels within the polygon.

- `{nutrient}_mean`  
  Mean soil value for the polygon.

- `{nutrient}_p05` to `{nutrient}_p95`  
  Percentile values for the feature distribution, generated for 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, and 95%.

Examples:

- `N_count`
- `N_mean`
- `N_p05`
- `N_p50`
- `N_p95`
- `K_count`
- `K_mean`
- `OC_OLM_p90`

### LULC summary fields

- `crop_cover_area`  
  Area covered by crop-related LULC classes (8, 9, 10, 11), reported in hectares.

- `tree_shrub_area`  
  Area covered by tree/shrub LULC classes (6, 12), reported in hectares.

---

## Example output field set

A typical output schema looks like:

```text
<original ROI fields>
N_count, N_mean, N_p05, N_p10, N_p20, N_p30, N_p40, N_p50, N_p60, N_p70, N_p80, N_p90, N_p95,
K_count, K_mean, K_p05, K_p10, K_p20, K_p30, K_p40, K_p50, K_p60, K_p70, K_p80, K_p90, K_p95,
P_count, P_mean, P_p05, P_p10, P_p20, P_p30, P_p40, P_p50, P_p60, P_p70, P_p80, P_p90, P_p95,
OC_count, OC_mean, OC_p05, OC_p10, OC_p20, OC_p30, OC_p40, OC_p50, OC_p60, OC_p70, OC_p80, OC_p90, OC_p95,
OC_OLM_count, OC_OLM_mean, OC_OLM_p05, OC_OLM_p10, OC_OLM_p20, OC_OLM_p30, OC_OLM_p40, OC_OLM_p50, OC_OLM_p60, OC_OLM_p70, OC_OLM_p80, OC_OLM_p90, OC_OLM_p95,
crop_cover_area,
tree_shrub_area
```

---

## Runtime notes

This pipeline is implemented in:

- `computing/soil_health/soil_health.py`
- `computing/soil_health/soil_health_helper.py`

The main local task entry point is:

```python
soil_health_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
)
```

The task performs the raster export first and then generates the vector summary from those outputs.
