# Forest Fire Indicator Pipeline

Computes long-term fire intensity and frequency indicators for each microwatershed using **MODIS active fire observations** from the Terra and Aqua satellites in **Google Earth Engine (GEE)**.

---

## Overview

The pipeline produces four complementary fire indicators per microwatershed:

| Indicator | Description |
|---|---|
| Annualized Fire Intensity | Total FRP sum normalized by analysis duration |
| Mean Fire Intensity | Average intensity across all fire detections |
| Peak Fire Intensity | Maximum FRP observed in the analysis period |
| Annual Fire Frequency | Average yearly fire occurrence count |

---

## Data Sources

| Dataset | Satellite | Variable |
|---|---|---|
| `MOD14A1` | Terra | `MaxFRP` — Maximum Fire Radiative Power |
| `MYD14A1` | Aqua | `MaxFRP` — Maximum Fire Radiative Power |

FRP measures fire intensity, thermal energy release, and combustion severity. Using both sensors improves temporal coverage and reduces missed detections.

Dataset paths are defined in `forest_fire_utils.py`.

---

## Core Parameters

Defined in `forest_fire_utils.py`.

| Parameter | Value | Description |
|---|---|---|
| `SCALE` | 1000 m | Spatial aggregation resolution |
| `MAXPIX` | 1e13 | Maximum pixels allowed in reducers |

---

## Pipeline Workflow

### Step 1 — Initialize GEE
Initialize Earth Engine, construct asset paths, define export names, and load MWS boundaries.
Entry point: `generate_forest_fire_layer()`

### Step 2 — Define Temporal Scope
```python
N = end_year - start_year + 1
```
All intensity and frequency metrics are normalized by `N` to enable fair comparison across regions and time windows.

### Step 3 — Load & Merge MODIS Fire Collections
Implemented in `load_fire_collections()`.

```python
terra = ee.ImageCollection(TERRA_FIRE_PATH)
aqua  = ee.ImageCollection(AQUA_FIRE_PATH)

fires = terra.merge(aqua) \
             .filterDate(start_date, end_date) \
             .select("MaxFRP")
```

### Step 4 — Preprocess Fire Data
Implemented in `prepare_frp_images()`.

**Mask non-fire pixels** — retain active detections only:
```python
img.updateMask(img.gt(0))
```

**Create binary fire layer** — for frequency analysis:
```python
img.gt(0).unmask(0).rename("fire")
# 1 = fire detected, 0 = no fire
```

### Step 5 — Construct Aggregated Fire Products

| Product | Implementation | Interpretation |
|---|---|---|
| Annualized FRP Sum | `frp_masked.sum().divide(n_years)` | Average yearly fire energy release |
| Mean FRP | `frp_masked.mean()` | Typical fire intensity across detections |
| Peak FRP | `frp_masked.max()` | Maximum intensity / extreme fire events |
| Annual Fire Frequency | `fire_binary_collection.sum().divide(n_years)` | Average yearly fire occurrence |

**Formulas:**

$$\text{Annualized FRP Sum} = \frac{\sum \text{FRP}}{N}$$

$$\text{Annual Fire Count} = \frac{\sum \text{Fire Detections}}{N}$$

### Step 6 — Load Microwatershed Boundaries
```python
mws_fc = ee.FeatureCollection(roi_path)
```

### Step 7 — Geometry Validation & Repair
Before zonal aggregation, geometries are validated and repaired to prevent reducer failures.

```python
# Detect invalid geometries (zero area)
geom.area(1)

# Repair self-intersections and topology errors
f.geometry().buffer(0).simplify(10)
```

### Step 8 — Compute Fire Metrics per MWS
Implemented in `compute_fire_metrics()`.

Zonal reduction at 1 km MODIS scale:

| Metric | Reducer |
|---|---|
| FRP Sum | `Sum` |
| Mean FRP | `Mean` |
| Peak FRP | `Mean` applied to max image |
| Fire Count | `Sum` |

Watersheds with no fire activity receive `0` (no null propagation):
```python
ee.Algorithms.If(ee.Algorithms.IsEqual(val, None), 0, val)
```

---

## Output Attributes

| Attribute | Description |
|---|---|
| `fire_frp_sum_per_year` | Annualized total FRP |
| `fire_frp_mean` | Mean fire intensity |
| `fire_frp_max` | Peak fire intensity |
| `fire_count_per_year` | Annualized fire occurrence |

---

## Export

| Function | Purpose |
|---|---|
| `export_vector_asset_to_gee()` | Export results to GEE asset |
| `sync_fc_to_geoserver()` | Sync FeatureCollection to GeoServer |

---

## Workflow Summary

```
1. Define analysis period (start_year → end_year, compute N)
2. Load MODIS Terra and Aqua fire collections
3. Merge collections, filter by date, select MaxFRP
4. Mask non-fire pixels; create binary fire layers
5. Construct annualized FRP sum, mean FRP, peak FRP, fire frequency
6. Load MWS boundaries
7. Validate and repair geometries
8. Compute zonal fire statistics per MWS (1 km scale)
9. Normalize metrics by N; fill nulls with 0
10. Export to GEE and GeoServer
```
