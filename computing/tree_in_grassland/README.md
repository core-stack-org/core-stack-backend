# Tree-in-Grassland Degradation Indicator Pipeline

Identifies degradation within grazing landscapes by analysing the spatial relationship between trees and shrub-dominated grasslands over time. Combines **neighbourhood-based spatial context classification** with **temporal land-use transition analysis** using multi-year LULC datasets in **Google Earth Engine (GEE)**.

---

## Overview

The pipeline identifies:

- Trees embedded within shrubland-dominated grasslands
- Degradation and loss of tree-grassland systems
- Transitions from grassland-tree systems into barren land, cropland, built-up, or water

Results characterize degradation within livestock-dependent grazing ecosystems.

---

## Data Sources

| Dataset | Purpose |
|---|---|
| Pan-India LULC v3 | Land-cover classification |
| Microwatershed Boundaries | Spatial analysis units |

---

## Core Parameters

Defined in `tree_in_grassland_utils.py`.

| Parameter | Value | Description |
|---|---|---|
| `RADIUS_M` | 100 m | Neighbourhood radius |
| `THRESHOLD` | 0.5 | Shrub dominance threshold |
| `SCALE` | 30 m | Analysis resolution |
| `MAXPIX` | 1e12 | Maximum reducer pixels |

---

## Pipeline Workflow

### Step 1 — Initialize GEE
Initialize GEE, construct asset paths, load MWS boundaries, and prepare export metadata.
Entry point: `generate_tree_in_grassland_layer()`

### Step 2 — Load Microwatershed Boundaries
```python
mws_fc = ee.FeatureCollection(roi_path)
```
Each feature is one microwatershed polygon over which all indicators are computed.

### Step 3 — Load Multi-Year LULC Data
```python
load_pan_india_lulc(year)
# Asset path: pan_india_lulc_v3_{year-1}_{year}
```
Each image selects the `predicted_label` band, fills masked pixels with `0`, and casts to integer.

### Step 4 — Construct Temporal Windows
Overlapping 3-year windows are used for both the start and end periods to reduce classification noise.

```python
start_years = [start_year-1, start_year, start_year+1]
end_years   = [end_year-1,   end_year,   end_year+1]
```

### Step 5 — Compute Modal LULC Layers
The most frequent LULC class across each 3-year window is retained, capturing persistent land-cover rather than single-year anomalies.

```python
ee.ImageCollection([...]).reduce(ee.Reducer.mode())
```

Computed separately for the start and end periods.

### Step 6 — Neighbourhood-Based Tree-Grassland Classification
The core methodological step. Implemented in `tree_context_all()`.

**6.1 — Create neighbourhood kernel**
```python
kernel = ee.Kernel.circle(RADIUS_M, "meters")  # RADIUS_M = 100
```

**6.2 — Identify tree and shrub pixels**
```python
tree_mask  = lulc_img.eq(TREE_CLASS)   # TREE_CLASS  = 6
shrub_mask = lulc_img.eq(SHRUB_CLASS)  # SHRUB_CLASS = 12
```

**6.3 — Compute shrub fraction in neighbourhood**
```python
shrub_frac = shrub_pixels / total_pixels  # via reduceNeighborhood(ee.Reducer.sum(), kernel)
```

**6.4 — Classify tree-in-grassland pixels**
A tree pixel is classified as part of a shrubland ecosystem when >50% of its 100 m neighbourhood is shrub.
```python
tree_in_shrub = tree_mask.And(shrub_frac.gt(THRESHOLD))  # THRESHOLD = 0.5
```

**6.5 — Identify associated shrub pixels**
Shrub pixels spatially connected to tree-in-shrub regions are also captured.
```python
shrub_around_tree = shrub_mask.And(
    tree_in_shrub.focal_max(radius=RADIUS_M, units="meters")
)
```

**6.6 — Final context classification**

| Value | Meaning |
|---|---|
| `0` | Neither tree-in-shrub nor associated shrub |
| `1` | Tree embedded in shrubland |
| `2` | Shrub associated with embedded trees |

```python
ee.Image(0).where(tree_in_shrub, 1).where(shrub_around_tree, 2)
```

### Step 7 — Construct Stable Temporal Contexts
The neighbourhood classification is computed for each year in both temporal windows, then the modal class is retained across each 3-year period. Implemented in `temporal_context()`.

```python
context_start = ImageCollection(...).reduce(mode)
context_end   = ImageCollection(...).reduce(mode)
```

### Step 8 — Define Grassland Mask
```python
grassland_mask = context_start.eq(1).Or(context_start.eq(2))
```
Includes both tree pixels embedded in shrubland and their associated shrubland pixels — representing the initial grazing landscape used for transition analysis.

### Step 9 — Compute Tree Loss
Pixels that belonged to the grassland-tree system at the start but no longer do at the end.

```python
tree_loss = grassland_mask.And(context_end.eq(0))
```

### Step 10 — Compute Transition-Based Indicators

| Transition | Code |
|---|---|
| Barren land | `grassland_mask.And(lulc_end.eq(7))` |
| Built-up | `grassland_mask.And(lulc_end.eq(1))` |
| Kharif water | `grassland_mask.And(lulc_end.eq(2))` |
| Kharif-Rabi water | `grassland_mask.And(lulc_end.eq(3))` |
| Kharif-Rabi-Zaid water | `grassland_mask.And(lulc_end.eq(4))` |
| Croplands (all types) | `lulc_end.eq(5\|8\|9\|10\|11)` |

Cropland classes covered:

| Class | Meaning |
|---|---|
| 5 | Crops |
| 8 | Single Kharif |
| 9 | Single Non-Kharif |
| 10 | Double Cropping |
| 11 | Triple / Perennial Cropping |

### Step 11 — Compute Area Statistics
```python
ee.Image.pixelArea().reduceRegion(...)
```
Computed per MWS. All areas in **square meters**.

### Step 12 — Compute Normalized Indicators

| Indicator | Formula |
|---|---|
| Tree Loss to Grassland Ratio | `Tree Loss Area / Grassland Area` |
| Tree Loss to Tree-in-Shrub Ratio | `Tree Loss Area / Tree-in-Shrub Area` |

---

## Output Attributes

| Attribute | Description |
|---|---|
| `grassland_area_m2` | Total grassland-system area |
| `tree_in_shrub_area_m2` | Tree pixels embedded in shrubland |
| `isolated_shrub_area_m2` | Shrub pixels not associated with trees |
| `shrubland_area_m2` | Total shrubland area |
| `tree_loss_area_m2` | Loss of tree-grassland systems |
| `tree_loss_to_grassland_ratio` | Normalized tree loss |
| `tree_loss_to_tree_in_shrub_ratio` | Embedded-tree loss ratio |
| `tree_shrub_to_barren_area_m2` | Transition to barren land |
| `tree_shrub_to_built_area_m2` | Transition to built-up |
| `tree_shrub_to_kharif_water_area_m2` | Transition to kharif water |
| `tree_shrub_to_kharif_rabi_water_area_m2` | Transition to kharif-rabi water |
| `tree_shrub_to_kharif_rabi_zaid_water_area_m2` | Transition to kharif-rabi-zaid water |
| `tree_shrub_to_crops_area_m2` | Transition to croplands |

---

## Export

| Function | Purpose |
|---|---|
| `export_vector_asset_to_gee()` | Export results to GEE asset |
| `sync_fc_to_geoserver()` | Sync FeatureCollection to GeoServer |

---

## Workflow Summary

```
1.  Load yearly LULC layers
2.  Build modal LULC representations (3-year windows)
3.  Create neighbourhood shrub-density kernels (100 m radius)
4.  Identify tree-in-grassland systems (shrub fraction > 50%)
5.  Generate stable temporal context layers
6.  Construct grassland masks (start period)
7.  Detect tree-system loss (start → end context change)
8.  Compute transition-based degradation indicators
9.  Calculate area statistics and ratios per MWS
10. Export to GEE and GeoServer
```
