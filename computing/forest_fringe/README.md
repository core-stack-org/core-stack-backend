# Forest Fringe Degradation Pipeline

Identifies and quantifies degradation occurring along the edges of forest patches within micro-watersheds (MWS). Implemented in **Google Earth Engine (GEE)**.

---

## Overview

The pipeline computes forest-edge degradation indicators for each microwatershed by:

- Identifying stable forest patches
- Constructing forest fringe zones
- Measuring degradation and deforestation inside fringe regions
- Comparing fringe degradation against degradation across the entire MWS

---

## Data Sources

| Dataset | Purpose |
|---|---|
| Pan-India LULC v3 | Identification of stable tree cover |
| LTP/STP Change Product | Detection of tree deforestation |
| Overall Tree Health Change Product | Detection of tree degradation |
| Microwatershed Boundaries | Spatial analysis unit |

Dataset paths are defined in the utility module.

---

## Core Parameters

Defined in `forest_fringe_utils.py`.

| Parameter | Value | Description |
|---|---|---|
| `OUTER_BUFFER` | 100 m | Expansion distance around MWS |
| `FRINGE_WIDTH` | 50 m | Width of forest fringe zone |
| `SCALE` | 30 m | Analysis resolution |
| `MAXPIX` | 1e12 | Maximum pixels allowed in reducers |

---

## Pipeline Workflow

### Step 1 — Initialize GEE
Initialize GEE via service account, construct asset paths, define export layer names, and load microwatershed boundaries.
Entry point: `generate_forest_fringe_layer()`

### Step 2 — Load Microwatershed Boundaries
```python
mws_fc = ee.FeatureCollection(roi_path)
```
Each feature represents one microwatershed polygon.

### Step 3 — Generate Stable Forest Layer
A temporal mode operation across three annual LULC images (2017–18, 2018–19, 2019–20) produces a stable forest mask.

```python
.select("predicted_label").eq(TREE_CLASS)  # TREE_CLASS = 6
masks.reduce(ee.Reducer.mode())
```

Output is a binary, self-masked image: `1` = stable forest, `masked` = non-forest.
Function: `load_tree_mode()`

### Step 4 — Expand MWS Boundary
Each MWS geometry is buffered outward by 100 m to prevent watershed-boundary pixels from being misclassified as forest edge.

```python
expanded_mws = mws_geom.buffer(OUTER_BUFFER, 1)  # OUTER_BUFFER = 100
```

### Step 5 — Extract Forest Patches
The stable forest image is clipped to the expanded MWS and vectorized into polygonal forest patches.

```python
forest = tree_mode.clip(expanded_mws)
forest.reduceToVectors(scale=30, geometryType='polygon', eightConnected=True)
```

### Step 6 — Filter Small Patches
Forest fragments smaller than 1 hectare (10,000 m²) are removed.

```python
ltps = forest_patches.filter(ee.Filter.area(10000, 1e13))
```

### Step 7 — Construct Fringe Zones
For each forest patch, a 50 m inward ring is created by subtracting an inner buffer from the outer polygon.

```python
outer = patch.geometry()
inner = outer.buffer(-fringe_width, 1)
fringe = outer.difference(inner, 1)
```

Function: `make_fringe()`

### Step 8 — Clip Fringe to Original MWS
Fringe geometries are intersected with the original (unexpanded) MWS boundary to remove any overhang.

```python
fr.geometry().intersection(mws_geom, 1)
```

### Step 9 — Rasterize Fringe
```python
ee.Image.constant(1).paint(fringes_clipped, 1).selfMask()
```

### Step 10 — Compute Fringe Area
```python
fringe_geom.area(1)
```

### Step 11 — Generate Degradation & Deforestation Masks

**Deforestation** (from LTP change product, constrained to forest):
```python
ltp_change.eq(6).Or(ltp_change.eq(7)).updateMask(tree_mode)
```

**Degradation** (from overall change product, constrained to forest):
```python
overall_change.eq(-1).updateMask(tree_mode)
```

### Step 12 — Compute Area Statistics
Pixel-area reduction across both the full MWS and the fringe zone.

```python
ee.Image.pixelArea().reduceRegion(...)
```

All areas are in **square meters**.

### Step 13 — Compute Normalized Indicators

| Indicator | Formula |
|---|---|
| Forest Fringe Ratio | `Forest Fringe Area / MWS Area` |
| Tree Degradation Fringe Ratio | `Degradation Area in Fringe / Fringe Area` |
| Tree Deforestation Fringe Ratio | `Deforestation Area in Fringe / Fringe Area` |

---

## Output Attributes

| Attribute | Description |
|---|---|
| `uid` | Microwatershed identifier |
| `mws_area_m2` | Total MWS area |
| `forest_fringe_area_m2` | Total forest fringe area |
| `forest_fringe_ratio` | Fringe area / MWS area |
| `tree_degradation_mws_area_m2` | Total degraded forest area in MWS |
| `tree_degradation_fringe_area_m2` | Degraded forest area within fringe |
| `tree_degradation_fringe_ratio` | Fraction of fringe degraded |
| `tree_deforestation_mws_area_m2` | Total deforested forest area in MWS |
| `tree_deforestation_fringe_area_m2` | Deforested area within fringe |
| `tree_deforestation_fringe_ratio` | Fraction of fringe deforested |

---

## Export

| Function | Purpose |
|---|---|
| `export_vector_asset_to_gee()` | Export results to GEE asset |
| `sync_fc_to_geoserver()` | Sync FeatureCollection to GeoServer |

---

## Workflow Summary

```
1. Load MWS boundaries
2. Generate stable forest mask (temporal mode, 2017–2020)
3. Expand MWS boundary by 100 m
4. Extract and vectorize forest patches
5. Remove patches < 1 ha
6. Create 50 m inward fringe rings
7. Clip fringe to original MWS
8. Generate degradation and deforestation masks
9. Compute area statistics (MWS-wide and fringe)
10. Compute normalized fringe indicators
11. Export to GEE and GeoServer
```
