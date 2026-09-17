# Tree in Grassland

This pipeline produces per-micro-watershed tree-in-grassland context metrics from Pan-India LULC v3 data. It combines a neighbourhood shrub-density test with a multi-year temporal window to identify tree-shrub grassland systems and quantify land-use transitions away from those systems.

## Scope

The current implementation in this module is designed to:

- Identify tree pixels embedded within shrub-dominated grassland neighbourhoods
- Capture adjacent shrub pixels associated with those tree patches
- Compare start- and end-period context to detect loss of tree-grassland systems
- Measure transition of those systems into barren land, built-up, water, or crop classes
- Export the result as a GEE vector asset or a local GeoPackage output

---

## Entry points

The module exposes three main workflows:

- `tree_in_grassland_for_AEZ(aez_no, start_year=None, end_year=None, gee_account_id=7)`
  - Filters AEZ polygons and then runs the GEE-based micro-watershed pipeline.
- `generate_tree_in_grassland_layer(...)`
  - Main GEE pipeline that computes the asset, optionally publishes it, and saves metadata.
- `generate_tree_in_grassland_local(...)`
  - Local vector workflow that clips a precomputed pan-India tree-in-grassland layer to an ROI and writes the output locally.

---

## Data sources and inputs

| Source | Purpose |
|---|---|
| Pan-India LULC v3 | Annual land-cover classification used to compute context and transitions |
| Micro-watershed boundary dataset | ROI used for per-feature statistics |
| AEZ boundary dataset | Optional grouping used by `tree_in_grassland_for_AEZ` |
| Local precomputed tree-in-grassland vector | Used by the local workflow when not running GEE export |

---

## Core constants

Defined in `tree_in_grassland_utils.py`:

| Constant | Value | Meaning |
|---|---:|---|
| `TREE_CLASS` | 6 | Tree class in LULC |
| `SHRUB_CLASS` | 12 | Shrub class in LULC |
| `SHRUB_THRESHOLD` | 0.5 | Secondary shrub threshold constant |
| `RADIUS_M` | 100 | 100 m neighbourhood radius |
| `THRESHOLD` | 0.5 | Tree-in-shrub classification threshold |
| `SCALE` | 30 | Raster resolution in metres |
| `MAXPIX` | 1e12 | Maximum reducer pixel limit |

---

## Workflow in the current code

### 1. Load annual LULC imagery

The pipeline loads one image per year in the selected analysis range:

```python
lulc_by_year = {
    year: load_pan_india_lulc(year) for year in range(start_year, end_year + 1)
}
```

`load_pan_india_lulc(year)` loads the asset named as:

```python
PAN_INDIA_LULC_V3_DATASET + f"{year}_{year + 1}"
```

and then does:

```python
.select("predicted_label")
.unmask(0)
.toInt()
```

### 2. Create the tree-shrub context image

The main classification function is `tree_context_all(lulc, aoi)`.

```python
kernel = ee.Kernel.circle(RADIUS_M, "meters")
lulc_img = lulc.clip(aoi.buffer(110))

tree_mask = lulc_img.eq(TREE_CLASS)
shrub_mask = lulc_img.eq(SHRUB_CLASS)

shrub_frac = (
    shrub_mask.toInt().reduceNeighborhood(ee.Reducer.sum(), kernel)
    .divide(total_px)
)

tree_in_shrub = tree_mask.And(shrub_frac.gt(THRESHOLD))
shrub_around_tree = shrub_mask.And(
    tree_in_shrub.focal_max(radius=RADIUS_M, units="meters")
)
```

Classification output values are:

| Value | Meaning |
|---:|---|
| 0 | Neither tree-in-shrub nor associated shrub |
| 1 | Tree pixel embedded in shrubland |
| 2 | Shrub pixel associated with those trees |

### 3. Build start and end temporal windows

The code uses overlapping 3-year windows instead of a single-year comparison:

```python
start_years = [start_year, start_year + 1, start_year + 2]
end_years = [end_year - 2, end_year - 1, end_year]
```

These are passed to `temporal_context(...)`, which computes a modal context image for each period:

```python
context_start = ee.ImageCollection(start_contexts).reduce(ee.Reducer.mode())
context_end = ee.ImageCollection(end_contexts).reduce(ee.Reducer.mode())
```

### 4. Define the grassland mask

This is the start-period system mask used in transition analysis:

```python
grassland_mask = context_start.eq(1).Or(context_start.eq(2))
```

This includes:

- tree pixels embedded in shrubland
- shrub pixels spatially associated with those embedded trees

### 5. Detect losses and transitions

The current implementation calculates:

```python
tree_loss = grassland_mask.And(context_end.eq(0))
tree_to_barren = grassland_mask.And(lulc_end.eq(7))

to_built = grassland_mask.And(lulc_end.eq(1))
to_kharif = grassland_mask.And(lulc_end.eq(2))
to_kharif_rabi = grassland_mask.And(lulc_end.eq(3))
to_zaid = grassland_mask.And(lulc_end.eq(4))

to_crops = grassland_mask.And(
    lulc_end.eq(5)
    .Or(lulc_end.eq(8))
    .Or(lulc_end.eq(9))
    .Or(lulc_end.eq(10))
    .Or(lulc_end.eq(11))
)
```

These represent the tree-in-grassland system transitioning into barren land, built-up, water classes, and crop classes.

### 6. Compute area statistics

The final per-feature values are calculated using `ee.Image.pixelArea()` and a reducer over each geometry:

```python
area_in_m2 = (
    pixel_area.updateMask(mask)
    .reduceRegion(ee.Reducer.sum(), aoi, SCALE, maxPixels=MAXPIX)
    .get("area")
)
return ee.Number(area_in_m2).multiply(0.0001)
```

This returns areas in hectares (`*_in_ha` fields), not square metres.

---

## Output fields in the generated vector

The final feature collection includes the following attributes, using the revised definitions below:

| Field | Meaning |
|---|---|
| `uid` | Watershed identifier |
| `area_in_ha` | Feature area |
| `shrubland_area_in_ha` | All shrub pixels (LULC class 12) |
| `isolated_shrub_area_in_ha` | Shrub pixels (LULC class 12) that are not near tree pixels |
| `shrubs_trees_area_in_ha` | Shrub and tree pixels for isolated tree pixels (> 50% shrubs around trees), in the first three years |
| `tree_in_shrubs_trees_area_in_ha` | Isolated tree pixels inside shrub and tree pixels |
| `tree_loss_in_tree_in_shrub_area_in_ha` | How much of the isolated tree pixels disappeared |
| `tree_in_tree_in_shrub_to_barren_area_in_ha` | Area of isolated tree pixels that turned into barren land |
| `tree_in_tree_in_shrub_to_built_area_in_ha` | Area of isolated tree pixels that turned into built-up |
| `tree_in_tree_in_shrub_to_kharif_water_area_in_ha` | Area of isolated tree pixels that turned into kharif water |
| `tree_in_tree_in_shrub_to_kharif_rabi_water_area_in_ha` | Area of isolated tree pixels that turned into kharif-rabi water |
| `tree_in_tree_in_shrub_to_kharif_rabi_zaid_water_area_in_ha` | Area of isolated tree pixels that turned into kharif-rabi-zaid water |
| `tree_in_tree_in_shrub_to_crops_area_in_ha` | Area of isolated tree pixels that turned into crops |

### Field semantics

- `shrubland_area_in_ha` = all shrub pixels (LULC class 12)
- `isolated_shrub_area_in_ha` = shrub pixels (LULC class 12) that are not near tree pixels
- `shrubs_trees_area_in_ha` = shrub and tree pixels for isolated tree pixels (> 50% shrubs around trees), in the first three years
- `tree_in_shrubs_trees_area_in_ha` = isolated tree pixels inside shrub and tree pixels
- `tree_loss_in_tree_in_shrub_area_in_ha` = how much of isolated tree pixels disappeared
- `tree_in_tree_in_shrub_to_*_area_in_ha` = what the disappearing isolated tree pixels turned into

---

## Local compute workflow

The local pipeline in `tree_in_grassland_local_compute.py` does not recompute the classification itself. Instead, it:

1. Loads the relevant watershed geometry
2. Reads the pan-India tree-in-grassland vector
3. Clips it to the ROI using `clip_vector_to_watersheds`
4. Saves the result as a local GeoPackage under the configured output folder
5. Optionally pushes the layer to GeoServer and updates layer metadata

The function is:

```python
generate_tree_in_grassland_local(...)
```

with inputs such as:

- `state`
- `district`
- `block`
- `roi_path`
- `precomputed_roi_dir`
- `push_to_geoserver`
- `sync_layer_metadata`

---

## Export and publication

After computing the result, the GEE workflow exports the vector to a GEE asset and optionally syncs it to GeoServer:

- `export_vector_asset_to_gee(...)`
- `sync_fc_to_geoserver(...)`
- `save_layer_info_to_db(...)`
- `update_layer_sync_status(...)`

The layer metadata includes start and end years under the `misc` field when the layer is persisted.

---

## Operational notes

- The module expects a valid ROI and an asset path configured through the project GEE settings.
- The tree-in-grassland logic uses LULC class 6 (tree) and 12 (shrub).
- The neighbourhood threshold is greater than 50% shrub coverage within 100 m.
- The output is stored in hectares for the generated attributes, not square metres.

---

## Typical execution pattern

```python
from computing.tree_in_grassland.tree_in_grassland import generate_tree_in_grassland_layer

generate_tree_in_grassland_layer(
    roi=some_feature_collection,
    asset_suffix="sample_block",
    asset_folder_list=["state", "district", "block"],
    start_year=2018,
    end_year=2021,
    gee_account_id=7,
    app_type="MWS",
    sync_to_db=True,
    sync_to_geoserver=True,
)
```

This matches the code in the current repository and reflects the actual implementation rather than the older documentation version.
