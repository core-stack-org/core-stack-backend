# KYL — Landslide susceptibility (Earth Engine)

This folder contains a scaffold to compute landslide susceptibility using Google Earth Engine (GEE). The goal is to produce a ~100 m raster of susceptibility, vectorize field-level polygons, and export assets with metadata.

Files
- `process_landslide_susceptibility.py` — Main Python script using the Earth Engine Python API.
- `ee_code_editor.js` — Small JavaScript snippet for the Earth Engine Code Editor (preview/visualization).
- `requirements.txt` — Python dependencies (see below).

Quick start
1. Install dependencies and authenticate Earth Engine:

```bash
pip install -r gee_kyl/requirements.txt
earthengine authenticate
```

2. Prepare an AOI GeoJSON file (single Feature or FeatureCollection).

3. (Optional) Create a `weights.json` using the weights from your methodology paper. Example:

```json
{
  "slope": 0.4,
  "curvature": 0.1,
  "flow_acc": 0.2,
  "lulc": 0.15,
  "rainfall": 0.15
}
```

4. Run the processor (example):

```bash
python gee_kyl/process_landslide_susceptibility.py \
  --aoi_geojson ./aoi.geojson \
  --out_raster_asset users/<yourname>/kyl_susceptibility_100m \
  --out_vector_asset users/<yourname>/kyl_susceptibility_polys \
  --scale 100 \
  --weights_json ./weights.json
```

Notes & Next steps
- The current implementation uses default/example datasets (SRTM, CHIRPS, Copernicus LULC). Supply dataset overrides via the `--dataset_overrides` JSON file with keys `dem`, `lulc`, `rainfall`, `soil`, `flow_acc` and Earth Engine asset IDs as values.
- Flow accumulation and accurate curvature are important for robust susceptibility modeling; producing those may require hydrological preprocessing or external assets. The script includes a placeholder proxy for flow accumulation — replace with a proper D8/flow accumulation asset for production.
- Vectorization uses `reduceToVectors()` and computes area (ha) and mean metrics per polygon. Export is triggered via Earth Engine Tasks — check the Tasks tab or list with `ee.batch.Task.list()`.
- After producing assets, use `ee_code_editor.js` to visualize or add the published asset ID.

Validation
- The repository includes placeholders for validation steps. To validate, provide a historical landslide inventory and run spatial overlap checks (e.g., confusion matrix by class) — this will be implemented next.

Contact
- For dataset/weight choices and domain validation, coordinate with mentors: @amanodt, @ankit-work7, @kapildadheech
