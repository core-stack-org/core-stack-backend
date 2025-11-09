"""Landslide susceptibility processing using Google Earth Engine (GEE).

This script provides a configurable workflow to:
- load an Area of Interest (AOI)
- load DEM and ancillary datasets (LULC, rainfall, soil, flow accumulation)
- compute topographic indices (slope, aspect) and prepare layers
- compute a weighted susceptibility score (weights configurable)
- classify the score into low/moderate/high
- vectorize results and compute polygon attributes
- export raster and vector assets to Earth Engine

Notes / assumptions:
- This is a scaffold implementation. Some datasets (flow accumulation, soil metrics)
  may not have direct public IDs; you can provide asset IDs via CLI arguments.
- The exact weights/thresholds from the paper must be supplied. This script
  accepts a JSON file of weights or uses default example weights (placeholders).
- Running this script requires the Earth Engine Python API and authentication
  (run `earthengine authenticate` beforehand).

Usage (example):
  python process_landslide_susceptibility.py \
    --aoi_geojson ./aoi.geojson \
    --out_raster_asset users/you/kyl_susceptibility_100m \
    --out_vector_asset users/you/kyl_susceptibility_polys \
    --scale 100 \
    --weights_json ./weights.json

"""

from __future__ import annotations

import json
import argparse
import sys
from datetime import datetime
from typing import Dict, Optional

try:
    import ee
except Exception as e:
    ee = None


DEFAULT_WEIGHTS = {
    # Example placeholder weights. Replace with values from the paper.
    "slope": 0.4,
    "curvature": 0.1,
    "flow_acc": 0.2,
    "lulc": 0.15,
    "rainfall": 0.15,
}


def init_ee(force: bool = False):
    """Initialize the Earth Engine API.

    Raises helpful error when ee is not installed or not authenticated.
    """
    if ee is None:
        raise RuntimeError("earthengine-api is not installed. Install with `pip install earthengine-api`")
    try:
        ee.Initialize()
    except Exception:
        # Try to give a helpful message
        raise RuntimeError(
            "Could not initialize Earth Engine. Have you run `earthengine authenticate` and do you have network access?"
        )


def load_aoi_from_geojson(path: str) -> ee.Geometry:
    """Load AOI local geojson and return an ee.Geometry."""
    import json

    with open(path, "r") as fh:
        gj = json.load(fh)

    # Expect a FeatureCollection or Feature
    if gj.get("type") == "FeatureCollection":
        geom = ee.Geometry(gj["features"][0]["geometry"])  # take first feature
    elif gj.get("type") == "Feature":
        geom = ee.Geometry(gj["geometry"])
    else:
        # Might be a raw geometry
        geom = ee.Geometry(gj)
    return geom


def get_datasets(aoi: ee.Geometry, scale: int = 100, overrides: Optional[Dict] = None) -> Dict[str, ee.Image]:
    """Return a dict of ee.Image datasets used in the model.

    You can override dataset IDs by passing `overrides` with keys:
    dem, lulc, rainfall, soil, flow_acc
    """
    overrides = overrides or {}

    # DEM: default to SRTM (30m) and reproject/resample to requested scale
    dem_id = overrides.get("dem", "USGS/SRTMGL1_003")
    dem = ee.Image(dem_id).select("elevation")

    # LULC: use Copernicus or a placeholder
    lulc_id = overrides.get("lulc", "COPERNICUS/Landcover/100m/Proba-V/Global")
    try:
        lulc = ee.Image(lulc_id).select([0]).rename("lulc")
    except Exception:
        # If the above fails, create a simple 0-image as placeholder
        lulc = ee.Image.constant(0).rename("lulc").clip(aoi)

    # Rainfall: CHIRPS (precipitation) - summed over a period may be needed
    rainfall_id = overrides.get("rainfall", "UCSB-CHG/CHIRPS/DAILY")
    try:
        rainfall = ee.ImageCollection(rainfall_id).select("precipitation")
        # Example: take long-term mean (this is simplistic; better: seasonal/annual sum)
        rainfall_img = rainfall.mean().rename("rainfall")
    except Exception:
        rainfall_img = ee.Image.constant(0).rename("rainfall").clip(aoi)

    # Soil: placeholder or user-provided asset
    soil_id = overrides.get("soil")
    if soil_id:
        soil = ee.Image(soil_id).rename("soil")
    else:
        soil = ee.Image.constant(0).rename("soil").clip(aoi)

    # Flow accumulation: ideally a precomputed asset (D8) or hydrological dataset
    flow_id = overrides.get("flow_acc")
    if flow_id:
        flow = ee.Image(flow_id).rename("flow_acc")
    else:
        # Placeholder: use slope-derived proxy (NOT a replacement for real flow accumulation)
        slope = ee.Terrain.slope(dem).rename("slope")
        flow = slope.multiply(-1).rename("flow_acc").clip(aoi)

    datasets = {
        "dem": dem.clip(aoi),
        "lulc": lulc.clip(aoi),
        "rainfall": rainfall_img.clip(aoi),
        "soil": soil.clip(aoi),
        "flow_acc": flow.clip(aoi),
    }
    return datasets


def compute_topographic_indices(dem: ee.Image) -> ee.Image:
    """Compute basic topographic indices from DEM: slope, aspect.

    Curvature and other second-order derivatives are not provided out-of-the-box
    in the Earth Engine API; computing true curvature may require focal
    operations or external preprocessing. This function provides slope and
    aspect and a placeholder curvature computed from slope's Laplacian-like kernel.
    """
    slope = ee.Terrain.slope(dem).rename("slope")
    aspect = ee.Terrain.aspect(dem).rename("aspect")

    # Approximate curvature with a simple 3x3 Laplacian kernel applied to the DEM
    kernel = ee.Kernel.fixed(3, 3, [[1, 1, 1], [1, -8, 1], [1, 1, 1]], -1, False)
    lap = dem.convolve(kernel).rename("curvature")

    return slope.addBands(aspect).addBands(lap)


def normalize_image(img: ee.Image, band: str, aoi: ee.Geometry, scale: int) -> ee.Image:
    """Normalize band to 0..1 using min/max over AOI (robust to outliers?)."""
    stats = img.select(band).reduceRegion(
        reducer=ee.Reducer.percentile([2, 98]),
        geometry=aoi,
        scale=scale,
        bestEffort=True,
        maxPixels=1e13,
    )
    p2 = ee.Number(stats.get(band + "_p2"))
    p98 = ee.Number(stats.get(band + "_p98"))
    # handle equal p2/p98
    denom = p98.subtract(p2)
    denom = ee.Algorithms.If(denom.eq(0), 1, denom)
    norm = img.select(band).subtract(p2).divide(denom).clamp(0, 1).rename(band + "_norm")
    return norm


def compute_susceptibility_score(datasets: Dict[str, ee.Image], aoi: ee.Geometry, weights: Dict, scale: int = 100) -> ee.Image:
    """Compute weighted susceptibility score (0..1) from dataset images and weights.

    weights keys: slope, curvature, flow_acc, lulc, rainfall, soil (optional)
    """
    dem = datasets["dem"]
    topo = compute_topographic_indices(dem)
    slope = topo.select("slope")
    curvature = topo.select("curvature")

    # Normalize continuous layers
    slope_n = normalize_image(slope, "slope", aoi, scale)
    curvature_n = normalize_image(curvature, "curvature", aoi, scale)

    flow = datasets.get("flow_acc")
    flow_n = normalize_image(flow.rename("flow_acc"), "flow_acc", aoi, scale)

    rainfall = datasets.get("rainfall")
    rainfall_n = normalize_image(rainfall.select([0]).rename("rainfall"), "rainfall", aoi, scale)

    # LULC: categorical - map to susceptibility scores using a simple lookup
    lulc = datasets.get("lulc")
    # Default mapping: produce a normalized continuous score via remapping classes
    # (Users should supply domain-specific mapping)
    lulc_map = {0: 0.1, 10: 0.1, 20: 0.4, 30: 0.7, 40: 0.9}
    # Create a default image where known classes are mapped; unknowns -> 0.2
    def map_lulc_to_score(img):
        out = ee.Image.constant(0.2)
        for k, v in lulc_map.items():
            out = out.where(img.eq(k), ee.Image.constant(v))
        return out.rename("lulc_score")

    lulc_score = map_lulc_to_score(lulc.select([0]))

    # Weighted sum (weights should sum to 1 ideally)
    w = weights
    components = []
    components.append(slope_n.multiply(w.get("slope", 0)))
    components.append(curvature_n.multiply(w.get("curvature", 0)))
    components.append(flow_n.multiply(w.get("flow_acc", 0)))
    components.append(lulc_score.multiply(w.get("lulc", 0)))
    components.append(rainfall_n.multiply(w.get("rainfall", 0)))

    # Soil can be added similarly
    img_sum = ee.Image(0)
    for c in components:
        img_sum = img_sum.add(c)

    # Normalize to 0..1 by dividing by sum of weights (in case they don't sum to 1)
    total_w = sum([w.get(k, 0) for k in ["slope", "curvature", "flow_acc", "lulc", "rainfall", "soil"]])
    if total_w == 0:
        total_w = 1
    susceptibility = img_sum.divide(total_w).rename("susceptibility")
    return susceptibility


def classify_image(sus_img: ee.Image, thresholds: Optional[Dict[str, float]] = None) -> ee.Image:
    """Classify susceptibility image into classes 1: low, 2: moderate, 3: high.

    thresholds: dict with 'low_mod' and 'mod_high' threshold values in 0..1.
    If not provided, default to quantile-based thresholds [0.33, 0.66].
    """
    if thresholds is None:
        thresholds = {"low_mod": 0.33, "mod_high": 0.66}

    s = sus_img.select("susceptibility")
    classified = s.lt(thresholds["low_mod"]).multiply(1)
    classified = classified.add(s.gte(thresholds["low_mod"]).And(s.lt(thresholds["mod_high"])).multiply(2))
    classified = classified.add(s.gte(thresholds["mod_high"]).multiply(3)).rename("sus_class")
    return classified


def vectorize_and_add_metrics(class_img: ee.Image, metrics_imgs: Dict[str, ee.Image], aoi: ee.Geometry, scale: int, out_vector_asset: Optional[str] = None):
    """Vectorize classified raster into polygons and compute attributes.

    Attributes added: sus_class, area_ha, mean_slope, mean_curvature, mean_lulc, mean_rainfall
    The resulting feature collection can be exported to an EE asset.
    """
    # Use reduceToVectors to get polygons for each class
    vectors = class_img.addBands(class_img).reduceToVectors(
        geometry=aoi,
        crs=class_img.projection(),
        scale=scale,
        geometryType="polygon",
        eightConnected=True,
        labelProperty="sus_class",
        maxPixels=1e13,
    )

    # Compute area and zonal stats
    def compute_attrs(feature):
        geom = feature.geometry()
        area_ha = geom.area().divide(10000)
        stats = {}
        reducers = ee.Reducer.mean()
        # Build list of images to reduce
        imgs = []
        names = []
        for k, img in metrics_imgs.items():
            imgs.append(img.rename(k))
            names.append(k)

        stacked = ee.Image.cat(imgs)
        reduced = stacked.reduceRegion(
            reducer=reducers,
            geometry=geom,
            scale=scale,
            bestEffort=True,
            maxPixels=1e13,
        )

        props = feature.toDictionary()
        props = props.set("area_ha", area_ha)
        for n in names:
            props = props.set(f"mean_{n}", reduced.get(n))
        return ee.Feature(geom, props)

    vectors_with_attrs = vectors.map(compute_attrs)

    # Optionally export to asset (triggered externally)
    if out_vector_asset:
        task = ee.batch.Export.table.toAsset(
            collection=vectors_with_attrs,
            description="export_vectors_{}".format(datetime.utcnow().strftime("%Y%m%dT%H%M%S")),
            assetId=out_vector_asset,
        )
        task.start()
        print("Started export task for vectors:", out_vector_asset)

    return vectors_with_attrs


def export_raster_to_asset(img: ee.Image, out_asset_id: str, aoi: ee.Geometry, scale: int, description: Optional[str] = None):
    description = description or f"export_raster_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    task = ee.batch.Export.image.toAsset(
        image=img,
        description=description,
        assetId=out_asset_id,
        region=aoi,
        scale=scale,
        maxPixels=1e13,
    )
    task.start()
    print("Started export task for raster:", out_asset_id)


def main(args=None):
    parser = argparse.ArgumentParser(description="Compute landslide susceptibility with GEE")
    parser.add_argument("--aoi_geojson", help="Path to AOI GeoJSON file", required=True)
    parser.add_argument("--out_raster_asset", help="EE asset id for output raster", required=True)
    parser.add_argument("--out_vector_asset", help="EE asset id for output vectors", required=True)
    parser.add_argument("--scale", type=int, default=100, help="Output resolution in meters (default 100)")
    parser.add_argument("--weights_json", help="JSON file containing model weights (optional)")
    parser.add_argument("--dataset_overrides", help="JSON file with dataset overrides (optional)")
    parsed = parser.parse_args(args=args)

    init_ee()

    aoi = load_aoi_from_geojson(parsed.aoi_geojson)

    overrides = None
    if parsed.dataset_overrides:
        with open(parsed.dataset_overrides, "r") as fh:
            overrides = json.load(fh)

    datasets = get_datasets(aoi, scale=parsed.scale, overrides=overrides)

    weights = DEFAULT_WEIGHTS
    if parsed.weights_json:
        with open(parsed.weights_json, "r") as fh:
            w = json.load(fh)
            # Validate keys
            weights.update(w)

    sus = compute_susceptibility_score(datasets, aoi, weights, scale=parsed.scale)

    classified = classify_image(sus)

    # Export raster to asset
    export_raster_to_asset(sus, parsed.out_raster_asset, aoi, parsed.scale)

    # Vectorize and compute metrics
    metrics_imgs = {
        "slope": ee.Terrain.slope(datasets["dem"]),
        "curvature": compute_topographic_indices(datasets["dem"]).select("curvature"),
        "lulc": datasets["lulc"],
        "rainfall": datasets["rainfall"],
    }

    vecs = vectorize_and_add_metrics(classified, metrics_imgs, aoi, parsed.scale, out_vector_asset=parsed.out_vector_asset)

    print("Vectorization triggered. Feature count (estimated):", vecs.size().getInfo())
    print("Processing started. Monitor tasks in your Earth Engine Tasks tab or via ee.batch.Task.list().")


if __name__ == "__main__":
    main()
