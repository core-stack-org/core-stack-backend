"""
Phase 2 — Convert raw per-cell JSON files into clipped GeoParquets.

Strategy:
  1. Use DuckDB to read all raw JSON files rapidly and extract ALL landscape
     features (field, trees, dug_well, farm_pond, other_water) into memory.
  2. Hand off to GeoPandas for spatial operations: WKB geometry parsing,
     polygon clipping to the tehsil boundary, geometry validation.
  3. Write one GeoParquet file per structure type — allowing downstream
     pipelines to consume each layer independently.

Outputs:
    data/farm_boundaries/<state>/<district>/<block>/farm_boundaries.parquet
    data/farm_boundaries/<state>/<district>/<block>/trees.parquet
    data/farm_boundaries/<state>/<district>/<block>/dug_wells.parquet
    data/farm_boundaries/<state>/<district>/<block>/farm_ponds.parquet
    data/farm_boundaries/<state>/<district>/<block>/other_water.parquet

Usage (standalone / debug):
    from computing.farm_boundaries.convert import convert_to_geoparquet
    result = convert_to_geoparquet("rajasthan", "jaipur", "sanganer")
    print(result)   # {"farm_boundaries": {...}, "trees": {...}, ...}
"""

import json
import logging
import os

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from shapely.validation import make_valid

from utilities.constants import FARM_BOUNDARIES_PATH, SOI_TEHSIL

logger = logging.getLogger(__name__)

CRS = "EPSG:4326"

# Map from alu_type value in the API response to output parquet filename
ALU_TYPE_TO_PARQUET = {
    "field":      "farm_boundaries.parquet",
    "trees":      "trees.parquet",
    "dug_well":   "dug_wells.parquet",
    "farm_pond":  "farm_ponds.parquet",
    "other_water": "other_water.parquet",
}


# ── helpers ───────────────────────────────────────────────────────────────────


def _get_tehsil_polygon(state: str, district: str, block: str):
    """Re-load the tehsil polygon (shared with fetch_raw.py)."""
    soi = gpd.read_file(SOI_TEHSIL)
    mask = (
        (soi["STATE"].str.lower() == state)
        & (soi["District"].str.lower() == district)
        & (soi["TEHSIL"].str.lower() == block)
    )
    subset = soi[mask]
    if subset.empty:
        raise ValueError(
            f"Tehsil not found: state={state}, district={district}, block={block}"
        )
    return subset.dissolve().geometry.iloc[0]


def _raw_dir(state: str, district: str, block: str) -> str:
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block, "raw")


def _manifest_path(state: str, district: str, block: str) -> str:
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block, "manifest.json")


def _output_dir(state: str, district: str, block: str) -> str:
    return os.path.join(FARM_BOUNDARIES_PATH, state, district, block)


def _load_fetched_tokens(manifest_file: str) -> list:
    """Return only the tokens that have actual landscape data."""
    if not os.path.exists(manifest_file):
        raise FileNotFoundError(
            f"Manifest not found at {manifest_file}. "
            "Run Phase 1 (fetch_raw_boundaries) first."
        )
    with open(manifest_file) as f:
        manifest = json.load(f)
    return manifest.get("fetched", [])


# ── DuckDB extraction ─────────────────────────────────────────────────────────


def _extract_all_features_with_duckdb(raw_dir: str, tokens: list) -> list:
    """
    Extract ALL landscape features from raw cell JSON files.

    Uses the Python parser directly (DuckDB UNNEST-on-JSON is not supported
    in the installed DuckDB version). The Python path is fast enough for
    typical tehsil sizes (< 1000 cells).

    Returns a list of dicts, each with keys:
        cell_token, farm_uid, alu_type, geometry_geojson, properties_json
    """
    return _extract_all_features_python_fallback(raw_dir, tokens)


def _extract_all_features_python_fallback(raw_dir: str, tokens: list) -> list:
    """
    Pure-Python fallback: reads every cell JSON file and collects ALL
    landscape features. Used when DuckDB JSON parsing fails.
    """
    features = []
    for token in tokens:
        path = os.path.join(raw_dir, f"{token}.json")
        if not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as exc:
            logger.warning("Could not parse %s: %s", path, exc)
            continue

        landscape = data.get("landscape", {})
        geojson_raw = landscape.get("geojson", "")
        if not geojson_raw:
            continue

        try:
            fc = json.loads(geojson_raw) if isinstance(geojson_raw, str) else geojson_raw
        except json.JSONDecodeError as exc:
            logger.warning("Invalid GeoJSON in cell %s: %s", token, exc)
            continue

        for feat in fc.get("features", []):
            props = feat.get("properties", {})
            alu_type = props.get("alu_type", "")
            if not alu_type:
                continue
            features.append(
                {
                    "cell_token": token,
                    "plus_code": feat.get("id", ""),
                    "farm_uid": feat.get("id", ""),
                    "alu_type": alu_type,
                    "geometry_geojson": json.dumps(feat.get("geometry", {})),
                    "properties_json": json.dumps(props),
                }
            )
    return features


# ── GeoPandas spatial processing ──────────────────────────────────────────────


def _build_geodataframe(records: list) -> gpd.GeoDataFrame:
    """
    Convert the flat list of feature dicts into a GeoDataFrame.
    Parses the embedded GeoJSON geometry string into Shapely geometries.
    """
    if not records:
        return gpd.GeoDataFrame(
            columns=["farm_uid", "cell_token", "alu_type", "geometry"],
            geometry="geometry",
            crs=CRS,
        )

    rows = []
    for rec in records:
        try:
            geom_dict = (
                json.loads(rec["geometry_geojson"])
                if isinstance(rec["geometry_geojson"], str)
                else rec["geometry_geojson"]
            )
            geom = shape(geom_dict)
            if not geom.is_valid:
                geom = make_valid(geom)
        except Exception as exc:
            logger.debug("Skipping invalid geometry: %s", exc)
            continue

        # Parse properties blob for any extra attributes we want to keep.
        try:
            props = (
                json.loads(rec["properties_json"])
                if rec.get("properties_json")
                else {}
            )
        except Exception:
            props = {}

        rows.append(
            {
                "farm_uid": rec.get("farm_uid", "") or rec.get("plus_code", ""),
                "cell_token": rec.get("cell_token", ""),
                "alu_type": rec.get("alu_type") or props.get("alu_type", "field"),
                "plus_code": rec.get("plus_code", ""),       # from feature-level id
                "area_m2": props.get("area_sq_m", None),
                "class_confidence": props.get("class_confidence", None),
                "capture_date": props.get("capture_timestamp_sec", None),
                "geometry": geom,
            }
        )

    if not rows:
        return gpd.GeoDataFrame(geometry=[], crs=CRS)

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)
    return gdf


def _assign_farm_ids(
    gdf: gpd.GeoDataFrame, state: str, district: str, block: str
) -> gpd.GeoDataFrame:
    """
    Assign a unique, human-readable farm_id to every row.
    Format: <state>_<district>_<block>_<zero-padded index>
    """
    prefix = f"{state}_{district}_{block}"
    gdf = gdf.reset_index(drop=True)
    gdf["farm_id"] = [f"{prefix}_{i:06d}" for i in gdf.index]
    return gdf


# ── public entry point ───────────────────────────────────────────────────────


def convert_to_geoparquet(
    state: str,
    district: str,
    block: str,
    overwrite: bool = False,
) -> dict:
    """
    Phase 2 pipeline: read raw cell JSON files, extract ALL structure types,
    clip them to the tehsil boundary, and write one GeoParquet per type.

    Structure types:
        field       -> farm_boundaries.parquet
        trees       -> trees.parquet
        dug_well    -> dug_wells.parquet
        farm_pond   -> farm_ponds.parquet
        other_water -> other_water.parquet

    Parameters
    ----------
    state, district, block : str
        Lower-cased administrative names.
    overwrite : bool
        If False (default), skip any structure whose parquet already exists.
        If True, regenerate all parquets.

    Returns
    -------
    dict
        Top-level keys: each alu_type name -> {path, count, skipped}.
        Also includes 'path' pointing to farm_boundaries.parquet for
        backward compatibility with the Celery task.
    """
    out_dir = _output_dir(state, district, block)
    os.makedirs(out_dir, exist_ok=True)

    logger.info(
        "Phase 2 — converting raw JSON to GeoParquets for %s/%s/%s",
        state, district, block,
    )

    # 1. Load manifest --------------------------------------------------------
    manifest_file = _manifest_path(state, district, block)
    fetched_tokens = _load_fetched_tokens(manifest_file)
    logger.info("%d cells with landscape data to process.", len(fetched_tokens))

    if not fetched_tokens:
        logger.warning("No data cells found in manifest. Returning empty result.")
        return {"path": None, "farm_count": 0, "state": state, "district": district, "block": block}

    # 2. Check which structure types still need processing --------------------
    structures_to_process = {}
    all_results = {}
    for alu_type, parquet_name in ALU_TYPE_TO_PARQUET.items():
        out_path = os.path.join(out_dir, parquet_name)
        if not overwrite and os.path.exists(out_path):
            logger.info("  [%s] Already exists at %s — skipping.", alu_type, out_path)
            all_results[alu_type] = {"path": out_path, "skipped": True}
        else:
            structures_to_process[alu_type] = out_path

    if not structures_to_process:
        logger.info("All structure parquets already exist. Use overwrite=True to regenerate.")
        # Backward compatibility: return path to farm_boundaries.parquet
        farm_path = os.path.join(out_dir, ALU_TYPE_TO_PARQUET["field"])
        return {"path": farm_path, "skipped": True, "all_structures": all_results}

    # 3. Extract ALL features from raw JSON -----------------------------------
    raw_dir = _raw_dir(state, district, block)
    logger.info("Extracting all features from %d cells...", len(fetched_tokens))
    all_records = _extract_all_features_with_duckdb(raw_dir, fetched_tokens)
    logger.info("Total features extracted across all types: %d", len(all_records))

    # 4. Load tehsil boundary once (shared for all structure clips) -----------
    tehsil_geom = _get_tehsil_polygon(state, district, block)
    tehsil_gdf = gpd.GeoDataFrame(geometry=[tehsil_geom], crs=CRS)

    # 5. Build GeoDataFrame for ALL records -----------------------------------
    full_gdf = _build_geodataframe(all_records)
    logger.info("Built GeoDataFrame: %d total features.", len(full_gdf))

    # 6. Per-structure-type: filter, clip, assign IDs, save -------------------
    for alu_type, out_path in structures_to_process.items():
        logger.info("  [%s] Processing...", alu_type)

        # Filter to this structure type
        subset = full_gdf[full_gdf["alu_type"] == alu_type].copy()
        if subset.empty:
            logger.info("  [%s] No features found — writing empty parquet.", alu_type)
            subset.to_parquet(out_path, index=False)
            all_results[alu_type] = {"path": out_path, "count": 0, "skipped": False}
            continue

        logger.info("  [%s] %d raw features before clipping.", alu_type, len(subset))

        # Clip to tehsil boundary
        subset = gpd.clip(subset, tehsil_gdf)
        logger.info("  [%s] %d features after clipping.", alu_type, len(subset))

        # Assign unique IDs (prefix differs per type)
        prefix_map = {
            "field": f"{state}_{district}_{block}",
            "trees": f"{state}_{district}_{block}_tree",
            "dug_well": f"{state}_{district}_{block}_well",
            "farm_pond": f"{state}_{district}_{block}_pond",
            "other_water": f"{state}_{district}_{block}_water",
        }
        prefix = prefix_map.get(alu_type, f"{state}_{district}_{block}_{alu_type}")
        subset = subset.reset_index(drop=True)
        subset["feature_id"] = [f"{prefix}_{i:06d}" for i in subset.index]
        # Keep farm_id alias for fields (backward compatibility)
        if alu_type == "field":
            subset["farm_id"] = subset["feature_id"]

        # Reorder columns
        priority_cols = ["feature_id", "farm_id", "farm_uid", "cell_token",
                         "alu_type", "plus_code", "area_m2", "class_confidence",
                         "capture_date", "geometry"]
        existing = [c for c in priority_cols if c in subset.columns]
        subset = subset[existing]

        # Save
        subset.to_parquet(out_path, index=False)
        logger.info("  [%s] Saved %d features -> %s", alu_type, len(subset), out_path)
        all_results[alu_type] = {"path": out_path, "count": len(subset), "skipped": False}

    # Backward compatibility: top-level 'path' points to farm_boundaries.parquet
    farm_info = all_results.get("field", {})
    summary = {
        "state": state,
        "district": district,
        "block": block,
        "path": farm_info.get("path"),
        "farm_count": farm_info.get("count", 0),
        "all_structures": all_results,
    }
    logger.info("Phase 2 complete: %s", summary)
    return summary
