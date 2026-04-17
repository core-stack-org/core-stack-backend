#!/usr/bin/env python3
"""
Add Geometry to Closest-Facilities CSV — State-wise GeoJSON Joiner.

Reads a CSV with village-level closest-facility distances (keyed by
censuscode2011) and joins polygon/multipolygon geometry from state-wise
GeoJSON files, producing per-state GeoJSON output files.

The script is designed for **large datasets** (600K+ villages, GeoJSON files
up to 700 MB each).  It uses:
  - **polars** for fast CSV operations
  - **orjson** (or stdlib json) for GeoJSON read/write
  - State-by-state processing to keep memory bounded

Usage:
------

  # Default paths (pan-India run):
  python add_geometry.py ^
      --csv  "data/closest_facilities/closest_facilities.csv" ^
      --geojson-dir "data/statewise_base_geojsons" ^
      --output-dir  "data/statewise_geojsons_facilities"

  # Process only specific states:
  python add_geometry.py ^
      --csv  data/closest_facilities.csv ^
      --geojson-dir data/statewise_geojsons ^
      --output-dir  data/out ^
      --states "Bihar" "Uttar Pradesh"

  # Keep only distance columns in the output GeoJSON properties:
  python add_geometry.py ^
      --csv data/closest_facilities.csv ^
      --geojson-dir data/statewise_geojsons ^
      --output-dir data/out ^
      --distance-only

  # Return ALL geometries from shapefiles (even if no CSV match), with NULL for missing *_distance:
  python add_geometry.py ^
      --csv data/closest_facilities.csv ^
      --geojson-dir data/statewise_geojsons ^
      --output-dir data/out ^
      --return-all-geometries

  # Create only India-wide combined file (skip individual state files):
  python add_geometry.py ^
      --csv data/closest_facilities.csv ^
      --geojson-dir data/statewise_geojsons ^
      --output-dir data/out ^
      --return-all-geometries ^
      --india-wide ^
      --skip-state-files

  # Rename columns using mapping:
  python add_geometry.py ^
      --csv data/closest_facilities.csv ^
      --geojson-dir data/statewise_geojsons ^
      --output-dir data/out ^
      --return-all-geometries ^
      --rename-columns "censuscode:censuscode2011,censusco_1:censuscode2001,lgd_subdis:lgd_subdistrict,lgd_distri:lgd_district,lgd_statec:lgd_state"

  # Last Usage
  python add_geometry_v1.py --csv "data/closest_facilities/statewise_closest_facilities.csv" --geojson-dir "data/statewise_base_geojsons" --output-dir "data/statewise_geojsons_facilities" --return-all-geometries --india-wide      

Dependencies:
  pip install polars orjson   # orjson is optional (falls back to json)
"""

import argparse
import os
import sys
import time
import zipfile
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Iterator, Tuple

try:
    import polars as pl
except ImportError:
    print("Error: polars is required.  pip install polars")
    sys.exit(1)

# Prefer orjson for speed; fall back to stdlib json
try:
    import orjson

    def json_loads(b: bytes) -> Any:
        return orjson.loads(b)

    def json_dumps(obj: Any) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2)

    JSON_ENGINE = "orjson"
except ImportError:
    import json

    def json_loads(b: bytes) -> Any:
        return json.loads(b)

    def json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")

    JSON_ENGINE = "json (stdlib)"


# ---------------------------------------------------------------------------
#  Column Mapping Configuration
# ---------------------------------------------------------------------------

# Default column rename mapping (from shapefile column names to standardized names)
DEFAULT_RENAME_MAP: Dict[str, str] = {
    "OBJECTID": "objectid",
    "centroid_lon": "centroid_long",
    "lgd_vill_1": "lgd_village",
    "lgd_villag": "lgd_village_name",
    "subdistric": "subdistrict",
}

# Columns to remove from output (area calculations, redundant ids, etc.)
DEFAULT_REMOVE_COLUMNS: Set[str] = {
    "area_ha",
    "area_sqm", 
    "id",
}

# Specific columns to remove for spatial index creation as requested by user
SPATIAL_INDEX_REMOVE_COLUMNS: Set[str] = {
    "OBJECTID",
    "id",
    "censuscode2011",
    "censuscode2001",    
    "censusname", 
    "lgd_villag",
    "lgd_vill_1",
    "name",
    "tru_2011",
    "level_2011", 
    "area_sqm", 
    "area_ha", 
    "centroid_lon", 
    "centroid_lat",
}

# Specific columns to keep for tehsil-level spatial indexing as requested by user
TEHSIL_WHITELIST_COLUMNS: Set[str] = {
    "lgd_state",
    "state",
    "lgd_distri",      # Original name in GeoJSON
    "lgd_district",    # Target name
    "district",
    "lgd_subdis",      # Original name in GeoJSON
    "lgd_subdistrict", # Target name
    "subdistric",
}


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def state_name_to_filename(state: str) -> str:
    """Convert 'Andaman and Nicobar Islands' → 'Andaman_and_Nicobar_Islands.geojson'."""
    return state.replace(" ", "_") + ".geojson"


def parse_rename_columns(rename_str: str) -> Dict[str, str]:
    """
    Parse a rename string like "old1:new1,old2:new2" into a dictionary.
    Returns mapping of old column names to new column names.
    """
    if not rename_str:
        return {}
    
    rename_map = {}
    for pair in rename_str.split(","):
        pair = pair.strip()
        if ":" in pair:
            old_name, new_name = pair.split(":", 1)
            rename_map[old_name.strip()] = new_name.strip()
    return rename_map


def build_distance_lookup(
    df_state: pl.DataFrame,
) -> Dict[int, Dict[str, Any]]:
    """
    Build a lookup dict: censuscode2011 → {col_name: value, ...}
    Only includes *_distance columns from the DataFrame.
    
    Uses dictionary comprehension and direct iteration for speed.
    Memory-efficient: only stores distance columns, not full row data.
    """
    # Get only distance columns
    dist_cols = [c for c in df_state.columns if c.endswith("_distance")]
    
    lookup: Dict[int, Dict[str, Any]] = {}
    
    for row in df_state.iter_rows(named=True):
        code = row.get("censuscode2011")
        if code is None:
            continue
        try:
            code_int = int(code)
        except (ValueError, TypeError):
            continue
        
        # Build dict with only distance columns
        dist_values = {}
        for col in dist_cols:
            val = row.get(col)
            # Handle NaN -> None
            if isinstance(val, float) and val != val:
                dist_values[col] = None
            else:
                dist_values[col] = val
        
        lookup[code_int] = dist_values
    
    return lookup


def get_distance_columns(df: pl.DataFrame) -> List[str]:
    """Get list of columns ending with '_distance'."""
    return [c for c in df.columns if c.endswith("_distance")]


def apply_column_transforms(
    props: Dict[str, Any],
    rename_map: Dict[str, str],
    remove_columns: Set[str],
    distance_columns: List[str],
    distance_lookup: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Apply column transformations to a properties dict:
    1. Remove specified columns
    2. Rename columns according to rename_map
    3. Add distance columns from lookup (NULL if not found)
    
    Returns transformed properties dict.
    """
    new_props = {}
    
    # Lowercase remove_columns for case-insensitive matching
    remove_lower = {c.lower() for c in remove_columns}
    
    # Process existing properties
    for key, value in props.items():
        # Skip removed columns (check both original and lowercase key)
        if key in remove_columns or key.lower() in remove_lower:
            continue
        
        # Determine new key name
        new_key = rename_map.get(key, key)
        
        # Convert NaN to None for JSON serialization
        if isinstance(value, float) and value != value:
            new_props[new_key] = None
        else:
            new_props[new_key] = value
    
    # Add distance columns from lookup
    for col in distance_columns:
        if distance_lookup:
            val = distance_lookup.get(col)
            # Handle NaN
            if isinstance(val, float) and val != val:
                new_props[col] = None
            else:
                new_props[col] = val
        else:
            new_props[col] = None
    
    return new_props


def process_geojson_with_distances(
    geojson_path: str,
    distance_lookup: Dict[int, Dict[str, Any]],
    distance_columns: List[str],
    rename_map: Dict[str, str],
    remove_columns: Set[str],
    keep_all_geometries: bool = True,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Read a GeoJSON FeatureCollection and attach *_distance columns from lookup.
    
    If keep_all_geometries is True:
      - All features from the GeoJSON are kept
      - Features without CSV match get NULL values for *_distance columns
    
    If keep_all_geometries is False:
      - Only features with matching censuscode2011 in CSV are kept
    
    Returns tuple of (features list, matched count, unmatched count).
    """
    with open(geojson_path, "rb" if JSON_ENGINE == "orjson" else "r", encoding="utf-8" if JSON_ENGINE != "orjson" else None) as f:
        if JSON_ENGINE == "orjson":
            data = json_loads(f.read())
        else:
            data = json_loads(f.read().encode("utf-8"))
    
    features: List[Dict[str, Any]] = []
    matched = 0
    unmatched = 0
    
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        code = props.get("censuscode2011")
        
        # Also check for alternative column names
        if code is None:
            code = props.get("censuscode")
        if code is None:
            code = props.get("censusco_1")  # Sometimes censuscode2001 is in censusco_1
        
        code_int = None
        if code is not None:
            try:
                code_int = int(code)
            except (ValueError, TypeError):
                pass
        
        # Get distance values from lookup
        dist_values = distance_lookup.get(code_int) if code_int else None
        
        if dist_values is None and not keep_all_geometries:
            # Skip this feature - no CSV match and we're not keeping all
            continue
        
        # Apply column transformations
        new_props = apply_column_transforms(
            props,
            rename_map,
            remove_columns,
            distance_columns,
            dist_values,
        )
        
        features.append({
            "type": "Feature",
            "properties": new_props,
            "geometry": feat["geometry"],
        })
        
        # Track matched vs unmatched
        if dist_values is not None:
            matched += 1
        else:
            unmatched += 1
    
    return features, matched, unmatched


def stream_geojson_features(
    geojson_path: str,
    distance_lookup: Dict[int, Dict[str, Any]],
    distance_columns: List[str],
    rename_map: Dict[str, str],
    remove_columns: Set[str],
) -> Iterator[Dict[str, Any]]:
    """
    Generator that yields GeoJSON features one by one.
    Memory-efficient for building India-wide file.
    
    Always keeps all geometries (with NULL for missing CSV data).
    """
    with open(geojson_path, "rb" if JSON_ENGINE == "orjson" else "r", encoding="utf-8" if JSON_ENGINE != "orjson" else None) as f:
        if JSON_ENGINE == "orjson":
            data = json_loads(f.read())
        else:
            data = json_loads(f.read().encode("utf-8"))
    
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        code = props.get("censuscode2011")
        
        # Also check for alternative column names
        if code is None:
            code = props.get("censuscode")
        if code is None:
            code = props.get("censusco_1")
        
        code_int = None
        if code is not None:
            try:
                code_int = int(code)
            except (ValueError, TypeError):
                pass
        
        # Get distance values from lookup
        dist_values = distance_lookup.get(code_int) if code_int else None
        
        # Apply column transformations
        new_props = apply_column_transforms(
            props,
            rename_map,
            remove_columns,
            distance_columns,
            dist_values,
        )
        
        yield {
            "type": "Feature",
            "properties": new_props,
            "geometry": feat["geometry"],
        }


def write_india_wide_file_direct(
    geojson_dir: str,
    process_states: List[str],
    output_dir: str,
    df: pl.DataFrame,
    distance_columns: List[str],
    rename_map: Dict[str, str],
    remove_columns: Set[str],
) -> int:
    """
    Build India file directly from source GeoJSONs without creating intermediate state files.
    Uses streaming approach to minimize memory usage.
    
    This is the most memory-efficient method when --skip-state-files is used.
    
    Returns total features written.
    """
    india_path = os.path.join(output_dir, "India.geojson")
    print(f"\n  Building India-wide file (direct streaming): {india_path}")
    
    t_india_start = time.time()
    total_features = 0
    
    # Write opening of GeoJSON
    with open(india_path, "wb") as f_out:
        f_out.write(b'{"type":"FeatureCollection","features":[\n')
        
        first_feature = True
        
        for i, state_name in enumerate(process_states, 1):
            print(f"\n[{i}/{len(process_states)}] {state_name}")
            t1 = time.time()
            
            # Filter CSV rows for this state
            df_state = df.filter(pl.col("state") == state_name)
            n_rows = len(df_state)
            
            if n_rows == 0:
                print("    No CSV rows — keeping all geometries with NULL distances")
                distance_lookup = {}
            else:
                # Build distance lookup
                distance_lookup = build_distance_lookup(df_state)
                print(f"    CSV rows: {n_rows:,}   Distance entries: {len(distance_lookup):,}")
            
            # Find corresponding GeoJSON file
            geojson_filename = state_name_to_filename(state_name)
            geojson_path = os.path.join(geojson_dir, geojson_filename)
            
            if not os.path.exists(geojson_path):
                print(f"    ⚠ GeoJSON not found: {geojson_filename} — skipping")
                del df_state, distance_lookup
                continue
            
            # Stream features from source GeoJSON
            print(f"    Streaming {geojson_filename}…")
            state_count = 0
            
            for feat in stream_geojson_features(
                geojson_path,
                distance_lookup,
                distance_columns,
                rename_map,
                remove_columns,
            ):
                if not first_feature:
                    f_out.write(b',\n')
                first_feature = False
                f_out.write(json_dumps(feat))
                state_count += 1
                total_features += 1
            
            print(f"    ✓ {state_count:,} features in {time.time() - t1:.1f}s")
            
            # Free memory
            del df_state, distance_lookup
        
        # Close the JSON
        f_out.write(b'\n]}')
    
    print(f"\n  ✓ India-wide file written: {total_features:,} features in {time.time() - t_india_start:.1f}s")
    
    return total_features


def write_india_wide_file_streaming(
    state_output_dir: str,
    process_states: List[str],
    output_dir: str,
) -> int:
    """
    Build India file from already-processed state output files.
    Uses incremental JSON writing to minimize memory usage.
    
    Returns total features written.
    """
    india_path = os.path.join(output_dir, "India.geojson")
    print(f"\n  Building India-wide file (streaming): {india_path}")
    
    t_india_start = time.time()
    total_features = 0
    
    # Write opening of GeoJSON
    with open(india_path, "wb") as f_out:
        f_out.write(b'{"type":"FeatureCollection","features":[\n')
        
        first_feature = True
        
        for i, state_name in enumerate(process_states, 1):
            geojson_filename = state_name_to_filename(state_name)
            geojson_path = os.path.join(state_output_dir, geojson_filename)
            
            if not os.path.exists(geojson_path):
                print(f"    [{i}/{len(process_states)}] ⚠ {geojson_filename} not found, skipping")
                continue
            
            print(f"    [{i}/{len(process_states)}] Streaming {geojson_filename}…")
            
            # Read state file
            with open(geojson_path, "rb") as f_in:
                data = json_loads(f_in.read())
            
            state_features = data.get("features", [])
            state_count = len(state_features)
            
            for feat in state_features:
                if not first_feature:
                    f_out.write(b',\n')
                first_feature = False
                f_out.write(json_dumps(feat))
                total_features += 1
            
            # Free memory
            del data, state_features
            
            print(f"      Added {state_count:,} features")
        
        # Close the JSON
        f_out.write(b'\n]}')
    
    print(f"    ✓ India-wide file written: {total_features:,} features in {time.time() - t_india_start:.1f}s")
    
    return total_features


# ---------------------------------------------------------------------------
#  Main processing
# ---------------------------------------------------------------------------

def process(
    csv_path: str,
    geojson_dir: str,
    output_dir: str,
    states: Optional[List[str]] = None,
    distance_only: bool = False,
    return_all_geometries: bool = False,
    india_wide: bool = False,
    skip_state_files: bool = False,
    rename_columns: Optional[str] = None,
) -> None:
    """Run the full state-by-state join and export pipeline."""

    print("=" * 70)
    print("  Add Geometry — State-wise GeoJSON Joiner")
    print("=" * 70)
    print(f"  JSON engine:  {JSON_ENGINE}")
    print(f"  CSV:          {csv_path}")
    print(f"  GeoJSON dir:  {geojson_dir}")
    print(f"  Output dir:   {output_dir}")
    print(f"  Mode:         {'Keep ALL geometries' if return_all_geometries else 'CSV rows only'}")
    if india_wide:
        print(f"  India-wide:   Yes (will create India.geojson)")
    if skip_state_files:
        print(f"  State files:  Skipped (only India.geojson will be created)")
    print()

    # --- Build column transform config ---
    # Start with default rename map
    rename_map = DEFAULT_RENAME_MAP.copy()
    
    # Parse and merge user-provided rename mappings
    if rename_columns:
        user_renames = parse_rename_columns(rename_columns)
        rename_map.update(user_renames)
        print(f"  Column renames: {len(rename_map)} mappings")
    
    remove_columns = DEFAULT_REMOVE_COLUMNS.copy()
    
    # --- Load CSV ---
    t0 = time.time()
    print("Loading CSV …")
    df = pl.read_csv(csv_path)
    print(f"  Rows: {len(df):,}   Columns: {len(df.columns)}")
    print(f"  Loaded in {time.time() - t0:.1f}s")

    # Ensure censuscode2011 is int
    df = df.with_columns(pl.col("censuscode2011").cast(pl.Int64, strict=False))

    # Get distance columns
    distance_columns = get_distance_columns(df)
    print(f"  Distance columns found: {len(distance_columns)}")
    
    if not distance_columns:
        print("  ⚠ No *_distance columns found in CSV!")

    # --- Determine states ---
    all_csv_states = df["state"].unique().sort().to_list()

    if states:
        # Validate
        valid = [s for s in states if s in all_csv_states]
        invalid = [s for s in states if s not in all_csv_states]
        if invalid:
            print(f"\n  ⚠ States not found in CSV: {invalid}")
        process_states = valid
    else:
        process_states = all_csv_states

    print(f"\n  States to process: {len(process_states)}")

    # --- Create output dir ---
    os.makedirs(output_dir, exist_ok=True)

    # --- Determine processing mode ---
    total_features = 0
    total_matched = 0
    total_unmatched = 0
    skipped_states: List[str] = []
    total_start = time.time()
    
    # If skip_state_files is True, use direct streaming to India.geojson
    if skip_state_files and india_wide:
        total_features = write_india_wide_file_direct(
            geojson_dir,
            process_states,
            output_dir,
            df,
            distance_columns,
            rename_map,
            remove_columns,
        )
        # Note: matched/unmatched counts not tracked in direct streaming mode
    else:
        # Process state by state and write individual files
        for i, state_name in enumerate(process_states, 1):
            print(f"\n[{i}/{len(process_states)}] {state_name}")
            t1 = time.time()

            # Filter CSV rows for this state
            df_state = df.filter(pl.col("state") == state_name)
            n_rows = len(df_state)
            if n_rows == 0:
                print("    No CSV rows — skipping")
                skipped_states.append(state_name)
                continue

            # Unique census codes for this state
            codes = set(df_state["censuscode2011"].drop_nulls().to_list())
            print(f"    CSV rows: {n_rows:,}   Unique codes: {len(codes):,}")

            # Find corresponding GeoJSON file
            geojson_filename = state_name_to_filename(state_name)
            geojson_path = os.path.join(geojson_dir, geojson_filename)

            if not os.path.exists(geojson_path):
                print(f"    ⚠ GeoJSON not found: {geojson_filename} — skipping")
                skipped_states.append(state_name)
                continue

            # Build distance lookup from CSV
            print(f"    Building distance lookup from CSV …")
            distance_lookup = build_distance_lookup(df_state)
            print(f"    Distance entries: {len(distance_lookup):,}")

            # Process GeoJSON: add distance columns to features
            print(f"    Processing {geojson_filename} …")
            t_geo = time.time()
            features, matched, unmatched = process_geojson_with_distances(
                geojson_path,
                distance_lookup,
                distance_columns,
                rename_map,
                remove_columns,
                keep_all_geometries=return_all_geometries,
            )
            
            total_features += len(features)
            total_matched += matched
            total_unmatched += unmatched
            
            print(f"    Features: {len(features):,} (matched: {matched:,}, unmatched: {unmatched:,})  ({time.time() - t_geo:.1f}s)")

            # Write output GeoJSON
            output_path = os.path.join(output_dir, geojson_filename)
            geojson_out = {
                "type": "FeatureCollection",
                "features": features,
            }

            print(f"    Writing {len(features):,} features → {geojson_filename}")
            t_write = time.time()
            with open(output_path, "wb") as f_out:
                f_out.write(json_dumps(geojson_out))
            print(f"    ✓ Done in {time.time() - t1:.1f}s  (write: {time.time() - t_write:.1f}s)")

            # Free memory
            del distance_lookup, features, geojson_out, df_state

        total_elapsed = time.time() - total_start
        
        # --- Build India-wide file if requested ---
        india_features = 0
        if india_wide and return_all_geometries and not skip_state_files:
            # Use streaming approach: read from already-written state files
            india_features = write_india_wide_file_streaming(output_dir, process_states, output_dir)

    total_elapsed = time.time() - total_start

    print(f"\n{'=' * 70}")
    print(f"  COMPLETE")
    print(f"{'=' * 70}")
    if skip_state_files:
        print(f"  Total features written: {total_features:,}")
    else:
        print(f"  Total features written: {total_features:,}")
        print(f"    Matched (with CSV data): {total_matched:,}")
        if return_all_geometries:
            print(f"    Unmatched (NULL distances): {total_unmatched:,}")
        if india_wide:
            print(f"  India-wide file: {india_features:,} features")
        if skipped_states:
            print(f"  Skipped states:         {', '.join(skipped_states)}")
    print(f"  Output directory:       {output_dir}")
    print(f"  Total time:             {total_elapsed:.1f}s")


def create_spatial_index(
    geojson_dir: str,
    output_zip: str,
    states: Optional[List[str]] = None,
    remove_columns: Optional[Set[str]] = None,
    tehsil_level: bool = False,
) -> None:
    """
    Create a zipped collection of Shapefiles from GeoJSON files.
    Optimized for speed and memory efficiency.
    """
    import geopandas as gpd
    import numpy as np

    print("=" * 70)
    print("  Spatial Index Creator — GeoJSON to Zipped Shapefiles")
    print("=" * 70)
    print(f"  GeoJSON dir:  {geojson_dir}")
    print(f"  Output ZIP:   {output_zip}")
    print(f"  Mode:         {'Tehsil-level (Whitelist)' if tehsil_level else 'Full-level (Blacklist)'}")
    
    if tehsil_level:
        print(f"  Keeping columns: {', '.join(sorted(TEHSIL_WHITELIST_COLUMNS))}")
    else:
        rm_cols = remove_columns or SPATIAL_INDEX_REMOVE_COLUMNS
        print(f"  Removing columns: {', '.join(sorted(rm_cols))}")

    # Ensure output directory for the zip exists
    zip_path = Path(output_zip)
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    # Determine files to process
    if states:
        files_to_process = [state_name_to_filename(s) for s in states]
    else:
        files_to_process = [f for f in os.listdir(geojson_dir) if f.endswith(".geojson") and f != "India.geojson"]

    if not files_to_process:
        print("Error: No GeoJSON files found to process.")
        return

    print(f"  Files to process: {len(files_to_process)}")

    t_start = time.time()
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        all_gdfs: List[gpd.GeoDataFrame] = []

        for i, filename in enumerate(files_to_process, 1):
            geojson_path = os.path.join(geojson_dir, filename)
            if not os.path.exists(geojson_path):
                print(f"  [{i}/{len(files_to_process)}] ⚠ {filename} not found, skipping")
                continue

            state_name = Path(filename).stem
            print(f"  [{i}/{len(files_to_process)}] Loading {state_name}...", end=" ", flush=True)
            t_batch = time.time()

            try:
                # Use geopandas to read
                gdf = gpd.read_file(geojson_path)
                
                if tehsil_level:
                    # Whitelist logic
                    cols_to_keep = [c for c in TEHSIL_WHITELIST_COLUMNS if c in gdf.columns]
                    if 'geometry' not in cols_to_keep:
                        cols_to_keep.append('geometry')
                    gdf = gdf[cols_to_keep]
                    
                    # Casting LGD codes to int32 (as requested for GEE)
                    lgd_cols = ["lgd_state", "lgd_distri", "lgd_subdis", "lgd_district", "lgd_subdistrict"]
                    for col in lgd_cols:
                        if col in gdf.columns:
                            try:
                                gdf[col] = gdf[col].fillna(0).astype(np.int32)
                            except Exception as e:
                                print(f"Warning: Could not cast {col} to int32: {e}")
                else:
                    # Filter columns (Blacklist logic)
                    cols_to_drop = [c for c in rm_cols if c in gdf.columns]
                    # Also check common variations
                    if "centroid_lon" in rm_cols and "centroid_long" in gdf.columns:
                        cols_to_drop.append("centroid_long")

                    if cols_to_drop:
                        gdf = gdf.drop(columns=cols_to_drop)
                
                all_gdfs.append(gdf)
                print(f"OK ({len(gdf):,} features, {time.time() - t_batch:.1f}s)")
            except Exception as e:
                print(f"FAILED: {e}")

        if not all_gdfs:
            print("  ⚠ No data loaded!")
            return

        # Concatenate all GDFs
        print(f"\n  Merging {len(all_gdfs)} states into pan-India dataset...")
        t_merge = time.time()
        import pandas as pd
        india_gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs=all_gdfs[0].crs)
        print(f"  ✓ Merged {len(india_gdf):,} features in {time.time() - t_merge:.1f}s")
        
        # Free memory of individual GDFs
        del all_gdfs

        # Export to single Shapefile
        print(f"  Exporting to Shapefile (India.shp)...", end=" ", flush=True)
        t_exp = time.time()
        india_shp = tmp_path / "India.shp"
        # Handle field name truncation warning (GDAL/ESRI limit)
        india_gdf.to_file(str(india_shp), driver="ESRI Shapefile")
        print(f"Done ({time.time() - t_exp:.1f}s)")

        # Zipping the 5 component files
        shp_components = []
        for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
            comp = tmp_path / f"India{ext}"
            if comp.exists():
                shp_components.append(comp)

        print(f"\n  Zipping {len(shp_components)} component files to {output_zip}...")
        t_zip = time.time()
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for comp in shp_components:
                zipf.write(comp, arcname=comp.name)
        
        print(f"  ✓ Zipped in {time.time() - t_zip:.1f}s")

    total_elapsed = time.time() - t_start
    print(f"\n  COMPLETE: {len(files_to_process)} states processed in {total_elapsed:.1f}s")
    print(f"  Final ZIP: {output_zip}")


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Join polygon geometry from state-wise GeoJSONs to a village-level "
                    "closest-facilities CSV, producing per-state GeoJSON outputs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--csv", required=False,
        help="Path to closest_facilities CSV (e.g. closest_facilities.csv)",
    )
    parser.add_argument(
        "--geojson-dir", required=True,
        help="Directory containing state-wise GeoJSON files (e.g. Bihar.geojson)",
    )
    parser.add_argument(
        "--output-dir", required=False,
        help="Directory to write output state-wise GeoJSON files",
    )
    parser.add_argument(
        "--states", nargs="+", default=None,
        help='Process only these states (e.g. --states "Bihar" "Uttar Pradesh")',
    )
    parser.add_argument(
        "--distance-only", action="store_true",
        help="Keep only censuscode2011 + distance columns in output properties",
    )
    parser.add_argument(
        "--return-all-geometries", action="store_true",
        help="Keep ALL geometries from shapefiles. Features without CSV match get NULL values "
             "for *_distance columns. This inverts the join: adds distance columns TO shapefiles "
             "instead of adding shapes to CSV rows. All original shapefile columns are preserved.",
    )
    parser.add_argument(
        "--india-wide", action="store_true",
        help="Also create an India.geojson file combining all state files. "
             "Uses streaming approach to minimize memory usage. "
             "Requires --return-all-geometries.",
    )
    parser.add_argument(
        "--skip-state-files", action="store_true",
        help="Skip writing individual state files. Only creates India.geojson. "
             "Requires --india-wide --return-all-geometries. "
             "Most memory-efficient option for creating India-wide output.",
    )
    parser.add_argument(
        "--spatial-index", action="store_true",
        help="Create spatially indexable zipped Shapefiles from GeoJSONs.",
    )
    parser.add_argument(
        "--output-zip", type=str, default=None,
        help="Path for the output ZIP file (required if --spatial-index is used).",
    )
    parser.add_argument(
        "--tehsil-level", action="store_true",
        help="Create a tehsil-level spatial index with whitelisted columns and int32 types.",
    )
    parser.add_argument(
        "--rename-columns", type=str, default=None,
        help='Comma-separated list of column renames in format "old:new,old2:new2". '
             'Example: --rename-columns "censuscode:censuscode2011,lgd_vill_1:lgd_village"',
    )

    args = parser.parse_args()

    # Mode: Spatial Index Creation
    if args.spatial_index:
        if not args.geojson_dir:
            print("Error: --geojson-dir is required")
            sys.exit(1)
        if not args.output_zip:
            print("Error: --output-zip is required when using --spatial-index")
            sys.exit(1)
        
        create_spatial_index(
            geojson_dir=args.geojson_dir,
            output_zip=args.output_zip,
            states=args.states,
            tehsil_level=args.tehsil_level,
        )
        return

    # Mode: Join Geometry (Default)
    if not args.csv:
        print("Error: --csv is required for geometry join mode")
        sys.exit(1)
    if not args.output_dir:
        print("Error: --output-dir is required for geometry join mode")
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"Error: CSV not found: {args.csv}")
        sys.exit(1)
    if not os.path.isdir(args.geojson_dir):
        print(f"Error: GeoJSON directory not found: {args.geojson_dir}")
        sys.exit(1)
    
    if args.india_wide and not args.return_all_geometries:
        print("Error: --india-wide requires --return-all-geometries")
        sys.exit(1)
    
    if args.skip_state_files and not args.india_wide:
        print("Error: --skip-state-files requires --india-wide")
        sys.exit(1)

    process(
        csv_path=args.csv,
        geojson_dir=args.geojson_dir,
        output_dir=args.output_dir,
        states=args.states,
        distance_only=args.distance_only,
        return_all_geometries=args.return_all_geometries,
        india_wide=args.india_wide,
        skip_state_files=args.skip_state_files,
        rename_columns=args.rename_columns,
    )


if __name__ == "__main__":
    main()