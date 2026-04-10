#!/usr/bin/env python3
"""
Closest Facility Finder — KDTree-based nearest-neighbor distance calculator.

Computes the shortest distance (in km) from each centroid point to the nearest
facility point, using a 3D Cartesian KDTree or BallTree with haversine metric.

Supports two modes:
  - 'single': One centroid file + one facility file → one output file
  - 'batch':  One centroid file + a directory of facility CSVs → one merged output

Usage:
------

  ——— SINGLE MODE ———
  Basic usage with auto-detected lat/lon columns
  python closest_facility_finder.py single centroids.csv facilities.csv output.csv

  Specify custom column names
  python closest_facility_finder.py single centroids.csv facilities.csv output.csv \\
      --centroid-lat lat --centroid-lon lon \\
      --facility-lat school_lat --facility-lon school_long

  # Find 3 nearest facilities
  python closest_facility_finder.py single centroids.csv facilities.csv output.csv -k 3

  Use haversine metric (more accurate for long distances)
  python closest_facility_finder.py single centroids.csv facilities.csv output.csv --use-haversine

  ——— BATCH MODE ———
  Process ALL facility CSVs in a directory against village centroids.
  Auto-detects *_lat / *_long column pairs in each facility file.
  Skips files without detectable coordinates.
  Output: village attribute columns + one {filename}_distance column per facility.
  
  Pan-India village centroids → closest facility distances:

    python closest_facility_finder.py batch --centroids "data\village_centroids\india_village_centroids.csv" --facility-dir "data\facilities\cleaned" --output "data\closest_facilities\closest_facilities.csv"

  Batch mode with haversine (slower, more precise on very long distances):
    python closest_facility_finder.py batch --centroids "data\village_centroids\india_village_centroids.csv" --facility-dir "data\facilities\cleaned" --output "data\closest_facilities\closest_facilities.csv" --use-haversine

Dependencies:
  pip install numpy pandas scipy scikit-learn
"""

import csv
import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.spatial import KDTree
from typing import Tuple, Optional, List
import warnings

# Default column name candidates for auto-detection
DEFAULT_LAT_COLUMNS = [
    "lat",
    "latitude",
    "y",
    "Latitude",
    "LAT",
    "LATITUDE"
]

DEFAULT_LON_COLUMNS = [
    "lon",
    "longitude",
    "x",
    "Longitude",
    "LON",
    "LONGITUDE"
]

DEFAULT_ID_COLUMNS = [
    "id",
    "objectid",
    "ID",
    "OBJECTID",
    "Id",
    "ObjectId"
]


# ---------------------------------------------------------------------------
#  Column helpers
# ---------------------------------------------------------------------------

def find_column(df: pd.DataFrame, candidates: list, default: Optional[str] = None) -> Optional[str]:
    """
    Find a column in DataFrame from a list of candidates.

    Args:
        df: Input DataFrame
        candidates: List of column names to search for
        default: Default column name to return if none found

    Returns:
        Column name or None
    """
    for col in candidates:
        if col in df.columns:
            return col
    return default


def find_latlon_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """
    Auto-detect latitude and longitude columns.

    Searches in order:
      1. Standard names (latitude, longitude, lat, lon, …)
      2. Suffix patterns: *_lat / *_long

    Returns:
        (lat_col, lon_col) — either or both may be None.
    """
    # 1) standard names
    lat_col = find_column(df, DEFAULT_LAT_COLUMNS)
    lon_col = find_column(df, DEFAULT_LON_COLUMNS)
    if lat_col and lon_col:
        return lat_col, lon_col

    # 2) suffix patterns: *_lat / *_long  or *_latitude / *_longitude
    cols = list(df.columns)
    if lat_col is None:
        for c in cols:
            lc = c.lower()
            if lc.endswith('_lat') or lc.endswith('_latitude'):
                lat_col = c
                break
    if lon_col is None:
        for c in cols:
            lc = c.lower()
            if lc.endswith('_long') or lc.endswith('_longitude') or lc.endswith('_lon'):
                lon_col = c
                break

    return lat_col, lon_col


# ---------------------------------------------------------------------------
#  Coordinate math
# ---------------------------------------------------------------------------

def lonlat_to_xyz(lat: np.ndarray, lon: np.ndarray, earth_radius_km: float = 6371.0) -> np.ndarray:
    """
    Convert latitude/longitude to 3D Cartesian coordinates.

    Args:
        lat: Latitude in radians
        lon: Longitude in radians
        earth_radius_km: Earth's radius in kilometers

    Returns:
        Array of shape (n, 3) with x, y, z coordinates
    """
    x = earth_radius_km * np.cos(lat) * np.cos(lon)
    y = earth_radius_km * np.cos(lat) * np.sin(lon)
    z = earth_radius_km * np.sin(lat)
    return np.column_stack((x, y, z))


def haversine_distance(lat1: np.ndarray, lon1: np.ndarray,
                       lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """
    Calculate haversine distance between two sets of points.

    Args:
        lat1, lon1: First set of coordinates in radians
        lat2, lon2: Second set of coordinates in radians

    Returns:
        Distance in kilometers
    """
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    return 6371.0 * c


# ---------------------------------------------------------------------------
#  Core nearest-neighbor search
# ---------------------------------------------------------------------------

def find_closest_facilities_kdtree(
    df_centroids: pd.DataFrame,
    df_facilities: pd.DataFrame,
    centroid_lat_col: str,
    centroid_lon_col: str,
    facility_lat_col: str,
    facility_lon_col: str,
    k: int = 1,
    earth_radius_km: float = 6371.0
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Find closest facilities using KD-Tree with 3D Cartesian coordinates.

    Returns:
        Tuple of (distances, indices)
    """
    v_lat = np.radians(df_centroids[centroid_lat_col].values.astype(float))
    v_lon = np.radians(df_centroids[centroid_lon_col].values.astype(float))
    f_lat = np.radians(df_facilities[facility_lat_col].values.astype(float))
    f_lon = np.radians(df_facilities[facility_lon_col].values.astype(float))

    village_coords = lonlat_to_xyz(v_lat, v_lon, earth_radius_km)
    facility_coords = lonlat_to_xyz(f_lat, f_lon, earth_radius_km)

    tree = KDTree(facility_coords)
    distances, indices = tree.query(village_coords, k=k)

    return distances, indices


def find_closest_facilities_haversine(
    df_centroids: pd.DataFrame,
    df_facilities: pd.DataFrame,
    centroid_lat_col: str,
    centroid_lon_col: str,
    facility_lat_col: str,
    facility_lon_col: str,
    k: int = 1
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Find closest facilities using BallTree with haversine metric.

    Returns:
        Tuple of (distances_km, indices)
    """
    try:
        from sklearn.neighbors import BallTree
    except ImportError:
        print("Error: scikit-learn is required for haversine metric.")
        print("Install with: pip install scikit-learn")
        sys.exit(1)

    v_lat = np.radians(df_centroids[centroid_lat_col].values.astype(float))
    v_lon = np.radians(df_centroids[centroid_lon_col].values.astype(float))
    f_lat = np.radians(df_facilities[facility_lat_col].values.astype(float))
    f_lon = np.radians(df_facilities[facility_lon_col].values.astype(float))

    village_coords = np.column_stack((v_lat, v_lon))
    facility_coords = np.column_stack((f_lat, f_lon))

    tree = BallTree(facility_coords, metric='haversine')
    distances, indices = tree.query(village_coords, k=k)

    # Convert radians to kilometres
    distances = distances * 6371.0

    return distances, indices


# ---------------------------------------------------------------------------
#  Compute distance-only for a single facility file
# ---------------------------------------------------------------------------

def compute_distance_column(
    df_centroids: pd.DataFrame,
    facility_path: str,
    centroid_lat_col: str,
    centroid_lon_col: str,
    facility_lat_col: Optional[str] = None,
    facility_lon_col: Optional[str] = None,
    use_haversine: bool = False,
) -> Optional[np.ndarray]:
    """
    Read a single facility CSV and return a 1-D array of closest-facility
    distances (km) aligned to *df_centroids*.

    Returns None if the facility file has no valid coordinate columns or
    no valid rows after dropping NaN coordinates.
    """
    df_fac = pd.read_csv(facility_path)

    # Auto-detect lat/lon if not specified
    if facility_lat_col is None or facility_lon_col is None:
        detected_lat, detected_lon = find_latlon_columns(df_fac)
        facility_lat_col = facility_lat_col or detected_lat
        facility_lon_col = facility_lon_col or detected_lon

    if not facility_lat_col or not facility_lon_col:
        return None  # no coordinate columns
    if facility_lat_col not in df_fac.columns or facility_lon_col not in df_fac.columns:
        return None

    # Drop rows with NaN coordinates in facility
    before = len(df_fac)
    df_fac = df_fac.dropna(subset=[facility_lat_col, facility_lon_col])
    if len(df_fac) == 0:
        return None
    if len(df_fac) < before:
        print(f"    Dropped {before - len(df_fac)} facility rows with NaN coords")

    if use_haversine:
        distances, _ = find_closest_facilities_haversine(
            df_centroids, df_fac,
            centroid_lat_col, centroid_lon_col,
            facility_lat_col, facility_lon_col, k=1
        )
    else:
        distances, _ = find_closest_facilities_kdtree(
            df_centroids, df_fac,
            centroid_lat_col, centroid_lon_col,
            facility_lat_col, facility_lon_col, k=1
        )

    # KDTree returns Euclidean distance in 3-D; convert to great-circle km
    if not use_haversine:
        # distances here are chord lengths in a sphere of radius R.
        # Convert: great-circle = R * 2 * arcsin(chord / (2*R))
        R = 6371.0
        distances = R * 2 * np.arcsin(np.clip(distances / (2 * R), -1, 1))

    return distances.ravel()


# ---------------------------------------------------------------------------
#  Merge helper for single mode (backward-compatible)
# ---------------------------------------------------------------------------

def merge_results(
    df_centroids: pd.DataFrame,
    df_facilities: pd.DataFrame,
    distances: np.ndarray,
    indices: np.ndarray,
    k: int = 1,
    centroid_id_col: Optional[str] = None,
    facility_id_col: Optional[str] = None,
    exclude_geometry: bool = True
) -> pd.DataFrame:
    """
    Merge results with original data.
    """
    result = df_centroids.copy()

    geometry_cols = set()
    if exclude_geometry:
        geometry_cols.update(DEFAULT_LAT_COLUMNS + DEFAULT_LON_COLUMNS)
        geometry_cols = geometry_cols.intersection(result.columns)

    for i in range(k):
        suffix = "" if k == 1 else f"_{i + 1}"
        facility_data = df_facilities.iloc[indices].copy() if k == 1 else df_facilities.iloc[indices[:, i]].copy()
        facility_data.columns = [f"facility_{col}{suffix}" for col in facility_data.columns]
        dist = distances if k == 1 else distances[:, i]
        result[f"distance_km{suffix}"] = dist
        result = pd.concat([result.reset_index(drop=True),
                            facility_data.reset_index(drop=True)], axis=1)

    if exclude_geometry:
        cols_to_drop = [col for col in result.columns if col in geometry_cols]
        result = result.drop(columns=cols_to_drop)

    return result


def _save_analysis_record(analysis_path: str, record: dict):
    """
    Append a single analysis record to the analysis CSV file.
    """
    file_exists = os.path.isfile(analysis_path)
    with open(analysis_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=record.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


# ---------------------------------------------------------------------------
#  High-level entry points
# ---------------------------------------------------------------------------

def find_closest_facilities(
    centroid_path: str,
    facility_path: str,
    output_path: str,
    centroid_lat_col: Optional[str] = None,
    centroid_lon_col: Optional[str] = None,
    centroid_id_col: Optional[str] = None,
    facility_lat_col: Optional[str] = None,
    facility_lon_col: Optional[str] = None,
    facility_id_col: Optional[str] = None,
    k: int = 1,
    use_haversine: bool = False,
    exclude_geometry: bool = True
) -> None:
    """Single-file mode: find closest facilities and save full merged output."""
    print(f"Loading datasets...")
    print(f"  Centroids: {centroid_path}")
    print(f"  Facilities: {facility_path}")

    df_centroids = pd.read_csv(centroid_path)
    df_facilities = pd.read_csv(facility_path)

    print(f"  Centroids: {len(df_centroids):,} rows")
    print(f"  Facilities: {len(df_facilities):,} rows")

    # Auto-detect columns
    if centroid_lat_col is None or centroid_lon_col is None:
        clat, clon = find_latlon_columns(df_centroids)
        centroid_lat_col = centroid_lat_col or clat
        centroid_lon_col = centroid_lon_col or clon
        if centroid_lat_col:
            print(f"  Auto-detected centroid latitude column: {centroid_lat_col}")
        if centroid_lon_col:
            print(f"  Auto-detected centroid longitude column: {centroid_lon_col}")

    if facility_lat_col is None or facility_lon_col is None:
        flat, flon = find_latlon_columns(df_facilities)
        facility_lat_col = facility_lat_col or flat
        facility_lon_col = facility_lon_col or flon
        if facility_lat_col:
            print(f"  Auto-detected facility latitude column: {facility_lat_col}")
        if facility_lon_col:
            print(f"  Auto-detected facility longitude column: {facility_lon_col}")

    if centroid_id_col is None:
        centroid_id_col = find_column(df_centroids, DEFAULT_ID_COLUMNS)
    if facility_id_col is None:
        facility_id_col = find_column(df_facilities, DEFAULT_ID_COLUMNS)

    # Validate
    for label, col, df in [
        ("centroid latitude", centroid_lat_col, df_centroids),
        ("centroid longitude", centroid_lon_col, df_centroids),
        ("facility latitude", facility_lat_col, df_facilities),
        ("facility longitude", facility_lon_col, df_facilities),
    ]:
        if not col or col not in df.columns:
            print(f"Error: Could not find {label} column. Specify it explicitly.")
            sys.exit(1)

    print(f"\nFinding {k} closest facility(ies) for each centroid...")

    if use_haversine:
        print("Using BallTree with haversine metric...")
        distances, indices = find_closest_facilities_haversine(
            df_centroids, df_facilities,
            centroid_lat_col, centroid_lon_col,
            facility_lat_col, facility_lon_col, k
        )
    else:
        print("Using KD-Tree with 3D Cartesian coordinates...")
        distances, indices = find_closest_facilities_kdtree(
            df_centroids, df_facilities,
            centroid_lat_col, centroid_lon_col,
            facility_lat_col, facility_lon_col, k
        )

    print("Merging results...")
    result = merge_results(
        df_centroids, df_facilities, distances, indices,
        k, centroid_id_col, facility_id_col, exclude_geometry
    )

    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    print(f"Saving output to: {output_path}")
    result.to_csv(output_path, index=False)

    # KDTree returns Euclidean distance in 3-D; convert to great-circle km if not haversine
    if not use_haversine:
        R = 6371.0
        dist_km = R * 2 * np.arcsin(np.clip(distances / (2 * R), -1, 1))
    else:
        dist_km = distances

    # Analysis Logging
    analysis_path = output_path.replace(".csv", "_analysis.csv")
    analysis_record = {
        "facility_name": Path(facility_path).stem,
        "total_rows": len(df_facilities), # note: already dropped nans in compute_distance_column? No, single mode loads separately.
        "valid_rows": len(df_facilities.dropna(subset=[facility_lat_col, facility_lon_col])),
        "lat_col": facility_lat_col,
        "lon_col": facility_lon_col,
        "min_dist_km": dist_km.min(),
        "mean_dist_km": dist_km.mean(),
        "median_dist_km": np.median(dist_km),
        "max_dist_km": dist_km.max(),
        "elapsed_sec": 0.0 # calculate if needed, but not easily done for single-mode snippet without more wrapping
    }
    _save_analysis_record(analysis_path, analysis_record)

    print(f"\n✓ Success! Output saved to: {output_path}")
    print(f"  Analysis saved to: {analysis_path}")
    print(f"  Total rows: {len(result):,}")
    print(f"  Total columns: {len(result.columns)}")


def find_closest_facilities_batch(
    centroid_path: str,
    facility_dir: str,
    output_path: str,
    centroid_lat_col: Optional[str] = None,
    centroid_lon_col: Optional[str] = None,
    use_haversine: bool = False,
    drop_coords: bool = True,
) -> None:
    """
    Batch mode: iterate all CSVs in *facility_dir*, compute closest-facility
    distance for each, and produce a single output CSV.

    Output columns:
        <centroid attribute columns> (minus lat/lon if drop_coords=True)
        {facility_filename_stem}_distance   (km, one per facility file)
    """
    print("=" * 70)
    print("  BATCH MODE — Closest Facility Distance Calculator")
    print("=" * 70)

    # --- Load centroids once ---
    print(f"\nLoading centroids: {centroid_path}")
    df_centroids = pd.read_csv(centroid_path)
    print(f"  Rows: {len(df_centroids):,}")

    # Auto-detect centroid lat/lon
    if centroid_lat_col is None or centroid_lon_col is None:
        clat, clon = find_latlon_columns(df_centroids)
        centroid_lat_col = centroid_lat_col or clat
        centroid_lon_col = centroid_lon_col or clon

    if not centroid_lat_col or not centroid_lon_col:
        print("Error: Cannot detect centroid lat/lon columns. Specify explicitly.")
        sys.exit(1)

    print(f"  Centroid lat column: {centroid_lat_col}")
    print(f"  Centroid lon column: {centroid_lon_col}")

    # Drop centroid rows with NaN coords
    before = len(df_centroids)
    df_centroids = df_centroids.dropna(subset=[centroid_lat_col, centroid_lon_col]).reset_index(drop=True)
    if len(df_centroids) < before:
        print(f"  Dropped {before - len(df_centroids)} centroid rows with NaN coords")

    # Pre-compute centroid 3-D coords (reuse across all facility files)
    v_lat_rad = np.radians(df_centroids[centroid_lat_col].values.astype(float))
    v_lon_rad = np.radians(df_centroids[centroid_lon_col].values.astype(float))
    centroid_xyz = lonlat_to_xyz(v_lat_rad, v_lon_rad) if not use_haversine else None
    centroid_radcoords = np.column_stack((v_lat_rad, v_lon_rad)) if use_haversine else None

    # --- Discover facility files ---
    facility_dir_path = Path(facility_dir)
    facility_files = sorted(facility_dir_path.glob('*.csv'))
    print(f"\nFound {len(facility_files)} CSV file(s) in: {facility_dir}")

    # --- Process each facility file ---
    processed = 0
    skipped: List[str] = []
    total_start = time.time()

    for fac_path in facility_files:
        stem = fac_path.stem  # e.g. "health_chc"
        col_name = f"{stem}_distance"
        print(f"\n[{processed + 1 + len(skipped)}/{len(facility_files)}] {fac_path.name}")

        t0 = time.time()
        df_fac = pd.read_csv(fac_path)
        print(f"    Rows: {len(df_fac):,}")

        # Auto-detect facility lat/lon
        flat, flon = find_latlon_columns(df_fac)
        if not flat or not flon:
            print(f"    ⚠ SKIP — no detectable lat/lon columns (cols: {list(df_fac.columns)})")
            skipped.append(fac_path.name)
            continue
        print(f"    Facility lat: {flat}, lon: {flon}")

        # Drop NaN facility coords
        bf = len(df_fac)
        df_fac = df_fac.dropna(subset=[flat, flon]).reset_index(drop=True)
        if len(df_fac) < bf:
            print(f"    Dropped {bf - len(df_fac)} facility rows with NaN coords")
        if len(df_fac) == 0:
            print(f"    ⚠ SKIP — no valid facility rows remaining")
            skipped.append(fac_path.name)
            continue

        # Compute distances
        f_lat_rad = np.radians(df_fac[flat].values.astype(float))
        f_lon_rad = np.radians(df_fac[flon].values.astype(float))

        if use_haversine:
            from sklearn.neighbors import BallTree
            facility_coords = np.column_stack((f_lat_rad, f_lon_rad))
            tree = BallTree(facility_coords, metric='haversine')
            dists, _ = tree.query(centroid_radcoords, k=1)
            distances_km = (dists * 6371.0).ravel()
        else:
            facility_xyz = lonlat_to_xyz(f_lat_rad, f_lon_rad)
            tree = KDTree(facility_xyz)
            chord_dists, _ = tree.query(centroid_xyz, k=1)
            # Convert chord distance → great-circle km
            R = 6371.0
            distances_km = (R * 2 * np.arcsin(np.clip(chord_dists / (2 * R), -1, 1))).ravel()

        df_centroids[col_name] = distances_km
        elapsed = time.time() - t0
        print(f"    ✓ Done in {elapsed:.1f}s  |  min={distances_km.min():.2f} km, "
              f"mean={distances_km.mean():.2f} km, max={distances_km.max():.2f} km")
        
        # Analysis Logging (runtime save)
        analysis_path = output_path.replace(".csv", "_analysis.csv")
        _save_analysis_record(analysis_path, {
            "facility_name": stem,
            "total_rows": bf,
            "valid_rows": len(df_fac),
            "lat_col": flat,
            "lon_col": flon,
            "min_dist_km": distances_km.min(),
            "mean_dist_km": distances_km.mean(),
            "median_dist_km": np.median(distances_km),
            "max_dist_km": distances_km.max(),
            "elapsed_sec": elapsed
        })
        
        processed += 1
    result = df_centroids.copy()
    
    # --- Drop coordinate columns from output ---
    # if drop_coords:
    #     coord_cols = [c for c in result.columns
    #                   if c.lower() in {x.lower() for x in DEFAULT_LAT_COLUMNS + DEFAULT_LON_COLUMNS}]
    #     if coord_cols:
    #         result = result.drop(columns=coord_cols)
    #         print(f"\nDropped coordinate columns from output: {coord_cols}")

    # --- Save ---
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    print(f"\nSaving output to: {output_path}")
    result.to_csv(output_path, index=False)

    # Final summary to analysis
    analysis_path = output_path.replace(".csv", "_analysis.csv")
    print(f"Analysis logged to: {analysis_path}")

    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 70}")
    print(f"  BATCH COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Processed: {processed} facility files")
    if skipped:
        print(f"  Skipped:   {len(skipped)} — {', '.join(skipped)}")
    print(f"  Output:    {output_path}")
    print(f"  Rows:      {len(result):,}")
    print(f"  Columns:   {len(result.columns)}")
    print(f"  Time:      {total_elapsed:.1f}s")
    print(f"  Columns:   {list(result.columns)}")


# ---------------------------------------------------------------------------
#  Ultrafast Single-Pass Batch Mode for Pan-India GeoJSON Aggregation
# ---------------------------------------------------------------------------

def find_closest_facilities_ultrafast(
    state_dir: str,
    facility_dir: str,
    output_zip: str,
    use_haversine: bool = False
) -> None:
    """
    Ultrafast Pan-India batch mode: reads all statewise GeoJSONs and multiple
    facility CSVs. Pre-builds KD/Ball trees for all facilities, then processes
    each state geojson once against all trees. Consolidates into a single 
    output zip file containing a GEE compatible pan-Indian layer.
    """
    try:
        import geopandas as gpd
    except ImportError:
        print("Error: geopandas is required for ultrafast processing.")
        print("Install with: pip install geopandas")
        sys.exit(1)
    
    import zipfile
    
    print("=" * 70)
    print("  ULTRAFAST MODE — Pan-India Facility Distance Consolidation")
    print("=" * 70)
    
    facility_dir_path = Path(facility_dir)
    facility_files = sorted(facility_dir_path.glob('*.csv'))
    
    if not facility_files:
        print(f"Error: No CSV files found in {facility_dir}")
        sys.exit(1)
        
    # 1. Pre-build trees for all facilities
    facility_trees = {}
    print(f"Scanning facility files in: {facility_dir}")
    for fac_path in facility_files:
        stem = fac_path.stem
        df_fac = pd.read_csv(fac_path)
        flat, flon = find_latlon_columns(df_fac)
        if not flat or not flon:
            continue
        df_fac = df_fac.dropna(subset=[flat, flon]).reset_index(drop=True)
        if len(df_fac) == 0:
            continue
            
        f_lat_rad = np.radians(df_fac[flat].values.astype(float))
        f_lon_rad = np.radians(df_fac[flon].values.astype(float))
        
        if use_haversine:
            from sklearn.neighbors import BallTree
            coords = np.column_stack((f_lat_rad, f_lon_rad))
            tree = BallTree(coords, metric='haversine')
            facility_trees[stem] = (tree, 'haversine')
        else:
            xyz = lonlat_to_xyz(f_lat_rad, f_lon_rad)
            tree = KDTree(xyz)
            facility_trees[stem] = (tree, 'kdtree')
            
    print(f"Pre-built search trees for {len(facility_trees)} facility types.")
    
    state_dir_path = Path(state_dir)
    state_files = [f for f in sorted(state_dir_path.glob('*.geojson')) if f.is_file()]
    if not state_files:
        print(f"Error: No state geojson files found in {state_dir}")
        sys.exit(1)
        
    print(f"Found {len(state_files)} state geojson files in {state_dir}")
    
    all_gdfs = []
    
    # Mapping to conform to requested column names
    rename_map = {
        'lgd_state': 'lgd_statecode',
        'lgd_district': 'lgd_districtcode',
        'lgd_subdistrict': 'lgd_subdistrictcode',
        'subdistric': 'subdistrict',
        'lgd_vill_1': 'lgd_villagecode',
        'lgd_villag': 'lgd_villagename'
    }
    
    # Requested standard output columns
    req_cols = [
        'OBJECTID', 'id', 'name', 'lgd_statecode', 'state', 'district',
        'lgd_districtcode', 'lgd_subdistrictcode', 'subdistrict',
        'lgd_villagecode', 'lgd_villagename', 'censuscode2011',
        'censuscode2001', 'censusname', 'level_2011', 'tru_2011', 'geometry'
    ]
    
    # 2. Process each state chunk
    for s_idx, state_file in enumerate(state_files):
        print(f"\n[{s_idx+1}/{len(state_files)}] Processing {state_file.name}...")
        gdf = gpd.read_file(state_file)
        
        # Ensure lat/lon centroids are present for computation
        if 'centroid_lat' not in gdf.columns or 'centroid_lon' not in gdf.columns:
            print(f"  Warning: No centroid_lat/lon found, computing dynamically...")
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                centroids = gdf.geometry.centroid
            gdf['centroid_lon'] = centroids.x
            gdf['centroid_lat'] = centroids.y
            
        gdf_clean = gdf.dropna(subset=['centroid_lat', 'centroid_lon']).copy()
        if len(gdf_clean) == 0:
            print(f"  Skipped {state_file.name} (no valid coordinates)")
            continue
            
        v_lat_rad = np.radians(gdf_clean['centroid_lat'].values.astype(float))
        v_lon_rad = np.radians(gdf_clean['centroid_lon'].values.astype(float))
        
        centroid_xyz = lonlat_to_xyz(v_lat_rad, v_lon_rad) if not use_haversine else None
        centroid_radcoords = np.column_stack((v_lat_rad, v_lon_rad)) if use_haversine else None
        
        dist_cols = {}
        for stem, (tree, metric) in facility_trees.items():
            if metric == 'haversine':
                dists, _ = tree.query(centroid_radcoords, k=1)
                distances_km = (dists * 6371.0).ravel()
            else:
                chord_dists, _ = tree.query(centroid_xyz, k=1)
                distances_km = (6371.0 * 2 * np.arcsin(np.clip(chord_dists / (2 * 6371.0), -1, 1))).ravel()
            dist_cols[f"{stem}_distance"] = distances_km
            
        # Add distance cols to that chunk
        for col, dists in dist_cols.items():
            gdf_clean[col] = dists
            
        # Rename columns to standard GEE output specification
        gdf_clean = gdf_clean.rename(columns=rename_map)
        
        # Keep requested columns that exist + distance columns
        keep_cols = [c for c in req_cols if c in gdf_clean.columns] + list(dist_cols.keys())
        gdf_clean = gdf_clean[keep_cols]
        all_gdfs.append(gdf_clean)
        
    if not all_gdfs:
        print("Error: No valid data processed from states.")
        sys.exit(1)
        
    print("\nConcatenating all state chunks into Pan-India GeoDataFrame...")
    final_gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs=all_gdfs[0].crs)
    print(f"Final Pan-India dataset has {len(final_gdf):,} features/rows.")
    
    os.makedirs(os.path.dirname(output_zip) or '.', exist_ok=True)
    
    # 3. Export to single ZIP for Earth Engine uploading
    import shutil

    if not output_zip.endswith('.zip'):
        output_zip = output_zip + '.zip'
        
    temp_dir = Path("Y:/core-stack-org/core-stack-backend/data/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # We use a unique sub-directory for this run to avoid conflicts
    run_temp_dir = temp_dir / f"shp_export_{int(time.time())}"
    run_temp_dir.mkdir(parents=True, exist_ok=True)
    
    shp_name = "pan_india_facilities"
    shp_path = run_temp_dir / f"{shp_name}.shp"
    
    print(f"Exporting massive Pan-India payload to ESRI Shapefile: {shp_path}")
    
    # Note: Shapefile DBF limits column names to 10 characters. GeoPandas will truncate them automatically.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")
        # Ensure CRS is set correctly if it was missing (GeoJSON usually WGS84, but GEE likes PRJ)
        if final_gdf.crs is None:
             final_gdf.set_crs(epsg=4326, inplace=True)
             
        final_gdf.to_file(str(shp_path), driver="ESRI Shapefile")
        
    print(f"Zipping shapefile components (.shp, .shx, .dbf, .prj, etc.) to {output_zip}...")
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in run_temp_dir.glob("*"):
            if file.is_file():
                zf.write(str(file), file.name)
                
    print(f"Cleaning up sub-directory {run_temp_dir}...")
    shutil.rmtree(run_temp_dir)
            
    print(f"✓ Success! Ultrafast Pan-India batch layer stored mapped & zipped at: {output_zip}")


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find closest facilities for centroids using KD-Tree / BallTree.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest='mode', help='Operating mode')

    # ---- single mode ----
    sp_single = subparsers.add_parser(
        'single',
        help='Process one centroid + one facility file → output',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sp_single.add_argument("centroid_file", help="Path to centroid CSV file")
    sp_single.add_argument("facility_file", help="Path to facility CSV file")
    sp_single.add_argument("output_file", help="Path to output CSV file")
    sp_single.add_argument("--centroid-lat", help="Latitude column for centroids")
    sp_single.add_argument("--centroid-lon", help="Longitude column for centroids")
    sp_single.add_argument("--centroid-id", help="ID column for centroids")
    sp_single.add_argument("--facility-lat", help="Latitude column for facilities")
    sp_single.add_argument("--facility-lon", help="Longitude column for facilities")
    sp_single.add_argument("--facility-id", help="ID column for facilities")
    sp_single.add_argument("-k", type=int, default=1,
                           help="Number of nearest facilities to find (default: 1)")
    sp_single.add_argument("--use-haversine", action="store_true",
                           help="Use haversine metric (more accurate, slower)")
    sp_single.add_argument("--keep-geometry", action="store_true",
                           help="Keep geometry columns in output")

    # ---- batch mode ----
    sp_batch = subparsers.add_parser(
        'batch',
        help='Process ALL facility CSVs in a directory against centroids',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            '  python closest_facility_finder.py batch ^\n'
            '      --centroids village_centroids.csv ^\n'
            '      --facility-dir ./data/facilities/cleaned ^\n'
            '      --output closest_facilities.csv\n'
        ),
    )
    sp_batch.add_argument("--centroids", required=True,
                          help="Path to centroids CSV (e.g. india_village_centroids.csv)")
    sp_batch.add_argument("--facility-dir", required=True,
                          help="Directory containing facility CSV files")
    sp_batch.add_argument("--output", required=True,
                          help="Path to output CSV file")
    sp_batch.add_argument("--centroid-lat",
                          help="Override centroid latitude column name")
    sp_batch.add_argument("--centroid-lon",
                          help="Override centroid longitude column name")
    sp_batch.add_argument("--use-haversine", action="store_true",
                          help="Use haversine metric (more accurate, slower)")
    sp_batch.add_argument("--keep-coords", action="store_true",
                          help="Keep lat/lon coordinate columns in output")

    # ---- ultrafast mode ----
    sp_ultrafast = subparsers.add_parser(
        'ultrafast',
        help='Compute pan-India distances by combining statewise geojsons and save to zipped GEE-compatible GeoJSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sp_ultrafast.add_argument("--state-dir", required=True,
                              help="Directory containing statewise base geojsons")
    sp_ultrafast.add_argument("--facility-dir", required=True,
                              help="Directory containing facility CSV files")
    sp_ultrafast.add_argument("--output-zip", required=True,
                              help="Path to output .zip single pan_india file")
    sp_ultrafast.add_argument("--use-haversine", action="store_true",
                              help="Use haversine metric (more accurate, slower)")

    args = parser.parse_args()

    if args.mode is None:
        parser.print_help()
        sys.exit(1)

    if args.mode == 'single':
        if not os.path.exists(args.centroid_file):
            print(f"Error: Centroid file not found: {args.centroid_file}")
            sys.exit(1)
        if not os.path.exists(args.facility_file):
            print(f"Error: Facility file not found: {args.facility_file}")
            sys.exit(1)

        os.makedirs(os.path.dirname(args.output_file) or '.', exist_ok=True)

        find_closest_facilities(
            args.centroid_file,
            args.facility_file,
            args.output_file,
            centroid_lat_col=args.centroid_lat,
            centroid_lon_col=args.centroid_lon,
            centroid_id_col=args.centroid_id,
            facility_lat_col=args.facility_lat,
            facility_lon_col=args.facility_lon,
            facility_id_col=args.facility_id,
            k=args.k,
            use_haversine=args.use_haversine,
            exclude_geometry=not args.keep_geometry
        )

    elif args.mode == 'batch':
        if not os.path.exists(args.centroids):
            print(f"Error: Centroids file not found: {args.centroids}")
            sys.exit(1)
        if not os.path.isdir(args.facility_dir):
            print(f"Error: Facility directory not found: {args.facility_dir}")
            sys.exit(1)

        find_closest_facilities_batch(
            centroid_path=args.centroids,
            facility_dir=args.facility_dir,
            output_path=args.output,
            centroid_lat_col=args.centroid_lat,
            centroid_lon_col=args.centroid_lon,
            use_haversine=args.use_haversine,
            drop_coords=not args.keep_coords,
        )

    elif args.mode == 'ultrafast':
        if not os.path.exists(args.state_dir):
            print(f"Error: State geojsons directory not found: {args.state_dir}")
            sys.exit(1)
        if not os.path.exists(args.facility_dir):
            print(f"Error: Facility directory not found: {args.facility_dir}")
            sys.exit(1)

        find_closest_facilities_ultrafast(
            state_dir=args.state_dir,
            facility_dir=args.facility_dir,
            output_zip=args.output_zip,
            use_haversine=args.use_haversine
        )


if __name__ == '__main__':
    main()