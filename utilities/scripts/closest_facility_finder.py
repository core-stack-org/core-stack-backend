#!/usr/bin/env python3
"""
Closest Facility Finder for village centroids and statewise vector layers.

Fast nearest-facility calculations for:
- centroid CSV files
- centroid directories
- GeoJSON / Shapefile / GPKG / other vector centroid inputs

The `batch` command accepts either a single centroid file or a whole directory
of centroid files and writes one merged CSV output.

Examples
--------

Batch over a centroid directory:
    python utilities/scripts/closest_facility_finder.py batch \
        --centroids "data/village_centroids/" \
        --facility-dir "data/facilities/cleaned" \
        --output "data/closest_facilities/closest_facilities.csv" \
        --use-haversine

Batch over statewise GeoJSON files:
    python utilities/scripts/closest_facility_finder.py batch \
        --centroids "data/statewise_base_geojsons" \
        --facility-dir "data/facilities/cleaned" \
        --output "data/closest_facilities/statewise_closest_facilities.csv"

Single centroid file against one facility file:
    python utilities/scripts/closest_facility_finder.py single \
        "data/village_centroids/india_village_centroids.csv" \
        "data/facilities/cleaned/health_phc.csv" \
        "data/closest_facilities/closest_health_phc.csv"
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from scipy.spatial import KDTree

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import pyogrio
except ImportError:
    pyogrio = None

try:
    import geopandas as gpd
except ImportError:
    gpd = None

try:
    import pyarrow  # noqa: F401
except ImportError:
    pyarrow = None

EARTH_RADIUS_KM = 6371.0
TABULAR_EXTENSIONS = {".csv", ".tsv"}
VECTOR_EXTENSIONS = {
    ".geojson",
    ".json",
    ".shp",
    ".gpkg",
    ".fgb",
    ".parquet",
    ".sqlite",
}

DEFAULT_LAT_COLUMNS = [
    "latitude",
    "lat",
    "centroid_lat",
    "y",
    "point_y",
]

DEFAULT_LON_COLUMNS = [
    "longitude",
    "lon",
    "long",
    "centroid_lon",
    "x",
    "point_x",
]

DEFAULT_ID_COLUMNS = [
    "id",
    "objectid",
    "object_id",
    "village_id",
    "censuscode2011",
    "lgd_village",
]


@dataclass
class PointTable:
    path: Path
    frame: pd.DataFrame
    lat: np.ndarray
    lon: np.ndarray
    lat_col: str
    lon_col: str
    total_rows: int
    valid_rows: int
    coords_from_geometry: bool


@dataclass
class FacilityIndex:
    name: str
    path: Path
    tree: Any
    metric: str
    total_rows: int
    valid_rows: int
    lat_col: str
    lon_col: str
    attrs: pd.DataFrame


@dataclass
class QueryCoordinates:
    radians: Optional[np.ndarray]
    xyz: Optional[np.ndarray]


def normalize_cli_path(path_value: str) -> str:
    """Normalize CLI paths so Windows-style separators also work on Unix."""
    if not path_value:
        return path_value
    expanded = os.path.expanduser(path_value.strip().strip('"').strip("'"))
    if os.path.exists(expanded):
        return expanded
    alternate = expanded.replace("\\", os.sep)
    return alternate


def normalize_name(value: str) -> str:
    """Normalize a field name for fuzzy matching."""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def find_column(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    """Find a column using exact, case-insensitive, and normalized matching."""
    if not columns:
        return None
    column_map = {str(column): str(column) for column in columns}
    lower_map = {str(column).lower(): str(column) for column in columns}
    normalized_map = {normalize_name(str(column)): str(column) for column in columns}

    for candidate in candidates:
        if candidate in column_map:
            return column_map[candidate]
        lowered = candidate.lower()
        if lowered in lower_map:
            return lower_map[lowered]
        normalized = normalize_name(candidate)
        if normalized in normalized_map:
            return normalized_map[normalized]
    return None


def find_latlon_columns(columns: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    """Auto-detect latitude and longitude columns."""
    lat_col = find_column(columns, DEFAULT_LAT_COLUMNS)
    lon_col = find_column(columns, DEFAULT_LON_COLUMNS)
    if lat_col and lon_col:
        return lat_col, lon_col

    for column in columns:
        lowered = str(column).lower()
        if lat_col is None and (
            lowered.endswith("_lat")
            or lowered.endswith("_latitude")
            or lowered == "lat"
            or lowered == "latitude"
        ):
            lat_col = str(column)
        if lon_col is None and (
            lowered.endswith("_lon")
            or lowered.endswith("_long")
            or lowered.endswith("_longitude")
            or lowered == "lon"
            or lowered == "long"
            or lowered == "longitude"
        ):
            lon_col = str(column)
    return lat_col, lon_col


def discover_input_files(path_value: str, recursive: bool = False) -> List[Path]:
    """Resolve a file or discover supported files inside a directory."""
    resolved = Path(normalize_cli_path(path_value))
    if resolved.is_file():
        return [resolved]
    if not resolved.is_dir():
        raise FileNotFoundError(f"Path not found: {resolved}")

    pattern = "**/*" if recursive else "*"
    files = []
    for file_path in sorted(resolved.glob(pattern)):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() in TABULAR_EXTENSIONS | VECTOR_EXTENSIONS:
            files.append(file_path)
    return files


def read_csv_fast(path: Path) -> pd.DataFrame:
    """Read CSV/TSV using Polars and prefer PyArrow-backed pandas when available."""
    separator = "\t" if path.suffix.lower() == ".tsv" else ","
    null_values = ["", "null", "NULL", "None", "none", "nan", "NaN"]
    if pl is not None:
        frame = pl.read_csv(
            path,
            separator=separator,
            null_values=null_values,
            infer_schema_length=10000,
            try_parse_dates=False,
            ignore_errors=False,
        )
        if pyarrow is not None:
            try:
                return frame.to_pandas(use_pyarrow_extension_array=True)
            except (ModuleNotFoundError, ImportError):
                pass
        try:
            return frame.to_pandas(use_pyarrow_extension_array=False)
        except (ModuleNotFoundError, ImportError):
            return pd.DataFrame(frame.to_dict(as_series=False))
    return pd.read_csv(path, sep=separator, na_values=null_values, low_memory=False)


def lonlat_to_xyz(lat_radians: np.ndarray, lon_radians: np.ndarray) -> np.ndarray:
    """Convert lon/lat radians to 3D Cartesian coordinates on a sphere."""
    x = EARTH_RADIUS_KM * np.cos(lat_radians) * np.cos(lon_radians)
    y = EARTH_RADIUS_KM * np.cos(lat_radians) * np.sin(lon_radians)
    z = EARTH_RADIUS_KM * np.sin(lat_radians)
    return np.column_stack((x, y, z))


def chord_distance_to_km(chord_distances: np.ndarray) -> np.ndarray:
    """Convert spherical chord distance to great-circle kilometres."""
    return EARTH_RADIUS_KM * 2.0 * np.arcsin(
        np.clip(np.asarray(chord_distances) / (2.0 * EARTH_RADIUS_KM), -1.0, 1.0)
    )


def prepare_query_coordinates(lat: np.ndarray, lon: np.ndarray, use_haversine: bool) -> QueryCoordinates:
    """Precompute centroid coordinates once per chunk for repeated facility lookups."""
    lat_radians = np.radians(lat.astype(float))
    lon_radians = np.radians(lon.astype(float))
    if use_haversine:
        return QueryCoordinates(
            radians=np.column_stack((lat_radians, lon_radians)),
            xyz=None,
        )
    return QueryCoordinates(
        radians=None,
        xyz=lonlat_to_xyz(lat_radians, lon_radians),
    )


def dataframe_with_valid_coords(
    frame: pd.DataFrame,
    lat_col: str,
    lon_col: str,
) -> Tuple[pd.DataFrame, np.ndarray, np.ndarray, int]:
    """Coerce coordinate columns to numeric and drop invalid rows."""
    working = frame.copy()
    lat_values = pd.to_numeric(working[lat_col], errors="coerce")
    lon_values = pd.to_numeric(working[lon_col], errors="coerce")

    valid_mask = (
        lat_values.notna()
        & lon_values.notna()
        & np.isfinite(lat_values.to_numpy(dtype=float, na_value=np.nan))
        & np.isfinite(lon_values.to_numpy(dtype=float, na_value=np.nan))
        & (lat_values >= -90.0)
        & (lat_values <= 90.0)
        & (lon_values >= -180.0)
        & (lon_values <= 180.0)
    )

    dropped_rows = int((~valid_mask).sum())
    filtered = working.loc[valid_mask].copy().reset_index(drop=True)
    lat_array = lat_values.loc[valid_mask].to_numpy(dtype=float)
    lon_array = lon_values.loc[valid_mask].to_numpy(dtype=float)
    return filtered, lat_array, lon_array, dropped_rows


def read_vector_metadata(path: Path) -> List[str]:
    """Read vector field names without loading the whole dataset."""
    if pyogrio is None:
        raise RuntimeError("pyogrio is required for vector inputs. Install it in the environment.")
    info = pyogrio.read_info(path)
    fields = info.get("fields")
    return [str(field) for field in fields] if fields is not None else []


def read_vector_dataframe(path: Path, read_geometry: bool = True) -> Any:
    """Read a vector dataset with Arrow when possible, then fall back safely."""
    if pyogrio is None:
        raise RuntimeError("pyogrio is required for vector inputs.")
    try:
        return pyogrio.read_dataframe(path, use_arrow=True, read_geometry=read_geometry)
    except (ModuleNotFoundError, ImportError, RuntimeError):
        return pyogrio.read_dataframe(path, use_arrow=False, read_geometry=read_geometry)


def load_vector_table(
    path: Path,
    lat_override: Optional[str],
    lon_override: Optional[str],
    keep_coords: bool,
) -> PointTable:
    """Load a vector dataset and derive point coordinates from columns or geometry."""
    if pyogrio is None or gpd is None:
        raise RuntimeError("Vector inputs require both pyogrio and geopandas.")

    fields = read_vector_metadata(path)
    lat_col = lat_override if lat_override in fields else None
    lon_col = lon_override if lon_override in fields else None
    if lat_col is None or lon_col is None:
        detected_lat, detected_lon = find_latlon_columns(fields)
        lat_col = lat_col or detected_lat
        lon_col = lon_col or detected_lon

    if lat_col and lon_col:
        frame = read_vector_dataframe(path, read_geometry=False)
        total_rows = len(frame)
        filtered, lat, lon, _ = dataframe_with_valid_coords(frame, lat_col, lon_col)
        if not keep_coords:
            filtered = filtered.drop(columns=[c for c in [lat_col, lon_col] if c in filtered.columns])
        return PointTable(
            path=path,
            frame=filtered,
            lat=lat,
            lon=lon,
            lat_col=lat_col,
            lon_col=lon_col,
            total_rows=total_rows,
            valid_rows=len(filtered),
            coords_from_geometry=False,
        )

    gdf = read_vector_dataframe(path, read_geometry=True)
    total_rows = len(gdf)
    if total_rows == 0:
        raise ValueError(f"No rows found in vector dataset: {path}")
    if getattr(gdf, "crs", None) is None:
        gdf = gdf.set_crs(4326, allow_override=True)

    geometry_name = gdf.geometry.name
    valid_geometry_mask = gdf.geometry.notna() & ~gdf.geometry.is_empty
    gdf = gdf.loc[valid_geometry_mask].copy().reset_index(drop=True)
    if gdf.empty:
        raise ValueError(f"No usable geometries found in vector dataset: {path}")

    geom_types = gdf.geometry.geom_type.fillna("")
    if geom_types.eq("Point").all():
        points_wgs84 = gdf.to_crs(4326) if gdf.crs and gdf.crs.to_epsg() != 4326 else gdf
        lat = points_wgs84.geometry.y.to_numpy(dtype=float)
        lon = points_wgs84.geometry.x.to_numpy(dtype=float)
    else:
        metric_gdf = gdf.to_crs(3857) if gdf.crs and gdf.crs.is_geographic else gdf
        centroid_series = metric_gdf.geometry.centroid
        centroid_gs = gpd.GeoSeries(centroid_series, crs=metric_gdf.crs)
        points_wgs84 = centroid_gs.to_crs(4326) if centroid_gs.crs and centroid_gs.crs.to_epsg() != 4326 else centroid_gs
        lat = points_wgs84.y.to_numpy(dtype=float)
        lon = points_wgs84.x.to_numpy(dtype=float)

    attrs = pd.DataFrame(gdf.drop(columns=[geometry_name]))
    attrs = attrs.reset_index(drop=True)
    if keep_coords:
        lat_col = "latitude"
        lon_col = "longitude"
        attrs[lon_col] = lon
        attrs[lat_col] = lat
    else:
        lat_col = "latitude"
        lon_col = "longitude"

    return PointTable(
        path=path,
        frame=attrs,
        lat=lat,
        lon=lon,
        lat_col=lat_col,
        lon_col=lon_col,
        total_rows=total_rows,
        valid_rows=len(attrs),
        coords_from_geometry=True,
    )


def load_point_table(
    path_value: str | Path,
    lat_override: Optional[str] = None,
    lon_override: Optional[str] = None,
    keep_coords: bool = False,
) -> PointTable:
    """Load either a tabular centroid/facility file or a vector dataset."""
    path = Path(path_value)
    suffix = path.suffix.lower()
    if suffix in TABULAR_EXTENSIONS:
        frame = read_csv_fast(path)
        total_rows = len(frame)
        lat_col = lat_override if lat_override in frame.columns else None
        lon_col = lon_override if lon_override in frame.columns else None
        if lat_col is None or lon_col is None:
            detected_lat, detected_lon = find_latlon_columns(list(frame.columns))
            lat_col = lat_col or detected_lat
            lon_col = lon_col or detected_lon
        if not lat_col or not lon_col:
            raise ValueError(f"Could not detect latitude/longitude columns in {path}")
        filtered, lat, lon, _ = dataframe_with_valid_coords(frame, lat_col, lon_col)
        if not keep_coords:
            filtered = filtered.drop(columns=[c for c in [lat_col, lon_col] if c in filtered.columns])
        return PointTable(
            path=path,
            frame=filtered,
            lat=lat,
            lon=lon,
            lat_col=lat_col,
            lon_col=lon_col,
            total_rows=total_rows,
            valid_rows=len(filtered),
            coords_from_geometry=False,
        )

    if suffix in VECTOR_EXTENSIONS:
        return load_vector_table(path, lat_override, lon_override, keep_coords)

    raise ValueError(f"Unsupported input format: {path}")


def build_facility_index(
    facility_path: Path,
    use_haversine: bool,
    lat_override: Optional[str] = None,
    lon_override: Optional[str] = None,
) -> FacilityIndex:
    """Build a reusable KDTree or BallTree for one facility dataset."""
    facility_table = load_point_table(
        facility_path,
        lat_override=lat_override,
        lon_override=lon_override,
        keep_coords=True,
    )
    if facility_table.valid_rows == 0:
        raise ValueError(f"No valid facility rows found in {facility_path}")

    lat_radians = np.radians(facility_table.lat.astype(float))
    lon_radians = np.radians(facility_table.lon.astype(float))

    if use_haversine:
        try:
            from sklearn.neighbors import BallTree
        except ImportError as exc:
            raise RuntimeError("scikit-learn is required for --use-haversine") from exc
        tree = BallTree(np.column_stack((lat_radians, lon_radians)), metric="haversine")
        metric = "haversine"
    else:
        tree = KDTree(lonlat_to_xyz(lat_radians, lon_radians))
        metric = "kdtree"

    return FacilityIndex(
        name=facility_path.stem,
        path=facility_path,
        tree=tree,
        metric=metric,
        total_rows=facility_table.total_rows,
        valid_rows=facility_table.valid_rows,
        lat_col=facility_table.lat_col,
        lon_col=facility_table.lon_col,
        attrs=facility_table.frame.reset_index(drop=True),
    )


def query_facility_index(
    facility_index: FacilityIndex,
    query_coords: QueryCoordinates,
    k: int = 1,
) -> Tuple[np.ndarray, np.ndarray]:
    """Query one facility search tree for nearest distances and row indices."""
    if facility_index.metric == "haversine":
        distances, indices = facility_index.tree.query(query_coords.radians, k=k)
        return np.asarray(distances) * EARTH_RADIUS_KM, np.asarray(indices)

    distances, indices = facility_index.tree.query(query_coords.xyz, k=k)
    return chord_distance_to_km(np.asarray(distances)), np.asarray(indices)


def _save_analysis_record(analysis_path: Path, record: dict) -> None:
    """Append a single analysis record to a CSV file."""
    file_exists = analysis_path.exists()
    with analysis_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(record.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


def discover_facility_files(facility_dir: str, recursive: bool = False) -> List[Path]:
    """Discover supported facility datasets in a directory."""
    facility_root = Path(normalize_cli_path(facility_dir))
    if not facility_root.is_dir():
        raise FileNotFoundError(f"Facility directory not found: {facility_root}")
    return discover_input_files(str(facility_root), recursive=recursive)


def drop_output_coords(frame: pd.DataFrame, point_table: PointTable) -> pd.DataFrame:
    """Remove coordinate columns from output when requested."""
    coord_candidates = [point_table.lat_col, point_table.lon_col]
    existing = [column for column in coord_candidates if column in frame.columns]
    if not existing:
        return frame
    return frame.drop(columns=existing)


def prepare_output_directory(output_path: Path) -> None:
    """Create parent directories for outputs."""
    output_path.parent.mkdir(parents=True, exist_ok=True)


def find_closest_facilities(
    centroid_path: str,
    facility_path: str,
    output_path: str,
    centroid_lat_col: Optional[str] = None,
    centroid_lon_col: Optional[str] = None,
    facility_lat_col: Optional[str] = None,
    facility_lon_col: Optional[str] = None,
    k: int = 1,
    use_haversine: bool = False,
    keep_coords: bool = False,
) -> None:
    """Single-mode lookup for one centroid file against one facility file."""
    centroid_file = Path(normalize_cli_path(centroid_path))
    facility_file = Path(normalize_cli_path(facility_path))
    output_file = Path(normalize_cli_path(output_path))

    centroid_table = load_point_table(
        centroid_file,
        lat_override=centroid_lat_col,
        lon_override=centroid_lon_col,
        keep_coords=keep_coords,
    )
    facility_index = build_facility_index(
        facility_file,
        use_haversine=use_haversine,
        lat_override=facility_lat_col,
        lon_override=facility_lon_col,
    )

    query_coords = prepare_query_coordinates(centroid_table.lat, centroid_table.lon, use_haversine)
    distances, indices = query_facility_index(facility_index, query_coords, k=k)
    if k == 1:
        distances = distances.reshape(-1, 1)
        indices = indices.reshape(-1, 1)

    result = centroid_table.frame.copy()
    if not keep_coords:
        result = drop_output_coords(result, centroid_table)

    for rank in range(k):
        suffix = "" if k == 1 else f"_{rank + 1}"
        result[f"distance_km{suffix}"] = distances[:, rank]
        nearest = facility_index.attrs.iloc[indices[:, rank]].reset_index(drop=True).copy()
        nearest.columns = [f"facility_{column}{suffix}" for column in nearest.columns]
        result = pd.concat([result.reset_index(drop=True), nearest], axis=1)

    prepare_output_directory(output_file)
    result.to_csv(output_file, index=False)

    analysis_path = output_file.with_name(f"{output_file.stem}_analysis.csv")
    _save_analysis_record(
        analysis_path,
        {
            "centroid_source": centroid_file.name,
            "facility_name": facility_index.name,
            "total_rows": facility_index.total_rows,
            "valid_rows": facility_index.valid_rows,
            "lat_col": facility_index.lat_col,
            "lon_col": facility_index.lon_col,
            "min_dist_km": float(np.min(distances[:, 0])),
            "mean_dist_km": float(np.mean(distances[:, 0])),
            "median_dist_km": float(np.median(distances[:, 0])),
            "max_dist_km": float(np.max(distances[:, 0])),
        },
    )

    print(f"Created: {output_file}")
    print(f"Centroid rows written: {len(result):,}")
    print(f"Facility source: {facility_file}")
    print(f"Analysis: {analysis_path}")


def find_closest_facilities_batch(
    centroid_source: str,
    facility_dir: str,
    output_path: str,
    centroid_lat_col: Optional[str] = None,
    centroid_lon_col: Optional[str] = None,
    use_haversine: bool = False,
    keep_coords: bool = False,
    recursive: bool = False,
) -> None:
    """Batch mode for one centroid file or an entire centroid directory."""
    centroid_inputs = discover_input_files(centroid_source, recursive=recursive)
    if not centroid_inputs:
        raise FileNotFoundError(f"No centroid files found in: {centroid_source}")

    facility_files = discover_facility_files(facility_dir, recursive=recursive)
    if not facility_files:
        raise FileNotFoundError(f"No facility files found in: {facility_dir}")

    output_file = Path(normalize_cli_path(output_path))
    prepare_output_directory(output_file)
    if output_file.exists():
        output_file.unlink()

    analysis_path = output_file.with_name(f"{output_file.stem}_analysis.csv")
    if analysis_path.exists():
        analysis_path.unlink()

    print("=" * 72)
    print("Batch Closest Facility Finder")
    print("=" * 72)
    print(f"Centroid inputs: {len(centroid_inputs)}")
    print(f"Facility inputs: {len(facility_files)}")
    print(f"Metric: {'haversine' if use_haversine else 'kd-tree great-circle'}")

    print("\nBuilding facility search trees...")
    facility_indexes: List[FacilityIndex] = []
    skipped_facilities: List[str] = []
    for index, facility_path in enumerate(facility_files, start=1):
        try:
            facility_index = build_facility_index(facility_path, use_haversine=use_haversine)
            facility_indexes.append(facility_index)
            print(
                f"[{index}/{len(facility_files)}] {facility_path.name}: "
                f"{facility_index.valid_rows:,} valid rows"
            )
        except Exception as exc:
            skipped_facilities.append(f"{facility_path.name} ({exc})")
            print(f"[{index}/{len(facility_files)}] Skipped {facility_path.name}: {exc}")

    if not facility_indexes:
        raise RuntimeError("No facility datasets could be indexed.")

    header_written = False
    total_rows_written = 0
    batch_start = time.time()

    for centroid_index, centroid_path in enumerate(centroid_inputs, start=1):
        chunk_start = time.time()
        print(f"\n[{centroid_index}/{len(centroid_inputs)}] Processing {centroid_path.name}")
        centroid_table = load_point_table(
            centroid_path,
            lat_override=centroid_lat_col,
            lon_override=centroid_lon_col,
            keep_coords=keep_coords,
        )
        if centroid_table.valid_rows == 0:
            print("  Skipped: no valid centroid coordinates")
            continue

        result = centroid_table.frame.copy()
        if not keep_coords:
            result = drop_output_coords(result, centroid_table)

        query_coords = prepare_query_coordinates(
            centroid_table.lat,
            centroid_table.lon,
            use_haversine=use_haversine,
        )

        for facility_index in facility_indexes:
            distances_km, _ = query_facility_index(facility_index, query_coords, k=1)
            result[f"{facility_index.name}_distance"] = distances_km.ravel()

            _save_analysis_record(
                analysis_path,
                {
                    "centroid_source": centroid_path.name,
                    "facility_name": facility_index.name,
                    "total_rows": facility_index.total_rows,
                    "valid_rows": facility_index.valid_rows,
                    "lat_col": facility_index.lat_col,
                    "lon_col": facility_index.lon_col,
                    "min_dist_km": float(np.min(distances_km)),
                    "mean_dist_km": float(np.mean(distances_km)),
                    "median_dist_km": float(np.median(distances_km)),
                    "max_dist_km": float(np.max(distances_km)),
                    "elapsed_sec": round(time.time() - chunk_start, 3),
                },
            )

        result.to_csv(output_file, index=False, mode="a" if header_written else "w", header=not header_written)
        header_written = True
        total_rows_written += len(result)

        print(
            f"  Wrote {len(result):,} rows in {time.time() - chunk_start:.1f}s "
            f"from {centroid_table.valid_rows:,} valid centroid points"
        )

    print(f"\nCreated: {output_file}")
    print(f"Analysis: {analysis_path}")
    print(f"Rows written: {total_rows_written:,}")
    print(f"Facility columns added: {len(facility_indexes)}")
    print(f"Elapsed: {time.time() - batch_start:.1f}s")
    if skipped_facilities:
        print("Skipped facility files:")
        for message in skipped_facilities:
            print(f"  - {message}")


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Find closest facilities for centroid CSVs or vector layers.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="mode")

    single = subparsers.add_parser(
        "single",
        help="Process one centroid file against one facility file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    single.add_argument("centroid_file", help="Centroid CSV or vector file")
    single.add_argument("facility_file", help="Facility CSV or vector file")
    single.add_argument("output_file", help="Output CSV path")
    single.add_argument("--centroid-lat", help="Override centroid latitude column")
    single.add_argument("--centroid-lon", help="Override centroid longitude column")
    single.add_argument("--facility-lat", help="Override facility latitude column")
    single.add_argument("--facility-lon", help="Override facility longitude column")
    single.add_argument("-k", type=int, default=1, help="Number of nearest facilities to attach")
    single.add_argument("--use-haversine", action="store_true", help="Use BallTree haversine distances")
    single.add_argument("--keep-coords", action="store_true", help="Keep centroid coordinate columns in output")

    batch = subparsers.add_parser(
        "batch",
        help="Process a centroid file or centroid directory against all facility files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    batch.add_argument(
        "-c",
        "--centroids",
        required=True,
        help="Centroid CSV/vector file or a directory containing centroid files",
    )
    batch.add_argument(
        "-f",
        "--facility-dir",
        required=True,
        help="Directory containing facility CSV/vector files",
    )
    batch.add_argument("-o", "--output", required=True, help="Merged output CSV path")
    batch.add_argument("--centroid-lat", help="Override centroid latitude column")
    batch.add_argument("--centroid-lon", help="Override centroid longitude column")
    batch.add_argument("--use-haversine", action="store_true", help="Use BallTree haversine distances")
    batch.add_argument("--keep-coords", action="store_true", help="Keep centroid coordinate columns in output")
    batch.add_argument("-R", "--recursive", action="store_true", help="Recursively scan centroid and facility directories")

    ultrafast = subparsers.add_parser(
        "ultrafast",
        help="Alias of batch for statewise vector directories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ultrafast.add_argument("--state-dir", required=True, help="Statewise centroid/vector directory")
    ultrafast.add_argument("--facility-dir", required=True, help="Directory containing facility files")
    ultrafast.add_argument("--output", required=True, help="Merged output CSV path")
    ultrafast.add_argument("--centroid-lat", help="Override centroid latitude column")
    ultrafast.add_argument("--centroid-lon", help="Override centroid longitude column")
    ultrafast.add_argument("--use-haversine", action="store_true", help="Use BallTree haversine distances")
    ultrafast.add_argument("--keep-coords", action="store_true", help="Keep centroid coordinate columns in output")
    ultrafast.add_argument("-R", "--recursive", action="store_true", help="Recursively scan directories")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.mode is None:
        parser.print_help()
        return 1

    try:
        if args.mode == "single":
            find_closest_facilities(
                centroid_path=args.centroid_file,
                facility_path=args.facility_file,
                output_path=args.output_file,
                centroid_lat_col=args.centroid_lat,
                centroid_lon_col=args.centroid_lon,
                facility_lat_col=args.facility_lat,
                facility_lon_col=args.facility_lon,
                k=args.k,
                use_haversine=args.use_haversine,
                keep_coords=args.keep_coords,
            )
            return 0

        if args.mode == "batch":
            find_closest_facilities_batch(
                centroid_source=args.centroids,
                facility_dir=args.facility_dir,
                output_path=args.output,
                centroid_lat_col=args.centroid_lat,
                centroid_lon_col=args.centroid_lon,
                use_haversine=args.use_haversine,
                keep_coords=args.keep_coords,
                recursive=args.recursive,
            )
            return 0

        if args.mode == "ultrafast":
            find_closest_facilities_batch(
                centroid_source=args.state_dir,
                facility_dir=args.facility_dir,
                output_path=args.output,
                centroid_lat_col=args.centroid_lat,
                centroid_lon_col=args.centroid_lon,
                use_haversine=args.use_haversine,
                keep_coords=args.keep_coords,
                recursive=args.recursive,
            )
            return 0

        parser.error(f"Unknown mode: {args.mode}")
        return 2

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
