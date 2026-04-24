#!/usr/bin/env python3
"""
High-Performance Vector Utility
===============================

`shapefile_util.py` is a single-file GDAL/OGR-first utility for fast, typed
vector data inspection, extraction, filtering, clipping, and format conversion.
It is designed to replace slower dataframe-heavy shapefile helpers with a
streaming architecture that keeps the heavy work inside GDAL's C/C++ engine.

Why this script exists
----------------------
- Fast filtering of large village / GP / block / district layers.
- Reliable field type preservation when writing output vector files.
- Smart extraction from messy administrative datasets where field names vary.
- A single standalone script that works both as a CLI and as an importable
  module for team workflows.

Core design choices
-------------------
- GDAL/OGR is the primary engine.
- Features stream from source layer to destination layer without loading the
  whole dataset into memory.
- Attribute filters and spatial filters are pushed down to the layer engine
  whenever possible.
- Output schema is created explicitly so datatypes stay correct.
- Shapefile limitations are handled explicitly instead of silently corrupting
  field definitions.
- Existing outputs are backed up automatically into a sibling `_backups/`
  folder before replacement.
- `--select` / `--schema` acts like a lightweight SQL select list for column
  projection, renaming, and type enforcement.

Typical usage
-------------
Batch process every supported vector file in a directory while preserving each
file's current format:
    python utilities/scripts/shapefile_util.py batch \\
        -i data/statewise_base_geojsons \\
        -o data/statewise_base_geojsons \\
        -S "censuscode2011::int, lgd_villagecode AS lgd_village::int, name, censusname, lgd_villagename AS lgd_village_name, censuscode2001::int, state, district, subdistrict, level_2011, tru_2011, lgd_statecode AS lgd_state::int, lgd_districtcode AS lgd_district::int, lgd_subdistrictcode AS lgd_subdistrict::int, id"

Inspect a layer:
    python shapefile_util.py inspect path/to/villages.shp --head 3

Extract a village by likely admin aliases:
    python shapefile_util.py extract -i input.shp -o output.gpkg \\
        --match "village_id=123456,subdistrict_id=10101"

Extract by actual field names:
    python shapefile_util.py extract -i input.shp -o output.fgb \\
        --match "lgd_subdis=10101,lgd_vill_1=123456"

Extract by SQL:
    python shapefile_util.py extract -i input.shp -o output.geojson \\
        --where "state = 'Jharkhand' AND district = 'Giridih'"

Clip around a coordinate and radius:
    python shapefile_util.py clip -i input.gpkg -o output.gpkg \\
        --center "85.2799,23.3441" --center-srs EPSG:4326 --radius 5km

Clip by bbox:
    python shapefile_util.py clip -i input.shp -o output.shp \\
        --bbox "85.10,23.10,85.40,23.50"

Convert to a faster format while keeping types:
    python shapefile_util.py convert -i input.shp -o output.fgb

Create centroid outputs instead of original geometries:
    python shapefile_util.py batch --centroid \\
        -i data/statewise_geojsons \\
        -o data/statewise_base_centroid \\
        -S "censuscode2011::int, lgd_villagecode AS lgd_village::int, name, state, district"

Drop fields:
    python shapefile_util.py remove-fields input.shp cleaned.gpkg \\
        --drop-fields "remove_me,temp_col"

Force field casts:
    python shapefile_util.py convert input.geojson output.gpkg \\
        --cast "lgd_vill_1=int64,censusco_1=int64,name=str:120"

Rename, cast, and keep only the required columns:
    python shapefile_util.py convert -i input.geojson -o output.shp \\
        --select "censuscode2011::int, lgd_villagecode AS lgd_village::int, \\
        name, censusname, lgd_villagename AS lgd_village_name, \\
        censuscode2001::int, state, district, subdistrict, level_2011, \\
        tru_2011, lgd_statecode AS lgd_state::int, \\
        lgd_districtcode AS lgd_district::int, \\
        lgd_subdistrictcode AS lgd_subdistrict::int, id"

Interactive wizard:
    python shapefile_util.py wizard

Library examples
----------------
from shapefile_util import (
    inspect_dataset,
    list_fields,
    read_first_features,
    extract_dataset,
    clip_dataset,
    convert_dataset,
)

summary = inspect_dataset("villages.shp", head=3)
fields = list_fields("villages.shp")

extract_dataset(
    "villages.shp",
    "jharkhand_sample.gpkg",
    match="state=Jharkhand,district=Giridih",
)

clip_dataset(
    "villages.shp",
    "village_5km.gpkg",
    center="85.2799,23.3441",
    center_srs="EPSG:4326",
    radius="5km",
)

Datatype notes
--------------
- GeoPackage / FlatGeobuf / Parquet / GeoJSON generally preserve modern types
  much better than ESRI Shapefile.
- ESRI Shapefile has hard technical limits:
  - field names are short
  - datetime/time support is weak
  - boolean handling is limited
  - string width matters
- This script preserves types as well as the target format allows, and it
  warns or coerces when `.shp` cannot represent a source field faithfully.

Interactive help behavior
-------------------------
The `wizard` command asks what goal you have first, then only asks the follow-up
questions needed for that goal. It is intended for quick ad hoc extraction by
ID, name, bbox, coordinate + radius, or simple format conversion.

Environment
-----------
Run this inside a GDAL-enabled Python environment, for example your
`osgeo_env` conda environment where `from osgeo import gdal` works.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

try:
    import orjson  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency
    orjson = None

try:
    import polars as pl  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency
    pl = None

try:
    import ijson  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency
    ijson = None

try:
    from osgeo import gdal, ogr, osr

    gdal.UseExceptions()
    ogr.UseExceptions()
    GDAL_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - exercised only outside GDAL env
    gdal = None
    ogr = None
    osr = None
    GDAL_IMPORT_ERROR = exc


DEFAULT_ENCODING = "UTF-8"
DEFAULT_HEAD = 5
DEFAULT_BUFFER_SRS = "EPSG:3857"
DEFAULT_RADIUS_UNIT = "m"
DEFAULT_SHAPEFILE_STRING_WIDTH = 254
DEFAULT_SHAPEFILE_INT_WIDTH = 10
DEFAULT_SHAPEFILE_INT64_WIDTH = 18
DEFAULT_SHAPEFILE_REAL_WIDTH = 24
DEFAULT_SHAPEFILE_REAL_PRECISION = 8
SHAPEFILE_FIELD_NAME_LIMIT = 10
BACKUP_DIR_NAME = "_backups"
SHAPEFILE_COMPONENT_SUFFIXES = (".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix", ".sbn", ".sbx")

VECTOR_DRIVER_BY_EXTENSION = {
    ".shp": "ESRI Shapefile",
    ".gpkg": "GPKG",
    ".fgb": "FlatGeobuf",
    ".geojson": "GeoJSON",
    ".json": "GeoJSON",
    ".parquet": "Parquet",
    ".csv": "CSV",
}

CANONICAL_EXTENSION_BY_DRIVER = {
    "ESRI Shapefile": ".shp",
    "GPKG": ".gpkg",
    "FlatGeobuf": ".fgb",
    "GeoJSON": ".geojson",
    "Parquet": ".parquet",
    "CSV": ".csv",
}

TYPE_ALIASES = {
    "int": "int",
    "int32": "int",
    "integer": "int",
    "long": "int64",
    "int64": "int64",
    "bigint": "int64",
    "float": "float",
    "float64": "float",
    "double": "float",
    "real": "float",
    "str": "str",
    "string": "str",
    "text": "str",
    "char": "char",
    "bool": "bool",
    "boolean": "bool",
    "date": "date",
    "datetime": "datetime",
    "timestamp": "datetime",
    "time": "time",
}

FIELD_ALIASES = {
    "state_id": [
        "lgd_state",
        "st_lgd",
        "state_lgd",
        "state_code",
        "statecode",
        "state_id",
        "stateid",
    ],
    "state": [
        "state",
        "state_name",
        "st_name",
        "st_nm",
        "state_nm",
    ],
    "district_id": [
        "lgd_distri",
        "district_lgd",
        "dist_lgd",
        "district_code",
        "districtcode",
        "district_id",
        "districtid",
        "d_pan_code",
    ],
    "district": [
        "district",
        "district_name",
        "dist_name",
        "district_nm",
        "d_pan_name",
    ],
    "subdistrict_id": [
        "lgd_subdis",
        "subdistrict_id",
        "subdistrictcode",
        "subdistrict_code",
        "tehsil_lgd",
        "tehsil_id",
        "tahsil_id",
    ],
    "subdistrict": [
        "subdistrict",
        "sub_district",
        "subdistric",
        "subdistrict_name",
        "tehsil",
        "tehsil_name",
        "tahsil",
        "tahsil_name",
    ],
    "block_id": [
        "blklgdcode",
        "block_lgd",
        "block_code",
        "block_id",
        "blk_lgd",
        "blk_code",
    ],
    "block": [
        "block",
        "block_name",
        "blk_name",
        "block_nm",
    ],
    "gp_id": [
        "gplgdcode",
        "gp_lgd",
        "gp_code",
        "gp_id",
        "grampanchayat_id",
        "grampanchayatcode",
    ],
    "gp": [
        "gp",
        "gp_name",
        "gram_panchayat",
        "grampanchayat",
        "panchayat",
        "panchayat_name",
    ],
    "village_id": [
        "lgd_vill_1",
        "lgd_village",
        "lgd_vill",
        "village_id",
        "village_code",
        "vilcode11",
        "vilcode_n",
        "censusco_1",
        "censuscode2011",
    ],
    "village": [
        "village",
        "village_name",
        "villagenam",
        "vilname",
        "name",
        "village_nm",
    ],
    "name": [
        "name",
        "village_name",
        "district_name",
        "block_name",
        "gp_name",
        "state_name",
    ],
}


@dataclass
class FieldSpec:
    source_name: str
    output_name: str
    type_name: str
    width: int = 0
    precision: int = 0
    subtype_name: Optional[str] = None


@dataclass
class MatchCondition:
    requested_name: str
    field_name: str
    raw_value: str
    typed_value: Any
    type_name: str


@dataclass
class OperationResult:
    output_path: str
    feature_count: int
    layer_name: str
    driver_name: str
    field_mapping: Dict[str, str]
    backup_path: Optional[str] = None


class StrictArgumentParser(argparse.ArgumentParser):
    """ArgumentParser with long-option abbreviation disabled by default."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)


def require_gdal() -> None:
    """Fail with a useful message when GDAL is not importable."""
    if gdal is None or ogr is None or osr is None:
        message = [
            "GDAL/OGR Python bindings are required for shapefile_util.py.",
            "Run this script inside a GDAL-enabled environment, for example your",
            "`osgeo_env` conda environment where `from osgeo import gdal` works.",
        ]
        if GDAL_IMPORT_ERROR is not None:
            message.append(f"Original import error: {GDAL_IMPORT_ERROR}")
        raise RuntimeError("\n".join(message))


def normalize_name(value: str) -> str:
    """Normalize field-like names for alias and fuzzy matching."""
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def slugify(value: str) -> str:
    """Create a filesystem-safe slug."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned or "output"


def split_csv_arg(value: Optional[str]) -> List[str]:
    """Split a comma-separated CLI value while respecting quotes."""
    if value is None:
        return []
    reader = csv.reader([value], skipinitialspace=True)
    row = next(reader, [])
    return [item.strip() for item in row if item and item.strip()]


def json_dumps_text(obj: Any) -> str:
    """Serialize JSON using orjson when available, else stdlib json."""
    if orjson is not None:
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
    return json.dumps(obj, indent=2, default=str)


def strip_wrapping_quotes(value: str) -> str:
    """Strip one matching pair of wrapping quotes from a CLI value."""
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1].strip()
    return text


def normalize_joined_arg(value: Optional[Any]) -> Optional[str]:
    """Join argparse `nargs='+'` values into a single string."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        return " ".join(str(part) for part in value).strip()
    return str(value).strip()


def parse_sql_like_match_string(value: str) -> Dict[str, str]:
    """
    Parse a simple SQL-like equality expression into match pairs.

    Supported:
        district = Bagalkot AND subdistric = Hungund
        district='Bagalkot' and subdistric='Hungund'

    Not supported:
        OR, LIKE, >, <, IN, parentheses
    """
    if re.search(r"\bOR\b", value, flags=re.IGNORECASE):
        raise ValueError("`--match` supports only AND/equality pairs. Use --where for OR clauses.")

    pairs: Dict[str, str] = {}
    parts = re.split(r"\bAND\b", value, flags=re.IGNORECASE)
    for part in parts:
        match = re.fullmatch(r"\s*([A-Za-z0-9_]+)\s*=\s*(.*?)\s*", part)
        if not match:
            raise ValueError(
                "Could not parse match expression. Use `field=value,field2=value2` "
                "or `field = value AND field2 = value2`."
            )
        key = match.group(1).strip()
        raw = strip_wrapping_quotes(match.group(2))
        if not key:
            raise ValueError(f"Empty key in match expression: {part!r}")
        pairs[key] = raw
    return pairs


def parse_key_value_string(value: Optional[Any]) -> Dict[str, str]:
    """
    Parse key=value pairs from a comma-separated CLI string.

    Example:
        "state=Jharkhand,district=Giridih" -> {"state": "Jharkhand", ...}
    """
    value = normalize_joined_arg(value)
    pairs: Dict[str, str] = {}
    if not value:
        return pairs

    if "," not in value and re.search(r"\bAND\b", value, flags=re.IGNORECASE):
        return parse_sql_like_match_string(value)

    for item in split_csv_arg(value):
        if "=" not in item:
            raise ValueError(f"Expected key=value pair, got: {item!r}")
        key, raw = item.split("=", 1)
        key = key.strip()
        raw = strip_wrapping_quotes(raw)
        if not key:
            raise ValueError(f"Empty key in pair: {item!r}")
        pairs[key] = raw
    return pairs


def parse_bbox(value: Optional[str]) -> Optional[Tuple[float, float, float, float]]:
    """Parse xmin,ymin,xmax,ymax."""
    if not value:
        return None
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 4:
        raise ValueError("BBOX must be xmin,ymin,xmax,ymax")
    xmin, ymin, xmax, ymax = map(float, parts)
    if xmin > xmax or ymin > ymax:
        raise ValueError("Invalid bbox: xmin/xmax or ymin/ymax order is reversed")
    return xmin, ymin, xmax, ymax


def parse_point(value: Optional[str]) -> Optional[Tuple[float, float]]:
    """Parse x,y or lon,lat."""
    if not value:
        return None
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 2:
        raise ValueError("Point must be x,y")
    return float(parts[0]), float(parts[1])


def parse_radius(value: Optional[str], default_unit: str = DEFAULT_RADIUS_UNIT) -> Tuple[Optional[float], str]:
    """
    Parse radius strings like 500, 500m, 5km, 2.5mi.

    Returns:
        (value_in_unit, normalized_unit)
    """
    if not value:
        return None, default_unit

    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)\s*", value)
    if not match:
        raise ValueError("Radius must look like 500, 500m, 5km, or 2.5mi")

    distance = float(match.group(1))
    unit = match.group(2).lower() or default_unit

    unit_map = {
        "m": "m",
        "meter": "m",
        "meters": "m",
        "metre": "m",
        "metres": "m",
        "km": "km",
        "kilometer": "km",
        "kilometers": "km",
        "kilometre": "km",
        "kilometres": "km",
        "mi": "mi",
        "mile": "mi",
        "miles": "mi",
        "deg": "deg",
        "degree": "deg",
        "degrees": "deg",
    }
    normalized = unit_map.get(unit)
    if not normalized:
        raise ValueError(f"Unsupported radius unit: {unit}")
    return distance, normalized


def distance_to_meters(distance: float, unit: str) -> float:
    """Convert supported distance units to meters."""
    if unit == "m":
        return distance
    if unit == "km":
        return distance * 1000.0
    if unit == "mi":
        return distance * 1609.344
    raise ValueError(f"Cannot convert unit {unit!r} to meters")


def infer_driver_name(path: str, explicit_format: Optional[str] = None) -> str:
    """Infer GDAL driver name from output path or explicit CLI format."""
    if explicit_format:
        explicit = explicit_format.strip()
        if explicit in VECTOR_DRIVER_BY_EXTENSION.values():
            return explicit
        guessed = VECTOR_DRIVER_BY_EXTENSION.get(f".{explicit.lower().lstrip('.')}")
        if guessed:
            return guessed
        raise ValueError(f"Unsupported output format: {explicit_format}")

    suffix = Path(path).suffix.lower()
    driver_name = VECTOR_DRIVER_BY_EXTENSION.get(suffix)
    if not driver_name:
        raise ValueError(
            f"Cannot infer output format from {path!r}. "
            "Use a known extension like .shp, .gpkg, .fgb, .geojson, .parquet, or .csv."
        )
    return driver_name


def canonical_extension_for_driver(driver_name: str) -> str:
    """Return the preferred file extension for a GDAL/OGR driver."""
    extension = CANONICAL_EXTENSION_BY_DRIVER.get(driver_name)
    if extension:
        return extension

    for suffix, candidate_driver in VECTOR_DRIVER_BY_EXTENSION.items():
        if candidate_driver == driver_name:
            return suffix
    raise ValueError(f"No canonical extension registered for driver: {driver_name}")


def is_supported_vector_file(path: Path) -> bool:
    """Return True for supported primary vector dataset files."""
    return path.is_file() and path.suffix.lower() in VECTOR_DRIVER_BY_EXTENSION


def iter_vector_files(directory: str, recursive: bool = False) -> List[Path]:
    """List supported vector files in a directory, excluding backup folders."""
    root = Path(directory)
    if not root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {directory}")
    if not root.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {directory}")

    iterator = root.rglob("*") if recursive else root.iterdir()
    files: List[Path] = []
    for path in iterator:
        if BACKUP_DIR_NAME in path.parts:
            continue
        if is_supported_vector_file(path):
            files.append(path)
    return sorted(files)


def infer_choice(value: str, choices: Sequence[str], default: str) -> str:
    """Fuzzy-match short interactive inputs like `exteact` -> `extract`."""
    if not value:
        return default

    normalized_value = normalize_name(value)
    normalized_choices = {normalize_name(choice): choice for choice in choices}

    if normalized_value in normalized_choices:
        return normalized_choices[normalized_value]

    for normalized_choice, original in normalized_choices.items():
        if normalized_choice.startswith(normalized_value) or normalized_value.startswith(normalized_choice):
            return original

    close = difflib.get_close_matches(normalized_value, list(normalized_choices), n=1, cutoff=0.6)
    if close:
        return normalized_choices[close[0]]

    return default


def default_output_path(
    source_path: str,
    requested_output: Optional[str],
    label: str,
    preferred_extension: Optional[str] = None,
) -> str:
    """Build an output path when the user did not provide one."""
    if requested_output:
        return requested_output

    source = Path(source_path)
    extension = preferred_extension or source.suffix or ".gpkg"
    return str(source.with_name(f"{source.stem}__{slugify(label)}{extension}"))


def ensure_parent_dir(path: str) -> None:
    """Create destination parent directory when needed."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def output_exists(path: str, driver_name: str) -> bool:
    """Check whether a target output already exists for the selected driver."""
    candidate = Path(path)
    if driver_name == "ESRI Shapefile":
        stem = candidate.with_suffix("")
        return any(stem.with_suffix(suffix).exists() for suffix in SHAPEFILE_COMPONENT_SUFFIXES)
    return candidate.exists()


def build_backup_path(path: str, driver_name: str) -> Path:
    """Build a unique backup destination path inside a sibling `_backups` folder."""
    original = Path(path)
    backup_dir = original.parent / BACKUP_DIR_NAME
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    stem = original.stem
    suffix = original.suffix
    candidate = backup_dir / f"{stem}__backup_{timestamp}{suffix}"
    counter = 1

    while output_exists(str(candidate), driver_name):
        candidate = backup_dir / f"{stem}__backup_{timestamp}_{counter}{suffix}"
        counter += 1
    return candidate


def backup_existing_vector_output(path: str, driver_name: str) -> Optional[str]:
    """
    Move an existing output aside before replacement.

    Returns:
        Absolute backup path for the dataset's main file, or None when no
        existing output was present.
    """
    if not output_exists(path, driver_name):
        return None

    original = Path(path)
    backup_path = build_backup_path(path, driver_name)

    if driver_name == "ESRI Shapefile":
        original_stem = original.with_suffix("")
        backup_stem = backup_path.with_suffix("")
        for suffix in SHAPEFILE_COMPONENT_SUFFIXES:
            source_component = original_stem.with_suffix(suffix)
            if source_component.exists():
                destination_component = backup_stem.with_suffix(suffix)
                destination_component.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source_component), str(destination_component))
        return str(backup_path.resolve())

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(original), str(backup_path))
    return str(backup_path.resolve())


def move_vector_output(source_path: str, destination_path: str, driver_name: str) -> None:
    """Move a vector dataset from a temp location into its final location."""
    source = Path(source_path)
    destination = Path(destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if driver_name == "ESRI Shapefile":
        source_stem = source.with_suffix("")
        destination_stem = destination.with_suffix("")
        for suffix in SHAPEFILE_COMPONENT_SUFFIXES:
            source_component = source_stem.with_suffix(suffix)
            if source_component.exists():
                destination_component = destination_stem.with_suffix(suffix)
                destination_component.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source_component), str(destination_component))
        return

    shutil.move(str(source), str(destination))


def delete_existing_vector_output(path: str, driver_name: str) -> None:
    """Delete an existing destination safely using the relevant driver."""
    require_gdal()
    if not output_exists(path, driver_name):
        return

    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise RuntimeError(f"OGR driver not available: {driver_name}")

    if driver_name == "ESRI Shapefile":
        stem = Path(path).with_suffix("")
        for suffix in SHAPEFILE_COMPONENT_SUFFIXES:
            candidate = stem.with_suffix(suffix)
            if candidate.exists():
                candidate.unlink()
        return

    result = driver.DeleteDataSource(path)
    if result != 0:
        raise RuntimeError(f"Could not delete existing output: {path}")


def ogr_type_name(field_defn: Any) -> str:
    """Map OGR field definition to a friendly type string."""
    require_gdal()
    field_type = field_defn.GetType()
    subtype = field_defn.GetSubType()

    if field_type == ogr.OFTInteger:
        if subtype == ogr.OFSTBoolean:
            return "bool"
        return "int"
    if field_type == ogr.OFTInteger64:
        return "int64"
    if field_type == ogr.OFTReal:
        return "float"
    if field_type == ogr.OFTDate:
        return "date"
    if field_type == ogr.OFTDateTime:
        return "datetime"
    if field_type == ogr.OFTTime:
        return "time"
    return "str"


def ogr_type_from_name(type_name: str) -> Tuple[int, Optional[int]]:
    """Translate friendly type names into OGR field type + subtype."""
    require_gdal()
    normalized = TYPE_ALIASES.get(type_name.strip().lower(), type_name.strip().lower())

    if normalized == "int":
        return ogr.OFTInteger, None
    if normalized == "int64":
        return ogr.OFTInteger64, None
    if normalized == "float":
        return ogr.OFTReal, None
    if normalized in {"str", "char"}:
        return ogr.OFTString, None
    if normalized == "bool":
        return ogr.OFTInteger, ogr.OFSTBoolean
    if normalized == "date":
        return ogr.OFTDate, None
    if normalized == "datetime":
        return ogr.OFTDateTime, None
    if normalized == "time":
        return ogr.OFTTime, None
    raise ValueError(f"Unsupported cast type: {type_name}")


def parse_type_descriptor(raw_spec: str) -> Tuple[str, int, int]:
    """Parse `type[:width[:precision]]` descriptors."""
    parts = [part.strip() for part in raw_spec.split(":")]
    type_name = TYPE_ALIASES.get(parts[0].lower(), parts[0].lower())
    width = 0
    precision = 0

    if len(parts) >= 2 and parts[1]:
        width = int(parts[1])
    if len(parts) >= 3 and parts[2]:
        precision = int(parts[2])

    if type_name == "char" and width == 0:
        type_name = "str"
        width = 1
    return type_name, width, precision


def parse_casts(value: Optional[str]) -> Dict[str, FieldSpec]:
    """
    Parse field cast specs.

    Examples:
        "lgd_vill_1=int64,name=str:120,pop=float:20:4"
        "flag=bool,abbr=char:1"
    """
    casts: Dict[str, FieldSpec] = {}
    if not value:
        return casts

    for item in split_csv_arg(value):
        if "=" not in item:
            raise ValueError(f"Expected field=type in cast spec, got: {item!r}")
        name, raw_spec = item.split("=", 1)
        name = name.strip()
        raw_spec = raw_spec.strip()
        type_name, width, precision = parse_type_descriptor(raw_spec)

        casts[normalize_name(name)] = FieldSpec(
            source_name=name,
            output_name=name,
            type_name=type_name,
            width=width,
            precision=precision,
        )
    return casts


def parse_select_schema(layer: Any, value: Optional[str]) -> Optional[List[FieldSpec]]:
    """
    Parse a SQL-like projection string into field specs.

    Grammar per item:
      source
      source AS output
      source::type
      source AS output::type
      source AS output::type:width[:precision]

    Example:
      censuscode2011::int,
      lgd_villagecode AS lgd_village::int,
      lgd_villagename AS lgd_village_name,
      name
    """
    if not value:
        return None

    source_specs = field_specs_from_layer(layer)
    source_lookup = {spec.source_name: spec for spec in source_specs}

    parsed_specs: List[FieldSpec] = []
    for item in split_csv_arg(value):
        type_name_override: Optional[str] = None
        width_override = 0
        precision_override = 0

        left_side = item
        type_match = re.fullmatch(r"(.*?)(?:::([A-Za-z0-9_]+(?:\s*:\s*\d+){0,2}))?", item)
        if type_match:
            left_side = type_match.group(1).strip()
            if type_match.group(2):
                type_name_override, width_override, precision_override = parse_type_descriptor(type_match.group(2))

        alias_parts = re.split(r"\s+AS\s+", left_side, maxsplit=1, flags=re.IGNORECASE)
        requested_source = alias_parts[0].strip()
        requested_output = alias_parts[1].strip() if len(alias_parts) == 2 else requested_source

        actual_source = resolve_field_name(layer, requested_source)
        base_spec = source_lookup[actual_source]

        parsed_specs.append(
            FieldSpec(
                source_name=actual_source,
                output_name=requested_output,
                type_name=type_name_override or base_spec.type_name,
                width=width_override or base_spec.width,
                precision=precision_override or base_spec.precision,
                subtype_name=base_spec.subtype_name,
            )
        )

    return parsed_specs


def open_dataset(path: str) -> Any:
    """Open a vector dataset with GDAL."""
    require_gdal()
    ds = gdal.OpenEx(path, gdal.OF_VECTOR)
    if ds is None:
        raise FileNotFoundError(f"Could not open vector dataset: {path}")
    return ds


def get_layer(dataset: Any, layer_name: Optional[str] = None) -> Any:
    """Get a named layer or the first layer."""
    if layer_name:
        layer = dataset.GetLayerByName(layer_name)
        if layer is None:
            raise ValueError(f"Layer not found: {layer_name}")
        return layer

    layer = dataset.GetLayer(0)
    if layer is None:
        raise RuntimeError("Dataset has no readable layers")
    return layer


def get_layer_name(layer: Any, fallback: str = "layer") -> str:
    """Return a non-empty layer name."""
    name = layer.GetName()
    return name or fallback


def get_dataset_driver_name(dataset: Any) -> str:
    """Return a stable driver name across GDAL Python variants."""
    driver = dataset.GetDriver() if hasattr(dataset, "GetDriver") else None
    if driver is None:
        return "Unknown"

    for attr_name in ("GetName", "GetDescription"):
        attr = getattr(driver, attr_name, None)
        if callable(attr):
            try:
                value = attr()
            except TypeError:
                value = None
            if value:
                return str(value)

    for attr_name in ("ShortName", "short_name", "name"):
        value = getattr(driver, attr_name, None)
        if value:
            return str(value)

    return driver.__class__.__name__


def layer_srs(layer: Any) -> Optional[Any]:
    """Get layer spatial reference if available."""
    srs = layer.GetSpatialRef()
    if srs is None:
        return None
    return srs.Clone()


def srs_from_user_input(srs_text: Optional[str]) -> Optional[Any]:
    """Create an OSR spatial reference from user text like EPSG:4326."""
    require_gdal()
    if not srs_text:
        return None

    srs = osr.SpatialReference()
    if srs.SetFromUserInput(srs_text) != 0:
        raise ValueError(f"Could not parse spatial reference: {srs_text}")
    return srs


def spatial_ref_to_string(srs: Optional[Any]) -> Optional[str]:
    """Return a readable CRS description."""
    if srs is None:
        return None
    authority = srs.GetAuthorityName(None)
    code = srs.GetAuthorityCode(None)
    if authority and code:
        return f"{authority}:{code}"
    pretty = srs.GetName()
    return pretty or srs.ExportToWkt()


def make_coordinate_transformation(source_srs: Optional[Any], target_srs: Optional[Any]) -> Optional[Any]:
    """Build a coordinate transformation when both SRS values are known."""
    require_gdal()
    if source_srs is None or target_srs is None:
        return None
    if source_srs.IsSame(target_srs):
        return None
    transformer = osr.CoordinateTransformation(source_srs, target_srs)
    return transformer


def clone_geometry(geometry: Optional[Any]) -> Optional[Any]:
    """Clone OGR geometry safely."""
    if geometry is None:
        return None
    return geometry.Clone()


def make_geometry_from_bbox(
    bbox: Tuple[float, float, float, float],
    bbox_srs: Optional[Any],
    target_srs: Optional[Any],
) -> Any:
    """Build a polygon geometry from bbox and transform it if needed."""
    require_gdal()
    xmin, ymin, xmax, ymax = bbox
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(xmin, ymin)
    ring.AddPoint(xmax, ymin)
    ring.AddPoint(xmax, ymax)
    ring.AddPoint(xmin, ymax)
    ring.AddPoint(xmin, ymin)

    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    if bbox_srs:
        poly.AssignSpatialReference(bbox_srs)

    transformer = make_coordinate_transformation(bbox_srs, target_srs)
    if transformer:
        poly.Transform(transformer)
    return poly


def make_geometry_from_center_radius(
    center: Tuple[float, float],
    center_srs: Optional[Any],
    target_srs: Optional[Any],
    radius_value: float,
    radius_unit: str,
    buffer_srs_text: str = DEFAULT_BUFFER_SRS,
) -> Any:
    """
    Create a circular filter geometry.

    If the center SRS is geographic and radius is metric, buffering happens in
    `buffer_srs_text` and is then transformed to the target SRS.
    """
    require_gdal()
    source_srs = center_srs.Clone() if center_srs else None
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(center[0], center[1])
    if source_srs:
        point.AssignSpatialReference(source_srs)

    if radius_unit == "deg":
        buffered = point.Buffer(radius_value)
        transformer = make_coordinate_transformation(source_srs, target_srs)
        if transformer:
            buffered.Transform(transformer)
        return buffered

    metric_distance = distance_to_meters(radius_value, radius_unit)
    buffer_srs = srs_from_user_input(buffer_srs_text)
    if buffer_srs is None:
        raise ValueError(f"Invalid buffer CRS: {buffer_srs_text}")

    if source_srs is None:
        raise ValueError("A center SRS is required when using a radius clip")

    to_buffer = clone_geometry(point)
    transformer_to_buffer = make_coordinate_transformation(source_srs, buffer_srs)
    if transformer_to_buffer:
        to_buffer.Transform(transformer_to_buffer)
    buffered = to_buffer.Buffer(metric_distance)
    buffered.AssignSpatialReference(buffer_srs)

    transformer_to_target = make_coordinate_transformation(buffer_srs, target_srs)
    if transformer_to_target:
        buffered.Transform(transformer_to_target)
    return buffered


def union_layer_geometry(
    mask_path: str,
    layer_name: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    target_srs: Optional[Any] = None,
) -> Any:
    """Union all geometries from a mask layer and optionally transform them."""
    require_gdal()
    dataset = open_dataset(mask_path)
    layer = get_layer(dataset, layer_name)

    where = normalize_joined_arg(where)
    conditions = parse_match_conditions(layer, parse_key_value_string(match))
    python_matcher = apply_best_attribute_filter(layer, where, conditions)

    source_srs = layer_srs(layer)
    union_geom: Optional[Any] = None

    layer.ResetReading()
    for feature in layer:
        if python_matcher and not python_matcher(feature):
            continue
        geometry = feature.GetGeometryRef()
        if geometry is None:
            continue
        geom = geometry.Clone()
        if union_geom is None:
            union_geom = geom
        else:
            union_geom = union_geom.Union(geom)

    if union_geom is None:
        raise RuntimeError(f"No mask geometry found in: {mask_path}")

    transformer = make_coordinate_transformation(source_srs, target_srs)
    if transformer:
        union_geom.Transform(transformer)
    return union_geom


def driver_lco(driver_name: str, encoding: str) -> List[str]:
    """Layer creation options tuned for common vector formats."""
    options: List[str] = []
    if driver_name == "ESRI Shapefile":
        options.append(f"ENCODING={encoding}")
    elif driver_name == "GeoJSON":
        options.append("RFC7946=NO")
    elif driver_name == "CSV":
        options.append("GEOMETRY=AS_WKT")
    return options


def field_specs_from_layer(layer: Any) -> List[FieldSpec]:
    """Read source schema into FieldSpec objects."""
    layer_defn = layer.GetLayerDefn()
    specs: List[FieldSpec] = []
    for index in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(index)
        specs.append(
            FieldSpec(
                source_name=field_defn.GetNameRef(),
                output_name=field_defn.GetNameRef(),
                type_name=ogr_type_name(field_defn),
                width=field_defn.GetWidth(),
                precision=field_defn.GetPrecision(),
                subtype_name="bool" if field_defn.GetSubType() == ogr.OFSTBoolean else None,
            )
        )
    return specs


def sanitize_field_specs_for_driver(
    field_specs: List[FieldSpec],
    driver_name: str,
    cast_map: Optional[Dict[str, FieldSpec]] = None,
) -> Tuple[List[FieldSpec], Dict[str, str]]:
    """
    Prepare output schema and field mapping for the chosen driver.

    Returns:
        (sanitized_specs, source_to_output_name_map)
    """
    cast_map = cast_map or {}
    output_specs: List[FieldSpec] = []
    mapping: Dict[str, str] = {}

    seen_output_names: set[str] = set()

    for spec in field_specs:
        override = cast_map.get(normalize_name(spec.source_name))
        type_name = override.type_name if override else spec.type_name
        width = override.width if override and override.width else spec.width
        precision = override.precision if override and override.precision else spec.precision
        output_name = spec.output_name

        if driver_name == "ESRI Shapefile":
            output_name = shorten_shapefile_field_name(output_name, seen_output_names)
            type_name, width, precision = coerce_shapefile_type(type_name, width, precision)

        if type_name == "str" and width <= 0 and driver_name == "ESRI Shapefile":
            width = DEFAULT_SHAPEFILE_STRING_WIDTH
        if type_name == "int" and width <= 0 and driver_name == "ESRI Shapefile":
            width = DEFAULT_SHAPEFILE_INT_WIDTH
        if type_name == "int64" and width <= 0 and driver_name == "ESRI Shapefile":
            width = DEFAULT_SHAPEFILE_INT64_WIDTH
        if type_name == "float" and driver_name == "ESRI Shapefile":
            if width <= 0:
                width = DEFAULT_SHAPEFILE_REAL_WIDTH
            if precision <= 0:
                precision = DEFAULT_SHAPEFILE_REAL_PRECISION

        seen_output_names.add(output_name.lower())
        mapping[spec.source_name] = output_name
        output_specs.append(
            FieldSpec(
                source_name=spec.source_name,
                output_name=output_name,
                type_name=type_name,
                width=width,
                precision=precision,
                subtype_name=spec.subtype_name,
            )
        )

    return output_specs, mapping


def shorten_shapefile_field_name(name: str, seen_lower: set[str]) -> str:
    """Make field names valid and unique for `.shp` outputs."""
    base = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_") or "field"
    base = base[:SHAPEFILE_FIELD_NAME_LIMIT]
    candidate = base
    counter = 1

    while candidate.lower() in seen_lower:
        suffix = str(counter)
        candidate = f"{base[: SHAPEFILE_FIELD_NAME_LIMIT - len(suffix)]}{suffix}"
        counter += 1
    return candidate


def coerce_shapefile_type(type_name: str, width: int, precision: int) -> Tuple[str, int, int]:
    """Downgrade unsupported types to safer shapefile-compatible field types."""
    normalized = TYPE_ALIASES.get(type_name.lower(), type_name.lower())
    if normalized == "bool":
        return "int", max(width, 1), 0
    if normalized == "datetime":
        return "str", max(width, 25), 0
    if normalized == "time":
        return "str", max(width, 16), 0
    if normalized == "char":
        return "str", max(width, 1), 0
    return normalized, width, precision


def create_output_dataset(path: str, driver_name: str, overwrite: bool) -> Tuple[Any, Optional[str]]:
    """Create output datasource, backing up any existing target first."""
    require_gdal()
    ensure_parent_dir(path)
    _ = overwrite  # Retained for API compatibility. Replacement is now default.
    backup_path = backup_existing_vector_output(path, driver_name)

    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        raise RuntimeError(f"OGR driver not available: {driver_name}")

    dataset = driver.CreateDataSource(path)
    if dataset is None:
        raise RuntimeError(f"Could not create output dataset: {path}")
    return dataset, backup_path


def create_output_layer(
    dataset: Any,
    layer_name: str,
    srs: Optional[Any],
    geometry_type: int,
    field_specs: List[FieldSpec],
    driver_name: str,
    encoding: str,
) -> Any:
    """Create the destination layer and explicit schema."""
    require_gdal()
    layer_options = driver_lco(driver_name, encoding)
    output_layer = dataset.CreateLayer(layer_name, srs=srs, geom_type=geometry_type, options=layer_options)
    if output_layer is None:
        raise RuntimeError(f"Could not create output layer: {layer_name}")

    for spec in field_specs:
        ogr_type, subtype = ogr_type_from_name(spec.type_name)
        field_defn = ogr.FieldDefn(spec.output_name, ogr_type)
        if subtype is not None:
            field_defn.SetSubType(subtype)
        if spec.width > 0:
            field_defn.SetWidth(spec.width)
        if spec.precision > 0:
            field_defn.SetPrecision(spec.precision)
        if output_layer.CreateField(field_defn) != 0:
            raise RuntimeError(f"Could not create field {spec.output_name!r}")

    return output_layer


def resolve_field_name(layer: Any, requested_name: str) -> str:
    """
    Resolve a requested field or alias to an actual field name in the layer.

    Resolution order:
    1. exact field name
    2. normalized field name
    3. configured semantic aliases
    """
    layer_defn = layer.GetLayerDefn()
    actual_names = [layer_defn.GetFieldDefn(i).GetNameRef() for i in range(layer_defn.GetFieldCount())]
    exact = {name: name for name in actual_names}
    normalized = {normalize_name(name): name for name in actual_names}

    if requested_name in exact:
        return exact[requested_name]

    requested_normalized = normalize_name(requested_name)
    if requested_normalized in normalized:
        return normalized[requested_normalized]

    aliases = FIELD_ALIASES.get(requested_normalized)
    if aliases:
        for alias in aliases:
            resolved = normalized.get(normalize_name(alias))
            if resolved:
                return resolved

    raise KeyError(
        f"Could not resolve field or alias {requested_name!r}. "
        f"Available fields: {', '.join(actual_names)}"
    )


def python_typed_value(raw_value: str, type_name: str) -> Any:
    """Cast CLI text to a Python value based on field type."""
    normalized = TYPE_ALIASES.get(type_name.lower(), type_name.lower())

    if raw_value.lower() in {"null", "none"}:
        return None
    if normalized == "int":
        return int(raw_value)
    if normalized == "int64":
        return int(raw_value)
    if normalized == "float":
        return float(raw_value)
    if normalized == "bool":
        lowered = raw_value.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return 1
        if lowered in {"0", "false", "f", "no", "n"}:
            return 0
        raise ValueError(f"Cannot parse boolean value: {raw_value!r}")
    return raw_value


def parse_match_conditions(layer: Any, pairs: Dict[str, str]) -> List[MatchCondition]:
    """Resolve `--match` pairs against source schema."""
    conditions: List[MatchCondition] = []
    layer_defn = layer.GetLayerDefn()
    field_lookup = {layer_defn.GetFieldDefn(i).GetNameRef(): layer_defn.GetFieldDefn(i) for i in range(layer_defn.GetFieldCount())}

    for requested_name, raw_value in pairs.items():
        actual_name = resolve_field_name(layer, requested_name)
        field_defn = field_lookup[actual_name]
        type_name = ogr_type_name(field_defn)
        typed_value = python_typed_value(raw_value, type_name)
        conditions.append(
            MatchCondition(
                requested_name=requested_name,
                field_name=actual_name,
                raw_value=raw_value,
                typed_value=typed_value,
                type_name=type_name,
            )
        )
    return conditions


def sql_escape_literal(value: str) -> str:
    """Escape SQL string literals."""
    return value.replace("'", "''")


def normalize_where_expression(layer: Any, where: Optional[Any]) -> Optional[str]:
    """
    Normalize simple equality expressions so PowerShell-friendly unquoted input
    still works for string fields.

    Examples:
        district = Bagalkot AND subdistric = Hungund
        district='Bagalkot' OR state=Karnataka

    Complex SQL is left unchanged.
    """
    text = normalize_joined_arg(where)
    if not text:
        return None

    if "'" in text or '"' in text:
        return text

    if re.search(r"[<>!()]", text) or re.search(r"\bLIKE\b|\bIN\b|\bIS\b", text, flags=re.IGNORECASE):
        return text

    layer_defn = layer.GetLayerDefn()
    field_lookup = {layer_defn.GetFieldDefn(i).GetNameRef(): layer_defn.GetFieldDefn(i) for i in range(layer_defn.GetFieldCount())}

    rebuilt: List[str] = []
    parts = re.split(r"(\bAND\b|\bOR\b)", text, flags=re.IGNORECASE)
    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue

        if stripped.upper() in {"AND", "OR"}:
            rebuilt.append(stripped.upper())
            continue

        match = re.fullmatch(r"([A-Za-z0-9_]+)\s*=\s*(.+)", stripped)
        if not match:
            return text

        requested_field = match.group(1).strip()
        try:
            actual_field = resolve_field_name(layer, requested_field)
        except KeyError:
            actual_field = requested_field

        raw_value = strip_wrapping_quotes(match.group(2).strip())
        if raw_value.lower() in {"null", "none"}:
            rebuilt.append(f"{actual_field} IS NULL")
            continue

        field_defn = field_lookup.get(actual_field)
        type_name = ogr_type_name(field_defn) if field_defn is not None else "str"
        if type_name in {"int", "int64", "float", "bool"}:
            rebuilt.append(f"{actual_field} = {raw_value}")
        else:
            rebuilt.append(f"{actual_field} = '{sql_escape_literal(raw_value)}'")

    return " ".join(rebuilt)


def parse_simple_where_conditions(layer: Any, where: Optional[Any]) -> Optional[List[MatchCondition]]:
    """
    Parse simple `AND` equality where clauses into match conditions.

    This is used as a smart fallback when a raw OGR attribute filter produces
    zero matches or raises a filter error.
    """
    text = normalize_joined_arg(where)
    if not text:
        return None

    if re.search(r"\bOR\b", text, flags=re.IGNORECASE):
        return None
    if re.search(r"[<>!()]", text) or re.search(r"\bLIKE\b|\bIN\b", text, flags=re.IGNORECASE):
        return None

    parts = re.split(r"\bAND\b", text, flags=re.IGNORECASE)
    pairs: Dict[str, str] = {}
    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue

        null_match = re.fullmatch(r"([A-Za-z0-9_]+)\s+IS\s+NULL", stripped, flags=re.IGNORECASE)
        if null_match:
            pairs[null_match.group(1).strip()] = "null"
            continue

        match = re.fullmatch(r"([A-Za-z0-9_]+)\s*=\s*(.+)", stripped)
        if not match:
            return None
        field_name = match.group(1).strip()
        raw_value = strip_wrapping_quotes(match.group(2).strip())
        pairs[field_name] = raw_value

    if not pairs:
        return None
    return parse_match_conditions(layer, pairs)


def build_attribute_filter_expression(conditions: List[MatchCondition], case_insensitive_strings: bool = False) -> str:
    """Build a simple OGR attribute filter expression."""
    clauses: List[str] = []
    for condition in conditions:
        field = condition.field_name
        value = condition.typed_value
        if value is None:
            clauses.append(f"{field} IS NULL")
            continue

        if condition.type_name in {"int", "int64", "float", "bool"}:
            clauses.append(f"{field} = {value}")
            continue

        if case_insensitive_strings:
            clauses.append(f"UPPER({field}) = UPPER('{sql_escape_literal(str(value))}')")
        else:
            clauses.append(f"{field} = '{sql_escape_literal(str(value))}'")
    return " AND ".join(clauses) if clauses else ""


def make_python_matcher(conditions: List[MatchCondition]) -> Callable[[Any], bool]:
    """Fallback matcher for case-insensitive string comparisons."""
    normalized_targets: List[Tuple[str, Any, str]] = []
    for condition in conditions:
        target = condition.typed_value
        if condition.type_name not in {"int", "int64", "float", "bool"} and target is not None:
            target = str(target).strip().lower()
        normalized_targets.append((condition.field_name, target, condition.type_name))

    def matcher(feature: Any) -> bool:
        for field_name, target, type_name in normalized_targets:
            value = feature.GetField(field_name)
            if target is None:
                if value is not None:
                    return False
                continue
            if type_name in {"int", "int64", "float", "bool"}:
                if value != target:
                    return False
            else:
                if value is None or str(value).strip().lower() != target:
                    return False
        return True

    return matcher


def collect_zero_match_diagnostics(
    layer: Any,
    conditions: List[MatchCondition],
    max_unique_values: int = 2000,
    max_suggestions: int = 6,
) -> Dict[str, Any]:
    """
    Analyze a zero-match result and return clause-level counts and suggestions.

    The scan respects any active spatial filter on the layer, but ignores
    attribute filters so it can inspect the candidate population directly.
    """
    layer.SetAttributeFilter(None)
    layer.ResetReading()

    single_matchers = [make_python_matcher([condition]) for condition in conditions]
    combined_matcher = make_python_matcher(conditions)

    string_fields = {
        condition.field_name: condition
        for condition in conditions
        if condition.type_name not in {"int", "int64", "float", "bool"}
    }
    unique_values: Dict[str, Dict[str, str]] = {field_name: {} for field_name in string_fields}

    total_candidates = 0
    combined_count = 0
    clause_counts = [0 for _ in conditions]

    for feature in layer:
        total_candidates += 1

        if combined_matcher(feature):
            combined_count += 1

        for index, matcher in enumerate(single_matchers):
            if matcher(feature):
                clause_counts[index] += 1

        for field_name in unique_values:
            bucket = unique_values[field_name]
            if len(bucket) >= max_unique_values:
                continue
            raw_value = feature.GetField(field_name)
            if raw_value is None:
                continue
            text_value = str(raw_value).strip()
            if not text_value:
                continue
            bucket.setdefault(text_value.lower(), text_value)

    clauses: List[Dict[str, Any]] = []
    for index, condition in enumerate(conditions):
        clause_info: Dict[str, Any] = {
            "requested_name": condition.requested_name,
            "field_name": condition.field_name,
            "target": condition.typed_value,
            "type_name": condition.type_name,
            "match_count": clause_counts[index],
            "suggestions": [],
        }

        if condition.field_name in unique_values and condition.typed_value is not None:
            target_text = str(condition.typed_value).strip().lower()
            unique_bucket = unique_values[condition.field_name]

            exactish = []
            for normalized, original in unique_bucket.items():
                if target_text in normalized or normalized in target_text:
                    exactish.append(original)

            suggestion_values = exactish[:max_suggestions]
            if len(suggestion_values) < max_suggestions:
                close = difflib.get_close_matches(
                    target_text,
                    list(unique_bucket.keys()),
                    n=max_suggestions,
                    cutoff=0.6,
                )
                for match in close:
                    original = unique_bucket[match]
                    if original not in suggestion_values:
                        suggestion_values.append(original)
                    if len(suggestion_values) >= max_suggestions:
                        break
            clause_info["suggestions"] = suggestion_values

        clauses.append(clause_info)

    return {
        "total_candidates": total_candidates,
        "combined_count": combined_count,
        "clauses": clauses,
    }


def print_zero_match_diagnostics(layer: Any, where: Optional[str], conditions: Optional[List[MatchCondition]]) -> None:
    """Print helpful diagnostics when no features were written."""
    diagnostic_conditions = conditions or parse_simple_where_conditions(layer, where)

    print("No features matched the current filters.")
    if not diagnostic_conditions:
        if where:
            print(f"Active where filter: {normalize_joined_arg(where)}")
        else:
            print("No simple diagnostic could be derived from the current filter.")
        print("Try `inspect`, `head`, or `fields`, or use `--match` for simple equality filters.")
        return

    diagnostics = collect_zero_match_diagnostics(layer, diagnostic_conditions)
    print(f"Candidate features checked after any spatial filter: {diagnostics['total_candidates']}")
    print(f"Combined matches for all clauses: {diagnostics['combined_count']}")
    print("Clause diagnostics:")
    for clause in diagnostics["clauses"]:
        print(
            f"  - {clause['field_name']} = {clause['target']!r} "
            f"-> {clause['match_count']} matching feature(s)"
        )
        if clause["suggestions"]:
            print(f"    Similar values: {', '.join(clause['suggestions'])}")


def apply_best_attribute_filter(
    layer: Any,
    where: Optional[str],
    conditions: Optional[List[MatchCondition]],
) -> Optional[Callable[[Any], bool]]:
    """
    Apply the best available attribute filter to the layer.

    Returns:
        A Python fallback matcher only when needed.
    """
    layer.SetAttributeFilter(None)
    if where:
        normalized_where = normalize_where_expression(layer, where)
        fallback_conditions = parse_simple_where_conditions(layer, normalized_where)
        try:
            layer.SetAttributeFilter(normalized_where)
            matched = layer.GetFeatureCount()
            if matched != 0:
                return None
        except RuntimeError:
            pass

        layer.SetAttributeFilter(None)
        if fallback_conditions:
            return make_python_matcher(fallback_conditions)
        if normalized_where:
            layer.SetAttributeFilter(normalized_where)
        return None

    if not conditions:
        return None

    has_string = any(condition.type_name not in {"int", "int64", "float", "bool"} for condition in conditions)
    expression = build_attribute_filter_expression(conditions, case_insensitive_strings=False)
    if expression:
        layer.SetAttributeFilter(expression)
        if not has_string:
            return None

        matched = layer.GetFeatureCount()
        if matched > 0:
            return None

    layer.SetAttributeFilter(None)
    return make_python_matcher(conditions)


def apply_spatial_filter(
    layer: Any,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    bbox_srs_text: Optional[str] = None,
    center: Optional[Tuple[float, float]] = None,
    center_srs_text: Optional[str] = None,
    radius_value: Optional[float] = None,
    radius_unit: str = DEFAULT_RADIUS_UNIT,
    mask_path: Optional[str] = None,
    mask_layer: Optional[str] = None,
    mask_where: Optional[str] = None,
    mask_match: Optional[str] = None,
) -> Optional[Any]:
    """
    Build and apply a spatial filter geometry to the source layer.

    The returned geometry may also be reused as a clip geometry by callers.
    """
    layer.SetSpatialFilter(None)
    source_srs = layer_srs(layer)

    spatial_geom: Optional[Any] = None
    if bbox:
        bbox_srs = srs_from_user_input(bbox_srs_text) or source_srs
        spatial_geom = make_geometry_from_bbox(bbox, bbox_srs, source_srs)
    elif center and radius_value is not None:
        center_srs = srs_from_user_input(center_srs_text)
        spatial_geom = make_geometry_from_center_radius(
            center=center,
            center_srs=center_srs,
            target_srs=source_srs,
            radius_value=radius_value,
            radius_unit=radius_unit,
        )
    elif mask_path:
        spatial_geom = union_layer_geometry(
            mask_path=mask_path,
            layer_name=mask_layer,
            where=mask_where,
            match=mask_match,
            target_srs=source_srs,
        )

    if spatial_geom is not None:
        layer.SetSpatialFilter(spatial_geom)
    return spatial_geom


def select_field_specs(
    all_specs: List[FieldSpec],
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
) -> List[FieldSpec]:
    """Apply keep/drop field selection."""
    specs = list(all_specs)
    if keep_fields:
        keep_normalized = {normalize_name(name) for name in keep_fields}
        specs = [spec for spec in specs if normalize_name(spec.source_name) in keep_normalized]
    if drop_fields:
        drop_normalized = {normalize_name(name) for name in drop_fields}
        specs = [spec for spec in specs if normalize_name(spec.source_name) not in drop_normalized]
    return specs


def feature_to_preview_dict(feature: Any, layer_srs_obj: Optional[Any], include_geometry: bool = False) -> Dict[str, Any]:
    """Convert a feature into a lightweight preview dictionary."""
    properties: Dict[str, Any] = {}
    feature_defn = feature.GetDefnRef()
    for index in range(feature_defn.GetFieldCount()):
        field_name = feature_defn.GetFieldDefn(index).GetNameRef()
        properties[field_name] = feature.GetField(index)

    preview: Dict[str, Any] = {
        "fid": feature.GetFID(),
        "properties": properties,
    }

    geometry = feature.GetGeometryRef()
    if geometry is not None:
        envelope = geometry.GetEnvelope()
        geom_info: Dict[str, Any] = {
            "geometry_type": geometry.GetGeometryName(),
            "envelope": {
                "min_x": envelope[0],
                "max_x": envelope[1],
                "min_y": envelope[2],
                "max_y": envelope[3],
            },
            "srs": spatial_ref_to_string(layer_srs_obj),
        }
        if include_geometry:
            geom_info["wkt"] = geometry.ExportToWkt()
        preview["geometry"] = geom_info
    else:
        preview["geometry"] = None
    return preview


def inspect_dataset(
    source_path: str,
    layer_name: Optional[str] = None,
    head: int = DEFAULT_HEAD,
    include_geometry: bool = False,
) -> Dict[str, Any]:
    """Inspect a dataset and return a rich summary dict."""
    require_gdal()
    dataset = open_dataset(source_path)
    layer = get_layer(dataset, layer_name)
    all_layers = [dataset.GetLayer(i).GetName() for i in range(dataset.GetLayerCount())]
    srs = layer_srs(layer)
    try:
        extent = layer.GetExtent(can_return_null=True)
    except TypeError:
        extent = layer.GetExtent()

    fields = []
    for spec in field_specs_from_layer(layer):
        fields.append(
            {
                "name": spec.source_name,
                "type": spec.type_name,
                "width": spec.width,
                "precision": spec.precision,
            }
        )

    previews: List[Dict[str, Any]] = []
    layer.ResetReading()
    count = 0
    for feature in layer:
        if count >= max(head, 0):
            break
        previews.append(feature_to_preview_dict(feature, srs, include_geometry=include_geometry))
        count += 1

    summary = {
        "path": str(Path(source_path).resolve()),
        "driver": get_dataset_driver_name(dataset),
        "layer": get_layer_name(layer),
        "layers": all_layers,
        "feature_count": layer.GetFeatureCount(),
        "geometry_type": ogr.GeometryTypeToName(layer.GetGeomType()),
        "srs": spatial_ref_to_string(srs),
        "extent": None
        if extent is None
        else {"min_x": extent[0], "max_x": extent[1], "min_y": extent[2], "max_y": extent[3]},
        "fields": fields,
        "head": previews,
    }
    return summary


def print_inspection(summary: Dict[str, Any]) -> None:
    """Pretty-print an inspection summary."""
    print(f"Path: {summary['path']}")
    print(f"Driver: {summary['driver']}")
    print(f"Layer: {summary['layer']}")
    print(f"Layers: {', '.join(summary['layers'])}")
    print(f"Feature count: {summary['feature_count']}")
    print(f"Geometry type: {summary['geometry_type']}")
    print(f"SRS: {summary['srs']}")
    print(f"Extent: {summary['extent']}")
    print("\nFields:")
    for field in summary["fields"]:
        type_bits = field["type"]
        if field["width"]:
            type_bits += f"({field['width']}"
            if field["precision"]:
                type_bits += f",{field['precision']}"
            type_bits += ")"
        print(f"  - {field['name']}: {type_bits}")

    if summary["head"]:
        print("\nHead:")
        print(json_dumps_text(summary["head"]))


def list_fields(source_path: str, layer_name: Optional[str] = None) -> List[str]:
    """Return all field names from a dataset layer."""
    dataset = open_dataset(source_path)
    layer = get_layer(dataset, layer_name)
    return [spec.source_name for spec in field_specs_from_layer(layer)]


def read_first_features(
    source_path: str,
    layer_name: Optional[str] = None,
    max_features: int = DEFAULT_HEAD,
    include_geometry: bool = False,
) -> List[Dict[str, Any]]:
    """Return the first N features as lightweight dictionaries."""
    dataset = open_dataset(source_path)
    layer = get_layer(dataset, layer_name)
    srs = layer_srs(layer)

    previews: List[Dict[str, Any]] = []
    layer.ResetReading()
    for index, feature in enumerate(layer):
        if index >= max_features:
            break
        previews.append(feature_to_preview_dict(feature, srs, include_geometry=include_geometry))
    return previews


def set_feature_field_value(feature: Any, field_name: str, value: Any, spec: FieldSpec) -> None:
    """Set a destination field value with basic type coercion."""
    field_index = feature.GetFieldIndex(field_name)
    if value is None:
        if field_index < 0:
            return
        if hasattr(feature, "SetFieldNull"):
            feature.SetFieldNull(field_index)
        else:
            feature.UnsetField(field_index)
        return

    type_name = TYPE_ALIASES.get(spec.type_name.lower(), spec.type_name.lower())

    def set_null() -> None:
        if field_index < 0:
            return
        if hasattr(feature, "SetFieldNull"):
            feature.SetFieldNull(field_index)
        else:
            feature.UnsetField(field_index)

    if isinstance(value, str):
        text_value = value.strip()
        if text_value == "" or text_value.lower() in {"null", "none", "nan"}:
            set_null()
            return
    else:
        text_value = str(value).strip()

    try:
        if type_name in {"int", "int64"}:
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                coerced = int(float(text_value))
            feature.SetField(field_name, coerced)
        elif type_name == "bool":
            lowered = text_value.lower()
            if lowered in {"1", "true", "t", "yes", "y"}:
                feature.SetField(field_name, 1)
            elif lowered in {"0", "false", "f", "no", "n"}:
                feature.SetField(field_name, 0)
            else:
                set_null()
        elif type_name == "float":
            feature.SetField(field_name, float(value if not isinstance(value, str) else text_value))
        else:
            feature.SetField(field_name, value)
    except (TypeError, ValueError):
        set_null()


def copy_filtered_layer(
    source_path: str,
    output_path: str,
    *,
    layer_name: Optional[str] = None,
    output_layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select_sql: Optional[str] = None,
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
    casts: Optional[Dict[str, FieldSpec]] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    bbox_srs: Optional[str] = None,
    center: Optional[Tuple[float, float]] = None,
    center_srs: Optional[str] = None,
    radius_value: Optional[float] = None,
    radius_unit: str = DEFAULT_RADIUS_UNIT,
    mask_path: Optional[str] = None,
    mask_layer: Optional[str] = None,
    mask_where: Optional[str] = None,
    mask_match: Optional[str] = None,
    centroid_geometry: bool = False,
    clip_geometry: bool = False,
    dst_srs_text: Optional[str] = None,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> OperationResult:
    """
    Core streaming export path used by extract / clip / convert / remove-fields.

    This function:
    - opens the source layer
    - applies attribute filters and spatial filters
    - creates the output dataset with explicit schema
    - streams matching features into the destination
    """
    require_gdal()
    normalized_source = os.path.normcase(os.path.abspath(source_path))
    normalized_output = os.path.normcase(os.path.abspath(output_path))
    if normalized_source == normalized_output:
        raise ValueError("Input and output paths must be different.")

    source_dataset = open_dataset(source_path)
    source_layer = get_layer(source_dataset, layer_name)

    where = normalize_joined_arg(where)
    mask_where = normalize_joined_arg(mask_where)
    conditions = parse_match_conditions(source_layer, parse_key_value_string(match))
    python_matcher = apply_best_attribute_filter(source_layer, where, conditions)
    clip_geom = apply_spatial_filter(
        source_layer,
        bbox=bbox,
        bbox_srs_text=bbox_srs,
        center=center,
        center_srs_text=center_srs,
        radius_value=radius_value,
        radius_unit=radius_unit,
        mask_path=mask_path,
        mask_layer=mask_layer,
        mask_where=mask_where,
        mask_match=mask_match,
    )

    driver_name = infer_driver_name(output_path, explicit_format=output_format)
    output_dataset, backup_path = create_output_dataset(output_path, driver_name, overwrite=overwrite)

    source_srs = layer_srs(source_layer)
    target_srs = srs_from_user_input(dst_srs_text) or source_srs
    geometry_transform = make_coordinate_transformation(source_srs, target_srs)

    source_specs = field_specs_from_layer(source_layer)
    if select_sql:
        selected_specs = parse_select_schema(source_layer, select_sql) or source_specs
    else:
        selected_specs = select_field_specs(source_specs, keep_fields=keep_fields, drop_fields=drop_fields)
    sanitized_specs, field_mapping = sanitize_field_specs_for_driver(
        selected_specs,
        driver_name=driver_name,
        cast_map=casts,
    )

    if centroid_geometry:
        output_geom_type = ogr.wkbPoint
    elif clip_geometry:
        output_geom_type = ogr.wkbUnknown
    else:
        output_geom_type = source_layer.GetGeomType()

    destination_layer_name = output_layer_name or Path(output_path).stem or get_layer_name(source_layer)
    output_layer = create_output_layer(
        dataset=output_dataset,
        layer_name=destination_layer_name,
        srs=target_srs,
        geometry_type=output_geom_type,
        field_specs=sanitized_specs,
        driver_name=driver_name,
        encoding=encoding,
    )

    output_defn = output_layer.GetLayerDefn()
    source_layer.ResetReading()

    transaction_capability = getattr(ogr, "ODsCTransactions", None)
    use_transactions = bool(
        transaction_capability is not None
        and hasattr(output_dataset, "TestCapability")
        and output_dataset.TestCapability(transaction_capability)
    )
    if use_transactions:
        output_dataset.StartTransaction()

    created = 0
    for source_feature in source_layer:
        if python_matcher and not python_matcher(source_feature):
            continue

        source_geometry = source_feature.GetGeometryRef()
        output_geometry = clone_geometry(source_geometry)

        if clip_geometry and clip_geom is not None and output_geometry is not None:
            if not output_geometry.Intersects(clip_geom):
                continue
            output_geometry = output_geometry.Intersection(clip_geom)
            if output_geometry is None or output_geometry.IsEmpty():
                continue

        if centroid_geometry and output_geometry is not None:
            if output_geometry.IsEmpty():
                output_geometry = None
            else:
                output_geometry = output_geometry.Centroid()

        if geometry_transform and output_geometry is not None:
            output_geometry.Transform(geometry_transform)

        output_feature = ogr.Feature(output_defn)
        for spec in sanitized_specs:
            value = source_feature.GetField(spec.source_name)
            set_feature_field_value(output_feature, spec.output_name, value, spec)

        if output_geometry is not None:
            output_feature.SetGeometry(output_geometry)
        if output_layer.CreateFeature(output_feature) != 0:
            raise RuntimeError("Failed to create output feature")

        created += 1

    if use_transactions:
        output_dataset.CommitTransaction()
    output_layer.SyncToDisk()
    output_dataset.FlushCache()

    if created == 0 and (where or conditions):
        print_zero_match_diagnostics(source_layer, where=where, conditions=conditions)

    return OperationResult(
        output_path=str(Path(output_path).resolve()),
        feature_count=created,
        layer_name=destination_layer_name,
        driver_name=driver_name,
        field_mapping=field_mapping,
        backup_path=backup_path,
    )


def extract_dataset(
    source_path: str,
    output_path: Optional[str] = None,
    *,
    layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select: Optional[str] = None,
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
    cast: Optional[str] = None,
    bbox: Optional[str] = None,
    bbox_srs: Optional[str] = None,
    center: Optional[str] = None,
    center_srs: Optional[str] = None,
    radius: Optional[str] = None,
    mask_path: Optional[str] = None,
    mask_layer: Optional[str] = None,
    mask_where: Optional[str] = None,
    mask_match: Optional[str] = None,
    centroid: bool = False,
    dst_srs: Optional[str] = None,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> OperationResult:
    """Extract matching features to a new dataset without clipping geometry."""
    bbox_tuple = parse_bbox(bbox)
    center_tuple = parse_point(center)
    radius_value, radius_unit = parse_radius(radius)
    casts = parse_casts(cast)

    label_parts = []
    if match:
        label_parts.append(match.replace("=", "-").replace(",", "__"))
    elif where:
        label_parts.append("where")
    if bbox:
        label_parts.append("bbox")
    if center and radius:
        label_parts.append("radius")
    label = "__".join(label_parts) or "extract"

    if output_path is None:
        extension = ".gpkg"
        if output_format:
            for suffix, driver_name in VECTOR_DRIVER_BY_EXTENSION.items():
                if driver_name == infer_driver_name(f"dummy{suffix}", output_format):
                    extension = suffix
                    break
        output_path = default_output_path(source_path, None, label, preferred_extension=extension)

    return copy_filtered_layer(
        source_path=source_path,
        output_path=output_path,
        layer_name=layer_name,
        output_format=output_format,
        where=where,
        match=match,
        select_sql=select,
        keep_fields=keep_fields,
        drop_fields=drop_fields,
        casts=casts,
        bbox=bbox_tuple,
        bbox_srs=bbox_srs,
        center=center_tuple,
        center_srs=center_srs,
        radius_value=radius_value,
        radius_unit=radius_unit,
        mask_path=mask_path,
        mask_layer=mask_layer,
        mask_where=mask_where,
        mask_match=mask_match,
        centroid_geometry=centroid,
        clip_geometry=False,
        dst_srs_text=dst_srs,
        overwrite=overwrite,
        encoding=encoding,
    )


def clip_dataset(
    source_path: str,
    output_path: Optional[str] = None,
    *,
    layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select: Optional[str] = None,
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
    cast: Optional[str] = None,
    bbox: Optional[str] = None,
    bbox_srs: Optional[str] = None,
    center: Optional[str] = None,
    center_srs: Optional[str] = None,
    radius: Optional[str] = None,
    mask_path: Optional[str] = None,
    mask_layer: Optional[str] = None,
    mask_where: Optional[str] = None,
    mask_match: Optional[str] = None,
    centroid: bool = False,
    dst_srs: Optional[str] = None,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> OperationResult:
    """Clip features to bbox, center+radius, or mask geometry."""
    bbox_tuple = parse_bbox(bbox)
    center_tuple = parse_point(center)
    radius_value, radius_unit = parse_radius(radius)
    casts = parse_casts(cast)

    label = "clip"
    if output_path is None:
        output_path = default_output_path(source_path, None, label, preferred_extension=".gpkg")

    return copy_filtered_layer(
        source_path=source_path,
        output_path=output_path,
        layer_name=layer_name,
        output_format=output_format,
        where=where,
        match=match,
        select_sql=select,
        keep_fields=keep_fields,
        drop_fields=drop_fields,
        casts=casts,
        bbox=bbox_tuple,
        bbox_srs=bbox_srs,
        center=center_tuple,
        center_srs=center_srs,
        radius_value=radius_value,
        radius_unit=radius_unit,
        mask_path=mask_path,
        mask_layer=mask_layer,
        mask_where=mask_where,
        mask_match=mask_match,
        centroid_geometry=centroid,
        clip_geometry=True,
        dst_srs_text=dst_srs,
        overwrite=overwrite,
        encoding=encoding,
    )


def convert_dataset(
    source_path: str,
    output_path: str,
    *,
    layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select: Optional[str] = None,
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
    cast: Optional[str] = None,
    centroid: bool = False,
    dst_srs: Optional[str] = None,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> OperationResult:
    """Convert a dataset to another vector format, optionally filtering it."""
    casts = parse_casts(cast)
    return copy_filtered_layer(
        source_path=source_path,
        output_path=output_path,
        layer_name=layer_name,
        output_format=output_format,
        where=where,
        match=match,
        select_sql=select,
        keep_fields=keep_fields,
        drop_fields=drop_fields,
        casts=casts,
        centroid_geometry=centroid,
        dst_srs_text=dst_srs,
        overwrite=overwrite,
        encoding=encoding,
    )


def remove_fields(
    source_path: str,
    fields_to_remove: Sequence[str],
    output_path: Optional[str] = None,
    *,
    layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select: Optional[str] = None,
    centroid: bool = False,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> OperationResult:
    """Create a copy of the dataset without specific fields."""
    output_path = output_path or default_output_path(source_path, None, "fields_removed", preferred_extension=".gpkg")
    return copy_filtered_layer(
        source_path=source_path,
        output_path=output_path,
        layer_name=layer_name,
        output_format=output_format,
        where=where,
        match=match,
        select_sql=select,
        drop_fields=list(fields_to_remove),
        centroid_geometry=centroid,
        overwrite=overwrite,
        encoding=encoding,
    )


def batch_process_directory(
    input_dir: str,
    output_dir: Optional[str] = None,
    *,
    recursive: bool = False,
    layer_name: Optional[str] = None,
    output_format: Optional[str] = None,
    where: Optional[str] = None,
    match: Optional[str] = None,
    select: Optional[str] = None,
    keep_fields: Optional[Sequence[str]] = None,
    drop_fields: Optional[Sequence[str]] = None,
    cast: Optional[str] = None,
    bbox: Optional[str] = None,
    bbox_srs: Optional[str] = None,
    center: Optional[str] = None,
    center_srs: Optional[str] = None,
    radius: Optional[str] = None,
    mask_path: Optional[str] = None,
    mask_layer: Optional[str] = None,
    mask_where: Optional[str] = None,
    mask_match: Optional[str] = None,
    centroid: bool = False,
    dst_srs: Optional[str] = None,
    overwrite: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> List[OperationResult]:
    """
    Apply the same transformation to every supported vector file in a directory.

    If `output_format` is omitted, each file keeps its original extension and
    driver. This makes in-place schema cleanup safe for directories of mixed
    GeoJSON, Shapefile, GeoPackage, and FlatGeobuf inputs.
    """
    require_gdal()
    source_root = Path(input_dir).resolve()
    destination_root = Path(output_dir or input_dir).resolve()
    source_files = iter_vector_files(str(source_root), recursive=recursive)
    if not source_files:
        raise FileNotFoundError(f"No supported vector files found in directory: {input_dir}")

    casts = parse_casts(cast)
    bbox_tuple = parse_bbox(bbox)
    center_tuple = parse_point(center)
    radius_value, radius_unit = parse_radius(radius)
    results: List[OperationResult] = []

    with tempfile.TemporaryDirectory(prefix="shapefile_util_batch_") as temp_dir_name:
        temp_root = Path(temp_dir_name)

        for source_file in source_files:
            relative_path = source_file.relative_to(source_root)
            final_driver_name = (
                infer_driver_name(source_file.name, explicit_format=output_format)
                if output_format
                else infer_driver_name(str(source_file))
            )
            final_suffix = canonical_extension_for_driver(final_driver_name)
            final_relative_path = relative_path.with_suffix(final_suffix)
            temp_output_path = temp_root / final_relative_path
            final_output_path = destination_root / final_relative_path

            result = copy_filtered_layer(
                source_path=str(source_file),
                output_path=str(temp_output_path),
                layer_name=layer_name,
                output_layer_name=source_file.stem,
                output_format=output_format,
                where=where,
                match=match,
                select_sql=select,
                keep_fields=keep_fields,
                drop_fields=drop_fields,
                casts=casts,
                bbox=bbox_tuple,
                bbox_srs=bbox_srs,
                center=center_tuple,
                center_srs=center_srs,
                radius_value=radius_value,
                radius_unit=radius_unit,
                mask_path=mask_path,
                mask_layer=mask_layer,
                mask_where=mask_where,
                mask_match=mask_match,
                centroid_geometry=centroid,
                clip_geometry=False,
                dst_srs_text=dst_srs,
                overwrite=overwrite,
                encoding=encoding,
            )

            backup_path = backup_existing_vector_output(str(final_output_path), final_driver_name)
            move_vector_output(str(temp_output_path), str(final_output_path), final_driver_name)

            results.append(
                OperationResult(
                    output_path=str(final_output_path.resolve()),
                    feature_count=result.feature_count,
                    layer_name=result.layer_name,
                    driver_name=final_driver_name,
                    field_mapping=result.field_mapping,
                    backup_path=backup_path,
                )
            )

    return results


def build_common_io_arguments(parser: argparse.ArgumentParser, include_output: bool = True) -> None:
    """Attach common CLI arguments used by extract / clip / convert workflows."""
    parser.add_argument("source", nargs="?", help="Input vector dataset path")
    parser.add_argument("-i", "--input", dest="source_option", help="Input vector dataset path")
    if include_output:
        parser.add_argument("output", nargs="?", help="Output vector dataset path")
        parser.add_argument("-o", "--output", dest="output_option", help="Output vector dataset path")
    parser.add_argument("-l", "--layer", help="Source layer name (defaults to first layer)")
    parser.add_argument("-f", "--output-format", help="Explicit output format or driver name")
    parser.add_argument(
        "-w",
        "--where",
        nargs="+",
        help="Raw OGR attribute filter expression. Quoted form is best, but unquoted PowerShell-style input also works.",
    )
    parser.add_argument(
        "-m",
        "--match",
        help=(
            "Simple equality filters. Accepts `field=value,field2=value2` or "
            "`field = value AND field2 = value2`. Keys can be actual field names or "
            "semantic aliases like village_id, village, district_id, district, state, subdistrict_id."
        ),
    )
    parser.add_argument(
        "-S",
        "--select",
        "--schema",
        dest="select",
        help=(
            "SQL-like projection list to keep, rename, and cast columns in one go. "
            "Example: \"old_col AS new_col::int, name, state\". "
            "When provided, this takes precedence over --keep-fields."
        ),
    )
    parser.add_argument("-k", "--keep-fields", help="Comma-separated fields to keep when --select is not used")
    parser.add_argument("-d", "--drop-fields", help="Comma-separated fields to drop")
    parser.add_argument(
        "-t",
        "--cast",
        help="Comma-separated field casts like 'lgd_vill_1=int64,name=str:120,pop=float:20:4'",
    )
    parser.add_argument("-s", "--dst-srs", help="Destination CRS like EPSG:4326")
    parser.add_argument(
        "-G",
        "--centroid",
        action="store_true",
        help="Write centroid point geometries instead of the original geometries",
    )
    parser.add_argument(
        "-O",
        "--overwrite",
        action="store_true",
        help="Compatibility flag. Outputs are now replaced with a backup by default.",
    )
    parser.add_argument("-e", "--encoding", default=DEFAULT_ENCODING, help="Text encoding for compatible drivers")


def build_spatial_arguments(parser: argparse.ArgumentParser) -> None:
    """Attach shared spatial filter arguments."""
    parser.add_argument("-b", "--bbox", help="Spatial filter bbox as xmin,ymin,xmax,ymax")
    parser.add_argument("-B", "--bbox-srs", help="CRS of the bbox coordinates, default is source CRS")
    parser.add_argument("-c", "--center", help="Center point x,y or lon,lat")
    parser.add_argument("-C", "--center-srs", help="CRS of the center coordinates, e.g. EPSG:4326")
    parser.add_argument("-r", "--radius", help="Radius like 500, 500m, 5km, or 2mi")
    parser.add_argument("-M", "--mask", help="Mask dataset path for spatial filtering or clipping")
    parser.add_argument("-L", "--mask-layer", help="Mask layer name")
    parser.add_argument(
        "-W",
        "--mask-where",
        nargs="+",
        help="Attribute filter for the mask layer. Quoted form is best, but unquoted PowerShell-style input also works.",
    )
    parser.add_argument("-Q", "--mask-match", help="Smart match filter for the mask layer")


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser."""
    parser = StrictArgumentParser(
        description="Fast GDAL/OGR-based vector file utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quick recipes:
  batch process a directory without changing file types
    python utilities/scripts/shapefile_util.py batch \\
      -i data/statewise_base_geojsons \\
      -o data/statewise_base_geojsons \\
      -S "censuscode2011::int, lgd_villagecode AS lgd_village::int, name, censusname, lgd_villagename AS lgd_village_name, censuscode2001::int, state, district, subdistrict, level_2011, tru_2011, lgd_statecode AS lgd_state::int, lgd_districtcode AS lgd_district::int, lgd_subdistrictcode AS lgd_subdistrict::int, id"

  batch process centroids from polygon inputs
    python utilities/scripts/shapefile_util.py batch --centroid \\
      -i data/statewise_geojsons \\
      -o data/statewise_base_centroid \\
      -S "censuscode2011::int, lgd_villagecode AS lgd_village::int, name, state, district"

  inspect a file
    python shapefile_util.py inspect -i villages.shp -n 3

  extract one village using semantic aliases
    python shapefile_util.py extract -i villages.shp -o village_123.gpkg \\
      -m "village_id=123456,subdistrict_id=10101"

  extract by actual field names
    python shapefile_util.py extract -i villages.shp -o village_123.gpkg \\
      -m "lgd_subdis=10101,lgd_vill_1=123456"

  clip around coordinates
    python shapefile_util.py clip -i villages.shp -o around_me.gpkg \\
      -c "85.2799,23.3441" -C EPSG:4326 -r 5km

  convert to FlatGeobuf for speed
    python shapefile_util.py convert -i villages.shp -o villages.fgb

  rename, keep, and cast columns with one schema string
    python shapefile_util.py convert -i villages.shp -o villages_clean.gpkg \\
      -S "censuscode2011::int, lgd_villagecode AS lgd_village::int, name, state, district"

  run the interactive helper
    python shapefile_util.py wizard
        """.strip(),
    )

    subparsers = parser.add_subparsers(dest="command", parser_class=StrictArgumentParser)

    inspect_cmd = subparsers.add_parser("inspect", help="Inspect metadata, fields, extent, and sample records")
    inspect_cmd.add_argument("source", nargs="?", help="Input vector dataset path")
    inspect_cmd.add_argument("-i", "--input", dest="source_option", help="Input vector dataset path")
    inspect_cmd.add_argument("-l", "--layer", help="Layer name")
    inspect_cmd.add_argument("-n", "--head", type=int, default=DEFAULT_HEAD, help="Number of sample features to print")
    inspect_cmd.add_argument("-g", "--geometry", action="store_true", help="Include WKT geometry in head output")
    inspect_cmd.add_argument("-j", "--json", action="store_true", help="Print inspection as JSON")

    fields_cmd = subparsers.add_parser("fields", help="List only the fields")
    fields_cmd.add_argument("source", nargs="?", help="Input vector dataset path")
    fields_cmd.add_argument("-i", "--input", dest="source_option", help="Input vector dataset path")
    fields_cmd.add_argument("-l", "--layer", help="Layer name")

    head_cmd = subparsers.add_parser("head", help="Preview the first N features quickly")
    head_cmd.add_argument("source", nargs="?", help="Input vector dataset path")
    head_cmd.add_argument("-i", "--input", dest="source_option", help="Input vector dataset path")
    head_cmd.add_argument("-l", "--layer", help="Layer name")
    head_cmd.add_argument("-n", "--count", type=int, default=DEFAULT_HEAD, help="Number of features to read")
    head_cmd.add_argument("-g", "--geometry", action="store_true", help="Include WKT geometry in output")

    batch_cmd = subparsers.add_parser("batch", help="Apply the same transformation to all supported vector files in a directory")
    build_common_io_arguments(batch_cmd, include_output=True)
    build_spatial_arguments(batch_cmd)
    for action in batch_cmd._actions:
        if action.dest in {"source", "source_option"}:
            action.help = "Input directory containing supported vector datasets"
        elif action.dest in {"output", "output_option"}:
            action.help = "Output directory for processed datasets (defaults to the input directory)"
    batch_cmd.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Process files recursively under the input directory",
    )

    extract_cmd = subparsers.add_parser("extract", aliases=["filter"], help="Extract a filtered feature set")
    build_common_io_arguments(extract_cmd, include_output=True)
    build_spatial_arguments(extract_cmd)

    clip_cmd = subparsers.add_parser("clip", help="Clip a dataset by bbox, center+radius, or mask geometry")
    build_common_io_arguments(clip_cmd, include_output=True)
    build_spatial_arguments(clip_cmd)

    convert_cmd = subparsers.add_parser("convert", help="Convert a vector dataset to another format")
    build_common_io_arguments(convert_cmd, include_output=True)

    remove_cmd = subparsers.add_parser("remove-fields", help="Copy a dataset while dropping fields")
    build_common_io_arguments(remove_cmd, include_output=True)

    wizard_cmd = subparsers.add_parser("wizard", help="Interactive helper for ad hoc workflows")
    wizard_cmd.add_argument("-i", "--input", dest="source_option", help="Optional input path to pre-fill")
    wizard_cmd.add_argument("--source", help="Optional input path to pre-fill")

    return parser


def print_result(result: OperationResult) -> None:
    """Print a compact success summary."""
    print(f"Created: {result.output_path}")
    if result.backup_path:
        print(f"Backup: {result.backup_path}")
    print(f"Driver: {result.driver_name}")
    print(f"Layer: {result.layer_name}")
    print(f"Features written: {result.feature_count}")

    renamed = {source: target for source, target in result.field_mapping.items() if source != target}
    if renamed:
        print("Field renames:")
        for source, target in renamed.items():
            print(f"  - {source} -> {target}")


def print_batch_results(results: Sequence[OperationResult]) -> None:
    """Print a compact batch summary."""
    print(f"Processed files: {len(results)}")
    total_features = 0
    for result in results:
        total_features += result.feature_count
        print(f"- {result.output_path}")
        if result.backup_path:
            print(f"  Backup: {result.backup_path}")
        print(f"  Driver: {result.driver_name}")
        print(f"  Layer: {result.layer_name}")
        print(f"  Features written: {result.feature_count}")
    print(f"Total features written across batch: {total_features}")


def resolve_output_argument(args: argparse.Namespace) -> Optional[str]:
    """Support both positional output and `-o/--output`."""
    positional_output = getattr(args, "output", None)
    option_output = getattr(args, "output_option", None)

    if positional_output and option_output and positional_output != option_output:
        raise ValueError("Provide output either positionally or with --output, not both.")
    return option_output or positional_output


def resolve_source_argument(args: argparse.Namespace) -> str:
    """Support both positional source and `-i/--input`."""
    positional_source = getattr(args, "source", None)
    option_source = getattr(args, "source_option", None)

    if positional_source and option_source and positional_source != option_source:
        raise ValueError("Provide input either positionally or with --input, not both.")

    source = option_source or positional_source
    if not source:
        raise ValueError("An input dataset path is required.")
    return source


def prompt(text: str, default: Optional[str] = None) -> str:
    """Prompt with an optional default value."""
    suffix = f" [{default}]" if default else ""
    answer = input(f"{text}{suffix}: ").strip()
    return answer or (default or "")


def run_wizard(source_hint: Optional[str] = None) -> int:
    """Interactive helper that narrows questions based on the chosen goal."""
    require_gdal()
    print("Interactive Vector Utility Wizard")
    print("Goals: inspect, extract, clip, convert, remove-fields")

    goal_text = prompt("What do you want to do", "inspect").lower()
    goal = infer_choice(
        goal_text,
        ["inspect", "extract", "clip", "convert", "remove-fields"],
        default="inspect",
    )

    source = prompt("Input dataset path", source_hint or "")
    if not source:
        raise SystemExit("Input path is required")

    if goal == "inspect":
        head = int(prompt("How many features should I preview", str(DEFAULT_HEAD)))
        summary = inspect_dataset(source, head=head)
        print_inspection(summary)
        return 0

    if goal == "convert":
        output = prompt("Output dataset path")
        keep_fields = split_csv_arg(prompt("Keep only these fields (comma-separated, blank for all)", ""))
        drop_fields = split_csv_arg(prompt("Drop these fields (comma-separated, blank for none)", ""))
        cast = prompt("Field casts like name=str:120,code=int64 (blank for none)", "")
        dst_srs = prompt("Destination CRS like EPSG:4326 (blank to keep source)", "")
        result = convert_dataset(
            source,
            output,
            keep_fields=keep_fields or None,
            drop_fields=drop_fields or None,
            cast=cast or None,
            dst_srs=dst_srs or None,
            overwrite=True,
        )
        print_result(result)
        return 0

    if goal == "remove-fields":
        output = prompt("Output dataset path")
        drop = split_csv_arg(prompt("Fields to drop (comma-separated)"))
        result = remove_fields(source, drop, output_path=output, overwrite=True)
        print_result(result)
        return 0

    output = prompt("Output dataset path")
    filter_mode = infer_choice(
        prompt("How should I find features: match / where / bbox / radius / mask", "match").lower(),
        ["match", "where", "bbox", "radius", "mask"],
        default="match",
    )
    common_kwargs: Dict[str, Any] = {"overwrite": True}

    if filter_mode == "where":
        common_kwargs["where"] = prompt("SQL where expression")
    elif filter_mode == "bbox":
        common_kwargs["bbox"] = prompt("BBox xmin,ymin,xmax,ymax")
        common_kwargs["bbox_srs"] = prompt("BBox CRS", "")
    elif filter_mode == "radius":
        common_kwargs["center"] = prompt("Center x,y")
        common_kwargs["center_srs"] = prompt("Center CRS", "EPSG:4326")
        common_kwargs["radius"] = prompt("Radius", "5km")
    elif filter_mode == "mask":
        common_kwargs["mask_path"] = prompt("Mask dataset path")
        common_kwargs["mask_layer"] = prompt("Mask layer name", "")
        common_kwargs["mask_match"] = prompt("Mask match filter key=value,...", "")
        common_kwargs["mask_where"] = prompt("Mask where expression", "")
    else:
        common_kwargs["match"] = prompt(
            "Filters. Use `field=value,field2=value2` or `field = value AND field2 = value2`"
        )

    keep_fields = split_csv_arg(prompt("Keep only these fields (blank for all)", ""))
    drop_fields = split_csv_arg(prompt("Drop these fields (blank for none)", ""))
    cast = prompt("Field casts like code=int64,name=str:120 (blank for none)", "")
    dst_srs = prompt("Destination CRS like EPSG:4326 (blank to keep source)", "")
    if keep_fields:
        common_kwargs["keep_fields"] = keep_fields
    if drop_fields:
        common_kwargs["drop_fields"] = drop_fields
    if cast:
        common_kwargs["cast"] = cast
    if dst_srs:
        common_kwargs["dst_srs"] = dst_srs

    if goal == "clip":
        result = clip_dataset(source, output, **common_kwargs)
    else:
        result = extract_dataset(source, output, **common_kwargs)
    print_result(result)
    return 0


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    """Main CLI dispatcher."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    try:
        if args.command == "inspect":
            cli_source = resolve_source_argument(args)
            summary = inspect_dataset(cli_source, layer_name=args.layer, head=args.head, include_geometry=args.geometry)
            if args.json:
                print(json_dumps_text(summary))
            else:
                print_inspection(summary)
            return 0

        if args.command == "fields":
            cli_source = resolve_source_argument(args)
            for field_name in list_fields(cli_source, layer_name=args.layer):
                print(field_name)
            return 0

        if args.command == "head":
            cli_source = resolve_source_argument(args)
            data = read_first_features(
                cli_source,
                layer_name=args.layer,
                max_features=args.count,
                include_geometry=args.geometry,
            )
            print(json_dumps_text(data))
            return 0

        cli_source = resolve_source_argument(args)
        cli_output = resolve_output_argument(args) if hasattr(args, "output") or hasattr(args, "output_option") else None

        if args.command == "batch":
            results = batch_process_directory(
                cli_source,
                cli_output or cli_source,
                recursive=args.recursive,
                layer_name=args.layer,
                output_format=args.output_format,
                where=args.where,
                match=args.match,
                select=args.select,
                keep_fields=split_csv_arg(args.keep_fields) or None,
                drop_fields=split_csv_arg(args.drop_fields) or None,
                cast=args.cast,
                bbox=args.bbox,
                bbox_srs=args.bbox_srs,
                center=args.center,
                center_srs=args.center_srs,
                radius=args.radius,
                mask_path=args.mask,
                mask_layer=args.mask_layer,
                mask_where=args.mask_where,
                mask_match=args.mask_match,
                centroid=args.centroid,
                dst_srs=args.dst_srs,
                overwrite=args.overwrite,
                encoding=args.encoding,
            )
            print_batch_results(results)
            return 0

        if args.command in {"extract", "filter"}:
            result = extract_dataset(
                cli_source,
                cli_output,
                layer_name=args.layer,
                output_format=args.output_format,
                where=args.where,
                match=args.match,
                select=args.select,
                keep_fields=split_csv_arg(args.keep_fields) or None,
                drop_fields=split_csv_arg(args.drop_fields) or None,
                cast=args.cast,
                bbox=args.bbox,
                bbox_srs=args.bbox_srs,
                center=args.center,
                center_srs=args.center_srs,
                radius=args.radius,
                mask_path=args.mask,
                mask_layer=args.mask_layer,
                mask_where=args.mask_where,
                mask_match=args.mask_match,
                centroid=args.centroid,
                dst_srs=args.dst_srs,
                overwrite=args.overwrite,
                encoding=args.encoding,
            )
            print_result(result)
            return 0

        if args.command == "clip":
            result = clip_dataset(
                cli_source,
                cli_output,
                layer_name=args.layer,
                output_format=args.output_format,
                where=args.where,
                match=args.match,
                select=args.select,
                keep_fields=split_csv_arg(args.keep_fields) or None,
                drop_fields=split_csv_arg(args.drop_fields) or None,
                cast=args.cast,
                bbox=args.bbox,
                bbox_srs=args.bbox_srs,
                center=args.center,
                center_srs=args.center_srs,
                radius=args.radius,
                mask_path=args.mask,
                mask_layer=args.mask_layer,
                mask_where=args.mask_where,
                mask_match=args.mask_match,
                centroid=args.centroid,
                dst_srs=args.dst_srs,
                overwrite=args.overwrite,
                encoding=args.encoding,
            )
            print_result(result)
            return 0

        if args.command == "convert":
            if not cli_output:
                raise ValueError("convert requires an output path")
            result = convert_dataset(
                cli_source,
                cli_output,
                layer_name=args.layer,
                output_format=args.output_format,
                where=args.where,
                match=args.match,
                select=args.select,
                keep_fields=split_csv_arg(args.keep_fields) or None,
                drop_fields=split_csv_arg(args.drop_fields) or None,
                cast=args.cast,
                centroid=args.centroid,
                dst_srs=args.dst_srs,
                overwrite=args.overwrite,
                encoding=args.encoding,
            )
            print_result(result)
            return 0

        if args.command == "remove-fields":
            if not split_csv_arg(args.drop_fields):
                raise ValueError("remove-fields requires --drop-fields")
            result = remove_fields(
                cli_source,
                split_csv_arg(args.drop_fields),
                output_path=cli_output,
                layer_name=args.layer,
                output_format=args.output_format,
                where=args.where,
                match=args.match,
                select=args.select,
                centroid=args.centroid,
                overwrite=args.overwrite,
                encoding=args.encoding,
            )
            print_result(result)
            return 0

        if args.command == "wizard":
            return run_wizard(source_hint=getattr(args, "source_option", None) or args.source)

        parser.error(f"Unknown command: {args.command}")
        return 2

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def main() -> None:
    """Entry point used by `python shapefile_util.py ...`."""
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
