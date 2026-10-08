#!/usr/bin/env python3
"""Shared helpers for the facilities pipeline."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


CLASS_K_COUNT = 8
CLASS_K_COLUMNS = [f"class_k{i}" for i in range(1, CLASS_K_COUNT + 1)]

LOG_FORMAT = "%(asctime)s  [%(levelname)-7s]  %(message)s"


def setup_logging(debug: bool = False, log_path: Optional[Path] = None) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(level=level, format=LOG_FORMAT, datefmt="%H:%M:%S", handlers=handlers, force=True)
    return logging.getLogger("facilities")


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists() and (candidate / "utilities").is_dir():
            return candidate
    raise RuntimeError(f"Could not find repo root from {start}")


def resolve_path(value: str | Path, repo_root: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else repo_root / path


def read_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required. Run with: uv run --with pyyaml ...") from exc
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def write_yaml(path: Path, data: Dict[str, Any]) -> None:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required. Run with: uv run --with pyyaml ...") from exc

    class Dumper(yaml.SafeDumper):
        def ignore_aliases(self, data: Any) -> bool:
            return True

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data, Dumper=Dumper, sort_keys=False, allow_unicode=True, width=120), encoding="utf-8")


def slugify(value: Any) -> str:
    text = "" if pd.isna(value) else str(value)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower())
    return re.sub(r"_+", "_", text).strip("_")


def clean_string_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA, "NaN": pd.NA})
    )


TITLE_STOPWORDS = {"of", "the", "and", "in", "for", "at", "to", "a", "an", "by", "on", "or", "as", "with"}
KNOWN_ABBREVIATIONS = {
    "IIT",
    "IIM",
    "IIIT",
    "NIT",
    "AIIMS",
    "CBSE",
    "ICSE",
    "SSC",
    "HSC",
    "KV",
    "JNV",
    "DAV",
    "PG",
    "ITI",
}


def clean_text_value(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    text = re.sub(r"\s+", " ", str(value).strip())
    text = re.sub(r"^\d+[\.\)\-\s]+", "", text).strip()
    if not text:
        return pd.NA
    words = []
    for index, word in enumerate(text.split(" ")):
        stripped = word.strip()
        bare = stripped.strip(".,;:!?()[]{}'\"")
        if not bare:
            words.append(stripped)
            continue
        if bare.upper() in KNOWN_ABBREVIATIONS:
            words.append(stripped.replace(bare, bare.upper()))
        elif index > 0 and bare.lower() in TITLE_STOPWORDS:
            words.append(bare.lower())
        elif bare.isupper() and len(bare) <= 4:
            words.append(bare)
        else:
            words.append(bare[:1].upper() + bare[1:].lower())
    return " ".join(words)


def clean_text_series(series: pd.Series) -> pd.Series:
    return series.map(clean_text_value).astype("string")


def enforce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def validate_pincode(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.where(values.between(100000, 999999)).astype("Int64")


def validate_year(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.where(values.between(1800, 2035)).astype("Int64")


def clean_identifier(series: pd.Series) -> pd.Series:
    values = clean_string_series(series)
    decimal_int = values.str.match(r"^-?\d+\.0+$", na=False)
    values.loc[decimal_int] = values.loc[decimal_int].str.replace(r"\.0+$", "", regex=True)
    return values


def parse_coordinate_pair(value: Any) -> Tuple[float, float]:
    if pd.isna(value):
        return (math.nan, math.nan)
    numbers = re.findall(r"-?\d+(?:\.\d+)?", str(value))
    if len(numbers) < 2:
        return (math.nan, math.nan)
    lon = float(numbers[0])
    lat = float(numbers[1])
    return lon, lat


def coordinate_status(lat: pd.Series, lon: pd.Series) -> pd.Series:
    valid = (
        lat.notna()
        & lon.notna()
        & lat.between(6.0, 38.8)
        & lon.between(68.0, 98.5)
        & (lat != 0)
        & (lon != 0)
    )
    return pd.Series(valid.map({True: "valid", False: "invalid_coordinate"}), index=lat.index, dtype="string")


def coordinate_key(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").round(7)
    return numeric.map(lambda value: "na" if pd.isna(value) else f"{value:.7f}").astype("string")


def file_fingerprint(paths: Sequence[Path]) -> str:
    payload = []
    for path in paths:
        stat = path.stat()
        payload.append({"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    text = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_csv_selected(path: Path, usecols: Optional[Sequence[str]] = None, **kwargs: Any) -> pd.DataFrame:
    if usecols:
        header = pd.read_csv(path, nrows=0)
        available = set(header.columns)
        missing = [column for column in usecols if column not in available]
        if missing:
            logging.getLogger("facilities").warning("Missing columns in %s: %s", path.name, ", ".join(missing))
        usecols = [column for column in usecols if column in available]
    return pd.read_csv(path, usecols=usecols, low_memory=False, **kwargs)


def taxonomy_rows(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    filter_logic = config.get("filter_logic") or {}
    order = 0
    for domain, groups in (config.get("taxonomy") or {}).items():
        for filter_group, classes in (groups or {}).items():
            logic = (filter_logic.get(domain) or {}).get(filter_group, "direct")
            for facility_class, spec in (classes or {}).items():
                subtypes = (spec or {}).get("subtypes") or []
                rows.append(
                    {
                        "class_l1_domain": domain,
                        "class_l2_filter_group": filter_group,
                        "class_l3_facility_class": facility_class,
                        "configured_subtypes": ";".join(subtypes),
                        "filter_logic": logic,
                        "sort_order": order,
                    }
                )
                order += 1
    return rows


def taxonomy_lookup(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {row["class_l3_facility_class"]: row for row in taxonomy_rows(config)}


def add_attribute_table_to_gpkg(gpkg_path: Path, table_name: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    with sqlite3.connect(gpkg_path) as con:
        df.to_sql(table_name, con, if_exists="replace", index=False, chunksize=100_000)
        con.execute(
            """
            insert or replace into gpkg_contents
            (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id)
            values (?, 'attributes', ?, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), null, null, null, null, null)
            """,
            (table_name, table_name),
        )


def ensure_sqlite_indexes(gpkg_path: Path) -> None:
    with sqlite3.connect(gpkg_path) as con:
        con.execute("create index if not exists idx_facilities_uid on facilities(facility_uid)")
        con.execute("create index if not exists idx_membership_uid on facility_memberships(facility_uid)")
        con.execute(
            """
            create index if not exists idx_membership_classes
            on facility_memberships(class_l1_domain, class_l2_filter_group, class_l3_facility_class, class_l4_facility_subtype)
            """
        )
        con.commit()
