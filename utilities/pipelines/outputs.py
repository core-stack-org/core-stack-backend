"""Standard output bundle writers for local geospatial pipelines."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .unicode import normalize_unicode_data, normalize_unicode_text


def slug(value: Any) -> str:
    """Return a filesystem-safe lowercase slug."""

    import re

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def utc_now_text() -> str:
    """Return an ISO UTC timestamp without microseconds."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_hash(data: Any) -> str:
    """Return a stable SHA1 hash for JSON-serializable cache metadata."""

    payload = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def scope_output_identity(prefix: str, scope: Any) -> tuple[tuple[str, ...], str]:
    """Return output directory parts and a stable layer/result name for a scope."""

    level = str(getattr(scope, "level", "") or "").lower()
    state = slug(getattr(scope, "state_name", None))
    district = slug(getattr(scope, "district_name", None))
    tehsil = slug(getattr(scope, "tehsil_name", None))
    if level == "state":
        parts = tuple(part for part in (state,) if part)
        return parts, "_".join(part for part in (prefix, state) if part)
    if level == "district":
        parts = tuple(part for part in (state, district) if part)
        return parts, "_".join(part for part in (prefix, district) if part)
    if level == "village":
        village_ids = tuple(str(value) for value in (getattr(scope, "village_ids", None) or ()))
        digest = stable_hash({"village_ids": village_ids})[:10] if village_ids else "unknown"
        return ("village", digest), f"{prefix}_village_{digest}"
    parts = tuple(part for part in (state, district, tehsil) if part)
    return parts, "_".join(part for part in (prefix, district, tehsil) if part)


def mark_cached_result(result: Mapping[str, Any], started: float) -> dict[str, Any]:
    """Annotate a cached result with current lookup timing and original timing."""

    cached = dict(result)
    cached["cache_hit"] = True
    cached["cached_result_elapsed_seconds"] = cached.get("elapsed_seconds")
    cached["cache_lookup_seconds"] = round(time.perf_counter() - started, 3)
    cached["elapsed_seconds"] = cached["cache_lookup_seconds"]
    return cached


def file_signature(path: str | Path) -> dict[str, Any]:
    """Return a fast file signature and optional tracked SHA1 sidecar value."""

    file_path = Path(path)
    stat = file_path.stat()
    sha1_path = Path(f"{file_path.as_posix()}.sha1")
    sha1 = None
    if sha1_path.exists():
        sha1 = sha1_path.read_text().strip().split()[0] or None
    return {
        "path": file_path.as_posix(),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha1": sha1,
    }


def input_signatures(paths: Mapping[str, str | Path]) -> dict[str, dict[str, Any]]:
    """Return signatures for named input files that exist."""

    signatures: dict[str, dict[str, Any]] = {}
    for name, path in paths.items():
        file_path = Path(path)
        if file_path.exists():
            signatures[name] = file_signature(file_path)
    return signatures


def dataframe_eda(frame: pd.DataFrame, *, max_numeric_columns: int = 80) -> dict[str, Any]:
    """Build a compact EDA summary for a tabular or geospatial output."""

    column_names = [str(column) for column in frame.columns]
    seen: dict[str, int] = {}
    summary_names: list[str] = []
    null_counts: dict[str, int] = {}
    for position, column in enumerate(frame.columns):
        name = str(column)
        seen[name] = seen.get(name, 0) + 1
        summary_name = name if seen[name] == 1 else f"{name}__{seen[name]}"
        summary_names.append(summary_name)
        null_counts[summary_name] = int(frame.iloc[:, position].isna().sum())

    summary: dict[str, Any] = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": column_names,
        "null_counts": null_counts,
    }
    if "geometry" in frame.columns:
        geometry_position = column_names.index("geometry")
        summary["null_geometry_count"] = int(frame.iloc[:, geometry_position].isna().sum())
    numeric = frame.select_dtypes(include="number")
    numeric_summary: dict[str, dict[str, float | int | None]] = {}
    numeric_seen: dict[str, int] = {}
    for position, column in enumerate(list(numeric.columns)[:max_numeric_columns]):
        name = str(column)
        numeric_seen[name] = numeric_seen.get(name, 0) + 1
        summary_name = name if numeric_seen[name] == 1 else f"{name}__{numeric_seen[name]}"
        series = numeric.iloc[:, position].dropna()
        numeric_summary[summary_name] = {
            "count": int(series.count()),
            "min": None if series.empty else float(series.min()),
            "mean": None if series.empty else float(series.mean()),
            "max": None if series.empty else float(series.max()),
        }
    summary["numeric_summary"] = numeric_summary
    return summary


def friendly_datatype(series: pd.Series) -> str:
    """Return a human-readable datatype name for column documentation."""

    dtype = series.dtype
    if str(dtype) == "geometry":
        return "geometry"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "number"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"
    return "text"


def column_dictionary(
    frame: pd.DataFrame,
    describe: Mapping[str, str] | Any = None,
    rename: Mapping[str, str] | Any = None,
) -> list[dict[str, Any]]:
    """Return standard column metadata, including optional rename targets.

    `describe` may be a mapping of column name to description or a callable
    returning a description (or None) for a column name. `rename` follows the
    same convention and records only actual source-to-target changes.
    """

    entries: list[dict[str, Any]] = []
    for position, column in enumerate(frame.columns):
        name = str(column)
        if callable(describe):
            description = describe(name)
        elif describe:
            description = describe.get(name)
        else:
            description = None
        if callable(rename):
            rename_to = rename(name)
        elif rename:
            rename_to = rename.get(name)
        else:
            rename_to = None
        entry = {
            "column": name,
            "description": description,
            "datatype": friendly_datatype(frame.iloc[:, position]),
        }
        if rename_to and str(rename_to) != name:
            entry["rename_to"] = str(rename_to)
        entries.append(entry)
    return entries


def frame_profile(
    frame: pd.DataFrame,
    describe: Mapping[str, str] | Any = None,
    rename: Mapping[str, str] | Any = None,
) -> dict[str, Any]:
    """Return column docs, rename mapping, and EDA for one output layer."""

    columns = column_dictionary(frame, describe, rename)
    return {
        "columns": columns,
        "column_rename_mapping": {
            entry["column"]: entry["rename_to"]
            for entry in columns
            if entry.get("rename_to")
        },
        "eda": dataframe_eda(frame),
    }


def _safe_field_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float)):
        return normalize_unicode_text(value) if isinstance(value, str) else value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)
    return str(value)


def _prepare_gdf_for_file(gdf):
    """Coerce columns to Fiona-friendly scalar field values."""

    prepared = gdf.copy()
    geometry_name = prepared.geometry.name
    if "fid" in prepared.columns and "source_fid" not in prepared.columns:
        prepared = prepared.rename(columns={"fid": "source_fid"})
    for column in prepared.columns:
        if column == geometry_name:
            continue
        if str(prepared[column].dtype) == "bool":
            prepared[column] = prepared[column].astype("int8")
        elif str(prepared[column].dtype) in {"object", "string"}:
            prepared[column] = prepared[column].map(_safe_field_value)
    return prepared


@dataclass
class OutputBundle:
    """Writer for a standard local pipeline output directory."""

    root: str | Path
    name: str
    directory_name: str | None = None

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def path(self) -> Path:
        return self.root / slug(self.directory_name or self.name)

    def ensure(self) -> Path:
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def output_path(self, suffix: str) -> Path:
        self.ensure()
        return self.path / f"{slug(self.name)}{suffix}"

    def remove_outputs(self, *suffixes: str) -> list[str]:
        """Remove obsolete artifacts from an earlier contract revision."""

        removed: list[str] = []
        for suffix in suffixes:
            path = self.output_path(suffix)
            if path.exists():
                path.unlink()
                removed.append(path.as_posix())
        return removed

    def write_json(self, data: Mapping[str, Any], suffix: str) -> Path:
        path = self.output_path(suffix)
        payload = normalize_unicode_data(data)
        path.write_text(
            json.dumps(payload, indent=2, default=str, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return path

    def write_metadata(self, data: Mapping[str, Any]) -> Path:
        payload = {"generated_at_utc": utc_now_text(), **dict(data)}
        return self.write_json(payload, ".run_metadata.json")

    def write_links(self, data: Mapping[str, Any]) -> Path:
        """Write the single links manifest for all layers in this run."""

        payload = {"generated_at_utc": utc_now_text(), **dict(data)}
        return self.write_json(payload, ".links.json")

    def cache_manifest_path(self) -> Path:
        return self.output_path(".cache_manifest.json")

    def read_cache_manifest(self) -> dict[str, Any] | None:
        path = self.cache_manifest_path()
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def write_cache_manifest(self, data: Mapping[str, Any]) -> Path:
        payload = normalize_unicode_data(
            {"generated_at_utc": utc_now_text(), **dict(data)}
        )
        path = self.cache_manifest_path()
        path.write_text(
            json.dumps(payload, indent=2, default=str, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return path

    def cached_result(
        self,
        *,
        cache_key: str,
        signatures: Mapping[str, Any],
        required_result_paths: tuple[str, ...],
    ) -> dict[str, Any] | None:
        """Return a cached result only when request, inputs, and files match."""

        manifest = self.read_cache_manifest()
        if not manifest:
            return None
        if manifest.get("cache_key") != cache_key:
            return None
        if manifest.get("input_signatures") != dict(signatures):
            return None
        result = dict(manifest.get("result") or {})
        for key in required_result_paths:
            path = result.get(key)
            if not path or not Path(path).exists():
                return None
        result["status"] = "cached"
        result["cache_manifest_path"] = self.cache_manifest_path().as_posix()
        return result

    def write_readme(self, lines: list[str]) -> Path:
        self.ensure()
        path = self.path / "README.md"
        text = normalize_unicode_text("\n".join(lines).rstrip() + "\n")
        path.write_text(text, encoding="utf-8")
        return path

    def write_gpkg(
        self,
        layers: Mapping[str, Any],
        suffix: str = ".gpkg",
    ) -> Path:
        """Write GeoDataFrame layers to a GeoPackage using Fiona."""

        gpkg_path = self.output_path(suffix)
        if gpkg_path.exists():
            gpkg_path.unlink()
        for layer_name, gdf in layers.items():
            if gdf is None or len(gdf) == 0:
                continue
            gdf = _prepare_gdf_for_file(gdf)
            # Fiona append mode is brittle with GeoPackage in some local GDAL builds.
            # Since the output file is unlinked above, writing each named layer in
            # create/replace mode preserves earlier layers while avoiding append mode.
            gdf.to_file(
                gpkg_path,
                layer=layer_name,
                driver="GPKG",
                mode="w",
                engine="fiona",
            )
        if not gpkg_path.exists():
            raise ValueError("No non-empty layers were provided for GeoPackage output")
        return gpkg_path
