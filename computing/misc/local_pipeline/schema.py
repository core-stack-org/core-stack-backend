"""Request parsing, config loading, and lightweight validation helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import yaml

from .admin import AdminScope


def coerce_bool(value: Any, default: bool = False) -> bool:
    """Parse booleans from API, YAML, and form-style string values."""

    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "f", "no", "n", "off"}:
            return False
    return bool(value)


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML or JSON config file."""

    path = Path(path)
    text = path.read_text()
    if path.suffix.lower() == ".json":
        return json.loads(text)
    return yaml.safe_load(text) or {}


#: Standard values for the per-village `*_status` columns that every dataset
#: pipeline writes right after the admin columns. They explain data
#: availability to end users without needing the run metadata.
STATUS_NO_VILLAGE_ID = "no village id available"
STATUS_NO_DATA = "no data available for this village"
STATUS_MATCHED = "matched"
STATUS_COMPUTED = "computed"


def status_column_config(config: Mapping[str, Any]) -> tuple[str | None, frozenset[str]]:
    """Return the configured `*_status` column name and the outputs keeping it.

    Pipelines configure this under `output_contract.status_column` with a
    `name` and an `outputs` list (any of `csv`, `gpkg`, `geoserver`). Removing
    an entry from `outputs` drops the column from that artifact. The GeoServer
    layer is published from the GeoPackage, so `gpkg` and `geoserver` share
    one frame; the column is kept when either is listed.
    """

    contract = config.get("output_contract") or {}
    spec = contract.get("status_column") or {}
    name = spec.get("name")
    if not name:
        return None, frozenset()
    return str(name), frozenset(str(item).lower() for item in spec.get("outputs") or ())


@dataclass(frozen=True)
class OutputOptions:
    """Which artifacts a pipeline run writes.

    The default mode writes the full standard bundle: one GeoPackage, one CSV
    derived from the same data, a README, a metadata JSON (which includes the
    EDA summary), a STAC item, and a GeoServer layer published from the same
    GeoPackage. Each artifact can be turned off per request or per pipeline
    YAML (`default_outputs`).
    """

    gpkg: bool = True
    csv: bool = True
    readme: bool = True
    metadata: bool = True
    stac: bool = True
    geoserver: bool = True

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "OutputOptions":
        merged = {field.name: getattr(cls, field.name) for field in cls.__dataclass_fields__.values()}
        for key, value in dict(data or {}).items():
            name = str(key)
            if name in merged:
                merged[name] = coerce_bool(value, merged[name])
        return cls(**merged)


def resolve_output_options(request: "StandardRequest", config: Mapping[str, Any]) -> OutputOptions:
    """Resolve effective output flags: dataclass defaults, then the pipeline
    YAML `default_outputs`, then per-request `outputs` overrides."""

    merged: dict[str, Any] = dict(config.get("default_outputs") or {})
    request_outputs = request.raw.get("outputs")
    if isinstance(request_outputs, Mapping):
        merged.update(request_outputs)
    return OutputOptions.from_mapping(merged)


@dataclass(frozen=True)
class PublishOptions:
    sync_to_geoserver: bool = True
    overwrite: bool = True
    register_layers: bool = True
    use_pregenerated: bool = False
    geoserver_workspace: str | None = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "PublishOptions":
        values = dict(data or {})
        return cls(
            sync_to_geoserver=coerce_bool(values.get("sync_to_geoserver"), True),
            overwrite=coerce_bool(values.get("overwrite"), True),
            register_layers=coerce_bool(values.get("register_layers"), True),
            use_pregenerated=coerce_bool(values.get("use_pregenerated"), False),
            geoserver_workspace=str(values["geoserver_workspace"]) if values.get("geoserver_workspace") else None,
        )


@dataclass(frozen=True)
class BatchOptions:
    mode: str = "single"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "BatchOptions":
        return cls(mode=str((data or {}).get("mode", "single")))


def api_request_payload(data: Mapping[str, Any], *, overwrite: bool = True) -> dict[str, Any]:
    """Normalize an API body into the standard request payload.

    Two request shapes are supported. The simple shape matches the other
    Core Stack layer APIs and implies a tehsil scope:

        {"state": "jharkhand", "district": "dumka", "block": "masalia",
         "sync_to_geoserver": true, "overwrite": true}

    The structured shape addresses any scope level and toggles artifacts:

        {"scope": {"level": "district", "state_name": ..., "district_name": ...},
         "outputs": {"stac": false}, "publish": {"sync_to_geoserver": false}}

    `overwrite` is the pipeline's default when the body does not set it.
    Raises ValueError when the body names no resolvable geography.
    """

    body = dict(data)
    scope = body.get("scope")
    if not isinstance(scope, Mapping):
        scope = {
            "level": body.get("level") or body.get("scope_level") or "tehsil",
            "state_name": body.get("state_name") or body.get("state"),
            "district_name": body.get("district_name") or body.get("district"),
            "tehsil_name": body.get("tehsil_name") or body.get("block_name") or body.get("block"),
            "village_ids": body.get("village_ids") or body.get("village_id"),
        }
    scope = {key: value for key, value in dict(scope).items() if value is not None}
    scope.setdefault("level", "tehsil")
    if not scope.get("state_name") and not scope.get("village_ids"):
        raise ValueError("Provide state/district/block (or a scope object with state_name or village_ids).")

    publish = dict(body.get("publish") or {})
    for key, default in (
        ("sync_to_geoserver", True),
        ("overwrite", overwrite),
        ("register_layers", True),
        ("use_pregenerated", False),
    ):
        # Simple bodies carry publish flags at the top level.
        publish.setdefault(key, body.get(key, default))

    return {"scope": scope, "outputs": dict(body.get("outputs") or {}), "publish": publish}


@dataclass(frozen=True)
class StandardRequest:
    """Standard request object accepted by local pipeline implementations."""

    scope: AdminScope
    outputs: OutputOptions = field(default_factory=OutputOptions)
    publish: PublishOptions = field(default_factory=PublishOptions)
    batch: BatchOptions = field(default_factory=BatchOptions)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "StandardRequest":
        raw = dict(data)
        scope_data = raw.get("scope") if isinstance(raw.get("scope"), Mapping) else raw
        output_data = dict(raw.get("outputs") or {})
        return cls(
            scope=AdminScope.from_mapping(dict(scope_data)),
            outputs=OutputOptions.from_mapping(output_data),
            publish=PublishOptions.from_mapping(raw.get("publish")),
            batch=BatchOptions.from_mapping(raw.get("batch")),
            raw=raw,
        )


@dataclass(frozen=True)
class ValidationIssue:
    """A structured validation issue produced by schema checks."""

    severity: str
    field: str
    message: str
    count: int | None = None


def require_columns(frame: pd.DataFrame, columns: Iterable[str]) -> list[ValidationIssue]:
    """Return issues for missing required columns."""

    present = set(frame.columns)
    return [
        ValidationIssue("error", column, f"Missing required column: {column}")
        for column in columns
        if column not in present
    ]


def validate_value_set(
    frame: pd.DataFrame,
    *,
    columns: Iterable[str],
    allowed_values: Iterable[Any],
    allow_null: bool = True,
) -> list[ValidationIssue]:
    """Validate that selected columns contain only configured values."""

    allowed = set(allowed_values)
    issues: list[ValidationIssue] = []
    for column in columns:
        if column not in frame.columns:
            issues.append(ValidationIssue("error", column, f"Missing value-domain column: {column}"))
            continue
        series = frame[column]
        if allow_null:
            series = series.dropna()
        invalid = ~series.isin(allowed)
        count = int(invalid.sum())
        if count:
            issues.append(
                ValidationIssue(
                    "error",
                    column,
                    f"Column contains values outside {sorted(allowed)}",
                    count,
                )
            )
    return issues


def validate_numeric_range(
    frame: pd.DataFrame,
    *,
    columns: Iterable[str],
    minimum: float | None = None,
    maximum: float | None = None,
    allow_null: bool = True,
) -> list[ValidationIssue]:
    """Validate numeric columns against optional min/max bounds."""

    issues: list[ValidationIssue] = []
    for column in columns:
        if column not in frame.columns:
            issues.append(ValidationIssue("error", column, f"Missing numeric column: {column}"))
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        invalid = values.isna() & frame[column].notna()
        if not allow_null:
            invalid = invalid | values.isna()
        if minimum is not None:
            invalid = invalid | (values < minimum)
        if maximum is not None:
            invalid = invalid | (values > maximum)
        count = int(invalid.sum())
        if count:
            issues.append(ValidationIssue("error", column, "Column failed numeric range validation", count))
    return issues


def columns_ending_with(frame: pd.DataFrame, suffix: str) -> list[str]:
    """Return columns whose names end with a suffix."""

    return [column for column in frame.columns if str(column).endswith(suffix)]
