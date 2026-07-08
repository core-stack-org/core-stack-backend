"""Request parsing, config loading, and lightweight validation helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import yaml

from .admin import AdminScope


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML or JSON config file."""

    path = Path(path)
    text = path.read_text()
    if path.suffix.lower() == ".json":
        return json.loads(text)
    return yaml.safe_load(text) or {}


@dataclass(frozen=True)
class OutputOptions:
    gpkg: bool = True
    csv: bool = True
    readme: bool = True
    eda: bool = True
    stac: bool = True
    geoserver: bool = True
    excel_ready_csv: bool = False

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "OutputOptions":
        values = dict(data or {})
        return cls(**{field.name: bool(values.get(field.name, getattr(cls, field.name))) for field in cls.__dataclass_fields__.values()})


@dataclass(frozen=True)
class PublishOptions:
    sync_to_geoserver: bool = True
    overwrite: bool = True
    register_layers: bool = False
    use_pregenerated: bool = False

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "PublishOptions":
        values = dict(data or {})
        return cls(
            sync_to_geoserver=bool(values.get("sync_to_geoserver", True)),
            overwrite=bool(values.get("overwrite", True)),
            register_layers=bool(values.get("register_layers", False)),
            use_pregenerated=bool(values.get("use_pregenerated", False)),
        )


@dataclass(frozen=True)
class BatchOptions:
    mode: str = "single"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "BatchOptions":
        return cls(mode=str((data or {}).get("mode", "single")))


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
        return cls(
            scope=AdminScope.from_mapping(dict(scope_data)),
            outputs=OutputOptions.from_mapping(raw.get("outputs")),
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
