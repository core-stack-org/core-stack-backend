"""Standard output bundle writers for local geospatial pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def slug(value: Any) -> str:
    """Return a filesystem-safe lowercase slug."""

    import re

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


def utc_now_text() -> str:
    """Return an ISO UTC timestamp without microseconds."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def dataframe_eda(frame: pd.DataFrame, *, max_numeric_columns: int = 80) -> dict[str, Any]:
    """Build a compact EDA summary for a tabular or geospatial output."""

    summary: dict[str, Any] = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": [str(column) for column in frame.columns],
        "null_counts": {str(column): int(frame[column].isna().sum()) for column in frame.columns},
    }
    if "geometry" in frame.columns:
        summary["null_geometry_count"] = int(frame["geometry"].isna().sum())
    numeric = frame.select_dtypes(include="number")
    numeric_summary: dict[str, dict[str, float | int | None]] = {}
    for column in list(numeric.columns)[:max_numeric_columns]:
        series = numeric[column].dropna()
        numeric_summary[str(column)] = {
            "count": int(series.count()),
            "min": None if series.empty else float(series.min()),
            "mean": None if series.empty else float(series.mean()),
            "max": None if series.empty else float(series.max()),
        }
    summary["numeric_summary"] = numeric_summary
    return summary


@dataclass
class OutputBundle:
    """Writer for a standard local pipeline output directory."""

    root: str | Path
    name: str

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def path(self) -> Path:
        return self.root / slug(self.name)

    def ensure(self) -> Path:
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def output_path(self, suffix: str) -> Path:
        self.ensure()
        return self.path / f"{slug(self.name)}{suffix}"

    def write_csv(self, frame: pd.DataFrame, suffix: str = ".csv") -> Path:
        path = self.output_path(suffix)
        frame.to_csv(path, index=False)
        return path

    def write_json(self, data: Mapping[str, Any], suffix: str) -> Path:
        path = self.output_path(suffix)
        path.write_text(json.dumps(data, indent=2, default=str) + "\n")
        return path

    def write_metadata(self, data: Mapping[str, Any]) -> Path:
        payload = {"generated_at_utc": utc_now_text(), **dict(data)}
        return self.write_json(payload, ".run_metadata.json")

    def write_eda(self, frames: Mapping[str, pd.DataFrame]) -> Path:
        payload = {name: dataframe_eda(frame) for name, frame in frames.items()}
        return self.write_json(payload, ".eda.json")

    def write_readme(self, lines: list[str]) -> Path:
        self.ensure()
        path = self.path / "README.md"
        path.write_text("\n".join(lines).rstrip() + "\n")
        return path

    def write_gpkg(self, layers: Mapping[str, Any]) -> Path:
        """Write GeoDataFrame layers to a GeoPackage using Fiona."""

        gpkg_path = self.output_path(".gpkg")
        if gpkg_path.exists():
            gpkg_path.unlink()
        first = True
        for layer_name, gdf in layers.items():
            if gdf is None or len(gdf) == 0:
                continue
            gdf.to_file(
                gpkg_path,
                layer=layer_name,
                driver="GPKG",
                mode="w" if first else "a",
                engine="fiona",
            )
            first = False
        if first:
            raise ValueError("No non-empty layers were provided for GeoPackage output")
        return gpkg_path
