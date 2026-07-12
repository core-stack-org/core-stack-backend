#!/usr/bin/env python3
"""Merge recorded per-scope pipeline GeoPackages into pan-India GeoPackages.

This is step 2 of the pan-India asset build. It reads the run state written by
``run_pipelines.py``, unions the per-scope layer schemas, creates each
pan-India layer once, and appends the per-scope features in bounded row
batches through pyogrio (the same chunked GDAL write path used by the proven
admin-constrained asset builder). Columns are the union of the per-scope
schemas (scopes without data for a column carry nulls), so the merged
structure stays exactly what the tehsil and district API calls produce.

Example:
    PROJ_LIB=/usr/share/proj \
    python -m utilities.scripts.build_pan_india_gee_assets.merge_outputs \
      --asset all
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Sequence

import fiona
import pandas as pd
import pyogrio

from utilities.scripts.build_pan_india_gee_assets.common import (
    DEFAULT_CONFIG,
    load_config,
    log,
    repo_path,
    setup_logging,
    utc_now_text,
    write_yaml,
    RunState,
)


SUCCESS_STATUSES = {"success", "cached"}
DEFAULT_BATCH_ROWS = 100_000

#: Column-family promotion for merged schemas: integers widen to float when a
#: source is missing the column (nulls need NaN), floats absorb integers, and
#: any other mixture falls back to text.
_INT_DTYPES = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}
_FLOAT_DTYPES = {"float32", "float64"}


def _dtype_family(dtype: str) -> str:
    if dtype in _INT_DTYPES:
        return "int"
    if dtype in _FLOAT_DTYPES:
        return "float"
    if dtype == "bool":
        return "bool"
    if dtype.startswith("datetime"):
        return "datetime"
    return "text"


def _union_family(families: set[str], *, in_all_sources: bool) -> str:
    if families == {"bool"}:
        return "bool" if in_all_sources else "float"
    if families <= {"bool", "int"}:
        return "int" if in_all_sources else "float"
    if families <= {"bool", "int", "float"}:
        return "float"
    if families == {"datetime"}:
        return "datetime"
    return "text"


_FAMILY_FIONA_TYPE = {"bool": "bool", "int": "int64", "float": "float", "datetime": "datetime", "text": "str"}


def with_suffix_token(path: Path, token: str | None) -> Path:
    if not token:
        return path
    clean = token if token.startswith(".") else f".{token}"
    return path.with_name(f"{path.stem}{clean}{path.suffix}")


def asset_sources(asset_cfg: dict[str, Any], state: RunState) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (usable sources, problem records) for one configured asset."""

    path_key = asset_cfg["result_path_key"]
    configured_layer = asset_cfg["source_layer"]
    sources: list[dict[str, Any]] = []
    problems: list[dict[str, Any]] = []
    records = sorted(
        (record for record in state.iter_records(pipeline=asset_cfg["pipeline"]) if record.get("status") in SUCCESS_STATUSES),
        key=lambda record: record["key"],
    )
    for record in records:
        path_text = (record.get("paths") or {}).get(path_key)
        layer = record.get("layer_name") if configured_layer == "__scoped__" else configured_layer
        if not path_text or not layer:
            problems.append({"key": record["key"], "issue": f"missing {path_key} or layer name"})
            continue
        path = Path(path_text)
        if not path.exists():
            problems.append({"key": record["key"], "issue": f"recorded file missing: {path_text}"})
            continue
        if layer not in fiona.listlayers(path):
            # A valid empty result: the pipeline skips writing empty layers.
            problems.append({"key": record["key"], "issue": f"layer {layer!r} not present (empty for this scope)"})
            continue
        sources.append({"key": record["key"], "path": path, "layer": layer})
    return sources, problems


def scan_schema(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Union field families, geometry types, and CRS across source layers."""

    field_families: dict[str, set[str]] = {}
    field_presence: dict[str, int] = {}
    field_order: list[str] = []
    geometry_types: set[str] = set()
    crs = None
    total_rows = 0
    for source in sources:
        info = pyogrio.read_info(source["path"], layer=source["layer"])
        if crs is None:
            crs = info.get("crs")
        elif info.get("crs") and str(info["crs"]) != str(crs):
            raise ValueError(f"CRS mismatch in {source['path']}: {info['crs']} != {crs}")
        geometry_types.add(str(info.get("geometry_type") or "Unknown"))
        total_rows += int(info.get("features") or 0)
        for name, dtype in zip(info["fields"], info["dtypes"]):
            name = str(name)
            if name not in field_families:
                field_families[name] = set()
                field_presence[name] = 0
                field_order.append(name)
            field_families[name].add(_dtype_family(str(dtype)))
            field_presence[name] += 1
    families = {
        name: _union_family(field_families[name], in_all_sources=field_presence[name] == len(sources))
        for name in field_order
    }
    geometry = geometry_types.pop() if len(geometry_types) == 1 else "Unknown"
    return {
        "fields": field_order,
        "families": families,
        "geometry": geometry,
        "crs": crs,
        "expected_rows": total_rows,
    }


def create_empty_layer(output: Path, layer: str, schema_info: dict[str, Any]) -> None:
    """Create the merged layer once with the union schema, then append via pyogrio."""

    properties = {name: _FAMILY_FIONA_TYPE[schema_info["families"][name]] for name in schema_info["fields"]}
    with fiona.open(
        output,
        "w",
        driver="GPKG",
        layer=layer,
        schema={"geometry": schema_info["geometry"], "properties": properties},
        crs=str(schema_info["crs"]) if schema_info["crs"] else None,
    ):
        pass


def conform_frame(frame: Any, schema_info: dict[str, Any]) -> Any:
    """Reindex one source frame to the union schema with safe dtype coercion."""

    geometry = frame.geometry
    data = pd.DataFrame(frame.drop(columns=[frame.geometry.name]))
    data = data.reindex(columns=schema_info["fields"])
    for name in schema_info["fields"]:
        family = schema_info["families"][name]
        column = data[name]
        if family == "float":
            data[name] = pd.to_numeric(column, errors="coerce").astype("float64")
        elif family == "int":
            data[name] = column.astype("int64")
        elif family == "bool":
            data[name] = column.astype(bool)
        elif family == "text" and str(column.dtype) != "object":
            data[name] = column.astype(object).where(column.notna(), None)
        elif family == "text":
            data[name] = column.where(column.notna(), None)
    import geopandas as gpd

    return gpd.GeoDataFrame(data, geometry=geometry.values, crs=frame.crs)


def merge_asset(
    asset_name: str,
    asset_cfg: dict[str, Any],
    merge_cfg: dict[str, Any],
    state: RunState,
    *,
    output_suffix: str | None,
    batch_rows: int,
    fresh_outputs: set[Path],
) -> dict[str, Any]:
    started = time.monotonic()
    sources, problems = asset_sources(asset_cfg, state)
    if not sources:
        raise RuntimeError(f"No merged sources available for {asset_name}; run step 1 first")

    log.info("%s: scanning %s source layers", asset_name, len(sources))
    schema_info = scan_schema(sources)
    output = with_suffix_token(repo_path(merge_cfg["output_dir"]) / asset_cfg["gpkg"], output_suffix)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output not in fresh_outputs:
        # First layer written into this GeoPackage in this invocation replaces
        # the whole file; later layers are added alongside it.
        if output.exists():
            output.unlink()
        fresh_outputs.add(output)
    create_empty_layer(output, asset_cfg["layer"], schema_info)

    rows = 0
    sources_read = 0
    pending: list[Any] = []
    pending_rows = 0
    last_logged = time.monotonic()

    def flush() -> None:
        nonlocal rows, pending, pending_rows
        if not pending:
            return
        batch = pd.concat(pending, ignore_index=True) if len(pending) > 1 else pending[0]
        pyogrio.write_dataframe(batch, output, layer=asset_cfg["layer"], driver="GPKG", append=True)
        rows += len(batch)
        pending = []
        pending_rows = 0

    for source in sources:
        frame = pyogrio.read_dataframe(source["path"], layer=source["layer"], fid_as_index=False)
        pending.append(conform_frame(frame, schema_info))
        pending_rows += len(frame)
        sources_read += 1
        if pending_rows >= batch_rows:
            flush()
        if time.monotonic() - last_logged > 30:
            log.info(
                "%s: %s/%s sources, %s rows written",
                asset_name,
                sources_read,
                len(sources),
                f"{rows:,}",
            )
            last_logged = time.monotonic()
    flush()

    written = int(pyogrio.read_info(output, layer=asset_cfg["layer"]).get("features") or 0)
    if written != schema_info["expected_rows"]:
        raise RuntimeError(
            f"{asset_name}: merged rows {written} != expected {schema_info['expected_rows']} from source scan"
        )
    log.info("%s: %s/%s sources, %s rows written", asset_name, sources_read, len(sources), f"{rows:,}")

    return {
        "asset": asset_name,
        "output": output.as_posix(),
        "layer": asset_cfg["layer"],
        "rows": written,
        "columns": len(schema_info["fields"]) + 1,
        "geometry": schema_info["geometry"],
        "crs": str(schema_info["crs"]),
        "sources_merged": len(sources),
        "sources_skipped": len(problems),
        "skipped_details": problems[:50],
        "size_bytes": output.stat().st_size,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--asset", default="all", help="One configured asset name, or all.")
    parser.add_argument("--output-suffix", default=None, help="Token appended to output filenames for smoke runs.")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS, help="Rows accumulated per append batch.")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    setup_logging(args.debug, log_file=repo_path(config["run"].get("log_file", "logs/pan_india_gee_assets.log")))
    state = RunState(repo_path(config["run"]["state_file"]))
    assets = config["assets"]
    if args.asset != "all":
        if args.asset not in assets:
            raise KeyError(f"Unknown asset {args.asset!r}; choose one of: all, {', '.join(assets)}")
        assets = {args.asset: assets[args.asset]}

    summary: dict[str, Any] = {"merged_at_utc": utc_now_text(), "state_counts": state.counts(), "assets": {}}
    failures = 0
    fresh_outputs: set[Path] = set()
    for name, asset_cfg in assets.items():
        try:
            summary["assets"][name] = merge_asset(
                name,
                asset_cfg,
                config["merge"],
                state,
                output_suffix=args.output_suffix,
                batch_rows=max(1, args.batch_rows),
                fresh_outputs=fresh_outputs,
            )
            log.info(
                "%s: wrote %s rows to %s",
                name,
                f"{summary['assets'][name]['rows']:,}",
                summary["assets"][name]["output"],
            )
        except Exception as exc:  # noqa: BLE001 - keep merging the other assets
            failures += 1
            summary["assets"][name] = {"asset": name, "error_type": exc.__class__.__name__, "error": str(exc)[:500]}
            log.error("%s: merge failed: %s: %s", name, exc.__class__.__name__, exc)

    summary_path = with_suffix_token(repo_path(config["merge"]["summary_yaml"]), args.output_suffix)
    write_yaml(summary_path, summary)
    log.info("Wrote merge summary: %s", summary_path)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
