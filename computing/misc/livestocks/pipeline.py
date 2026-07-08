"""Livestock census local pipeline built from CSV plus standard admin geometry."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from django.conf import settings

from computing.misc.local_pipeline import AdminScope, CSAdminSource, StandardRequest, load_config
from computing.misc.local_pipeline.admin import admin_output_frame, admin_presentation_frame
from computing.misc.local_pipeline.batch import load_request_file
from computing.misc.local_pipeline.outputs import OutputBundle, input_signatures, slug, stable_hash, utc_now_text
from computing.misc.local_pipeline.publish import publish_gpkg_layer
from computing.misc.local_pipeline.schema import OutputOptions, ValidationIssue, validate_numeric_range
from computing.misc.local_pipeline.tabular import CSVSQLiteSidecar, csv_header
from nrm_app.celery import app


CONFIG_PATH = Path(__file__).with_name("livestocks_pipeline.yaml")
ALGORITHM = "local-livestock-csv-admin-join"
ALGORITHM_VERSION = "1.2"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _layer_name(prefix: str, district: str | None, tehsil: str | None) -> str:
    return f"{prefix}_{slug(district)}_{slug(tehsil)}".strip("_")


def _request_from_legacy_args(
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    output_mode: str = "focused",
) -> StandardRequest:
    return StandardRequest.from_mapping(
        {
            "scope": {
                "level": "tehsil",
                "state_name": state,
                "district_name": district,
                "tehsil_name": block,
            },
            "publish": {
                "sync_to_geoserver": sync_to_geoserver,
                "overwrite": overwrite,
                "register_layers": False,
            },
            "outputs": {
                "mode": output_mode,
                "geoserver": sync_to_geoserver,
            },
        }
    )


def _source_columns(config: Mapping[str, Any]) -> dict[str, list[str]]:
    header = csv_header(_repo_path(config["sources"]["csv"]))
    location = [col for col in config["source_location_columns"] if col in header]
    metrics = [col for col in config["metrics"]["count_columns"] if col in header]
    raw = [col for col in header if col not in set(location + metrics)]
    return {"header": header, "location": location, "metrics": metrics, "raw": raw}


def _sidecar(config: Mapping[str, Any]) -> CSVSQLiteSidecar:
    return CSVSQLiteSidecar(
        _repo_path(config["sources"]["csv"]),
        table_name=config["name"],
        key_columns=(config["keys"]["source_join_key"],),
        sidecar_path=_repo_path(config["sources"]["sidecar_sqlite"]),
        source_columns=tuple(config["metrics"].get("sidecar_columns", [])),
    )


def _schema(config: Mapping[str, Any]) -> dict[str, Any]:
    path = config.get("sources", {}).get("schema_yaml")
    return load_config(_repo_path(path)) if path else {}


def _derive_livestock_metrics(frame: pd.DataFrame, schema: Mapping[str, Any]) -> pd.DataFrame:
    derived = frame.copy()
    for _, animals in schema.get("livestock", {}).items():
        for fields in animals.values():
            male = fields.get("male")
            female = fields.get("female")
            total = fields.get("total")
            if male in derived.columns and female in derived.columns and total:
                derived[total] = (
                    pd.to_numeric(derived[male], errors="coerce").fillna(0)
                    + pd.to_numeric(derived[female], errors="coerce").fillna(0)
                )
    for metric, spec in schema.get("derived_metrics", {}).items():
        sources = spec.get("sources", [])
        if sources and all(source in derived.columns for source in sources):
            derived[metric] = sum(pd.to_numeric(derived[source], errors="coerce").fillna(0) for source in sources)
    return derived


def _validate_livestock(frame: pd.DataFrame, config: Mapping[str, Any]) -> list[ValidationIssue]:
    return validate_numeric_range(
        frame,
        columns=[column for column in config["metrics"]["count_columns"] if column in frame.columns],
        minimum=0,
        allow_null=True,
    )


def _merge_admin_livestock(admin_rows, source_rows: pd.DataFrame, config: Mapping[str, Any]):
    admin = admin_rows.copy()
    attrs = source_rows.copy()
    attrs = attrs.drop(columns=["state_name", "district_name"], errors="ignore")
    return admin.merge(
        attrs,
        left_on=config["keys"]["admin_join_key"],
        right_on=config["keys"]["source_join_key"],
        how="left",
        suffixes=("", "_livestock"),
    )


def _ordered_columns(frame: pd.DataFrame, config: Mapping[str, Any], columns: Mapping[str, list[str]]) -> list[str]:
    ordered = []
    ordered.extend([col for col in columns["location"] if col not in ordered])
    ordered.extend([col for col in columns["metrics"] if col not in ordered])
    ordered.extend([col for col in columns["raw"] if col not in ordered])
    return [col for col in ordered if col in frame.columns]


def _focused_frame(frame: pd.DataFrame, schema: Mapping[str, Any]) -> pd.DataFrame:
    focused = admin_presentation_frame(frame.drop(columns=["geometry"], errors="ignore"))
    source = frame.set_index("fid", drop=False) if "fid" in frame.columns else frame
    output_rows: list[dict[str, Any]] = []
    value_columns = schema.get("focused_columns", [])
    for _, admin_row in focused.iterrows():
        row = admin_row.to_dict()
        admin_index = row.get("index")
        values = source.loc[admin_index] if admin_index in source.index else pd.Series(dtype=object)
        has_village_id = pd.notna(row.get("village_id"))
        has_livestock = pd.notna(values.get("village_code")) if not values.empty else False
        if not has_village_id:
            row["livestock_status"] = "no village id"
        elif not has_livestock:
            row["livestock_status"] = "no livestock row"
        else:
            row["livestock_status"] = "matched"
            for column in value_columns:
                row[column] = values.get(column)
        output_rows.append(row)
    output = pd.DataFrame(output_rows)
    ordered = list(focused.columns)
    ordered.append("livestock_status")
    ordered.extend(column for column in value_columns if column in output.columns)
    return output.reindex(columns=list(dict.fromkeys(ordered)))


def _overview(frame: pd.DataFrame, group_columns: list[str], config: Mapping[str, Any]) -> pd.DataFrame:
    groups = [col for col in group_columns if col in frame.columns]
    if not groups:
        return pd.DataFrame()
    rows = []
    metrics = config["metrics"]["count_columns"]
    for keys, group in frame.groupby(groups, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(groups, keys))
        row["admin_village_rows"] = int(len(group))
        row["matched_livestock_rows"] = int(group["village_code"].notna().sum()) if "village_code" in group else 0
        for metric in metrics:
            if metric in group.columns:
                row[f"{metric}_sum"] = int(pd.to_numeric(group[metric], errors="coerce").fillna(0).sum())
        rows.append(row)
    return pd.DataFrame(rows)


def _readme_lines(
    *,
    request: StandardRequest,
    config: Mapping[str, Any],
    result_name: str,
    row_count: int,
    matched_rows: int,
    issues: list[ValidationIssue],
    geoserver: Mapping[str, Any] | None = None,
) -> list[str]:
    lines = [
        f"# {result_name}",
        "",
        f"Generated at: `{utc_now_text()}`",
        "",
        "## What This Contains",
        "",
        "This output joins village-level livestock census counts to the Core Stack standard village/admin boundary for the requested geography.",
        "",
        "## Request",
        "",
        f"- Level: `{request.scope.level}`",
        f"- State: `{request.scope.state_name}`",
        f"- District: `{request.scope.district_name}`",
        f"- Tehsil/block: `{request.scope.tehsil_name}`",
        "",
        "## Data Quality",
        "",
        f"- Admin village rows: `{row_count}`",
        f"- Matched livestock rows: `{matched_rows}`",
        f"- Validation issues: `{len(issues)}`",
        "",
        "## Method",
        "",
        "- The requested admin scope is selected from `cs_admin_standard.gpkg` using SQLite indexes.",
        "- Matching livestock rows are fetched from a generated SQLite sidecar for the source CSV.",
        "- Village geometries are joined only after keyed livestock rows are selected.",
        "",
    ]
    if geoserver and (geoserver.get("wfs_url") or geoserver.get("wms_url")):
        lines.extend(
            [
                "## GeoServer Layer",
                "",
                f"- WFS GeoJSON: {geoserver.get('wfs_url')}",
                f"- WMS layer: {geoserver.get('wms_url')}",
                "",
            ]
        )
    lines.extend(["## Cautions", ""])
    lines.extend([f"- {item}" for item in config.get("readme", {}).get("cautions", [])])
    return lines


def _stac_fragment(config: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": result["layer_name"],
        "properties": {
            "title": config["title"],
            "description": config["description"],
            "algorithm": ALGORITHM,
            "algorithm_version": ALGORITHM_VERSION,
            "generated_at_utc": utc_now_text(),
            "source_csv": config["sources"]["csv"],
            "source_admin_gpkg": config["sources"]["admin_gpkg"],
        },
        "assets": {
            "data": {"href": result.get("gpkg_path"), "type": "application/geopackage+sqlite3"},
            "readme": {"href": result.get("readme_path"), "type": "text/markdown"},
        },
    }


def _cache_input_signatures(config: Mapping[str, Any], config_path: str | Path) -> dict[str, dict[str, Any]]:
    sources = config.get("sources", {})
    paths: dict[str, str | Path] = {
        "pipeline_config": _repo_path(config_path),
        "admin_gpkg": _repo_path(sources["admin_gpkg"]),
        "livestock_csv": _repo_path(sources["csv"]),
    }
    if sources.get("schema_yaml"):
        paths["schema_yaml"] = _repo_path(sources["schema_yaml"])
    return input_signatures(paths)


def _cache_key(request: StandardRequest, outputs: OutputOptions) -> str:
    publish_options = asdict(request.publish)
    publish_options.pop("use_pregenerated", None)
    return stable_hash(
        {
            "algorithm": ALGORITHM,
            "algorithm_version": ALGORITHM_VERSION,
            "scope": asdict(request.scope),
            "outputs": asdict(outputs),
            "publish": publish_options,
        }
    )


def _required_result_paths(outputs: OutputOptions, request: StandardRequest) -> tuple[str, ...]:
    required: list[str] = ["run_metadata_path"]
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        required.append("gpkg_path")
    if outputs.focused_csv:
        required.append("focused_csv_path")
    if outputs.excel_ready_csv:
        required.append("excel_ready_csv_path")
    if outputs.eda:
        required.append("eda_path")
    if outputs.readme:
        required.append("readme_path")
    if outputs.stac:
        required.append("stac_fragment_path")
    if request.publish.sync_to_geoserver and outputs.geoserver:
        required.append("geoserver_links_path")
    return tuple(dict.fromkeys(required))


def run_livestocks_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    outputs = request.outputs
    if outputs.mode == "default" and config.get("default_outputs"):
        outputs = OutputOptions.from_mapping(config["default_outputs"])
    schema = _schema(config)
    columns = _source_columns(config)
    output_config = config["output"]
    layer_name = _layer_name(output_config["layer_prefix"], request.scope.district_name, request.scope.tehsil_name)
    output_root = _repo_path(output_config["root"]) / slug(request.scope.state_name) / slug(request.scope.district_name) / slug(request.scope.tehsil_name)
    bundle = OutputBundle(output_root, layer_name)
    cache_key = _cache_key(request, outputs)
    cache_signatures = _cache_input_signatures(config, config_path)
    required_result_paths = _required_result_paths(outputs, request)
    if request.publish.use_pregenerated:
        cached = bundle.cached_result(
            cache_key=cache_key,
            signatures=cache_signatures,
            required_result_paths=required_result_paths,
        )
        if cached:
            cached["cache_hit"] = True
            return cached

    t0 = time.perf_counter()
    admin_source = CSAdminSource(_repo_path(config["sources"]["admin_gpkg"]), table_name=config["sources"]["admin_layer"])
    include_geometry = outputs.gpkg or request.publish.sync_to_geoserver
    admin_selection = admin_source.read_scope(AdminScope.from_mapping(asdict(request.scope)), include_geometry=include_geometry)
    timings["read_admin_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    sidecar = _sidecar(config)
    sidecar_status = sidecar.materialize()
    source_rows = sidecar.fetch_by_values(config["keys"]["source_join_key"], admin_selection.pc11_village_ids)
    timings["read_livestock_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    validation_issues = _validate_livestock(source_rows, config)
    joined = _merge_admin_livestock(admin_selection.rows, source_rows, config)
    joined = _derive_livestock_metrics(joined, schema)
    ordered = _ordered_columns(joined, config, columns)
    csv_frame = admin_output_frame(joined.drop(columns=["geometry"], errors="ignore"), value_columns=ordered)
    focused_frame = _focused_frame(joined, schema)
    overview = _overview(csv_frame, ["state_name", "district_name", "tehsil_name"], config)
    matched_rows = int(csv_frame["village_code"].notna().sum()) if "village_code" in csv_frame else 0
    gpkg_frame = admin_output_frame(joined, value_columns=ordered, include_geometry=True)
    timings["build_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    paths: dict[str, str] = {}
    t0 = time.perf_counter()
    if outputs.csv or outputs.verbose_csv:
        if outputs.verbose_csv:
            paths["csv_path"] = bundle.write_csv(csv_frame, ".csv").as_posix()
        if not overview.empty:
            paths["overview_csv_path"] = bundle.write_csv(overview, ".overview.csv").as_posix()
    if outputs.focused_csv:
        paths["focused_csv_path"] = bundle.write_csv(focused_frame, ".focused.csv").as_posix()
    if outputs.excel_ready_csv:
        paths["excel_ready_csv_path"] = bundle.write_csv(focused_frame, ".excel_ready.csv").as_posix()
    if outputs.gpkg:
        paths["gpkg_path"] = bundle.write_gpkg({output_config["village_layer"]: gpkg_frame}).as_posix()
    if outputs.eda:
        paths["eda_path"] = bundle.write_eda({"villages": csv_frame, "focused": focused_frame, "overview": overview}).as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": layer_name,
        "rows": int(len(csv_frame)),
        "focused_rows": int(len(focused_frame)),
        "matched_rows": matched_rows,
        "join_coverage": round(matched_rows / len(csv_frame), 6) if len(csv_frame) else 0,
        "output_mode": outputs.mode,
        "validation_issues": [asdict(issue) for issue in validation_issues],
        "sidecar": sidecar_status,
        "admin_created_indexes": admin_selection.created_indexes,
        "state_name": request.scope.state_name,
        "district_name": request.scope.district_name,
        "tehsil": request.scope.tehsil_name,
        "output_dir": bundle.path.as_posix(),
        "sync_to_geoserver": request.publish.sync_to_geoserver,
        **paths,
    }

    if outputs.stac:
        result["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result), ".stac_fragment.json").as_posix()

    geoserver = None
    if request.publish.sync_to_geoserver and outputs.geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        geoserver_workspace = request.publish.geoserver_workspace or output_config["geoserver_workspace"]
        if not gpkg_path:
            geoserver = {
                "ok": False,
                "status": "missing_gpkg",
                "workspace": geoserver_workspace,
                "layer_name": layer_name,
                "error": "GeoPackage output is required for GeoServer publishing.",
            }
        else:
            try:
                geoserver_result = publish_gpkg_layer(
                    gpkg_path,
                    workspace=geoserver_workspace,
                    layer_name=layer_name,
                    overwrite=request.publish.overwrite,
                )
                geoserver = asdict(geoserver_result)
                geoserver["ok"] = True
                geoserver["status"] = "published"
                result["geoserver_links_path"] = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv").as_posix()
            except Exception as exc:
                geoserver = {
                    "ok": False,
                    "status": "publish_failed",
                    "workspace": geoserver_workspace,
                    "layer_name": layer_name,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc)[:500],
                }
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver
    if outputs.geoserver and "geoserver_links_path" not in result:
        stale_links = bundle.output_path(".geoserver_links.csv")
        if stale_links.exists():
            stale_links.unlink()
    if outputs.readme:
        result["readme_path"] = bundle.write_readme(
            _readme_lines(
                request=request,
                config=config,
                result_name=layer_name,
                row_count=len(csv_frame),
                matched_rows=matched_rows,
                issues=validation_issues,
                geoserver=geoserver,
            )
        ).as_posix()
    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["run_metadata_path"] = bundle.write_metadata(
        {"request": asdict(request), "result": result, "config_path": str(config_path)}
    ).as_posix()
    result["cache_manifest_path"] = bundle.write_cache_manifest(
        {
            "cache_key": cache_key,
            "input_signatures": cache_signatures,
            "required_result_paths": required_result_paths,
            "result": result,
        }
    ).as_posix()
    return result


def run_livestocks_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    return run_livestocks_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True)
def generate_livestocks_layer_task(
    self,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    output_mode: str = "focused",
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if payload is not None:
        return run_livestocks_request(payload)
    if not (state and district and block):
        raise ValueError("state, district, and block are required when payload is not provided")
    request = _request_from_legacy_args(state, district, block, sync_to_geoserver, overwrite, output_mode)
    return run_livestocks_pipeline(request)


def _run_batch(path: str | Path) -> list[dict[str, Any]]:
    return [run_livestocks_pipeline(request) for request in load_request_file(path)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local livestock pipeline.")
    parser.add_argument("--request-file", help="JSON, YAML, CSV, or simple text request file")
    parser.add_argument("--state")
    parser.add_argument("--district")
    parser.add_argument("--tehsil")
    parser.add_argument("--no-geoserver", action="store_true")
    args = parser.parse_args()
    if args.request_file:
        result = _run_batch(args.request_file)
    else:
        if not (args.state and args.district and args.tehsil):
            parser.error("--state, --district, and --tehsil are required without --request-file")
        result = run_livestocks_pipeline(
            _request_from_legacy_args(args.state, args.district, args.tehsil, not args.no_geoserver)
        )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
