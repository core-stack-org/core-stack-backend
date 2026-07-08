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
from computing.misc.local_pipeline.batch import load_request_file
from computing.misc.local_pipeline.outputs import OutputBundle, slug, utc_now_text
from computing.misc.local_pipeline.publish import publish_gpkg_layer
from computing.misc.local_pipeline.schema import ValidationIssue, validate_numeric_range
from computing.misc.local_pipeline.tabular import CSVSQLiteSidecar, csv_header
from nrm_app.celery import app


CONFIG_PATH = Path(__file__).with_name("livestocks_pipeline.yaml")
ALGORITHM = "local-livestock-csv-admin-join"
ALGORITHM_VERSION = "1.0"


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
                "gpkg": True,
                "csv": True,
                "readme": True,
                "eda": True,
                "stac": True,
                "geoserver": sync_to_geoserver,
                "excel_ready_csv": True,
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
    )


def _validate_livestock(frame: pd.DataFrame, config: Mapping[str, Any]) -> list[ValidationIssue]:
    return validate_numeric_range(
        frame,
        columns=config["metrics"]["count_columns"],
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
    ordered.extend(config["standard_admin_columns"])
    ordered.extend([col for col in columns["location"] if col not in ordered])
    ordered.extend([col for col in columns["metrics"] if col not in ordered])
    ordered.extend([col for col in columns["raw"] if col not in ordered])
    return [col for col in ordered if col in frame.columns]


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
        "## Cautions",
        "",
    ]
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


def run_livestocks_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    columns = _source_columns(config)
    output_config = config["output"]
    layer_name = _layer_name(output_config["layer_prefix"], request.scope.district_name, request.scope.tehsil_name)
    output_root = _repo_path(output_config["root"]) / slug(request.scope.state_name) / slug(request.scope.district_name) / slug(request.scope.tehsil_name)
    bundle = OutputBundle(output_root, layer_name)

    t0 = time.perf_counter()
    admin_source = CSAdminSource(_repo_path(config["sources"]["admin_gpkg"]), table_name=config["sources"]["admin_layer"])
    include_geometry = request.outputs.gpkg or request.publish.sync_to_geoserver
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
    ordered = _ordered_columns(joined, config, columns)
    csv_frame = pd.DataFrame(joined.drop(columns=["geometry"], errors="ignore"))[ordered]
    overview = _overview(csv_frame, ["state_name", "district_name", "TEHSIL"], config)
    matched_rows = int(csv_frame["village_code"].notna().sum()) if "village_code" in csv_frame else 0
    timings["build_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    paths: dict[str, str] = {}
    t0 = time.perf_counter()
    if request.outputs.csv:
        paths["csv_path"] = bundle.write_csv(csv_frame, ".csv").as_posix()
        if not overview.empty:
            paths["overview_csv_path"] = bundle.write_csv(overview, ".overview.csv").as_posix()
    if request.outputs.excel_ready_csv:
        paths["excel_ready_csv_path"] = bundle.write_csv(csv_frame, ".excel_ready.csv").as_posix()
    if request.outputs.gpkg:
        paths["gpkg_path"] = bundle.write_gpkg({output_config["village_layer"]: joined}).as_posix()
    if request.outputs.eda:
        paths["eda_path"] = bundle.write_eda({"villages": csv_frame, "overview": overview}).as_posix()
    if request.outputs.readme:
        paths["readme_path"] = bundle.write_readme(
            _readme_lines(
                request=request,
                config=config,
                result_name=layer_name,
                row_count=len(csv_frame),
                matched_rows=matched_rows,
                issues=validation_issues,
            )
        ).as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": layer_name,
        "rows": int(len(csv_frame)),
        "matched_rows": matched_rows,
        "join_coverage": round(matched_rows / len(csv_frame), 6) if len(csv_frame) else 0,
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

    if request.outputs.stac:
        result["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result), ".stac_fragment.json").as_posix()

    geoserver = None
    if request.publish.sync_to_geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        if not gpkg_path:
            gpkg_path = bundle.write_gpkg({output_config["village_layer"]: joined}).as_posix()
            result["gpkg_path"] = gpkg_path
        try:
            geoserver_result = publish_gpkg_layer(
                gpkg_path,
                workspace=output_config["geoserver_workspace"],
                layer_name=layer_name,
                overwrite=request.publish.overwrite,
            )
            geoserver = asdict(geoserver_result)
            result["geoserver_links_path"] = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv").as_posix()
        except Exception as exc:
            geoserver = {"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)[:500]}
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver
    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["run_metadata_path"] = bundle.write_metadata(
        {"request": asdict(request), "result": result, "config_path": str(config_path)}
    ).as_posix()
    return result


def run_livestocks_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    return run_livestocks_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True)
def generate_livestocks_layer_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    request = _request_from_legacy_args(state, district, block, sync_to_geoserver, overwrite)
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
