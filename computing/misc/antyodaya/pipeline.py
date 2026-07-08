"""Mission Antyodaya local pipeline built from CSV plus standard admin geometry."""

from __future__ import annotations

import argparse
import json
import re
import shutil
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
from computing.misc.local_pipeline.schema import (
    ValidationIssue,
    columns_ending_with,
    validate_numeric_range,
    validate_value_set,
)
from computing.misc.local_pipeline.tabular import CSVSQLiteSidecar, csv_header
from nrm_app.celery import app


CONFIG_PATH = Path(__file__).with_name("antyodaya_pipeline.yaml")
ALGORITHM = "local-antyodaya-csv-admin-join"
ALGORITHM_VERSION = "1.0"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


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
    validation = config["validation"]
    category_cluster = [col for col in header if col.endswith(validation["category_cluster_suffix"])]
    category_value = [col for col in header if col.endswith(validation["category_value_suffix"])]
    feature_value = [col for col in header if col.endswith(validation["feature_value_suffix"])]
    metric_columns = set(category_cluster + category_value + feature_value)
    location_columns = [col for col in config["source_location_columns"] if col in header]
    excluded_raw = set(location_columns) | metric_columns
    raw_columns = [col for col in header if col not in excluded_raw]
    return {
        "header": header,
        "location": location_columns,
        "category_cluster": category_cluster,
        "category_value": category_value,
        "feature_value": feature_value,
        "raw": raw_columns,
    }


def _sidecar(config: Mapping[str, Any]) -> CSVSQLiteSidecar:
    return CSVSQLiteSidecar(
        _repo_path(config["sources"]["csv"]),
        table_name=config["name"],
        key_columns=(config["keys"]["source_join_key"],),
        sidecar_path=_repo_path(config["sources"]["sidecar_sqlite"]),
    )


def _normalize_category_clusters(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].where(frame[column].isna(), frame[column].astype(str).str.upper())


def _validate_antyodaya(frame: pd.DataFrame, config: Mapping[str, Any]) -> list[ValidationIssue]:
    validation = config["validation"]
    category_columns = columns_ending_with(frame, validation["category_cluster_suffix"])
    value_columns = columns_ending_with(frame, validation["category_value_suffix"])
    value_columns.extend(columns_ending_with(frame, validation["feature_value_suffix"]))
    issues: list[ValidationIssue] = []
    issues.extend(
        validate_value_set(
            frame,
            columns=category_columns,
            allowed_values=validation["category_cluster_values"],
            allow_null=True,
        )
    )
    issues.extend(
        validate_numeric_range(
            frame,
            columns=value_columns,
            minimum=float(validation["value_min"]),
            maximum=float(validation["value_max"]),
            allow_null=True,
        )
    )
    return issues


def _merge_admin_antyodaya(admin_rows, source_rows: pd.DataFrame, config: Mapping[str, Any]):
    admin_key = config["keys"]["admin_join_key"]
    source_key = config["keys"]["source_join_key"]
    admin = admin_rows.copy()
    attrs = source_rows.copy()
    source_duplicate_columns = {
        "state_name",
        "district_name",
        "sub_district_name",
    }
    attrs = attrs.drop(columns=[col for col in source_duplicate_columns if col in attrs.columns], errors="ignore")
    return admin.merge(attrs, left_on=admin_key, right_on=source_key, how="left", suffixes=("", "_antyodaya"))


def _ordered_tabular_columns(frame: pd.DataFrame, config: Mapping[str, Any], columns: Mapping[str, list[str]]) -> list[str]:
    ordered = []
    ordered.extend(config["standard_admin_columns"])
    ordered.extend([col for col in columns["location"] if col not in ordered and col in frame.columns])
    ordered.extend([col for col in columns["category_cluster"] if col in frame.columns])
    ordered.extend([col for col in columns["category_value"] if col in frame.columns])
    ordered.extend([col for col in columns["feature_value"] if col in frame.columns])
    ordered.extend([col for col in columns["raw"] if col in frame.columns and col not in ordered])
    return [col for col in ordered if col in frame.columns]


def _overview(frame: pd.DataFrame, group_columns: list[str], columns: Mapping[str, list[str]]) -> pd.DataFrame:
    available_group_columns = [col for col in group_columns if col in frame.columns]
    if not available_group_columns:
        return pd.DataFrame()
    rows = []
    for keys, group in frame.groupby(available_group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(available_group_columns, keys))
        row["admin_village_rows"] = int(len(group))
        row["matched_antyodaya_rows"] = int(group["village_key"].notna().sum()) if "village_key" in group else 0
        for column in columns["category_value"]:
            if column in group.columns:
                row[f"{column}_mean"] = float(pd.to_numeric(group[column], errors="coerce").mean())
        for column in columns["category_cluster"]:
            if column not in group.columns:
                continue
            counts = group[column].fillna("NO_DATA").astype(str).str.upper().value_counts()
            for label in ("HIGH", "MEDIUM", "LOW", "NO_DATA"):
                row[f"{column}_{label.lower()}_count"] = int(counts.get(label, 0))
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
    readme = config.get("readme", {})
    lines = [
        f"# {result_name}",
        "",
        f"Generated at: `{utc_now_text()}`",
        "",
        "## What This Contains",
        "",
        "This output joins Mission Antyodaya 2020 village-level category, feature, and raw indicators to the Core Stack standard village/admin boundary for the requested geography.",
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
        f"- Matched Antyodaya rows: `{matched_rows}`",
        f"- Validation issues: `{len(issues)}`",
        "",
        "## Method",
        "",
        "- The requested admin scope is selected from `cs_admin_standard.gpkg` using SQLite indexes.",
        "- Matching Antyodaya rows are fetched from a generated SQLite sidecar for the source CSV.",
        "- Village geometries are joined only after keyed Antyodaya rows are selected.",
        "- Category clusters are normalized to `HIGH`, `MEDIUM`, and `LOW`.",
        "",
        "## Audience",
        "",
    ]
    lines.extend([f"- {item}" for item in readme.get("audience", [])])
    lines.extend(["", "## Cautions", ""])
    lines.extend([f"- {item}" for item in readme.get("cautions", [])])
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


def run_antyodaya_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    """Run a local Antyodaya request and write a standard output bundle."""

    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    columns = _source_columns(config)
    output_config = config["output"]
    layer_name = _layer_name(output_config["layer_prefix"], request.scope.district_name, request.scope.tehsil_name)
    result_name = layer_name or f"{output_config['layer_prefix']}_{slug(request.scope.level)}"
    output_root = _repo_path(output_config["root"]) / slug(request.scope.state_name) / slug(request.scope.district_name) / slug(request.scope.tehsil_name)
    bundle = OutputBundle(output_root, result_name)

    t0 = time.perf_counter()
    admin_source = CSAdminSource(_repo_path(config["sources"]["admin_gpkg"]), table_name=config["sources"]["admin_layer"])
    include_geometry = request.outputs.gpkg or request.publish.sync_to_geoserver
    admin_selection = admin_source.read_scope(AdminScope.from_mapping(asdict(request.scope)), include_geometry=include_geometry)
    timings["read_admin_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    sidecar = _sidecar(config)
    sidecar_status = sidecar.materialize()
    source_rows = sidecar.fetch_by_values(
        config["keys"]["source_join_key"],
        admin_selection.village_ids,
    )
    timings["read_antyodaya_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    _normalize_category_clusters(source_rows, columns["category_cluster"])
    validation_issues = _validate_antyodaya(source_rows, config)
    joined = _merge_admin_antyodaya(admin_selection.rows, source_rows, config)
    ordered_columns = _ordered_tabular_columns(joined, config, columns)
    csv_frame = pd.DataFrame(joined.drop(columns=["geometry"], errors="ignore"))[ordered_columns]
    overview = _overview(csv_frame, ["state_name", "district_name", "TEHSIL"], columns)
    matched_rows = int(csv_frame["village_key"].notna().sum()) if "village_key" in csv_frame else 0
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
                result_name=result_name,
                row_count=len(csv_frame),
                matched_rows=matched_rows,
                issues=validation_issues,
            )
        ).as_posix()
    mapping_output = bundle.path / "antyodaya_2020_mapping.yaml"
    bundle.ensure()
    shutil.copyfile(_repo_path(config["sources"]["mapping_yaml"]), mapping_output)
    paths["mapping_yaml_path"] = mapping_output.as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": result_name,
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
        paths["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result | paths), ".stac_fragment.json").as_posix()
        result["stac_fragment_path"] = paths["stac_fragment_path"]

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
                layer_name=result_name,
                overwrite=request.publish.overwrite,
            )
            geoserver = asdict(geoserver_result)
            links_path = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv")
            result["geoserver_links_path"] = links_path.as_posix()
        except Exception as exc:
            geoserver = {"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)[:500]}
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver

    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["run_metadata_path"] = bundle.write_metadata(
        {
            "request": asdict(request),
            "result": result,
            "config_path": str(config_path),
        }
    ).as_posix()
    return result


def run_antyodaya_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Run a pipeline request from a JSON-like mapping."""

    return run_antyodaya_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True)
def generate_antyodaya_layer_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Generate Mission Antyodaya outputs for one state/district/tehsil."""

    request = _request_from_legacy_args(state, district, block, sync_to_geoserver, overwrite)
    return run_antyodaya_pipeline(request)


def _run_batch(path: str | Path) -> list[dict[str, Any]]:
    results = []
    for request in load_request_file(path):
        results.append(run_antyodaya_pipeline(request))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Mission Antyodaya pipeline.")
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
        result = run_antyodaya_pipeline(
            _request_from_legacy_args(args.state, args.district, args.tehsil, not args.no_geoserver)
        )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
