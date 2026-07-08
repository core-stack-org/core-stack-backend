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
from computing.misc.local_pipeline.admin import admin_output_frame, admin_presentation_frame
from computing.misc.local_pipeline.batch import load_request_file
from computing.misc.local_pipeline.outputs import OutputBundle, input_signatures, slug, stable_hash, utc_now_text
from computing.misc.local_pipeline.publish import publish_gpkg_layer
from computing.misc.local_pipeline.schema import (
    OutputOptions,
    ValidationIssue,
    columns_ending_with,
    validate_numeric_range,
    validate_value_set,
)
from computing.misc.local_pipeline.tabular import CSVSQLiteSidecar, csv_header
from nrm_app.celery import app


CONFIG_PATH = Path(__file__).with_name("antyodaya_pipeline.yaml")
ALGORITHM = "local-antyodaya-csv-admin-join"
ALGORITHM_VERSION = "1.2"


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


def _sidecar(config: Mapping[str, Any], columns: Mapping[str, list[str]]) -> CSVSQLiteSidecar:
    source_columns = [
        config["keys"]["source_join_key"],
        config["keys"]["source_unique_key"],
        *columns["category_cluster"],
        *columns["category_value"],
        *columns["feature_value"],
        *columns["raw"],
    ]
    return CSVSQLiteSidecar(
        _repo_path(config["sources"]["csv"]),
        table_name=config["name"],
        key_columns=(config["keys"]["source_join_key"],),
        sidecar_path=_repo_path(config["sources"]["sidecar_sqlite"]),
        source_columns=tuple(dict.fromkeys(source_columns)),
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


def _ordered_tabular_columns(frame: pd.DataFrame, columns: Mapping[str, list[str]]) -> list[str]:
    ordered = []
    ordered.extend([col for col in columns["location"] if col not in ordered and col in frame.columns])
    ordered.extend([col for col in columns["category_cluster"] if col in frame.columns])
    ordered.extend([col for col in columns["category_value"] if col in frame.columns])
    ordered.extend([col for col in columns["feature_value"] if col in frame.columns])
    ordered.extend([col for col in columns["raw"] if col in frame.columns and col not in ordered])
    return [col for col in ordered if col in frame.columns]


def _focused_value_columns(frame: pd.DataFrame, columns: Mapping[str, list[str]]) -> list[str]:
    """Return Antyodaya columns for report/Excel outputs."""

    ordered = ["antyodaya_status"]
    ordered.extend([col for col in columns["category_cluster"] if col in frame.columns])
    ordered.extend([col for col in columns["category_value"] if col in frame.columns])
    ordered.extend([col for col in columns["raw"] if col in frame.columns])
    return [col for col in ordered if col in frame.columns or col == "antyodaya_status"]


def _focused_frame(frame: pd.DataFrame, columns: Mapping[str, list[str]]) -> pd.DataFrame:
    focused = admin_presentation_frame(frame.drop(columns=["geometry"], errors="ignore"))
    source = frame.set_index("fid", drop=False) if "fid" in frame.columns else frame
    metric_columns = [
        *columns["category_cluster"],
        *columns["category_value"],
        *columns["raw"],
    ]
    output_rows: list[dict[str, Any]] = []
    for _, admin_row in focused.iterrows():
        row = admin_row.to_dict()
        admin_index = row.get("index")
        values = source.loc[admin_index] if admin_index in source.index else pd.Series(dtype=object)
        has_village_id = pd.notna(row.get("village_id"))
        has_antyodaya = pd.notna(values.get("village_key")) if not values.empty else False
        if not has_village_id:
            row["antyodaya_status"] = "no village id"
        elif not has_antyodaya:
            row["antyodaya_status"] = "no antyodaya row"
        else:
            row["antyodaya_status"] = "matched"
            for column in metric_columns:
                if column in values:
                    row[column] = values.get(column)
        output_rows.append(row)
    output = pd.DataFrame(output_rows)
    ordered = list(focused.columns)
    ordered.extend(column for column in _focused_value_columns(output, columns) if column not in ordered)
    return output.reindex(columns=ordered)


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
        "- Feature-level cluster columns are intentionally excluded; category-level clusters carry the standard cluster interpretation.",
        "",
        "## Reports And Explorer",
        "",
        f"- Full report PDF: `{config['sources'].get('report_pdf')}`",
        f"- Blog/short PDF: `{config['sources'].get('blog_pdf')}`",
        f"- GEE explorer: {config['sources'].get('gee_explorer_url')}",
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


def _cache_input_signatures(config: Mapping[str, Any], config_path: str | Path) -> dict[str, dict[str, Any]]:
    sources = config.get("sources", {})
    paths: dict[str, str | Path] = {
        "pipeline_config": _repo_path(config_path),
        "admin_gpkg": _repo_path(sources["admin_gpkg"]),
        "antyodaya_csv": _repo_path(sources["csv"]),
        "mapping_yaml": _repo_path(sources["mapping_yaml"]),
    }
    for key in ("report_pdf", "blog_pdf"):
        if sources.get(key):
            paths[key] = _repo_path(sources[key])
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
    required: list[str] = ["run_metadata_path", "mapping_yaml_path"]
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
    if outputs.geoserver:
        required.append("geoserver_links_path")
    return tuple(dict.fromkeys(required))


def run_antyodaya_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    """Run a local Antyodaya request and write a standard output bundle."""

    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = load_config(config_path)
    outputs = request.outputs
    if outputs.mode == "default" and config.get("default_outputs"):
        outputs = OutputOptions.from_mapping(config["default_outputs"])
    columns = _source_columns(config)
    output_config = config["output"]
    layer_name = _layer_name(output_config["layer_prefix"], request.scope.district_name, request.scope.tehsil_name)
    result_name = layer_name or f"{output_config['layer_prefix']}_{slug(request.scope.level)}"
    output_root = _repo_path(output_config["root"]) / slug(request.scope.state_name) / slug(request.scope.district_name) / slug(request.scope.tehsil_name)
    bundle = OutputBundle(output_root, result_name)
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
    sidecar = _sidecar(config, columns)
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
    ordered_columns = _ordered_tabular_columns(joined, columns)
    csv_frame = admin_output_frame(joined.drop(columns=["geometry"], errors="ignore"), value_columns=ordered_columns)
    focused_frame = _focused_frame(joined, columns)
    overview = _overview(csv_frame, ["state_name", "district_name", "tehsil_name"], columns)
    matched_rows = int(csv_frame["village_key"].notna().sum()) if "village_key" in csv_frame else 0
    gpkg_frame = admin_output_frame(joined, value_columns=ordered_columns, include_geometry=True)
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
    if outputs.readme:
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
        paths["stac_fragment_path"] = bundle.write_json(_stac_fragment(config, result | paths), ".stac_fragment.json").as_posix()
        result["stac_fragment_path"] = paths["stac_fragment_path"]

    geoserver = None
    if outputs.geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        if not request.publish.sync_to_geoserver:
            geoserver = {
                "ok": False,
                "status": "not_requested",
                "workspace": output_config["geoserver_workspace"],
                "layer_name": result_name,
            }
        elif not gpkg_path:
            geoserver = {
                "ok": False,
                "status": "missing_gpkg",
                "workspace": output_config["geoserver_workspace"],
                "layer_name": result_name,
                "error": "GeoPackage output is required for GeoServer publishing.",
            }
        else:
            try:
                geoserver_result = publish_gpkg_layer(
                    gpkg_path,
                    workspace=output_config["geoserver_workspace"],
                    layer_name=result_name,
                    overwrite=request.publish.overwrite,
                )
                geoserver = asdict(geoserver_result)
                geoserver.setdefault("status", "published" if geoserver.get("ok") else "publish_failed")
            except Exception as exc:
                geoserver = {
                    "ok": False,
                    "status": "publish_failed",
                    "workspace": output_config["geoserver_workspace"],
                    "layer_name": result_name,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc)[:500],
                }
        links_path = bundle.write_csv(pd.DataFrame([geoserver]), ".geoserver_links.csv")
        result["geoserver_links_path"] = links_path.as_posix()
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver

    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    result["run_metadata_path"] = bundle.write_metadata(
        {
            "request": asdict(request),
            "effective_outputs": asdict(outputs),
            "result": result,
            "config_path": str(config_path),
        }
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


def run_antyodaya_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Run a pipeline request from a JSON-like mapping."""

    return run_antyodaya_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True)
def generate_antyodaya_layer_task(
    self,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    output_mode: str = "focused",
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate Mission Antyodaya outputs for one state/district/tehsil."""

    if payload is not None:
        return run_antyodaya_request(payload)
    if not (state and district and block):
        raise ValueError("state, district, and block are required when payload is not provided")
    request = _request_from_legacy_args(state, district, block, sync_to_geoserver, overwrite, output_mode)
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
