"""Mission Antyodaya local pipeline built from CSV plus standard admin geometry."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from django.conf import settings

from utilities.pipelines import AdminScope, CSAdminSource, StandardRequest, load_config
from utilities.pipelines.admin import (
    ADMIN_COLUMN_DESCRIPTIONS,
    admin_output_frame,
    admin_presentation_frame,
)
from utilities.pipelines.outputs import (
    OutputBundle,
    column_dictionary,
    frame_profile,
    input_signatures,
    slug,
    stable_hash,
    utc_now_text,
)
from utilities.pipelines.publish import publish_gpkg_layer, register_layer
from utilities.pipelines.schema import (
    STATUS_MATCHED,
    STATUS_NO_DATA,
    STATUS_NO_VILLAGE_ID,
    OutputOptions,
    ValidationIssue,
    columns_ending_with,
    resolve_output_options,
    status_column_config,
    validate_numeric_range,
    validate_value_set,
)
from utilities.pipelines.tabular import CSVSQLiteSidecar, csv_header
from utilities.pipelines.unicode import normalize_unicode_frame
from nrm_app.celery import app
from utilities.constants import ANTYODAYA_GEOSERVER_WORKSPACE


CONFIG_PATH = Path(__file__).with_name("antyodaya_pipeline.yaml")
ALGORITHM = "local-antyodaya-csv-admin-join"
ALGORITHM_VERSION = "2.0"


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _layer_name(prefix: str, district: str | None, tehsil: str | None) -> str:
    return f"{prefix}_{slug(district)}_{slug(tehsil)}".strip("_")


def _cli_request(state: str, district: str, tehsil: str, sync_to_geoserver: bool = True) -> StandardRequest:
    return StandardRequest.from_mapping(
        {
            "scope": {
                "level": "tehsil",
                "state_name": state,
                "district_name": district,
                "tehsil_name": tehsil,
            },
            "publish": {"sync_to_geoserver": sync_to_geoserver, "overwrite": False},
            "outputs": {"geoserver": sync_to_geoserver},
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


def _report_value_columns(columns: Mapping[str, list[str]]) -> list[str]:
    """Report CSV value columns: category clusters, then category values, then
    raw survey columns. Feature-level columns stay out of the report CSV."""

    return [
        *columns["category_cluster"],
        *columns["category_value"],
        *columns["raw"],
    ]


def _focused_frame(frame: pd.DataFrame, columns: Mapping[str, list[str]], status_name: str | None) -> pd.DataFrame:
    focused = admin_presentation_frame(frame.drop(columns=["geometry"], errors="ignore"))
    source = frame.set_index("fid", drop=False) if "fid" in frame.columns else frame
    metric_columns = _report_value_columns(columns)
    output_rows: list[dict[str, Any]] = []
    for _, admin_row in focused.iterrows():
        row = admin_row.to_dict()
        admin_index = row.get("index")
        values = source.loc[admin_index] if admin_index in source.index else pd.Series(dtype=object)
        has_village_id = pd.notna(row.get("village_id"))
        has_antyodaya = pd.notna(values.get("village_key")) if not values.empty else False
        status = STATUS_MATCHED
        if not has_village_id:
            status = STATUS_NO_VILLAGE_ID
        elif not has_antyodaya:
            status = STATUS_NO_DATA
        else:
            for column in metric_columns:
                if column in values:
                    row[column] = values.get(column)
        if status_name:
            row[status_name] = status
        output_rows.append(row)
    output = pd.DataFrame(output_rows)
    ordered = list(focused.columns)
    if status_name:
        ordered.append(status_name)
    ordered.extend(column for column in metric_columns if column in output.columns and column not in ordered)
    return output.reindex(columns=ordered)


def _category_titles(config: Mapping[str, Any]) -> dict[str, str]:
    """Map category column stems to readable titles from the mapping YAML."""

    titles: dict[str, str] = {}
    try:
        mapping = load_config(_repo_path(config["sources"]["mapping_yaml"]))
    except Exception:
        return titles
    for entry in mapping.get("categories", []):
        name = str(entry.get("category") or "").strip()
        if name:
            titles[slug(name)] = name
    return titles


def _raw_column_categories(config: Mapping[str, Any]) -> dict[str, list[str]]:
    """Map raw survey columns to the categories that use them."""

    used_by: dict[str, list[str]] = {}
    try:
        mapping = load_config(_repo_path(config["sources"]["mapping_yaml"]))
    except Exception:
        return used_by
    for entry in mapping.get("categories", []):
        name = str(entry.get("category") or "").strip()
        for column in entry.get("raw_columns_used", []) or []:
            used_by.setdefault(str(column), []).append(name)
    return used_by


def _column_describer(config: Mapping[str, Any], columns: Mapping[str, list[str]]):
    """Describe Antyodaya output columns from suffix structure and mapping YAML."""

    validation = config["validation"]
    cluster_suffix = validation["category_cluster_suffix"]
    value_suffix = validation["category_value_suffix"]
    feature_suffix = validation["feature_value_suffix"]
    titles = _category_titles(config)
    raw_used_by = _raw_column_categories(config)
    status_name, _ = status_column_config(config)
    raw_columns = set(columns["raw"])

    def title_for(stem: str) -> str:
        return titles.get(stem, stem.replace("_", " ").title())

    def describe(name: str) -> str | None:
        if name in ADMIN_COLUMN_DESCRIPTIONS:
            return ADMIN_COLUMN_DESCRIPTIONS[name]
        if status_name and name == status_name:
            return (
                "Row data availability: `matched` when a Mission Antyodaya 2020 record was joined, "
                f"`{STATUS_NO_VILLAGE_ID}` when the admin row lacks a village identifier, and "
                f"`{STATUS_NO_DATA}` when no Antyodaya record matched this village."
            )
        if name == "village_key":
            return "Unique Mission Antyodaya 2020 village record key."
        if name.endswith(cluster_suffix):
            return (
                f"Relative class (LOW/MEDIUM/HIGH) of the {title_for(name[: -len(cluster_suffix)])} category "
                "index, assigned by MiniBatch K-means over the 2020 all-India distribution."
            )
        if name.endswith(value_suffix):
            return f"Normalized (0-1) {title_for(name[: -len(value_suffix)])} category index value."
        if name.endswith(feature_suffix):
            return f"Normalized (0-1) {title_for(name[: -len(feature_suffix)])} feature value."
        if name in raw_columns:
            categories = raw_used_by.get(name)
            if categories:
                return f"Raw Mission Antyodaya 2020 survey column (used in: {', '.join(categories)})."
            return "Raw Mission Antyodaya 2020 survey column."
        return None

    return describe


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


def _column_reference_lines(column_entries: list[Mapping[str, Any]]) -> list[str]:
    lines = [
        "## Column Reference",
        "",
        "| Column | Type | Description |",
        "| --- | --- | --- |",
    ]
    for entry in column_entries:
        description = str(entry.get("description") or "").replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{entry['column']}` | {entry.get('datatype', '')} | {description} |")
    lines.append("")
    return lines


def _readme_lines(
    *,
    request: StandardRequest,
    config: Mapping[str, Any],
    result_name: str,
    row_count: int,
    matched_rows: int,
    issues: list[ValidationIssue],
    geoserver: Mapping[str, Any] | None = None,
    column_entries: list[Mapping[str, Any]] | None = None,
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
        "- The GeoPackage keeps the complete structured attribute set, including category, feature, and raw survey columns; column descriptions and optional rename mappings are in the run metadata.",
        "",
        "## Reports And Explorer",
        "",
        f"- Full report PDF: `{config['sources'].get('report_pdf')}`",
        f"- Blog/short PDF: `{config['sources'].get('blog_pdf')}`",
        f"- GEE explorer: {config['sources'].get('gee_explorer_url')}",
        "",
    ]
    if column_entries:
        lines.extend(_column_reference_lines(column_entries))
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
    lines.extend(["## Audience", ""])
    lines.extend([f"- {item}" for item in readme.get("audience", [])])
    lines.extend(["", "## Cautions", ""])
    lines.extend([f"- {item}" for item in readme.get("cautions", [])])
    return lines


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
    required: list[str] = ["mapping_yaml_path", "links_path"]
    if outputs.metadata:
        required.append("run_metadata_path")
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        required.append("gpkg_path")
    if outputs.readme:
        required.append("readme_path")
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
    outputs = resolve_output_options(request, config)
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
    status_name, status_outputs = status_column_config(config)
    if status_name:
        village_keys = joined["village_key"] if "village_key" in joined.columns else pd.Series([None] * len(joined), index=joined.index)
        joined[status_name] = [
            STATUS_NO_VILLAGE_ID
            if pd.isna(village_id)
            else (STATUS_MATCHED if pd.notna(village_key) else STATUS_NO_DATA)
            for village_id, village_key in zip(joined["village_id"], village_keys)
        ]
    ordered_columns = _ordered_tabular_columns(joined, columns)
    villages_frame = admin_output_frame(joined.drop(columns=["geometry"], errors="ignore"), value_columns=ordered_columns)
    matched_rows = int(villages_frame["village_key"].notna().sum()) if "village_key" in villages_frame else 0
    gpkg_value_columns = list(ordered_columns)
    if status_name and {"gpkg", "geoserver"} & status_outputs:
        gpkg_value_columns = [status_name, *gpkg_value_columns]
    gpkg_frame = admin_output_frame(joined, value_columns=gpkg_value_columns, include_geometry=True)
    gpkg_frame = normalize_unicode_frame(gpkg_frame)
    describe = _column_describer(config, columns)
    timings["build_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    paths: dict[str, str] = {}
    t0 = time.perf_counter()
    bundle.remove_outputs(".csv", ".stac_fragment.json", ".geoserver_links.csv")
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        # The GPKG table name becomes the GeoServer feature-type name, so it
        # must be the scoped layer name rather than a generic table name.
        paths["gpkg_path"] = bundle.write_gpkg({result_name: gpkg_frame}).as_posix()
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
        "rows": int(len(villages_frame)),
        "matched_rows": matched_rows,
        "join_coverage": round(matched_rows / len(villages_frame), 6) if len(villages_frame) else 0,
        "validation_issues": [asdict(issue) for issue in validation_issues],
        "sidecar": sidecar_status,
        "admin_created_indexes": admin_selection.created_indexes,
        "state_name": request.scope.state_name,
        "district_name": request.scope.district_name,
        "tehsil": request.scope.tehsil_name,
        "output_dir": bundle.path.as_posix(),
        "sync_to_geoserver": request.publish.sync_to_geoserver,
        "links_path": bundle.output_path(".links.json").as_posix(),
        **paths,
    }

    geoserver = None
    if request.publish.sync_to_geoserver and outputs.geoserver:
        t0 = time.perf_counter()
        gpkg_path = result.get("gpkg_path")
        geoserver_workspace = (
            request.publish.geoserver_workspace
            or output_config.get("geoserver_workspace")
            or ANTYODAYA_GEOSERVER_WORKSPACE
        )
        if not gpkg_path:
            geoserver = {
                "ok": False,
                "status": "missing_gpkg",
                "workspace": geoserver_workspace,
                "layer_name": result_name,
                "error": "GeoPackage output is required for GeoServer publishing.",
            }
        else:
            try:
                geoserver_result = publish_gpkg_layer(
                    gpkg_path,
                    workspace=geoserver_workspace,
                    layer_name=result_name,
                    overwrite=request.publish.overwrite,
                )
                geoserver = asdict(geoserver_result)
                geoserver["ok"] = True
                geoserver["status"] = "published"
                if request.publish.register_layers:
                    result["layer_registration"] = register_layer(
                        dataset_name=output_config.get("dataset_name", "Antyodaya 2020"),
                        layer_name=result_name,
                        scope=request.scope,
                        workspace=geoserver_workspace,
                        geoserver_url=geoserver.get("wfs_url"),
                        algorithm=ALGORITHM,
                        algorithm_version=ALGORITHM_VERSION,
                        misc={
                            "source_csv": config["sources"]["csv"],
                            "gpkg_path": result.get("gpkg_path"),
                            "links_path": result.get("links_path"),
                            "output_dir": bundle.path.as_posix(),
                            "geoserver_layer_name": result_name,
                            "geoserver_url": geoserver.get("wfs_url"),
                            "rows": result.get("rows"),
                            "matched_rows": result.get("matched_rows"),
                            "join_coverage": result.get("join_coverage"),
                        },
                        overwrite=request.publish.overwrite,
                    )
                    result["layer_id"] = (result["layer_registration"] or {}).get("layer_id")
            except Exception as exc:
                geoserver = {
                    "ok": False,
                    "status": "publish_failed",
                    "workspace": geoserver_workspace,
                    "layer_name": result_name,
                    "error_type": exc.__class__.__name__,
                    "error": str(exc)[:500],
                }
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
    result["geoserver"] = geoserver
    if outputs.readme:
        result["readme_path"] = bundle.write_readme(
            _readme_lines(
                request=request,
                config=config,
                result_name=result_name,
                row_count=len(villages_frame),
                matched_rows=matched_rows,
                issues=validation_issues,
                geoserver=geoserver,
                column_entries=column_dictionary(
                    pd.DataFrame(gpkg_frame.drop(columns=["geometry"], errors="ignore")),
                    describe,
                ),
            )
        ).as_posix()
    bundle.write_links(
        {
            "local": {
                "gpkg_path": result.get("gpkg_path"),
                "layer_name": result_name,
                "mapping_yaml_path": result.get("mapping_yaml_path"),
                "readme_path": result.get("readme_path"),
            },
            "geoserver": geoserver,
        }
    )

    result["timings"] = timings
    result["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    if outputs.metadata:
        result["run_metadata_path"] = bundle.write_metadata(
            {
                "request": asdict(request),
                "effective_outputs": asdict(outputs),
                "result": result,
                "config_path": str(config_path),
                "outputs": {
                    "villages": frame_profile(
                        pd.DataFrame(gpkg_frame.drop(columns=["geometry"], errors="ignore")),
                        describe,
                    ),
                },
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
def generate_antyodaya_layer_task(self, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Generate Mission Antyodaya outputs for a standard request payload."""

    return run_antyodaya_request(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Mission Antyodaya pipeline.")
    parser.add_argument("--state")
    parser.add_argument("--district")
    parser.add_argument("--tehsil")
    parser.add_argument("--no-geoserver", action="store_true")
    args = parser.parse_args()
    if not (args.state and args.district and args.tehsil):
        parser.error("--state, --district, and --tehsil are required")
    result = run_antyodaya_pipeline(
        _cli_request(args.state, args.district, args.tehsil, not args.no_geoserver)
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
