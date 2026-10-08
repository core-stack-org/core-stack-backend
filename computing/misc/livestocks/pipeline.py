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

from computing.utils import save_layer_info_to_db, update_layer_sync_status
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
    resolved_scope_output_identity,
    slug,
    stable_hash,
    utc_now_text,
)
from utilities.pipelines.publish import publish_gpkg_layer
from utilities.pipelines.schema import (
    STATUS_MATCHED,
    STATUS_NO_DATA,
    STATUS_NO_VILLAGE_ID,
    OutputOptions,
    ValidationIssue,
    resolve_output_options,
    status_column_config,
    validate_numeric_range,
)
from utilities.pipelines.tabular import CSVSQLiteSidecar, csv_header
from utilities.pipelines.unicode import normalize_unicode_frame
from nrm_app.celery import app
from utilities.constants import (
    ADMIN_BOUNDARY_GPKG,
    LIVESTOCK_CENSUS_20_CSV,
    LIVESTOCK_GEOSERVER_WORKSPACE,
)
from utilities.pipelines import api_request_payload

CONFIG_PATH = Path(__file__).with_name("livestocks_pipeline.yaml")
ALGORITHM = "local-livestock-csv-admin-join"
ALGORITHM_VERSION = "2.1"
SOURCE_DEFAULTS = {
    "admin_gpkg": ADMIN_BOUNDARY_GPKG,
    "csv": LIVESTOCK_CENSUS_20_CSV,
}


def _repo_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    base_dir = Path(settings.BASE_DIR) if settings.configured else Path.cwd()
    return base_dir / path


def _apply_source_defaults(config: Mapping[str, Any]) -> dict[str, Any]:
    """Use constants for production resources and YAML only for overrides."""

    resolved = dict(config)
    sources = dict(resolved.get("sources") or {})
    for name, default in SOURCE_DEFAULTS.items():
        sources[name] = sources.get(name) or default
    sources["sidecar_sqlite"] = (
        sources.get("sidecar_sqlite") or f"{sources['csv']}.sqlite"
    )
    resolved["sources"] = sources
    return resolved


def _cli_request(
    state: str, district: str, tehsil: str, sync_to_geoserver: bool = True
) -> StandardRequest:
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


def _derive_livestock_metrics(
    frame: pd.DataFrame, schema: Mapping[str, Any]
) -> pd.DataFrame:
    derived = frame.copy()
    for _, animals in schema.get("livestock", {}).items():
        for fields in animals.values():
            male = fields.get("male")
            female = fields.get("female")
            total = fields.get("total")
            if male in derived.columns and female in derived.columns and total:
                derived[total] = pd.to_numeric(derived[male], errors="coerce").fillna(
                    0
                ) + pd.to_numeric(derived[female], errors="coerce").fillna(0)
    for metric, spec in schema.get("derived_metrics", {}).items():
        sources = spec.get("sources", [])
        if sources and all(source in derived.columns for source in sources):
            derived[metric] = sum(
                pd.to_numeric(derived[source], errors="coerce").fillna(0)
                for source in sources
            )
    return derived


def _validate_livestock(
    frame: pd.DataFrame, config: Mapping[str, Any]
) -> list[ValidationIssue]:
    return validate_numeric_range(
        frame,
        columns=[
            column
            for column in config["metrics"]["count_columns"]
            if column in frame.columns
        ],
        minimum=0,
        allow_null=True,
    )


def _merge_admin_livestock(
    admin_rows, source_rows: pd.DataFrame, config: Mapping[str, Any]
):
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


def _ordered_columns(
    frame: pd.DataFrame, config: Mapping[str, Any], columns: Mapping[str, list[str]]
) -> list[str]:
    ordered = []
    ordered.extend([col for col in columns["location"] if col not in ordered])
    ordered.extend([col for col in columns["metrics"] if col not in ordered])
    ordered.extend([col for col in columns["raw"] if col not in ordered])
    return [col for col in ordered if col in frame.columns]


def _focused_frame(
    frame: pd.DataFrame, value_columns: list[str], status_name: str | None
) -> pd.DataFrame:
    """Return the report CSV frame: admin columns, the status column, then the
    configured value columns for matched villages."""

    focused = admin_presentation_frame(
        frame.drop(columns=["geometry"], errors="ignore")
    )
    source = frame.set_index("fid", drop=False) if "fid" in frame.columns else frame
    output_rows: list[dict[str, Any]] = []
    for _, admin_row in focused.iterrows():
        row = admin_row.to_dict()
        admin_index = row.get("index")
        values = (
            source.loc[admin_index]
            if admin_index in source.index
            else pd.Series(dtype=object)
        )
        has_village_id = pd.notna(row.get("village_id"))
        has_livestock = (
            pd.notna(values.get("village_code")) if not values.empty else False
        )
        status = STATUS_MATCHED
        if not has_village_id:
            status = STATUS_NO_VILLAGE_ID
        elif not has_livestock:
            status = STATUS_NO_DATA
        else:
            for column in value_columns:
                row[column] = values.get(column)
        if status_name:
            row[status_name] = status
        output_rows.append(row)
    output = pd.DataFrame(output_rows)
    ordered = list(focused.columns)
    if status_name:
        ordered.append(status_name)
    ordered.extend(column for column in value_columns if column in output.columns)
    return output.reindex(columns=list(dict.fromkeys(ordered)))


def _column_describer(schema: Mapping[str, Any], config: Mapping[str, Any]):
    """Describe livestock output columns from the runtime schema YAML."""

    status_name, _ = status_column_config(config)
    descriptions = dict(ADMIN_COLUMN_DESCRIPTIONS)
    if status_name:
        descriptions[status_name] = (
            "Row data availability: `matched` when a 20th Livestock Census record was joined, "
            f"`{STATUS_NO_VILLAGE_ID}` when the admin row lacks a village identifier, and "
            f"`{STATUS_NO_DATA}` when no census record matched this village."
        )
    for group_key, animals in (schema.get("livestock") or {}).items():
        group_label = str(group_key).replace("_", " ")
        for animal, fields in animals.items():
            if fields.get("total"):
                descriptions[fields["total"]] = (
                    f"Total {animal} count (female + male) for the village, "
                    f"20th Livestock Census (2019), {group_label} group."
                )
            if fields.get("female"):
                descriptions[fields["female"]] = (
                    f"Female {animal} count for the village, 20th Livestock Census (2019)."
                )
            if fields.get("male"):
                descriptions[fields["male"]] = (
                    f"Male {animal} count for the village, 20th Livestock Census (2019)."
                )
    for metric, spec in (schema.get("derived_metrics") or {}).items():
        sources = ", ".join(spec.get("sources", []))
        label = spec.get("label", metric.replace("_", " ").title())
        descriptions[metric] = f"{label}: sum of {sources}."
    descriptions["village_code"] = (
        "Census village code used to join livestock census records."
    )
    return descriptions


def _overview(
    frame: pd.DataFrame, group_columns: list[str], config: Mapping[str, Any]
) -> pd.DataFrame:
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
        row["matched_livestock_rows"] = (
            int(group["village_code"].notna().sum()) if "village_code" in group else 0
        )
        for metric in metrics:
            if metric in group.columns:
                row[f"{metric}_sum"] = int(
                    pd.to_numeric(group[metric], errors="coerce").fillna(0).sum()
                )
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
        description = (
            str(entry.get("description") or "").replace("|", "\\|").replace("\n", " ")
        )
        lines.append(
            f"| `{entry['column']}` | {entry.get('datatype', '')} | {description} |"
        )
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
        "- The GeoPackage keeps animal totals with female and male counts in schema order; column descriptions and optional rename mappings are in the run metadata.",
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
    lines.extend(["## Cautions", ""])
    lines.extend([f"- {item}" for item in config.get("readme", {}).get("cautions", [])])
    return lines


def _cache_input_signatures(
    config: Mapping[str, Any], config_path: str | Path
) -> dict[str, dict[str, Any]]:
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


def _required_result_paths(
    outputs: OutputOptions, request: StandardRequest
) -> tuple[str, ...]:
    required: list[str] = ["links_path"]
    if outputs.metadata:
        required.append("run_metadata_path")
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        required.append("gpkg_path")
    if outputs.readme:
        required.append("readme_path")
    return tuple(dict.fromkeys(required))


def run_livestocks_pipeline(
    request: StandardRequest,
    *,
    config_path: str | Path = CONFIG_PATH,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    config = _apply_source_defaults(load_config(config_path))
    outputs = resolve_output_options(request, config)
    schema = _schema(config)
    columns = _source_columns(config)
    output_config = config["output"]

    t0 = time.perf_counter()
    admin_source = CSAdminSource(
        _repo_path(config["sources"]["admin_gpkg"]),
        table_name=config["sources"]["admin_layer"],
    )
    include_geometry = outputs.gpkg or request.publish.sync_to_geoserver
    (
        admin_selection,
        registration_scope,
        output_parts,
        layer_name,
    ) = resolved_scope_output_identity(
        admin_source,
        output_config["layer_prefix"],
        AdminScope.from_mapping(asdict(request.scope)),
        include_geometry=include_geometry,
    )
    timings["read_admin_seconds"] = round(time.perf_counter() - t0, 3)

    output_root = _repo_path(output_config["root"]).joinpath(*output_parts)
    bundle = OutputBundle(
        output_root,
        layer_name,
        directory_name=output_config["directory_name"],
    )
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
    sidecar = _sidecar(config)
    sidecar_status = sidecar.materialize()
    source_rows = sidecar.fetch_by_values(
        config["keys"]["source_join_key"], admin_selection.pc11_village_ids
    )
    timings["read_livestock_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    validation_issues = _validate_livestock(source_rows, config)
    joined = _merge_admin_livestock(admin_selection.rows, source_rows, config)
    joined = _derive_livestock_metrics(joined, schema)
    status_name, status_outputs = status_column_config(config)
    if status_name:
        village_codes = (
            joined["village_code"]
            if "village_code" in joined.columns
            else pd.Series([None] * len(joined), index=joined.index)
        )
        joined[status_name] = [
            (
                STATUS_NO_VILLAGE_ID
                if pd.isna(village_id)
                else (STATUS_MATCHED if pd.notna(village_code) else STATUS_NO_DATA)
            )
            for village_id, village_code in zip(joined["village_id"], village_codes)
        ]
    ordered = _ordered_columns(joined, config, columns)
    villages_frame = admin_output_frame(
        joined.drop(columns=["geometry"], errors="ignore"), value_columns=ordered
    )
    matched_rows = (
        int(villages_frame["village_code"].notna().sum())
        if "village_code" in villages_frame
        else 0
    )
    gpkg_value_columns = [
        column for column in schema.get("gpkg_columns", []) if column in joined.columns
    ] or ordered
    if status_name and {"gpkg", "geoserver"} & status_outputs:
        gpkg_value_columns = [status_name, *gpkg_value_columns]
    gpkg_frame = admin_output_frame(
        joined, value_columns=gpkg_value_columns, include_geometry=True
    )
    gpkg_frame = normalize_unicode_frame(gpkg_frame)
    describe = _column_describer(schema, config)
    timings["build_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    paths: dict[str, str] = {}
    t0 = time.perf_counter()
    bundle.remove_outputs(".csv", ".stac_fragment.json", ".geoserver_links.csv")
    if outputs.gpkg or (request.publish.sync_to_geoserver and outputs.geoserver):
        # The GPKG table name becomes the GeoServer feature-type name, so it
        # must be the scoped layer name rather than a generic table name.
        paths["gpkg_path"] = bundle.write_gpkg({layer_name: gpkg_frame}).as_posix()
    timings["write_local_outputs_seconds"] = round(time.perf_counter() - t0, 3)

    result: dict[str, Any] = {
        "status": "success",
        "algorithm": ALGORITHM,
        "algorithm_version": ALGORITHM_VERSION,
        "layer_name": layer_name,
        "rows": int(len(villages_frame)),
        "matched_rows": matched_rows,
        "join_coverage": (
            round(matched_rows / len(villages_frame), 6) if len(villages_frame) else 0
        ),
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
            or LIVESTOCK_GEOSERVER_WORKSPACE
        )
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
    if request.publish.register_layers and geoserver and geoserver.get("ok"):
        state = registration_scope.state_name
        district = registration_scope.district_name
        block = registration_scope.tehsil_name
        dataset_name = output_config.get("dataset_name", "Livestock Census")
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id="not applicable: local compute GeoServer layer",
            dataset_name=dataset_name,
            algorithm=ALGORITHM,
            algorithm_version=ALGORITHM_VERSION,
            misc={"is_generated_locally": True},
            is_override=request.publish.overwrite,
        )
        if layer_id is None:
            raise RuntimeError(
                f"Database registration failed for layer {layer_name!r}."
            )
        if update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True) is None:
            raise RuntimeError(
                f"GeoServer sync status update failed for layer ID {layer_id}."
            )
        result["layer_id"] = layer_id
        result["layer_registration"] = {
            "ok": True,
            "status": "registered",
            "dataset": dataset_name,
            "layer_id": layer_id,
        }
    if outputs.readme:
        result["readme_path"] = bundle.write_readme(
            _readme_lines(
                request=request,
                config=config,
                result_name=layer_name,
                row_count=len(villages_frame),
                matched_rows=matched_rows,
                issues=validation_issues,
                geoserver=geoserver,
                column_entries=column_dictionary(
                    pd.DataFrame(
                        gpkg_frame.drop(columns=["geometry"], errors="ignore")
                    ),
                    describe,
                ),
            )
        ).as_posix()
    bundle.write_links(
        {
            "local": {
                "gpkg_path": result.get("gpkg_path"),
                "layer_name": layer_name,
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
                        pd.DataFrame(
                            gpkg_frame.drop(columns=["geometry"], errors="ignore")
                        ),
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


def run_livestocks_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    return run_livestocks_pipeline(StandardRequest.from_mapping(payload))


@app.task(bind=True)
def generate_livestocks_layer_task(
    self,
    state: str | None = None,
    district: str | None = None,
    block: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate livestock census outputs for a standard request payload."""
    if payload is None:
        payload = api_request_payload(
            {"state": state, "district": district, "block": block},
            overwrite=True,
        )
    return run_livestocks_request(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local livestock pipeline.")
    parser.add_argument("--state")
    parser.add_argument("--district")
    parser.add_argument("--tehsil")
    parser.add_argument("--no-geoserver", action="store_true")
    args = parser.parse_args()
    if not (args.state and args.district and args.tehsil):
        parser.error("--state, --district, and --tehsil are required")
    result = run_livestocks_pipeline(
        _cli_request(args.state, args.district, args.tehsil, not args.no_geoserver)
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
