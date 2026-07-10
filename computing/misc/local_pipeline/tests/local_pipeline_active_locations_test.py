#!/usr/bin/env python
"""Run local geospatial pipeline smoke/scale tests across active locations."""

from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import random
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from unittest.mock import patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")


PIPELINE_MODULES = {
    "facilities": ("computing.misc.facilities.pipeline", "run_facilities_request"),
    "antyodaya": ("computing.misc.antyodaya.pipeline", "run_antyodaya_request"),
    "livestocks": ("computing.misc.livestocks.pipeline", "run_livestocks_request"),
}
DEFAULT_TEST_GEOSERVER_WORKSPACE = "testworkspace"

# Named output-flag overrides exercised by the test matrix. Empty means the
# pipeline default bundle (gpkg, csv, readme, metadata, stac, geoserver).
OUTPUT_VARIANTS: dict[str, dict[str, Any]] = {
    "default": {},
    "data_only": {"readme": False, "metadata": False, "stac": False},
    "metadata_only": {"gpkg": False, "csv": False, "readme": False, "stac": False, "geoserver": False},
}

API_ENDPOINTS = {
    "facilities": ("generate_facilities_proximity", "generate_facilities_proximity_task"),
    "antyodaya": ("generate_antyodaya", "generate_antyodaya_layer_task"),
    "livestocks": ("generate_livestocks", "generate_livestocks_layer_task"),
}


@dataclass(frozen=True)
class Location:
    state_name: str
    state_id: str | None
    district_name: str
    district_id: str | None
    tehsil_name: str
    tehsil_id: str | None


@dataclass(frozen=True)
class Case:
    pipeline: str
    scope_level: str
    variant: str
    use_pregenerated: bool
    location: Location
    village_ids: tuple[Any, ...] = ()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_locations(path: Path) -> list[Location]:
    data = json.loads(path.read_text())
    locations: list[Location] = []
    for state in data:
        for district in state.get("district", []):
            for block in district.get("blocks", []):
                locations.append(
                    Location(
                        state_name=state.get("label"),
                        state_id=state.get("state_id"),
                        district_name=district.get("label"),
                        district_id=district.get("district_id"),
                        tehsil_name=block.get("label"),
                        tehsil_id=block.get("tehsil_id") or block.get("block_id"),
                    )
                )
    return locations


def resolve_pipelines(names: list[str]) -> dict[str, Callable[[dict[str, Any]], dict[str, Any]]]:
    try:
        import django

        django.setup()
    except Exception as exc:
        print(f"WARN django.setup failed: {exc.__class__.__name__}: {exc}")
    selected = list(PIPELINE_MODULES) if names == ["all"] else names
    resolved: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}
    for name in selected:
        module_name, function_name = PIPELINE_MODULES[name]
        try:
            module = importlib.import_module(module_name)
            resolved[name] = getattr(module, function_name)
        except Exception as exc:
            print(f"SKIP pipeline={name}: {exc.__class__.__name__}: {exc}")
    return resolved


def payload_for(
    case: Case,
    *,
    sync_geoserver: bool,
    overwrite: bool,
    geoserver_workspace: str | None = None,
) -> dict[str, Any]:
    scope: dict[str, Any] = {"level": case.scope_level}
    if case.scope_level in {"state", "district", "tehsil", "block"}:
        scope["state_name"] = case.location.state_name
    if case.scope_level in {"district", "tehsil", "block"}:
        scope["district_name"] = case.location.district_name
    if case.scope_level in {"tehsil", "block"}:
        scope["tehsil_name"] = case.location.tehsil_name
    if case.scope_level == "village":
        scope["village_ids"] = list(case.village_ids)
    publish = {
        "sync_to_geoserver": sync_geoserver,
        "overwrite": overwrite,
        "use_pregenerated": case.use_pregenerated,
    }
    if sync_geoserver:
        publish["geoserver_workspace"] = geoserver_workspace or DEFAULT_TEST_GEOSERVER_WORKSPACE
    return {
        "scope": scope,
        "outputs": dict(OUTPUT_VARIANTS[case.variant]),
        "publish": publish,
        "test_context": {
            "state_id": case.location.state_id,
            "district_id": case.location.district_id,
            "tehsil_id": case.location.tehsil_id,
        },
    }


def select_locations(locations: list[Location], count: int, seed: int) -> list[Location]:
    rng = random.Random(seed)
    items = list(locations)
    rng.shuffle(items)
    return items[: min(count, len(items))]


def filter_only_locations(locations: list[Location], targets: list[str]) -> list[Location]:
    if not targets:
        return locations
    wanted: set[tuple[str, str, str]] = set()
    for target in targets:
        parts = [part.strip().lower() for part in target.split("|")]
        if len(parts) != 3 or not all(parts):
            raise ValueError("--only-location must use 'state|district|tehsil'")
        wanted.add(tuple(parts))
    return [
        location
        for location in locations
        if (
            str(location.state_name).strip().lower(),
            str(location.district_name).strip().lower(),
            str(location.tehsil_name).strip().lower(),
        )
        in wanted
    ]


def unique_by(items: list[Location], key: Callable[[Location], tuple[Any, ...]], limit: int) -> list[Location]:
    if limit <= 0:
        return []
    seen: set[tuple[Any, ...]] = set()
    selected: list[Location] = []
    for item in items:
        marker = key(item)
        if marker in seen:
            continue
        seen.add(marker)
        selected.append(item)
        if len(selected) >= limit:
            break
    return selected


def sample_village_ids(locations: list[Location], limit: int, ids_per_tehsil: int) -> dict[Location, tuple[Any, ...]]:
    if limit <= 0 or ids_per_tehsil <= 0:
        return {}
    try:
        import django

        django.setup()
        from computing.misc.local_pipeline import AdminScope, CSAdminSource
    except Exception as exc:
        print(f"SKIP village scope sampling: {exc.__class__.__name__}: {exc}")
        return {}

    sampled: dict[Location, tuple[Any, ...]] = {}
    admin = CSAdminSource()
    for location in locations:
        if len(sampled) >= limit:
            break
        try:
            selection = admin.read_scope(
                AdminScope(
                    level="tehsil",
                    state_name=location.state_name,
                    district_name=location.district_name,
                    tehsil_name=location.tehsil_name,
                ),
                include_geometry=False,
            )
            ids = tuple(selection.village_ids[:ids_per_tehsil])
            if ids:
                sampled[location] = ids
        except Exception as exc:
            print(
                "SKIP village ids "
                f"{location.state_name}/{location.district_name}/{location.tehsil_name}: "
                f"{exc.__class__.__name__}: {exc}"
            )
    return sampled


def filter_admin_resolvable_locations(locations: list[Location]) -> tuple[list[Location], list[dict[str, Any]]]:
    try:
        import django

        django.setup()
        from computing.misc.local_pipeline import AdminScope, CSAdminSource
    except Exception as exc:
        return [], [{"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)}]

    resolved: list[Location] = []
    records: list[dict[str, Any]] = []
    admin = CSAdminSource()
    for location in locations:
        record = {
            "state_name": location.state_name,
            "district_name": location.district_name,
            "tehsil_name": location.tehsil_name,
            "state_id": location.state_id,
            "district_id": location.district_id,
            "tehsil_id": location.tehsil_id,
        }
        try:
            selection = admin.read_scope(
                AdminScope(
                    level="tehsil",
                    state_name=location.state_name,
                    district_name=location.district_name,
                    tehsil_name=location.tehsil_name,
                ),
                include_geometry=False,
            )
            record.update({"ok": True, "admin_rows": int(len(selection.rows))})
            resolved.append(location)
        except Exception as exc:
            record.update({"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)})
        records.append(record)
    return resolved, records


def build_cases(
    *,
    locations: list[Location],
    pipelines: list[str],
    max_tehsils: int,
    all_sample: int,
    metadata_sample: int,
    district_sample: int,
    state_sample: int,
    village_sample: int,
    village_ids_per_tehsil: int,
    seed: int,
) -> list[Case]:
    selected_tehsils = select_locations(locations, max_tehsils, seed)
    district_locations = unique_by(
        selected_tehsils,
        lambda loc: (loc.state_name, loc.district_name),
        district_sample,
    )
    state_locations = unique_by(selected_tehsils, lambda loc: (loc.state_name,), state_sample)
    village_id_map = sample_village_ids(selected_tehsils, village_sample, village_ids_per_tehsil)

    cases: list[Case] = []
    for pipeline in pipelines:
        for location in selected_tehsils:
            cases.append(Case(pipeline, "tehsil", "default", False, location))
            cases.append(Case(pipeline, "tehsil", "default", True, location))
        for location in selected_tehsils[:all_sample]:
            cases.append(Case(pipeline, "tehsil", "data_only", False, location))
        for location in selected_tehsils[:metadata_sample]:
            cases.append(Case(pipeline, "tehsil", "metadata_only", False, location))
        for location in district_locations:
            cases.append(Case(pipeline, "district", "default", False, location))
        for location in state_locations:
            cases.append(Case(pipeline, "state", "default", False, location))
        for location, village_ids in village_id_map.items():
            cases.append(Case(pipeline, "village", "default", False, location, village_ids))
    return cases


def gpkg_layers(path: str | None) -> dict[str, int]:
    if not path or not Path(path).exists():
        return {}
    layers: dict[str, int] = {}
    with sqlite3.connect(path) as connection:
        names = [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type IN ('features', 'attributes')"
            ).fetchall()
        ]
        for name in names:
            try:
                layers[name] = int(connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])
            except sqlite3.DatabaseError:
                layers[name] = -1
    return layers


ADMIN_PREFIX = [
    "index",
    "state_id",
    "district_id",
    "tehsil_id",
    "village_id",
    "state_name",
    "district_name",
    "tehsil_name",
    "village_name",
]
STATUS_COLUMNS = {"facilities_status", "antyodaya_status", "livestock_status"}
MACHINE_COLUMN_MARKERS = ("l2_", "l3_")
LIVESTOCK_CSV_CONTRACT = [
    *ADMIN_PREFIX,
    "livestock_status",
    "all_livestock_total",
    "large_animals_total",
    "cattle_total",
    "buffalo_total",
    "small_animals_total",
    "sheep_total",
    "goat_total",
    "pig_total",
]


def csv_check(path: str | None, pipeline: str | None = None) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {"exists": False}
    frame = pd.read_csv(path, nrows=50)
    columns = [str(column) for column in frame.columns]
    info: dict[str, Any] = {
        "exists": True,
        "column_count": int(len(columns)),
        "has_cs_feature_id": "cs_feature_id" in columns,
        "columns": columns,
    }
    # Report CSV contract: admin columns first, then the status column, and
    # never the machine (l2_*/l3_*) or feature-level columns.
    info["admin_prefix_ok"] = columns[: len(ADMIN_PREFIX)] == ADMIN_PREFIX
    info["status_after_admin"] = len(columns) > len(ADMIN_PREFIX) and columns[len(ADMIN_PREFIX)] in STATUS_COLUMNS
    info["machine_columns_in_csv"] = [
        column for column in columns if column.startswith(MACHINE_COLUMN_MARKERS)
    ]
    info["feature_columns_in_csv"] = [column for column in columns if column.endswith("_feat_value")]
    if pipeline == "livestocks":
        info["contract_ok"] = columns == LIVESTOCK_CSV_CONTRACT
    elif pipeline == "antyodaya":
        clusters = [position for position, column in enumerate(columns) if column.endswith("_cat_cluster")]
        values = [position for position, column in enumerate(columns) if column.endswith("_cat_value")]
        info["contract_ok"] = bool(clusters) and bool(values) and max(clusters) < min(values)
    elif pipeline == "facilities":
        info["contract_ok"] = (
            "essential_education_cat_distance_km" in columns
            and "market_cat_distance_km" in columns
            and not info["machine_columns_in_csv"]
        )
    return info


def metadata_check(path: str | None) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {"exists": False}
    data = json.loads(Path(path).read_text())
    profiles = data.get("outputs") or {}
    checks: dict[str, Any] = {"exists": True, "profiles": sorted(profiles)}
    csv_profile = profiles.get("csv") or {}
    columns = csv_profile.get("columns") or []
    checks["csv_columns_documented"] = len(columns)
    checks["csv_columns_missing_description"] = [
        entry["column"] for entry in columns if not entry.get("description")
    ]
    checks["csv_has_eda"] = bool((csv_profile.get("eda") or {}).get("row_count") is not None)
    return checks


def validate_result(result: dict[str, Any], pipeline: str | None = None) -> dict[str, Any]:
    csv_info = csv_check(result.get("csv_path") or result.get("village_service_csv_path"), pipeline)
    layers = gpkg_layers(result.get("gpkg_path"))
    missing_paths = []
    for key, value in result.items():
        if key.endswith("_path") and value and not Path(str(value)).exists():
            missing_paths.append(key)
    return {
        "csv_exists": csv_info["exists"],
        "csv_has_cs_feature_id": csv_info.get("has_cs_feature_id"),
        "csv_admin_prefix_ok": csv_info.get("admin_prefix_ok"),
        "csv_status_after_admin": csv_info.get("status_after_admin"),
        "csv_machine_columns": csv_info.get("machine_columns_in_csv"),
        "csv_feature_columns": csv_info.get("feature_columns_in_csv"),
        "csv_contract_ok": csv_info.get("contract_ok"),
        "metadata": metadata_check(result.get("run_metadata_path")),
        "gpkg_layers": layers,
        "missing_result_paths": missing_paths,
    }


def run_case(
    case: Case,
    runner: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    sync_geoserver: bool,
    overwrite: bool,
    geoserver_workspace: str | None = None,
) -> dict[str, Any]:
    payload = payload_for(
        case,
        sync_geoserver=sync_geoserver,
        overwrite=overwrite,
        geoserver_workspace=geoserver_workspace,
    )
    started = time.perf_counter()
    record: dict[str, Any] = {
        "pipeline": case.pipeline,
        "scope_level": case.scope_level,
        "variant": case.variant,
        "use_pregenerated": case.use_pregenerated,
        "state_name": case.location.state_name,
        "district_name": case.location.district_name,
        "tehsil_name": case.location.tehsil_name,
        "village_ids": ",".join(map(str, case.village_ids)),
        "payload": payload,
    }
    try:
        result = runner(payload)
        record.update(
            {
                "ok": result.get("status") in {"success", "cached"},
                "status": result.get("status"),
                "cache_hit": bool(result.get("cache_hit")),
                "elapsed_seconds_reported": result.get("elapsed_seconds"),
                "rows": result.get("rows") or result.get("village_rows"),
                "matched_rows": result.get("matched_rows"),
                "output_dir": result.get("output_dir"),
                "result": result,
                "checks": validate_result(result, case.pipeline),
            }
        )
    except Exception as exc:
        record.update(
            {
                "ok": False,
                "status": "error",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            }
        )
    record["elapsed_seconds_wall"] = round(time.perf_counter() - started, 3)
    return record


def run_api_normalization_smoke(
    *,
    pipeline_names: list[str],
    locations: list[Location],
    output_dir: Path,
) -> list[dict[str, Any]]:
    try:
        import django

        django.setup()
        from rest_framework.test import APIRequestFactory, force_authenticate
        import computing.api as api_module
    except Exception as exc:
        return [{"ok": False, "status": "skip", "error": f"{exc.__class__.__name__}: {exc}"}]

    factory = APIRequestFactory()
    records: list[dict[str, Any]] = []
    location = locations[0]
    dummy_user = type("LocalPipelineTestUser", (), {"is_authenticated": True, "is_active": True})()
    for pipeline in pipeline_names:
        if pipeline not in API_ENDPOINTS:
            continue
        view_name, task_name = API_ENDPOINTS[pipeline]
        if not hasattr(api_module, view_name) or not hasattr(api_module, task_name):
            records.append({"pipeline": pipeline, "ok": False, "status": "skip", "error": "API view or task not available"})
            continue
        captured: list[dict[str, Any]] = []

        def fake_apply_async(*args, **kwargs):
            captured.append({"args": args, "kwargs": kwargs})
            return type("FakeAsyncResult", (), {"id": "local-test"})()

        # Both request shapes must queue a task: the simple state/district/block
        # body (normalized to a tehsil scope) and the structured scope body.
        # Only a body naming no geography is rejected, with 400.
        simple_body = {
            "state": location.state_name,
            "district": location.district_name,
            "block": location.tehsil_name,
            "sync_to_geoserver": False,
            "overwrite": True,
        }
        structured_body = {
            "scope": {
                "level": "tehsil",
                "state_name": location.state_name,
                "district_name": location.district_name,
                "tehsil_name": location.tehsil_name,
            },
            "outputs": {"metadata": False, "stac": False},
            "publish": {"sync_to_geoserver": False, "use_pregenerated": True},
        }
        empty_body = {"sync_to_geoserver": False}
        view = getattr(api_module, view_name)
        task = getattr(api_module, task_name)
        with patch.object(task, "apply_async", side_effect=fake_apply_async):
            for label, body, expect_queued in (
                ("simple", simple_body, True),
                ("structured", structured_body, True),
                ("no_geography_rejected", empty_body, False),
            ):
                queued_before = len(captured)
                request = factory.post(f"/api/v1/{view_name}/", body, format="json")
                force_authenticate(request, user=dummy_user)
                response = view(request)
                queued = len(captured) > queued_before
                scope_ok = True
                if expect_queued:
                    ok = 200 <= response.status_code < 300 and queued
                    if queued:
                        # The task must always receive a resolved tehsil scope.
                        # captured entries hold apply_async's own kwargs, so the
                        # task payload sits under kwargs["kwargs"]["payload"].
                        task_kwargs = captured[-1]["kwargs"].get("kwargs") or {}
                        scope = (task_kwargs.get("payload") or {}).get("scope") or {}
                        scope_ok = (
                            scope.get("level") == "tehsil"
                            and scope.get("state_name") == location.state_name
                            and scope.get("district_name") == location.district_name
                            and scope.get("tehsil_name") == location.tehsil_name
                        )
                        ok = ok and scope_ok
                else:
                    ok = response.status_code == 400 and not queued
                records.append(
                    {
                        "pipeline": pipeline,
                        "body_type": label,
                        "ok": ok,
                        "normalized_scope_ok": scope_ok,
                        "status_code": response.status_code,
                        "response": getattr(response, "data", None),
                        "captured_apply_async": captured[-1] if captured else None,
                    }
                )
    path = output_dir / "api_normalization_smoke.json"
    path.write_text(json.dumps(records, indent=2, default=str) + "\n")
    return records


def write_records(records: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "pipeline_cases.jsonl"
    with jsonl_path.open("w") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str) + "\n")

    summary_path = output_dir / "pipeline_cases_summary.csv"
    columns = [
        "pipeline",
        "scope_level",
        "variant",
        "use_pregenerated",
        "state_name",
        "district_name",
        "tehsil_name",
        "ok",
        "status",
        "cache_hit",
        "elapsed_seconds_wall",
        "elapsed_seconds_reported",
        "rows",
        "matched_rows",
        "error_type",
        "error",
        "output_dir",
    ]
    with summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local pipeline tests across active locations.")
    parser.add_argument("--locations", default="data/proposed_blocks_active_locations.json")
    parser.add_argument("--pipelines", nargs="+", default=["all"], choices=["all", *PIPELINE_MODULES])
    parser.add_argument("--max-tehsils", type=int, default=100)
    parser.add_argument("--all-sample", type=int, default=5)
    parser.add_argument("--metadata-sample", type=int, default=10)
    parser.add_argument("--district-sample", type=int, default=10)
    parser.add_argument("--state-sample", type=int, default=3)
    parser.add_argument("--village-sample", type=int, default=20)
    parser.add_argument("--village-ids-per-tehsil", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sync-geoserver", action="store_true")
    parser.add_argument(
        "--test-geoserver-workspace",
        default=DEFAULT_TEST_GEOSERVER_WORKSPACE,
        help="GeoServer workspace used when --sync-geoserver is enabled.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--api-smoke", action="store_true")
    parser.add_argument("--admin-resolvable-only", action="store_true")
    parser.add_argument(
        "--only-location",
        action="append",
        default=[],
        help="Restrict tests to one active location, formatted as 'state|district|tehsil'. Repeatable.",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    pipeline_names = list(PIPELINE_MODULES) if args.pipelines == ["all"] else args.pipelines
    runners = resolve_pipelines(pipeline_names)
    if not runners:
        raise SystemExit("No requested pipeline modules are available on this branch.")

    output_dir = Path(args.output_dir or REPO_ROOT / "data" / "local_pipeline_test_runs" / utc_stamp())
    output_dir.mkdir(parents=True, exist_ok=True)
    locations = load_locations(REPO_ROOT / args.locations)
    locations = filter_only_locations(locations, args.only_location)
    if not locations:
        raise SystemExit("No active locations matched --only-location filters.")
    resolution_records: list[dict[str, Any]] = []
    if args.admin_resolvable_only:
        locations, resolution_records = filter_admin_resolvable_locations(locations)
        resolution_path = output_dir / "admin_resolution_report.csv"
        if resolution_records:
            with resolution_path.open("w", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=sorted({key for record in resolution_records for key in record}),
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(resolution_records)
        if not locations:
            raise SystemExit("No active locations resolved against cs_admin_standard.gpkg.")
    selected_pipeline_names = list(runners)
    cases = build_cases(
        locations=locations,
        pipelines=selected_pipeline_names,
        max_tehsils=args.max_tehsils,
        all_sample=args.all_sample,
        metadata_sample=args.metadata_sample,
        district_sample=args.district_sample,
        state_sample=args.state_sample,
        village_sample=args.village_sample,
        village_ids_per_tehsil=args.village_ids_per_tehsil,
        seed=args.seed,
    )

    records: list[dict[str, Any]] = []
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "locations_file": args.locations,
        "available_locations": len(locations),
        "pipelines": selected_pipeline_names,
        "case_count": len(cases),
        "args": vars(args),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")

    if args.api_smoke:
        run_api_normalization_smoke(
            pipeline_names=selected_pipeline_names,
            locations=select_locations(locations, 1, args.seed),
            output_dir=output_dir,
        )

    print(f"Running {len(cases)} cases. Output: {output_dir}", flush=True)
    for index, case in enumerate(cases, start=1):
        print(
            f"[{index}/{len(cases)}] {case.pipeline} {case.scope_level} "
            f"{case.variant} cache={case.use_pregenerated} "
            f"{case.location.state_name}/{case.location.district_name}/{case.location.tehsil_name}",
            flush=True,
        )
        record = run_case(
            case,
            runners[case.pipeline],
            sync_geoserver=args.sync_geoserver,
            overwrite=args.overwrite,
            geoserver_workspace=args.test_geoserver_workspace,
        )
        records.append(record)
        write_records(records, output_dir)
        if args.fail_fast and not record["ok"]:
            break

    ok_count = sum(1 for record in records if record.get("ok"))
    summary = {
        "records": len(records),
        "ok": ok_count,
        "failed": len(records) - ok_count,
        "output_dir": output_dir.as_posix(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(json.dumps(summary, indent=2))
    return 0 if ok_count == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
