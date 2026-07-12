#!/usr/bin/env python3
"""Run the local dataset pipelines across every scope in cs_admin_standard.gpkg.

This is step 1 of the pan-India asset build. Each enumerated scope is turned
into the same request body the tehsil/district APIs accept, normalized through
``api_request_payload``, and executed in-process with the same pipeline
functions the API tasks call. GeoServer, GeoLibre, and Layer DB registration
stay off; the per-scope GeoPackage bundles are the product.

Every attempt is recorded in an append-only JSONL run state keyed by
(pipeline, scope), so interrupted builds resume by skipping recorded
successes. Failed scopes are retried on the next invocation unless
``--skip-failed`` is passed.

Example (smoke):
    PROJ_LIB=/usr/share/proj DJANGO_SETTINGS_MODULE=nrm_app.settings \
    python -m utilities.scripts.build_pan_india_gee_assets.run_pipelines \
      --pipelines livestocks --states jharkhand --limit 2

Example (full build):
    PROJ_LIB=/usr/share/proj DJANGO_SETTINGS_MODULE=nrm_app.settings \
    nohup python -m utilities.scripts.build_pan_india_gee_assets.run_pipelines \
      --pipelines facilities antyodaya livestocks \
      > logs/pan_india_run_$(date -u +%Y%m%dT%H%M%SZ).log 2>&1 &
"""

from __future__ import annotations

import argparse
import importlib
import os
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Sequence

from utilities.scripts.build_pan_india_gee_assets.common import (
    DEFAULT_CONFIG,
    load_config,
    log,
    repo_path,
    scope_key,
    setup_logging,
    utc_now_text,
    RunState,
)


_BOOTSTRAPPED = False


def bootstrap_django() -> None:
    """Configure Django once so pipeline modules import like they do in the API."""

    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    import django

    django.setup()
    _BOOTSTRAPPED = True


def enumerate_scopes(config: dict[str, Any], level: str) -> tuple[list[dict[str, Any]], int]:
    """Return distinct admin scopes at a level and the village rows they exclude.

    Rows with a null state, district, or tehsil name cannot be addressed by a
    name-based scope, so they are counted and reported rather than silently
    dropped.
    """

    admin_cfg = config["admin"]
    gpkg = repo_path(admin_cfg["gpkg"])
    layer = admin_cfg["layer"]
    name_columns = {
        "tehsil": ("state_name", "district_name", "TEHSIL"),
        "district": ("state_name", "district_name"),
    }
    if level not in name_columns:
        raise ValueError(f"Unsupported scope level: {level}; use tehsil or district")
    columns = name_columns[level]
    not_null = " AND ".join(f'"{column}" IS NOT NULL AND TRIM("{column}") != \'\'' for column in columns)
    select = ", ".join(f'"{column}"' for column in columns)
    with sqlite3.connect(gpkg) as con:
        rows = con.execute(
            f'SELECT DISTINCT {select} FROM "{layer}" WHERE {not_null} ORDER BY {select}'
        ).fetchall()
        excluded_villages = int(
            con.execute(f'SELECT COUNT(*) FROM "{layer}" WHERE NOT ({not_null})').fetchone()[0]
        )
    scopes = []
    for row in rows:
        scope = {"level": level, "state_name": row[0], "district_name": row[1]}
        if level == "tehsil":
            scope["tehsil_name"] = row[2]
        scopes.append(scope)
    return scopes, excluded_villages


def resolve_entry(entry: str) -> Callable[..., dict[str, Any]]:
    module_name, _, function_name = entry.partition(":")
    module = importlib.import_module(module_name)
    return getattr(module, function_name)


def request_body(scope: dict[str, Any], defaults: dict[str, Any], *, force: bool) -> dict[str, Any]:
    body: dict[str, Any] = {
        "scope": dict(scope),
        "outputs": dict(defaults.get("outputs") or {}),
        "sync_to_geoserver": bool(defaults.get("sync_to_geoserver", False)),
        "register_layers": bool(defaults.get("register_layers", False)),
        "use_pregenerated": bool(defaults.get("use_pregenerated", True)) and not force,
        "overwrite": force,
    }
    return body


def execute_one(task: dict[str, Any]) -> dict[str, Any]:
    """Run one pipeline over one scope and return the run-state record."""

    bootstrap_django()
    from computing.misc.local_pipeline.schema import StandardRequest, api_request_payload

    started = time.perf_counter()
    record: dict[str, Any] = {
        "key": task["key"],
        "pipeline": task["pipeline"],
        "scope": task["scope"],
        "finished_at_utc": utc_now_text(),
    }
    try:
        payload = api_request_payload(task["body"], overwrite=bool(task["body"].get("overwrite")))
        run = resolve_entry(task["entry"])
        result = run(StandardRequest.from_mapping(payload))
        status = str(result.get("status") or "")
        if status not in {"success", "cached"}:
            raise RuntimeError(f"Pipeline returned unexpected status: {status or 'missing'}")
        paths = {
            key: value
            for key, value in result.items()
            if key.endswith("gpkg_path") and isinstance(value, str)
        }
        missing = [path for path in paths.values() if not Path(path).exists()]
        if not paths or missing:
            raise RuntimeError(f"Pipeline reported missing GeoPackage outputs: {missing or 'none recorded'}")
        record.update(
            {
                "status": status,
                "layer_name": result.get("layer_name"),
                "paths": paths,
                "rows": {
                    key: value
                    for key, value in result.items()
                    if key.endswith("_rows") and isinstance(value, int)
                },
            }
        )
    except Exception as exc:  # noqa: BLE001 - every scope failure must be recorded, not fatal
        record.update(
            {
                "status": "failed",
                "error_type": exc.__class__.__name__,
                "error": str(exc)[:500],
            }
        )
    record["elapsed_seconds"] = round(time.perf_counter() - started, 3)
    record["finished_at_utc"] = utc_now_text()
    return record


def build_tasks(
    config: dict[str, Any],
    args: argparse.Namespace,
    state: RunState,
) -> tuple[list[dict[str, Any]], dict[str, int], int]:
    run_cfg = config["run"]
    level = args.level or run_cfg.get("scope_level", "tehsil")
    scopes, excluded_villages = enumerate_scopes(config, level)

    def selected(scope: dict[str, Any]) -> bool:
        def norm(value: Any) -> str:
            return " ".join(str(value or "").strip().lower().split())

        if args.states and norm(scope["state_name"]) not in {norm(item) for item in args.states}:
            return False
        if args.districts and norm(scope["district_name"]) not in {norm(item) for item in args.districts}:
            return False
        if args.tehsils and norm(scope.get("tehsil_name")) not in {norm(item) for item in args.tehsils}:
            return False
        return True

    scopes = [scope for scope in scopes if selected(scope)]
    if args.limit:
        scopes = scopes[: args.limit]

    pipelines = config["pipelines"]
    requested = args.pipelines or list(pipelines)
    unknown = [name for name in requested if name not in pipelines]
    if unknown:
        raise KeyError(f"Unknown pipelines {unknown}; configured: {', '.join(pipelines)}")

    defaults = run_cfg.get("request_defaults") or {}
    tasks: list[dict[str, Any]] = []
    skipped = {"success": 0, "cached": 0, "failed": 0}
    for pipeline_name in requested:
        for scope in scopes:
            key = scope_key(pipeline_name, scope)
            previous = state.get(key)
            if previous and not args.force:
                status = previous.get("status")
                if status in {"success", "cached"} or (status == "failed" and args.skip_failed):
                    skipped[status] = skipped.get(status, 0) + 1
                    continue
            tasks.append(
                {
                    "key": key,
                    "pipeline": pipeline_name,
                    "entry": pipelines[pipeline_name]["entry"],
                    "scope": scope,
                    "body": request_body(scope, defaults, force=args.force),
                }
            )
    return tasks, skipped, excluded_villages


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--pipelines", nargs="+", default=None, help="Subset of configured pipelines.")
    parser.add_argument("--level", choices=("tehsil", "district"), default=None)
    parser.add_argument("--states", nargs="+", default=None, help="Only these state names.")
    parser.add_argument("--districts", nargs="+", default=None, help="Only these district names.")
    parser.add_argument("--tehsils", nargs="+", default=None, help="Only these tehsil names.")
    parser.add_argument("--limit", type=int, default=None, help="Cap the number of scopes per pipeline.")
    parser.add_argument("--jobs", type=int, default=1, help="Parallel worker processes.")
    parser.add_argument("--force", action="store_true", help="Rerun every scope and overwrite cached bundles.")
    parser.add_argument("--skip-failed", action="store_true", help="Do not retry scopes recorded as failed.")
    parser.add_argument("--dry-run", action="store_true", help="List pending work without running pipelines.")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    run_cfg = config["run"]
    setup_logging(args.debug, log_file=repo_path(run_cfg.get("log_file", "logs/pan_india_gee_assets.log")))
    state = RunState(repo_path(run_cfg["state_file"]))

    tasks, skipped, excluded_villages = build_tasks(config, args, state)
    log.info(
        "Pending runs: %s (skipped recorded: %s); admin rows outside name-addressable scopes: %s",
        len(tasks),
        skipped,
        excluded_villages,
    )
    if args.dry_run:
        for task in tasks[:50]:
            log.info("PENDING %s %s", task["pipeline"], task["scope"])
        if len(tasks) > 50:
            log.info("... and %s more", len(tasks) - 50)
        return

    if not tasks:
        log.info("Nothing to run; state counts: %s", state.counts())
        return

    bootstrap_django()
    started = time.monotonic()
    completed = 0
    failures = 0

    def record_result(record: dict[str, Any]) -> None:
        nonlocal completed, failures
        completed += 1
        if record["status"] == "failed":
            failures += 1
            log.warning(
                "[%s/%s] FAILED %s %s: %s: %s",
                completed,
                len(tasks),
                record["pipeline"],
                record["scope"],
                record.get("error_type"),
                record.get("error"),
            )
        else:
            log.info(
                "[%s/%s] %s %s %s in %.1fs",
                completed,
                len(tasks),
                record["status"].upper(),
                record["pipeline"],
                record.get("layer_name") or record["scope"],
                record["elapsed_seconds"],
            )
        state.append(record)

    if args.jobs <= 1:
        for task in tasks:
            record_result(execute_one(task))
    else:
        with ProcessPoolExecutor(max_workers=args.jobs, initializer=bootstrap_django) as executor:
            futures = {executor.submit(execute_one, task): task for task in tasks}
            for future in as_completed(futures):
                record_result(future.result())

    log.info(
        "Finished %s runs in %.1f minutes (%s failed); state counts: %s",
        completed,
        (time.monotonic() - started) / 60,
        failures,
        state.counts(),
    )
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
