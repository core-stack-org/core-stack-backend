#!/usr/bin/env python
"""Interactive test console for the local dataset pipelines.

Run it from the repo root while Django and Celery run in other terminals
(API mode) or on its own (direct mode, no server needed):

    python computing/misc/local_pipeline/tests/interactive_cli.py

The console lets you pick a pipeline, pick or search an active location,
toggle outputs, run the request either in-process or through the live API,
and then inspect what was produced: CSV head, GPKG layers, metadata column
dictionary, EDA, README, and timings.
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

LOCATIONS_FILE = REPO_ROOT / "data" / "proposed_blocks_active_locations.json"
DEFAULT_API_BASE = os.environ.get("LOCAL_PIPELINE_API_BASE", "http://localhost:8000")
API_TOKEN_ENV = "LOCAL_PIPELINE_JWT"

PIPELINES = {
    "facilities": {
        "module": "computing.misc.facilities.pipeline",
        "runner": "run_facilities_request",
        "endpoint": "/api/v1/generate_facilities_proximity/",
    },
    "antyodaya": {
        "module": "computing.misc.antyodaya.pipeline",
        "runner": "run_antyodaya_request",
        "endpoint": "/api/v1/generate_antyodaya/",
    },
    "livestocks": {
        "module": "computing.misc.livestocks.pipeline",
        "runner": "run_livestocks_request",
        "endpoint": "/api/v1/generate_livestocks/",
    },
}


def _print(text: str = "") -> None:
    print(text, flush=True)


def _ask(prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or (default or "")


def _ask_bool(prompt: str, default: bool) -> bool:
    value = _ask(prompt, "y" if default else "n").lower()
    return value in {"y", "yes", "true", "1"}


def _choose(prompt: str, options: list[str], default_index: int = 0) -> str:
    for position, option in enumerate(options, start=1):
        marker = "*" if position - 1 == default_index else " "
        _print(f"  {position}){marker} {option}")
    while True:
        raw = _ask(prompt, str(default_index + 1))
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        _print("  Pick a number from the list.")


def load_locations() -> list[dict[str, str]]:
    data = json.loads(LOCATIONS_FILE.read_text())
    locations = []
    for state in data:
        for district in state.get("district", []):
            for block in district.get("blocks", []):
                locations.append(
                    {
                        "state": state.get("label"),
                        "district": district.get("label"),
                        "tehsil": block.get("label"),
                    }
                )
    return locations


def pick_location(locations: list[dict[str, str]]) -> dict[str, str]:
    _print("\nLocation: type to search active locations, 'r' for random, 'm' for manual entry.")
    while True:
        raw = _ask("Search").lower()
        if raw == "r":
            return random.choice(locations)
        if raw == "m":
            return {
                "state": _ask("State name"),
                "district": _ask("District name"),
                "tehsil": _ask("Tehsil/block name"),
            }
        matches = [
            location
            for location in locations
            if raw
            and any(raw in str(location[key]).lower() for key in ("state", "district", "tehsil"))
        ]
        if not matches:
            _print("  No matches; try again ('r' random, 'm' manual).")
            continue
        labels = [f"{m['state']} / {m['district']} / {m['tehsil']}" for m in matches[:15]]
        chosen = _choose("Pick location", labels)
        return matches[labels.index(chosen)]


def build_payload(location: dict[str, str]) -> dict[str, Any]:
    _print("\nScope level:")
    level = _choose("Level", ["tehsil", "district", "state", "village"], 0)
    scope: dict[str, Any] = {"level": level, "state_name": location["state"]}
    if level in {"district", "tehsil"}:
        scope["district_name"] = location["district"]
    if level == "tehsil":
        scope["tehsil_name"] = location["tehsil"]
    if level == "village":
        scope = {"level": "village", "village_ids": [v.strip() for v in _ask("Village ids (comma-separated)").split(",") if v.strip()]}

    _print("\nOutputs (defaults write the full bundle):")
    outputs: dict[str, Any] = {}
    if not _ask_bool("Write all default outputs?", True):
        for flag in ("gpkg", "csv", "readme", "metadata", "stac", "geoserver"):
            outputs[flag] = _ask_bool(f"  outputs.{flag}", True)

    sync = _ask_bool("Publish to GeoServer (testworkspace)?", False)
    publish: dict[str, Any] = {
        "sync_to_geoserver": sync,
        "overwrite": _ask_bool("Overwrite existing outputs/layers?", True),
        "use_pregenerated": _ask_bool("Allow cached result (use_pregenerated)?", False),
    }
    if sync:
        publish["geoserver_workspace"] = _ask("GeoServer workspace", "testworkspace")
    return {"scope": scope, "outputs": outputs, "publish": publish}


def run_direct(pipeline: str, payload: dict[str, Any]) -> dict[str, Any]:
    import django

    django.setup()
    import importlib

    spec = PIPELINES[pipeline]
    module = importlib.import_module(spec["module"])
    runner: Callable[[dict[str, Any]], dict[str, Any]] = getattr(module, spec["runner"])
    started = time.perf_counter()
    result = runner(payload)
    result["_wall_seconds"] = round(time.perf_counter() - started, 3)
    return result


def run_api(pipeline: str, payload: dict[str, Any]) -> dict[str, Any]:
    """POST to the live API and, since the task runs async on Celery, watch the
    expected output directory for a fresh run metadata file."""

    import requests

    token = os.environ.get(API_TOKEN_ENV) or _ask("JWT access token (or set $LOCAL_PIPELINE_JWT)")
    base = _ask("API base URL", DEFAULT_API_BASE)
    url = base.rstrip("/") + PIPELINES[pipeline]["endpoint"]
    response = requests.post(url, json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    _print(f"HTTP {response.status_code}: {json.dumps(response.json(), default=str)[:400]}")
    if response.status_code >= 300:
        return {"status": "api_error", "status_code": response.status_code}
    if not _ask_bool("Task queued. Watch output directory for completion?", True):
        return {"status": "queued"}
    watch_seconds = int(_ask("Max seconds to wait", "600"))
    deadline = time.time() + watch_seconds
    marker = time.time()
    _print("Waiting for a run_metadata.json newer than the request...")
    while time.time() < deadline:
        candidates = sorted(
            (REPO_ROOT / "data").rglob("*.run_metadata.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[:5]
        for path in candidates:
            if path.stat().st_mtime >= marker:
                data = json.loads(path.read_text())
                scope = ((data.get("request") or {}).get("scope")) or {}
                wanted = payload["scope"]
                if all(
                    str(scope.get(key) or "").lower() == str(value or "").lower()
                    for key, value in wanted.items()
                    if key.endswith("_name")
                ):
                    _print(f"Found completed run: {path}")
                    return dict(data.get("result") or {"status": "completed", "run_metadata_path": str(path)})
        time.sleep(5)
    return {"status": "timeout_waiting_for_output"}


def _show_result_summary(result: dict[str, Any]) -> None:
    keys = (
        "status", "cache_hit", "layer_name", "rows", "village_rows", "matched_rows",
        "join_coverage", "nearest_rows", "candidate_pool", "elapsed_seconds", "_wall_seconds", "output_dir",
    )
    _print("\n=== Result ===")
    for key in keys:
        if key in result and result[key] is not None:
            _print(f"  {key}: {result[key]}")
    geoserver = result.get("geoserver")
    if geoserver:
        _print(f"  geoserver: {geoserver.get('status')} {geoserver.get('wfs_url') or geoserver.get('error') or ''}")
    issues = result.get("validation_issues") or []
    _print(f"  validation_issues: {len(issues)}")
    for issue in issues[:5]:
        _print(f"    - {issue}")
    timings = result.get("timings") or {}
    if timings:
        _print("  timings: " + ", ".join(f"{k}={v}s" for k, v in timings.items()))


def _inspect_csv(path: str) -> None:
    import pandas as pd

    frame = pd.read_csv(path)
    _print(f"\n{path}\nrows={len(frame)} columns={len(frame.columns)}")
    _print("columns: " + ", ".join(map(str, frame.columns[:20])) + (" ..." if len(frame.columns) > 20 else ""))
    with pd.option_context("display.max_columns", 12, "display.width", 200):
        _print(str(frame.head(8)))
    numeric = frame.select_dtypes("number")
    if not numeric.empty:
        _print("\nNumeric summary (first 10 columns):")
        _print(str(numeric.iloc[:, :10].describe().round(3)))


def _inspect_gpkg(path: str) -> None:
    with sqlite3.connect(path) as connection:
        layers = [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type IN ('features','attributes')"
            )
        ]
        for layer in layers:
            count = connection.execute(f'SELECT COUNT(*) FROM "{layer}"').fetchone()[0]
            columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{layer}")')]
            _print(f"\nlayer {layer}: {count} rows, {len(columns)} columns")
            _print("columns: " + ", ".join(columns[:25]) + (" ..." if len(columns) > 25 else ""))


def _inspect_metadata(path: str) -> None:
    data = json.loads(Path(path).read_text())
    _print(f"\n{path}")
    _print("keys: " + ", ".join(data.keys()))
    for name, profile in (data.get("outputs") or {}).items():
        eda = profile.get("eda") or {}
        _print(f"\n[{name}] rows={eda.get('row_count')} cols={eda.get('column_count')}")
        for entry in (profile.get("columns") or [])[:12]:
            description = textwrap.shorten(str(entry.get("description") or ""), 90)
            _print(f"  {entry['column']:45s} {entry.get('datatype', ''):8s} {description}")
        if len(profile.get("columns") or []) > 12:
            _print(f"  ... {len(profile['columns']) - 12} more columns")


def inspect_result(result: dict[str, Any]) -> None:
    while True:
        artifacts = [
            (key, value)
            for key, value in sorted(result.items())
            if key.endswith("_path") and value and Path(str(value)).exists()
        ]
        if not artifacts:
            _print("No artifact paths in result.")
            return
        _print("\nInspect which artifact?")
        labels = [f"{key} -> {value}" for key, value in artifacts] + ["back"]
        chosen = _choose("Artifact", labels, len(labels) - 1)
        if chosen == "back":
            return
        key, path = artifacts[labels.index(chosen)]
        try:
            if str(path).endswith(".csv"):
                _inspect_csv(str(path))
            elif str(path).endswith(".gpkg"):
                _inspect_gpkg(str(path))
            elif str(path).endswith("run_metadata.json"):
                _inspect_metadata(str(path))
            elif str(path).endswith((".json", ".yaml", ".yml", ".md")):
                _print(Path(str(path)).read_text()[:4000])
            else:
                _print(f"(no inspector for {path})")
        except Exception as exc:  # keep the console alive on bad files
            _print(f"inspect failed: {exc.__class__.__name__}: {exc}")


def main() -> None:
    _print("Local pipeline interactive test console. Ctrl+C to exit.")
    locations = load_locations()
    _print(f"Loaded {len(locations)} active block locations from {LOCATIONS_FILE.name}.")
    last_result: dict[str, Any] | None = None
    while True:
        _print("\n=== Menu ===")
        action = _choose(
            "Action",
            [
                "run a pipeline request (direct, in-process)",
                "run a pipeline request (through live API + celery)",
                "inspect last result artifacts",
                "quit",
            ],
            0,
        )
        if action == "quit":
            return
        if action == "inspect last result artifacts":
            if last_result is None:
                _print("Nothing run yet.")
                continue
            inspect_result(last_result)
            continue
        pipeline = _choose("Pipeline", list(PIPELINES), 0)
        location = pick_location(locations)
        _print(f"Selected: {location['state']} / {location['district']} / {location['tehsil']}")
        payload = build_payload(location)
        _print("\nRequest payload:\n" + json.dumps(payload, indent=2))
        if not _ask_bool("Run it?", True):
            continue
        try:
            if action.startswith("run a pipeline request (direct"):
                last_result = run_direct(pipeline, payload)
            else:
                last_result = run_api(pipeline, payload)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            _print(f"RUN FAILED: {exc.__class__.__name__}: {exc}")
            continue
        _show_result_summary(last_result)
        if _ask_bool("Inspect artifacts now?", True):
            inspect_result(last_result)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nbye")
