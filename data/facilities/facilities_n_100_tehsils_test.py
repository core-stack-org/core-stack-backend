"""Run the facilities proximity pipeline for up to 100 source tehsils.

Usage:
  python manage.py shell < data/facilities/facilities_n_100_tehsils_test.py

Optional environment variables:
  FACILITIES_TEST_LIMIT=100
  FACILITIES_TEST_SEED=20260703
  FACILITIES_TEST_FORCE_CACHE=0
  FACILITIES_TEST_OUTPUTS=all
  FACILITIES_TEST_SYNC_GEOSERVER=0
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import statistics
import time
from pathlib import Path


def _ensure_django() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    import django
    from django.apps import apps

    if not apps.ready:
        django.setup()


def _bool_env(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "y"}


def _source_tehsils(source_path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(source_path) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT state_name, district_name, TEHSIL
            FROM village_shapes
            WHERE state_name IS NOT NULL
              AND district_name IS NOT NULL
              AND TEHSIL IS NOT NULL
            """
        ).fetchall()
    return [(str(state), str(district), str(tehsil)) for state, district, tehsil in rows]


def main() -> None:
    _ensure_django()

    from computing.misc.facilities_proximity import generate_facilities_proximity
    from utilities.constants import FACILITIES_PROXIMITY_GPKG
    from django.conf import settings

    limit = int(os.environ.get("FACILITIES_TEST_LIMIT", "100"))
    seed = int(os.environ.get("FACILITIES_TEST_SEED", "20260703"))
    outputs = os.environ.get("FACILITIES_TEST_OUTPUTS", "all")
    force_cache = _bool_env("FACILITIES_TEST_FORCE_CACHE")
    sync_to_geoserver = _bool_env("FACILITIES_TEST_SYNC_GEOSERVER")

    source_path = Path(FACILITIES_PROXIMITY_GPKG)
    if not source_path.is_absolute():
        source_path = Path(settings.BASE_DIR) / source_path

    tehsils = _source_tehsils(source_path)
    random.Random(seed).shuffle(tehsils)
    selected = tehsils[: max(1, min(limit, len(tehsils)))]

    rows: list[dict] = []
    failures: list[dict] = []
    started = time.perf_counter()
    for index, (state, district, tehsil) in enumerate(selected, 1):
        try:
            result = generate_facilities_proximity(
                state=state,
                district=district,
                block=tehsil,
                sync_to_geoserver=sync_to_geoserver,
                outputs=outputs,
                zip_output=False,
                force_cache=force_cache,
            )
        except Exception as exc:
            failure = {
                "index": index,
                "state": state,
                "district": district,
                "tehsil": tehsil,
                "error_type": exc.__class__.__name__,
                "error": str(exc)[:300],
            }
            failures.append(failure)
            print("FAIL", json.dumps(failure), flush=True)
            continue

        row = {
            "index": index,
            "state": state,
            "district": district,
            "tehsil": tehsil,
            "cache_hit": result["cache_hit"],
            "elapsed_seconds": result["elapsed_seconds"],
            "row_counts": result["row_counts"],
            "gpkg_path": result["gpkg_path"],
            "timings": result["timings"],
        }
        rows.append(row)
        if index <= 3 or index % 10 == 0:
            print("OK", json.dumps(row), flush=True)

    times = [row["elapsed_seconds"] for row in rows]
    summary = {
        "source": source_path.as_posix(),
        "limit": limit,
        "seed": seed,
        "outputs": outputs,
        "force_cache": force_cache,
        "sync_to_geoserver": sync_to_geoserver,
        "successes": len(rows),
        "failures": len(failures),
        "wall_seconds": round(time.perf_counter() - started, 3),
        "subsecond_count": sum(value < 1 for value in times),
        "max_seconds": max(times) if times else None,
        "mean_seconds": statistics.mean(times) if times else None,
        "median_seconds": statistics.median(times) if times else None,
        "p95_seconds": statistics.quantiles(times, n=20)[18] if len(times) >= 20 else None,
        "slowest": sorted(rows, key=lambda item: item["elapsed_seconds"], reverse=True)[:10],
        "first_failures": failures[:10],
    }
    print("SUMMARY", json.dumps(summary, indent=2), flush=True)


main()
