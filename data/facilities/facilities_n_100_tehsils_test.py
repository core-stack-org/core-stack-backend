"""Run the facilities proximity pipeline for up to 100 source tehsils.

Usage:
  python data/facilities/facilities_n_100_tehsils_test.py
  python manage.py shell < data/facilities/facilities_n_100_tehsils_test.py

Optional environment variables:
  FACILITIES_TEST_LIMIT=100
  FACILITIES_TEST_SEED=20260703
  FACILITIES_TEST_OUTPUTS=all
  FACILITIES_TEST_SYNC_GEOSERVER=0
  FACILITIES_TEST_REPORT_DIR=data/facilities/outputs/tehsil_data
"""

from __future__ import annotations

import json
import os
import random
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2] if "__file__" in globals() else Path.cwd()


def _ensure_django() -> None:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    import django
    from django.apps import apps

    if not apps.ready:
        django.setup()


def _bool_env(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "y"}


def _slug(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")


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


def _geoserver_wfs_url(base_url: str, workspace: str, layer_name: str) -> str:
    base = base_url.rstrip("/") if base_url else "<GEOSERVER_URL>"
    return (
        f"{base}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature"
        f"&typeName={workspace}:{layer_name}&outputFormat=application/json"
    )


def _report_paths(report_dir: Path, selected_count: int, seed: int) -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"facilities_n_{selected_count}_tehsils_test_{stamp}_seed_{seed}"
    return report_dir / f"{stem}.md", report_dir / f"{stem}.json"


def _brief_row(row: dict) -> dict:
    return {
        "index": row["index"],
        "state": row["state"],
        "district": row["district"],
        "tehsil": row["tehsil"],
        "elapsed_seconds": row["elapsed_seconds"],
        "row_counts": row["row_counts"],
        "gpkg_path": row["gpkg_path"],
        "timings": row["timings"],
    }


def _write_report(
    *,
    report_path: Path,
    json_path: Path,
    summary: dict,
    rows: list[dict],
    failures: list[dict],
    selected: list[tuple[str, str, str]],
    workspace: str,
    geoserver_url: str,
    source_path: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": summary,
        "tehsils": rows,
        "failures": failures,
        "selected_tehsils": [
            {"state": state, "district": district, "tehsil": tehsil}
            for state, district, tehsil in selected
        ],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Facilities Proximity Test Report",
        "",
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}",
        f"Source GPKG: `{source_path.as_posix()}`",
        f"Requested outputs: `{summary['outputs']}`",
        f"GeoServer sync attempted: `{summary['sync_to_geoserver']}`",
        f"GeoServer workspace: `{workspace}`",
        f"GeoServer base URL: `{geoserver_url or '<GEOSERVER_URL not configured>'}`",
        "",
        "## What This Run Did",
        "",
        f"- Sampled `{len(selected)}` tehsil(s) from `village_shapes` in the source GPKG using seed `{summary['seed']}`.",
        "- Ran `generate_facilities_proximity(...)` once per selected tehsil.",
        "- Wrote one local GeoPackage per successful tehsil under `data/facilities/outputs/tehsil_data/...`.",
        "- Each full run GeoPackage contains `facilities_inventory`, `facilities_nearest`, and `facilities_village_service` unless `FACILITIES_TEST_OUTPUTS` narrows the output set.",
        "- Did not write cache files, manifests, or local zip outputs.",
    ]
    if summary["sync_to_geoserver"]:
        lines.append("- Attempted to publish each requested output as a separate GeoServer layer.")
    else:
        lines.append(
            "- Did not publish to GeoServer. Set `FACILITIES_TEST_SYNC_GEOSERVER=1` to publish; expected layer names and WFS URLs are listed below."
        )

    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Successes: `{summary['successes']}`",
            f"- Failures: `{summary['failures']}`",
            f"- Wall seconds: `{summary['wall_seconds']}`",
            f"- Subsecond count: `{summary['subsecond_count']}`",
            f"- Mean seconds: `{summary['mean_seconds']}`",
            f"- Median seconds: `{summary['median_seconds']}`",
            f"- Max seconds: `{summary['max_seconds']}`",
            f"- JSON details: `{json_path.as_posix()}`",
            "",
            "## Selected Tehsils",
            "",
            "| # | Status | State | District | Tehsil | Seconds | Local GPKG |",
            "|---|---|---|---|---|---:|---|",
        ]
    )
    row_by_key = {
        (row["state"], row["district"], row["tehsil"]): row
        for row in rows
    }
    failure_by_key = {
        (failure["state"], failure["district"], failure["tehsil"]): failure
        for failure in failures
    }
    for index, (state, district, tehsil) in enumerate(selected, 1):
        row = row_by_key.get((state, district, tehsil))
        failure = failure_by_key.get((state, district, tehsil))
        if row:
            lines.append(
                f"| {index} | ok | {state} | {district} | {tehsil} | {row['elapsed_seconds']} | `{row['gpkg_path']}` |"
            )
        elif failure:
            lines.append(
                f"| {index} | failed | {state} | {district} | {tehsil} |  | `{failure['error_type']}: {failure['error']}` |"
            )

    lines.extend(["", "## GeoServer Access", ""])
    lines.append("When GeoServer sync is enabled, each tehsil is published as separate layers and a layer group.")
    lines.append("")
    lines.append("- WFS URL pattern:")
    lines.append(f"  `{_geoserver_wfs_url(geoserver_url, workspace, '<layer_name>')}`")
    lines.append("")

    for row in rows:
        group_name = f"facilities_{_slug(row['district'])}_{_slug(row['tehsil'])}"
        lines.extend(
            [
                f"### {row['index']}. {row['state']} / {row['district']} / {row['tehsil']}",
                "",
                f"- Local GPKG: `{row['gpkg_path']}`",
                f"- Layer group: `{workspace}:{group_name}`",
            ]
        )
        for output in row["outputs"]:
            url = output["geoserver_url"] or output["expected_wfs_url"]
            lines.append(
                f"- `{output['output_key']}`: local layer `{output['local_layer']}`, GeoServer layer `{workspace}:{output['geoserver_layer_name']}`, WFS `{url}`"
            )
        lines.append("")

    if failures:
        lines.extend(["## Failures", ""])
        for failure in failures:
            lines.append(
                f"- {failure['state']} / {failure['district']} / {failure['tehsil']}: `{failure['error_type']}` {failure['error']}"
            )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    _ensure_django()

    from computing.misc.facilities_proximity import generate_facilities_proximity
    from utilities.constants import FACILITIES_GEOSERVER_WORKSPACE, FACILITIES_PROXIMITY_GPKG
    from django.conf import settings

    limit = int(os.environ.get("FACILITIES_TEST_LIMIT", "100"))
    seed = int(os.environ.get("FACILITIES_TEST_SEED", "20260703"))
    outputs = os.environ.get("FACILITIES_TEST_OUTPUTS", "all")
    sync_to_geoserver = _bool_env("FACILITIES_TEST_SYNC_GEOSERVER")
    report_dir = Path(
        os.environ.get("FACILITIES_TEST_REPORT_DIR", "data/facilities/outputs/tehsil_data")
    )
    if not report_dir.is_absolute():
        report_dir = Path(settings.BASE_DIR) / report_dir

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
            "elapsed_seconds": result["elapsed_seconds"],
            "row_counts": result["row_counts"],
            "gpkg_path": result["gpkg_path"],
            "timings": result["timings"],
            "selected_outputs": result["selected_outputs"],
            "outputs": [
                {
                    "output_key": output_key,
                    "local_layer": output["local_layer"],
                    "geoserver_layer_name": output["geoserver_layer_name"],
                    "geoserver_url": output["geoserver_url"],
                    "expected_wfs_url": _geoserver_wfs_url(
                        settings.GEOSERVER_URL,
                        FACILITIES_GEOSERVER_WORKSPACE,
                        output["geoserver_layer_name"],
                    ),
                    "row_count": output["row_count"],
                    "layer_id": output["layer_id"],
                    "registration_error": output["registration_error"],
                }
                for output_key, output in result["outputs"].items()
            ],
            "geoserver_layer_group_created": result["geoserver_layer_group_created"],
        }
        rows.append(row)
        if index <= 3 or index % 10 == 0:
            print("OK", json.dumps(_brief_row(row)), flush=True)

    times = [row["elapsed_seconds"] for row in rows]
    summary = {
        "source": source_path.as_posix(),
        "limit": limit,
        "seed": seed,
        "outputs": outputs,
        "sync_to_geoserver": sync_to_geoserver,
        "successes": len(rows),
        "failures": len(failures),
        "wall_seconds": round(time.perf_counter() - started, 3),
        "subsecond_count": sum(value < 1 for value in times),
        "max_seconds": max(times) if times else None,
        "mean_seconds": statistics.mean(times) if times else None,
        "median_seconds": statistics.median(times) if times else None,
        "p95_seconds": statistics.quantiles(times, n=20)[18] if len(times) >= 20 else None,
        "slowest": [
            _brief_row(row)
            for row in sorted(rows, key=lambda item: item["elapsed_seconds"], reverse=True)[:10]
        ],
        "first_failures": failures[:10],
    }
    report_path, json_path = _report_paths(report_dir, len(selected), seed)
    summary["report_path"] = report_path.as_posix()
    summary["json_path"] = json_path.as_posix()
    _write_report(
        report_path=report_path,
        json_path=json_path,
        summary=summary,
        rows=rows,
        failures=failures,
        selected=selected,
        workspace=FACILITIES_GEOSERVER_WORKSPACE,
        geoserver_url=settings.GEOSERVER_URL,
        source_path=source_path,
    )
    print("SUMMARY", json.dumps(summary, indent=2), flush=True)
    print(f"REPORT {report_path.as_posix()}", flush=True)
    print(f"JSON {json_path.as_posix()}", flush=True)


main()
