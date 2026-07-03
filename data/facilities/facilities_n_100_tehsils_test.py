"""
Run the facilities proximity pipeline for up to 100 source tehsils.

Usage:
  python data/facilities/facilities_n_100_tehsils_test.py
  python manage.py shell < data/facilities/facilities_n_100_tehsils_test.py

Optional environment variables:
  FACILITIES_TEST_LIMIT=100
  FACILITIES_TEST_SEED=20260703
  FACILITIES_TEST_OUTPUTS=all
  FACILITIES_TEST_SYNC_GEOSERVER=1
  FACILITIES_TEST_OVERWRITE_GEOSERVER=1
  FACILITIES_TEST_VERIFY_GEOSERVER=1
  FACILITIES_TEST_REGISTER_DB=0
  FACILITIES_TEST_DELAY=0.0          # Seconds to sleep between tehsils to avoid GeoServer exhaustion
  FACILITIES_TEST_CLEANUP_GEOSERVER=0 # Set 1 to delete existing facilities_* layers before running
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
import urllib.error
import urllib.parse
import urllib.request
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


def _float_env(name: str, default: float = 0.0) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


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


def _geoserver_wfs_probe_url(base_url: str, workspace: str, layer_name: str) -> str:
    base = base_url.rstrip("/") if base_url else "<GEOSERVER_URL>"
    params = urllib.parse.urlencode(
        {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": f"{workspace}:{layer_name}",
            "outputFormat": "application/json",
            "maxFeatures": "1",
        }
    )
    return f"{base}/{workspace}/ows?{params}"


def _geoserver_rest_url(base_url: str, workspace: str, endpoint: str) -> str:
    base = base_url.rstrip("/") if base_url else ""
    return f"{base}/rest/workspaces/{workspace}/{endpoint}"


def _geoserver_request(url: str, method: str = "GET", username: str = "", password: str = "") -> dict | None:
    """Make a basic GeoServer REST request and return parsed JSON or None."""
    try:
        pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        if username and password:
            pwd_mgr.add_password(None, url, username, password)
        handler = urllib.request.HTTPBasicAuthHandler(pwd_mgr)
        opener = urllib.request.build_opener(handler)
        req = urllib.request.Request(url, method=method)
        req.add_header("Accept", "application/json")
        with opener.open(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def _verify_wfs_layer(base_url: str, workspace: str, layer_name: str) -> dict:
    url = _geoserver_wfs_probe_url(base_url, workspace, layer_name)
    if not base_url:
        return {
            "ok": False,
            "url": url,
            "status_code": None,
            "feature_count": None,
            "error": "GEOSERVER_URL is not configured",
        }
    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            content_type = response.headers.get("Content-Type", "")
            status_code = getattr(response, "status", None)
        if "json" not in content_type.lower():
            return {
                "ok": False,
                "url": url,
                "status_code": status_code,
                "feature_count": None,
                "error": body[:500],
            }
        payload = json.loads(body)
        features = payload.get("features", [])
        return {
            "ok": True,
            "url": url,
            "status_code": status_code,
            "feature_count": len(features),
            "error": None,
        }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "url": url,
            "status_code": exc.code,
            "feature_count": None,
            "error": body[:500],
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "status_code": None,
            "feature_count": None,
            "error": str(exc)[:500],
        }


def _cleanup_geoserver_layers(base_url: str, workspace: str, username: str, password: str, dry_run: bool = False) -> int:
    """Delete all layers starting with 'facilities_' in the workspace. Returns count deleted."""
    layers_url = _geoserver_rest_url(base_url, workspace, "layers.json")
    data = _geoserver_request(layers_url, username=username, password=password)
    if not data or "layers" not in data or "layer" not in data["layers"]:
        print("CLEANUP No layers found or could not list layers.", flush=True)
        return 0

    to_delete = [l["name"] for l in data["layers"]["layer"] if l["name"].startswith("facilities_")]
    if not to_delete:
        print("CLEANUP No facilities_* layers to remove.", flush=True)
        return 0

    print(f"CLEANUP Found {len(to_delete)} facilities_* layer(s) to remove.", flush=True)
    deleted = 0
    pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    if username and password:
        pwd_mgr.add_password(None, base_url, username, password)
    handler = urllib.request.HTTPBasicAuthHandler(pwd_mgr)
    opener = urllib.request.build_opener(handler)

    for name in to_delete:
        del_url = _geoserver_rest_url(base_url, workspace, f"layers/{name}?recurse=true")
        if dry_run:
            print(f"CLEANUP [DRY RUN] Would DELETE {name}", flush=True)
            deleted += 1
            continue
        try:
            req = urllib.request.Request(del_url, method="DELETE")
            opener.open(req, timeout=30)
            deleted += 1
            print(f"CLEANUP Deleted layer: {name}", flush=True)
        except urllib.error.HTTPError as e:
            print(f"CLEANUP Failed to delete {name}: HTTP {e.code} {e.reason}", flush=True)
        except Exception as e:
            print(f"CLEANUP Failed to delete {name}: {e}", flush=True)
        time.sleep(0.2)  # Brief pause between deletes

    # Clean up layer groups
    lg_url = _geoserver_rest_url(base_url, workspace, "layergroups.json")
    lg_data = _geoserver_request(lg_url, username=username, password=password)
    if lg_data and "layerGroups" in lg_data and "layerGroup" in lg_data["layerGroups"]:
        lg_to_delete = [lg["name"] for lg in lg_data["layerGroups"]["layerGroup"] if lg["name"].startswith("facilities_")]
        for name in lg_to_delete:
            del_url = _geoserver_rest_url(base_url, workspace, f"layergroups/{name}?recurse=true")
            if dry_run:
                print(f"CLEANUP [DRY RUN] Would DELETE layer group: {name}", flush=True)
                continue
            try:
                req = urllib.request.Request(del_url, method="DELETE")
                opener.open(req, timeout=30)
                print(f"CLEANUP Deleted layer group: {name}", flush=True)
            except Exception as e:
                print(f"CLEANUP Failed to delete layer group {name}: {e}", flush=True)
            time.sleep(0.2)

    print(f"CLEANUP Done. Removed {deleted} layer(s).", flush=True)
    return deleted


def _report_paths(report_dir: Path, selected_count: int, seed: int) -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem = f"facilities_n_{selected_count}_tehsils_test_{stamp}_seed_{seed}"
    return report_dir / f"{stem}.md", report_dir / f"{stem}.json"


def _safe_get(d: dict, key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default


def _brief_row(row: dict) -> dict:
    verification = [output.get("wfs_verification") for output in row.get("outputs", [])]
    verified = sum(1 for item in verification if item and item.get("ok"))
    failed = sum(1 for item in verification if item and not item.get("ok"))
    return {
        "index": row["index"],
        "state": row["state"],
        "district": row["district"],
        "tehsil": row["tehsil"],
        "elapsed_seconds": row["elapsed_seconds"],
        "row_counts": row["row_counts"],
        "gpkg_path": row["gpkg_path"],
        "timings": row["timings"],
        "geoserver_verified": verified,
        "geoserver_verify_failed": failed,
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
        f"GeoServer overwrite enabled: `{summary['overwrite_geoserver']}`",
        f"GeoServer WFS verification attempted: `{summary['verify_geoserver']}`",
        f"Django DB layer registration attempted: `{summary['register_db']}`",
        f"GeoServer workspace: `{workspace}`",
        f"GeoServer base URL: `{geoserver_url or '<GEOSERVER_URL not configured>'}`",
        f"Inter-tehsil delay: `{summary['delay_seconds']}s`",
        f"Pre-run GeoServer cleanup: `{summary['cleanup_geoserver']}`",
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
        if summary["cleanup_geoserver"]:
            lines.append("- Pre-run cleanup: deleted all existing `facilities_*` layers and layer groups from the workspace.")
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
            f"- P95 seconds: `{summary.get('p95_seconds', 'N/A')}`",
            f"- Expected GeoServer layers: `{summary['expected_geoserver_layers']}`",
            f"- Published GeoServer URLs returned: `{summary['published_geoserver_urls']}`",
            f"- WFS verified layers: `{summary['verified_geoserver_layers']}`",
            f"- WFS verification failures: `{summary['failed_geoserver_verifications']}`",
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
            reg_errors = [o["registration_error"] for o in row["outputs"] if o["registration_error"]]
            wfs_errors = [
                o.get("wfs_verification")
                for o in row["outputs"]
                if o.get("wfs_verification") and not o["wfs_verification"].get("ok")
            ]
            status = "ok" if not reg_errors and not wfs_errors else f"partial ({len(reg_errors) + len(wfs_errors)} err)"
            lines.append(
                f"| {index} | {status} | {state} | {district} | {tehsil} | {row['elapsed_seconds']} | `{row['gpkg_path']}` |"
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
                f"- Layer group: `{workspace}:{group_name}` (created: `{row['geoserver_layer_group_created']}`)",
            ]
        )
        for output in row["outputs"]:
            url = output["geoserver_url"] or output["expected_wfs_url"]
            verify = output.get("wfs_verification")
            verify_str = ""
            if verify:
                if verify.get("ok"):
                    verify_str = f", WFS verified: `true`, probe features: `{verify.get('feature_count')}`"
                else:
                    verify_str = f", WFS verified: `false`, error: `{verify.get('error')}`"
            err_str = f", registration error: `{output['registration_error']}`" if output["registration_error"] else ""
            lines.append(
                f"- `{output['output_key']}`: local layer `{output['local_layer']}`, GeoServer layer `{workspace}:{output['geoserver_layer_name']}`, WFS `{url}`{verify_str}{err_str}"
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
    sync_to_geoserver = _bool_env("FACILITIES_TEST_SYNC_GEOSERVER", "1")
    overwrite_geoserver = _bool_env("FACILITIES_TEST_OVERWRITE_GEOSERVER", "1")
    verify_geoserver = _bool_env("FACILITIES_TEST_VERIFY_GEOSERVER", "1")
    register_db = _bool_env("FACILITIES_TEST_REGISTER_DB", "0")
    delay_seconds = _float_env("FACILITIES_TEST_DELAY", 0.0)
    cleanup_geoserver = _bool_env("FACILITIES_TEST_CLEANUP_GEOSERVER")
    report_dir = Path(
        os.environ.get("FACILITIES_TEST_REPORT_DIR", "data/facilities/outputs/tehsil_data")
    )
    if not report_dir.is_absolute():
        report_dir = Path(settings.BASE_DIR) / report_dir

    source_path = Path(FACILITIES_PROXIMITY_GPKG)
    if not source_path.is_absolute():
        source_path = Path(settings.BASE_DIR) / source_path

    geoserver_url = getattr(settings, "GEOSERVER_URL", "")
    geoserver_user = getattr(settings, "GEOSERVER_USERNAME", "")
    geoserver_pass = getattr(settings, "GEOSERVER_PASSWORD", "")
    workspace = FACILITIES_GEOSERVER_WORKSPACE

    # --- Pre-run cleanup ---
    if sync_to_geoserver and cleanup_geoserver and geoserver_url:
        print(f"PRE-RUN Cleaning up existing facilities_* layers in workspace '{workspace}'...", flush=True)
        _cleanup_geoserver_layers(geoserver_url, workspace, geoserver_user, geoserver_pass)

    tehsils = _source_tehsils(source_path)
    random.Random(seed).shuffle(tehsils)
    selected = tehsils[: max(1, min(limit, len(tehsils)))]

    print(
        "START Processing "
        f"{len(selected)} tehsil(s), sync_to_geoserver={sync_to_geoserver}, "
        f"overwrite_geoserver={overwrite_geoserver}, verify_geoserver={verify_geoserver}, "
        f"register_db={register_db}, "
        f"delay={delay_seconds}s",
        flush=True,
    )

    rows: list[dict] = []
    failures: list[dict] = []
    started = time.perf_counter()

    for index, (state, district, tehsil) in enumerate(selected, 1):
        # Inter-tehsil delay (skip before the first one)
        if index > 1 and delay_seconds > 0:
            time.sleep(delay_seconds)

        try:
            result = generate_facilities_proximity(
                state=state,
                district=district,
                block=tehsil,
                sync_to_geoserver=sync_to_geoserver,
                overwrite=overwrite_geoserver,
                outputs=outputs,
                register_layers=register_db,
            )
        except Exception as exc:
            failure = {
                "index": index,
                "state": state,
                "district": district,
                "tehsil": tehsil,
                "error_type": exc.__class__.__name__,
                "error": str(exc)[:500],
            }
            failures.append(failure)
            print("FAIL", json.dumps(failure), flush=True)
            continue

        result_outputs = _safe_get(result, "outputs", {})
        output_rows = []
        for output_key, output in result_outputs.items():
            geoserver_layer_name = _safe_get(output, "geoserver_layer_name", "")
            verification = None
            if sync_to_geoserver and verify_geoserver:
                verification = _verify_wfs_layer(
                    geoserver_url,
                    workspace,
                    geoserver_layer_name,
                )
            output_rows.append(
                {
                    "output_key": output_key,
                    "local_layer": _safe_get(output, "local_layer", ""),
                    "geoserver_layer_name": geoserver_layer_name,
                    "geoserver_url": _safe_get(output, "geoserver_url"),
                    "expected_wfs_url": _geoserver_wfs_url(
                        geoserver_url,
                        workspace,
                        geoserver_layer_name,
                    ),
                    "row_count": _safe_get(output, "row_count"),
                    "layer_id": _safe_get(output, "layer_id"),
                    "registration_error": _safe_get(output, "registration_error"),
                    "wfs_verification": verification,
                }
            )
        row = {
            "index": index,
            "state": state,
            "district": district,
            "tehsil": tehsil,
            "elapsed_seconds": _safe_get(result, "elapsed_seconds", 0),
            "row_counts": _safe_get(result, "row_counts", {}),
            "gpkg_path": _safe_get(result, "gpkg_path", ""),
            "timings": _safe_get(result, "timings", {}),
            "selected_outputs": _safe_get(result, "selected_outputs", []),
            "outputs": output_rows,
            "geoserver_layer_group_created": _safe_get(result, "geoserver_layer_group_created", False),
        }
        rows.append(row)

        # Log progress
        reg_errors = [o["registration_error"] for o in row["outputs"] if o["registration_error"]]
        wfs_errors = [
            o["wfs_verification"]
            for o in row["outputs"]
            if o.get("wfs_verification") and not o["wfs_verification"].get("ok")
        ]
        status_tag = "OK" if not reg_errors and not wfs_errors else "PARTIAL"
        if index <= 5 or index % 10 == 0 or reg_errors or wfs_errors:
            print(f"{status_tag} [{index}/{len(selected)}]", json.dumps(_brief_row(row)), flush=True)

    times = [row["elapsed_seconds"] for row in rows]
    all_outputs = [output for row in rows for output in row["outputs"]]
    verified_outputs = [
        output
        for output in all_outputs
        if output.get("wfs_verification") and output["wfs_verification"].get("ok")
    ]
    failed_verifications = [
        output
        for output in all_outputs
        if output.get("wfs_verification") and not output["wfs_verification"].get("ok")
    ]
    summary = {
        "source": source_path.as_posix(),
        "limit": limit,
        "seed": seed,
        "outputs": outputs,
        "sync_to_geoserver": sync_to_geoserver,
        "overwrite_geoserver": overwrite_geoserver,
        "verify_geoserver": verify_geoserver and sync_to_geoserver,
        "register_db": register_db and sync_to_geoserver,
        "delay_seconds": delay_seconds,
        "cleanup_geoserver": cleanup_geoserver and sync_to_geoserver,
        "successes": len(rows),
        "failures": len(failures),
        "expected_geoserver_layers": len(all_outputs) if sync_to_geoserver else 0,
        "published_geoserver_urls": sum(1 for output in all_outputs if output.get("geoserver_url")),
        "verified_geoserver_layers": len(verified_outputs),
        "failed_geoserver_verifications": len(failed_verifications),
        "wall_seconds": round(time.perf_counter() - started, 3),
        "subsecond_count": sum(1 for v in times if v < 1),
        "max_seconds": max(times) if times else None,
        "mean_seconds": round(statistics.mean(times), 3) if times else None,
        "median_seconds": round(statistics.median(times), 3) if times else None,
        "p95_seconds": round(statistics.quantiles(times, n=20)[18], 3) if len(times) >= 20 else None,
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
        workspace=workspace,
        geoserver_url=geoserver_url,
        source_path=source_path,
    )
    print("SUMMARY", json.dumps(summary, indent=2), flush=True)
    print(f"REPORT {report_path.as_posix()}", flush=True)
    print(f"JSON {json_path.as_posix()}", flush=True)


main()
