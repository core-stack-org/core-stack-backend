#!/usr/bin/env python3
"""
Live API v2 smoke + units/JSON format validation against a running Docker backend.

Default base: http://localhost:9001/api/v2/

Usage:
  export PUBLIC_API_KEY='...'
  python public_api/live_v2_api_test.py

Optional overrides:
  PUBLIC_API_BASE, PUBLIC_API_STATE, PUBLIC_API_DISTRICT, PUBLIC_API_TEHSIL,
  PUBLIC_API_MWS_ID, PUBLIC_API_UID, PUBLIC_API_LAT, PUBLIC_API_LON
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

BASE = os.environ.get("PUBLIC_API_BASE", "http://localhost:9001/api/v2").rstrip("/")
API_KEY = os.environ.get("PUBLIC_API_KEY", "")

STATE = os.environ.get("PUBLIC_API_STATE", "Rajasthan")
DISTRICT = os.environ.get("PUBLIC_API_DISTRICT", "Bhilwara")
TEHSIL = os.environ.get("PUBLIC_API_TEHSIL", "Mandalgarh")
MWS_ID = os.environ.get("PUBLIC_API_MWS_ID", "12_100174")
UID = os.environ.get("PUBLIC_API_UID", "12_100174_104")
LAT = os.environ.get("PUBLIC_API_LAT", "25.20231618101583")
LON = os.environ.get("PUBLIC_API_LON", "75.0868641493802")

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

EXPECTED_MWS_FORTNIGHT_UNITS = {
    "time": "iso8601",
    "time_step": "15_days",
    "et": "mm",
    "runoff": "mm",
    "precipitation": "mm",
}

EXPECTED_WATERBODY_METADATA_UNITS = {
    "UID": "id",
    "sum": "ha",
    "total_cropable_area_ever_hydroyear": "ha",
    "zoi": "count",
    "zoi_area": "ha",
}

EXPECTED_WATERBODY_ANNUAL_UNITS = {
    "time": "agricultural_year",
    "cropping_intensity": "ratio",
    "doubly_cropped_area": "ha",
    "single_cropped_area": "ha",
    "single_kharif_cropped_area": "ha",
    "single_non_kharif_cropped_area": "ha",
    "triply_cropped_area": "ha",
}


class Failures(list):
    def add(self, endpoint: str, message: str) -> None:
        self.append(f"{endpoint}: {message}")


# Upstream data not present on this Docker/act4dws5 setup — still hit the route,
# but treat as SKIP rather than FAIL so format checks on available data stay green.
SKIP_ERROR_SNIPPETS = (
    "Data not found for this state, district, tehsil",
    "No such file or directory",
    "Invalid response from MWS GeoServer",
    "Mws Layer is not generated",
    "Latitude and longitude is not in SOI boundary",
    "Feature type pan_india_asset",
)


def is_skippable_data_gap(message: str | None) -> bool:
    text = str(message or "")
    return any(snippet in text for snippet in SKIP_ERROR_SNIPPETS)


def request_json(path: str, params: dict[str, Any] | None = None) -> tuple[int, Any]:
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"X-API-Key": API_KEY})
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"status": "error", "error_message": raw[:300]}
        return exc.code, body


def assert_success_envelope(endpoint: str, body: Any, failures: Failures) -> dict | None:
    if not isinstance(body, dict):
        failures.add(endpoint, f"body is not an object: {type(body)}")
        return None
    for key in ("status", "error_message", "data"):
        if key not in body:
            failures.add(endpoint, f"missing envelope key '{key}'")
    if body.get("status") != "success":
        failures.add(
            endpoint,
            f"expected status=success, got {body.get('status')}: {body.get('error_message')}",
        )
        return None
    if body.get("error_message") is not None:
        failures.add(endpoint, f"error_message should be null, got {body.get('error_message')!r}")
    # Strict JSON: reject NaN/Infinity which are not valid JSON.
    try:
        json.loads(json.dumps(body, allow_nan=False))
    except (TypeError, ValueError) as exc:
        failures.add(endpoint, f"body is not valid JSON: {exc}")
    return body.get("data")


def assert_aligned_numeric_series(
    endpoint: str,
    block: dict,
    units: dict,
    time_key: str,
    failures: Failures,
    *,
    time_pattern: re.Pattern[str] | None = None,
) -> None:
    if not isinstance(block, dict) or not isinstance(units, dict):
        failures.add(endpoint, "timeseries block/units must be objects")
        return
    if time_key not in block:
        failures.add(endpoint, f"missing {time_key} in timeseries block")
        return
    times = block[time_key]
    if not isinstance(times, list) or not times:
        failures.add(endpoint, f"{time_key} must be a non-empty list")
        return
    n = len(times)
    for t in times:
        if not isinstance(t, str):
            failures.add(endpoint, f"{time_key} values must be strings")
            break
        if time_pattern and not time_pattern.match(t):
            failures.add(endpoint, f"{time_key} value {t!r} does not match expected pattern")
            break

    for metric, series in block.items():
        if metric == time_key:
            continue
        if not isinstance(series, list):
            failures.add(endpoint, f"{metric} must be a list")
            continue
        if len(series) != n:
            failures.add(endpoint, f"{metric} length {len(series)} != time length {n}")
        if metric not in units:
            failures.add(endpoint, f"missing unit for metric '{metric}'")
        for value in series:
            if value is None:
                continue
            if isinstance(value, (dict, list, str)):
                continue
            if not isinstance(value, (int, float)):
                failures.add(endpoint, f"{metric} has non-numeric value {value!r}")
            elif isinstance(value, float) and value != round(value, 2):
                failures.add(endpoint, f"{metric} float not rounded to 2 decimals: {value}")


def validate_mws_data(endpoint: str, data: Any, failures: Failures) -> None:
    if not isinstance(data, dict):
        failures.add(endpoint, "data must be object")
        return
    expected_keys = {"metadata", "fortnight", "fortnight_units"}
    if set(data.keys()) != expected_keys:
        failures.add(endpoint, f"data keys {set(data.keys())} != {expected_keys}")
    if "time_series" in data or "hourly" in data:
        failures.add(endpoint, "legacy time_series/hourly keys must not appear in v2")
    metadata = data.get("metadata") or {}
    if not isinstance(metadata, dict) or "mws_id" not in metadata:
        failures.add(endpoint, "metadata.mws_id required")
    units = data.get("fortnight_units")
    if units != EXPECTED_MWS_FORTNIGHT_UNITS:
        failures.add(endpoint, f"fortnight_units mismatch: {units}")
    assert_aligned_numeric_series(
        endpoint,
        data.get("fortnight") or {},
        units or {},
        "time",
        failures,
        time_pattern=ISO_DATE_RE,
    )


def validate_waterbody_item(endpoint: str, item: Any, failures: Failures) -> None:
    if not isinstance(item, dict):
        failures.add(endpoint, "waterbody item must be object")
        return
    for key in ("metadata", "metadata_units", "annual", "annual_units"):
        if key not in item:
            failures.add(endpoint, f"missing '{key}'")
    meta_units = item.get("metadata_units") or {}
    for key, expected in EXPECTED_WATERBODY_METADATA_UNITS.items():
        if key in (item.get("metadata") or {}) and meta_units.get(key) != expected:
            failures.add(
                endpoint,
                f"metadata_units[{key}]={meta_units.get(key)!r} expected {expected!r}",
            )
    annual_units = item.get("annual_units") or {}
    for key, expected in EXPECTED_WATERBODY_ANNUAL_UNITS.items():
        if key in (item.get("annual") or {}) and annual_units.get(key) != expected:
            failures.add(
                endpoint,
                f"annual_units[{key}]={annual_units.get(key)!r} expected {expected!r}",
            )
    assert_aligned_numeric_series(
        endpoint,
        item.get("annual") or {},
        annual_units,
        "time",
        failures,
    )


def validate_hints_or_units(
    endpoint: str, data: dict, map_key: str, failures: Failures
) -> None:
    if map_key not in data:
        failures.add(endpoint, f"missing '{map_key}'")
        return
    unit_map = data[map_key]
    if not isinstance(unit_map, dict) or not unit_map:
        failures.add(endpoint, f"{map_key} must be a non-empty object")
        return
    _assert_leaf_string_units(endpoint, map_key, unit_map, failures)


def _assert_leaf_string_units(
    endpoint: str, map_key: str, unit_map: dict, failures: Failures
) -> None:
    for key, value in unit_map.items():
        if not isinstance(key, str):
            failures.add(endpoint, f"{map_key} keys must be strings")
            continue
        if isinstance(value, dict):
            if not value:
                failures.add(endpoint, f"{map_key}.{key} must be a non-empty object")
                continue
            _assert_leaf_string_units(endpoint, f"{map_key}.{key}", value, failures)
            continue
        if not isinstance(value, str) or not value.strip():
            failures.add(endpoint, f"{map_key}[{key}] must be a non-empty string")


def main() -> int:
    if not API_KEY:
        print("PUBLIC_API_KEY is required", file=sys.stderr)
        return 2

    failures = Failures()
    skips: list[str] = []
    results: list[tuple[str, int, str]] = []

    geo = {"state": STATE, "district": DISTRICT, "tehsil": TEHSIL}
    mws = {**geo, "mws_id": MWS_ID}
    wb = {**geo, "uid": UID}
    latlon = {"latitude": LAT, "longitude": LON}

    endpoints: list[tuple[str, dict | None, str]] = [
        ("/get_active_locations/", None, "active_locations"),
        ("/get_admin_details_by_latlon/", latlon, "admin_latlon"),
        ("/get_mwsid_by_latlon/", latlon, "mws_latlon"),
        ("/get_tehsil_data/", geo, "tehsil_data"),
        ("/get_mws_data/", mws, "mws_data"),
        ("/get_mws_kyl_indicators/", mws, "kyl"),
        ("/get_generated_layer_urls/", geo, "layers"),
        ("/get_mws_report/", mws, "mws_report"),
        ("/get_mws_geometries/", mws, "mws_geom"),
        ("/get_village_geometries/", geo, "village_geom"),
        ("/get_waterbodies_data_by_admin/", geo, "waterbodies_admin"),
        ("/get_waterbody_data/", wb, "waterbody"),
    ]

    for path, params, kind in endpoints:
        code, body = request_json(path, params)
        status_label = body.get("status") if isinstance(body, dict) else "?"
        results.append((path, code, str(status_label)))

        if code != 200 or (isinstance(body, dict) and body.get("status") != "success"):
            # Still require error responses to be valid JSON objects with status/error_message.
            if not isinstance(body, dict) or "status" not in body or "error_message" not in body:
                failures.add(path, f"HTTP {code}: invalid error envelope {body!r}"[:240])
            elif is_skippable_data_gap(body.get("error_message")):
                skips.append(f"{path}: HTTP {code}: {body.get('error_message')}")
            else:
                failures.add(path, f"HTTP {code}: {body.get('error_message')}")
            continue

        data = assert_success_envelope(path, body, failures)
        if data is None:
            continue

        if kind == "mws_data":
            validate_mws_data(path, data, failures)
        elif kind == "waterbody":
            validate_waterbody_item(path, data, failures)
        elif kind == "waterbodies_admin":
            if not isinstance(data, list) or not data:
                failures.add(path, "data must be a non-empty list")
            else:
                validate_waterbody_item(path + "[0]", data[0], failures)
        elif kind == "active_locations":
            if not isinstance(data, dict) or "locations" not in data:
                failures.add(path, "data.locations required")
            validate_hints_or_units(path, data, "location_field_hints", failures)
        elif kind == "admin_latlon":
            if not isinstance(data, dict) or "admin_details" not in data:
                failures.add(path, "data.admin_details required")
            validate_hints_or_units(path, data, "admin_field_hints", failures)
        elif kind == "mws_latlon":
            if not isinstance(data, dict) or "mws_details" not in data:
                failures.add(path, "data.mws_details required")
            if "mws_field_hints" in data:
                validate_hints_or_units(path, data, "mws_field_hints", failures)
        elif kind == "tehsil_data":
            if not isinstance(data, dict):
                failures.add(path, "data must be object")
            else:
                if "tehsil_units" in data:
                    validate_hints_or_units(path, data, "tehsil_units", failures)
                if "annual_units" in data:
                    validate_hints_or_units(path, data, "annual_units", failures)
        elif kind == "kyl":
            if not isinstance(data, dict):
                failures.add(path, "data must be object")
            else:
                for ukey in ("metadata_units", "annual_units", "fortnight_units"):
                    if ukey in data:
                        validate_hints_or_units(path, data, ukey, failures)
        elif kind == "layers":
            if not isinstance(data, dict) or "layers" not in data:
                failures.add(path, "data.layers required")
            validate_hints_or_units(path, data, "layer_field_units", failures)
        elif kind == "mws_report":
            if not isinstance(data, dict) or "report" not in data:
                failures.add(path, "data.report required")
            validate_hints_or_units(path, data, "report_field_hints", failures)
        elif kind == "mws_geom":
            if not isinstance(data, dict) or "mws_geometry" not in data:
                failures.add(path, "data.mws_geometry required")
            validate_hints_or_units(path, data, "mws_geometry_field_hints", failures)
        elif kind == "village_geom":
            if not isinstance(data, dict) or "villages" not in data:
                failures.add(path, "data.villages required")
            validate_hints_or_units(path, data, "village_field_hints", failures)

    print(f"Base: {BASE}")
    print(f"Geo: {STATE}/{DISTRICT}/{TEHSIL} mws_id={MWS_ID} uid={UID}")
    print("\nResults:")
    for path, code, status_label in results:
        print(f"  {code:3} {status_label:7} {path}")

    if skips:
        print(f"\nSKIPPED data gaps ({len(skips)}):")
        for item in skips:
            print(f"  - {item}")

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for item in failures:
            print(f"  - {item}")
        return 1

    print("\nAll reachable v2 endpoints passed envelope/units/JSON checks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
