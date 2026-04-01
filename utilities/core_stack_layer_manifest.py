"""Export a GeoNode and QGIS ready manifest for Core-Stack layers.

This utility reads the existing public Core-Stack dataset APIs and normalizes the
returned layer metadata into a transport-friendly manifest. The output is useful
for:

- GeoNode catalog seeding or harvesting preparation
- QGIS onboarding
- per-location download inventories
- publication audits
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import requests


DEFAULT_API_BASE_URL = "https://geoserver.core-stack.org/api/v1"
DEFAULT_TIMEOUT_SECONDS = 60


class ManifestExportError(RuntimeError):
    """Raised when the manifest cannot be exported cleanly."""


def normalize_api_base_url(api_base_url: str) -> str:
    return str(api_base_url).rstrip("/")


def normalize_text(value: str) -> str:
    return str(value).strip().lower()


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_auth_headers(api_key: Optional[str]) -> Dict[str, str]:
    if not api_key:
        return {}
    return {"X-API-Key": api_key}


def invoke_api(
    session: requests.Session,
    api_base_url: str,
    endpoint: str,
    api_key: str,
    params: Optional[Dict[str, str]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    url = f"{normalize_api_base_url(api_base_url)}/{endpoint.lstrip('/')}"
    response = session.get(
        url,
        params=params or {},
        headers=build_auth_headers(api_key),
        timeout=timeout_seconds,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise ManifestExportError(
            f"Request failed for {url} with status {response.status_code}: {response.text[:400]}"
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise ManifestExportError(f"API did not return JSON for {url}") from exc


def flatten_active_locations(active_locations_payload: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
    flattened: List[Dict[str, str]] = []

    for state_obj in active_locations_payload:
        state = normalize_text(state_obj.get("label", ""))
        for district_obj in state_obj.get("district", []):
            district = normalize_text(district_obj.get("label", ""))
            for tehsil_obj in district_obj.get("blocks", []):
                tehsil = normalize_text(tehsil_obj.get("label", ""))
                if not (state and district and tehsil):
                    continue
                flattened.append(
                    {
                        "state": state,
                        "district": district,
                        "tehsil": tehsil,
                    }
                )

    return flattened


def get_geoserver_root(parsed_url) -> str:
    geoserver_marker = "/geoserver"
    path = parsed_url.path or ""
    marker_index = path.find(geoserver_marker)
    if marker_index == -1:
        return f"{parsed_url.scheme}://{parsed_url.netloc}"
    prefix = path[: marker_index + len(geoserver_marker)]
    return f"{parsed_url.scheme}://{parsed_url.netloc}{prefix}"


def split_workspace_and_name(layer_identifier: str) -> Tuple[str, str]:
    if ":" not in layer_identifier:
        return "", layer_identifier
    workspace, name = layer_identifier.split(":", 1)
    return workspace, name


def infer_service_details(layer_url: str) -> Dict[str, str]:
    if not layer_url:
        return {
            "service": "",
            "workspace": "",
            "resource_name": "",
            "resource_identifier": "",
            "geoserver_root": "",
            "ows_url": "",
            "wms_url": "",
        }

    parsed = urlparse(layer_url)
    query_params = parse_qs(parsed.query)
    geoserver_root = get_geoserver_root(parsed)

    service = ""
    workspace = ""
    resource_name = ""
    resource_identifier = ""

    if "service" in query_params and query_params["service"]:
        service = query_params["service"][0].upper()

    if "typeName" in query_params and query_params["typeName"]:
        resource_identifier = query_params["typeName"][0]
        workspace, resource_name = split_workspace_and_name(resource_identifier)
    elif "CoverageId" in query_params and query_params["CoverageId"]:
        resource_identifier = query_params["CoverageId"][0]
        workspace, resource_name = split_workspace_and_name(resource_identifier)
    elif "layers" in query_params and query_params["layers"]:
        resource_identifier = query_params["layers"][0]
        workspace, resource_name = split_workspace_and_name(resource_identifier)

    ows_url = f"{geoserver_root}/{workspace}/ows" if workspace else ""
    wms_url = f"{geoserver_root}/wms" if geoserver_root else ""

    return {
        "service": service,
        "workspace": workspace,
        "resource_name": resource_name,
        "resource_identifier": resource_identifier,
        "geoserver_root": geoserver_root,
        "ows_url": ows_url,
        "wms_url": wms_url,
    }


def infer_qgis_provider(layer_type: str, service: str) -> str:
    normalized_layer_type = normalize_text(layer_type)
    normalized_service = normalize_text(service)

    if normalized_layer_type in {"vector", "point"} or normalized_service == "wfs":
        return "WFS"
    if normalized_layer_type == "raster" or normalized_service == "wcs":
        return "WCS"
    if normalized_service == "wms":
        return "WMS"
    return ""


def infer_download_format(qgis_provider: str) -> str:
    normalized_provider = normalize_text(qgis_provider)
    if normalized_provider == "wfs":
        return "GeoJSON"
    if normalized_provider == "wcs":
        return "GeoTIFF"
    if normalized_provider == "wms":
        return "Rendered map image"
    return ""


def infer_style_format(style_url: str) -> str:
    lowered = str(style_url).lower()
    if lowered.endswith(".qml"):
        return "QML"
    if lowered.endswith(".sld"):
        return "SLD"
    if lowered.endswith(".json"):
        return "JSON"
    return ""


def enrich_layer_record(location: Dict[str, str], layer: Dict[str, Any]) -> Dict[str, Any]:
    layer_url = str(layer.get("layer_url", "")).strip()
    style_url = str(layer.get("style_url", "")).strip()
    service_details = infer_service_details(layer_url)
    qgis_provider = infer_qgis_provider(
        layer_type=str(layer.get("layer_type", "")),
        service=service_details["service"],
    )

    dataset_name = str(layer.get("dataset_name", "")).strip()
    layer_name = str(layer.get("layer_name", "")).strip()

    return {
        "state": location["state"],
        "district": location["district"],
        "tehsil": location["tehsil"],
        "dataset_name": dataset_name,
        "layer_name": layer_name,
        "layer_type": str(layer.get("layer_type", "")).strip(),
        "layer_version": str(layer.get("layer_version", "")).strip(),
        "layer_url": layer_url,
        "style_url": style_url,
        "style_format": infer_style_format(style_url),
        "gee_asset_path": str(layer.get("gee_asset_path", "")).strip(),
        "service_type": service_details["service"],
        "workspace": service_details["workspace"],
        "resource_identifier": service_details["resource_identifier"],
        "resource_name": service_details["resource_name"] or layer_name,
        "geoserver_root": service_details["geoserver_root"],
        "ows_url": service_details["ows_url"],
        "wms_url": service_details["wms_url"],
        "qgis_provider": qgis_provider,
        "download_format": infer_download_format(qgis_provider),
        "geonode_publish_strategy": "remote-service-from-geoserver",
    }


def collect_locations(
    session: requests.Session,
    api_base_url: str,
    api_key: str,
    state: Optional[str],
    district: Optional[str],
    tehsil: Optional[str],
    all_active_locations: bool,
    max_locations: Optional[int],
    timeout_seconds: int,
) -> List[Dict[str, str]]:
    if all_active_locations:
        active_locations_payload = invoke_api(
            session=session,
            api_base_url=api_base_url,
            endpoint="get_active_locations/",
            api_key=api_key,
            timeout_seconds=timeout_seconds,
        )
        locations = flatten_active_locations(active_locations_payload)
    else:
        if not (state and district and tehsil):
            raise ManifestExportError(
                "Provide --state, --district, and --tehsil together, or use --all-active-locations."
            )
        locations = [
            {
                "state": normalize_text(state),
                "district": normalize_text(district),
                "tehsil": normalize_text(tehsil),
            }
        ]

    if max_locations is not None:
        locations = locations[:max_locations]

    if not locations:
        raise ManifestExportError("No locations were resolved for export.")

    return locations


def export_manifest(
    api_base_url: str,
    api_key: str,
    state: Optional[str],
    district: Optional[str],
    tehsil: Optional[str],
    all_active_locations: bool,
    max_locations: Optional[int],
    timeout_seconds: int,
) -> Dict[str, Any]:
    with requests.Session() as session:
        locations = collect_locations(
            session=session,
            api_base_url=api_base_url,
            api_key=api_key,
            state=state,
            district=district,
            tehsil=tehsil,
            all_active_locations=all_active_locations,
            max_locations=max_locations,
            timeout_seconds=timeout_seconds,
        )

        records: List[Dict[str, Any]] = []

        for location in locations:
            payload = invoke_api(
                session=session,
                api_base_url=api_base_url,
                endpoint="get_generated_layer_urls/",
                api_key=api_key,
                params=location,
                timeout_seconds=timeout_seconds,
            )

            if not isinstance(payload, list):
                raise ManifestExportError(
                    "Expected get_generated_layer_urls/ to return a JSON list."
                )

            for layer in payload:
                records.append(enrich_layer_record(location=location, layer=layer))

    unique_wms_urls = sorted(
        {record["wms_url"] for record in records if record.get("wms_url")}
    )
    unique_workspaces = sorted(
        {record["workspace"] for record in records if record.get("workspace")}
    )

    layer_type_counts: Dict[str, int] = {}
    for record in records:
        layer_type = normalize_text(record.get("layer_type", "")) or "unknown"
        layer_type_counts[layer_type] = layer_type_counts.get(layer_type, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "api_base_url": normalize_api_base_url(api_base_url),
        "scope": {
            "all_active_locations": all_active_locations,
            "requested_state": normalize_text(state) if state else "",
            "requested_district": normalize_text(district) if district else "",
            "requested_tehsil": normalize_text(tehsil) if tehsil else "",
            "location_count": len(locations),
        },
        "summary": {
            "layer_count": len(records),
            "layer_type_counts": layer_type_counts,
            "unique_workspaces": unique_workspaces,
            "unique_wms_urls": unique_wms_urls,
        },
        "locations": locations,
        "layers": records,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export a GeoNode and QGIS ready Core-Stack layer manifest from the "
            "existing public APIs."
        )
    )
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help=f"Core-Stack API base URL. Default: {DEFAULT_API_BASE_URL}",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("CORE_STACK_API_KEY"),
        help="API key. Defaults to CORE_STACK_API_KEY from the environment.",
    )
    parser.add_argument("--state", help="State name for a single-location export.")
    parser.add_argument("--district", help="District name for a single-location export.")
    parser.add_argument("--tehsil", help="Tehsil name for a single-location export.")
    parser.add_argument(
        "--all-active-locations",
        action="store_true",
        help="Export manifests for every active location returned by get_active_locations/.",
    )
    parser.add_argument(
        "--max-locations",
        type=int,
        help="Optional limit when exporting all active locations.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP timeout per request. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON output file. If omitted, JSON is printed to stdout.",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        help="Optional CSV output file for the flattened layer records.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.api_key:
        parser.error(
            "An API key is required. Pass --api-key or set CORE_STACK_API_KEY."
        )

    if args.all_active_locations and any([args.state, args.district, args.tehsil]):
        parser.error(
            "Use either --all-active-locations or the --state/--district/--tehsil trio, not both."
        )

    if not args.all_active_locations:
        trio = [args.state, args.district, args.tehsil]
        if any(trio) and not all(trio):
            parser.error(
                "--state, --district, and --tehsil must all be provided together."
            )
        if not all(trio):
            parser.error(
                "Provide --state, --district, and --tehsil, or use --all-active-locations."
            )

    manifest = export_manifest(
        api_base_url=args.api_base_url,
        api_key=args.api_key,
        state=args.state,
        district=args.district,
        tehsil=args.tehsil,
        all_active_locations=args.all_active_locations,
        max_locations=args.max_locations,
        timeout_seconds=args.timeout_seconds,
    )

    if args.output:
        write_json(args.output, manifest)
    else:
        print(json.dumps(manifest, indent=2))

    if args.csv_output:
        write_csv(args.csv_output, manifest["layers"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
