"""Export a GeoNode and QGIS ready manifest for Core-Stack layers.

This Django management command reads layer metadata directly from the Django database
and generates a transport-friendly manifest without requiring external API calls.
The output is useful for:

- GeoNode catalog seeding or harvesting preparation
- QGIS onboarding
- per-location download inventories
- publication audits
"""

from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from computing.models import Layer, Dataset, LayerType
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI
from stats_generator.utils import get_url
from nrm_app.settings import GEOSERVER_URL


def raster_tiff_download_url(workspace, layer_name):
    """Generate GeoTIFF download URL for raster layers."""
    return (
        f"{GEOSERVER_URL}/{workspace}/wcs?service=WCS&version=2.0.1"
        f"&request=GetCoverage&CoverageId={workspace}:{layer_name}"
        f"&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256"
    )


def infer_service_details(layer_url: str) -> dict:
    """Parse layer URL to extract service details."""
    from urllib.parse import parse_qs, urlparse

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

    geoserver_marker = "/geoserver"
    path = parsed.path or ""
    marker_index = path.find(geoserver_marker)
    if marker_index == -1:
        geoserver_root = f"{parsed.scheme}://{parsed.netloc}"
    else:
        prefix = path[: marker_index + len(geoserver_marker)]
        geoserver_root = f"{parsed.scheme}://{parsed.netloc}{prefix}"

    service = query_params.get("service", [""])[0].upper()
    workspace = ""
    resource_name = ""
    resource_identifier = ""

    if "typeName" in query_params and query_params["typeName"]:
        resource_identifier = query_params["typeName"][0]
        if ":" in resource_identifier:
            workspace, resource_name = resource_identifier.split(":", 1)
    elif "CoverageId" in query_params and query_params["CoverageId"]:
        resource_identifier = query_params["CoverageId"][0]
        if ":" in resource_identifier:
            workspace, resource_name = resource_identifier.split(":", 1)
    elif "layers" in query_params and query_params["layers"]:
        resource_identifier = query_params["layers"][0]
        if ":" in resource_identifier:
            workspace, resource_name = resource_identifier.split(":", 1)

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
    """Infer QGIS provider from layer type and service."""
    layer_type = str(layer_type).strip().lower()
    service = str(service).strip().lower()

    if layer_type in {"vector", "point"} or service == "wfs":
        return "WFS"
    if layer_type == "raster" or service == "wcs":
        return "WCS"
    if service == "wms":
        return "WMS"
    return ""


def infer_download_format(qgis_provider: str) -> str:
    """Infer download format from QGIS provider."""
    provider = str(qgis_provider).strip().lower()
    if provider == "wfs":
        return "GeoJSON"
    if provider == "wcs":
        return "GeoTIFF"
    if provider == "wms":
        return "Rendered map image"
    return ""


def infer_style_format(style_url: str) -> str:
    """Infer style format from style URL."""
    lowered = str(style_url).lower()
    if lowered.endswith(".qml"):
        return "QML"
    if lowered.endswith(".sld"):
        return "SLD"
    if lowered.endswith(".json"):
        return "JSON"
    return ""


def build_layer_record(layer: Layer) -> dict:
    """Build a layer record from a Layer model instance."""
    dataset = layer.dataset
    workspace = dataset.workspace or ""
    layer_type = dataset.layer_type or ""
    layer_name = layer.layer_name or ""
    gee_asset_path = layer.gee_asset_path or ""

    # Get style URLs from dataset misc
    style_url = ""
    sld_url = ""
    if dataset.misc:
        style_url = dataset.misc.get("style_url", "")
        sld_url = dataset.misc.get("sld_url", "")

    # Generate layer URL based on type
    if layer_type in [LayerType.VECTOR, LayerType.POINT]:
        layer_url = get_url(workspace, layer_name)
    elif layer_type == LayerType.RASTER:
        layer_url = raster_tiff_download_url(workspace, layer_name)
    else:
        layer_url = ""

    service_details = infer_service_details(layer_url)
    qgis_provider = infer_qgis_provider(layer_type, service_details["service"])

    # Build state, district, tehsil names
    state_name = layer.state.state_name if layer.state else ""
    district_name = layer.district.district_name if layer.district else ""
    tehsil_name = layer.block.tehsil_name if layer.block else ""

    return {
        "state": state_name.lower() if state_name else "",
        "district": district_name.lower() if district_name else "",
        "tehsil": tehsil_name.lower() if tehsil_name else "",
        "dataset_name": dataset.name or "",
        "layer_name": layer_name,
        "layer_type": layer_type,
        "layer_version": layer.layer_version or "",
        "layer_url": layer_url,
        "style_url": style_url,
        "sld_url": sld_url,
        "style_format": infer_style_format(style_url),
        "sld_format": "SLD" if sld_url else "",
        "gee_asset_path": gee_asset_path,
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


class Command(BaseCommand):
    help = "Export a GeoNode and QGIS ready Core-Stack layer manifest from the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            help="State name for filtering (case-insensitive)",
        )
        parser.add_argument(
            "--district",
            help="District name for filtering (case-insensitive)",
        )
        parser.add_argument(
            "--tehsil",
            help="Tehsil/block name for filtering (case-insensitive)",
        )
        parser.add_argument(
            "--all-active-locations",
            action="store_true",
            help="Export manifest for all locations that have layers",
        )
        parser.add_argument(
            "--max-locations",
            type=int,
            help="Limit number of locations when using --all-active-locations",
        )
        parser.add_argument(
            "--output",
            type=Path,
            help="Output JSON file path",
        )
        parser.add_argument(
            "--csv-output",
            type=Path,
            help="Output CSV file path for flattened layer records",
        )
        parser.add_argument(
            "--exclude-keywords",
            type=str,
            default="run_off,evapotranspiration,precipitation",
            help="Comma-separated keywords to exclude from export",
        )

    def handle(self, *args, **options):
        state_name = options.get("state")
        district_name = options.get("district")
        tehsil_name = options.get("tehsil")
        all_active = options.get("all_active_locations")
        max_locations = options.get("max_locations")
        output_path = options.get("output")
        csv_output_path = options.get("csv_output")
        exclude_keywords = options.get("exclude_keywords", "").split(",")

        # Build base queryset
        layers_qs = Layer.objects.select_related(
            "dataset", "state", "district", "block"
        ).filter(is_sync_to_geoserver=True)

        # Filter by location if specified
        locations = []
        if all_active:
            # Get all unique locations with synced layers
            locations = (
                layers_qs.values("state__state_name", "district__district_name", "block__tehsil_name")
                .distinct()
            )
            locations = [
                {
                    "state": loc["state__state_name"],
                    "district": loc["district__district_name"],
                    "tehsil": loc["block__tehsil_name"],
                }
                for loc in locations
            ]
            if max_locations:
                locations = locations[:max_locations]
        elif state_name and district_name and tehsil_name:
            locations = [{"state": state_name, "district": district_name, "tehsil": tehsil_name}]
        else:
            if state_name or district_name or tehsil_name:
                raise CommandError(
                    "Provide --state, --district, and --tehsil together, or use --all-active-locations"
                )
            locations = [{"state": "", "district": "", "tehsil": ""}]

        # Collect all layer records
        all_records = []

        for loc in locations:
            # Build filters for this location
            filters = Q(is_sync_to_geoserver=True)
            if loc.get("state"):
                filters &= Q(state__state_name__iexact=loc["state"])
            if loc.get("district"):
                filters &= Q(district__district_name__iexact=loc["district"])
            if loc.get("tehsil"):
                filters &= Q(block__tehsil_name__iexact=loc["tehsil"])

            # Apply exclusion filters
            for keyword in exclude_keywords:
                if keyword.strip():
                    filters &= ~Q(layer_name__icontains=keyword.strip())

            location_layers = layers_qs.filter(filters).order_by("layer_name", "-layer_version")

            # Deduplicate by layer_name (keep latest version)
            seen_layers = {}
            for layer in location_layers:
                name = layer.layer_name.lower()
                if name not in seen_layers:
                    seen_layers[name] = layer
                else:
                    # Compare versions
                    current_version = float(seen_layers[name].layer_version or 0)
                    new_version = float(layer.layer_version or 0)
                    if new_version > current_version:
                        seen_layers[name] = layer

            for layer in seen_layers.values():
                record = build_layer_record(layer)
                all_records.append(record)

        # Build summary statistics
        unique_wms_urls = sorted(set(r["wms_url"] for r in all_records if r.get("wms_url")))
        unique_workspaces = sorted(set(r["workspace"] for r in all_records if r.get("workspace")))

        layer_type_counts = {}
        for record in all_records:
            lt = record.get("layer_type", "unknown") or "unknown"
            layer_type_counts[lt] = layer_type_counts.get(lt, 0) + 1

        # Build manifest
        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "django_database",
            "scope": {
                "all_active_locations": all_active,
                "requested_state": state_name.lower() if state_name else "",
                "requested_district": district_name.lower() if district_name else "",
                "requested_tehsil": tehsil_name.lower() if tehsil_name else "",
                "location_count": len(locations),
            },
            "summary": {
                "layer_count": len(all_records),
                "layer_type_counts": layer_type_counts,
                "unique_workspaces": unique_workspaces,
                "unique_wms_urls": unique_wms_urls,
            },
            "locations": locations,
            "layers": all_records,
        }

        # Output JSON
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            import json

            output_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            self.stdout.write(self.style.SUCCESS(f"Manifest exported to {output_path}"))
        else:
            import json

            self.stdout.write(json.dumps(manifest, indent=2))

        # Output CSV if requested
        if csv_output_path:
            import csv

            csv_output_path.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = []
            for record in all_records:
                for key in record.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

            with csv_output_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in all_records:
                    writer.writerow(record)

            self.stdout.write(self.style.SUCCESS(f"CSV exported to {csv_output_path}"))

        self.stdout.write(
            self.style.SUCCESS(f"Exported {len(all_records)} layers from {len(locations)} locations")
        )
