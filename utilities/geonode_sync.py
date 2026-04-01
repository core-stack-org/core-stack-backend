#!/usr/bin/env python
"""Sync Core-Stack layers to GeoNode via Remote Services.

This utility registers Core-Stack layers from GeoServer into GeoNode using the
Remote Services approach. This avoids duplicating data and keeps GeoServer as the
authoritative source.

Usage:
    # Register a single layer
    python utilities/geonode_sync.py --layer "mws:mws_bihar_hilsa"

    # Register all layers from a manifest
    python utilities/geonode_sync.py --manifest data/manifests/nalanda_hilsa.json

    # Register all active locations
    python utilities/geonode_sync.py --all-active --api-key "$CORE_STACK_API_KEY"

    # Dry run mode (show what would be registered)
    python utilities/geonode_sync.py --manifest data/manifests/nalanda_hilsa.json --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests


DEFAULT_GEONODE_URL = os.environ.get("GEONODE_URL", "https://geonode.core-stack.org")
DEFAULT_GEONODE_USER = os.environ.get("GEONODE_USER", "admin")
DEFAULT_GEONODE_PASSWORD = os.environ.get("GEONODE_PASSWORD", "")


class GeoNodeSyncError(RuntimeError):
    """Raised when sync operations fail."""
    pass


def get_geoserver_root(url: str) -> str:
    """Extract GeoServer root URL from a layer URL."""
    parsed = urlparse(url)
    path = parsed.path or ""
    marker_index = path.find("/geoserver")
    if marker_index == -1:
        return f"{parsed.scheme}://{parsed.netloc}"
    prefix = path[: marker_index + len("/geoserver")]
    return f"{parsed.scheme}://{parsed.netloc}{prefix}"


def parse_layer_url(layer_url: str) -> Dict[str, str]:
    """Parse layer URL to extract workspace and layer name."""
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(layer_url)
    query = parse_qs(parsed.query)

    workspace = ""
    layer_name = ""

    if "typeName" in query and query["typeName"]:
        resource = query["typeName"][0]
        if ":" in resource:
            workspace, layer_name = resource.split(":", 1)
    elif "CoverageId" in query and query["CoverageId"]:
        resource = query["CoverageId"][0]
        if ":" in resource:
            workspace, layer_name = resource.split(":", 1)
    elif "layers" in query and query["layers"]:
        resource = query["layers"][0]
        if ":" in resource:
            workspace, layer_name = resource.split(":", 1)

    return {"workspace": workspace, "layer_name": layer_name}


def get_geoserver_capabilities_url(geoserver_root: str, workspace: str) -> str:
    """Get WMS capabilities URL for a workspace."""
    return f"{geoserver_root}/{workspace}/wms?service=WMS&request=GetCapabilities"


def get_auth_token(geonode_url: str, username: str, password: str) -> str:
    """Authenticate with GeoNode and get API token."""
    session = requests.Session()

    # Try to get session token first
    login_url = f"{geonode_url}/accounts/login/"
    try:
        response = session.get(login_url, timeout=30)
        csrf_token = session.cookies.get("csrftoken", "")

        # Attempt login
        login_data = {
            "username": username,
            "password": password,
            "csrfmiddlewaretoken": csrf_token,
            "this_is_the_login_form": "1",
        }
        response = session.post(login_url, data=login_data, timeout=30)

        if response.status_code == 200 and "csrfmiddlewaretoken" in response.text:
            # Try to get API key from admin profile
            profile_url = f"{geonode_url}/api/users"
            response = session.get(profile_url, timeout=30)

            # For now, return session-based auth indicator
            return "session_auth"
    except Exception as e:
        print(f"Login attempt failed: {e}")

    return "session_auth"


def create_remote_service(
    session: requests.Session,
    geonode_url: str,
    base_url: str,
    name: str,
    type: str = "WMS",
    method: str = "remote",
) -> Optional[Dict[str, Any]]:
    """Create a remote service in GeoNode."""
    # Get CSRF token
    response = session.get(f"{geonode_url}/", timeout=30)
    csrf_token = session.cookies.get("csrftoken", "")

    service_data = {
        "name": name,
        "base_url": base_url,
        "service_type": type,
        "method": method,
        "description": f"Remote service for {name}",
        "enabled": True,
    }

    headers = {
        "X-CSRFToken": csrf_token,
        "Referer": f"{geonode_url}/",
    }

    try:
        # Try the newer API endpoint
        response = session.post(
            f"{geonode_url}/api/remoteservices",
            data=service_data,
            headers=headers,
            timeout=60,
        )

        if response.status_code in (200, 201):
            return response.json()
        elif response.status_code == 409:
            # Service already exists, try to get it
            print(f"Remote service '{name}' already exists, attempting to update...")
            return {"name": name, "base_url": base_url}
        else:
            print(f"Failed to create remote service: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as e:
        print(f"Error creating remote service: {e}")
        return None


def harvest_remote_service(
    session: requests.Session,
    geonode_url: str,
    service_id: str,
) -> bool:
    """Harvest layers from a remote service."""
    # Get CSRF token
    response = session.get(f"{geonode_url}/", timeout=30)
    csrf_token = session.cookies.get("csrftoken", "")

    headers = {
        "X-CSRFToken": csrf_token,
        "Referer": f"{geonode_url}/",
    }

    try:
        # Trigger harvest job
        response = session.post(
            f"{geonode_url}/api/harvesters/",
            data={
                "remote_service": service_id,
                "action": "harvest",
            },
            headers=headers,
            timeout=120,
        )

        if response.status_code in (200, 201):
            print(f"Harvest triggered for service {service_id}")
            return True
        else:
            print(f"Failed to trigger harvest: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error triggering harvest: {e}")
        return False


def sync_single_layer(
    session: requests.Session,
    geonode_url: str,
    layer_info: Dict[str, Any],
    dry_run: bool = False,
) -> bool:
    """Sync a single layer to GeoNode."""
    layer_url = layer_info.get("layer_url", "")
    workspace = layer_info.get("workspace", "")
    layer_name = layer_info.get("layer_name", "")
    dataset_name = layer_info.get("dataset_name", "")
    layer_type = layer_info.get("layer_type", "")

    if not layer_url:
        print("No layer URL provided, skipping")
        return False

    # Parse the layer URL
    parsed = parse_layer_url(layer_url)
    workspace = workspace or parsed.get("workspace", "")
    layer_name = layer_name or parsed.get("layer_name", "")

    if not workspace or not layer_name:
        print(f"Could not parse workspace/layer from URL: {layer_url}")
        return False

    # Determine service type
    service_type = "WMS"
    if layer_type == "raster" or "wcs" in layer_url.lower():
        service_type = "WCS"
    elif layer_type in ("vector", "point"):
        service_type = "WMS"  # Can also use WFS

    # Get base URL
    geoserver_root = get_geoserver_root(layer_url)
    base_url = f"{geoserver_root}/{workspace}/wms"

    service_name = f"{workspace}_{layer_name}"

    if dry_run:
        print(f"[DRY RUN] Would register: {service_name} -> {base_url}")
        return True

    # Create or update remote service
    service = create_remote_service(session, geonode_url, base_url, service_name, service_type)

    if service:
        print(f"Registered remote service: {service_name}")
        # Note: Full harvesting would require admin access and is typically
        # handled through the GeoNode admin UI or scheduled tasks
        return True

    return False


def load_manifest(manifest_path: Path) -> Dict[str, Any]:
    """Load manifest from JSON file."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    content = manifest_path.read_text(encoding="utf-8")
    return json.loads(content)


def export_and_sync(
    geonode_url: str,
    api_key: str,
    api_base_url: str,
    state: Optional[str],
    district: Optional[str],
    tehsil: Optional[str],
    all_active: bool,
    max_locations: int,
    dry_run: bool,
) -> Dict[str, Any]:
    """Export manifest from API and return layers for sync."""
    # Import the manifest export utility
    import requests as req

    from utilities.core_stack_layer_manifest import (
        export_manifest,
    )

    manifest = export_manifest(
        api_base_url=api_base_url,
        api_key=api_key,
        state=state,
        district=district,
        tehsil=tehsil,
        all_active_locations=all_active,
        max_locations=max_locations,
        timeout_seconds=60,
    )

    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync Core-Stack layers to GeoNode via Remote Services"
    )
    parser.add_argument(
        "--geonode-url",
        default=DEFAULT_GEONODE_URL,
        help=f"GeoNode base URL. Default: {DEFAULT_GEONODE_URL}",
    )
    parser.add_argument(
        "--username",
        default=DEFAULT_GEONODE_USER,
        help=f"GeoNode username. Default: {DEFAULT_GEONODE_USER}",
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_GEONODE_PASSWORD,
        help="GeoNode password. Defaults to GEONODE_PASSWORD env var.",
    )
    parser.add_argument(
        "--layer",
        help="Single layer identifier (format: workspace:layer_name)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Path to layer manifest JSON file",
    )
    parser.add_argument(
        "--api-base-url",
        default="https://geoserver.core-stack.org/api/v1",
        help="Core-Stack API base URL",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("CORE_STACK_API_KEY"),
        help="Core-Stack API key. Defaults to CORE_STACK_API_KEY env var.",
    )
    parser.add_argument(
        "--state",
        help="State name (for direct API export)",
    )
    parser.add_argument(
        "--district",
        help="District name (for direct API export)",
    )
    parser.add_argument(
        "--tehsil",
        help="Tehsil name (for direct API export)",
    )
    parser.add_argument(
        "--all-active",
        action="store_true",
        help="Export and sync all active locations",
    )
    parser.add_argument(
        "--max-locations",
        type=int,
        help="Limit number of locations when using --all-active",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without making changes",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # Validate arguments
    has_layer = bool(args.layer)
    has_manifest = bool(args.manifest)
    has_api_export = args.state or args.district or args.tehsil or args.all_active

    if not (has_layer or has_manifest or has_api_export):
        parser.error("Provide --layer, --manifest, or API export options")

    if has_api_export and not args.api_key:
        parser.error("API key is required for API export. Set CORE_STACK_API_KEY.")

    # Initialize session
    session = requests.Session()

    # Handle single layer
    if has_layer:
        if ":" not in args.layer:
            parser.error("--layer must be in format workspace:layer_name")

        workspace, layer_name = args.layer.split(":", 1)
        layer_info = {
            "workspace": workspace,
            "layer_name": layer_name,
            "layer_url": f"https://geoserver.core-stack.org/{workspace}/wms?service=WMS&layers={workspace}:{layer_name}",
            "dataset_name": workspace,
            "layer_type": "vector",
        }

        success = sync_single_layer(
            session,
            args.geonode_url,
            layer_info,
            args.dry_run,
        )
        return 0 if success else 1

    # Handle manifest file
    if has_manifest:
        manifest = load_manifest(args.manifest)
        layers = manifest.get("layers", [])

        print(f"Processing {len(layers)} layers from manifest...")

        synced = 0
        for layer_info in layers:
            try:
                success = sync_single_layer(
                    session,
                    args.geonode_url,
                    layer_info,
                    args.dry_run,
                )
                if success:
                    synced += 1
            except Exception as e:
                print(f"Error syncing layer: {e}")

        print(f"Synced {synced} of {len(layers)} layers")
        return 0

    # Handle API export
    if has_api_export:
        manifest = export_and_sync(
            geonode_url=args.geonode_url,
            api_key=args.api_key,
            api_base_url=args.api_base_url,
            state=args.state,
            district=args.district,
            tehsil=args.tehsil,
            all_active=args.all_active,
            max_locations=args.max_locations or 25,
            dry_run=args.dry_run,
        )

        layers = manifest.get("layers", [])
        print(f"Processing {len(layers)} layers from API export...")

        synced = 0
        for layer_info in layers:
            try:
                success = sync_single_layer(
                    session,
                    args.geonode_url,
                    layer_info,
                    args.dry_run,
                )
                if success:
                    synced += 1
            except Exception as e:
                print(f"Error syncing layer: {e}")

        print(f"Synced {synced} of {len(layers)} layers")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
