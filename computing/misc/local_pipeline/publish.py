"""GeoServer publishing helpers for local pipeline outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GeoServerPublishResult:
    layer_name: str
    workspace: str
    gpkg_path: str
    response: Any
    wfs_url: str
    wms_url: str


def geoserver_wfs_url(workspace: str, layer_name: str) -> str:
    """Build a standard WFS GetFeature URL for a published layer."""

    return (
        "https://geoserver.core-stack.org:8443/geoserver/"
        f"{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&"
        f"typeName={workspace}:{layer_name}&outputFormat=application/json"
    )


def geoserver_wms_url(workspace: str, layer_name: str) -> str:
    """Build a standard WMS GetMap base URL for a published layer."""

    return (
        "https://geoserver.core-stack.org:8443/geoserver/"
        f"{workspace}/wms?service=WMS&version=1.1.0&request=GetMap&"
        f"layers={workspace}:{layer_name}"
    )


def publish_gpkg_layer(
    gpkg_path: str | Path,
    *,
    workspace: str,
    layer_name: str,
    overwrite: bool = True,
) -> GeoServerPublishResult:
    """Publish a GeoPackage layer using the repo's existing GeoServer utility."""

    from computing.utils import push_shape_to_geoserver

    gpkg_path = Path(gpkg_path)
    response = push_shape_to_geoserver(
        str(gpkg_path.with_suffix("")),
        store_name=layer_name,
        workspace=workspace,
        layer_name=layer_name if overwrite else None,
        file_type="gpkg",
    )
    return GeoServerPublishResult(
        layer_name=layer_name,
        workspace=workspace,
        gpkg_path=gpkg_path.as_posix(),
        response=response,
        wfs_url=geoserver_wfs_url(workspace, layer_name),
        wms_url=geoserver_wms_url(workspace, layer_name),
    )
