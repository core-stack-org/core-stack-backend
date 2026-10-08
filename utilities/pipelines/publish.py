"""GeoServer publishing helpers for local pipeline GeoPackage outputs."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from xml.sax.saxutils import escape

import requests


class GeoServerPublishError(RuntimeError):
    """Raised when GeoServer publication or verification fails."""


@dataclass(frozen=True)
class GeoServerPublishResult:
    layer_name: str
    workspace: str
    store_name: str
    gpkg_path: str
    source_layer: str
    published_feature_type: str
    upload_response: dict[str, Any]
    publish_response: dict[str, Any]
    wfs_url: str
    wms_url: str
    wfs_verified: bool
    source_feature_count: int
    wfs_feature_count: int | None
    wfs_properties: list[str]
    property_count: int


def _geoserver_settings() -> tuple[str, str, str]:
    from nrm_app.settings import GEOSERVER_PASSWORD, GEOSERVER_URL, GEOSERVER_USERNAME

    return GEOSERVER_URL.rstrip("/"), GEOSERVER_USERNAME, GEOSERVER_PASSWORD


def _auth() -> tuple[str, str]:
    _, username, password = _geoserver_settings()
    return username, password


def _rest_url(path: str) -> str:
    base_url, _, _ = _geoserver_settings()
    return f"{base_url}/{path.lstrip('/')}"


def _request(
    method: str,
    path: str,
    *,
    ok: tuple[int, ...],
    timeout: int = 120,
    **kwargs: Any,
) -> requests.Response:
    response = requests.request(
        method,
        _rest_url(path),
        auth=_auth(),
        timeout=timeout,
        **kwargs,
    )
    if response.status_code not in ok:
        message = response.text or response.content.decode("utf-8", errors="replace")
        raise GeoServerPublishError(
            f"GeoServer {method.upper()} {path} failed with HTTP "
            f"{response.status_code}: {message[:1000]}"
        )
    return response


def geoserver_wfs_url(workspace: str, layer_name: str) -> str:
    """Build a WFS GeoJSON URL for a published layer."""

    base_url, _, _ = _geoserver_settings()
    return (
        f"{base_url}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&"
        f"typeName={workspace}:{layer_name}&outputFormat=application/json"
    )


def geoserver_wms_url(workspace: str, layer_name: str) -> str:
    """Build a WMS GetMap base URL for a published layer."""

    base_url, _, _ = _geoserver_settings()
    return (
        f"{base_url}/{workspace}/wms?service=WMS&version=1.1.0&request=GetMap&"
        f"layers={workspace}:{layer_name}"
    )


def ensure_workspace_ready(workspace: str, *, create: bool = True) -> None:
    """Ensure a workspace exists and has a matching namespace.

    GeoServer can return an internal server error when a datastore is uploaded
    into a workspace whose namespace is missing or mismatched. We check that
    explicitly before upload so pipeline tests fail with an actionable message.
    """

    workspace_path = f"rest/workspaces/{workspace}.json"
    namespace_path = f"rest/namespaces/{workspace}.json"
    workspace_response = requests.get(
        _rest_url(workspace_path),
        auth=_auth(),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    if workspace_response.status_code == 404 and create:
        body = f"<workspace><name>{escape(workspace)}</name></workspace>"
        _request(
            "post",
            "rest/workspaces",
            ok=(201,),
            data=body,
            headers={"Content-Type": "text/xml"},
            timeout=60,
        )
    elif workspace_response.status_code != 200:
        raise GeoServerPublishError(
            f"GeoServer workspace `{workspace}` is not readable: "
            f"HTTP {workspace_response.status_code}: {workspace_response.text[:500]}"
        )

    namespace_response = requests.get(
        _rest_url(namespace_path),
        auth=_auth(),
        headers={"Accept": "application/json"},
        timeout=60,
    )
    if namespace_response.status_code == 200:
        return
    if namespace_response.status_code != 404:
        raise GeoServerPublishError(
            f"GeoServer namespace `{workspace}` is not readable: "
            f"HTTP {namespace_response.status_code}: {namespace_response.text[:500]}"
        )
    raise GeoServerPublishError(
        f"GeoServer workspace `{workspace}` exists but namespace `{workspace}` "
        "does not. Use a workspace with a matching namespace, or repair the "
        "GeoServer catalog before publishing."
    )


def delete_datastore(workspace: str, store_name: str) -> None:
    """Delete a vector datastore if present."""

    response = requests.delete(
        _rest_url(f"rest/workspaces/{workspace}/datastores/{store_name}"),
        auth=_auth(),
        params={"recurse": "true"},
        timeout=60,
    )
    if response.status_code not in (200, 202, 404):
        raise GeoServerPublishError(
            f"Could not delete GeoServer datastore `{workspace}:{store_name}`: "
            f"HTTP {response.status_code}: {response.text[:500]}"
        )


def _gpkg_feature_layers(gpkg_path: Path) -> list[str]:
    with sqlite3.connect(gpkg_path) as connection:
        return [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type = 'features' "
                "ORDER BY table_name"
            ).fetchall()
        ]


def _resolve_source_layer(gpkg_path: Path, source_layer: str | None) -> str:
    layers = _gpkg_feature_layers(gpkg_path)
    if source_layer:
        if source_layer not in layers:
            raise GeoServerPublishError(
                f"GeoPackage layer `{source_layer}` was not found in {gpkg_path}. "
                f"Available layers: {layers}"
            )
        return source_layer
    if len(layers) != 1:
        raise GeoServerPublishError(
            f"GeoPackage {gpkg_path} has {len(layers)} feature layers. "
            "Pass source_layer explicitly."
        )
    return layers[0]


def _gpkg_geometry_column(gpkg_path: Path, layer_name: str) -> str | None:
    with sqlite3.connect(gpkg_path) as connection:
        row = connection.execute(
            "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
            (layer_name,),
        ).fetchone()
    return str(row[0]) if row else None


def _gpkg_columns(gpkg_path: Path, layer_name: str) -> list[str]:
    geometry_column = _gpkg_geometry_column(gpkg_path, layer_name)
    with sqlite3.connect(gpkg_path) as connection:
        rows = connection.execute(f'PRAGMA table_info("{layer_name}")').fetchall()
    return [
        str(row[1])
        for row in rows
        if str(row[1]) != geometry_column and not bool(row[5])
    ]


def _gpkg_row_count(gpkg_path: Path, layer_name: str) -> int:
    with sqlite3.connect(gpkg_path) as connection:
        return int(connection.execute(f'SELECT COUNT(*) FROM "{layer_name}"').fetchone()[0])


def _upload_gpkg_store(gpkg_path: Path, *, workspace: str, store_name: str) -> dict[str, Any]:
    with gpkg_path.open("rb") as handle:
        response = _request(
            "put",
            f"rest/workspaces/{workspace}/datastores/{store_name}/file.gpkg"
            "?configure=none&update=overwrite",
            ok=(200, 201, 202),
            data=handle,
            headers={
                "Content-Type": "application/geopackage+sqlite3",
                "Accept": "application/json, application/xml",
            },
        )
    return {
        "status_code": response.status_code,
        "text": response.text[:500],
        "upload_format": "gpkg",
    }


def _publish_feature_type(
    *,
    workspace: str,
    store_name: str,
    source_layer: str,
    layer_name: str,
) -> dict[str, Any]:
    body = (
        "<featureType>"
        f"<name>{escape(layer_name)}</name>"
        f"<nativeName>{escape(source_layer)}</nativeName>"
        f"<title>{escape(layer_name)}</title>"
        "<srs>EPSG:4326</srs>"
        "<enabled>true</enabled>"
        "</featureType>"
    )
    response = _request(
        "post",
        f"rest/workspaces/{workspace}/datastores/{store_name}/featuretypes",
        ok=(200, 201, 202),
        data=body,
        headers={"Content-Type": "text/xml", "Accept": "application/json, application/xml"},
    )
    return {"status_code": response.status_code, "text": response.text[:500]}


def verify_wfs_layer(
    *,
    workspace: str,
    layer_name: str,
    expected_properties: list[str],
    expected_count: int | None = None,
) -> dict[str, Any]:
    """Download a WFS GeoJSON sample and verify count/property availability."""

    url = f"{geoserver_wfs_url(workspace, layer_name)}&maxFeatures=1"
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise GeoServerPublishError(
            f"WFS verification failed for `{workspace}:{layer_name}` with HTTP "
            f"{response.status_code}: {response.text[:500]}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise GeoServerPublishError(
            f"WFS verification for `{workspace}:{layer_name}` did not return JSON: "
            f"{response.text[:500]}"
        ) from exc
    features = payload.get("features") or []
    if not features and expected_count != 0:
        raise GeoServerPublishError(
            f"WFS verification for `{workspace}:{layer_name}` returned no features."
        )
    properties = list((features[0].get("properties") or {}).keys()) if features else []
    missing = [name for name in expected_properties if name not in properties]
    if missing:
        raise GeoServerPublishError(
            f"WFS verification for `{workspace}:{layer_name}` missed properties: "
            f"{missing[:20]}"
        )
    total = payload.get("totalFeatures")
    try:
        total_count = int(total) if total is not None else None
    except (TypeError, ValueError):
        total_count = None
    if expected_count is not None and total_count is not None and total_count != expected_count:
        raise GeoServerPublishError(
            f"WFS verification for `{workspace}:{layer_name}` returned "
            f"{total_count} features, expected {expected_count}."
        )
    return {
        "url": url,
        "feature_count": total_count,
        "sample_feature_count": len(features),
        "properties": properties,
    }


def publish_gpkg_layer(
    gpkg_path: str | Path,
    *,
    workspace: str,
    layer_name: str,
    overwrite: bool = True,
    source_layer: str | None = None,
) -> GeoServerPublishResult:
    """Publish a local GeoPackage layer to GeoServer and verify WFS output."""

    results = publish_gpkg_layers(
        gpkg_path,
        workspace=workspace,
        store_name=layer_name,
        layers={layer_name: source_layer},
        overwrite=overwrite,
    )
    return results[layer_name]


def publish_gpkg_layers(
    gpkg_path: str | Path,
    *,
    workspace: str,
    store_name: str,
    layers: Mapping[str, str | None],
    overwrite: bool = True,
) -> dict[str, GeoServerPublishResult]:
    """Publish several layers from one GeoPackage through one datastore.

    ``layers`` maps each public GeoServer feature-type name to its source
    GeoPackage layer. The package is uploaded once, then every feature type is
    configured and independently verified through WFS.
    """

    gpkg_path = Path(gpkg_path)
    if not gpkg_path.exists():
        raise GeoServerPublishError(f"GeoPackage does not exist: {gpkg_path}")
    if not layers:
        raise GeoServerPublishError("At least one GeoPackage layer is required")

    sources = {
        str(layer_name): _resolve_source_layer(gpkg_path, source_layer)
        for layer_name, source_layer in layers.items()
    }
    profiles = {
        layer_name: (
            _gpkg_columns(gpkg_path, source_layer),
            _gpkg_row_count(gpkg_path, source_layer),
        )
        for layer_name, source_layer in sources.items()
    }

    ensure_workspace_ready(workspace)
    if overwrite:
        delete_datastore(workspace, store_name)

    upload_response = _upload_gpkg_store(gpkg_path, workspace=workspace, store_name=store_name)
    results: dict[str, GeoServerPublishResult] = {}
    for layer_name, source_layer in sources.items():
        source_properties, source_feature_count = profiles[layer_name]
        publish_response = _publish_feature_type(
            workspace=workspace,
            store_name=store_name,
            source_layer=source_layer,
            layer_name=layer_name,
        )
        verification = verify_wfs_layer(
            workspace=workspace,
            layer_name=layer_name,
            expected_properties=source_properties,
            expected_count=source_feature_count,
        )
        results[layer_name] = GeoServerPublishResult(
            layer_name=layer_name,
            workspace=workspace,
            store_name=store_name,
            gpkg_path=gpkg_path.as_posix(),
            source_layer=source_layer,
            published_feature_type=layer_name,
            upload_response=upload_response,
            publish_response=publish_response,
            wfs_url=geoserver_wfs_url(workspace, layer_name),
            wms_url=geoserver_wms_url(workspace, layer_name),
            wfs_verified=True,
            source_feature_count=source_feature_count,
            wfs_feature_count=verification["feature_count"],
            wfs_properties=verification["properties"],
            property_count=len(verification["properties"]),
        )
    return results
