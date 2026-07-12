"""GeoLibre project, clickable HTML, and optional S3 publishing helpers.

The integration deliberately stores references to live GeoServer WFS layers
instead of copying feature data into GeoLibre artifacts.  A local pipeline run
therefore adds only a small ``.geolibre.json`` project and a clickable HTML
launcher after GeoServer publishing and before Core Stack layer registration.

The project and HTML contracts track GeoLibre upstream commit
``217a600725d35cf42692a5010b3e4e76f5331c06`` (2026-07-11), whose project
format version is ``0.2.0``.  See https://github.com/opengeos/GeoLibre.
"""

from __future__ import annotations

import html
import json
import math
import re
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

import requests

from .outputs import slug, utc_now_text
from .schema import coerce_bool
from .unicode import normalize_unicode_data, normalize_unicode_text


GEOLIBRE_REPOSITORY = "https://github.com/opengeos/GeoLibre"
GEOLIBRE_UPSTREAM_COMMIT = "217a600725d35cf42692a5010b3e4e76f5331c06"
GEOLIBRE_PROJECT_VERSION = "0.2.0"
DEFAULT_VIEWER_URL = "https://web.geolibre.app/"
DEFAULT_BASEMAP_STYLE = "https://tiles.openfreemap.org/styles/liberty"
DEFAULT_CENTER = (78.9629, 20.5937)
DEFAULT_ZOOM = 4.0
DEFAULT_DISCOVERY_TIMEOUT_SECONDS = 30.0
MAX_DISCOVERED_LAYERS_LIMIT = 100


class GeoLibreIntegrationError(RuntimeError):
    """Raised for invalid GeoLibre integration configuration or artifacts."""


@dataclass(frozen=True)
class GeoLibreAWSOptions:
    """Optional S3 publication settings.

    Credentials are intentionally absent: boto3 resolves them from the normal
    server-side AWS provider chain.  API callers should never send credentials.
    """

    enabled: bool = False
    bucket: str | None = None
    prefix: str = "geolibre/projects"
    region: str | None = None
    endpoint_url: str | None = None
    public_base_url: str | None = None
    upload_html: bool = False
    cache_control: str = "public, max-age=60"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "GeoLibreAWSOptions":
        values = dict(data or {})
        return cls(
            enabled=coerce_bool(values.get("enabled"), False),
            bucket=_optional_text(values.get("bucket")),
            prefix=str(values.get("prefix") or cls.prefix).strip("/"),
            region=_optional_text(values.get("region")),
            endpoint_url=_optional_text(values.get("endpoint_url")),
            public_base_url=_optional_text(values.get("public_base_url")),
            upload_html=coerce_bool(values.get("upload_html"), False),
            cache_control=str(values.get("cache_control") or cls.cache_control),
        )


@dataclass(frozen=True)
class GeoLibreOptions:
    """Effective settings for one pipeline's GeoLibre output step."""

    viewer_url: str = DEFAULT_VIEWER_URL
    include_tehsil_layers: bool = False
    max_layers: int = 25
    refresh_interval_seconds: int = 0
    discovery_timeout_seconds: float = DEFAULT_DISCOVERY_TIMEOUT_SECONDS
    aws: GeoLibreAWSOptions = field(default_factory=GeoLibreAWSOptions)

    @classmethod
    def from_mappings(
        cls,
        configured: Mapping[str, Any] | None,
        requested: Mapping[str, Any] | None,
    ) -> "GeoLibreOptions":
        """Merge pipeline YAML defaults with the request's ``geolibre`` block."""

        merged = dict(configured or {})
        request_values = dict(requested or {})
        configured_aws = dict(merged.get("aws") or {})
        requested_aws = dict(request_values.get("aws") or {})
        for key in (
            "include_tehsil_layers",
            "max_layers",
            "refresh_interval_seconds",
            "publish_to_aws",
            "upload_html_to_aws",
        ):
            if key in request_values:
                merged[key] = request_values[key]
        # AWS destinations are trusted deployment configuration, never
        # caller-selected infrastructure. Requests may only toggle publication
        # and the optional HTML copy.
        for key in ("enabled", "upload_html"):
            if key in requested_aws:
                configured_aws[key] = requested_aws[key]

        # Small top-level aliases keep simple API bodies simple.
        aliases = {
            "publish_to_aws": "enabled",
            "upload_html_to_aws": "upload_html",
        }
        for source, target in aliases.items():
            if source in merged:
                configured_aws[target] = merged[source]

        max_layers = _bounded_int(merged.get("max_layers"), 25, 1, MAX_DISCOVERED_LAYERS_LIMIT)
        refresh = _bounded_int(merged.get("refresh_interval_seconds"), 0, 0, 86_400)
        timeout = _bounded_float(
            merged.get("discovery_timeout_seconds"),
            DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
            1.0,
            120.0,
        )
        return cls(
            viewer_url=_safe_viewer_url(merged.get("viewer_url")),
            include_tehsil_layers=coerce_bool(merged.get("include_tehsil_layers"), False),
            max_layers=max_layers,
            refresh_interval_seconds=refresh,
            discovery_timeout_seconds=timeout,
            aws=GeoLibreAWSOptions.from_mapping(configured_aws),
        )


@dataclass(frozen=True)
class GeoServerFeatureType:
    qualified_name: str
    workspace: str
    layer_name: str
    title: str
    bbox: tuple[float, float, float, float] | None = None


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(parsed, minimum), maximum)


def _bounded_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed):
        return default
    return min(max(parsed, minimum), maximum)


def _safe_viewer_url(value: Any) -> str:
    candidate = _optional_text(value) or DEFAULT_VIEWER_URL
    parsed = urlparse(candidate)
    is_loopback = parsed.hostname in {"localhost", "127.0.0.1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and is_loopback):
        return DEFAULT_VIEWER_URL
    return candidate


def _replace_ogc_query(url: str, parameters: Iterable[tuple[str, str]]) -> str:
    parsed = urlparse(url)
    operation_keys = {
        "service",
        "request",
        "version",
        "typename",
        "typenames",
        "outputformat",
        "srsname",
        "maxfeatures",
        "count",
    }
    retained = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() not in operation_keys]
    query = urlencode([*retained, *parameters])
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, ""))


def wfs_capabilities_url(wfs_url: str) -> str:
    return _replace_ogc_query(
        wfs_url,
        (("service", "WFS"), ("version", "1.1.0"), ("request", "GetCapabilities")),
    )


def wfs_feature_url(wfs_url: str, qualified_name: str) -> str:
    """Build a stable, live GeoJSON URL while preserving non-OGC query tokens."""

    return _replace_ogc_query(
        wfs_url,
        (
            ("service", "WFS"),
            ("version", "1.0.0"),
            ("request", "GetFeature"),
            ("typeName", qualified_name),
            ("outputFormat", "application/json"),
            ("srsName", "EPSG:4326"),
        ),
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(element: ET.Element, local_name: str) -> str | None:
    for child in element.iter():
        if _local_name(child.tag) == local_name and child.text:
            return child.text.strip()
    return None


def _parse_pair(value: str | None) -> tuple[float, float] | None:
    if not value:
        return None
    try:
        numbers = [float(item) for item in re.split(r"[\s,]+", value.strip()) if item]
    except ValueError:
        return None
    if len(numbers) < 2 or not all(math.isfinite(item) for item in numbers[:2]):
        return None
    return numbers[0], numbers[1]


def _feature_bbox(feature_type: ET.Element) -> tuple[float, float, float, float] | None:
    for child in feature_type.iter():
        local = _local_name(child.tag)
        if local in {"WGS84BoundingBox", "BoundingBox"}:
            lower = _parse_pair(_child_text(child, "LowerCorner"))
            upper = _parse_pair(_child_text(child, "UpperCorner"))
            if lower and upper:
                return lower[0], lower[1], upper[0], upper[1]
        if local == "LatLongBoundingBox":
            try:
                values = tuple(
                    float(child.attrib[key])
                    for key in ("minx", "miny", "maxx", "maxy")
                )
                return values  # type: ignore[return-value]
            except (KeyError, TypeError, ValueError):
                continue
    return None


def parse_wfs_capabilities(xml_content: bytes | str) -> list[GeoServerFeatureType]:
    """Parse WFS 1.x/2.x feature names and WGS84 bounds without GIS libraries."""

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as exc:
        raise GeoLibreIntegrationError(f"Invalid WFS GetCapabilities XML: {exc}") from exc

    features: list[GeoServerFeatureType] = []
    for element in root.iter():
        if _local_name(element.tag) != "FeatureType":
            continue
        qualified_name = _child_text(element, "Name")
        if not qualified_name:
            continue
        if ":" in qualified_name:
            workspace, layer_name = qualified_name.split(":", 1)
        else:
            workspace, layer_name = "", qualified_name
        features.append(
            GeoServerFeatureType(
                qualified_name=qualified_name,
                workspace=workspace,
                layer_name=layer_name,
                title=_child_text(element, "Title") or layer_name,
                bbox=_feature_bbox(element),
            )
        )
    return features


def fetch_wfs_feature_types(
    wfs_url: str,
    *,
    timeout: float = DEFAULT_DISCOVERY_TIMEOUT_SECONDS,
) -> list[GeoServerFeatureType]:
    response = requests.get(
        wfs_capabilities_url(wfs_url),
        timeout=timeout,
        headers={"Accept": "application/xml,text/xml;q=0.9,*/*;q=0.1"},
    )
    response.raise_for_status()
    return parse_wfs_capabilities(response.content)


def _scope_suffixes(scope: Any) -> tuple[str, ...]:
    level = str(getattr(scope, "level", "") or "").lower()
    state = slug(getattr(scope, "state_name", None))
    district = slug(getattr(scope, "district_name", None))
    tehsil = slug(getattr(scope, "tehsil_name", None))
    if level in {"tehsil", "block"} and tehsil:
        exact_scope = f"{district}_{tehsil}" if district else tehsil
        return (f"_{exact_scope}",)
    if level == "district" and district:
        return (f"_{district}",)
    if level == "state" and state:
        return (f"_{state}",)
    return ()


def select_scope_feature_types(
    feature_types: Iterable[GeoServerFeatureType],
    *,
    current_qualified_name: str,
    scope: Any,
    include_scope_layers: bool,
    max_layers: int,
) -> list[GeoServerFeatureType]:
    """Select the current layer and optionally same-scope GeoServer siblings."""

    candidates = list(feature_types)
    current = next((item for item in candidates if item.qualified_name == current_qualified_name), None)
    selected: list[GeoServerFeatureType] = [current] if current else []
    if include_scope_layers:
        suffixes = _scope_suffixes(scope)
        for item in sorted(candidates, key=lambda candidate: candidate.qualified_name):
            if item.qualified_name == current_qualified_name:
                continue
            if suffixes and any(item.layer_name.endswith(suffix) for suffix in suffixes):
                selected.append(item)
            if len(selected) >= max_layers:
                break
    return selected[:max_layers]


def _union_bbox(feature_types: Iterable[GeoServerFeatureType]) -> tuple[float, float, float, float] | None:
    bounds = [item.bbox for item in feature_types if item.bbox is not None]
    if not bounds:
        return None
    return (
        min(item[0] for item in bounds),
        min(item[1] for item in bounds),
        max(item[2] for item in bounds),
        max(item[3] for item in bounds),
    )


def map_view_for_bbox(bbox: tuple[float, float, float, float] | None) -> dict[str, Any]:
    if not bbox:
        return {"center": list(DEFAULT_CENTER), "zoom": DEFAULT_ZOOM, "bearing": 0, "pitch": 0}
    minx, miny, maxx, maxy = bbox
    center = [(minx + maxx) / 2, (miny + maxy) / 2]
    lon_span = max(abs(maxx - minx), 1e-6)
    lat_span = max(abs(maxy - miny), 1e-6)
    span = max(lon_span, lat_span * 1.5)
    zoom = min(max(math.log2(360.0 / span) - 1.0, 2.0), 16.0)
    return {
        "center": [round(value, 8) for value in center],
        "zoom": round(zoom, 2),
        "bearing": 0,
        "pitch": 0,
        "bbox": [round(value, 8) for value in bbox],
    }


def _layer_color(layer_name: str) -> tuple[str, str]:
    palette = (
        ("#2563eb", "#1e3a8a"),
        ("#16a34a", "#14532d"),
        ("#ea580c", "#7c2d12"),
        ("#9333ea", "#581c87"),
        ("#0891b2", "#164e63"),
    )
    return palette[sum(layer_name.encode("utf-8")) % len(palette)]


def _project_layer(feature_type: GeoServerFeatureType, wfs_url: str, refresh_seconds: int) -> dict[str, Any]:
    layer_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"corestack-geolibre:{feature_type.qualified_name}"))
    fill, stroke = _layer_color(feature_type.layer_name)
    source_url = wfs_feature_url(wfs_url, feature_type.qualified_name)
    metadata: dict[str, Any] = {
        "service": "wfs",
        "sourceKind": "maplibre-gl-vector",
        "externalNativeLayer": True,
        "controlOwnsPaint": True,
        "identifiable": False,
        "vectorSource": "url",
        "vectorState": {
            "format": "geojson",
            "ingestMode": "table",
            "picker": True,
            "renderMode": "geojson",
            "style": {
                "fillColor": fill,
                "fillOpacity": 0.45,
                "lineColor": stroke,
                "lineWidth": 1.5,
                "circleColor": fill,
                "circleRadius": 6,
                "circleOpacity": 0.85,
            },
        },
        "corestack": {
            "geoserver_workspace": feature_type.workspace,
            "geoserver_layer": feature_type.layer_name,
            "wfs_type_name": feature_type.qualified_name,
        },
    }
    if feature_type.bbox:
        metadata["bounds"] = list(feature_type.bbox)
    if refresh_seconds > 0:
        metadata["refresh"] = {"enabled": True, "intervalMs": refresh_seconds * 1000}
    return {
        "id": layer_id,
        "name": feature_type.title or feature_type.layer_name,
        "type": "geojson",
        "source": {"type": "geojson", "url": source_url},
        "visible": True,
        "opacity": 1,
        "style": {"fillColor": fill, "strokeColor": stroke, "fillOpacity": 0.45, "strokeWidth": 1.5},
        "metadata": metadata,
        "sourcePath": source_url,
    }


def build_project(
    *,
    name: str,
    selected_layers: Iterable[GeoServerFeatureType],
    wfs_url: str,
    scope: Any,
    refresh_interval_seconds: int = 0,
    warnings: Iterable[str] = (),
) -> dict[str, Any]:
    """Build a non-redundant GeoLibre project referencing live WFS URLs."""

    layers = list(selected_layers)
    project_layers = [_project_layer(item, wfs_url, refresh_interval_seconds) for item in layers]
    return {
        "version": GEOLIBRE_PROJECT_VERSION,
        "name": name,
        "mapView": map_view_for_bbox(_union_bbox(layers)),
        "basemapStyleUrl": DEFAULT_BASEMAP_STYLE,
        "basemapVisible": True,
        "basemapOpacity": 1,
        "layers": project_layers,
        "styles": {item["id"]: item["style"] for item in project_layers},
        "metadata": {
            "generated_at_utc": utc_now_text(),
            "generated_by": "core-stack local pipeline",
            "scope": {
                "level": getattr(scope, "level", None),
                "state_name": getattr(scope, "state_name", None),
                "district_name": getattr(scope, "district_name", None),
                "tehsil_name": getattr(scope, "tehsil_name", None),
            },
            "data_contract": "live GeoServer WFS references; no feature data embedded",
            "geolibre": {
                "repository": GEOLIBRE_REPOSITORY,
                "upstream_commit": GEOLIBRE_UPSTREAM_COMMIT,
                "project_version": GEOLIBRE_PROJECT_VERSION,
            },
            "warnings": list(warnings),
        },
    }


def _viewer_with_flags(viewer_url: str) -> str:
    parsed = urlparse(_safe_viewer_url(viewer_url))
    params = parse_qsl(parsed.query, keep_blank_values=True)
    keys = {key.lower() for key, _ in params}
    if "embed" not in keys:
        params.append(("embed", "1"))
    if "welcome" not in keys:
        params.append(("welcome", "0"))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(params), parsed.fragment))


def build_clickable_html(project: Mapping[str, Any], *, title: str, viewer_url: str = DEFAULT_VIEWER_URL) -> str:
    """Build GeoLibre's supported postMessage-based standalone HTML launcher."""

    iframe_src = html.escape(_viewer_with_flags(viewer_url), quote=True)
    project_json = json.dumps(project, separators=(",", ":"), ensure_ascii=False).replace("<", "\\u003c")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{html.escape(title)}</title>
<style>
  html, body {{ margin: 0; padding: 0; height: 100%; }}
  #geolibre-frame {{ border: 0; display: block; width: 100%; height: 100vh; }}
</style>
</head>
<body>
<iframe id="geolibre-frame" src="{iframe_src}" allow="fullscreen; geolocation" allowfullscreen></iframe>
<script type="application/json" id="geolibre-project">{project_json}</script>
<script>
(function () {{
  var frame = document.getElementById("geolibre-frame");
  var project = JSON.parse(document.getElementById("geolibre-project").textContent);
  var viewerOrigin = new URL(frame.src).origin;
  var loaded = false;
  function load() {{
    if (loaded || !frame.contentWindow) return;
    loaded = true;
    frame.contentWindow.postMessage(
      {{ type: "geolibre:load-project", project: project, seq: 1 }},
      viewerOrigin
    );
  }}
  window.addEventListener("message", function (event) {{
    if (event.origin !== viewerOrigin || event.source !== frame.contentWindow) return;
    if (event.data && event.data.type === "geolibre:ready") load();
  }});
}})();
</script>
</body>
</html>
"""


def _public_object_url(base_url: str | None, key: str) -> str | None:
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/{quote(key, safe='/')}"


def _viewer_project_url(viewer_url: str, project_url: str) -> str:
    parsed = urlparse(_safe_viewer_url(viewer_url))
    params = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() != "url"]
    params.append(("url", project_url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(params), parsed.fragment))


def _scope_aws_prefix(scope: Any) -> str:
    values = (
        slug(getattr(scope, "state_name", None)),
        slug(getattr(scope, "district_name", None)),
        slug(getattr(scope, "tehsil_name", None)),
    )
    return "/".join(value for value in values if value)


def publish_artifacts_to_s3(
    *,
    project_path: Path,
    html_path: Path,
    scope: Any,
    options: GeoLibreAWSOptions,
    viewer_url: str,
    s3_client: Any = None,
) -> dict[str, Any]:
    """Upload the canonical project JSON and, only if requested, its HTML copy."""

    if not options.enabled:
        return {"enabled": False, "status": "disabled"}
    if not options.bucket:
        raise GeoLibreIntegrationError("GeoLibre AWS publishing requires geolibre.aws.bucket")
    if s3_client is None:
        try:
            import boto3
        except ImportError as exc:
            raise GeoLibreIntegrationError("Install boto3 before enabling GeoLibre AWS publishing") from exc
        client_options = {key: value for key, value in {
            "region_name": options.region,
            "endpoint_url": options.endpoint_url,
        }.items() if value}
        s3_client = boto3.client("s3", **client_options)

    key_parts = [options.prefix.strip("/"), _scope_aws_prefix(scope), project_path.name]
    project_key = "/".join(part for part in key_parts if part)
    project_response = s3_client.put_object(
        Bucket=options.bucket,
        Key=project_key,
        Body=project_path.read_bytes(),
        ContentType="application/json; charset=utf-8",
        CacheControl=options.cache_control,
    )
    public_project_url = _public_object_url(options.public_base_url, project_key)
    result: dict[str, Any] = {
        "enabled": True,
        "status": "uploaded",
        "bucket": options.bucket,
        "project_key": project_key,
        "project_s3_uri": f"s3://{options.bucket}/{project_key}",
        "project_url": public_project_url,
        "viewer_url": _viewer_project_url(viewer_url, public_project_url) if public_project_url else None,
        "project_etag": str(project_response.get("ETag", "")).strip('"') or None,
        "upload_policy": "canonical project JSON only" if not options.upload_html else "project JSON and HTML",
    }
    if options.upload_html:
        html_key = project_key.removesuffix(".geolibre.json") + ".geolibre.html"
        html_response = s3_client.put_object(
            Bucket=options.bucket,
            Key=html_key,
            Body=html_path.read_bytes(),
            ContentType="text/html; charset=utf-8",
            CacheControl=options.cache_control,
        )
        result.update(
            {
                "html_key": html_key,
                "html_s3_uri": f"s3://{options.bucket}/{html_key}",
                "html_url": _public_object_url(options.public_base_url, html_key),
                "html_etag": str(html_response.get("ETag", "")).strip('"') or None,
            }
        )
    return result


def create_geolibre_outputs(
    *,
    output_dir: str | Path,
    output_name: str,
    scope: Any,
    geoserver: Mapping[str, Any],
    geoserver_layers: Iterable[Mapping[str, Any]] | None = None,
    configured: Mapping[str, Any] | None = None,
    requested: Mapping[str, Any] | None = None,
    s3_client: Any = None,
) -> dict[str, Any]:
    """Create local GeoLibre artifacts and optionally publish the project to S3.

    This function is intentionally safe to call in the narrow gap between
    GeoServer publication and layer registration.  It returns a structured
    failure instead of raising, so a presentation-layer problem cannot discard
    valid pipeline and GeoServer outputs.
    """

    started = time.perf_counter()
    try:
        options = GeoLibreOptions.from_mappings(configured, requested)
        declared_layers = [geoserver, *(list(geoserver_layers or []))]
        unique_layers: list[Mapping[str, Any]] = []
        seen_qualified_names: set[str] = set()
        for declared in declared_layers:
            declared_workspace = _optional_text(declared.get("workspace"))
            declared_name = _optional_text(declared.get("layer_name"))
            if not (declared_workspace and declared_name):
                continue
            qualified_name = f"{declared_workspace}:{declared_name}"
            if qualified_name not in seen_qualified_names:
                unique_layers.append(declared)
                seen_qualified_names.add(qualified_name)
        primary = unique_layers[0] if unique_layers else geoserver
        wfs_url = _optional_text(primary.get("wfs_url"))
        workspace = _optional_text(primary.get("workspace"))
        layer_name = _optional_text(primary.get("layer_name"))
        if not (wfs_url and workspace and layer_name):
            raise GeoLibreIntegrationError("GeoServer result is missing wfs_url, workspace, or layer_name")
        current_qualified_name = f"{workspace}:{layer_name}"
        warnings: list[str] = []
        try:
            available = fetch_wfs_feature_types(wfs_url, timeout=options.discovery_timeout_seconds)
        except Exception as exc:
            available = []
            warnings.append(f"WFS capabilities discovery failed: {exc.__class__.__name__}: {str(exc)[:240]}")

        available_by_name = {item.qualified_name: item for item in available}
        selected: list[GeoServerFeatureType] = []
        for declared in unique_layers:
            declared_workspace = str(declared["workspace"])
            declared_name = str(declared["layer_name"])
            qualified_name = f"{declared_workspace}:{declared_name}"
            selected.append(
                available_by_name.get(qualified_name)
                or GeoServerFeatureType(
                    qualified_name=qualified_name,
                    workspace=declared_workspace,
                    layer_name=declared_name,
                    title=declared_name.replace("_", " ").title(),
                )
            )
        discovered = select_scope_feature_types(
            available,
            current_qualified_name=current_qualified_name,
            scope=scope,
            include_scope_layers=options.include_tehsil_layers,
            max_layers=options.max_layers,
        )
        selected_names = {item.qualified_name for item in selected}
        selected.extend(item for item in discovered if item.qualified_name not in selected_names)
        selected = selected[: options.max_layers]
        if options.include_tehsil_layers and len(selected) >= options.max_layers:
            warnings.append(f"GeoServer scope discovery was capped at {options.max_layers} layers")

        title = layer_name.replace("_", " ").title()
        project = build_project(
            name=title,
            selected_layers=selected,
            wfs_url=wfs_url,
            scope=scope,
            refresh_interval_seconds=options.refresh_interval_seconds,
            warnings=warnings,
        )
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        base_name = slug(output_name)
        project_path = directory / f"{base_name}.geolibre.json"
        html_path = directory / f"{base_name}.geolibre.html"
        project_path.write_text(
            json.dumps(normalize_unicode_data(project), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        html_path.write_text(
            normalize_unicode_text(
                build_clickable_html(project, title=title, viewer_url=options.viewer_url)
            ),
            encoding="utf-8",
        )

        try:
            aws = publish_artifacts_to_s3(
                project_path=project_path,
                html_path=html_path,
                scope=scope,
                options=options.aws,
                viewer_url=options.viewer_url,
                s3_client=s3_client,
            )
        except Exception as exc:
            # AWS is optional. Preserve the working local map and make the
            # cloud failure explicit instead of failing the pipeline output.
            aws = {
                "enabled": options.aws.enabled,
                "status": "upload_failed",
                "error_type": exc.__class__.__name__,
                "error": str(exc)[:500],
            }
        return {
            "ok": True,
            "status": "created",
            "project_path": project_path.resolve().as_posix(),
            "html_path": html_path.resolve().as_posix(),
            "local_viewer_url": html_path.resolve().as_uri(),
            "viewer_url": aws.get("viewer_url"),
            "layer_count": len(selected),
            "layers": [item.qualified_name for item in selected],
            "map_view": project["mapView"],
            "warnings": warnings,
            "options": asdict(options),
            "aws": aws,
            "geolibre_upstream": {
                "repository": GEOLIBRE_REPOSITORY,
                "commit": GEOLIBRE_UPSTREAM_COMMIT,
                "project_version": GEOLIBRE_PROJECT_VERSION,
            },
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": "creation_failed",
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }


def registration_metadata(result: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return compact GeoLibre fields suitable for ``Layer.misc``."""

    if not result:
        return {}
    aws = result.get("aws") if isinstance(result.get("aws"), Mapping) else {}
    return {
        "geolibre_status": result.get("status"),
        "geolibre_project_path": result.get("project_path"),
        "geolibre_html_path": result.get("html_path"),
        "geolibre_local_viewer_url": result.get("local_viewer_url"),
        "geolibre_viewer_url": result.get("viewer_url"),
        "geolibre_project_s3_uri": aws.get("project_s3_uri"),
        "geolibre_layer_count": result.get("layer_count"),
    }


def remove_geolibre_outputs(output_dir: str | Path, output_name: str) -> list[str]:
    """Remove stale local artifacts when GeoLibre output is disabled or fails."""

    removed: list[str] = []
    base = Path(output_dir) / slug(output_name)
    for path in (
        Path(f"{base.as_posix()}.geolibre.json"),
        Path(f"{base.as_posix()}.geolibre.html"),
    ):
        if path.exists():
            path.unlink()
            removed.append(path.as_posix())
    return removed
