import csv
import io
import logging
from functools import lru_cache
from urllib.request import Request, urlopen

from computing.models import Layer, LayerMapping
from computing.stac_layer_resolution import format_geoserver_name
from computing.STAC_specs.constants import LAYER_MAP_GITHUB_URL
from stats_generator.models import LayerInfo

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _mapping_rows_from_sheet():
    request = Request(
        LAYER_MAP_GITHUB_URL,
        headers={"Accept": "text/csv", "User-Agent": "core-stack-layer-comparison"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            content = response.read(2 * 1024 * 1024 + 1)
        if len(content) > 2 * 1024 * 1024:
            raise ValueError("Layer mapping sheet exceeds the supported size.")
        return tuple(csv.DictReader(io.StringIO(content.decode("utf-8-sig"))))
    except Exception:
        logger.exception("Could not load layer mapping sheet for comparison")
        return ()


def _mapping_from_sheet(layer):
    dataset_name = (layer.dataset.name or "").strip()
    for row in _mapping_rows_from_sheet():
        if (row.get("db_dataset_name") or "").strip() != dataset_name:
            continue
        template = (row.get("geoserver_layer_name") or "").strip()
        if template and format_geoserver_name(template, layer) == layer.layer_name:
            return row
        if (row.get("layer_name") or "").strip() == layer.layer_name:
            return row
    return None


def _mapping_for_layer(layer):
    """
    LayerMapping describes how a saved Layer maps to its canonical layer
    and GeoServer publication. Match using dataset name and layer name.
    """
    mappings = LayerMapping.objects.filter(db_dataset_name=layer.dataset.name)

    mapping = mappings.filter(geoserver_layer_name=layer.layer_name).first()

    if mapping:
        return mapping

    mapping = mappings.filter(ee_layer_name=layer.layer_name).first()

    if mapping:
        return mapping

    mapping = mappings.filter(layer_name=layer.layer_name).first()
    if mapping:
        return mapping

    for candidate in mappings.exclude(geoserver_layer_name=""):
        if format_geoserver_name(candidate.geoserver_layer_name, layer) == layer.layer_name:
            return candidate

    # Safe fallback only if this dataset has exactly one mapping.
    if mappings.count() == 1:
        return mappings.first()

    return _mapping_from_sheet(layer)


def _layer_config(layer):
    mapping = _mapping_for_layer(layer)

    workspace = (
        (getattr(mapping, "geoserver_workspace_name", "") if mapping else "")
        or (mapping.get("geoserver_workspace_name", "") if isinstance(mapping, dict) else "")
        or layer.dataset.workspace
        or ""
    )

    mapped_geoserver_layer = (
        format_geoserver_name(
            mapping.get("geoserver_layer_name", "")
            if isinstance(mapping, dict)
            else getattr(mapping, "geoserver_layer_name", ""),
            layer,
        )
        if mapping
        else ""
    )
    geoserver_layer = (
        mapped_geoserver_layer
        or layer.layer_name
        or ""
    )

    layer_info = None
    if workspace and geoserver_layer:
        layer_info = (
            LayerInfo.objects.filter(
                workspace=workspace,
                layer_name=geoserver_layer,
            )
            .values("style_name", "layer_desc")
            .first()
        )

    style = (
        (layer_info.get("style_name") if layer_info else None)
        or layer.dataset.style_name
        or ""
    )

    gee_asset_id = layer.gee_asset_path or ""
    gee_available = bool(
        gee_asset_id
        and gee_asset_id.strip().lower() not in {"not available", "na", "none", "null"}
    )

    # is_sync_to_geoserver is the existing Layer-level publication flag.
    local_available = bool(layer.is_sync_to_geoserver and workspace and geoserver_layer)

    misc = layer.misc or {}
    dataset_misc = layer.dataset.misc or {}

    visualization = (
        misc.get("gee_visualization")
        or misc.get("gee_visualization_params")
        or misc.get("visualization")
        or misc.get("vis_params")
        or dataset_misc.get("gee_visualization")
        or dataset_misc.get("gee_visualization_params")
        or dataset_misc.get("visualization")
        or dataset_misc.get("vis_params")
        or {}
    )

    if not isinstance(visualization, dict):
        visualization = {}

    asset_type = (
        misc.get("gee_asset_type")
        or dataset_misc.get("gee_asset_type")
        or (
            "FEATURE_COLLECTION"
            if mapping
            and (
                mapping.get("layer_type")
                if isinstance(mapping, dict)
                else mapping.layer_type
            )
            == "vector"
            else "IMAGE"
        )
    )

    def mapping_value(field, default=""):
        if isinstance(mapping, dict):
            return mapping.get(field, default)
        return getattr(mapping, field, default) if mapping else default

    mapped_layer_type = mapping_value("layer_type").strip().lower()

    return {
        "id": layer.id,
        "layer_type": mapped_layer_type,
        "name": mapping_value("display_name")
        or layer.layer_name
        or layer.dataset.name
        or f"Layer {layer.id}",
        "theme": mapping_value("theme") or "Other",
        "dataset_name": layer.dataset.name,
        "layer_name": layer.layer_name,
        "layer_version": layer.layer_version,
        "gee_asset_id": gee_asset_id,
        "gee_asset_type": asset_type,
        "gee_collection_method": misc.get("gee_collection_method", "mosaic"),
        "gee_visualization": visualization,
        "gee_style_url": mapping_value("style_file_url"),
        "gee_available": gee_available,
        "workspace": workspace,
        "geoserver_layer": geoserver_layer,
        "style": style,
        "local_available": local_available,
        "is_sync_to_geoserver": layer.is_sync_to_geoserver,
        "description": (layer_info.get("layer_desc", "") if layer_info else "") or "",
    }


def get_layers_for_location(state_id, district_id, block_id, layer_ids=None):
    """
    Discover every Layer row for the exact administrative selection.
    Missing GEE/GeoServer counterparts remain in the response with availability
    flags so the dashboard can identify incomplete migration outputs.
    """
    queryset = (
        Layer.objects.filter(
            state_id=state_id,
            district_id=district_id,
            block_id=block_id,
        )
        .select_related("dataset")
        .order_by("dataset__name", "layer_name", "layer_version")
    )

    if layer_ids is not None:
        queryset = queryset.filter(id__in=layer_ids)

    groups = {}
    for layer in queryset:
        config = _layer_config(layer)
        if config["layer_type"] != "raster":
            continue
        groups.setdefault(config["theme"], []).append(config)

    return [
        {"name": theme, "layers": layers}
        for theme, layers in sorted(groups.items(), key=lambda item: item[0].lower())
    ]
