import threading
import time

import ee
from django.conf import settings

from .gee_style_service import get_gee_style
from utilities.gee_utils import ee_initialize

_lock = threading.Lock()
_initialized = False
_tile_cache = {}
_STYLE_RENDERER_VERSION = 4


def get_gee_tile_url(layer):
    return get_gee_map_config(layer)["tile_url"]


def get_gee_map_config(layer):
    ee_initialize()

    ttl = getattr(settings, "LAYER_COMPARISON_GEE_CACHE_SECONDS", 1800)
    key = (
        _STYLE_RENDERER_VERSION,
        layer.get("gee_asset_id"),
        layer.get("gee_asset_type"),
        repr(layer.get("gee_visualization") or {}),
        layer.get("gee_style_url"),
        layer.get("gee_collection_method"),
    )

    now = time.time()
    cached = _tile_cache.get(key)
    if cached and cached["expires"] > now:
        return {"tile_url": cached["tile_url"], "bounds": cached["bounds"]}

    asset_id = layer["gee_asset_id"]
    asset_type = layer.get("gee_asset_type", "IMAGE").upper()
    visualization = layer.get("gee_visualization") or {}
    qml_style = get_gee_style(layer.get("gee_style_url", ""))

    is_vector = asset_type in {"VECTOR", "FEATURE_COLLECTION", "TABLE"}
    if is_vector:
        collection = ee.FeatureCollection(asset_id)
        image, vector_visualization = _paint_vector_style(collection, qml_style)
        visualization = {**visualization, **vector_visualization}
        geometry = collection.geometry()
    elif asset_type == "IMAGE_COLLECTION":
        collection = ee.ImageCollection(asset_id)
        method = layer.get("gee_collection_method", "mosaic")
        if method == "median":
            image = collection.median()
        elif method == "first":
            image = ee.Image(collection.first())
        else:
            image = collection.mosaic()
        geometry = image.geometry()
    else:
        image = ee.Image(asset_id)
        geometry = image.geometry()

    transparent_values = qml_style.get("_transparent_values", [])
    is_discrete_raster = not is_vector and qml_style.get("_discrete")
    qml_visualization = (
        {}
        if is_vector
        else {
            key: value
            for key, value in qml_style.items()
            if not key.startswith("_") and key != "renderer"
        }
    )
    visualization = {**visualization, **qml_visualization}
    if not is_vector and qml_style.get("_band"):
        image = image.select(qml_style["_band"] - 1)
    if not is_vector and (qml_style.get("palette") or is_discrete_raster):
        visualization.pop("bands", None)

    if transparent_values and not is_vector and not is_discrete_raster:
        visible_mask = image.mask()
        for value in transparent_values:
            visible_mask = visible_mask.And(image.neq(value))
        image = image.updateMask(visible_mask)

    if is_discrete_raster:
        image = _render_discrete_raster(image, qml_style)
        visualization = None

    map_info = image.getMapId(visualization) if visualization else image.getMapId()
    tile_url = map_info["tile_fetcher"].url_format
    try:
        bounds = geometry.bounds().getInfo()
    except Exception:
        bounds = None

    config = {"tile_url": tile_url, "bounds": bounds}
    _tile_cache[key] = {**config, "expires": now + ttl}
    return config


def _paint_vector_style(collection, style):
    categorized = style.get("renderer") == "categorized"
    categories = style.get("categories", [])
    field = style.get("field")

    if categorized and field and categories:
        palette = [category["color"] for category in categories]
        painted = ee.Image(0).byte()
        for index, category in enumerate(categories):
            matching_features = collection.filter(
                ee.Filter.eq(field, category["value"])
            )
            painted = painted.paint(matching_features, index)

        coverage = ee.Image(0).byte().paint(collection, 1)
        return painted.updateMask(coverage), {
            "min": 0,
            "max": max(len(palette) - 1, 1),
            "palette": palette,
        }

    color = style.get("color", "#3388ff")
    painted = ee.Image(0).byte().paint(collection, 1)
    return painted.updateMask(painted), {"min": 0, "max": 1, "palette": [color]}


def _render_discrete_raster(image, style):
    visible_mask = image.mask()
    for value in style.get("_transparent_values", []):
        visible_mask = visible_mask.And(image.neq(value))

    return image.updateMask(visible_mask).visualize(
        min=style["min"],
        max=style["max"],
        palette=style["palette"],
    )
