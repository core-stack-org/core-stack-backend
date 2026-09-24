from __future__ import annotations

import logging
import os
from typing import Any

import geopandas as gpd
import shapely
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


def _to_geom(roi: Any) -> BaseGeometry:
    """Normalize a GeoJSON ROI payload into a valid Shapely geometry."""
    if isinstance(roi, BaseGeometry):
        geometry = roi
    elif isinstance(roi, dict):
        roi_type = roi.get("type")
        if roi_type == "Feature":
            geometry = shape(roi["geometry"])
        elif roi_type == "FeatureCollection":
            features = roi.get("features") or []
            if not features:
                raise ValueError("Empty FeatureCollection")
            geometry = unary_union([shape(feature["geometry"]) for feature in features])
        else:
            geometry = shape(roi)
    else:
        raise ValueError("ROI must be a GeoJSON object")

    if not geometry.is_valid:
        geometry = shapely.make_valid(geometry)
    if geometry.is_empty:
        raise ValueError("Empty ROI geometry")
    return geometry


def _clip_gdf(swb_path: str, roi: BaseGeometry) -> gpd.GeoDataFrame:
    """Clip SWB features against the ROI after a bbox-prefiltered read."""
    minx, miny, maxx, maxy = roi.bounds
    bbox = (minx, miny, maxx, maxy)
    logger.info("Reading SWB source for clipping: path=%s bbox=%s", swb_path, bbox)

    previous_max_obj_size = os.environ.get("OGR_GEOJSON_MAX_OBJ_SIZE")
    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
    try:
        gdf = gpd.read_file(swb_path, bbox=bbox, engine="pyogrio")
    finally:
        if previous_max_obj_size is None:
            os.environ.pop("OGR_GEOJSON_MAX_OBJ_SIZE", None)
        else:
            os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = previous_max_obj_size

    if gdf.empty:
        logger.info("No SWB features found in bbox for source=%s", swb_path)
        return gdf

    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    clipped = gdf[gdf.geometry.intersects(roi)]
    if clipped.empty:
        logger.info("No SWB features intersect ROI after bbox read for source=%s", swb_path)
        return clipped

    clipped = clipped.copy()
    clipped["geometry"] = clipped.geometry.intersection(roi)
    clipped = clipped[~clipped.geometry.is_empty].reset_index(drop=True)
    logger.info("Finished SWB clipping: retained_features=%s", len(clipped))
    return clipped
