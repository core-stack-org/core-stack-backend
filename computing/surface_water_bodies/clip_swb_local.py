from __future__ import annotations

from typing import Any

import geopandas as gpd
import shapely
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union


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
    gdf = gpd.read_file(swb_path, bbox=(minx, miny, maxx, maxy), engine="pyogrio")
    if gdf.empty:
        return gdf

    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    clipped = gdf[gdf.geometry.intersects(roi)]
    if clipped.empty:
        return clipped

    clipped = clipped.copy()
    clipped["geometry"] = clipped.geometry.intersection(roi)
    return clipped[~clipped.geometry.is_empty].reset_index(drop=True)
