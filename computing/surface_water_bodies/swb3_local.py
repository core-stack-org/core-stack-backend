"""
Local (GeoDataFrame) version of swb3.py.

Adds to each SWB polygon: max stream order / catchment area, on-drainage flag,
river/canal waterbody type and intersecting village names, using the pan-India
layers on disk instead of GEE. Output property names match swb3.py.
"""

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
from rasterstats import zonal_stats

from computing.config_loader import (
    PAN_INDIA_CANAL_PATH,
    PAN_INDIA_CATCHMENT_AREA_PATH,
    PAN_INDIA_DRAINAGE_LINES_GPKG_PATH,
    PAN_INDIA_RIVER_PATH,
    PAN_INDIA_STREAM_ORDER_PATH,
    PAN_INDIA_VILLAGE_NAME_FIELD,
    PAN_INDIA_VILLAGE_PATH,
)

logger = logging.getLogger(__name__)

DEFAULT_WATERBODY_TYPE_BUFFER_M = 500
INDEXED_SUFFIXES = (".fgb", ".gpkg")


def resolve_indexed_path(path):
    """Prefer a spatially indexed sibling (.fgb/.gpkg) of a large GeoJSON."""
    path = Path(path)
    for suffix in INDEXED_SUFFIXES:
        candidate = path.with_suffix(suffix)
        if candidate.exists():
            return candidate
    return path


def read_vector_in_bounds(path, bounds, columns=None):
    """Read only features intersecting `bounds` (minx, miny, maxx, maxy; EPSG:4326)."""
    path = resolve_indexed_path(path)
    if not path.exists():
        raise FileNotFoundError(f"Vector layer not found: {path}")
    gdf = pyogrio.read_dataframe(path, bbox=tuple(bounds), columns=columns)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _padded_bounds(gdf, pad_m):
    # ~111 km per degree; generous so metric buffers never fall off the edge.
    pad = pad_m / 111000.0 * 2
    minx, miny, maxx, maxy = gdf.total_bounds
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def _require_file(path, label):
    if not Path(path).exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def add_max_stream_order_and_catchment(
    gdf,
    stream_order_path=PAN_INDIA_STREAM_ORDER_PATH,
    catchment_area_path=PAN_INDIA_CATCHMENT_AREA_PATH,
):
    """max_stream_order / max_catchment_area per polygon; 0 where no pixel (as GEE)."""
    _require_file(stream_order_path, "Stream order raster")
    _require_file(catchment_area_path, "Catchment area raster")

    geoms = list(gdf.geometry)
    for column, raster_path in (
        ("max_stream_order", stream_order_path),
        ("max_catchment_area", catchment_area_path),
    ):
        stats = zonal_stats(
            geoms, str(raster_path), stats=["max"], all_touched=True, nodata=-999
        )
        gdf[column] = [s["max"] if s and s["max"] is not None else 0 for s in stats]
    return gdf


def add_on_drainage_flag(gdf, drainage_path=PAN_INDIA_DRAINAGE_LINES_GPKG_PATH):
    _require_file(drainage_path, "Drainage lines")
    drainage = read_vector_in_bounds(
        drainage_path, _padded_bounds(gdf, 0), columns=[]
    )
    if drainage.empty:
        gdf["on_drainage_line"] = False
        return gdf
    hits = gpd.sjoin(
        gdf[["geometry"]], drainage[["geometry"]], how="inner", predicate="intersects"
    )
    gdf["on_drainage_line"] = gdf.index.isin(hits.index.unique())
    return gdf


def _load_reference(path, label, gdf, buffer_m, columns):
    try:
        ref = read_vector_in_bounds(path, _padded_bounds(gdf, max(buffer_m * 5, 5000)), columns)
    except FileNotFoundError:
        logger.warning("%s layer unavailable (%s); skipping", label, path)
        return None
    return ref


def _nearest_match(metric_gdf, metric_ref, buffer_m, columns):
    """First (nearest) reference feature within buffer_m (0 = intersecting) per polygon."""
    if metric_ref is None or metric_ref.empty:
        return pd.DataFrame(index=metric_gdf.index, columns=columns)
    joined = gpd.sjoin_nearest(
        metric_gdf[["geometry"]],
        metric_ref[columns + ["geometry"]],
        how="inner",
        max_distance=buffer_m,
        distance_col="_dist",
    )
    joined = joined.sort_values("_dist")
    joined = joined[~joined.index.duplicated(keep="first")]
    return joined[columns].reindex(metric_gdf.index)


def add_waterbody_type_flag(
    gdf,
    river_path=PAN_INDIA_RIVER_PATH,
    canal_path=PAN_INDIA_CANAL_PATH,
    buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
):
    river = _load_reference(river_path, "River", gdf, buffer_m, ["rivname", "objectid", "ripcode"])
    canal = _load_reference(canal_path, "Canal", gdf, buffer_m, ["canname", "cancode", "prjname"])

    if river is None and canal is None:
        gdf["waterbody_type"] = "unknown"
        return gdf

    metric_crs = gdf.estimate_utm_crs()
    metric_gdf = gdf[["geometry"]].to_crs(metric_crs)
    river_m = river.to_crs(metric_crs) if river is not None else None
    canal_m = canal.to_crs(metric_crs) if canal is not None else None

    river_hit = _nearest_match(metric_gdf, river_m, buffer_m, ["rivname", "objectid", "ripcode"])
    canal_hit = _nearest_match(metric_gdf, canal_m, buffer_m, ["canname", "cancode", "prjname"])

    has_river = river_hit["rivname"].notna() | river_hit["objectid"].notna()
    has_canal = canal_hit["canname"].notna() | canal_hit["cancode"].notna()
    # A river match wins; canal fields are only filled when there is no river match.
    canal_only = has_canal & ~has_river

    gdf["waterbody_type"] = "individual"
    gdf.loc[canal_only, "waterbody_type"] = "canal"
    gdf.loc[has_river, "waterbody_type"] = "river"
    gdf["waterbody_type_name"] = None
    gdf.loc[canal_only, "waterbody_type_name"] = canal_hit.loc[canal_only, "canname"]
    gdf.loc[has_river, "waterbody_type_name"] = river_hit.loc[has_river, "rivname"]
    gdf["river_objectid"] = river_hit["objectid"].where(has_river)
    gdf["rip_code"] = river_hit["ripcode"].where(has_river)
    gdf["canal_code"] = canal_hit["cancode"].where(canal_only)
    gdf["project_name"] = canal_hit["prjname"].where(canal_only)
    gdf["river_asset_loaded"] = river is not None
    gdf["canal_asset_loaded"] = canal is not None
    return gdf


def add_village_name_flag(
    gdf,
    village_path=PAN_INDIA_VILLAGE_PATH,
    village_name_field=PAN_INDIA_VILLAGE_NAME_FIELD,
):
    villages = _load_reference(village_path, "Village", gdf, 0, [village_name_field])
    if villages is None:
        gdf["intersecting_villages"] = "[]"
        gdf["intersecting_villages_count"] = 0
        gdf["covering_village_names"] = None
        gdf["village_name"] = None
        gdf["village_asset_loaded"] = False
        return gdf

    hits = gpd.sjoin(
        gdf[["geometry"]],
        villages[[village_name_field, "geometry"]],
        how="inner",
        predicate="intersects",
    )
    names = (
        hits[village_name_field]
        .dropna()
        .groupby(level=0)
        .agg(lambda s: sorted(set(s)))
        .reindex(gdf.index)
    )
    names = names.apply(lambda v: v if isinstance(v, list) else [])

    gdf["intersecting_villages"] = names.apply(
        lambda v: json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    )
    gdf["intersecting_villages_count"] = names.apply(len)
    joined_names = names.apply(lambda v: ", ".join(v) if v else None)
    gdf["covering_village_names"] = joined_names
    gdf["village_name"] = joined_names
    gdf["village_asset_loaded"] = True
    return gdf


def waterbody_catchment_streamorder_properties_local(
    gdf,
    buffer_m=DEFAULT_WATERBODY_TYPE_BUFFER_M,
    river_path=PAN_INDIA_RIVER_PATH,
    canal_path=PAN_INDIA_CANAL_PATH,
):
    """Local SWB3: returns a copy of `gdf` (EPSG:4326) with the SWB3 properties added."""
    out = gdf.copy()
    if out.crs is None:
        out = out.set_crs("EPSG:4326")
    elif out.crs.to_epsg() != 4326:
        out = out.to_crs("EPSG:4326")
    if out.empty:
        raise ValueError("No water bodies to enrich in SWB3.")
    out = out.reset_index(drop=True)

    logger.info("[SWB3 local] features=%s", len(out))
    out = add_max_stream_order_and_catchment(out)
    out = add_on_drainage_flag(out)
    out = add_waterbody_type_flag(
        out, river_path=river_path, canal_path=canal_path, buffer_m=buffer_m
    )
    out = add_village_name_flag(out)
    return out
