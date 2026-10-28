"""
Local (GeoDataFrame) version of swb4.py.

Matches each census water body point (WBC) to the SWB polygon that overlaps its
90 m buffer with the closest area, and copies the census columns onto that
polygon. Unmatched polygons get "NA" in the census columns, as in swb4.py.
"""

import logging

import geopandas as gpd
import numpy as np
import pandas as pd

from computing.config_loader import PAN_INDIA_WATERBODIES_CENSUS_PATH
from computing.surface_water_bodies.swb3_local import (
    _padded_bounds,
    read_vector_in_bounds,
)

logger = logging.getLogger(__name__)

CENSUS_BUFFER_M = 90
STATE_COLUMN = "State Name"


def _normalize_state(value):
    return str(value or "").upper().replace(" ", "")


def _fill_missing_area(census):
    """Water spread area, falling back to capacity / depth / 10000 (as swb4.py)."""
    area = pd.to_numeric(census["water_spread_area_of_water_body"], errors="coerce").fillna(0)
    capacity = pd.to_numeric(
        census["storage_capacity_water_body_original"], errors="coerce"
    ).fillna(0)
    depth = pd.to_numeric(
        census["max_depth_water_body_fully_filled"], errors="coerce"
    ).fillna(0)
    derived = np.where(depth != 0, capacity / depth.where(depth != 0, 1) / 10000, 0)
    return area.where(area != 0, derived)


def _load_census_points(gdf, state, census_path):
    census = read_vector_in_bounds(census_path, _padded_bounds(gdf, CENSUS_BUFFER_M))
    if STATE_COLUMN in census.columns:
        census = census[
            census[STATE_COLUMN].map(_normalize_state) == _normalize_state(state)
        ]
    return census.reset_index(drop=True)


def waterbody_wbc_intersection_local(
    gdf, state, census_path=PAN_INDIA_WATERBODIES_CENSUS_PATH
):
    """Local SWB4: returns `gdf` (EPSG:4326) with census properties merged in."""
    if not state:
        raise ValueError("State name must be provided to run SWB4")

    out = gdf.copy().reset_index(drop=True)
    if "UID" not in out.columns:
        out["UID"] = out["wb_id"] if "wb_id" in out.columns else out.index.astype(str)
    else:
        missing = out["UID"].isna()
        if missing.any() and "wb_id" in out.columns:
            out.loc[missing, "UID"] = out.loc[missing, "wb_id"]
    if "area_ored" not in out.columns:
        out["area_ored"] = out.to_crs(out.estimate_utm_crs()).area / 10000.0

    census = _load_census_points(out, state, census_path)
    logger.info("[SWB4 local] census points in extent: %s", len(census))
    census_cols = [c for c in census.columns if c != "geometry"]

    census["_spread"] = _fill_missing_area(census)
    metric_crs = out.estimate_utm_crs()
    buffered = census[["unique_id", "_spread", "geometry"]].to_crs(metric_crs)
    buffered["geometry"] = buffered.geometry.buffer(CENSUS_BUFFER_M)
    polygons = out[["UID", "area_ored", "geometry"]].to_crs(metric_crs)

    # Per census point: the intersecting polygon with the closest area.
    pairs = gpd.sjoin(
        buffered, polygons, how="inner", predicate="intersects"
    ).rename(columns={"index_right": "_poly_idx"})
    pairs["_diff"] = (pairs["area_ored"] - pairs["_spread"]).abs()
    pairs = pairs.sort_values("_diff")
    pairs = pairs[~pairs.index.duplicated(keep="first")]

    # Per polygon: the best census point (one census_id each, as swb4.py).
    best = pairs.sort_values("_diff").drop_duplicates("_poly_idx", keep="first")
    matched = census.loc[best.index, census_cols].copy()
    matched.index = best["_poly_idx"].values
    matched = matched.drop(columns=[c for c in ["uid"] if c in matched.columns])

    out["census_id"] = matched["unique_id"].reindex(out.index).fillna("NA")
    has_match = out.index.isin(matched.index)

    for column in matched.columns:
        values = matched[column].reindex(out.index)
        if column in out.columns and column != "census_id":
            # Census value overrides the polygon's own where present (copyProperties).
            out[column] = values.where(values.notna() & has_match, out[column])
        elif has_match.all():
            out[column] = values
        else:
            # Same as swb4.py: columns that only exist for matches become "NA" elsewhere.
            as_text = values.astype(object).where(values.isna(), values.astype(str))
            out[column] = as_text.where(has_match, "NA")

    logger.info(
        "[SWB4 local] matched %s of %s water bodies to census", has_match.sum(), len(out)
    )
    return out
