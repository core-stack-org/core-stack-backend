"""
Single definition of "the tehsil boundary" for the farm-boundary pipeline.

Used by fetch_raw (S2 cell enumeration), fetch_metadata (same cells),
convert (clip farms) and et_intersection (choose AEZ zones), so all phases
see exactly the same extent.

The boundary is the tehsil's MWS (micro-watershed) layer:

    {BASE_DATA_DIR}/tehsil_mws/{state}/{district}/{tehsil}/
        filtered_mws_{district}_{tehsil}_uid.geojson
"""

import logging
import os

import geopandas as gpd

from computing.base_layer_setup import tehsil_mws_file_candidates
from utilities.constants import BASE_DATA_DIR

logger = logging.getLogger(__name__)

# MWS polygons tile the tehsil, so after dissolving there can be hairline gaps
# between neighbours (digitising / snapping). Gaps up to ~2 * this value are
# closed so farms falling in a sliver are not clipped away. 1e-5 deg ~ 1.1 m;
# genuine enclaves (much larger) are kept.
GAP_CLOSE_TOLERANCE_DEG = 1e-5


def get_tehsil_polygon(state: str, district: str, block: str):
    """
    Return the tehsil boundary as a single Shapely geometry in EPSG:4326
    (all MWS polygons of the tehsil dissolved, hairline gaps closed).

    Raises FileNotFoundError if the MWS file is missing, ValueError if it has
    no features.
    """
    candidates = tehsil_mws_file_candidates(
        os.path.join(BASE_DATA_DIR, "tehsil_mws"),
        state,
        district,
        block,
        (".geojson",),
    )
    mws_path = next((p for p in candidates if p.exists()), None)
    if mws_path is None:
        raise FileNotFoundError(
            f"MWS boundary not found for {state}/{district}/{block}. "
            f"Expected one of: {[str(p) for p in candidates]}"
        )

    mws = gpd.read_file(mws_path)
    if mws.empty:
        raise ValueError(f"MWS boundary file has no features: {mws_path}")
    if mws.crs is not None and mws.crs.to_epsg() != 4326:
        mws = mws.to_crs(4326)

    geom = mws.dissolve().geometry.iloc[0]
    closed = geom.buffer(GAP_CLOSE_TOLERANCE_DEG, join_style="mitre", mitre_limit=2.0).buffer(
        -GAP_CLOSE_TOLERANCE_DEG, join_style="mitre", mitre_limit=2.0
    )
    if closed.is_empty or not closed.is_valid:
        closed = geom

    logger.info(
        "Tehsil boundary from MWS file: %s (%d polygons)", mws_path, len(mws)
    )
    return closed
