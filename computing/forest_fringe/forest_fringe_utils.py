"""
Utility functions and constants for the Forest Fringe pipeline.

Provides LULC loading, change-product helpers, and fringe geometry
construction for forest-edge analysis using Google Earth Engine.
"""

import ee

# ----------------------------------------
# PARAMETERS / CONSTANTS
# ----------------------------------------

TREE_CLASS = 6
FRINGE_WIDTH = 50
OUTER_BUFFER = 100
SCALE = 30
MAXPIX = 1e12

PAN_INDIA_LULC_PATH = (
    "projects/corestack-datasets/assets/datasets/LULC_v3_river_basin"
)

LTP_CHANGE_PATH = (
    "projects/corestack-datasets/assets/datasets/"
    "tree_health/final_ltp_stp_change_2017_2021"
)

OVERALL_CHANGE_PATH = (
    "projects/corestack-trees/assets/tree_characteristics/"
    "overall_change_2017_2023"
)

# LULC years used by the forest fringe pipeline
LULC_YEARS = [2017, 2018, 2019]


def load_tree_mode():
    """
    Load a multi-year tree-mode image from pan-India LULC v3.

    Uses agricultural years 2017-2018, 2018-2019, and 2019-2020.
    Returns a binary self-masked image where 1 = modal tree class.

    Returns:
        ee.Image – binary tree-mode image (1 where mode is tree, masked elsewhere).
    """
    lulc_imgs = ee.ImageCollection([
        ee.Image(
            f"{PAN_INDIA_LULC_PATH}/pan_india_lulc_v3_{year}_{year + 1}"
        ).select("predicted_label").eq(TREE_CLASS)
        for year in LULC_YEARS
    ])
    return lulc_imgs.reduce(ee.Reducer.mode()).selfMask()


def load_ltp_change():
    """
    Load the LTP/STP change product (2017-2021).

    Returns:
        ee.Image – mean of the ltp_stp_change image collection.
    """
    return ee.ImageCollection(LTP_CHANGE_PATH).mean()


def load_overall_change():
    """
    Load the overall tree-health change product (2017-2022).

    Returns:
        ee.Image – mean of the overall_change image collection.
    """
    return ee.ImageCollection(OVERALL_CHANGE_PATH).mean()


def make_fringe(patch, fringe_width=FRINGE_WIDTH):
    """
    Create a fringe ring for a single forest patch.

    The fringe is the area between the outer boundary and an inward
    buffer of ``fringe_width`` metres.

    Args:
        patch: ee.Feature – a single forest polygon.
        fringe_width: int – inward buffer distance in metres (default 50).

    Returns:
        ee.Feature – ring geometry representing the forest fringe.
    """
    outer = patch.geometry()
    inner = outer.buffer(-fringe_width, 1)
    return ee.Feature(outer.difference(inner, 1))
