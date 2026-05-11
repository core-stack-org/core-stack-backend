"""
Utility functions for the Tree in Grassland pipeline.

Provides spatial and temporal context analysis for tree-shrub relationships
using LULC raster data from Google Earth Engine.
"""

import ee

# ----------------------------------------
# PARAMETERS / CONSTANTS
# ----------------------------------------

TREE_CLASS = 6
SHRUB_CLASS = 12
SHRUB_THRESHOLD = 0.5
RADIUS_M = 100
SCALE = 30
MAXPIX = 1e12

LULC_CLASSES = {
    0: "Background",
    1: "Built_up",
    2: "Kharif_water",
    3: "Kharif_Rabi_water",
    4: "Kharif_Rabi_Zaid_water",
    5: "Crops",
    6: "Trees",
    7: "Barren_land",
    8: "Single_Kharif",
    9: "Single_Non_Kharif",
    10: "Double_Cropping",
    11: "Triple_Annual_Perennial",
    12: "Shrubs_Scrubs",
}

NEIGHBOR_CLASSES = [k for k in LULC_CLASSES.keys() if k != TREE_CLASS]
THRESHOLD = 0.5  # strictly > 50%

PAN_INDIA_LULC_PATH = (
    "projects/corestack-datasets/assets/datasets/LULC_v3_river_basin"
)


def load_pan_india_lulc(year):
    """
    Load a pan-India LULC v3 image for a given year.

    The GEE naming convention uses agricultural years:
        key year Y => path pan_india_lulc_v3_{Y-1}_{Y}

    Args:
        year: int, the year key (e.g., 2018 loads the 2017-2018 agri-year image).

    Returns:
        ee.Image with the 'predicted_label' band, unmasked and cast to Int.
    """
    return (
        ee.Image(
            f"{PAN_INDIA_LULC_PATH}/pan_india_lulc_v3_{year - 1}_{year}"
        )
        .select("predicted_label")
        .unmask(0)
        .toInt()
    )


def tree_context_all(lulc, aoi):
    """
    Compute tree-shrub spatial context classification for a given LULC image.

    For each pixel, determines whether a tree pixel is embedded in shrubland
    (i.e., > THRESHOLD fraction of neighbours within RADIUS_M are shrubs).

    Classification values:
        0 = neither tree-in-shrub nor associated shrub
        1 = tree pixel embedded in shrubland
        2 = shrub pixel associated with those trees

    Args:
        lulc: ee.Image - LULC image with 'predicted_label' band.
        aoi: ee.Geometry - feature geometry (single MWS polygon).

    Returns:
        ee.Image with classification values 0, 1, or 2, clipped to aoi.
    """
    kernel = ee.Kernel.circle(RADIUS_M, "meters")
    lulc_img = lulc.clip(aoi.buffer(110))

    tree_mask = lulc_img.eq(TREE_CLASS)
    shrub_mask = lulc_img.eq(SHRUB_CLASS)

    total_px = (
        ee.Image.constant(1)
        .clip(aoi.buffer(110))
        .reduceNeighborhood(ee.Reducer.sum(), kernel)
    )

    shrub_frac = (
        shrub_mask.toInt()
        .reduceNeighborhood(ee.Reducer.sum(), kernel)
        .divide(total_px)
    )

    # Tree embedded in shrubland
    tree_in_shrub = tree_mask.And(shrub_frac.gt(THRESHOLD))

    # Shrubs associated with those trees
    shrub_around_tree = shrub_mask.And(
        tree_in_shrub.focal_max(radius=RADIUS_M, units="meters")
    )

    return (
        ee.Image(0)
        .where(tree_in_shrub, 1)
        .where(shrub_around_tree, 2)
        .toInt()
        .clip(aoi)
    )


def temporal_context(lulc_by_year, aoi, start_years, end_years):
    """
    Compute temporal context images for start and end periods.

    Takes the mode (most frequent value) of tree-context classifications
    across multiple years to produce a stable start and end context.

    Args:
        lulc_by_year: dict mapping int year -> ee.Image (LULC images).
        aoi: ee.Geometry - area of interest.
        start_years: list of int years for the start context window
                     (e.g., [2018, 2019, 2020]).
        end_years: list of int years for the end context window
                   (e.g., [2019, 2020, 2021]).

    Returns:
        tuple of (context_start, context_end) as ee.Image.
    """
    start_contexts = [tree_context_all(lulc_by_year[y], aoi) for y in start_years]
    end_contexts = [tree_context_all(lulc_by_year[y], aoi) for y in end_years]

    context_start = (
        ee.ImageCollection(start_contexts).reduce(ee.Reducer.mode()).toInt()
    )

    context_end = (
        ee.ImageCollection(end_contexts).reduce(ee.Reducer.mode()).toInt()
    )

    return context_start, context_end
