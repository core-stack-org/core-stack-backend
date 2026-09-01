"""
Utility functions and constants for the Forest Fire pipeline.

Provides MODIS fire data loading, FRP preprocessing, and fire-binary
helpers for per-MWS fire risk analysis using Google Earth Engine.
"""

import ee

# ----------------------------------------
# PARAMETERS / CONSTANTS
# ----------------------------------------

SCALE = 1000
MAXPIX = 1e13

# MODIS Active Fire products (Terra + Aqua)
FIRE_INDEX_PATH = (
    "projects/corestack-datasets-alpha/assets/datasets/hazards/fire_index_FRP30"
)


def load_fire_image(roi):
    """
    Load yearly fire-index image.
    """
    return ee.Image(FIRE_INDEX_PATH).clip(roi.geometry())


def prepare_frp_images(fire_image, start_year, end_year):
    """
    Build aggregated images from yearly FRP and fireDays bands.
    """

    years = list(range(start_year, end_year + 1))
    n_years = len(years)

    frp_bands = [f"FRP_{y}" for y in years]
    fireday_bands = [f"fireDays_{y}" for y in years]

    frp = fire_image.select(frp_bands)
    firedays = fire_image.select(fireday_bands)

    return {
        "sum": frp.reduce(ee.Reducer.sum()).divide(n_years),
        "mean": frp.reduce(ee.Reducer.mean()),
        "max": frp.reduce(ee.Reducer.max()),
        "count": firedays.reduce(ee.Reducer.sum()).divide(n_years),
    }
