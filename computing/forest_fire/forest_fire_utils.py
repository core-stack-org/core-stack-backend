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
FIRE_INDEX_PATH = "projects/corestack-datasets-alpha/assets/datasets/hazards/fire_index/fire_index_FRP30"


def load_fire_image(aez_suffix=None):
    """
    Load yearly fire-index image.
    """
    if aez_suffix:
        return ee.Image(f"{FIRE_INDEX_PATH}_{aez_suffix}")

    return ee.Image(FIRE_INDEX_PATH)


def prepare_frp_images(fire_image, start_year, end_year):
    """
    Build aggregated images from yearly FRP and fireDays bands.
    """

    years = list(range(start_year, end_year + 1))
    n_years = len(years)

    frp_bands = [f"FRP_{y}" for y in years]
    fire_day_bands = [f"fireDays_{y}" for y in years]

    frp = fire_image.select(frp_bands)
    fire_days = fire_image.select(fire_day_bands)
    fire_day_total = firedays.reduce(ee.Reducer.sum())
    fire_day_average = fireday_total.divide(n_years)

    return {
        "mean": frp.reduce(ee.Reducer.sum()).divide(n_years).rename("mean"),
        "max": frp.reduce(ee.Reducer.max()),
        "count": fireday_average.where(fireday_total.lt(1), 0),
    }
