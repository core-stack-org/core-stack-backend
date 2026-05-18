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
TERRA_FIRE_PATH = "MODIS/061/MOD14A1"
AQUA_FIRE_PATH = "MODIS/061/MYD14A1"


def load_fire_collections(start_year, end_year):
    """
    Load and merge MODIS Terra + Aqua active fire collections.

    Filters the merged collection to the date range
    [start_year-01-01, end_year-12-31] and selects the MaxFRP band.

    Args:
        start_year: int – first year of the analysis window.
        end_year:   int – last year of the analysis window.

    Returns:
        ee.ImageCollection – merged, date-filtered MaxFRP collection.
    """
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    terra = ee.ImageCollection(TERRA_FIRE_PATH)
    aqua = ee.ImageCollection(AQUA_FIRE_PATH)

    fires = terra.merge(aqua).filterDate(start_date, end_date)
    return fires.select("MaxFRP")


def prepare_frp_images(frp_collection, n_years):
    """
    Pre-compute the four temporally aggregated fire images.

    Args:
        frp_collection: ee.ImageCollection – MaxFRP collection.
        n_years:        int – number of years in the analysis window.

    Returns:
        dict with keys 'sum', 'mean', 'max', 'count', each mapping to
        an ee.Image ready for spatial reduction.
    """
    # Mask zeros for FRP statistics
    def mask_fire(img):
        return img.updateMask(img.gt(0))

    frp_masked = frp_collection.map(mask_fire)

    # Binary fire occurrence for count
    def fire_binary(img):
        return img.gt(0).unmask(0).rename("fire")

    fire_binary_collection = frp_collection.map(fire_binary)

    return {
        "sum": frp_masked.sum().divide(n_years),          # yearly-normalised total FRP
        "mean": frp_masked.mean(),                         # temporal mean FRP
        # "max": frp_masked.max(),                           # peak FRP
        "max": frp_masked.max(), 
        "count": fire_binary_collection.sum().divide(n_years),  # yearly fire frequency
    }