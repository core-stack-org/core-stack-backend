"""
Tunables + constants for the GBIF biodiversity pipeline.

Everything a scientist/reviewer might want to adjust (cleaning thresholds, data-poor cutoff,
basis-of-record filter, IUCN categories) lives here so the analysis code stays declarative.
"""

import os

from utilities.gee_utils import get_gee_asset_path, valid_gee_text

# --- GBIF credentials (Download API needs a registered account) --------------------------------
# Set in the environment / .env, mirroring how other secrets are handled in the repo.
GBIF_USER = os.environ.get("GBIF_USER")
GBIF_PWD = os.environ.get("GBIF_PWD")
GBIF_EMAIL = os.environ.get("GBIF_EMAIL")

# --- Local working dirs -------------------------------------------------------------------------
DATA_DIR = os.environ.get(
    "GBIF_DATA_DIR", os.path.join(os.path.dirname(__file__), "_data")
)  # per-block raw downloads
CACHE_DIR = os.environ.get(
    "GBIF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "_cache")
)  # IUCN-category lookup cache (global per taxonKey)

# --- India bounding box (minlon, minlat, maxlon, maxlat) — coarse sanity clip -------------------
INDIA_BBOX = (68.0, 6.5, 97.5, 37.6)

# --- Cleaning thresholds ------------------------------------------------------------------------
MAX_COORD_UNCERTAINTY_M = 10000  # drop points whose stated uncertainty is worse than ~10 km
CENTROID_DUP_THRESHOLD = 1000    # >N identical coords = likely a country/province centroid dump

# --- Data-poor threshold ------------------------------------------------------------------------
# MWS with fewer than this many records are flagged data_poor (cannot assess), never "0 species".
MIN_RECORDS = 20

# --- basisOfRecord values we keep (real observations/specimens) ---------------------------------
KEEP_BASIS_OF_RECORD = [
    "HUMAN_OBSERVATION",
    "PRESERVED_SPECIMEN",
    "MACHINE_OBSERVATION",
    "OBSERVATION",
]

# --- IUCN Red List categories counted as "threatened" ------------------------------------------
THREATENED_IUCN_CATEGORIES = ["VU", "EN", "CR"]

# --- DB / GeoServer registration ---------------------------------------------------------------
WORKSPACE = "biodiversity"
DATASET_NAME_VECTOR = "Biodiversity Occurrence"
VECTOR_STYLE_NAME = "biodiversity_mws"  # GeoServer SLD; used by register_biodiversity_layer
ALGORITHM_NAME = "GBIF_GEE_BLOCK_JOIN"
ALGORITHM_VERSION = "2.0"

# --- Points upload to GEE ------------------------------------------------------------------------
# Earth Engine rejects any single request over 10 MB. An inline FeatureCollection costs ~200 bytes
# per point once serialised, so blocks with more than POINTS_INLINE_LIMIT cleaned points are exported
# to intermediate GEE assets in chunks of POINTS_CHUNK_SIZE instead (see gbif_mws_stats.points_to_ee_fc).
POINTS_INLINE_LIMIT = 20000
POINTS_CHUNK_SIZE = 20000


def get_gee_block_asset_id(state, district, block):
    """
    GEE asset id for the block's per-MWS biodiversity FeatureCollection (the layer output).
    Single source of truth used both for the idempotency check and the export target.
    e.g. projects/<project>/assets/.../<block>/biodiversity_<district>_<block>
    """
    return (
        get_gee_asset_path(state, district, block)
        + "biodiversity_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )


def get_gee_points_asset_id(state, district, block, part):
    """
    GEE asset id for one chunk of the block's cleaned GBIF points (intermediate; deleted once the
    per-MWS stats asset exists). e.g. .../<block>/biodiversity_points_<district>_<block>_part0
    """
    return (
        get_gee_asset_path(state, district, block)
        + "biodiversity_points_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + f"_part{part}"
    )
