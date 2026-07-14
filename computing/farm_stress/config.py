"""Shared constants for the Farm Stress Detection System.

Mirrors the design in data/plan.md, adapted to this repo's conventions:
GEE asset roots come from utilities.constants.GEE_PATHS["FARM_STRESS"]
(resolved against settings.GEE_STORAGE_PROJECT) rather than hardcoded
project ids, and GCS export paths use settings.GCS_BUCKET_NAME.
"""

import os

from nrm_app.settings import BASE_DIR
from utilities.constants import GEE_PATHS

# ── GEE asset roots ───────────────────────────────────────────────────────────
FARM_STRESS_ASSET_ROOT = GEE_PATHS["FARM_STRESS"]["GEE_ASSET_PATH"].rstrip("/")

# ── MODIS products ────────────────────────────────────────────────────────────
MODIS_LC_COLLECTION = "MODIS/061/MCD12Q1"  # land cover, annual, 500m
# MOD16A2GF (gap-filled) instead of raw MOD16A2: same ET/PET/PET_QC bands,
# but gap-filled composites instead of masked-out low-quality pixels - better
# for the historical archive. Verified on the GEE catalog: covers
# 2000-01-01 through 2025-12-27 (the GF product lags behind the live edge
# vs raw MOD16A2, since gap-filling needs later composites as input - keep
# this in mind for the operational current-period lookup in a later phase).
MODIS_ET_COLLECTION = "MODIS/061/MOD16A2GF"  # 8-day ET/PET, 500m, from 2000
MODIS_NDVI_COLLECTION = "MODIS/061/MOD13A1"  # 16-day NDVI, 500m, from 2000

# MODIS land cover class values treated as agricultural.
# IGBP classes: 12=Croplands, 14=Cropland/Natural mosaic
AGRI_LC_CLASSES = [12, 14]

# ── GSMaP rainfall ────────────────────────────────────────────────────────────
# v8/operational is a single continuous collection covering 1998-01-01 to
# present (near-real-time) - verified against the GEE catalog directly.
# Unlike v6 (which was split into separate reanalysis/2000-2014 and
# operational/2014-present collections), v8 needs no merge.
GSMAP_COLLECTION = "JAXA/GPM_L3/GSMaP/v8/operational"
GSMAP_BAND = "hourlyPrecipRate"  # mm/hr instantaneous rate, one image/hour

# ── Geography ─────────────────────────────────────────────────────────────────
# India bounding box (rectangle; not clipped to a country/agri boundary at
# this stage - Step 1 exports are the raw background-climatology inputs).
INDIA_BBOX_COORDS = [68.0, 6.5, 97.5, 37.5]

# ── Resolution ────────────────────────────────────────────────────────────────
EXPORT_SCALE_M = 500  # metres - all fused/alert exports at this scale
SPI_SCALE_M = 11000  # GSMaP native ~11km; SPI/SPEI fitted here, resampled for fusion

# ── Local download paths ──────────────────────────────────────────────────────
# Historical archive rasters (11km, single-band) are downloaded directly via
# ee.Image.getDownloadURL() rather than a GCS batch export - they're small
# enough that a synchronous direct download is simpler than submitting and
# polling an export task. See computing/farm_stress/local_download.py.
LOCAL_EXPORT_ROOT = os.path.join(BASE_DIR, "data", "farm_stress", "exports")
LOCAL_DIR_GSMAP_MONTHLY = os.path.join(LOCAL_EXPORT_ROOT, "gsmap_monthly")
LOCAL_DIR_MODIS_PET_MONTHLY = os.path.join(LOCAL_EXPORT_ROOT, "modis_pet_monthly")
LOCAL_DIR_WATER_BALANCE_MONTHLY = os.path.join(LOCAL_EXPORT_ROOT, "water_balance_monthly")
LOCAL_DIR_GSMAP_DAILY = os.path.join(LOCAL_EXPORT_ROOT, "gsmap_daily")

# GCS is still used for the much larger weekly 500m operational alert
# rasters (a later phase), which are too big for a direct download.
GCS_PATH_GSMAP_MONTHLY = "ksheetiz/farm_stress/gsmap_monthly/"
GCS_PATH_MODIS_PET_MONTHLY = "ksheetiz/farm_stress/modis_pet_monthly/"
GCS_PATH_WATER_BALANCE_MONTHLY = "ksheetiz/farm_stress/water_balance_monthly/"
GCS_PATH_GSMAP_DAILY = "ksheetiz/farm_stress/gsmap_daily/"

# ── Time / period convention ──────────────────────────────────────────────────
HISTORICAL_START = "2000-01-01"
HISTORICAL_END = "2025-12-31"
SPI1_HISTORICAL_START = "2000-01-01"  # GSMaP available from 2000
SPEI_HISTORICAL_START = "2000-01-01"  # MOD16A2GF available from 2000 (raw MOD16A2 was 2001)
KHARIF_START_DOY = 121  # May 1 - earliest possible onset scan start
KHARIF_END_DOY = 304  # Oct 31

# Epoch anchor for 28-day period counting. July 1 2017 is day-zero;
# periods are counted backwards/forwards in fixed 28-day blocks so that
# distribution fits are directly comparable across periods.
EPOCH_ANCHOR = "2017-07-01"

# ── SPI / SPEI ────────────────────────────────────────────────────────────────
SPI1_WINDOW = 1  # 28-day periods
SPEI3_WINDOW = 3  # 84 days total

MODIS_PET_BAND = "PET"
MODIS_PET_SCALE_FACTOR = 0.1  # stored as integer x 0.1 -> mm/8day
MODIS_COMPOSITE_DAYS = 8  # standard composite length; period 46 is 5-6 days,
# use the composite's actual system:time_end - system:time_start instead

# ── MAI thresholds (standardised anomaly) ─────────────────────────────────────
MAI_MODERATE_THRESHOLD = -1.0
MAI_SEVERE_THRESHOLD = -1.5
MAI_FORECAST_THRESHOLD = -1.0
MAI_MAX_COMPOSITE_AGE_DAYS = 16  # warn if latest MAI composite is older than this

# ── VCI thresholds ────────────────────────────────────────────────────────────
VCI_ABSOLUTE_STRESS = 35  # VCI below this -> stressed
VCI_TREND_THRESHOLD = -2.0  # VCI units per day
VCI_ANOMALY_THRESHOLD = -15.0  # % below similarity-weighted analog median
VCI_MAX_COMPOSITE_AGE_DAYS = 24  # warn if latest VCI composite is older than this
VCI_TREND_USE_ACTUAL_DAYS = True
VCI_PHENO_DAY_FROM_COMPOSITE = True

# ── Background drought thresholds ─────────────────────────────────────────────
# PRECONDITIONED: both immediate rainfall AND accumulated water balance deficient
SPI1_PRECONDITION = -0.8
SPEI3_PRECONDITION = -0.5
# WATCH: either signal is notably below normal
SPI1_WATCH = -0.4
SPEI3_WATCH = -0.3

BACKGROUND_NORMAL = "NORMAL"
BACKGROUND_WATCH = "WATCH"
BACKGROUND_PRECONDITIONED = "PRECONDITIONED"

# Applied to MAI stress thresholds: preconditioned pixels trigger at
# shallower anomalies because their soil profiles are already depleted.
THRESHOLD_MODIFIER = {
    BACKGROUND_PRECONDITIONED: 0.75,
    BACKGROUND_WATCH: 0.90,
    BACKGROUND_NORMAL: 1.00,
}

# ── Analog year selection ─────────────────────────────────────────────────────
N_ANALOG_YEARS = 5  # similarity-weighted analog years per pixel

# ── Soil water balance ────────────────────────────────────────────────────────
DRAINAGE_COEFF = 0.1  # fraction of excess above field capacity draining per day
STRESS_THRESHOLD_FRAC = 0.5  # soil water below this fraction of FC -> AET < PET
FORECAST_DAYS = 14

# ── Alert classes ─────────────────────────────────────────────────────────────
ALERT_CLASSES = {
    "NORMAL": 0,
    "WATCH": 1,
    "AT_RISK": 2,
    "STRESSED": 3,
    "CRITICAL": 4,
}

# MODIS composite period DOY boundaries (fixed by NASA, 8-day and 16-day).
# Used to map a date to a period_index without querying GEE each time.
MODIS_8DAY_PERIOD_START_DOYS = list(range(1, 366, 8))  # [1, 9, 17, ..., 361]
MODIS_16DAY_PERIOD_START_DOYS = list(range(1, 366, 16))  # [1, 17, 33, ..., 337]
