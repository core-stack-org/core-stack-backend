from datetime import datetime

import cupy as cp


LULC_SOURCE_DYNAMICWORLD = "dynamicworld"
LULC_SOURCE_INDIASATV3 = "indiasatv3"
LULC_SOURCES = (LULC_SOURCE_DYNAMICWORLD, LULC_SOURCE_INDIASATV3)

SEASON_KHARIF = "kharif"
SEASON_RABI = "rabi"
SEASON_ZAID = "zaid"
SEASON_STATIC = "static"

DW_WATER = 0
DW_TREES = 1
DW_CROPS = 4
DW_SHRUB_AND_SCRUB = 5
DW_BUILT = 6
DW_BARE = 7

INDIASAT_BACKGROUND = 0
INDIASAT_BUILT_UP = 1
INDIASAT_WATER_KHARIF = 2
INDIASAT_WATER_KHARIF_RABI = 3
INDIASAT_WATER_ALL_SEASONS = 4
INDIASAT_TREE_FORESTS = 6
INDIASAT_BARRENLANDS = 7
INDIASAT_SINGLE_CROPPING = 8
INDIASAT_SINGLE_NON_KHARIF_CROPPING = 9
INDIASAT_DOUBLE_CROPPING = 10
INDIASAT_TRIPLE_CROPPING = 11
INDIASAT_SHRUB_SCRUB = 12


def normalize_lulc_source(source: str) -> str:
    normalized = str(source or LULC_SOURCE_DYNAMICWORLD).strip().lower()
    if normalized not in LULC_SOURCES:
        raise ValueError(
            f"Unsupported LULC source {source!r}; expected one of {', '.join(LULC_SOURCES)}"
        )
    return normalized


def month_from_timestamp(timestamp) -> int:
    if hasattr(timestamp, "month"):
        return int(timestamp.month)

    text = str(timestamp)
    for fmt in ("%Y%m%d_%H", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).month
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).month
    except ValueError as exc:
        raise ValueError(f"Cannot parse rainfall timestamp {timestamp!r}") from exc


def season_from_month(month: int) -> str:
    if month in (7, 8, 9, 10):
        return SEASON_KHARIF
    if month in (11, 12, 1, 2):
        return SEASON_RABI
    if month in (3, 4, 5, 6):
        return SEASON_ZAID
    raise ValueError(f"Invalid month {month!r}")


def lulc_cache_key_for_timestamp(source: str, timestamp) -> str:
    source = normalize_lulc_source(source)
    if source == LULC_SOURCE_DYNAMICWORLD:
        return SEASON_STATIC
    return season_from_month(month_from_timestamp(timestamp))


def nodata_lulc_value_for_source(source: str) -> int:
    source = normalize_lulc_source(source)
    if source == LULC_SOURCE_DYNAMICWORLD:
        return DW_SHRUB_AND_SCRUB
    return INDIASAT_BACKGROUND


def map_lulc_to_dynamic_world(raw_lulc: cp.ndarray, source: str, timestamp) -> cp.ndarray:
    source = normalize_lulc_source(source)
    if source == LULC_SOURCE_DYNAMICWORLD:
        return raw_lulc

    season = season_from_month(month_from_timestamp(timestamp))
    return map_indiasatv3_to_dynamic_world(raw_lulc, season)


def map_indiasatv3_to_dynamic_world(raw_lulc: cp.ndarray, season: str) -> cp.ndarray:
    # Background/unknown classes stay shrub/scrub instead of becoming water.
    mapped = cp.full(raw_lulc.shape, DW_SHRUB_AND_SCRUB, dtype=cp.uint8)

    mapped = cp.where(raw_lulc == INDIASAT_BUILT_UP, DW_BUILT, mapped)
    mapped = cp.where(raw_lulc == INDIASAT_TREE_FORESTS, DW_TREES, mapped)
    mapped = cp.where(raw_lulc == INDIASAT_BARRENLANDS, DW_BARE, mapped)
    mapped = cp.where(raw_lulc == INDIASAT_SHRUB_SCRUB, DW_SHRUB_AND_SCRUB, mapped)

    water = (
        (raw_lulc == INDIASAT_WATER_KHARIF)
        | (raw_lulc == INDIASAT_WATER_KHARIF_RABI)
        | (raw_lulc == INDIASAT_WATER_ALL_SEASONS)
    )
    mapped = cp.where(water, DW_WATER, mapped)

    mapped = cp.where(
        raw_lulc == INDIASAT_SINGLE_CROPPING,
        DW_CROPS if season == SEASON_KHARIF else DW_SHRUB_AND_SCRUB,
        mapped,
    )
    mapped = cp.where(
        raw_lulc == INDIASAT_SINGLE_NON_KHARIF_CROPPING,
        DW_CROPS if season == SEASON_RABI else DW_SHRUB_AND_SCRUB,
        mapped,
    )
    mapped = cp.where(
        raw_lulc == INDIASAT_DOUBLE_CROPPING,
        DW_CROPS if season in (SEASON_KHARIF, SEASON_RABI) else DW_SHRUB_AND_SCRUB,
        mapped,
    )
    mapped = cp.where(raw_lulc == INDIASAT_TRIPLE_CROPPING, DW_CROPS, mapped)

    return mapped.astype(cp.uint8, copy=False)
