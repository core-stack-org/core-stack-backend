"""Monsoon onset per CHIRPS cell, rule v1.2 (rainfall only).

The rule and its settings were chosen against IMD's onset maps of 11 seasons (2008-2025): 77.6% of
India's cells within 7 days of IMD's date and 93.2% within 14, scoring each season with settings
fitted on the other ten. IMD's maps are not an input. Experiments and scores:
base_layers/drought/onset_vs_imd/onset_experiments.md ("Milestone v1.2").

For each season:
  1. Kerala date, one for all of India: the 2nd of the first 2 days in a row, on or after 22 May,
     on which at least 6 of IMD's 12 mainland Kerala onset stations get 15 mm or more.
Then for each cell:
  2. Latitude limit: Kerala date + max(0, latitude - 10) / speed.
  3. Climatology floor: Kerala date + usual lag - margin, not before the Kerala date. The usual lag
     is the median, over 1981 to the year before the season, of the cell's onset minus the Kerala
     date under steps 1, 2 and 5 alone (no floor).
  4. Earliest day: the later of 2 and 3, rounded up.
  5. Onset: the 2nd of the first 2 days in a row, from the earliest day on, on which the area rain
     (mean daily rain of the India cells in the 21 x 21-cell box around the cell, about 116 km)
     reaches the region's threshold. Where the region has no rain test (Ladakh), and where no such
     pair comes by 31 August, the onset is the earliest day.

Threshold, speed and margin are set per region: IMD's homogeneous regions by state, with the
Northeast, Jammu & Kashmir and Ladakh apart.

Files, under drought/<version>/rain/:
  onset_regions.tif     region code and inside-India flag per cell (from the Survey of India tehsils)
  onset_lag/<year>.tif  the year's onset minus its Kerala date under steps 1, 2 and 5 alone
"""

import datetime as dt
import json
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from rasterio.features import geometry_mask, rasterize
from scipy.ndimage import distance_transform_edt, uniform_filter

from computing.drought.local.config import ONSET_FIRST_WEEK, RAIN_RECORD_START
from computing.drought.local.store import raster_profile, read_bands, read_tags, write_stack
from computing.drought.local.weeks import week_of, week_start

log = logging.getLogger(__name__)

RULE = "v1.2: Kerala stations; later of latitude limit and usual lag; area rain on 2 days; else earliest day"

# Step 1. Centres of the CHIRPS cells of IMD's 12 mainland Kerala onset stations
# (Pai & Rajeevan 2009; Minicoy and Amini have no CHIRPS land cell).
KERALA_STATIONS = {
    "Thiruvananthapuram": (76.925, 8.475), "Punalur": (76.925, 9.025), "Kollam": (76.625, 8.875),
    "Alappuzha": (76.325, 9.525), "Kottayam": (76.525, 9.575), "Kochi": (76.275, 9.975),
    "Thrissur": (76.225, 10.525), "Kozhikode": (75.775, 11.275), "Thalassery": (75.475, 11.775),
    "Kannur": (75.375, 11.875), "Kudulu": (74.975, 12.525), "Mangaluru": (74.875, 12.925),
}
KERALA_WET_MM = 15.0
KERALA_MIN_STATIONS = 6
KERALA_FROM = (5, 22)  # the 2nd day of the pair on or after 22 May

LAST_DAY = (8, 31)  # onset search ends on 31 August
LATITUDE_FROM = 10.0
AREA_BOX = 21  # cells a side

# Region code: name, threshold (mm a day; None = no rain test), speed (degrees of latitude a day),
# margin (days). Fitted on IMD's onset lines of 2008, 2009, 2010, 2016 and 2019-2025.
REGIONS = {
    1: ("Central India", 2.5, 0.75, 3),
    2: ("East India", 5.0, 1.0, 0),
    3: ("Northwest India", 1.0, 0.75, 3),
    4: ("South Peninsula", 0.5, 0.5, 5),
    5: ("Northeast", 0.5, 2.0, 5),
    6: ("Jammu & Kashmir", 1.0, 0.75, 3),  # the Northwest's settings
    7: ("Ladakh", None, 0.75, 0),  # about 55 mm in June-September: too dry for a rain test
}
# STATE in the Survey of India tehsils -> region code. Tehsils marked DISPUTED, and cells outside
# every tehsil, take the nearest region.
STATE_REGION = {
    "CHHATISGARH": 1, "DADRA & NAGAR HAVELI & DAMAN & DIU": 1, "GOA": 1, "GUJARAT": 1,
    "MADHYA PRADESH": 1, "MADHYA PRADESH(DISPUTED)": 1, "MAHARASHTRA": 1, "ODISHA": 1,
    "BIHAR": 2, "JHARKHAND": 2, "WEST BENGAL": 2, "DISPUTED (WEST BENGAL)": 2,
    "CHANDIGARH": 3, "DELHI": 3, "HARYANA": 3, "HIMACHAL PRADESH": 3, "PUNJAB": 3, "RAJASTHAN": 3,
    "UTTAR PRADESH": 3, "UTTARAKHAND": 3,
    "ANDHRA PRADESH": 4, "KARNATAKA": 4, "KERALA": 4, "PUDUCHERRY": 4, "TAMIL NADU": 4, "TELANGANA": 4,
    "ANDAMAN & NICOBAR": 4, "LAKSHADWEEP": 4,
    "ARUNACHAL PRADESH": 5, "ASSAM": 5, "MANIPUR": 5, "MEGHALAYA": 5, "MIZORAM": 5, "NAGALAND": 5,
    "SIKKIM": 5, "TRIPURA": 5,
    "JAMMU AND KASHMIR": 6,
    "LADAKH": 7,
}

# Onset source codes stored in the season file.
SOURCE_RAIN, SOURCE_NO_RAIN_TEST, SOURCE_FALLBACK = 1, 2, 3
SOURCE_NOTE = "1 area rain on 2 days, 2 earliest day (no rain test), 3 earliest day (no rain by 31 August)"


# ---------------------------------------------------------------- files


def regions_file(paths):
    return paths.shared_dir / "rain" / "onset_regions.tif"


def lag_file(paths, year):
    return paths.shared_dir / "rain" / "onset_lag" / f"{year}.tif"


def settings_tag():
    parts = [f"{name} {'no rain test' if mm is None else f'{mm} mm'} {v} deg/d {x} d" for name, mm, v, x in REGIONS.values()]
    return "; ".join(parts) + f"; box {AREA_BOX}"


# ---------------------------------------------------------------- regions


def build_regions(tehsils, outline, transform, shape, land):
    """Region code per CHIRPS land cell (0 elsewhere) and the India cells: land cells whose centre
    lies inside the outline (mainland, no islands). tehsils, outline: GeoJSON feature lists."""
    shapes = []
    for feature in tehsils:
        code = STATE_REGION.get(str(feature["properties"].get("STATE", "")).strip().upper())
        if code:
            shapes.append((feature["geometry"], code))
    region = rasterize(shapes, out_shape=shape, transform=transform, fill=0, dtype="uint8")
    _, (rows, cols) = distance_transform_edt(region == 0, return_indices=True)
    region = np.where(land, region[rows, cols], 0).astype(np.uint8)
    india = land & ~geometry_mask([f["geometry"] for f in outline], shape, transform)
    return region, india


def ensure_regions(paths, read_day):
    """Write onset_regions.tif once, from the tehsils and the India outline."""
    path = regions_file(paths)
    if path.is_file():
        return
    with open(paths.soi_tehsils) as f:
        tehsils = json.load(f)["features"]
    with open(paths.india_outline) as f:
        outline = json.load(f)["features"]
    first = dt.date(RAIN_RECORD_START, 1, 1)
    land = ~np.isnan(read_day(paths, first))
    transform, crs, shape = raster_profile(paths.chirps_day(first))
    region, india = build_regions(tehsils, outline, transform, shape, land)
    write_stack(path, np.stack([region, india.astype(np.uint8)]), transform, crs, ["region", "inside_india"], 255,
                {"regions": ", ".join(f"{code} {r[0]}" for code, r in REGIONS.items()),
                 "inside_india": "cell centre inside the mainland outline", "tehsils": paths.soi_tehsils.name,
                 "outline": paths.india_outline.name})
    log.info("Onset regions written: %d India cells", int(india.sum()))


def read_regions(paths):
    region, india = read_bands(regions_file(paths))
    return region.astype(np.uint8), india == 1


# ---------------------------------------------------------------- the rule


def kerala_date(station_rain, days):
    """Day of year of the Kerala date. station_rain: (days, stations) for the given dates."""
    passing = (np.nan_to_num(station_rain) >= KERALA_WET_MM).sum(axis=1) >= KERALA_MIN_STATIONS
    first = dt.date(days[0].year, *KERALA_FROM)
    for i in range(1, len(days)):
        if days[i] >= first and passing[i] and passing[i - 1]:
            return days[i].timetuple().tm_yday
    raise ValueError(f"no Kerala date by {days[-1]}")


def area_rain(daily, india):
    """Mean daily rain of the India cells in the AREA_BOX x AREA_BOX box around each cell;
    NaN where the box holds no India cell. daily: (days, r, c)."""
    rain = np.where(india, np.nan_to_num(daily), 0).astype(np.float32)
    total = uniform_filter(rain, size=(1, AREA_BOX, AREA_BOX), mode="constant")
    share = uniform_filter(india.astype(np.float32), size=AREA_BOX, mode="constant")
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(share >= 0.5 / AREA_BOX**2, total / np.maximum(share, 1e-6), np.nan).astype(np.float32)


def region_settings(region):
    """Threshold (NaN = no rain test), speed and margin per cell."""
    threshold = np.full(region.shape, np.nan, np.float32)
    speed = np.ones(region.shape, np.float32)
    margin = np.zeros(region.shape, np.float32)
    for code, (_, mm, v, x) in REGIONS.items():
        sel = region == code
        threshold[sel] = np.nan if mm is None else mm
        speed[sel], margin[sel] = v, x
    return threshold, speed, margin


def latitude_limit(kerala, lat, speed):
    return kerala + np.maximum(0, lat - LATITUDE_FROM) / speed


def first_pair(area, first_doy, threshold, earliest):
    """Onset day of year and source per cell: the 2nd of the first 2 days in a row, from the earliest
    day (rounded up) on, with area rain >= threshold; the earliest day where the threshold is NaN or
    no pair comes by the last day. area: (days, ...) from day of year first_doy."""
    n_days = area.shape[0]
    start = np.clip(np.ceil(earliest).astype(int) - first_doy, 1, n_days - 1)
    with np.errstate(invalid="ignore"):
        wet = area >= threshold
    pair = np.zeros_like(wet)
    pair[1:] = wet[1:] & wet[:-1]
    pair &= np.arange(n_days).reshape((-1,) + (1,) * start.ndim) >= start
    found = pair.any(axis=0)
    no_test = np.isnan(threshold)
    day = np.where(found & ~no_test, pair.argmax(axis=0), start) + first_doy
    source = np.where(no_test, SOURCE_NO_RAIN_TEST, np.where(found, SOURCE_RAIN, SOURCE_FALLBACK))
    return day.astype(np.float32), source.astype(np.int16)


# ---------------------------------------------------------------- one year


def onset_days(year):
    """Days read for the onset: the first day of week 19 (7 May) to 31 August."""
    first, last = week_start(year, ONSET_FIRST_WEEK), dt.date(year, *LAST_DAY)
    return [first + dt.timedelta(days=i) for i in range((last - first).days + 1)]


def cell_lat(transform, shape):
    return np.repeat((transform.f + (np.arange(shape[0]) + 0.5) * transform.e)[:, None], shape[1], axis=1)


def station_cells(transform):
    cells = [~transform @ xy for xy in KERALA_STATIONS.values()]
    return np.array([int(r) for _, r in cells]), np.array([int(c) for c, _ in cells])


def year_rain(paths, year, read_day, india, transform):
    """Kerala date (day of year), area rain (days, r, c) and the day of year of its first day."""
    days = onset_days(year)
    daily = np.stack([read_day(paths, d) for d in days])
    rows, cols = station_cells(transform)
    kerala = kerala_date(daily[:, rows, cols], days)
    return kerala, area_rain(daily, india), days[0].timetuple().tm_yday


def _lag_job(args):
    paths, year, read_day = args
    transform, crs, shape = raster_profile(regions_file(paths))
    region, india = read_regions(paths)
    kerala, area, first_doy = year_rain(paths, year, read_day, india, transform)
    threshold, speed, _ = region_settings(region)
    day, _ = first_pair(area, first_doy, threshold, latitude_limit(kerala, cell_lat(transform, shape), speed))
    lag = np.where(region > 0, day - kerala, np.nan).astype(np.float32)
    write_stack(lag_file(paths, year), lag, transform, crs, ["lag_days"], np.nan,
                {"year": year, "kerala_day_of_year": kerala, "onset_rule": RULE, "settings": settings_tag()})
    return year


def lag_is_current(paths, year):
    path = lag_file(paths, year)
    if not path.is_file():
        return False
    tags = read_tags(path)
    return tags.get("onset_rule") == RULE and tags.get("settings") == settings_tag()


def ensure_lags(paths, years, read_day, workers=8):
    """Write onset_lag/<year>.tif for every year that lacks it or has it from other settings."""
    todo = [(paths, y, read_day) for y in years if not lag_is_current(paths, y)]
    if not todo:
        return
    log.info("Onset lags: %d years with %d workers", len(todo), workers)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for year in pool.map(_lag_job, todo):
            log.info("Onset lag %s written", year)


def usual_lag(paths, season):
    """Median lag over RAIN_RECORD_START to season - 1 (0 where there is none)."""
    years = range(RAIN_RECORD_START, season)
    if not years:
        return 0.0
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        lag = np.nanmedian(np.stack([read_bands(lag_file(paths, y))[0] for y in years]), axis=0)
    return np.nan_to_num(lag)


def season_onset(paths, season, read_day):
    """Onset of one season per cell. Needs ensure_regions, and ensure_lags for the years before the
    season. Returns (r, c) arrays day (day of year), week, source and earliest (0 or NaN outside
    land), the region raster and India mask, and the Kerala date."""
    transform, _, shape = raster_profile(regions_file(paths))
    region, india = read_regions(paths)
    kerala, area, first_doy = year_rain(paths, season, read_day, india, transform)
    threshold, speed, margin = region_settings(region)
    floor = np.maximum(kerala + usual_lag(paths, season) - margin, kerala)
    earliest = np.ceil(np.maximum(latitude_limit(kerala, cell_lat(transform, shape), speed), floor))
    day, source = first_pair(area, first_doy, threshold, earliest)
    land = region > 0
    week_by_doy = np.array([week_of(dt.date(season, 1, 1) + dt.timedelta(days=i)) for i in range(366)])
    return {"day": np.where(land, day, np.nan),
            "week": np.where(land, week_by_doy[day.astype(int) - 1], 0),
            "source": np.where(land, source, 0),
            "earliest": np.where(land, earliest, np.nan),
            "region": region, "india": india,
            "kerala": dt.date(season, 1, 1) + dt.timedelta(days=kerala - 1)}
