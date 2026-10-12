"""Rainfall stage, computed on the CHIRPS 0.05 degree grid (about 5.5 km) for all of India.

Every 500 m pixel inside a CHIRPS pixel gets that pixel's rainfall (nearest
neighbour, as in GEE), so computing here and copying the results to the 500 m
grid later gives the same answer as computing at 500 m, with about 60 times less work.

Season Y is compared with the CHIRPS record from 1981 to Y-1 (as in GEE, without
the season's own year): the weekly normals, the dry-week test, the deviation and
the SPI gamma fits all use those years.

Onset, per cell (writeup 3.2.2.3, with the GEE code's zone percentiles):
  1. threshold: the zone's percentile of the cell's 52 weekly normals
  2. onset: the first week from 19 to 43 whose rain this year is above the threshold
  3. if there is none: the median of the past years' onset weeks, found the same way
     (same threshold; years without one left out; the later middle week when their count is even)
  4. if no past year has one either: the onset of the nearest cell that has one

Files, under drought/<version>/rain/:
  weekly/<year>.tif     rainfall per standard meteorological week (52 bands, mm)
  seasons/<year>.tif    onset, dry-spell weeks, and per window: trigger, deviation, SPI, dry spell
"""

import datetime as dt
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from scipy.spatial import cKDTree
from scipy.special import gammainc, ndtri

from computing.drought.local import rules
from computing.drought.local.config import (
    ONSET_FIRST_WEEK,
    RAIN_RECORD_START,
    SEASON_LAST_WEEK,
    WINDOW_WEEKS,
    rain_normal_years,
)
from computing.drought.local.store import read_bands, read_tags, to_int16, write_stack
from computing.drought.local.weeks import WEEKS_PER_YEAR, season_window_starts, week_end, week_of, week_start

log = logging.getLogger(__name__)

CHIRPS_NODATA = -9999.0
RAINY_DAY_MM = 2.5  # IMD's definition of a rainy day
WINDOW_STARTS = list(season_window_starts(ONSET_FIRST_WEEK))  # weeks 19 to 40
SEASON_WEEKS = list(range(ONSET_FIRST_WEEK, SEASON_LAST_WEEK + 1))  # weeks 19 to 43
SEASON_SPAN = slice(ONSET_FIRST_WEEK - 1, SEASON_LAST_WEEK)  # their rows in a 52-week stack

# Onset source codes stored in the season file.
ONSET_THIS_YEAR, ONSET_PAST_MEDIAN, ONSET_NEAREST = 1, 2, 3
ONSET_RULE = "this year; else median of past onsets; else nearest cell"

# Hydro-meteorological zones (writeup figure 3.20, same rectangles as the GEE pipeline):
# name, west, south, east, north, percentile of the normal weekly rainfall used as onset threshold.
HMZ = [
    ("northern", 69.17606600131256, 26.132493256032653, 82.09598787631256, 37.50688033728282, 75),
    ("western", 67.68192537631256, 18.36801791110178, 76.00956209506256, 26.148475498372616, 75),
    ("central", 76.0413365622055, 18.346936996159776, 82.0398717184555, 26.089074629747927, 69),
    ("eastern", 82.15028471689531, 18.390939094394867, 99.33290190439531, 30.784246603238188, 65),
    ("southern", 72.28204341674216, 7.596336766662568, 84.43292232299216, 18.348999397274717, 50),
]


# ---------------------------------------------------------------- paths and grid


def rain_dir(paths):
    return paths.shared_dir / "rain"


def weekly_file(paths, year):
    return rain_dir(paths) / "weekly" / f"{year}.tif"


def season_file(paths, year):
    return rain_dir(paths) / "seasons" / f"{year}.tif"


def chirps_grid(paths):
    """Transform, CRS and shape of the CHIRPS India window the downloader stores."""
    with rasterio.open(paths.chirps_day(dt.date(RAIN_RECORD_START, 1, 1))) as src:
        return src.transform, src.crs, (src.height, src.width)


def cell_lonlat(transform, shape):
    cols = np.arange(shape[1]) + 0.5
    rows = np.arange(shape[0]) + 0.5
    return np.meshgrid(transform.c + cols * transform.a, transform.f + rows * transform.e)


def zone_percentile(lon, lat):
    """Onset percentile per point: from the first zone rectangle (in the order above) that
    contains it, or from the nearest rectangle for points outside all five."""
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    dist = np.stack([
        np.hypot(np.maximum(np.maximum(west - lon, lon - east), 0), np.maximum(np.maximum(south - lat, lat - north), 0))
        for _, west, south, east, north, _ in HMZ
    ])
    inside = dist == 0
    zone = np.where(inside.any(axis=0), inside.argmax(axis=0), dist.argmin(axis=0))
    return np.array([z[5] for z in HMZ], dtype=float)[zone]


# ---------------------------------------------------------------- weekly totals


def _read_day(paths, day):
    with rasterio.open(paths.chirps_day(day)) as src:
        data = src.read(1).astype(np.float32)
    data[data == CHIRPS_NODATA] = np.nan
    return data


def compute_weekly(paths, year, last_week=WEEKS_PER_YEAR):
    """Rainfall per week 1..last_week (later weeks NaN), shape (52, rows, cols)."""
    _, _, shape = chirps_grid(paths)
    out = np.full((WEEKS_PER_YEAR,) + shape, np.nan, dtype=np.float32)
    for week in range(1, last_week + 1):
        total = np.zeros(shape, dtype=np.float32)
        day = week_start(year, week)
        while day <= week_end(year, week):
            total += _read_day(paths, day)
            day += dt.timedelta(days=1)
        out[week - 1] = total
    return out


def _weekly_job(args):
    data_dir, lulc_dir, year, last_week = args
    from computing.drought.local.config import Paths

    paths = Paths(data_dir, lulc_dir)
    data = compute_weekly(paths, year, last_week)
    transform, crs, _ = chirps_grid(paths)
    names = [f"week_{w:02d}" for w in range(1, WEEKS_PER_YEAR + 1)]
    write_stack(weekly_file(paths, year), data, transform, crs, names, np.nan,
                {"year": year, "last_week": last_week, "units": "mm"})
    return year


def ensure_weekly(paths, years, last_week=WEEKS_PER_YEAR, workers=8):
    """Write weekly/<year>.tif for every year that lacks it (or has fewer weeks)."""
    todo = []
    for year in years:
        path = weekly_file(paths, year)
        if path.is_file() and int(read_tags(path).get("last_week", 0)) >= last_week:
            continue
        todo.append((paths.data_dir, paths.lulc_dir, year, last_week))
    if not todo:
        return
    log.info("Weekly rainfall: %d years with %d workers", len(todo), workers)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for year in pool.map(_weekly_job, todo):
            log.info("Weekly rainfall %s written", year)


def read_weekly(paths, year):
    return read_bands(weekly_file(paths, year))


# ---------------------------------------------------------------- normals and SPI fit


def window_totals(weekly, starts=WINDOW_STARTS):
    """4-week totals for each window start week, shape (len(starts), rows, cols)."""
    return np.stack([weekly[w - 1:w - 1 + WINDOW_WEEKS].sum(axis=0) for w in starts])


def gamma_fit(totals):
    """Gamma fit per pixel of totals (years, ...) with Thom's maximum-likelihood
    approximation, as in standard SPI. Returns alpha, beta and the share of zero totals."""
    totals = np.asarray(totals, dtype=np.float64)
    years = totals.shape[0]
    rain = totals > 0
    n_rain = rain.sum(axis=0)
    zero_share = (years - n_rain) / years
    with warnings.catch_warnings(), np.errstate(divide="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        values = np.where(rain, totals, np.nan)
        mean = np.nanmean(values, axis=0)
        a = np.log(mean) - np.nanmean(np.log(values), axis=0)
        alpha = (1 + np.sqrt(1 + 4 * a / 3)) / (4 * a)
        beta = mean / alpha
    bad = (n_rain < 3) | ~(a > 0) | np.isnan(totals).any(axis=0)
    alpha[bad], beta[bad] = np.nan, np.nan
    return alpha, beta, zero_share


def spi(total, alpha, beta, zero_share):
    """Standardised precipitation index of total against a fitted gamma distribution."""
    total = np.asarray(total, dtype=np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        cdf = np.where(total > 0, zero_share + (1 - zero_share) * gammainc(alpha, total / beta), zero_share)
    value = ndtri(np.clip(cdf, 1e-6, 1 - 1e-6))
    value[np.isnan(alpha) | np.isnan(total)] = np.nan
    return value


def history_window_totals(history, start_week):
    """4-week totals of the past years for the window starting in start_week;
    history holds weeks 19-43 of each past year, shape (years, 25, ...)."""
    first = start_week - ONSET_FIRST_WEEK
    return history[:, first:first + WINDOW_WEEKS].sum(axis=1)


# ---------------------------------------------------------------- onset


def onset_threshold(normals, pct):
    """Each cell's onset threshold: its zone's percentile of the cell's 52 weekly normals."""
    threshold = np.full(pct.shape, np.nan)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for p in np.unique(pct[~np.isnan(pct)]):
            sel = pct == p
            threshold[sel] = np.nanpercentile(normals[:, sel], p, axis=0)
    return threshold


def first_week_above(season_weeks, threshold, axis=0):
    """First week (19-43) whose rain is above the threshold; NaN where no week is.
    season_weeks holds weeks 19-43 along axis."""
    with np.errstate(invalid="ignore"):
        above = np.asarray(season_weeks) > threshold
    return np.where(above.any(axis=axis), above.argmax(axis=axis) + ONSET_FIRST_WEEK, np.nan)


def upper_median(values, axis=0):
    """Median that takes the later of the two middle values when their count is even.
    NaN values are left out; the result is NaN where nothing is left."""
    values = np.sort(np.asarray(values, dtype=np.float64), axis=axis)  # NaN sort last
    count = (~np.isnan(values)).sum(axis=axis)
    index = np.expand_dims(np.minimum(count // 2, values.shape[axis] - 1), axis)
    middle = np.take_along_axis(values, index, axis).squeeze(axis)
    return np.where(count > 0, middle, np.nan)


def _unit_vectors(lon, lat):
    lon, lat = np.radians(lon), np.radians(lat)
    return np.column_stack([np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)])


def nearest_onset(week, lon, lat, missing):
    """Onset week of the nearest cell that has one, for each missing cell (great-circle distance).
    When several cells are equally near, the upper median of their weeks."""
    donors = ~np.isnan(week) & ~missing
    if not donors.any():
        raise ValueError("no cell has an onset to copy")
    k = int(min(8, donors.sum()))
    tree = cKDTree(_unit_vectors(lon[donors], lat[donors]))
    dist, index = tree.query(_unit_vectors(lon[missing], lat[missing]), k=k)
    dist, index = dist.reshape(-1, k), index.reshape(-1, k)
    tied = dist <= dist[:, :1] * (1 + 1e-9) + 1e-15
    return upper_median(np.where(tied, week[donors][index], np.nan), axis=1)


def season_onset(this_year, history, threshold, land, lon, lat):
    """Onset week and its source code per cell (NaN and 0 outside land).
    this_year: weeks 19-43 of the season, shape (25, r, c); history: the same weeks of
    every past year, shape (years, 25, r, c)."""
    week = np.where(land, first_week_above(this_year, threshold), np.nan)
    source = np.where(np.isnan(week), 0, ONSET_THIS_YEAR).astype(np.int16)

    todo = land & np.isnan(week)
    if todo.any():
        past = first_week_above(history[:, :, todo], threshold[todo], axis=1)  # (years, cells)
        median = upper_median(past, axis=0)
        week[todo] = median
        source[todo] = np.where(np.isnan(median), 0, ONSET_PAST_MEDIAN)

    missing = land & np.isnan(week)
    if missing.any():
        week[missing] = nearest_onset(week, lon, lat, missing)
        source[missing] = ONSET_NEAREST
    return week, source


def onset_day(paths, year, week):
    """Day of year of the first rainy day (2.5 mm or more) in each pixel's onset week;
    the first day with any rain if none reaches 2.5 mm; else the week's first day.
    Pixels with week 0 (outside land) get 0."""
    first = week_start(year, ONSET_FIRST_WEEK)
    days = [first + dt.timedelta(days=i) for i in range((week_end(year, SEASON_LAST_WEEK) - first).days + 1)]
    out = np.zeros(week.shape, dtype=np.int16)
    found_rainy = np.zeros(week.shape, dtype=bool)
    found_any = np.zeros(week.shape, dtype=bool)
    first_any = np.zeros(week.shape, dtype=np.int16)
    for day in days:
        in_week = week == week_of(day)
        if not in_week.any():
            continue
        rain = _read_day(paths, day)
        doy = day.timetuple().tm_yday
        rainy = in_week & ~found_rainy & (np.nan_to_num(rain) >= RAINY_DAY_MM)
        out[rainy], found_rainy[rainy] = doy, True
        wet = in_week & ~found_any & (np.nan_to_num(rain) > 0)
        first_any[wet], found_any[wet] = doy, True
    use_any = ~found_rainy & found_any
    out[use_any] = first_any[use_any]
    none = ~found_rainy & ~found_any & (week > 0)
    for w in np.unique(week[none]):
        out[none & (week == w)] = week_start(year, int(w)).timetuple().tm_yday
    return out


# ---------------------------------------------------------------- season


def season_rain(paths, year, normals, history):
    """Onset, dry-spell weeks and the per-window rainfall trigger for one season.
    normals: the 52 weekly means over the past years; history: weeks 19-43 of each past year."""
    transform, crs, shape = chirps_grid(paths)
    first_year, last_year = rain_normal_years(year)
    weekly = read_weekly(paths, year)
    lon, lat = cell_lonlat(transform, shape)
    pct = zone_percentile(lon, lat)
    pct[np.isnan(normals).all(axis=0)] = np.nan  # outside CHIRPS land
    land = ~np.isnan(pct)

    threshold = onset_threshold(normals, pct)
    onset, source = season_onset(weekly[SEASON_SPAN], history, threshold, land, lon, lat)
    onset_week = np.where(land, onset, 0).astype(np.int16)
    doy = onset_day(paths, year, onset_week)

    dry = rules.dry_weeks(weekly, normals)  # (52, r, c)
    week_numbers = np.arange(1, WEEKS_PER_YEAR + 1)[:, None, None]
    after_onset = dry & land & (week_numbers >= onset_week)
    dry_spell_weeks = rules.dry_spell_weeks(after_onset[SEASON_SPAN])
    longest_dry_spell = rules.longest_dry_spell(after_onset[SEASON_SPAN])

    totals = window_totals(weekly)
    normal_totals = window_totals(normals)
    bands = [onset_week, doy, source, to_int16(threshold, 10), dry_spell_weeks.astype(np.int16),
             longest_dry_spell.astype(np.int16)]
    names = ["onset_week", "onset_day_of_year", "onset_source", "onset_threshold_x10",
             "dry_spell_weeks", "longest_dry_spell_weeks"]
    for i, w in enumerate(WINDOW_STARTS):
        with np.errstate(divide="ignore", invalid="ignore"):
            deviation = np.where(normal_totals[i] > 0, (totals[i] - normal_totals[i]) / normal_totals[i] * 100, np.nan)
        index = spi(totals[i], *gamma_fit(history_window_totals(history, w)))
        dry_spell = rules.dry_spell_in_window(dry[w - 1:w - 1 + WINDOW_WEEKS])
        trigger = rules.rain_trigger(dry_spell, deviation, index)
        bands += [trigger.astype(np.int16), to_int16(deviation), to_int16(index, 100), dry_spell.astype(np.int16)]
        names += [f"trigger_w{w:02d}", f"deviation_pct_w{w:02d}", f"spi_x100_w{w:02d}", f"dry_spell_w{w:02d}"]

    data = np.stack(bands).astype(np.int16)
    data[:, ~land] = -32768
    years = f"{first_year}-{last_year}"
    write_stack(season_file(paths, year), data, transform, crs, names, -32768,
                {"season": year, "rain_normal_years": years, "onset_rule": ONSET_RULE,
                 "onset_source": "1 this year, 2 median of past onsets, 3 nearest cell",
                 "onset_threshold": "mm per week x10", "deviation": "% of normal, window total",
                 "spi": f"x100, gamma fit {years}"})
    return {"normal_years": years,
            "onset_this_year_share": round(float((source[land] == ONSET_THIS_YEAR).mean()), 4),
            "onset_past_median_cells": int((source == ONSET_PAST_MEDIAN).sum()),
            "onset_nearest_cells": int((source == ONSET_NEAREST).sum())}


def season_is_current(paths, year):
    """True when the season file exists and was made with this rule and these normal years."""
    path = season_file(paths, year)
    if not path.is_file():
        return False
    tags = read_tags(path)
    first_year, last_year = rain_normal_years(year)
    return tags.get("rain_normal_years") == f"{first_year}-{last_year}" and tags.get("onset_rule") == ONSET_RULE


def run(paths, seasons, workers=8):
    """Rainfall stage for the given seasons whose file is missing or made under an older rule.
    Returns {season: summary}."""
    todo = sorted(s for s in seasons if not season_is_current(paths, s))
    if not todo:
        return {}
    first = RAIN_RECORD_START
    ensure_weekly(paths, range(first, todo[-1]), workers=workers)  # whole past years
    ensure_weekly(paths, todo, last_week=SEASON_LAST_WEEK, workers=workers)

    # Read each past year once: a running sum for the normals and weeks 19-43 for SPI and onset.
    _, _, shape = chirps_grid(paths)
    history = np.empty((todo[-1] - first, len(SEASON_WEEKS)) + shape, dtype=np.float32)
    total = np.zeros((WEEKS_PER_YEAR,) + shape, dtype=np.float64)
    loaded, done = 0, {}
    for year in todo:
        while first + loaded < year:
            weekly = read_weekly(paths, first + loaded)
            total += weekly
            history[loaded] = weekly[SEASON_SPAN]
            loaded += 1
        log.info("Rainfall season %s against %s-%s", year, first, year - 1)
        normals = (total / loaded).astype(np.float32)
        done[year] = season_rain(paths, year, normals, history[:loaded])
    return done
