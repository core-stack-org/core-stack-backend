"""Rainfall stage, computed on the CHIRPS 0.05 degree grid (about 5.5 km) for all of India.

Every 500 m pixel inside a CHIRPS pixel gets that pixel's rainfall (nearest
neighbour, as in GEE), so computing here and copying the results to the 500 m
grid later gives the same answer as computing at 500 m, with about 60 times less work.

Season Y is compared with the CHIRPS record from 1981 to Y-1 (as in GEE, without
the season's own year): the weekly normals, the dry-week test, the deviation and
the SPI gamma fits all use those years.

Onset: daily rainfall rule v1.2 (onset.py). Dry spells count from the onset week.

Files, under drought/<version>/rain/:
  weekly/<year>.tif     rainfall per standard meteorological week (52 bands, mm)
  seasons/<year>.tif    onset, dry-spell weeks, and per window: trigger, deviation, SPI, dry spell
  onset_regions.tif, onset_lag/<year>.tif   onset inputs (see onset.py)
"""

import datetime as dt
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from scipy.special import gammainc, ndtri

from computing.drought.local import onset, rules
from computing.drought.local.config import (
    ONSET_FIRST_WEEK,
    RAIN_RECORD_START,
    SEASON_LAST_WEEK,
    WINDOW_WEEKS,
    rain_normal_years,
)
from computing.drought.local.store import read_bands, read_tags, to_int16, write_stack
from computing.drought.local.weeks import WEEKS_PER_YEAR, season_window_starts, week_end, week_start

log = logging.getLogger(__name__)

CHIRPS_NODATA = -9999.0
WINDOW_STARTS = list(season_window_starts(ONSET_FIRST_WEEK))  # weeks 19 to 40
SEASON_WEEKS = list(range(ONSET_FIRST_WEEK, SEASON_LAST_WEEK + 1))  # weeks 19 to 43
SEASON_SPAN = slice(ONSET_FIRST_WEEK - 1, SEASON_LAST_WEEK)  # their rows in a 52-week stack


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


def deviation_pct(total, normal_total):
    """Rainfall deviation from normal, %; NaN where the normal is 0."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(normal_total > 0, (total - normal_total) / normal_total * 100, np.nan)


def season_dry_spells(dry, onset_week):
    """Weeks inside dry spells and the longest dry spell over weeks 19-43, counting dry weeks
    (52, ...) from onset_week on (inf: none)."""
    week_numbers = np.arange(1, WEEKS_PER_YEAR + 1).reshape((-1,) + (1,) * np.ndim(onset_week))
    after_onset = (dry & (week_numbers >= onset_week))[SEASON_SPAN]
    return rules.dry_spell_weeks(after_onset), rules.longest_dry_spell(after_onset)


def history_window_totals(history, start_week):
    """4-week totals of the past years for the window starting in start_week;
    history holds weeks 19-43 of each past year, shape (years, 25, ...)."""
    first = start_week - ONSET_FIRST_WEEK
    return history[:, first:first + WINDOW_WEEKS].sum(axis=1)


# ---------------------------------------------------------------- season


def season_rain(paths, year, normals, history):
    """Onset, dry-spell weeks and the per-window rainfall trigger for one season.
    normals: the 52 weekly means over the past years; history: weeks 19-43 of each past year."""
    transform, crs, _ = chirps_grid(paths)
    first_year, last_year = rain_normal_years(year)
    weekly = read_weekly(paths, year)
    on = onset.season_onset(paths, year, _read_day)
    land = ~np.isnan(normals).all(axis=0) & (on["region"] > 0)  # CHIRPS land
    onset_week = np.where(land, on["week"], 0).astype(np.int16)

    dry = rules.dry_weeks(weekly, normals)  # (52, r, c)
    dry_spell_weeks, longest_dry_spell = season_dry_spells(dry, np.where(land, onset_week, np.inf))

    totals = window_totals(weekly)
    normal_totals = window_totals(normals)
    bands = [onset_week, to_int16(on["day"]), on["source"].astype(np.int16), to_int16(on["earliest"]),
             on["region"].astype(np.int16), dry_spell_weeks.astype(np.int16), longest_dry_spell.astype(np.int16)]
    names = ["onset_week", "onset_day_of_year", "onset_source", "onset_earliest_day", "onset_region",
             "dry_spell_weeks", "longest_dry_spell_weeks"]
    for i, w in enumerate(WINDOW_STARTS):
        deviation = deviation_pct(totals[i], normal_totals[i])
        index = spi(totals[i], *gamma_fit(history_window_totals(history, w)))
        dry_spell = rules.dry_spell_in_window(dry[w - 1:w - 1 + WINDOW_WEEKS])
        trigger = rules.rain_trigger(dry_spell, deviation, index)
        bands += [trigger.astype(np.int16), to_int16(deviation), to_int16(index, 100), dry_spell.astype(np.int16)]
        names += [f"trigger_w{w:02d}", f"deviation_pct_w{w:02d}", f"spi_x100_w{w:02d}", f"dry_spell_w{w:02d}"]

    data = np.stack(bands).astype(np.int16)
    data[:, ~land] = -32768
    years = f"{first_year}-{last_year}"
    write_stack(season_file(paths, year), data, transform, crs, names, -32768,
                {"season": year, "rain_normal_years": years, "onset_rule": onset.RULE,
                 "onset_settings": onset.settings_tag(), "kerala_onset": on["kerala"].isoformat(),
                 "onset_source": onset.SOURCE_NOTE,
                 "onset_region": ", ".join(f"{code} {r[0]}" for code, r in onset.REGIONS.items()),
                 "deviation": "% of normal, window total", "spi": f"x100, gamma fit {years}"})
    india = land & on["india"]
    return {"normal_years": years, "kerala_onset": on["kerala"].isoformat(),
            "onset_rain_share": round(float((on["source"][india] == onset.SOURCE_RAIN).mean()), 4),
            "onset_fallback_cells": int((on["source"][india] == onset.SOURCE_FALLBACK).sum())}


def season_is_current(paths, year):
    """True when the season file exists and was made with this onset rule and these normal years."""
    path = season_file(paths, year)
    if not path.is_file():
        return False
    tags = read_tags(path)
    first_year, last_year = rain_normal_years(year)
    return (tags.get("rain_normal_years") == f"{first_year}-{last_year}" and tags.get("onset_rule") == onset.RULE
            and tags.get("onset_settings") == onset.settings_tag())


def run(paths, seasons, workers=8):
    """Rainfall stage for the given seasons whose file is missing or made under an older rule.
    Returns {season: summary}."""
    todo = sorted(s for s in seasons if not season_is_current(paths, s))
    if not todo:
        return {}
    first = RAIN_RECORD_START
    ensure_weekly(paths, range(first, todo[-1]), workers=workers)  # whole past years
    ensure_weekly(paths, todo, last_week=SEASON_LAST_WEEK, workers=workers)
    onset.ensure_regions(paths, _read_day)
    onset.ensure_lags(paths, range(first, todo[-1]), _read_day, workers=workers)

    # Read each past year once: a running sum for the normals and weeks 19-43 for SPI.
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
