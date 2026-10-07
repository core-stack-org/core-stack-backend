"""Rainfall stage, computed on the CHIRPS 0.05 degree grid (about 5.5 km) for all of India.

Every 500 m pixel inside a CHIRPS pixel gets that pixel's rainfall (nearest
neighbour, as in GEE), so computing here and copying the results to the 500 m
grid later gives the same answer as computing at 500 m, with about 60 times less work.

Files, under drought/<version>/rain/:
  weekly/<year>.tif     rainfall per standard meteorological week (52 bands, mm)
  normals.tif           1991-2020 mean of each week (52 bands, mm)
  spi_fit.tif           gamma fit of 1991-2020 4-week totals, per window start week
  seasons/<year>.tif    onset, dry-spell weeks, and per window: trigger, deviation, SPI, dry spell
"""

import datetime as dt
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import rasterio
from scipy.special import gammainc, ndtri

from computing.drought.local import rules
from computing.drought.local.config import (
    ONSET_FIRST_WEEK,
    RAIN_NORMAL_YEARS,
    SEASON_LAST_WEEK,
    WINDOW_WEEKS,
)
from computing.drought.local.store import read_bands, read_tags, to_int16, write_stack
from computing.drought.local.weeks import WEEKS_PER_YEAR, season_window_starts, week_end, week_of, week_start

log = logging.getLogger(__name__)

CHIRPS_NODATA = -9999.0
RAINY_DAY_MM = 2.5  # IMD's definition of a rainy day
WINDOW_STARTS = list(season_window_starts(ONSET_FIRST_WEEK))  # weeks 19 to 40
SEASON_WEEKS = list(range(ONSET_FIRST_WEEK, SEASON_LAST_WEEK + 1))  # weeks 19 to 43

# Onset source codes stored in the season file.
ONSET_THIS_YEAR, ONSET_NORMAL, ONSET_DEFAULT = 1, 2, 3

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


def normals_file(paths):
    return rain_dir(paths) / "normals.tif"


def spi_fit_file(paths):
    return rain_dir(paths) / "spi_fit.tif"


def season_file(paths, year):
    return rain_dir(paths) / "seasons" / f"{year}.tif"


def chirps_grid(paths):
    """Transform, CRS and shape of the CHIRPS India window the downloader stores."""
    with rasterio.open(paths.chirps_day(dt.date(RAIN_NORMAL_YEARS[0], 1, 1))) as src:
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


def ensure_baselines(paths, workers=8):
    """Weekly totals for the normal years, the weekly normals, and the SPI fits."""
    first, last = RAIN_NORMAL_YEARS
    years = range(first, last + 1)
    ensure_weekly(paths, years, workers=workers)
    transform, crs, _ = chirps_grid(paths)
    tags = {"years": f"{first}-{last}"}

    if not normals_file(paths).is_file():
        log.info("Rainfall normals %s-%s", first, last)
        stack = np.stack([read_weekly(paths, y) for y in years])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            normals = np.nanmean(stack, axis=0).astype(np.float32)
        names = [f"week_{w:02d}" for w in range(1, WEEKS_PER_YEAR + 1)]
        write_stack(normals_file(paths), normals, transform, crs, names, np.nan, {**tags, "units": "mm"})

    if not spi_fit_file(paths).is_file():
        log.info("SPI gamma fits %s-%s for window start weeks %s-%s", first, last, WINDOW_STARTS[0], WINDOW_STARTS[-1])
        totals = np.stack([window_totals(read_weekly(paths, y)) for y in years])  # (years, windows, r, c)
        bands, names = [], []
        for i, w in enumerate(WINDOW_STARTS):
            alpha, beta, zero_share = gamma_fit(totals[:, i])
            bands += [alpha, beta, zero_share]
            names += [f"alpha_w{w:02d}", f"beta_w{w:02d}", f"zero_share_w{w:02d}"]
        write_stack(spi_fit_file(paths), np.stack(bands).astype(np.float32), transform, crs, names, np.nan, tags)


# ---------------------------------------------------------------- onset


def onset_week(weekly, normals, pct):
    """First week from week 19 whose rainfall exceeds the pixel's onset threshold (the zone's
    percentile of its normal weekly rainfall). If the season never exceeds it, the week the
    normal year would, else week 19. Returns (week, source)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        threshold = np.full(pct.shape, np.nan)
        for p in np.unique(pct[~np.isnan(pct)]):
            sel = pct == p
            threshold[sel] = np.nanpercentile(normals[:, sel], p, axis=0)
    span = slice(ONSET_FIRST_WEEK - 1, SEASON_LAST_WEEK)
    with np.errstate(invalid="ignore"):
        this_year = weekly[span] > threshold
        normal_year = normals[span] > threshold
    week = np.where(
        this_year.any(axis=0),
        this_year.argmax(axis=0) + ONSET_FIRST_WEEK,
        np.where(normal_year.any(axis=0), normal_year.argmax(axis=0) + ONSET_FIRST_WEEK, ONSET_FIRST_WEEK),
    )
    source = np.where(this_year.any(axis=0), ONSET_THIS_YEAR,
                      np.where(normal_year.any(axis=0), ONSET_NORMAL, ONSET_DEFAULT))
    return week.astype(np.int16), source.astype(np.int16)


def onset_day(paths, year, week):
    """Day of year of the first rainy day (2.5 mm or more) in each pixel's onset week;
    the first day with any rain if none reaches 2.5 mm; else the week's first day."""
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
    none = ~found_rainy & ~found_any
    for w in np.unique(week[none]):
        out[none & (week == w)] = week_start(year, int(w)).timetuple().tm_yday
    return out


# ---------------------------------------------------------------- season


def season_rain(paths, year):
    """Onset, dry-spell weeks and the per-window rainfall trigger for one season."""
    transform, crs, shape = chirps_grid(paths)
    weekly = read_weekly(paths, year)
    normals = read_bands(normals_file(paths))
    fit = read_bands(spi_fit_file(paths))
    lon, lat = cell_lonlat(transform, shape)
    pct = zone_percentile(lon, lat)
    pct[np.isnan(normals).all(axis=0)] = np.nan  # outside CHIRPS land

    onset, source = onset_week(weekly, normals, pct)
    doy = onset_day(paths, year, onset)

    dry = rules.dry_weeks(weekly, normals)  # (52, r, c)
    week_numbers = np.arange(1, WEEKS_PER_YEAR + 1)[:, None, None]
    after_onset = dry & (week_numbers >= onset)
    season = slice(ONSET_FIRST_WEEK - 1, SEASON_LAST_WEEK)
    dry_spell_weeks = rules.dry_spell_weeks(after_onset[season])
    longest_dry_spell = rules.longest_dry_spell(after_onset[season])

    totals = window_totals(weekly)
    normal_totals = window_totals(normals)
    bands = [onset, doy, source, dry_spell_weeks.astype(np.int16), longest_dry_spell.astype(np.int16)]
    names = ["onset_week", "onset_day_of_year", "onset_source", "dry_spell_weeks", "longest_dry_spell_weeks"]
    for i, w in enumerate(WINDOW_STARTS):
        with np.errstate(divide="ignore", invalid="ignore"):
            deviation = np.where(normal_totals[i] > 0, (totals[i] - normal_totals[i]) / normal_totals[i] * 100, np.nan)
        index = spi(totals[i], fit[3 * i], fit[3 * i + 1], fit[3 * i + 2])
        dry_spell = rules.dry_spell_in_window(dry[w - 1:w - 1 + WINDOW_WEEKS])
        trigger = rules.rain_trigger(dry_spell, deviation, index)
        bands += [trigger.astype(np.int16), to_int16(deviation), to_int16(index, 100), dry_spell.astype(np.int16)]
        names += [f"trigger_w{w:02d}", f"deviation_pct_w{w:02d}", f"spi_x100_w{w:02d}", f"dry_spell_w{w:02d}"]

    land = ~np.isnan(pct)
    data = np.stack(bands).astype(np.int16)
    data[:, ~land] = -32768
    write_stack(season_file(paths, year), data, transform, crs, names, -32768,
                {"season": year, "rain_normal_years": f"{RAIN_NORMAL_YEARS[0]}-{RAIN_NORMAL_YEARS[1]}",
                 "deviation": "% of normal, window total", "spi": "x100, gamma fit 1991-2020"})
    return {"onset_this_year_share": round(float((source[land] == ONSET_THIS_YEAR).mean()), 3)}


def run(paths, seasons, workers=8):
    """Rainfall stage for the given seasons. Returns {season: summary}."""
    ensure_baselines(paths, workers)
    ensure_weekly(paths, seasons, last_week=SEASON_LAST_WEEK, workers=workers)
    done = {}
    for year in seasons:
        if season_file(paths, year).is_file():
            continue
        log.info("Rainfall season %s", year)
        done[year] = season_rain(paths, year)
    return done
