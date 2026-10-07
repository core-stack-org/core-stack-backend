"""Classification stage: weekly drought labels and the season summary, on the 500 m grid.

For every window from the onset week (rainfall stage) to the season's end, a
cropland pixel's class follows the 2020 rule (rules.window_class): the rainfall
trigger (copied from the CHIRPS cell the pixel lies in) gates VCI, MAI and area
sown. Each week then takes the worst class of the windows it falls in.

Files, under drought/<region>/<version>/:
  classified/labels_<season>.tif   one band per week 19-43: 0 none, 1 mild, 2 moderate,
                                   3 severe; 255 outside the season or not cropland
  classified/summary_<season>.tif  weeks of none / mild / moderate / severe drought,
                                   weeks in the season, dry-spell weeks (total and longest),
                                   onset week, cropland, windows whose VCI used cloud-flagged images
  published/drought_<season>.tif   the summary in EPSG:4326 (nearest neighbour), for GeoServer
"""

import logging

import numpy as np
import rasterio
from rasterio.warp import Resampling, calculate_default_transform, reproject
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

from computing.drought.local import cropland, moisture, rainfall, rules, vegetation
from computing.drought.local.config import ONSET_FIRST_WEEK, SEASON_LAST_WEEK
from computing.drought.local.grid import load_grid, lookup_index, pixel_lonlat
from computing.drought.local.store import band_names, read_bands, write_stack

log = logging.getLogger(__name__)

UINT8_NODATA = 255
WINDOW_STARTS = rainfall.WINDOW_STARTS
SEASON_WEEKS = list(range(ONSET_FIRST_WEEK, SEASON_LAST_WEEK + 1))
STRIP_ROWS = 512
PUBLISH_RESOLUTION = 1 / 240  # degrees, 15 arc-seconds, about 460 m

SUMMARY_NAMES = ["weeks_none", "weeks_mild", "weeks_moderate", "weeks_severe", "season_weeks",
                 "dry_spell_weeks", "longest_dry_spell_weeks", "onset_week", "cropland",
                 "windows_cloud_fallback"]


def labels_file(paths, season):
    return paths.version_dir / "classified" / f"labels_{season}.tif"


def summary_file(paths, season):
    return paths.version_dir / "classified" / f"summary_{season}.tif"


def published_file(paths, season):
    return paths.version_dir / "published" / f"drought_{season}.tif"


def inputs_ready(paths, season):
    return all(p.is_file() for p in (
        rainfall.season_file(paths, season), cropland.normal_file(paths), cropland.share_file(paths, season),
        vegetation.vci_file(paths, season), moisture.mai_file(paths, season)))


def classify_arrays(onset, triggers, vci, mai, sown, crop, fallback, dry_total, dry_longest):
    """onset (r, c) week or NaN; triggers, vci, mai, fallback: (windows, r, c); sown, crop: (r, c).
    Returns (labels (weeks 19-43, r, c), summary (len(SUMMARY_NAMES), r, c)), both uint8."""
    land = ~np.isnan(onset)
    onset = np.where(land, onset, np.inf)
    sown_cls = rules.sown_class(sown)
    classes, used_fallback = [], np.zeros(onset.shape, np.int32)
    for i, w in enumerate(WINDOW_STARTS):
        counted = w >= onset
        trigger = counted & (np.nan_to_num(triggers[i]) == 1)
        classes.append(rules.window_class(trigger, rules.vci_class(vci[i]), rules.mai_class(mai[i]), sown_cls))
        used_fallback += (counted & (np.nan_to_num(fallback[i]) == 1)).astype(np.int32)
    labels = rules.weekly_labels(np.stack(classes))  # weeks 19-43
    in_season = np.array(SEASON_WEEKS)[:, None, None] >= onset
    labels = np.where(in_season & crop, labels, UINT8_NODATA).astype(np.uint8)

    counts = [((labels == c) & in_season).sum(axis=0) for c in (rules.NONE, rules.MILD, rules.MODERATE, rules.SEVERE)]
    season_weeks = in_season.sum(axis=0)
    out = [*counts, season_weeks, np.nan_to_num(dry_total), np.nan_to_num(dry_longest),
           np.where(land, onset, 0), crop.astype(np.int64), used_fallback]
    summary = np.stack([np.asarray(a, dtype=np.float64) for a in out])
    summary[:4, ~crop] = UINT8_NODATA      # drought weeks only on cropland
    summary[9, ~crop] = UINT8_NODATA
    summary[:, ~land] = UINT8_NODATA       # outside CHIRPS land
    return labels, np.clip(summary, 0, UINT8_NODATA).astype(np.uint8)


def classify_season(paths, season, region):
    grid = load_grid(paths)
    chirps_transform, _, chirps_shape = rainfall.chirps_grid(paths)
    rain_path = rainfall.season_file(paths, season)
    rain = dict(zip(band_names(rain_path), read_bands(rain_path)))
    vci_path, mai_path = vegetation.vci_file(paths, season), moisture.mai_file(paths, season)
    vci_names = [f"vci_w{w:02d}" for w in WINDOW_STARTS]
    fallback_names = [f"cloud_fallback_w{w:02d}" for w in WINDOW_STARTS]
    mai_names = [f"mai_w{w:02d}" for w in WINDOW_STARTS]

    labels = np.full((len(SEASON_WEEKS), region.height, region.width), UINT8_NODATA, np.uint8)
    summary = np.full((len(SUMMARY_NAMES), region.height, region.width), UINT8_NODATA, np.uint8)
    for r0 in range(0, region.height, STRIP_ROWS):
        h = min(STRIP_ROWS, region.height - r0)
        local = Window(0, r0, region.width, h)
        lon, lat = pixel_lonlat(grid, Window(region.col_off, region.row_off + r0, region.width, h))
        row, col = lookup_index(lon, lat, chirps_transform, chirps_shape)
        outside = row < 0

        def rain_at(name):
            values = rain[name][np.clip(row, 0, None), np.clip(col, 0, None)].astype(np.float64)
            values[outside] = np.nan
            return values

        strip_labels, strip_summary = classify_arrays(
            onset=rain_at("onset_week"),
            triggers=np.stack([rain_at(f"trigger_w{w:02d}") for w in WINDOW_STARTS]),
            vci=read_bands(vci_path, vci_names, window=local),
            mai=read_bands(mai_path, mai_names, window=local),
            sown=read_bands(cropland.share_file(paths, season), ["area_sown_pct"], window=local)[0],
            crop=read_bands(cropland.normal_file(paths), ["cropland"], window=local)[0] == 1,
            fallback=read_bands(vci_path, fallback_names, window=local),
            dry_total=rain_at("dry_spell_weeks"),
            dry_longest=rain_at("longest_dry_spell_weeks"),
        )
        labels[:, r0:r0 + h] = strip_labels
        summary[:, r0:r0 + h] = strip_summary

    inside = grid.mask[region.row_off:region.row_off + region.height, region.col_off:region.col_off + region.width]
    labels[:, ~inside] = UINT8_NODATA
    summary[:, ~inside] = UINT8_NODATA
    transform = window_transform(region, grid.transform)
    tags = {"season": season, "region": paths.region_key, "weeks": f"{SEASON_WEEKS[0]}-{SEASON_WEEKS[-1]}",
            "classes": "0 none, 1 mild, 2 moderate, 3 severe"}
    write_stack(labels_file(paths, season), labels, transform, grid.crs,
                [f"week_{w:02d}" for w in SEASON_WEEKS], UINT8_NODATA, tags)
    write_stack(summary_file(paths, season), summary, transform, grid.crs, SUMMARY_NAMES, UINT8_NODATA, tags)
    publish(paths, season)


def publish(paths, season):
    """EPSG:4326 copy of the season summary, nearest neighbour, so values are unchanged."""
    src_path = summary_file(paths, season)
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, "EPSG:4326", src.width, src.height, *src.bounds, resolution=PUBLISH_RESOLUTION)
        out = np.full((src.count, height, width), UINT8_NODATA, np.uint8)
        for i in range(src.count):
            reproject(rasterio.band(src, i + 1), out[i], src_transform=src.transform, src_crs=src.crs,
                      dst_transform=transform, dst_crs="EPSG:4326", resampling=Resampling.nearest,
                      src_nodata=UINT8_NODATA, dst_nodata=UINT8_NODATA)
        names = list(src.descriptions)
        tags = src.tags()
    write_stack(published_file(paths, season), out, transform, "EPSG:4326", names, UINT8_NODATA, tags)


def run(paths, seasons, region, workers=8):
    """Classification for the seasons whose stage outputs all exist. Returns the seasons written."""
    written = []
    for season in seasons:
        if summary_file(paths, season).is_file() and published_file(paths, season).is_file():
            continue
        if not inputs_ready(paths, season):
            log.info("Classify %s: earlier stages not finished; skipped", season)
            continue
        log.info("Classify season %s", season)
        classify_season(paths, season, region)
        written.append(season)
    return written
