"""MWS stage: the drought layer per micro-watershed (MWS), one pan-India GeoPackage per season.

The other stages' rasters are first aggregated over each MWS, and the 2020 rules then
classify the MWS itself, the same way they classify a pixel (method note "Drought on
Micro-watersheds"):
  pixels     the 500 m pixels whose centre lies inside the MWS; an MWS without any takes every
             pixel it touches. The grid is equal-area, so a mean over pixels is area-weighted
  rain       each pixel takes its CHIRPS cell's value, then the mean over the MWS: weekly rain,
             its normal (1981 to the year before the season) and each window's SPI
  onset      the median of the pixels' onset days
  trigger    per window from onset: a dry spell (4 weeks in a row each below 50% of normal),
             rainfall 60% or more below normal, or SPI below -1.5
  VCI        mean over the MWS's cropland pixels
  MAI        sum of ET / sum of PET over the cropland pixels, x 100
  area sown  the MWS's kharif-sown share this season as % of its 2017-2024 average
An MWS without cropland pixels gets the rainfall columns only.

The MWS are those of the hydrology layers: the tehsil files under
hydrology/runoff/commons/tehsil_watersheds, the first copy of each uid, so the
tehsil layers can be cut by uid. A --tile run covers the MWS whose pixels all lie
in the tile.

Files:
  drought/<version>/mws/watersheds.gpkg, pixels.npz       the MWS and their pixels, built once
  drought/<region>/<version>/mws/drought_mws_<season>.gpkg one row per MWS, EPSG:4326, with the
                                                           columns of the GEE drought layer (COLUMNS)
"""

import datetime as dt
import logging
import math
import os

import fiona
import numpy as np
import shapely
from rasterio.features import rasterize
from rasterio.windows import Window
from rasterio.windows import transform as window_transform
from scipy import sparse
from shapely.geometry import mapping, shape

from computing.drought.local import classify, cropland, moisture, rainfall, rules, vegetation
from computing.drought.local.config import MANUAL_EDITION, METHOD_VERSION, RAIN_RECORD_START, rain_normal_years
from computing.drought.local.grid import lonlat_at, lookup_index, sinusoidal_xy
from computing.drought.local.store import read_bands, read_tags
from computing.drought.local.weeks import WEEKS_PER_YEAR, week_of, week_start

log = logging.getLogger(__name__)

WINDOW_STARTS = rainfall.WINDOW_STARTS
SEASON_WEEKS = classify.SEASON_WEEKS
NODATA = classify.UINT8_NODATA
WATERSHED_LAYER = "watersheds"
ATTRIBUTES = {"uid": "str", "area_in_ha": "float", "bacode": "str", "sbcode": "str", "wsconc": "str"}
BATCH = 5000

# Columns per season (name_<season>), as in the GEE drought layer (merge_layers.py), plus dsw.
COLUMNS = {
    "drlb": "weekly classes from the onset week to week 43, a JSON list (0 none, 1 mild, 2 moderate, 3 severe)",
    "drysp": "longest dry spell, weeks",
    "dsw": "weeks inside dry spells",
    "frth0-3": "weeks of class k or worse",
    "inth0-3": "mean class of those weeks",
    "w_no, w_mld, w_mod, w_sev": "weeks of no, mild, moderate and severe drought",
    "kh_cr": "kharif-sown area, km2",
    "m_ons": "onset date, e.g. 2023-6-12",
    "pcr_k": "area sown, % of the MWS's 2017-2024 average",
    "t_wks": "weeks from the onset week to week 43",
    "rd<yy>-<m>-<d>": "rainfall deviation, %, of the 4-week window starting that day",
}


# ---------------------------------------------------------------- paths


def watersheds_file(paths):
    return paths.shared_dir / "mws" / "watersheds.gpkg"


def pixels_file(paths):
    return paths.shared_dir / "mws" / "pixels.npz"


def layer_file(paths, season):
    return paths.version_dir / "mws" / f"drought_mws_{season}.gpkg"


def manifest_file(paths):
    return paths.tehsil_watersheds / "tehsil_watershed_manifest.csv"


# ---------------------------------------------------------------- aggregation


def group_mean(groups, values, n):
    """Mean of values per group 0..n-1, NaN skipped; NaN for a group without values."""
    ok = ~np.isnan(values)
    total = np.bincount(groups[ok], values[ok], minlength=n)
    count = np.bincount(groups[ok], minlength=n)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(count > 0, total / count, np.nan)


def group_ratio(groups, num, den, n):
    """Sum of num / sum of den per group, over the members that have both; NaN where the sum of den is 0."""
    ok = ~np.isnan(num) & ~np.isnan(den)
    a = np.bincount(groups[ok], num[ok], minlength=n)
    b = np.bincount(groups[ok], den[ok], minlength=n)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(b > 0, a / b, np.nan)


def weighted_median(groups, values, weights, n):
    """Per group, the lowest value with at least half the group's weight at or below it (NaN skipped)."""
    ok = ~np.isnan(values)
    groups, values, weights = groups[ok], values[ok], np.asarray(weights, dtype=np.float64)[ok]
    order = np.lexsort((values, groups))
    groups, values, weights = groups[order], values[order], weights[order]
    out = np.full(n, np.nan)
    if not len(groups):
        return out
    first = np.r_[True, groups[1:] != groups[:-1]]
    cum = np.cumsum(weights)
    before = (cum - weights)[first]  # weight of the earlier groups
    within = cum - np.repeat(before, np.diff(np.r_[np.flatnonzero(first), len(groups)]))
    half = np.bincount(groups, weights, minlength=n)[groups] / 2
    hit = np.flatnonzero(within >= half)
    first_hit = np.r_[True, groups[hit][1:] != groups[hit][:-1]]
    out[groups[hit][first_hit]] = values[hit][first_hit]
    return out


def cell_mean(weights, values):
    """values: (bands, rows, cols) on the CHIRPS grid; weights: sparse (MWS, cells), pixels per cell.
    The mean over each MWS's pixels, NaN skipped: (bands, MWS)."""
    flat = values.reshape(values.shape[0], -1).T
    ok = ~np.isnan(flat)
    total = weights @ np.where(ok, flat, 0).astype(np.float64)
    count = weights @ ok.astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(count > 0, total / count, np.nan).T


# ---------------------------------------------------------------- MWS and their pixels


def mws_pixels(geoms, transform, shape):
    """geoms in the grid's CRS. Returns (mws, pixel): each MWS's pixels as flat indexes into the grid,
    the pixels whose centre lies inside it, or every pixel it touches when there is none."""
    index = rasterize(((g, i + 1) for i, g in enumerate(geoms) if not g.is_empty),
                      out_shape=shape, transform=transform, fill=0, dtype="int32")
    pixel = np.flatnonzero(index)
    mws = index.ravel()[pixel] - 1
    del index
    found = np.bincount(mws, minlength=len(geoms)) > 0
    extra_mws, extra_pixel = [mws], [pixel]
    for i in np.flatnonzero(~found):
        window = _bounds_window(geoms[i], transform, shape)
        if window is None:
            continue
        touched = rasterize([(geoms[i], 1)], out_shape=(window.height, window.width),
                            transform=window_transform(window, transform), fill=0, dtype="uint8", all_touched=True)
        r, c = np.nonzero(touched)
        extra_pixel.append((r + window.row_off) * shape[1] + c + window.col_off)
        extra_mws.append(np.full(len(r), i))
    return np.concatenate(extra_mws).astype(np.int32), np.concatenate(extra_pixel).astype(np.int64)


def _bounds_window(geom, transform, shape):
    """The grid window around geom, or None when geom is empty or outside the grid."""
    if geom.is_empty:
        return None
    x0, y0, x1, y1 = geom.bounds
    c0 = max(math.floor((x0 - transform.c) / transform.a) - 1, 0)
    c1 = min(math.ceil((x1 - transform.c) / transform.a) + 1, shape[1])
    r0 = max(math.floor((y1 - transform.f) / transform.e) - 1, 0)
    r1 = min(math.ceil((y0 - transform.f) / transform.e) + 1, shape[0])
    if c0 >= c1 or r0 >= r1:
        return None
    return Window(c0, r0, c1 - c0, r1 - r0)


def watersheds_current(paths, grid):
    """watersheds.gpkg and pixels.npz exist, are newer than the hydrology manifest and fit the grid."""
    out, pix = watersheds_file(paths), pixels_file(paths)
    if not (out.is_file() and pix.is_file()) or out.stat().st_mtime < manifest_file(paths).stat().st_mtime:
        return False
    with np.load(pix) as data:
        return tuple(data["grid_shape"]) == (grid.height, grid.width)


def build_watersheds(paths):
    """Copy the hydrology MWS into one file, in manifest order, keeping the first copy of each uid.
    As in the hydrology layers, invalid geometries are repaired with buffer(0) and empty ones dropped."""
    from computing.hydrology_gpu.watershed_boundary import find_pan_india_watersheds  # needs geopandas

    out = watersheds_file(paths)
    out.parent.mkdir(parents=True, exist_ok=True)
    pixels_file(paths).unlink(missing_ok=True)  # made from the old copy
    tmp = out.with_name(f"{out.stem}.tmp.gpkg")
    tmp.unlink(missing_ok=True)
    seen = set()
    schema = {"geometry": "Unknown", "properties": ATTRIBUTES}
    with fiona.open(tmp, "w", driver="GPKG", layer=WATERSHED_LAYER, crs="EPSG:4326", schema=schema) as dst:
        for path, _ in find_pan_india_watersheds(paths.tehsil_watersheds):
            records = []
            with fiona.open(path) as src:
                if src.crs and src.crs.to_epsg() != 4326:
                    raise ValueError(f"{path}: expected EPSG:4326, got {src.crs}")
                for feat in src:
                    uid = str(feat.properties["uid"])
                    if uid in seen or feat.geometry is None:
                        continue
                    geom = feat.geometry
                    g = shape(geom)
                    if not g.is_valid:
                        g = g.buffer(0)
                        geom = mapping(g)
                    if g.is_empty:
                        continue
                    seen.add(uid)
                    props = {k: feat.properties.get(k) for k in ATTRIBUTES}
                    records.append({"geometry": geom, "properties": {**props, "uid": uid}})
            dst.writerecords(records)
    os.replace(tmp, out)
    log.info("MWS: %d watersheds copied from %s", len(seen), paths.tehsil_watersheds)


def build_pixels(paths, grid):
    """pixels.npz: each MWS's pixels, the MWS's area (km2, on the equal-area grid) and its uid."""
    uids, geoms = [], []
    with fiona.open(watersheds_file(paths), layer=WATERSHED_LAYER) as src:
        for feat in src:
            uids.append(feat.properties["uid"])
            geoms.append(shape(feat.geometry))
    geoms = np.array(geoms, dtype=object)
    for i in range(0, len(geoms), 10000):
        geoms[i:i + 10000] = shapely.transform(geoms[i:i + 10000], lambda xy: np.column_stack(sinusoidal_xy(xy[:, 0], xy[:, 1])))
    log.info("MWS: rasterizing %d watersheds", len(geoms))
    mws, pixel = mws_pixels(geoms, grid.transform, (grid.height, grid.width))
    path = pixels_file(paths)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as handle:
        np.savez(handle, uid=np.array(uids), area_km2=shapely.area(geoms) / 1e6, mws=mws, pixel=pixel,
                 grid_shape=np.array([grid.height, grid.width]),
                 built_at=np.array(dt.datetime.now().isoformat(timespec="seconds")))
    os.replace(tmp, path)
    log.info("MWS: %d pixels; %d watersheds without one", len(pixel),
             int((np.bincount(mws, minlength=len(geoms)) == 0).sum()))


class Watersheds:
    """The MWS of a run's region and their pixels. Index i is row rows[i] of watersheds.gpkg."""

    def __init__(self, paths, grid, region):
        with np.load(pixels_file(paths)) as data:
            uid, area, mws, pixel = data["uid"], data["area_km2"], data["mws"], data["pixel"]
            self.built_at = str(data["built_at"])
        row, col = np.divmod(pixel, grid.width)
        if paths.region:
            inside = ((row >= region.row_off) & (row < region.row_off + region.height)
                      & (col >= region.col_off) & (col < region.col_off + region.width))
            n_pixels = np.bincount(mws, minlength=len(uid))
            n_inside = np.bincount(mws[inside], minlength=len(uid))
            self.rows = np.flatnonzero((n_pixels > 0) & (n_inside == n_pixels))
        else:
            self.rows = np.arange(len(uid))
        self.position = np.full(len(uid), -1)  # index of each row of watersheds.gpkg, -1 if left out
        self.position[self.rows] = np.arange(len(self.rows))
        keep = self.position[mws] >= 0
        self.n = len(self.rows)
        self.uid, self.area_km2 = uid[self.rows], area[self.rows]
        self.mws = self.position[mws[keep]]
        self.row, self.col = row[keep] - region.row_off, col[keep] - region.col_off  # in the region's rasters
        self.grid_row, self.grid_col = row[keep], col[keep]

    def cell_weights(self, grid, chirps_transform, chirps_shape):
        """Sparse (MWS, CHIRPS cells): how many of each MWS's pixels lie in each cell."""
        lon, lat = lonlat_at(grid, self.grid_row, self.grid_col)
        r, c = lookup_index(lon, lat, chirps_transform, chirps_shape)
        ok = r >= 0
        cells = chirps_shape[0] * chirps_shape[1]
        return sparse.csr_matrix((np.ones(ok.sum()), (self.mws[ok], r[ok] * chirps_shape[1] + c[ok])),
                                 shape=(self.n, cells))

    def values(self, path, name, select=None):
        """A band of a region raster at the MWS pixels (or at the selected ones)."""
        band = read_bands(path, [name])[0]
        if select is None:
            return band[self.row, self.col]
        return band[self.row[select], self.col[select]]


# ---------------------------------------------------------------- per MWS


def mws_rain(season, onset_day, weekly, normal, spi):
    """MWS rainfall. onset_day (n,) day of year; weekly rain and normal (52, n); SPI (windows, n).
    Returns onset week (NaN without rain), dry-spell weeks, the longest dry spell, and per window
    the trigger and the deviation (windows, n)."""
    jan1 = dt.date(season, 1, 1)
    week_by_day = np.array([week_of(jan1 + dt.timedelta(days=i)) for i in range(366)], dtype=np.float64)
    has = ~np.isnan(onset_day)
    onset_week = np.full(onset_day.shape, np.nan)
    onset_week[has] = week_by_day[onset_day[has].astype(int) - 1]

    dry = rules.dry_weeks(weekly, normal)
    dry_spell_weeks, longest = rainfall.season_dry_spells(dry, np.where(has, onset_week, np.inf))
    deviation = rainfall.deviation_pct(rainfall.window_totals(weekly), rainfall.window_totals(normal))
    dry_spell = np.stack([rules.dry_spell_in_window(dry[w - 1:w + 3]) for w in WINDOW_STARTS])
    return {"onset_day": onset_day, "onset_week": onset_week, "dry_spell_weeks": dry_spell_weeks,
            "longest_dry_spell": longest, "deviation": deviation,
            "triggers": rules.rain_trigger(dry_spell, deviation, spi)}


def mws_classes(rain, vci, mai, sown, has_crop):
    """The 2020 rules on the MWS values, with the raster's code (classify.classify_arrays) run on
    one row of MWS. Returns labels (weeks 19-43, n) and the summary (classify.SUMMARY_NAMES, n)."""
    labels, summary = classify.classify_arrays(
        onset=rain["onset_week"][None], triggers=rain["triggers"][:, None], vci=vci[:, None], mai=mai[:, None],
        sown=sown[None], crop=has_crop[None], fallback=np.zeros_like(vci)[:, None],
        dry_total=rain["dry_spell_weeks"][None], dry_longest=rain["longest_dry_spell"][None])
    return labels[:, 0], summary[:, 0]


def _ints(values, present):
    values = np.where(present, values, 0).astype(np.int64).tolist()
    return [v if p else None for v, p in zip(values, present.tolist())]


def _floats(values, digits):
    return [None if math.isnan(v) else v for v in np.round(np.asarray(values, dtype=np.float64), digits).tolist()]


def layer_columns(season, rain, labels, summary, kharif_km2, sown):
    """[(name, fiona type, values)] for the season's columns, None where a value is missing."""
    y = season
    has_rain = ~np.isnan(rain["onset_week"])
    has_class = summary[0] != NODATA
    names = classify.SUMMARY_NAMES
    in_season = labels != NODATA
    cols = [(f"drlb_{y}", "str", [("[" + ",".join(str(v) for v in week if v != NODATA) + "]") if ok else None
                                  for week, ok in zip(labels.T.tolist(), has_class.tolist())]),
            (f"drysp_{y}", "int", _ints(rain["longest_dry_spell"], has_rain)),
            (f"dsw_{y}", "int", _ints(rain["dry_spell_weeks"], has_rain))]
    freq, intensity = [], []
    for k in (rules.NONE, rules.MILD, rules.MODERATE, rules.SEVERE):
        at_least = in_season & (labels >= k)
        n = at_least.sum(axis=0)
        total = np.where(at_least, labels, 0).sum(axis=0)
        with np.errstate(divide="ignore", invalid="ignore"):
            mean = np.where(n > 0, total / n, 0)
        freq.append((f"frth{k}_{y}", "int", _ints(n, has_class)))
        intensity.append((f"inth{k}_{y}", "float", _floats(np.where(has_class, mean, np.nan), 3)))
    cols += freq + intensity
    for i, short in enumerate(("w_no", "w_mld", "w_mod", "w_sev")):  # summary rows 0-3: weeks_none ... weeks_severe
        cols.append((f"{short}_{y}", "int", _ints(summary[i], has_class)))
    jan1 = dt.date(season, 1, 1)
    onset = [None if math.isnan(d) else jan1 + dt.timedelta(days=int(d) - 1) for d in rain["onset_day"].tolist()]
    cols += [(f"kh_cr_{y}", "float", _floats(kharif_km2, 4)),
             (f"m_ons_{y}", "str", [None if d is None else f"{d.year}-{d.month}-{d.day}" for d in onset]),
             (f"pcr_k_{y}", "float", _floats(sown, 2)),
             (f"t_wks_{y}", "int", _ints(summary[names.index("season_weeks")], has_rain))]
    for i, w in enumerate(WINDOW_STARTS):
        start = week_start(season, w)
        cols.append((f"rd{start:%y}-{start.month}-{start.day}", "float", _floats(rain["deviation"][i], 2)))
    return cols


# ---------------------------------------------------------------- season


def input_files(paths, season):
    return classify.input_files(paths, season) + [rainfall.weekly_file(paths, season)]


def inputs_ready(paths, season):
    past = [rainfall.weekly_file(paths, y) for y in range(RAIN_RECORD_START, season)]
    return all(p.is_file() for p in input_files(paths, season) + past) and moisture.is_current(paths, season)


def inputs_stamp(paths, season, watersheds):
    """When each input was written. A season is made again when any input is rewritten."""
    return ";".join([read_tags(p).get("written_at", "") for p in input_files(paths, season)] + [watersheds.built_at])


def is_current(paths, season, watersheds):
    path = layer_file(paths, season)
    if not path.is_file():
        return False
    with fiona.open(path) as src:
        return src.tags().get("inputs_written_at") == inputs_stamp(paths, season, watersheds)


def season_layer(paths, season, ws, weights, normals):
    """Compute and write one season's MWS layer. normals: weekly normals on the CHIRPS grid (52, r, c)."""
    rain_path = rainfall.season_file(paths, season)
    links = weights.tocoo()
    onset_day = read_bands(rain_path, ["onset_day_of_year"])[0].ravel()
    spi = read_bands(rain_path, [f"spi_x100_w{w:02d}" for w in WINDOW_STARTS]) / 100
    rain = mws_rain(season,
                    onset_day=weighted_median(links.row, onset_day[links.col], links.data, ws.n),
                    weekly=cell_mean(weights, rainfall.read_weekly(paths, season)),
                    normal=cell_mean(weights, normals),
                    spi=cell_mean(weights, spi))

    crop = ws.values(cropland.normal_file(paths), "cropland") == 1
    crop_mws = ws.mws[crop]
    vci_path, mai_path = vegetation.vci_file(paths, season), moisture.mai_file(paths, season)
    vci = np.stack([group_mean(crop_mws, ws.values(vci_path, f"vci_w{w:02d}", crop), ws.n) for w in WINDOW_STARTS])
    mai = np.stack([group_ratio(crop_mws, ws.values(mai_path, et, crop), ws.values(mai_path, pet, crop), ws.n) * 100
                    for et, pet in zip(moisture.ET_NAMES, moisture.PET_NAMES)])

    share_path = cropland.share_file(paths, season)
    kharif, valid = ws.values(share_path, "kharif_cells"), ws.values(share_path, "valid_cells")
    normal_share = ws.values(cropland.normal_file(paths), "kharif_share_x10000") / 10000
    ok = (valid > 0) & ~np.isnan(normal_share) & ~np.isnan(kharif)
    season_share = group_ratio(ws.mws[ok], kharif[ok], valid[ok], ws.n)
    usual_share = group_ratio(ws.mws[ok], normal_share[ok] * valid[ok], valid[ok], ws.n)
    with np.errstate(divide="ignore", invalid="ignore"):
        sown = np.where(usual_share > 0, season_share / usual_share * 100, np.nan)

    has_crop = np.bincount(crop_mws, minlength=ws.n) > 0
    labels, summary = mws_classes(rain, vci, mai, sown, has_crop)
    columns = layer_columns(season, rain, labels, summary, season_share * ws.area_km2, sown)
    first, last = rain_normal_years(season)
    tags = {"method_version": METHOD_VERSION, "manual": MANUAL_EDITION, "season": season,
            "region": paths.region_key, "written_at": dt.datetime.now().isoformat(timespec="seconds"),
            "inputs_written_at": inputs_stamp(paths, season, ws), "rain_normal_years": f"{first}-{last}",
            "classes": "0 none, 1 mild, 2 moderate, 3 severe",
            "method": "indicators aggregated over each MWS, then the 2020 rules (computing/drought/local/mws.py)"}
    log.info("MWS season %s: %d MWS, %d with cropland, %d with rainfall; writing %s", season, ws.n,
             int(has_crop.sum()), int((~np.isnan(rain["onset_week"])).sum()), layer_file(paths, season).name)
    write_layer(paths, season, ws, columns, tags)


def write_layer(paths, season, ws, columns, tags):
    """The MWS of watersheds.gpkg (geometry and attributes) with the season's columns."""
    out = layer_file(paths, season)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f"{out.stem}.tmp.gpkg")
    tmp.unlink(missing_ok=True)
    schema = {"geometry": "Unknown", "properties": {**ATTRIBUTES, **{name: kind for name, kind, _ in columns}}}
    with fiona.open(watersheds_file(paths), layer=WATERSHED_LAYER) as src, \
            fiona.open(tmp, "w", driver="GPKG", layer=out.stem, crs="EPSG:4326", schema=schema) as dst:
        dst.update_tags({k: str(v) for k, v in tags.items()})
        batch = []
        for row, feat in enumerate(src):
            j = ws.position[row]
            if j < 0:
                continue
            props = {k: feat.properties.get(k) for k in ATTRIBUTES}
            props.update((name, values[j]) for name, _, values in columns)
            batch.append({"geometry": feat.geometry, "properties": props})
            if len(batch) == BATCH:
                dst.writerecords(batch)
                batch = []
        dst.writerecords(batch)
    os.replace(tmp, out)


def run(paths, seasons, grid, region):
    """MWS layers for the seasons whose stage outputs exist and are newer than the last layer.
    Returns the seasons written."""
    ready = []
    for season in seasons:
        if inputs_ready(paths, season):
            ready.append(season)
        elif moisture.mai_file(paths, season).is_file() and not moisture.is_current(paths, season):
            log.info("MWS %s: %s has no window ET and PET yet; run the mai stage again", season,
                     moisture.mai_file(paths, season).name)
        else:
            log.info("MWS %s: earlier stages not finished; skipped", season)
    if not ready:
        return []
    if not manifest_file(paths).is_file():
        log.warning("MWS stage skipped: %s not found (the hydrology tehsil watersheds)", manifest_file(paths))
        return []
    if not watersheds_current(paths, grid):
        build_watersheds(paths)
        build_pixels(paths, grid)
    ws = Watersheds(paths, grid, region)
    todo = [s for s in ready if not is_current(paths, s, ws)]
    if not todo:
        return []

    chirps_transform, _, chirps_shape = rainfall.chirps_grid(paths)
    weights = ws.cell_weights(grid, chirps_transform, chirps_shape)
    total, loaded = np.zeros((WEEKS_PER_YEAR,) + chirps_shape), 0
    for season in sorted(todo):
        while RAIN_RECORD_START + loaded < season:  # the normal: 1981 to the year before, as in the rain stage
            total += rainfall.read_weekly(paths, RAIN_RECORD_START + loaded)
            loaded += 1
        season_layer(paths, season, ws, weights, (total / loaded).astype(np.float32))
    return todo
