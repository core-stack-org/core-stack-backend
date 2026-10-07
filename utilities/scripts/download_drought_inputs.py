#!/usr/bin/env python3
"""
Download the inputs of the drought layer and store them for India only, under
<data-dir>/base_layers/periodic/:

  rainfall_chirps/  CHIRPS v2.0 daily rainfall (Climate Hazards Center), 1981 to the latest
                    day. One GeoTIFF per day for 67.6-99.4E, 6-38N on the 0.05 degree grid.
  mod09a1/          MODIS MOD09A1 v061 8-day reflectance (NASA LP DAAC): b01 b02 b04 b05
                    b06 and StateQA, 2000 to the latest composite.
  mod16a2gf/        MODIS MOD16A2GF v061 8-day ET and PET (NASA LP DAAC), 2017 to the latest
                    year. NASA publishes a year only after it ends.

For MODIS only composites that start on day 121-337 (1 May - 3 Dec) are kept. The tiles
are merged on the MODIS sinusoidal grid without resampling, and pixels more than 10 km
outside India are set to no-data. MODIS needs an Earthdata login: USERNAME_GESDISC and
PASSWORD_GESDISC in nrm_app/.env or in the environment.

Finished files are skipped, so running it again continues an interrupted run or adds the
data published since the last run.

Example:
    python utilities/scripts/download_drought_inputs.py
"""

import argparse
import calendar
import datetime as dt
import fcntl
import gzip
import json
import logging
import os
import re
import shutil
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse


def _set_gdal_env():
    # Same GDAL/PROJ data folders that nrm_app/settings.py sets for Django.
    share = Path(sys.prefix) / "share"
    for var, sub in (("GDAL_DATA", "gdal"), ("PROJ_DATA", "proj"), ("PROJ_LIB", "proj")):
        if (share / sub).is_dir():
            os.environ.setdefault(var, str(share / sub))
    os.environ.setdefault("GDAL_CACHEMAX", "256")  # MB
    # CHIRPS files are read over HTTP; fetch only the blocks that cover India.
    os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".cog")
    os.environ.setdefault("GDAL_HTTP_MAX_RETRY", "5")
    os.environ.setdefault("GDAL_HTTP_RETRY_DELAY", "10")
    os.environ.setdefault("GDAL_HTTP_TIMEOUT", "180")


_set_gdal_env()

import numpy as np
import rasterio
import requests
import shapely
from rasterio.crs import CRS
from rasterio.errors import NotGeoreferencedWarning
from rasterio.features import rasterize
from rasterio.transform import Affine
from rasterio.windows import Window
from requests.adapters import HTTPAdapter
from shapely.geometry import shape
from urllib3.util.retry import Retry

log = logging.getLogger("drought_inputs")

REPO_DIR = Path(__file__).resolve().parents[2]
# Docker sets APP_ENV_FILE for the backend containers.
ENV_FILE = Path(os.environ.get("APP_ENV_FILE") or REPO_DIR / "nrm_app" / ".env")
DEFAULT_BOUNDARY = "base_layers/PanIndia_Boundaries/india_state_outer_no_islands.geojson"

# CHIRPS: global 0.05 degree grid with origin (-180, 50), the same grid GEE uses.
CHIRPS_COG_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/cogs/p05"
CHIRPS_TIF_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05"
CHIRPS_FIRST_YEAR = 1981
CHIRPS_RES = 0.05
CHIRPS_NODATA = -9999.0
# West, south, east, north. Covers the five monsoon-zone rectangles the drought onset
# uses (67.7-99.3E, 7.6-37.5N).
CHIRPS_BOUNDS = (67.6, 6.0, 99.4, 38.0)
CHIRPS_WINDOW = Window(
    round((CHIRPS_BOUNDS[0] + 180) / CHIRPS_RES),
    round((50 - CHIRPS_BOUNDS[3]) / CHIRPS_RES),
    round((CHIRPS_BOUNDS[2] - CHIRPS_BOUNDS[0]) / CHIRPS_RES),
    round((CHIRPS_BOUNDS[3] - CHIRPS_BOUNDS[1]) / CHIRPS_RES),
)
CHIRPS_TRANSFORM = Affine(
    CHIRPS_RES, 0, -180 + CHIRPS_WINDOW.col_off * CHIRPS_RES,
    0, -CHIRPS_RES, 50 - CHIRPS_WINDOW.row_off * CHIRPS_RES,
)

# MODIS sinusoidal grid: 2400 x 2400 pixel tiles, numbered h (columns) and v (rows).
SINUSOIDAL = CRS.from_proj4("+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs")
EARTH_RADIUS = 6371007.181
GRID_X0, GRID_Y0 = -20015109.354, 10007554.677
TILE_PX = 2400
PIXEL = 1111950.5196666666 / TILE_PX
MASK_BUFFER_M = 10_000
MASK_FILE = "modis_india_mask.tif"

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_BBOX = "66,5,100,39"  # only narrows the search; tiles are picked from the India mask
URS_HOST = "urs.earthdata.nasa.gov"
MODIS_DAYS = list(range(121, 338, 8))  # composite start days kept: 1 May - 3 Dec
WAIT_FOR_TILES_DAYS = 45  # after this, a composite with missing tiles is written without them
DOWNLOAD_ATTEMPTS = 5

MODIS_PRODUCTS = {
    "mod09a1": {
        "short_name": "MOD09A1",
        "version": "061",
        "first_year": 2000,
        # file suffix: (HDF layer, dtype, no-data)
        "layers": {
            "b01": ("sur_refl_b01", "int16", -28672),
            "b02": ("sur_refl_b02", "int16", -28672),
            "b04": ("sur_refl_b04", "int16", -28672),
            "b05": ("sur_refl_b05", "int16", -28672),
            "b06": ("sur_refl_b06", "int16", -28672),
            "state": ("sur_refl_state_500m", "uint16", 65535),
        },
    },
    "mod16a2gf": {
        "short_name": "MOD16A2GF",
        "version": "061",
        "first_year": 2017,
        # Values above 32700 are class codes (water, urban, barren, ...), not data.
        "layers": {
            "ET": ("ET_500m", "int16", 32767),
            "PET": ("PET_500m", "int16", 32767),
        },
    },
}
PRODUCTS = ["chirps", *MODIS_PRODUCTS]

MISSING_LOGIN = """\
Earthdata login not found.

MODIS files (mod09a1, mod16a2gf) come from NASA LP DAAC and need a free Earthdata account:
  1. Create one at https://urs.earthdata.nasa.gov
  2. Add it to nrm_app/.env (keep the single quotes if the password contains $):
       USERNAME_GESDISC=your-username
       PASSWORD_GESDISC='your-password'
  3. Run this script again. It reads nrm_app/.env itself, so no restart is needed.

CHIRPS needs no login. To start with it now, run with --products chirps."""


class LoginError(RuntimeError):
    pass


# ---------------------------------------------------------------- shared helpers


def http_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=5, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def write_json(path, data):
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, path)


@contextmanager
def folder_lock(folder, name=".lock", wait=False):
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / name, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError:
            log.error("%s is in use by another run of this script. Wait for it to finish.", folder)
            raise SystemExit(2)
        yield


@contextmanager
def no_lock():
    yield


def same_crs(written, expected):
    if written is None:
        return False
    if expected.to_epsg():
        return written.to_epsg() == expected.to_epsg()

    def params(crs):
        return {p for p in crs.to_proj4().split() if not p.startswith(("+no_defs", "+type"))}

    return params(written) == params(expected)


def write_geotiff(path, data, transform, crs, nodata, predictor, tags):
    """Write to <path>.tmp and check the CRS; the caller moves it into place."""
    tmp = path.with_name(path.name + ".tmp")
    profile = {
        "driver": "GTiff",
        "width": data.shape[1],
        "height": data.shape[0],
        "count": 1,
        "dtype": data.dtype.name,
        "crs": crs,
        "transform": transform,
        "nodata": nodata,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "deflate",
        "predictor": predictor,
        "bigtiff": "IF_SAFER",
        "num_threads": "ALL_CPUS",
    }
    with rasterio.open(tmp, "w", **profile) as dst:
        dst.write(data, 1)
        dst.update_tags(**{k: str(v) for k, v in tags.items()})
    with rasterio.open(tmp) as check:
        if not same_crs(check.crs, crs):
            raise RuntimeError(
                f"{tmp} was not written with the expected CRS (PROJ problem in this "
                "environment?). Not moving it into place."
            )
    return tmp


# ---------------------------------------------------------------- Earthdata login


class EarthdataSession(requests.Session):
    """Sends the login to urs.earthdata.nasa.gov but drops it on the final redirect to
    the file store, which refuses requests that carry two kinds of credentials."""

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        if "Authorization" in headers:
            old = urlparse(response.request.url).hostname
            new = urlparse(prepared_request.url).hostname
            if old != new and URS_HOST not in (old, new):
                del headers["Authorization"]


def read_env_file(path):
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def earthdata_login():
    user = os.environ.get("USERNAME_GESDISC", "").strip()
    password = os.environ.get("PASSWORD_GESDISC", "")
    if (not user or not password) and ENV_FILE.is_file():
        try:
            values = read_env_file(ENV_FILE)
        except OSError as exc:
            log.warning("Could not read %s: %s", ENV_FILE, exc)
            values = {}
        user = user or values.get("USERNAME_GESDISC", "").strip()
        password = password or values.get("PASSWORD_GESDISC", "")
    return (user, password) if user and password else None


def raise_for_login(response, first_chunk):
    if response.status_code == 401:
        raise LoginError(
            "Earthdata rejected the username or password (USERNAME_GESDISC / "
            "PASSWORD_GESDISC). Check them by logging in at https://urs.earthdata.nasa.gov"
        )
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} for {response.url}")
    start = first_chunk.lstrip()[:15].lower()
    if "text/html" in response.headers.get("Content-Type", "").lower() or start.startswith((b"<!doctype", b"<html")):
        raise LoginError(
            "Earthdata returned a web page instead of the file. Log in at "
            "https://urs.earthdata.nasa.gov, accept any pending agreement or application "
            "approval, then run again."
        )


def check_login(http, auth):
    """Fetch the start of one small MODIS file to prove the login works."""
    entries = cmr_search(http, "MOD16A2GF", "061", dt.date(2024, 7, 11), dt.date(2024, 7, 11),
                         page_size=1, all_pages=False)
    granule = parse_granule(entries[0]) if entries else None
    if granule is None:
        raise RuntimeError("NASA's catalogue (CMR) returned no test file")
    session = EarthdataSession()
    session.auth = auth
    try:
        with session.get(granule.url, stream=True, timeout=(30, 120)) as response:
            raise_for_login(response, next(response.iter_content(64 * 1024), b""))
    finally:
        session.close()


def hdf4_supported():
    with rasterio.Env() as env:
        return "HDF4" in env.drivers()


# ---------------------------------------------------------------- CHIRPS


def chirps_path(root, day):
    return root / "daily" / str(day.year) / f"chirps_{day:%Y_%m_%d}.tif"


def chirps_days_on_server(http, year):
    """Day -> URL. The COG folder lags months behind the .tif.gz folder, so days it does
    not have yet come from the .tif.gz files (same values, but the whole globe)."""
    days = {}
    for base, ext in ((CHIRPS_TIF_URL, "tif.gz"), (CHIRPS_COG_URL, "cog")):  # COG wins
        response = http.get(f"{base}/{year}/", timeout=120)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        pattern = rf"chirps-v2\.0\.({year})\.(\d\d)\.(\d\d)\.{re.escape(ext)}"
        for y, m, d in set(re.findall(pattern, response.text)):
            days[dt.date(int(y), int(m), int(d))] = f"{base}/{y}/chirps-v2.0.{y}.{m}.{d}.{ext}"
    return days


def read_chirps_window(src, url):
    t = src.transform
    if (src.width, src.height) != (7200, 2000) or max(
        abs(t.a - CHIRPS_RES), abs(t.e + CHIRPS_RES), abs(t.c + 180), abs(t.f - 50)
    ) > 1e-6:
        raise ValueError(f"{url}: not on the expected 0.05 degree grid")
    return src.read(1, window=CHIRPS_WINDOW)


def chirps_fetch_day(day, url, out_path):
    if url.endswith(".cog"):
        with rasterio.open(f"/vsicurl/{url}") as src:
            data = read_chirps_window(src, url)
    else:
        response = requests.get(url, timeout=(30, 300))
        response.raise_for_status()
        with rasterio.MemoryFile(gzip.decompress(response.content)) as mem, mem.open() as src:
            data = read_chirps_window(src, url)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tags = {"source": url, "date": day.isoformat(), "units": "mm/day"}
    tmp = write_geotiff(out_path, data, CHIRPS_TRANSFORM, CRS.from_epsg(4326), CHIRPS_NODATA, 3, tags)
    os.replace(tmp, out_path)


def chirps_fetch_with_retries(day, url, out_path, attempts=3):
    for attempt in range(1, attempts + 1):
        try:
            return chirps_fetch_day(day, url, out_path)
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(10 * attempt)


def run_chirps(periodic_dir, http, args):
    root = periodic_dir / "rainfall_chirps"
    manifest_path = root / "manifest.json"
    manifest = read_json(manifest_path)
    manifest.update(
        product="CHIRPS v2.0 daily",
        source=CHIRPS_COG_URL,
        bounds_west_south_east_north=list(CHIRPS_BOUNDS),
        crs="EPSG:4326",
        resolution_deg=CHIRPS_RES,
        units="mm/day",
        nodata=CHIRPS_NODATA,
    )
    failed = manifest.setdefault("failed", {})
    years = manifest.setdefault("years", {})
    with (folder_lock(root) if not args.dry_run else no_lock()):
        try:
            for year in range(args.chirps_start_year, args.end_year + 1):
                year_dir = root / "daily" / str(year)
                have = {p.name for p in year_dir.glob("chirps_*.tif")} if year_dir.is_dir() else set()
                days_in_year = 366 if calendar.isleap(year) else 365
                if len(have) >= days_in_year:
                    continue
                on_server = chirps_days_on_server(http, year)
                todo = [d for d in sorted(on_server) if chirps_path(root, d).name not in have]
                if not todo:
                    continue
                if args.dry_run:
                    log.info("CHIRPS %s: would fetch %d days (%d here, %d on the server)",
                             year, len(todo), len(have), len(on_server))
                    continue
                log.info("CHIRPS %s: fetching %d days with %d workers", year, len(todo), args.chirps_workers)
                start, done = time.time(), 0
                with ThreadPoolExecutor(args.chirps_workers) as pool:
                    futures = {pool.submit(chirps_fetch_with_retries, d, on_server[d], chirps_path(root, d)): d
                               for d in todo}
                    try:
                        for future in as_completed(futures):
                            day = futures[future]
                            try:
                                future.result()
                                failed.pop(day.isoformat(), None)
                                done += 1
                            except Exception as exc:
                                failed[day.isoformat()] = str(exc)
                                log.warning("CHIRPS %s failed: %s", day, exc)
                    except KeyboardInterrupt:
                        for future in futures:
                            future.cancel()
                        raise
                files = len(list(year_dir.glob("chirps_*.tif")))
                years[str(year)] = {"files": files, "complete": files >= days_in_year}
                log.info("CHIRPS %s: %d new days, %d files now (%.0f s)", year, done, files, time.time() - start)
                manifest["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
                write_json(manifest_path, manifest)
        finally:
            if not args.dry_run:
                latest = sorted((root / "daily").glob("*/chirps_*.tif"))
                manifest["latest_day"] = latest[-1].stem[len("chirps_"):].replace("_", "-") if latest else None
                manifest["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
                write_json(manifest_path, manifest)
    return len(failed)


# ---------------------------------------------------------------- MODIS


def to_sinusoidal(coords):
    lon, lat = np.radians(coords[:, 0]), np.radians(coords[:, 1])
    return np.column_stack((EARTH_RADIUS * lon * np.cos(lat), EARTH_RADIUS * lat))


def build_mask(boundary_path):
    """India boundary + 10 km, rasterised on the MODIS grid."""
    collection = json.loads(Path(boundary_path).read_text())
    crs_name = str(((collection.get("crs") or {}).get("properties") or {}).get("name", "CRS84"))
    if not crs_name.upper().endswith(("CRS84", "4326")):
        raise ValueError(f"{boundary_path} must be in WGS84 (EPSG:4326); it is {crs_name}")
    india = shapely.union_all([shape(f["geometry"]) for f in collection["features"]])
    area = shapely.transform(india, to_sinusoidal).buffer(MASK_BUFFER_M, quad_segs=4)
    xmin, ymin, xmax, ymax = area.bounds
    col0 = int(np.floor((xmin - GRID_X0) / PIXEL))
    row0 = int(np.floor((GRID_Y0 - ymax) / PIXEL))
    width = int(np.ceil((xmax - GRID_X0) / PIXEL)) - col0
    height = int(np.ceil((GRID_Y0 - ymin) / PIXEL)) - row0
    transform = Affine(PIXEL, 0, GRID_X0 + col0 * PIXEL, 0, -PIXEL, GRID_Y0 - row0 * PIXEL)
    mask = rasterize([(area, 1)], out_shape=(height, width), transform=transform, all_touched=True, dtype="uint8")
    return mask, transform


def load_modis_grid(periodic_dir, boundary_path, save):
    """Grid and India mask shared by both MODIS products. Built once and kept, so every
    file stays on the same grid; delete the mask only together with the MODIS folders."""
    path = periodic_dir / MASK_FILE
    # Both MODIS products may start at once; only one of them builds the mask.
    with (folder_lock(periodic_dir, ".mask.lock", wait=True) if save else no_lock()):
        if path.exists():
            with rasterio.open(path) as src:
                mask, transform = src.read(1), src.transform
        else:
            if not Path(boundary_path).is_file():
                log.error("Boundary file not found: %s", boundary_path)
                raise SystemExit(2)
            log.info("Building the India mask on the MODIS grid from %s", boundary_path)
            mask, transform = build_mask(boundary_path)
            if save:
                tags = {"boundary": Path(boundary_path).name, "buffer_m": MASK_BUFFER_M}
                os.replace(write_geotiff(path, mask, transform, SINUSOIDAL, None, 2, tags), path)
    mask = mask.astype(bool)
    col0 = round((transform.c - GRID_X0) / PIXEL)
    row0 = round((GRID_Y0 - transform.f) / PIXEL)
    height, width = mask.shape
    tiles = []
    for h in range(col0 // TILE_PX, (col0 + width - 1) // TILE_PX + 1):
        for v in range(row0 // TILE_PX, (row0 + height - 1) // TILE_PX + 1):
            r, c = v * TILE_PX - row0, h * TILE_PX - col0
            if mask[max(r, 0):max(r + TILE_PX, 0), max(c, 0):max(c + TILE_PX, 0)].any():
                tiles.append((h, v))
    return SimpleNamespace(mask=mask, transform=transform, col0=col0, row0=row0,
                           height=height, width=width, tiles=tiles)


def cmr_search(http, short_name, version, first_day, last_day, page_size=2000, all_pages=True):
    params = {
        "short_name": short_name,
        "version": version,
        "temporal": f"{first_day}T00:00:00Z,{last_day}T23:59:59Z",
        "bounding_box": CMR_BBOX,
        "page_size": page_size,
    }
    headers, entries = {}, []
    while True:
        response = http.get(CMR_URL, params=params, headers=headers, timeout=120)
        response.raise_for_status()
        page = response.json()["feed"]["entry"]
        entries += page
        token = response.headers.get("CMR-Search-After")
        if not all_pages or len(page) < page_size or not token:
            return entries
        headers["CMR-Search-After"] = token


GRANULE_RE = re.compile(r"\.A(\d{4})(\d{3})\.h(\d\d)v(\d\d)\.\d{3}\.(\d{13})$")


def parse_granule(entry):
    match = GRANULE_RE.search(entry.get("title", ""))
    url = next(
        (link["href"] for link in entry.get("links", [])
         if link.get("href", "").startswith("https://") and link["href"].endswith(".hdf")),
        None,
    )
    if not match or not url:
        return None
    year, doy, h, v, made = match.groups()
    return SimpleNamespace(title=entry["title"], year=int(year), doy=int(doy), tile=(int(h), int(v)),
                           made=made, url=url, size_mb=float(entry.get("granule_size") or 0))


def composite_day(year, doy):
    return dt.date(year, 1, 1) + dt.timedelta(days=doy - 1)


def layer_path(product_dir, short_name, day, suffix):
    return product_dir / str(day.year) / f"{short_name}_{day:%Y_%m_%d}_{suffix}.tif"


def composite_done(product_dir, spec, day):
    return all(layer_path(product_dir, spec["short_name"], day, s).exists() for s in spec["layers"])


def download_granule(granule, raw_dir, auth):
    path = raw_dir / Path(urlparse(granule.url).path).name
    if path.exists():
        return path
    raw_dir.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        session = EarthdataSession()
        session.auth = auth
        try:
            with session.get(granule.url, stream=True, timeout=(30, 300)) as response:
                chunks = response.iter_content(1024 * 1024)
                first = next(chunks, b"")
                raise_for_login(response, first)
                expected = int(response.headers.get("Content-Length") or 0)
                with open(part, "wb") as handle:
                    handle.write(first)
                    for chunk in chunks:
                        handle.write(chunk)
            size = part.stat().st_size
            if size == 0 or (expected and size != expected):
                raise IOError(f"incomplete download ({size} of {expected} bytes)")
            os.replace(part, path)
            return path
        except LoginError:
            raise
        except Exception as exc:
            part.unlink(missing_ok=True)
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            log.warning("%s: attempt %d failed (%s); retrying", path.name, attempt, exc)
            time.sleep(15 * attempt)
        finally:
            session.close()


def read_tile(hdf_path, layer, tile, dtype):
    # The HDF file itself has no grid; only its layers do.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        with rasterio.open(hdf_path) as container:
            names = {sub.rsplit(":", 1)[-1]: sub for sub in container.subdatasets}
    if layer not in names:
        raise KeyError(f"{layer} not in {hdf_path.name}; it has {', '.join(names)}")
    h, v = tile
    with rasterio.open(names[layer]) as src:
        t = src.transform
        on_grid = (
            (src.width, src.height) == (TILE_PX, TILE_PX)
            and abs(t.a - PIXEL) < 1e-6
            and abs((t.c - GRID_X0) / PIXEL - h * TILE_PX) < 0.01
            and abs((GRID_Y0 - t.f) / PIXEL - v * TILE_PX) < 0.01
        )
        if not on_grid:
            raise ValueError(f"{hdf_path.name} {layer}: not on the MODIS grid for h{h:02d}v{v:02d}")
        data, tags = src.read(1), src.tags()
    if data.dtype != np.dtype(dtype):
        raise ValueError(f"{hdf_path.name} {layer}: expected {dtype}, got {data.dtype}")
    return data, tags


def process_composite(spec, day, granules, missing, grid, product_dir, auth, workers, keep_raw):
    """Download the tiles of one composite, merge each layer for India and write it."""
    raw_dir = product_dir / "_raw" / f"{day:%Y_%m_%d}"
    with ThreadPoolExecutor(workers) as pool:
        paths = dict(zip(granules, pool.map(lambda g: download_granule(g, raw_dir, auth), granules.values())))

    out_dir = product_dir / str(day.year)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for suffix, (layer, dtype, nodata) in spec["layers"].items():
        out = np.full((grid.height, grid.width), nodata, dtype=dtype)
        layer_tags = {}
        for tile, path in paths.items():
            data, layer_tags = read_tile(path, layer, tile, dtype)
            r, c = tile[1] * TILE_PX - grid.row0, tile[0] * TILE_PX - grid.col0
            r0, r1 = max(r, 0), min(r + TILE_PX, grid.height)
            c0, c1 = max(c, 0), min(c + TILE_PX, grid.width)
            if r0 < r1 and c0 < c1:
                out[r0:r1, c0:c1] = data[r0 - r:r1 - r, c0 - c:c1 - c]
        out[~grid.mask] = nodata
        tags = {
            "product": f"{spec['short_name']} v{spec['version']}",
            "layer": layer,
            "composite_start": day.isoformat(),
            "day_of_year": day.timetuple().tm_yday,
            "granules": " ".join(sorted(g.title for g in granules.values())),
            "missing_tiles": " ".join(f"h{h:02d}v{v:02d}" for h, v in missing) or "none",
        }
        for key in ("scale_factor", "add_offset", "valid_range", "units", "long_name"):
            if key in layer_tags:
                tags[key] = layer_tags[key]
        path = layer_path(product_dir, spec["short_name"], day, suffix)
        written[path] = write_geotiff(path, out, grid.transform, SINUSOIDAL, nodata, 2, tags)
    for path, tmp in written.items():
        os.replace(tmp, path)
    if not keep_raw:
        shutil.rmtree(raw_dir, ignore_errors=True)


def run_modis(key, periodic_dir, http, auth, args):
    spec = MODIS_PRODUCTS[key]
    product_dir = periodic_dir / key
    start_year = getattr(args, f"{key}_start_year")
    manifest_path = product_dir / "manifest.json"
    manifest = read_json(manifest_path)
    failed = manifest.setdefault("failed", {})
    partial = manifest.setdefault("partial", {})
    missing_at_source = manifest.setdefault("missing_at_source", {})
    years = manifest.setdefault("years", {})
    today = dt.date.today()
    to_fetch, to_fetch_mb = 0, 0.0

    with (folder_lock(product_dir) if not args.dry_run else no_lock()):
        grid = load_modis_grid(periodic_dir, args.boundary, save=not args.dry_run)
        manifest.update(
            product=f"{spec['short_name']} v{spec['version']}",
            source="NASA LP DAAC (found through CMR)",
            layers={s: layer for s, (layer, _, _) in spec["layers"].items()},
            nodata={s: nodata for s, (_, _, nodata) in spec["layers"].items()},
            composite_start_days=f"{MODIS_DAYS[0]}-{MODIS_DAYS[-1]} every 8 days",
            crs=SINUSOIDAL.to_proj4(),
            transform=list(grid.transform)[:6],
            width=grid.width,
            height=grid.height,
            tiles=[f"h{h:02d}v{v:02d}" for h, v in grid.tiles],
        )
        try:
            for year in range(start_year, args.end_year + 1):
                days = [composite_day(year, doy) for doy in MODIS_DAYS]
                todo = [d for d in days if not composite_done(product_dir, spec, d)]
                if not todo or todo[0] > today:
                    continue
                found = {}
                for entry in cmr_search(http, spec["short_name"], spec["version"], days[0], days[-1]):
                    g = parse_granule(entry)
                    if g and g.year == year and g.doy in MODIS_DAYS and g.tile in grid.tiles:
                        current = found.setdefault(g.doy, {}).get(g.tile)
                        if current is None or g.made > current.made:
                            found[g.doy][g.tile] = g
                year_tiles = set().union(*(set(t) for t in found.values())) if found else set()
                for day in todo:
                    granules = found.get(day.timetuple().tm_yday)
                    if not granules:
                        # NASA has the year but not this composite, long after it should have
                        # appeared: a gap at the source, not a file still to come.
                        if found and (today - day).days >= WAIT_FOR_TILES_DAYS:
                            if day.isoformat() not in missing_at_source:
                                log.info("%s %s: NASA has no data for this composite",
                                         spec["short_name"], day)
                            missing_at_source[day.isoformat()] = "no granules in CMR"
                        continue  # not published yet
                    missing_at_source.pop(day.isoformat(), None)
                    missing = sorted(year_tiles - set(granules))
                    if missing and (today - day).days < WAIT_FOR_TILES_DAYS:
                        log.info("%s %s: waiting for tiles %s", spec["short_name"], day, missing)
                        continue
                    size_mb = sum(g.size_mb for g in granules.values())
                    if args.dry_run:
                        to_fetch += 1
                        to_fetch_mb += size_mb
                        continue
                    start = time.time()
                    try:
                        process_composite(spec, day, granules, missing, grid, product_dir, auth,
                                          args.workers, args.keep_raw)
                        failed.pop(day.isoformat(), None)
                        if missing:
                            partial[day.isoformat()] = [f"h{h:02d}v{v:02d}" for h, v in missing]
                        log.info("%s %s: done, %d tiles, %.0f MB, %.0f s", spec["short_name"], day,
                                 len(granules), size_mb, time.time() - start)
                    except LoginError:
                        raise
                    except Exception as exc:
                        failed[day.isoformat()] = str(exc)
                        log.error("%s %s failed: %s", spec["short_name"], day, exc)
                    manifest["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
                    write_json(manifest_path, manifest)
                if not args.dry_run:
                    done = sum(composite_done(product_dir, spec, d) for d in days)
                    gaps = sum(d.isoformat() in missing_at_source for d in days)
                    years[str(year)] = {"composites": done, "missing_at_source": gaps, "of": len(days)}
        finally:
            if not args.dry_run:
                finished = sorted(product_dir.glob(f"*/{spec['short_name']}_*_{next(iter(spec['layers']))}.tif"))
                manifest["latest_composite"] = (
                    finished[-1].name.split("_", 1)[1][:10].replace("_", "-") if finished else None
                )
                manifest["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
                write_json(manifest_path, manifest)

    if args.dry_run:
        log.info("%s: would fetch %d composites, about %.0f GB to download",
                 spec["short_name"], to_fetch, to_fetch_mb / 1024)
    return len(failed)


# ---------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--products", default=",".join(PRODUCTS),
                        help=f"comma-separated, any of {', '.join(PRODUCTS)} (default: all)")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR") or str(REPO_DIR / "data"),
                        help="data folder; files go to <data-dir>/base_layers/periodic (default: $DATA_DIR or ./data)")
    parser.add_argument("--boundary", help=f"India boundary GeoJSON for the MODIS mask (default: <data-dir>/{DEFAULT_BOUNDARY})")
    parser.add_argument("--chirps-start-year", type=int, default=CHIRPS_FIRST_YEAR)
    parser.add_argument("--mod09a1-start-year", type=int, default=MODIS_PRODUCTS["mod09a1"]["first_year"])
    parser.add_argument("--mod16a2gf-start-year", type=int, default=MODIS_PRODUCTS["mod16a2gf"]["first_year"])
    parser.add_argument("--end-year", type=int, default=dt.date.today().year, help="last year to fetch (default: this year)")
    parser.add_argument("--workers", type=int, default=4, help="MODIS tiles downloaded in parallel (default: 4)")
    parser.add_argument("--chirps-workers", type=int, default=8, help="CHIRPS days fetched in parallel (default: 8)")
    parser.add_argument("--keep-raw", action="store_true", help="keep the downloaded MODIS HDF files in _raw/")
    parser.add_argument("--dry-run", action="store_true", help="check the login and show what would be fetched, without downloading")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    products = [p.strip().lower() for p in args.products.split(",") if p.strip()]
    unknown = sorted(set(products) - set(PRODUCTS))
    if unknown:
        parser.error(f"unknown product(s): {', '.join(unknown)}")
    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        parser.error(f"data folder not found: {data_dir}")
    args.boundary = args.boundary or str(data_dir / DEFAULT_BOUNDARY)
    periodic_dir = data_dir / "base_layers" / "periodic"
    http = http_session()

    auth = None
    if any(p in MODIS_PRODUCTS for p in products):
        if not hdf4_supported():
            log.error("This GDAL cannot read HDF4, which the MODIS files use. Run inside the "
                      "Docker backend container or the corestack conda environment.")
            return 2
        auth = earthdata_login()
        if auth is None:
            log.error(MISSING_LOGIN)
            if not args.dry_run:
                return 2
        else:
            try:
                check_login(http, auth)
                log.info("Earthdata login works (user %s).", auth[0])
            except Exception as exc:
                if not isinstance(exc, LoginError):
                    exc = f"Could not check the Earthdata login (network problem?): {exc}"
                log.error("%s", exc)
                if not args.dry_run:
                    return 2

    failed = 0
    try:
        for product in products:
            if product == "chirps":
                failed += run_chirps(periodic_dir, http, args)
            else:
                failed += run_modis(product, periodic_dir, http, auth, args)
    except LoginError as exc:
        log.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        log.warning("Stopped. Run the same command again to continue.")
        return 130

    if args.dry_run:
        return 0
    if failed:
        log.warning("%d day(s)/composite(s) failed; they are listed in manifest.json. Run again to retry them.", failed)
        return 1
    log.info("Up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
