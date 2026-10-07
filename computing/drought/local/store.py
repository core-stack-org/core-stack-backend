"""Reading and writing the drought layer's files and its status file.

Every raster is written to <name>.tmp and moved into place only when complete,
so a file that exists is always whole and a stopped run can simply be rerun.
"""

import datetime as dt
import fcntl
import json
import os
from contextlib import contextmanager

import numpy as np
import rasterio

from computing.drought.local.config import MANUAL_EDITION, METHOD_VERSION


def write_stack(path, data, transform, crs, names, nodata, tags=None):
    """data: (bands, rows, cols). Band descriptions = names, so readers ask for bands by name."""
    data = np.asarray(data)
    if data.ndim == 2:
        data = data[None]
    if len(names) != data.shape[0]:
        raise ValueError(f"{len(names)} names for {data.shape[0]} bands")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    float_data = np.issubdtype(data.dtype, np.floating)
    profile = {
        "driver": "GTiff",
        "count": data.shape[0],
        "height": data.shape[1],
        "width": data.shape[2],
        "dtype": data.dtype.name,
        "crs": crs,
        "transform": transform,
        "nodata": nodata,
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
        "compress": "deflate",
        "predictor": 3 if float_data else 2,
        "interleave": "band",
        "bigtiff": "IF_SAFER",
    }
    base_tags = {"method_version": METHOD_VERSION, "manual": MANUAL_EDITION,
                 "written_at": dt.datetime.now().isoformat(timespec="seconds")}
    with rasterio.open(tmp, "w", **profile) as dst:
        dst.write(data)
        for i, name in enumerate(names, start=1):
            dst.set_band_description(i, name)
        dst.update_tags(**{k: str(v) for k, v in {**base_tags, **(tags or {})}.items()})
    os.replace(tmp, path)


def band_names(path):
    with rasterio.open(path) as src:
        return list(src.descriptions)


def read_bands(path, names=None, window=None):
    """Bands as float32 with nodata turned into NaN; names default to all bands."""
    with rasterio.open(path) as src:
        all_names = list(src.descriptions)
        names = names or all_names
        indexes = [all_names.index(n) + 1 for n in names]
        data = src.read(indexes, window=window).astype(np.float32)
        if src.nodata is not None and not np.isnan(src.nodata):
            data[data == src.nodata] = np.nan
    return data


def read_tags(path):
    with rasterio.open(path) as src:
        return src.tags()


def raster_profile(path):
    with rasterio.open(path) as src:
        return src.transform, src.crs, (src.height, src.width)


INT16_NODATA = -32768


def to_int16(values, scale=1.0, low=-32767, high=32767):
    """Scale and round floats to int16; NaN becomes the nodata value -32768."""
    values = np.asarray(values, dtype=np.float64) * scale
    out = np.clip(np.round(values), low, high)
    out[np.isnan(values)] = INT16_NODATA
    return out.astype(np.int16)


# ---------------------------------------------------------------- status file


def read_status(paths):
    try:
        return json.loads(paths.status_file.read_text())
    except (OSError, ValueError):
        return {}


def update_status(paths, change):
    """Apply change(status_dict) and write the file atomically. Call inside run_lock()."""
    status = read_status(paths)
    status.setdefault("method_version", METHOD_VERSION)
    change(status)
    status["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    paths.status_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = paths.status_file.with_name(f"{paths.status_file.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(status, indent=2, sort_keys=True))
    os.replace(tmp, paths.status_file)


@contextmanager
def run_lock(paths):
    """One run at a time writes into drought/."""
    paths.base.mkdir(parents=True, exist_ok=True)
    with open(paths.lock_file, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"Another run is using {paths.base}. Wait for it to finish.")
        yield
