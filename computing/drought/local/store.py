"""Reading and writing the drought layer's files and its status file.

Every raster is written to <name>.tmp and moved into place only when complete,
so a file that exists is always whole and a stopped run can simply be rerun.
"""

import datetime as dt
import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path

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


class RunInProgress(RuntimeError):
    """Another run holds the lock of drought/."""


@contextmanager
def run_lock(paths):
    """One run at a time writes into drought/."""
    paths.base.mkdir(parents=True, exist_ok=True)
    with open(paths.lock_file, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunInProgress(f"Another run is using {paths.base}. Wait for it to finish.")
        yield


def run_in_progress(paths):
    """True while a run holds the lock of drought/."""
    if not paths.lock_file.is_file():
        return False
    with open(paths.lock_file) as handle:  # read-only is enough to test a lock
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle, fcntl.LOCK_UN)
    return False


# ---------------------------------------------------------------- file owner


def run_as_checkout_owner(repo_dir):
    """When started as root (docker compose exec), switch to the owner of the checkout, as the
    containers' as-owner.sh does for the web app and the workers, so a script run writes files
    the workers can still change. Nothing changes when not root or when root owns the checkout."""
    if os.geteuid() != 0:
        return
    owner = os.stat(repo_dir)
    if owner.st_uid == 0:
        return
    home = Path("/tmp/corestack-home")
    home.mkdir(parents=True, exist_ok=True)
    os.chown(home, owner.st_uid, owner.st_gid)
    os.setgroups([])
    os.setgid(owner.st_gid)
    os.setuid(owner.st_uid)
    os.environ.update(HOME=str(home), USER="corestack", LOGNAME="corestack")


class NotWritable(PermissionError):
    """Folders a run must write to belong to another user."""


def not_writable(*roots):
    """A message naming the folders (and lock files) under roots that this process cannot write,
    with the fix; None when all are writable. Runs write through <name>.tmp and a rename, so a
    writable folder is enough for every file but the lock files."""
    bad = []
    for root in map(Path, roots):
        if not root.exists():
            continue
        for folder, _, files in os.walk(root):
            if not os.access(folder, os.W_OK | os.X_OK):
                bad.append(folder)
            bad += [os.path.join(folder, f) for f in files
                    if f.endswith(".lock") and not os.access(os.path.join(folder, f), os.W_OK)]
    if not bad:
        return None
    uid, gid = os.getuid(), os.getgid()
    shown = ", ".join(bad[:5]) + (f" and {len(bad) - 5} more" if len(bad) > 5 else "")
    return (f"Not writable by this process (uid {uid}): {shown}. They were made by another user, for example "
            f"by a script run as root before scripts switched to the checkout owner. Fix once with: "
            f"docker compose exec backend chown -R {uid}:{gid} {' '.join(map(str, roots))}")

