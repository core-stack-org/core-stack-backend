"""Pan-India temporal correction of LULC v4: reading, writing and year-to-year state.

The rules are in rules.py. This module reads the raw yearly rasters block by block, applies
the rules and writes one corrected raster per year, the way 4_temporal_correction.py does
for `--inputs DIR`, so both produce the same pixels.

Inputs: one GeoTIFF per year in one folder, all on one grid, named with
<YYYY>-07-01_<YYYY+1>-06-30. Band 1 is predicted_label, band 2 predicted_cluster. No data
is 0 (or the nodata value) in integer rasters and NaN in float ones.

Outputs: <prefix>_<YYYY>-07-01_<YYYY+1>-06-30_temporal_corrected.tif, one band, uint8 with
nodata 0 for integer inputs, float32 with NaN for float inputs. temporal_correction_manifest.json
in the output folder records each year as final or provisional.

  six-year      years Y..Y+5 corrected together. Y..Y+4 are final; Y+5 is provisional, since
                it was never tested against a following year.
  incremental   finalises the oldest provisional year N from corrected N-1, raw N and raw N+1,
                then writes raw N+1 through as the new provisional year. all_years=True keeps
                going while raw inputs exist.

The raw inputs must be kept: the incremental step reads raw N and raw N+1.
"""

import collections
import fcntl
import json
import logging
import multiprocessing
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window

from .rules import BAND_CLUSTER, BAND_LABEL, correct_block_6yr, correct_block_incremental

logger = logging.getLogger(__name__)

INPUT_RE = re.compile(r"(?P<y1>\d{4})-07-01_(?P<y2>\d{4})-06-30")
MANIFEST_NAME = "temporal_correction_manifest.json"
LOCK_NAME = ".temporal_correction.lock"
WINDOW_YEARS = 6
DEFAULT_BLOCK = 2048
DEFAULT_WORKERS = 6

MODE_SIX_YEAR = "batch6"  # the manifest keeps the reference script's mode names
MODE_INCREMENTAL = "incremental"
MODE_PASSTHROUGH = "passthrough"


class TemporalCorrectionError(ValueError):
    """The inputs or the output folder's state do not allow the requested run."""


class RunInProgress(RuntimeError):
    """Another run is writing into the same output folder."""


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
def span_for(year):
    return f"{year}-07-01_{year + 1}-06-30"


def discover_inputs(input_dir):
    """{start_year: path} for the yearly GeoTIFFs in input_dir."""
    input_dir = Path(input_dir)
    if not input_dir.is_dir():
        raise TemporalCorrectionError(f"input folder {input_dir} does not exist")
    found = {}
    for path in sorted(input_dir.glob("*.tif")):
        match = INPUT_RE.search(path.name)
        if not match:
            continue
        year = int(match.group("y1"))
        if year in found:
            raise TemporalCorrectionError(f"two inputs for {year}: {found[year].name} and {path.name}")
        found[year] = path.resolve()
    if not found:
        raise TemporalCorrectionError(f"no <YYYY>-07-01_<YYYY+1>-06-30 GeoTIFFs in {input_dir}")
    return dict(sorted(found.items()))


def _grid(path):
    with rasterio.open(path) as src:
        return src.width, src.height, src.crs, src.transform


def check_same_grid(paths):
    """Every year must share one grid, so a block window is the same pixels in every year."""
    paths = list(paths)
    ref = _grid(paths[0])
    for path in paths[1:]:
        if _grid(path) != ref:
            raise TemporalCorrectionError(f"{Path(path).name} is not on the same grid as {Path(paths[0]).name}")


def check_inputs(paths):
    for path in paths:
        with rasterio.open(path) as src:
            if src.count < 2:
                raise TemporalCorrectionError(
                    f"{Path(path).name} has {src.count} band(s); it needs predicted_label and predicted_cluster"
                )
    check_same_grid(paths)


def output_name(found):
    """The output file prefix: the first input's name up to its year span."""
    first = next(iter(found.values())).name
    return first[: INPUT_RE.search(first).start()].rstrip("_") or "LULC"


def output_dtype(ref):
    """uint8 (nodata 0) for integer inputs, float32 (nodata NaN) for float inputs."""
    with rasterio.open(ref) as src:
        return "uint8" if np.issubdtype(np.dtype(src.dtypes[0]), np.integer) else "float32"


def out_path(out_dir, name, year):
    return Path(out_dir) / f"{name}_{span_for(year)}_temporal_corrected.tif"


# ---------------------------------------------------------------------------
# Block reading and writing
# ---------------------------------------------------------------------------
def _nodata(dtype):
    return 0 if np.issubdtype(np.dtype(dtype), np.integer) else float("nan")


def _read_block(path, win, bands=(BAND_LABEL,)):
    """(labels as uint8 arrays, valid mask of the first band). No data comes back as 0."""
    with rasterio.open(path) as src:
        data = src.read(list(bands), window=win)
        nodata = src.nodata
    if np.issubdtype(data.dtype, np.floating):
        missing = np.isnan(data)
        if nodata is not None and not np.isnan(nodata):
            missing |= data == nodata
        outs = [np.rint(np.where(m, 0.0, b)).astype(np.uint8) for b, m in zip(data, missing)]
        return outs, ~missing[0]
    nodata = 0 if nodata is None else nodata
    outs = [np.where(b == nodata, 0, b).astype(np.uint8) for b in data]
    return outs, data[0] != nodata


def _writer(path, ref, dtype):
    with rasterio.open(ref) as src:
        profile = dict(
            driver="GTiff", dtype=dtype, nodata=_nodata(dtype), count=1,
            width=src.width, height=src.height, crs=src.crs, transform=src.transform,
            tiled=True, blockxsize=512, blockysize=512,
            compress="DEFLATE", zlevel=6, BIGTIFF="YES", num_threads="ALL_CPUS",
        )
    dst = rasterio.open(path, "w", **profile)
    dst.set_band_description(1, "predicted_label")
    return dst


def _tmp(path):
    return path.with_name(path.name + ".tmp")


def _blocks(ref, size):
    with rasterio.open(ref) as src:
        width, height = src.width, src.height
    for row in range(0, height, size):
        for col in range(0, width, size):
            yield Window(col, row, min(size, width - col), min(size, height - row))


def _emit(dst, arr, valid, win, dtype):
    out = arr.astype(dtype)
    out[~valid] = _nodata(dtype)
    dst.write(out, 1, window=win)


def _map_blocks(func, jobs, workers):
    """func(job) for every job, in job order. With workers > 1 the jobs run in a process pool,
    at most 2 x workers at a time, so finished blocks never pile up faster than they are written.

    Fork is used explicitly: the celery-heavy worker runs tasks in its main process (solo pool),
    and forked children need no Django setup to run the module-level block functions.
    """
    if workers <= 1:
        yield from map(func, jobs)
        return
    with multiprocessing.get_context("fork").Pool(workers) as pool:
        pending = collections.deque()
        for job in jobs:
            pending.append(pool.apply_async(func, (job,)))
            if len(pending) >= 2 * workers:
                yield pending.popleft().get()
        while pending:
            yield pending.popleft().get()


class _Progress:
    def __init__(self, total, label):
        self.total, self.label, self.t0 = total, label, time.time()
        self.every = max(1, total // 50)

    def __call__(self, i, with_data, empty):
        if i % self.every and i != self.total:
            return
        elapsed = time.time() - self.t0
        logger.info(
            "%s block %d/%d (%d with data, %d empty), %.1f min, ~%.0f min left",
            self.label, i, self.total, with_data, empty, elapsed / 60,
            elapsed / i * (self.total - i) / 60,
        )


# ---------------------------------------------------------------------------
# Manifest and lock
# ---------------------------------------------------------------------------
def manifest_path(out_dir):
    return Path(out_dir) / MANIFEST_NAME


def load_manifest(out_dir):
    path = manifest_path(out_dir)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"name": None, "years": {}, "history": []}


def save_manifest(out_dir, man):
    path = manifest_path(out_dir)
    tmp = _tmp(path)
    with open(tmp, "w") as f:
        json.dump(man, f, indent=2)
    os.replace(tmp, path)


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _record(man, name, year, status, mode):
    man["name"] = name
    man["years"][str(year)] = {
        "span": span_for(year),
        "status": status,  # "final" or "provisional"
        "file": out_path("", name, year).name,
        "mode": mode,
        "written": _now(),
    }


def year_states(out_dir):
    """{year: status} from the manifest in out_dir; empty when there is none."""
    return {int(y): v["status"] for y, v in load_manifest(out_dir).get("years", {}).items()}


@contextmanager
def run_lock(out_dir):
    """One run at a time writes into out_dir."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    with open(Path(out_dir) / LOCK_NAME, "w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunInProgress(f"Another temporal-correction run is writing into {out_dir}.")
        yield


def run_in_progress(out_dir):
    """True while a run holds the lock of out_dir."""
    lock = Path(out_dir) / LOCK_NAME
    if not lock.is_file():
        return False
    with open(lock) as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle, fcntl.LOCK_UN)
    return False


# ---------------------------------------------------------------------------
# Plans: what a run would do, checked before queueing and again when it starts
# ---------------------------------------------------------------------------
def plan_six_year(input_dir, start_year=None):
    """{"name", "years"} for a six-year run; raises TemporalCorrectionError if it cannot run."""
    found = discover_inputs(input_dir)
    start = int(start_year) if start_year is not None else next(iter(found))
    window = [start + i for i in range(WINDOW_YEARS)]
    missing = [y for y in window if y not in found]
    if missing:
        raise TemporalCorrectionError(
            f"the six-year run needs {window[0]}..{window[-1]}; missing input years: {missing}"
        )
    return {"name": output_name(found), "years": window}


def plan_incremental(input_dir, out_dir, all_years=False):
    """{"name", "steps", "provisional"} for an incremental run: the years it finalises, and the
    year left provisional. Raises TemporalCorrectionError if there is nothing it can do."""
    found = discover_inputs(input_dir)
    man = load_manifest(out_dir)
    if not man.get("years"):
        raise TemporalCorrectionError(f"no {MANIFEST_NAME} in {out_dir}; run the six-year API first")
    name = output_name(found)
    if man.get("name") not in (None, name):
        raise TemporalCorrectionError(
            f"{out_dir} holds '{man['name']}' outputs, but the inputs are named '{name}'"
        )
    provisional = sorted(int(y) for y, v in man["years"].items() if v["status"] == "provisional")
    if not provisional:
        raise TemporalCorrectionError("no provisional year to finalise")
    year = provisional[0]
    steps = []
    while year + 1 in found:
        steps.append(year)
        if not all_years:
            break
        year += 1
    if not steps:
        raise TemporalCorrectionError(
            f"{year} stays provisional until the raw {span_for(year + 1)} input is added"
        )
    return {"name": name, "steps": steps, "provisional": steps[-1] + 1}


# ---------------------------------------------------------------------------
# Six-year run
# ---------------------------------------------------------------------------
def _six_year_block(job):
    paths, win = job
    fine, cluster, valid = [], [], None
    for path in paths:
        (lab, clu), v = _read_block(path, win, (BAND_LABEL, BAND_CLUSTER))
        fine.append(lab)
        cluster.append(clu)
        valid = v if valid is None else (valid | v)
    if not valid.any():  # most of the canvas is outside the data
        return win, None, valid
    return win, correct_block_6yr(fine, cluster), valid


def run_six_year(input_dir, out_dir, start_year=None, workers=DEFAULT_WORKERS, block=DEFAULT_BLOCK):
    """Correct six years as one window. Returns a summary of what was written."""
    out_dir = Path(out_dir)
    with run_lock(out_dir):
        plan = plan_six_year(input_dir, start_year)
        name, window = plan["name"], plan["years"]
        found = discover_inputs(input_dir)
        paths = [found[y] for y in window]
        check_inputs(paths)
        ref = paths[0]
        dtype = output_dtype(ref)

        with rasterio.open(ref) as src:
            logger.info("six-year %s, years %d..%d, grid %d x %d, dtype %s, block %d, workers %d",
                        name, window[0], window[-1], src.width, src.height, dtype, block, workers)

        jobs = [(paths, win) for win in _blocks(ref, block)]
        progress = _Progress(len(jobs), "six-year")
        targets = [out_path(out_dir, name, y) for y in window]
        dsts = [_writer(_tmp(t), ref, dtype) for t in targets]
        try:
            done = skipped = 0
            for i, (win, finals, valid) in enumerate(_map_blocks(_six_year_block, jobs, workers), 1):
                if finals is None:
                    skipped += 1
                    finals = [np.zeros(valid.shape, np.uint8)] * len(window)
                else:
                    done += 1
                for dst, arr in zip(dsts, finals):
                    _emit(dst, arr, valid, win, dtype)
                progress(i, done, skipped)
        finally:
            for dst in dsts:
                dst.close()
        for target in targets:
            os.replace(_tmp(target), target)

        man = load_manifest(out_dir)
        for i, year in enumerate(window):
            # the last year is the window edge - never a singlet centre - so the incremental
            # run finalises it once the following year exists
            _record(man, name, year, "final" if i < WINDOW_YEARS - 1 else "provisional", MODE_SIX_YEAR)
        man["history"].append({"mode": MODE_SIX_YEAR, "years": window, "at": _now()})
        save_manifest(out_dir, man)

    logger.info("six-year: years %d..%d final; %d provisional (finalised when %d is added)",
                window[0], window[-2], window[-1], window[-1] + 1)
    return {
        "mode": "six_year",
        "name": name,
        "output_dir": str(out_dir),
        "final": window[:-1],
        "provisional": window[-1],
        "files": [t.name for t in targets],
    }


# ---------------------------------------------------------------------------
# Incremental run
# ---------------------------------------------------------------------------
def _incremental_block(job):
    prev_p, cur_p, next_p, win = job
    (cur, clu), v_cur = _read_block(cur_p, win, (BAND_LABEL, BAND_CLUSTER))
    (nxt,), v_nxt = _read_block(next_p, win)
    (prev,), v_prev = _read_block(prev_p, win)
    valid = v_cur | v_nxt | v_prev
    if not valid.any():
        return win, None, valid, 0, 0
    final, aba = correct_block_incremental(prev, cur, clu, nxt)
    return win, final, valid, int((aba & v_cur).sum()), int(v_cur.sum())


def _incremental_step(out_dir, found, name, dtype, year, workers, block):
    """Finalise `year` from corrected year-1, raw year and raw year+1. Returns (flipped, valid)."""
    prev_p = out_path(out_dir, name, year - 1)
    if not prev_p.exists():
        raise TemporalCorrectionError(f"need the corrected {year - 1} at {prev_p}; run the six-year API first")
    ref = found[year]
    check_same_grid([ref, prev_p])

    target = out_path(out_dir, name, year)
    jobs = [(prev_p, found[year], found[year + 1], win) for win in _blocks(ref, block)]
    progress = _Progress(len(jobs), str(year))
    dst = _writer(_tmp(target), ref, dtype)
    flipped = total = done = skipped = 0
    try:
        for i, (win, final, valid, f, t) in enumerate(_map_blocks(_incremental_block, jobs, workers), 1):
            if final is None:
                skipped += 1
                final = np.zeros(valid.shape, np.uint8)
            else:
                done += 1
            flipped += f
            total += t
            _emit(dst, final, valid, win, dtype)
            progress(i, done, skipped)
    finally:
        dst.close()
    os.replace(_tmp(target), target)

    pct = flipped / total * 100 if total else 0.0
    logger.info("%d finalised: %s of %s valid px flipped (%.3f%%)", year, f"{flipped:,}", f"{total:,}", pct)
    return flipped, total


def _passthrough_block(job):
    path, win = job
    (lab,), valid = _read_block(path, win)
    return win, lab, valid


def _write_uncorrected(out_dir, src, name, dtype, year, workers, block):
    """Write the newest year through unmodified so the folder holds every year."""
    target = out_path(out_dir, name, year)
    dst = _writer(_tmp(target), src, dtype)
    try:
        jobs = [(src, win) for win in _blocks(src, block)]
        for win, lab, valid in _map_blocks(_passthrough_block, jobs, workers):
            _emit(dst, lab, valid, win, dtype)
    finally:
        dst.close()
    os.replace(_tmp(target), target)
    logger.info("%d written uncorrected (provisional)", year)


def run_incremental(input_dir, out_dir, all_years=False, workers=DEFAULT_WORKERS, block=DEFAULT_BLOCK):
    """Finalise the oldest provisional year (all_years: every year that can be). Returns a summary."""
    out_dir = Path(out_dir)
    with run_lock(out_dir):
        plan = plan_incremental(input_dir, out_dir, all_years)
        name, steps, newest = plan["name"], plan["steps"], plan["provisional"]
        found = discover_inputs(input_dir)
        check_inputs([found[y] for y in sorted(set(steps) | {newest})])
        dtype = output_dtype(found[steps[0]])
        logger.info("incremental %s, finalising %s, workers %d", name, steps, workers)

        man = load_manifest(out_dir)
        flips = {}
        for year in steps:
            flipped, total = _incremental_step(out_dir, found, name, dtype, year, workers, block)
            flips[year] = {"flipped": flipped, "valid": total}
            _record(man, name, year, "final", MODE_INCREMENTAL)
            save_manifest(out_dir, man)  # a run stopped later resumes from the next year

        _write_uncorrected(out_dir, found[newest], name, dtype, newest, workers, block)
        _record(man, name, newest, "provisional", MODE_PASSTHROUGH)
        man["history"].append({"mode": MODE_INCREMENTAL, "years": steps, "at": _now()})
        save_manifest(out_dir, man)

    logger.info("incremental: %d left provisional (finalised when %d is added)", newest, newest + 1)
    return {
        "mode": "incremental",
        "name": name,
        "output_dir": str(out_dir),
        "final": steps,
        "provisional": newest,
        "flips": {str(y): v for y, v in flips.items()},
    }
