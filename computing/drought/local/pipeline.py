"""Runs the drought layer's stages in order for the given seasons. The command line
(utilities/scripts/generate_drought_layer.py) and the API pan-india/drought/ both call
run_pipeline().

Each stage skips what is already done, so a run continues an interrupted one or adds
seasons whose inputs have arrived; drought/status.json records what each stage finished.
Only one run writes into drought/ at a time (store.run_lock).

rerun_stages deletes a stage's own files for the given seasons before the run, so the
stage makes them again and the later stages follow. Files that several seasons share are
kept: weekly rainfall and onset inputs (rain), the 2017-2024 normal (cropland), the yearly
window maxima (vegetation) and the copy of the MWS (mws).
"""

import datetime as dt
import logging
import os

from computing.drought.local import classify, cropland, moisture, mws, rainfall, vegetation
from computing.drought.local import status as season_status
from computing.drought.local.config import FIRST_SEASON, METHOD_VERSION, Paths
from computing.drought.local.grid import load_grid, tile_window
from computing.drought.local.status import season_states, stage_can_run
from computing.drought.local.store import NotWritable, not_writable, run_lock, update_status

log = logging.getLogger(__name__)

STAGES = ["rain", "cropland", "vegetation", "mai", "classify", "mws"]


def stage_list(value):
    """Stages from a list or a comma-separated string, in run order. ValueError for unknown names."""
    names = value.split(",") if isinstance(value, str) else list(value)
    stages = {str(s).strip().lower() for s in names if str(s).strip()}
    unknown = sorted(stages - set(STAGES))
    if unknown:
        raise ValueError(f"unknown stage(s): {', '.join(unknown)}; available: {', '.join(STAGES)}")
    return [s for s in STAGES if s in stages]


def season_outputs(paths, stage, season):
    """The files a stage writes for one season alone."""
    return {
        "rain": [rainfall.season_file(paths, season)],
        "cropland": [cropland.share_file(paths, season)],
        "vegetation": [vegetation.vci_file(paths, season)],
        "mai": [moisture.mai_file(paths, season)],
        "classify": [classify.labels_file(paths, season), classify.summary_file(paths, season),
                     classify.published_file(paths, season)],
        "mws": [mws.layer_file(paths, season)],
    }[stage]


def clear_outputs(paths, stages, seasons):
    """Delete the stages' own files for the seasons; returns the files deleted."""
    removed = []
    for stage in stages:
        for season in seasons:
            for path in season_outputs(paths, stage, season):
                if path.is_file():
                    path.unlink()
                    removed.append(str(path))
    if removed:
        log.info("Deleted %d file(s) to run %s again: %s", len(removed), ", ".join(stages), ", ".join(removed))
    return removed


def now():
    return dt.datetime.now().isoformat(timespec="seconds")


def run_rain(paths, seasons, workers):
    runnable = [s for s in seasons if stage_can_run(paths, "rain", s)]
    skipped = sorted(set(seasons) - set(runnable))
    if skipped:
        log.info("Rain stage: inputs incomplete for %s; skipped", ", ".join(map(str, skipped)))
    if not runnable:
        return
    summaries = rainfall.run(paths, runnable, workers)

    def record(status):
        rain = status.setdefault("shared", {}).setdefault("rain", {})
        rain.pop("baselines", None)  # from the earlier fixed 1991-2020 normals
        seasons_done = rain.setdefault("seasons", {})
        for season in runnable:
            if str(season) not in seasons_done or season in summaries:
                seasons_done[str(season)] = {"finished_at": now(), **summaries.get(season, {})}
        # New rainfall results: every region has to classify these seasons again.
        for region in status.get("regions", {}).values():
            for season in summaries:
                region.get("seasons", {}).pop(str(season), None)
                region.get("mws", {}).get("seasons", {}).pop(str(season), None)

    update_status(paths, record)


def run_cropland(paths, seasons, workers, region):
    runnable = [s for s in seasons if stage_can_run(paths, "cropland", s)]
    skipped = sorted(set(seasons) - set(runnable))
    if skipped:
        log.info("Cropland stage: LULC missing for %s; skipped", ", ".join(map(str, skipped)))
    if not runnable:
        return
    written = cropland.run(paths, runnable, region, workers)

    def record(status):
        part = status.setdefault("regions", {}).setdefault(paths.region_key, {}).setdefault("cropland", {})
        part.setdefault("normal", {"finished_at": now(), "years": "2017-2024"})
        done = part.setdefault("seasons", {})
        for season in runnable:
            if season in written or str(season) not in done:
                done[str(season)] = {"finished_at": now()}

    update_status(paths, record)


def run_region_stage(name, module, paths, seasons, workers, region):
    """Run a stage that works on the region's 500 m grid and record the seasons it finished."""
    runnable = [s for s in seasons if stage_can_run(paths, name, s)]
    skipped = sorted(set(seasons) - set(runnable))
    if skipped:
        log.info("%s stage: inputs incomplete for %s; skipped", name.capitalize(), ", ".join(map(str, skipped)))
    if not runnable:
        return
    written = module.run(paths, runnable, region, workers)

    def record(status):
        done = status.setdefault("regions", {}).setdefault(paths.region_key, {}).setdefault(name, {}).setdefault("seasons", {})
        for season in runnable:
            if season in written or str(season) not in done:
                done[str(season)] = {"finished_at": now()}

    update_status(paths, record)


def run_classify(paths, seasons, workers, region):
    runnable = [s for s in seasons if stage_can_run(paths, "classify", s)]
    if not runnable:
        return
    written = classify.run(paths, runnable, region, workers)

    def record(status):
        done = status.setdefault("regions", {}).setdefault(paths.region_key, {}).setdefault("seasons", {})
        for season in runnable:
            if classify.summary_file(paths, season).is_file() and (season in written or str(season) not in done):
                done[str(season)] = {"state": "done", "method_version": METHOD_VERSION, "finished_at": now(),
                                     "published": str(classify.published_file(paths, season))}

    update_status(paths, record)


def run_mws(paths, seasons, grid, region):
    runnable = [s for s in seasons if stage_can_run(paths, "mws", s)]
    if not runnable:
        return
    written = mws.run(paths, runnable, grid, region)

    def record(status):
        done = status.setdefault("regions", {}).setdefault(paths.region_key, {}).setdefault("mws", {}).setdefault("seasons", {})
        for season in runnable:
            if mws.layer_file(paths, season).is_file() and (season in written or str(season) not in done):
                done[str(season)] = {"finished_at": now(), "layer": str(mws.layer_file(paths, season))}

    update_status(paths, record)


def default_seasons():
    return list(range(FIRST_SEASON, dt.date.today().year + 1))


def run_pipeline(seasons=None, stages=None, rerun_stages=None, tile=None, workers=None, data_dir=None, lulc_dir=None):
    """Run the stages for the seasons (default: 2017 to this year; seasons whose inputs are missing are
    left out). Raises store.RunInProgress when another run holds the lock and store.NotWritable when
    drought/ holds files of another user. Returns a summary with each season's state after the run."""
    paths = Paths.build(data_dir, lulc_dir, tile)
    seasons = sorted({int(s) for s in seasons}) if seasons else default_seasons()
    stages = stage_list(stages) if stages else list(STAGES)
    rerun = stage_list(rerun_stages) if rerun_stages else []
    if set(rerun) - set(stages):
        raise ValueError(f"rerun_stages must be among the stages run: {', '.join(sorted(set(rerun) - set(stages)))}")
    workers = workers or min(16, os.cpu_count() or 1)
    message = not_writable(paths.base)
    if message:
        raise NotWritable(message)
    season_status._listdir.cache_clear()  # a long-lived worker lists the input folders afresh
    grid = load_grid(paths)
    region = tile_window(grid, tile) if tile else grid.full

    with run_lock(paths):
        removed = clear_outputs(paths, rerun, seasons)
        if "rain" in stages:
            run_rain(paths, seasons, workers)
        if "cropland" in stages:
            run_cropland(paths, seasons, workers, region)
        if "vegetation" in stages:
            run_region_stage("vegetation", vegetation, paths, seasons, workers, region)
        if "mai" in stages:
            run_region_stage("mai", moisture, paths, seasons, workers, region)
        if "classify" in stages:
            run_classify(paths, seasons, workers, region)
        if "mws" in stages:
            run_mws(paths, seasons, grid, region)

    return {"seasons": seasons, "stages": stages, "rerun_stages": rerun, "removed": removed,
            "region": paths.region_key,
            "states": [{"season": s, "state": state, "detail": detail} for s, state, detail in season_states(paths, seasons)]}
