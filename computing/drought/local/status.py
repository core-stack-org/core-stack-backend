"""Which seasons are done, which can run, and what the others are waiting for.

Readiness comes from the files and manifests that download_drought_inputs.py
writes. Progress comes from drought/status.json, which the stages update as they
finish:

    {"method_version": "v1",
     "shared":  {"rain": {"baselines": {...}, "seasons": {"2023": {...}}}},
     "regions": {"india": {"seasons": {"2023": {"state": "done", ...}}}, "h25v07": {...}}}
"""

import datetime as dt
import json
import os
from functools import lru_cache

from computing.drought.local.config import (
    FIRST_SEASON,
    METHOD_VERSION,
    MOD09A1_LAYERS,
    MOD16A2GF_LAYERS,
    ONSET_FIRST_WEEK,
    RAIN_NORMAL_YEARS,
    SEASON_LAST_WEEK,
    SOWN_NORMAL_YEARS,
)
from computing.drought.local.store import read_status
from computing.drought.local.weeks import season_composites, vci_history_years, week_end, week_start

DONE, OUTDATED, READY, WAITING = "done", "outdated", "ready", "waiting"
MODIS_LAYERS = {"mod09a1": MOD09A1_LAYERS, "mod16a2gf": MOD16A2GF_LAYERS}

# Which inputs each stage needs.
STAGE_INPUTS = {"rain": {"chirps"}}


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


@lru_cache(maxsize=None)
def _listdir(path):
    try:
        return frozenset(os.listdir(path))
    except OSError:
        return frozenset()


def _days(first, last):
    return [first + dt.timedelta(days=i) for i in range((last - first).days + 1)]


def missing_chirps_days(paths, first, last):
    return [d for d in _days(first, last) if paths.chirps_day(d).name not in _listdir(paths.chirps_day(d).parent)]


def missing_composites(paths, product, year):
    """Season composites of a year that are neither downloaded nor known to be missing at NASA."""
    gaps = read_json(paths.modis(product) / "manifest.json").get("missing_at_source", {})
    missing = []
    for start in season_composites(year):
        names = _listdir(paths.modis_layer(product, start, "x").parent)
        have = all(paths.modis_layer(product, start, s).name in names for s in MODIS_LAYERS[product])
        if not have and start.isoformat() not in gaps:
            missing.append(start)
    return missing


def _plural(n, word):
    return f"{n} {word}{'s' if n != 1 else ''}"


def baseline_gaps(paths):
    """[(input, reason)] for inputs every season shares."""
    gaps = []
    first, last = RAIN_NORMAL_YEARS
    missing = missing_chirps_days(paths, dt.date(first, 1, 1), dt.date(last, 12, 31))
    if missing:
        gaps.append(("chirps", f"CHIRPS {first}-{last} normals: {_plural(len(missing), 'day')} missing"))
    lulc = [y for y in range(SOWN_NORMAL_YEARS[0], SOWN_NORMAL_YEARS[1] + 1) if not paths.lulc_file(y).is_file()]
    if lulc:
        gaps.append(("lulc", "area-sown normal: " + ", ".join(paths.lulc_file(y).name for y in lulc) + " not found"))
    return gaps


def season_gaps(paths, season):
    """[(input, reason)] for what a season still needs; empty when it can run."""
    gaps = []
    first, last = week_start(season, ONSET_FIRST_WEEK), week_end(season, SEASON_LAST_WEEK)
    days = missing_chirps_days(paths, first, last)
    if days:
        gaps.append(("chirps", f"CHIRPS: {_plural(len(days), 'day')} missing up to {last}"))

    history = {y: missing_composites(paths, "mod09a1", y) for y in vci_history_years(season)}
    short = {y: len(m) for y, m in history.items() if m}
    if short:
        parts = ", ".join(f"{_plural(n, 'composite')} in {y}" for y, n in short.items())
        gaps.append(("mod09a1", f"MOD09A1: {parts} missing"))

    et = missing_composites(paths, "mod16a2gf", season)
    if len(et) == len(season_composites(season)):
        gaps.append(("mod16a2gf", f"MOD16A2GF {season}: not published yet (NASA releases a year in Jan-Feb of the next)"))
    elif et:
        gaps.append(("mod16a2gf", f"MOD16A2GF {season}: {_plural(len(et), 'composite')} missing"))

    if not paths.lulc_file(season).is_file():
        gaps.append(("lulc", f"LULC: {paths.lulc_file(season).name} not found"))
    return gaps


def stage_can_run(paths, stage, season):
    needs = STAGE_INPUTS[stage]
    return not any(inp in needs for inp, _ in baseline_gaps(paths) + season_gaps(paths, season))


def stages_done(status, season):
    done = []
    if str(season) in status.get("shared", {}).get("rain", {}).get("seasons", {}):
        done.append("rain")
    return done


def season_states(paths, seasons=None):
    """[(season, state, detail)] for the given seasons (default: FIRST_SEASON to this year)."""
    seasons = seasons or range(FIRST_SEASON, dt.date.today().year + 1)
    status = read_status(paths)
    if status.get("method_version", METHOD_VERSION) != METHOD_VERSION:
        status = {}  # made by another method version: nothing counts as done
    record = status.get("regions", {}).get(paths.region_key, {}).get("seasons", {})
    shared = baseline_gaps(paths)
    rows = []
    for season in seasons:
        done = record.get(str(season), {})
        if done.get("state") == DONE:
            if done.get("method_version") == METHOD_VERSION:
                rows.append((season, DONE, f"finished {done.get('finished_at', '?')}"))
            else:
                rows.append((season, OUTDATED, f"made with {done.get('method_version')}, now {METHOD_VERSION}"))
            continue
        gaps = shared + season_gaps(paths, season)
        stages = stages_done(status, season)
        progress = f"; stages done: {', '.join(stages)}" if stages else ""
        if gaps:
            rows.append((season, WAITING, "; ".join(text for _, text in gaps) + progress))
        else:
            rows.append((season, READY, "all inputs present" + progress))
    return rows
