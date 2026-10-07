"""Which seasons are done, which can run, and what the others are waiting for.

Readiness comes from the files and manifests that download_drought_inputs.py
writes; "done" comes from status.json, which the stages write as they finish.
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
from computing.drought.local.weeks import season_composites, vci_history_years, week_end, week_start

DONE, OUTDATED, READY, WAITING = "done", "outdated", "ready", "waiting"
MODIS_LAYERS = {"mod09a1": MOD09A1_LAYERS, "mod16a2gf": MOD16A2GF_LAYERS}


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


def baseline_gaps(paths):
    """Inputs every season shares: rainfall normal years and the area-sown normal years."""
    reasons = []
    first, last = RAIN_NORMAL_YEARS
    missing = missing_chirps_days(paths, dt.date(first, 1, 1), dt.date(last, 12, 31))
    if missing:
        reasons.append(f"CHIRPS {first}-{last} normals: {len(missing)} days missing")
    lulc = [y for y in range(SOWN_NORMAL_YEARS[0], SOWN_NORMAL_YEARS[1] + 1) if not paths.lulc_file(y).is_file()]
    if lulc:
        reasons.append("area-sown normal: " + ", ".join(paths.lulc_file(y).name for y in lulc) + " not found")
    return reasons


def season_gaps(paths, season):
    """What a season still needs, in words; empty when it can run."""
    reasons = []
    first, last = week_start(season, ONSET_FIRST_WEEK), week_end(season, SEASON_LAST_WEEK)
    days = missing_chirps_days(paths, first, last)
    if days:
        reasons.append(f"CHIRPS: {len(days)} days missing up to {last}")

    history = {y: missing_composites(paths, "mod09a1", y) for y in vci_history_years(season)}
    short = {y: len(m) for y, m in history.items() if m}
    if short:
        parts = ", ".join(f"{n} composite{'s' if n > 1 else ''} in {y}" for y, n in short.items())
        reasons.append(f"MOD09A1: {parts} missing")

    et = missing_composites(paths, "mod16a2gf", season)
    if len(et) == len(season_composites(season)):
        reasons.append(f"MOD16A2GF {season}: not published yet (NASA releases a year in Jan-Feb of the next)")
    elif et:
        reasons.append(f"MOD16A2GF {season}: {len(et)} composite{'s' if len(et) > 1 else ''} missing")

    if not paths.lulc_file(season).is_file():
        reasons.append(f"LULC: {paths.lulc_file(season).name} not found")
    return reasons


def read_status(paths):
    return read_json(paths.status_file)


def season_states(paths, seasons=None):
    """[(season, state, detail)] for the given seasons (default: FIRST_SEASON to this year)."""
    seasons = seasons or range(FIRST_SEASON, dt.date.today().year + 1)
    record = read_status(paths).get("seasons", {})
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
        rows.append((season, WAITING, "; ".join(gaps)) if gaps else (season, READY, "all inputs present"))
    return rows
