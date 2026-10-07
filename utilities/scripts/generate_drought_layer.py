#!/usr/bin/env python3
"""
Generate the pan-India 500 m kharif drought layer from the inputs that
download_drought_inputs.py stores, into <data-dir>/base_layers/drought/.

The method is the CoRE v3 writeup (section 3.2.2.1) with the thresholds and
severity rule of the Manual for Drought Management, updated to December 2020.

Stages so far:
  rain      on the CHIRPS grid for all of India: weekly totals, 1991-2020 normals,
            SPI fits, monsoon onset, dry-spell weeks and each window's rainfall trigger
  cropland  on the 500 m grid of the region: each pixel's kharif-sown share from the
            10 m LULC, the 2017-2024 normal, the cropland mask, and area sown per season
  vegetation  on the 500 m grid of the region: each window's highest NDVI and NDWI per
            year from 2000 (cloud-flagged images only as a fallback), and VCI per season

Each stage skips what is already done, so running again continues an
interrupted run or adds seasons whose inputs have arrived.

Examples:
    python utilities/scripts/generate_drought_layer.py --status
    python utilities/scripts/generate_drought_layer.py --seasons 2017-2024
    python utilities/scripts/generate_drought_layer.py --tile h25v07 --seasons 2023
"""

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_DIR))

from computing.drought.local import cropland, rainfall, vegetation  # noqa: E402
from computing.drought.local.config import FIRST_SEASON, MANUAL_EDITION, METHOD_VERSION, Paths  # noqa: E402
from computing.drought.local.grid import load_grid, tile_window, window_mask  # noqa: E402
from computing.drought.local.status import season_states, stage_can_run  # noqa: E402
from computing.drought.local.store import run_lock, update_status  # noqa: E402

STAGES = ["rain", "cropland", "vegetation"]
log = logging.getLogger("drought_layer")


def parse_seasons(text):
    """'2017-2024' or '2023' or '2018,2023'."""
    seasons = []
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
            seasons.extend(range(a, b + 1))
        elif part:
            seasons.append(int(part))
    return sorted(set(seasons))


def parse_stages(text):
    stages = [s.strip() for s in text.split(",") if s.strip()]
    unknown = sorted(set(stages) - set(STAGES))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown stage(s): {', '.join(unknown)}; available: {', '.join(STAGES)}")
    return stages


def print_status(paths, seasons):
    print(f"Drought layer {METHOD_VERSION} ({MANUAL_EDITION})")
    print(f"Output: {paths.root}")
    print(f"LULC:   {paths.lulc_dir}")
    print()
    rows = season_states(paths, seasons)
    width = max(len(state) for _, state, _ in rows)
    print(f"{'Season':<8}{'State':<{width + 2}}Detail")
    for season, state, detail in rows:
        print(f"{season:<8}{state:<{width + 2}}{detail}")


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
        rain["baselines"] = {"finished_at": rain.get("baselines", {}).get("finished_at") or now(),
                             "normal_years": "1991-2020"}
        seasons_done = rain.setdefault("seasons", {})
        for season in runnable:
            if str(season) not in seasons_done or season in summaries:
                seasons_done[str(season)] = {"finished_at": now(), **summaries.get(season, {})}

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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--status", action="store_true", help="show the state of each season and exit")
    parser.add_argument("--seasons", type=parse_seasons,
                        help=f"e.g. 2017-2024 or 2023 (default: {FIRST_SEASON} to this year)")
    parser.add_argument("--stages", type=parse_stages, default=STAGES,
                        help=f"comma-separated, any of {', '.join(STAGES)} (default: all)")
    parser.add_argument("--tile", help="limit the run to one MODIS tile, e.g. h25v07; outputs go to drought/tiles/<tile>/")
    parser.add_argument("--workers", type=int, default=min(16, os.cpu_count() or 1), help="parallel processes")
    parser.add_argument("--data-dir", help="data folder (default: $DATA_DIR or ./data)")
    parser.add_argument("--lulc-dir", help="folder with lulc_v3_<year>_<year+1>.tif (default: the LULC folder in config.yaml)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    paths = Paths.build(args.data_dir, args.lulc_dir, args.tile)
    if not paths.data_dir.is_dir():
        parser.error(f"data folder not found: {paths.data_dir}")
    seasons = args.seasons or list(range(FIRST_SEASON, dt.date.today().year + 1))

    grid = load_grid(paths)
    region = grid.full
    if args.tile:
        try:
            region = tile_window(grid, args.tile)
        except ValueError as exc:
            parser.error(str(exc))
        inside = int(window_mask(grid, region).sum())
        print(f"Region: tile {args.tile}, {region.width} x {region.height} pixels, {inside:,} inside India")

    if not args.status:
        with run_lock(paths):
            if "rain" in args.stages:
                run_rain(paths, seasons, args.workers)
            if "cropland" in args.stages:
                run_cropland(paths, seasons, args.workers, region)
            if "vegetation" in args.stages:
                run_region_stage("vegetation", vegetation, paths, seasons, args.workers, region)
        print()

    print_status(paths, seasons)
    return 0


if __name__ == "__main__":
    sys.exit(main())
