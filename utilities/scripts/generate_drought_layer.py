#!/usr/bin/env python3
"""
Generate the pan-India 500 m kharif drought layer from the inputs that
download_drought_inputs.py stores, into <data-dir>/base_layers/drought/.

The method is the CoRE v3 writeup (section 3.2.2.1) with the thresholds and
severity rule of the Manual for Drought Management, updated to December 2020.

Stages:
  rain      on the CHIRPS grid for all of India: weekly totals since 1981, monsoon onset,
            dry-spell weeks and each window's rainfall trigger, each season against
            1981 to the year before it
  cropland  on the 500 m grid of the region: each pixel's kharif-sown share from the
            10 m LULC, the 2017-2024 normal, the cropland mask, and area sown per season
  vegetation  on the 500 m grid of the region: each window's highest NDVI and NDWI per
            year from 2000 (cloud-flagged images only as a fallback), and VCI per season
  mai       on the 500 m grid of the region: ET / PET per window from MOD16A2GF, and the
            window ET and PET
  classify  weekly drought labels with the 2020 rules, the season summary, and an
            EPSG:4326 copy of the summary for publishing
  mws       per micro-watershed: the indicators aggregated over each MWS, then the 2020
            rules, one pan-India GeoPackage per season

Each stage skips what is already done, so running again continues an
interrupted run or adds seasons whose inputs have arrived. The stages are run by
computing/drought/local/pipeline.py, which the API pan-india/drought/ also uses.

Examples:
    python utilities/scripts/generate_drought_layer.py --status
    python utilities/scripts/generate_drought_layer.py --seasons 2017-2024
    python utilities/scripts/generate_drought_layer.py --tile h25v07 --seasons 2023
    python utilities/scripts/generate_drought_layer.py --seasons 2023 --rerun-stages mai,classify,mws
"""

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_DIR))

from computing.drought.local.config import FIRST_SEASON, MANUAL_EDITION, METHOD_VERSION, Paths  # noqa: E402
from computing.drought.local.grid import load_grid, tile_window, window_mask  # noqa: E402
from computing.drought.local.pipeline import STAGES, run_pipeline, stage_list  # noqa: E402
from computing.drought.local.status import season_states  # noqa: E402
from computing.drought.local.store import NotWritable, RunInProgress, run_as_checkout_owner  # noqa: E402


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
    try:
        return stage_list(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


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


def main():
    run_as_checkout_owner(REPO_DIR)  # files stay usable by the workers, which run as the owner
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--status", action="store_true", help="show the state of each season and exit")
    parser.add_argument("--seasons", type=parse_seasons,
                        help=f"e.g. 2017-2024 or 2023 (default: {FIRST_SEASON} to this year)")
    parser.add_argument("--stages", type=parse_stages, default=STAGES,
                        help=f"comma-separated, any of {', '.join(STAGES)} (default: all)")
    parser.add_argument("--rerun-stages", type=parse_stages, default=[],
                        help="comma-separated stages whose files for these seasons are deleted first and made again")
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
    if set(args.rerun_stages) - set(args.stages):
        parser.error("--rerun-stages must be among --stages")

    if args.tile:
        grid = load_grid(paths)
        try:
            region = tile_window(grid, args.tile)
        except ValueError as exc:
            parser.error(str(exc))
        inside = int(window_mask(grid, region).sum())
        print(f"Region: tile {args.tile}, {region.width} x {region.height} pixels, {inside:,} inside India")

    if not args.status:
        try:
            run_pipeline(seasons, args.stages, args.rerun_stages, args.tile, args.workers, args.data_dir, args.lulc_dir)
        except (RunInProgress, NotWritable) as exc:
            raise SystemExit(str(exc))
        print()

    print_status(paths, seasons)
    return 0


if __name__ == "__main__":
    sys.exit(main())
