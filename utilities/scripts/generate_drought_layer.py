#!/usr/bin/env python3
"""
Generate the pan-India 500 m kharif drought layer from the inputs that
download_drought_inputs.py stores, into <data-dir>/base_layers/drought/.

The method is the CoRE v3 writeup (section 3.2.2.1) with the thresholds and
severity rule of the Manual for Drought Management, updated to December 2020.

The stages are being added one at a time. For now the script shows which
seasons are done, which can run, and what the others are waiting for.

Examples:
    python utilities/scripts/generate_drought_layer.py --status
    python utilities/scripts/generate_drought_layer.py --status --tile h25v07
"""

import argparse
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_DIR))

from computing.drought.local.config import MANUAL_EDITION, METHOD_VERSION, Paths  # noqa: E402
from computing.drought.local.grid import load_grid, tile_window, window_mask  # noqa: E402
from computing.drought.local.status import season_states  # noqa: E402


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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--status", action="store_true", help="show the state of each season and exit")
    parser.add_argument("--seasons", type=parse_seasons, help="e.g. 2017-2024 or 2023 (default: 2017 to this year)")
    parser.add_argument("--tile", help="limit the run to one MODIS tile, e.g. h25v07; outputs go to drought/tiles/<tile>/")
    parser.add_argument("--data-dir", help="data folder (default: $DATA_DIR or ./data)")
    parser.add_argument("--lulc-dir", help="folder with lulc_v3_<year>_<year+1>.tif (default: the LULC folder in config.yaml)")
    args = parser.parse_args()

    paths = Paths.build(args.data_dir, args.lulc_dir, args.tile)
    if not paths.data_dir.is_dir():
        parser.error(f"data folder not found: {paths.data_dir}")

    if args.tile:
        grid = load_grid(paths)
        try:
            region = tile_window(grid, args.tile)
        except ValueError as exc:
            parser.error(str(exc))
        inside = int(window_mask(grid, region).sum())
        print(f"Region: tile {args.tile}, {region.width} x {region.height} pixels, {inside:,} inside India")

    print_status(paths, args.seasons)
    if not args.status:
        print("\nNo stages are implemented yet; nothing was computed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
