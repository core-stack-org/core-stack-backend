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

MODIS needs an Earthdata login: USERNAME_GESDISC and PASSWORD_GESDISC in nrm_app/.env or in
the environment. Finished files are skipped, so running it again continues an interrupted run
or adds the data published since the last run. The work is done by
computing/drought/local/download.py, which the API pan-india/drought_inputs/ also runs.

Example:
    python utilities/scripts/download_drought_inputs.py
"""

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_DIR))

from computing.drought.local.download import (  # noqa: E402
    CHIRPS_FIRST_YEAR,
    DEFAULT_BOUNDARY,
    MODIS_PRODUCTS,
    PRODUCTS,
    DownloadError,
    LoginError,
    run_download,
)
from computing.drought.local.store import run_as_checkout_owner  # noqa: E402

log = logging.getLogger("drought_inputs")


def main():
    run_as_checkout_owner(REPO_DIR)  # files stay usable by the workers, which run as the owner
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
    if not Path(args.data_dir).is_dir():
        parser.error(f"data folder not found: {args.data_dir}")

    try:
        summary = run_download(
            products=products,
            data_dir=args.data_dir,
            boundary=args.boundary,
            chirps_start_year=args.chirps_start_year,
            mod09a1_start_year=args.mod09a1_start_year,
            mod16a2gf_start_year=args.mod16a2gf_start_year,
            end_year=args.end_year,
            workers=args.workers,
            chirps_workers=args.chirps_workers,
            keep_raw=args.keep_raw,
            dry_run=args.dry_run,
        )
    except (DownloadError, LoginError) as exc:
        log.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        log.warning("Stopped. Run the same command again to continue.")
        return 130

    if args.dry_run:
        return 0
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
