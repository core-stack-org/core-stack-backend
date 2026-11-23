"""Paths and method settings for the local pan-India drought layer.

The method is the CoRE v3 writeup (section 3.2.2.1) with the thresholds and
severity rule of the Manual for Drought Management, updated to December 2020.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from computing.config_loader import DATA_DIR, LULC_TEMPORAL_CORRECTION_OUTPUT_DIR

METHOD_VERSION = "v1"
MANUAL_EDITION = "Manual for Drought Management, updated to December 2020"

# LULC: the temporally corrected LULC v4 (pan-india/lulc_temporal_correction_* APIs), one file
# per year, <prefix>_<Y>-07-01_<Y+1>-06-30_temporal_corrected.tif. Its manifest marks the newest
# year provisional until the next year's raw layer is added.
LULC_VERSION = "v4"
LULC_SUFFIX = "temporal_corrected.tif"
LULC_MANIFEST = "temporal_correction_manifest.json"

FIRST_SEASON = 2017
RAIN_RECORD_START = 1981  # CHIRPS's first year
SOWN_NORMAL_YEARS = (2017, 2024)
VCI_HISTORY_START = 2000

# Standard meteorological weeks.
ONSET_FIRST_WEEK = 19  # 7-13 May, "the second week of May" (writeup 3.2.2.3)
SEASON_LAST_WEEK = 43  # 22-28 October, the last full week before the 31 October deadline
WINDOW_WEEKS = 4

# Layers each stage reads, as named by utilities/scripts/download_drought_inputs.py.
MOD09A1_LAYERS = ("b01", "b02", "b06", "state")
MOD16A2GF_LAYERS = ("ET", "PET")
MODIS_FILE_PREFIX = {"mod09a1": "MOD09A1", "mod16a2gf": "MOD16A2GF"}


def rain_normal_years(season):
    """Season Y's rainfall is compared with 1981 to Y-1, as in GEE; the season's own year is left out,
    so a finished season never changes when later years are added."""
    return RAIN_RECORD_START, season - 1


@dataclass(frozen=True)
class Paths:
    data_dir: Path
    lulc_dir: Path
    region: str | None = None  # a MODIS tile such as "h25v07" for test runs

    @classmethod
    def build(cls, data_dir=None, lulc_dir=None, region=None):
        data_dir = Path(data_dir or DATA_DIR)
        if lulc_dir is None:
            try:
                lulc_dir = data_dir / LULC_TEMPORAL_CORRECTION_OUTPUT_DIR.relative_to(DATA_DIR)
            except ValueError:
                lulc_dir = LULC_TEMPORAL_CORRECTION_OUTPUT_DIR
        return cls(data_dir, Path(lulc_dir), region)

    @property
    def periodic(self):
        return self.data_dir / "base_layers" / "periodic"

    @property
    def chirps(self):
        return self.periodic / "rainfall_chirps"

    @property
    def modis_mask(self):
        return self.periodic / "modis_india_mask.tif"

    @property
    def india_outline(self):
        """Mainland India without islands; the download script's default boundary."""
        return self.data_dir / "base_layers" / "PanIndia_Boundaries" / "india_state_outer_no_islands.geojson"

    @property
    def soi_tehsils(self):
        return self.data_dir / "base_layers" / "admin-boundary" / "input" / "soi_tehsil.geojson"

    @property
    def tehsil_watersheds(self):
        """The MWS of the pan-India hydrology layers, one file per tehsil, with a manifest."""
        return self.data_dir / "base_layers" / "hydrology" / "runoff" / "commons" / "tehsil_watersheds"

    def modis(self, product):
        return self.periodic / product

    @property
    def base(self):
        return self.data_dir / "base_layers" / "drought"

    @property
    def root(self):
        """Outputs of this run's region: all of India, or drought/tiles/<tile>/ for a test run."""
        return self.base / "tiles" / self.region if self.region else self.base

    @property
    def version_dir(self):
        return self.root / METHOD_VERSION

    @property
    def shared_dir(self):
        """Pan-India products every region uses, such as the rainfall stage on the CHIRPS grid."""
        return self.base / METHOD_VERSION

    @property
    def region_key(self):
        return self.region or "india"

    @property
    def status_file(self):
        return self.base / "status.json"

    @property
    def lock_file(self):
        return self.base / ".lock"

    def lulc_manifest(self):
        try:
            return json.loads((self.lulc_dir / LULC_MANIFEST).read_text())
        except (OSError, ValueError):
            return {}

    def lulc_file(self, year):
        """The year's LULC v4 file: the one the manifest names, else the only file with the year's
        span; when there is none, the name it would have (for messages)."""
        span = f"{year}-07-01_{year + 1}-06-30"
        manifest = self.lulc_manifest()
        listed = manifest.get("years", {}).get(str(year), {}).get("file")
        if listed and (self.lulc_dir / listed).is_file():
            return self.lulc_dir / listed
        found = sorted(self.lulc_dir.glob(f"*_{span}_{LULC_SUFFIX}"))
        if len(found) > 1:
            raise ValueError(f"two LULC files for {year} in {self.lulc_dir}: {found[0].name}, {found[1].name}")
        if found:
            return found[0]
        return self.lulc_dir / f"{manifest.get('name') or 'LULC_v4'}_{span}_{LULC_SUFFIX}"

    def lulc_status(self, year):
        """"final" or "provisional" from the manifest; None when it does not list the year."""
        return self.lulc_manifest().get("years", {}).get(str(year), {}).get("status")

    def chirps_day(self, day):
        return self.chirps / "daily" / str(day.year) / f"chirps_{day:%Y_%m_%d}.tif"

    def modis_layer(self, product, start, suffix):
        prefix = MODIS_FILE_PREFIX[product]
        return self.modis(product) / str(start.year) / f"{prefix}_{start:%Y_%m_%d}_{suffix}.tif"
