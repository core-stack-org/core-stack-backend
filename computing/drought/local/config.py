"""Paths and method settings for the local pan-India drought layer.

The method is the CoRE v3 writeup (section 3.2.2.1) with the thresholds and
severity rule of the Manual for Drought Management, updated to December 2020.
"""

from dataclasses import dataclass
from pathlib import Path

from computing.config_loader import DATA_DIR, LULC_BASE_DIR

METHOD_VERSION = "v1"
MANUAL_EDITION = "Manual for Drought Management, updated to December 2020"

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
                lulc_dir = data_dir / LULC_BASE_DIR.relative_to(DATA_DIR)
            except ValueError:
                lulc_dir = LULC_BASE_DIR
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

    def lulc_file(self, year):
        return self.lulc_dir / f"lulc_v3_{year}_{year + 1}.tif"

    def chirps_day(self, day):
        return self.chirps / "daily" / str(day.year) / f"chirps_{day:%Y_%m_%d}.tif"

    def modis_layer(self, product, start, suffix):
        prefix = MODIS_FILE_PREFIX[product]
        return self.modis(product) / str(start.year) / f"{prefix}_{start:%Y_%m_%d}_{suffix}.tif"
