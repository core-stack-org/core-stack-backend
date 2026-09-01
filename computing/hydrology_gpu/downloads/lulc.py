import geemap
import xarray
from .. import config as cfg
from . import GenericDownloader, ee
import json
import requests
from .rainfall import DownloaderBase as RainfallDownloader

class Downloader(GenericDownloader):
    """
    Currently, the LULC is mode of lulc's from start date to end date. Static
    """
    def main(self):
        region = self.load_region()

        end_date = ee.Date(cfg.ARG_END_DATE)
        start_date = ee.Date(cfg.ARG_START_DATE)

        dw_col = (ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')
                  .filterDate(start_date, end_date)
                  .filterBounds(region)
                  .select('label'))

        dw_image = dw_col.reduce(ee.Reducer.mode()).rename('lulc')

        dw_clip = dw_image.clipToBoundsAndScale(
            geometry=region,
            scale=cfg.GEE_SCALE
        )

        self.save_from_gee(dw_clip, region, cfg.LULC_PATH)
