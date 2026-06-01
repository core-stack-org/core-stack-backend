from argparse import ArgumentParser
from .. import config as cfg
import xarray
from . import GenericDownloader, ee
import geemap

class Downloader(GenericDownloader):
    def main(self):
        region = self.load_region()

        hsg_image = ee.Image('projects/ee-dharmisha-siddharth/assets/HYSOGs250m')

        hsg_clip = hsg_image.clipToBoundsAndScale(
            geometry=region,
            scale=cfg.GEE_SCALE
        )

        self.save_from_gee(hsg_clip, region, cfg.SOIL_PATH)
