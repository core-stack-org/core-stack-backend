"""
Currently, this calculates Slope (gradient) on GEE Servers.
DEM is not downloaded.
"""
from pathlib import Path

import geopandas as gpd
import rasterio
from rasterio.mask import mask
import xarray
from .. import config as cfg
from . import GenericDownloader, ee
import geemap


def clip_local_raster(source_path, boundary_path, output_path=None, logger=None, fill_value=0):
    source_path = Path(source_path)
    boundary_path = Path(boundary_path)
    output_path = Path(output_path or cfg.DEMFILE_PATH)

    if logger:
        logger.info("Clipping local terrain raster %s to %s", source_path, output_path)
        logger.warning(
            "Reading local terrain raster with GTIFF_IGNORE_READ_ERRORS=YES; unreadable source tiles may be filled by GDAL"
        )

    gdf = gpd.read_file(boundary_path)
    if gdf.empty:
        raise ValueError(f"No geometries found in boundary file: {boundary_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.Env(GTIFF_IGNORE_READ_ERRORS="YES"):
        with rasterio.open(source_path) as src:
            if gdf.crs is None:
                gdf = gdf.set_crs(src.crs)
            elif gdf.crs != src.crs:
                gdf = gdf.to_crs(src.crs)

            data, transform = mask(
                src,
                gdf.geometry,
                crop=True,
                filled=True,
                nodata=fill_value,
            )

            profile = src.profile.copy()
            profile.update(
                driver="GTiff",
                height=data.shape[1],
                width=data.shape[2],
                transform=transform,
                nodata=fill_value,
                tiled=True,
                compress="ZSTD",
                ZSTD_LEVEL=1,
                NUM_THREADS=10,
            )

            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(data)

    if logger:
        logger.info("Saved clipped local terrain raster to %s", output_path)

    return output_path


class Downloader(GenericDownloader):
    def __init__(self):
        """
        Override parent class's init. As GeoTiff handler needs one tif file for CRS reference
        We download DEM as this reference
        """
        pass

    def main(self):
        dataset = ee.Image('USGS/SRTMGL1_003')
        elevation = dataset.select('elevation')

        region = self.load_region()

        # 1. Calculate slope in degrees
        slope_deg = ee.Terrain.slope(elevation)

        # 2. Convert to Gradient: tan(slope * pi / 180)
        slope_gradient = slope_deg.multiply(3.141592).divide(180).tan()

        elevation_clip = slope_gradient.clipToBoundsAndScale(
            geometry=region,
            scale=cfg.GEE_SCALE
        )

        self.save_from_gee(elevation_clip, region, cfg.DEMFILE_PATH)
