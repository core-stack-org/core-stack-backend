import os
import shutil
from pathlib import Path
from logging import Logger
from dataclasses import dataclass
import ee
import geedim
import geopandas as gpd
import requests

from .. import config as cfg
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from .. import utils
from ..utils import GeoTIFFHandler


def _initialize_earth_engine():
    project = getattr(cfg, "GEE_PROJECT_NAME", "")
    try:
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
        return
    except Exception:
        pass

    try:
        from utilities.gee_utils import ee_initialize_safe

        ee_initialize_safe()
    except Exception as exc:
        print(f"Skipping Earth Engine initialization: {exc}")


_initialize_earth_engine()

class GenericDownloader:
    # Singleton pattern
    # _instance = None
    # def __new__(cls, *args, **kwargs):
    #     if cls._instance is None:
    #         cls._instance = super().__new__(cls, *args, **kwargs)
    #     return cls._instance

    @dataclass
    class InitializationData:
        gauth: GoogleAuth = None
        drive: GoogleDrive = None
    _init_structs = None

    def __init__(self):
        self.logger = utils.tif_handler.logger
        self.tif_loader = utils.tif_handler

        if GenericDownloader._init_structs is None:
            GenericDownloader._init_structs = GenericDownloader.InitializationData()

        self.gauth = GenericDownloader._init_structs.gauth
        self.drive = GenericDownloader._init_structs.drive

    def download_gdrive_file(self, file_id, path):
        if GenericDownloader._init_structs.gauth is None:
            settings_path = Path(__file__).resolve().parents[1] / "pydrive_settings.yaml"
            GenericDownloader._init_structs.gauth = GoogleAuth(settings_file=str(settings_path))
            GenericDownloader._init_structs.drive = GoogleDrive(GenericDownloader._init_structs.gauth)
            self.gauth = GenericDownloader._init_structs.gauth
            self.drive = GenericDownloader._init_structs.drive

        file_obj = self.drive.CreateFile({'id': file_id})
        # Fetching title first to name the local file
        file_obj.FetchMetadata()
        self.logger.info(f"Downloading {file_obj['title']}...")
        file_obj.GetContentFile(path + file_obj['title'])
        return f"Finished {file_obj['title']}"


    @staticmethod
    def empty_folder(folder):
        shutil.rmtree(folder)
        os.mkdir(folder)

    def load_region(self):
        # Load the file - GeoPandas handles FeatureCollection vs Feature automatically
        gdf = gpd.read_file(cfg.BOUNDARY_GEOJSON_PATH)

        # Generalize to a single geometry (Unions everything if there are multiple features)
        # Helpful if the input itself is a vector of different mws.
        combined_geom = gdf.unary_union

        # Convert to Earth Engine Geometry
        # __geo_interface__ is a standard way to get GeoJSON-like dicts from objects
        geojson_struct = combined_geom.__geo_interface__
        region = ee.Geometry(geojson_struct)
        return region

    @staticmethod
    def save_from_gee(collection, region, tif_file_path):
        logger = utils.tif_handler.logger if utils.tif_handler else None
        os.makedirs(os.path.dirname(tif_file_path) or ".", exist_ok=True)

        try:
            # 1. Attempt the fast direct download
            if logger:
                logger.info(f"Requesting Earth Engine download URL for {tif_file_path}")

            url = collection.getDownloadURL({
                'format': 'GEO_TIFF',
                'scale': cfg.GEE_SCALE,
                'region': region
            })

            if logger:
                logger.info(f"Downloading {tif_file_path}")

            response = requests.get(url, timeout=(30, 900))

            # If Earth Engine says "Too Large", the status_code will not be 200
            if response.status_code == 200:
                with open(tif_file_path, 'wb') as f:
                    f.write(response.content)

                if logger:
                    logger.info(f"Downloaded {tif_file_path} with `getDownloadURL`")
                else:
                    print(f"Downloaded {tif_file_path} with `getDownloadURL`")
            else:
                raise ValueError(f"Image too large for direct URL (Status {response.status_code})")

        except Exception as e:
            if logger:
                logger.warning(
                    "Direct Earth Engine download failed for %s: %s. Falling back to tiled download.",
                    tif_file_path,
                    e,
                )
            else:
                print(f"Direct Earth Engine download failed for {tif_file_path}: {e}")
                print("Falling back to tiled download.")

            # getDownloadURL uses a single Earth Engine thumbnail request, which
            # fails for district-scale rasters over the 50 MB limit. geedim
            # splits the same image into smaller computePixels requests and
            # stitches them into one local GeoTIFF.
            prepared_image = collection.gd.prepareForExport(
                scale=cfg.GEE_SCALE,
                region=region,
                resampling="near",
            )
            prepared_image.gd.toGeoTIFF(
                tif_file_path,
                overwrite=True,
                max_tile_size=4,
                max_requests=2,
                max_cpus=1,
            )

            if logger:
                logger.info(f"Downloaded {tif_file_path} with tiled Earth Engine download")
            else:
                print(f"Downloaded {tif_file_path} with tiled Earth Engine download")

    def main(self):
        pass
