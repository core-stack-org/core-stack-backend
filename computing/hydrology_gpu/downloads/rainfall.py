import gc
import json
import os
import pathlib
import shutil
import threading
import time
from argparse import ArgumentParser
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from .. import config as cfg
import xarray as xr
import requests
import concurrent.futures

import geopandas as gpd
import zarr
from rasterio.enums import Resampling
from shapely.geometry import box
from rasterio.transform import Affine, from_origin
from rasterio.warp import reproject
from tqdm import tqdm
# import geedim as gd
import cupy as cp
import numpy as np
import pandas as pd
import cucim.skimage.transform as cimg

from . import GenericDownloader, ee, Logger

class DownloaderBase(GenericDownloader):
    def __init__(self):

        super().__init__()
        self.zarr_path = os.path.join(cfg.RAINFALL_FOLDER, "rainfall_archive.zarr")
        pathlib.Path(self.zarr_path).parent.mkdir(parents=True, exist_ok=True)

    def load_region_gdf(self):
        gdf = gpd.read_file(cfg.BOUNDARY_GEOJSON_PATH)
        if gdf.empty:
            raise ValueError(f"No geometries found in {cfg.BOUNDARY_GEOJSON_PATH}")

        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf

    def load_local_region_geometry(self):
        gdf = self.load_region_gdf()
        if hasattr(gdf.geometry, "union_all"):
            return gdf.geometry.union_all()
        return gdf.unary_union

    def load_buffered_bounds_geometry(self, buffer_m=12000):
        """
        Build a small EE rectangle from local bounds instead of sending the full
        boundary polygon to Earth Engine. Large pan-India polygons can exceed
        EE's 10 MB request payload limit.
        """
        gdf = self.load_region_gdf()
        metric_gdf = gdf.to_crs("EPSG:3857")
        minx, miny, maxx, maxy = metric_gdf.geometry.buffer(buffer_m).total_bounds
        bounds_geom = gpd.GeoSeries([box(minx, miny, maxx, maxy)], crs="EPSG:3857").to_crs("EPSG:4326")
        west, south, east, north = [float(value) for value in bounds_geom.total_bounds]
        self.logger.info("Using local buffered boundary bounds: [%s, %s, %s, %s]", west, south, east, north)
        return ee.Geometry.Rectangle([west, south, east, north], proj="EPSG:4326", geodesic=False)

    def init_zarr(self, dates, dummy_da):
        """
        Initializes a Zarr store with the full time extent to allow parallel region writes.
        """
        native_y, native_x = dummy_da.y.size, dummy_da.x.size

        # Create the skeleton Dataset
        # We use empty/zeros but with compute=False, so no data is actually written yet
        ds_skeleton = xr.Dataset(
            {"precipitation": (["time", "y", "x"],
                               np.zeros((len(dates), native_y, native_x), dtype='float32'))},
            coords={
                "time": dates,
                "y": dummy_da.y.values,
                "x": dummy_da.x.values
            }
        )

        # Metadata/CRS (Important for GeoZarr)
        ds_skeleton.rio.write_crs(dummy_da.rio.crs, inplace=True)
        ds_skeleton.rio.write_transform(dummy_da.rio.transform(), inplace=True)

        # Encoding: chunking by 1 day is standard for daily time-series
        encoding = {
            "precipitation": {"chunks": (1, native_y, native_x)},
            "time": {
                "units": "hours since 2017-07-01 00:00:00",
                "calendar": "proleptic_gregorian",
                "dtype": "int64"  # Ensuring integer storage for hours
            }
        }

        # Write ONLY metadata
        ds_skeleton.to_zarr(self.zarr_path, mode='w', encoding=encoding, compute=False,  zarr_format=2)
        self.logger.info(f"Initialized Zarr skeleton at {self.zarr_path} with {len(dates)} slots.")


    def save_geozarr(self, flat_data, timestamp, dummy_da, t):
        """
        Saves data as a 3D (Time, Y, X) chunked array with full spatial coordinates.
        """

        # 1. Reshape flat data to native grid dimensions
        # Using the shape from self.dummy_da (e.g., [Lat, Lon])
        native_y, native_x = dummy_da.y.size, dummy_da.x.size
        raster_data = flat_data.reshape(native_y, native_x)

        # 2. Create the DataArray with proper spatial coords
        da = xr.DataArray(
            raster_data[np.newaxis, ...],  # Shape: (1, Y, X)
            dims=("time", "y", "x"),
            coords={
                "time": [pd.to_datetime(timestamp, format='%Y%m%d_%H')],
                "y": dummy_da.y.values,
                "x": dummy_da.x.values
            },
            name="precipitation"
        )

        # 3. Add CRS and metadata
        da.rio.write_crs(dummy_da.rio.crs, inplace=True)
        da.rio.write_transform(dummy_da.rio.transform(), inplace=True)

        ds = da.to_dataset().drop_vars(["y", "x", "spatial_ref"])

        ds.to_zarr(self.zarr_path, region={"time": slice(t, t + 1)})

    def load_geozarr(self):
        """
        Loads the Zarr archive as a standard Xarray Dataset.
        """

        if not os.path.exists(self.zarr_path):
            self.logger.error(f"Zarr not found at {self.zarr_path}")
            return None

        # chunks={} opens it lazily using Dask
        ds = xr.open_zarr(self.zarr_path, consolidated=True, chunks={})
        return ds

class Download_to_database(DownloaderBase):
    def main(self):
        self.ingest_rainfall_to_zarr()

    def ingest_rainfall_to_zarr(self):
        """
        Part 1: Purely fetches data, sums it on GPU, and saves to GeoZarr.
        """
        self.logger.info("Starting rainfall ingestion to GeoZarr")
        self.logger.info("Loading boundary bounds")
        buffered_region = self.load_buffered_bounds_geometry()

        self.logger.info(
            "Preparing GSMaP rainfall collection for [%s, %s)",
            cfg.ARG_START_DATE,
            cfg.ARG_END_DATE,
        )
        rainfall_collection = (
            ee.ImageCollection('JAXA/GPM_L3/GSMaP/v6/operational')
            .filterDate(cfg.ARG_START_DATE, cfg.ARG_END_DATE)
            .select('hourlyPrecipRate')
        )

        # rainfall_collection = (
        #     ee.ImageCollection("NASA/GPM_L3/IMERG_DAILY_V06")
        #     .filterDate(cfg.ARG_START_DATE, cfg.ARG_END_DATE)
        #     .select('total_accum')
        # )
        #
        # # 1. Define the time range
        # start_date = ee.Date(cfg.ARG_START_DATE)
        # end_date = ee.Date(cfg.ARG_END_DATE)
        #
        # # 2. Calculate the number of days between start and end
        # n_days = end_date.difference(start_date, 'days')
        #
        # def sum_daily(day_offset):
        #     # Calculate the start and end of each 24-hour window
        #     start = start_date.advance(ee.Number(day_offset), 'days')
        #     end = start.advance(1, 'days')
        #
        #     # Filter the collection for this specific day and sum
        #     daily_sum = (rainfall_collection
        #                  .filterDate(start, end)
        #                  .sum())  # Sums the 'hourlyPrecipRate'
        #
        #     # Return the image with its date metadata (important for further filtering)
        #     return daily_sum.set({
        #         'system:time_start': start.millis(),
        #         'date_string': start.format('YYYY-MM-DD')
        #     })
        #
        # # 3. Create a sequence of days and map the function
        # daily_collection = ee.ImageCollection(
        #     ee.List.sequence(0, n_days.subtract(1)).map(sum_daily)
        # )

        first_img = rainfall_collection.first()
        self.logger.info("Fetching GSMaP native projection")
        native_proj = first_img.projection()

        self.logger.info("Opening Earth Engine dataset through xarray/xee; this can take a few minutes for pan-India yearly ranges")
        ds = xr.open_dataset(
            rainfall_collection,
            engine='ee',
            projection=native_proj,
            geometry=buffered_region,
            fast_time_slicing=True,
        )
        self.logger.info("Earth Engine dataset opened; preparing rainfall coordinates")

        da = ds['hourlyPrecipRate'].rename({'lat': 'y', 'lon': 'x'}).transpose("time", "y", "x")
        total_pixels = da.y.size * da.x.size
        dummy_da = da.isel(time=0)

        N = len(da.time)
        K = 24  # Hours per day
        self.logger.info(
            "Rainfall grid has %s hourly slices and %sx%s native pixels",
            N,
            da.y.size,
            da.x.size,
        )

        dates = pd.date_range(start=cfg.ARG_START_DATE, end=cfg.ARG_END_DATE, freq='D', inclusive='left')
        self.init_zarr(dates, dummy_da)

        ASK_BUFF = 50
        WORKERS = 20

        def process_slice(t, ticket_num):
            da_slice = da.isel(time=slice(t, min(t + K, N)))

            # Extract raw data and sum on GPU
            raw_data = da_slice.values
            gpu_sum = cp.zeros(total_pixels, dtype=cp.float32)

            for i in range(raw_data.shape[0]):
                gpu_sum += cp.asarray(raw_data[i]).ravel()

            timestamp = da_slice.time[0].dt.strftime('%Y%m%d_%H').item()

            # Save the flat sum to the database
            # Assuming self.save_geozarr handles time-indexing inside the Zarr
            self.save_geozarr(gpu_sum.get(), timestamp, dummy_da, ticket_num)

        with (
            concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor,
            tqdm(range(N), desc="Downloading Rainfall") as pbar
        ):
            futures = []

            while pbar.n < pbar.total:
                if len(futures) == 0:
                    gc.collect()
                    t = pbar.n
                    assert t % K == 0
                    for i in range(ASK_BUFF):
                        current_hour = t+i*K
                        day_index = current_hour // K
                        if current_hour >= N:
                            break
                        futures.append(executor.submit(
                            process_slice,
                            current_hour,
                            day_index
                        ))
                futures.pop().result()
                pbar.update(K)

        zarr.consolidate_metadata(self.zarr_path)
        self.logger.info("Ingestion complete.")


def transform_from_center_coords(x_values, y_values):
    if len(x_values) < 2 or len(y_values) < 2:
        raise ValueError("Rainfall archive must have at least two x and y coordinates")

    dx = float(np.median(np.diff(x_values)))
    dy = float(np.median(np.diff(y_values)))
    return Affine.translation(float(x_values[0]) - dx / 2, float(y_values[0]) - dy / 2) * Affine.scale(dx, dy)


def build_reference_index_map(source_shape, original_size, src_transform, src_crs, tif_handler):
    source_indices = np.arange(original_size, dtype=np.int32).reshape(source_shape)
    mapped_indices = np.full(
        (tif_handler.height, tif_handler.width),
        original_size,
        dtype=np.int32,
    )

    reproject(
        source=source_indices,
        destination=mapped_indices,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=tif_handler.transform,
        dst_crs=tif_handler.crs,
        dst_nodata=original_size,
        resampling=Resampling.nearest,
    )
    return mapped_indices


class Load_from_database(DownloaderBase):

    def __init__(self):
        super().__init__()
        self.tif_handler = self.tif_loader
        self.ds = self.load_geozarr()
        if self.ds is None:
            raise FileNotFoundError(f"Rainfall archive not found at {self.zarr_path}")

        self.src_transform = transform_from_center_coords(
            self.ds.x.values,
            self.ds.y.values,
        )
        self.src_crs = self.ds.rio.crs or "EPSG:4326"
        self.source_shape = (self.ds.sizes["y"], self.ds.sizes["x"])
        self.original_size = self.source_shape[0] * self.source_shape[1]
        self.GPU_LUT = cp.asarray(build_reference_index_map(
            self.source_shape,
            self.original_size,
            self.src_transform,
            self.src_crs,
            self.tif_handler,
        ))

        self.STATIC_METADATA = {
            "bounds": self.tif_handler.bounds,
            "transform": self.tif_handler.transform,
            "crs": self.tif_handler.crs,
            "shape": (self.tif_handler.height, self.tif_handler.width),
            "original_size": self.original_size,
        }

    def main(self):
        self.logger.info("Starting rainfall load from GeoZarr onto reference grid")

        yield from self.stream_reprojected_rainfall()

    def stream_reprojected_rainfall(self):
        for i in tqdm(range(len(self.ds.time))):
            # Extract 2D slice and flatten for the GPU LUT
            hourly_slice = self.ds.precipitation.isel(time=i)
            flat_cpu = hourly_slice.values.ravel() 
            
            # Move to GPU
            gpu_src = cp.asarray(flat_cpu)
            
            # Append sink pixel for nodata and apply LUT
            projected_buffer = cp.concatenate([gpu_src, cp.array([0], dtype=gpu_src.dtype)])
            gpu_final = projected_buffer[self.GPU_LUT]

            yield {
                "timestamp": hourly_slice.time.dt.strftime('%Y%m%d_%H').item(),
                "data": gpu_final.get(),
                "bounds": self.STATIC_METADATA["bounds"],
                "transform": self.STATIC_METADATA["transform"],
                "crs": self.STATIC_METADATA["crs"]
            }


class LoadTile_from_database(DownloaderBase):
    def __init__(self, tif_handler):
        super().__init__()
        self.tif_handler = tif_handler
        self.ds = self.load_geozarr()
        if self.ds is None:
            raise FileNotFoundError(f"Rainfall archive not found at {self.zarr_path}")

        self.src_transform = transform_from_center_coords(
            self.ds.x.values,
            self.ds.y.values,
        )
        self.src_crs = self.ds.rio.crs or "EPSG:4326"
        self.source_shape = (self.ds.sizes["y"], self.ds.sizes["x"])
        self.original_size = self.source_shape[0] * self.source_shape[1]
        self.GPU_LUT = cp.asarray(build_reference_index_map(
            self.source_shape,
            self.original_size,
            self.src_transform,
            self.src_crs,
            self.tif_handler,
        ))

    def main(self):
        self.logger.info("Starting tiled rainfall load from GeoZarr")
        yield from self.stream_reprojected_rainfall()

    def stream_reprojected_rainfall(self):
        for i in tqdm(range(len(self.ds.time)), desc="Projecting rainfall tile"):
            hourly_slice = self.ds.precipitation.isel(time=i)
            flat_cpu = hourly_slice.values.ravel()
            gpu_src = cp.asarray(flat_cpu)
            projected_buffer = cp.concatenate([gpu_src, cp.array([0], dtype=gpu_src.dtype)])
            gpu_final = projected_buffer[self.GPU_LUT]
            data = gpu_final.get()
            del gpu_src, projected_buffer, gpu_final

            yield {
                "timestamp": hourly_slice.time.dt.strftime('%Y%m%d_%H').item(),
                "data": data,
                "bounds": self.tif_handler.bounds,
                "transform": self.tif_handler.transform,
                "crs": self.tif_handler.crs,
            }
