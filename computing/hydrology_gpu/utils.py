import json
from pathlib import Path
from shapely.geometry import shape
import rasterio
from rasterio.coords import BoundingBox
from rasterio import features
from rasterio.windows import Window, bounds as window_bounds, from_bounds, transform as window_transform
import os
import logging
import numpy.typing as nptypes
import cupy as cp
import numpy as np
from tqdm import tqdm
from typing import Any, List
import xarray as xr
import rioxarray
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT

def load_tif_image(file_path) -> nptypes.NDArray[Any]:
    """Load a .tif image efficiently and return a NumPy array (float32)."""
    with rasterio.open(file_path) as src:
        image = src.read(1)  # Read only the first band
        print(f"Loaded Raster - Shape: {image.shape}, Dtype: {image.dtype}")
        return image  # Kept as NumPy array for easier slicing

def make_logger(file_name, gpu_mem_usage=False, level=logging.INFO):
    os.makedirs("logs", exist_ok=True)

    # Create logger
    logger = logging.getLogger("mylog")
    # without level, nothing gets print. I guess level is set to WARN etc
    logger.setLevel(level)

    # File handler
    fh = logging.FileHandler("logs/" + file_name)
    fh.setLevel(level)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

    if gpu_mem_usage:
        class GPUMemUsageAdapter(logging.LoggerAdapter):
            def __init__(self, logger, extra) -> None:
                super().__init__(logger, extra)
                # pynvml.nvmlInit()
                # Get handle for the first GPU (device 0)
                # Loop pynvml.nvmlDeviceGetCount() for multi-GPU setups
                # self.GPU_HANDLE = pynvml.nvmlDeviceGetHandleByIndex(0)
                self.mempool = cp.get_default_memory_pool()
                # logger.info(f"pynvml initialized {self.GPU_HANDLE}")

            def process(self, msg, kwargs):
                # mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.GPU_HANDLE)
                used_bytes = self.mempool.used_bytes()
                # cp.cuda.runtime.memGetInfo() returns (free, total) bytes
                free_device, total_device = cp.cuda.runtime.memGetInfo()
                gpu_mem_str = f" | CuPy Used: {used_bytes / (1024**2):.0f}/{total_device / (1024**2):.0f} MiB"
                return msg + gpu_mem_str, kwargs

        return GPUMemUsageAdapter(logger, {})

    return logger


class GeoTIFFHandler:
    """
        Saves tiff file with correct crs and transforms.
    """

    def __init__(self, tiff_path: str, logger: logging.Logger):
        """
        Initialize by loading an existing GeoTIFF file and storing its properties.
        """
        with rasterio.open(tiff_path) as src:
            self.crs = src.crs
            self.transform = src.transform
            self.width = src.width
            self.height = src.height
            self.dtype = src.dtypes[0]  # Get the data type of the first band
            self.count = src.count  # Number of bands
            self.window = self._window_from_bounds
            self.bounds = src.bounds

            # Read original data (optional)
            # self.original_data = src.read(1)

        self.logger = logger
        logger.info(f"Loaded TIFF: {tiff_path}")
        logger.info(f"CRS: {self.crs}, Transform: {self.transform}, Size: {self.width}x{self.height}, Type: {self.dtype}, Window: {self.window}")

    def _window_from_bounds(self, left, bottom, right, top):
        return from_bounds(left, bottom, right, top, transform=self.transform)

    def iter_windows(self, tile_size: int):
        if tile_size <= 0:
            raise ValueError("tile_size must be positive")

        for row_off in range(0, self.height, tile_size):
            height = min(tile_size, self.height - row_off)
            for col_off in range(0, self.width, tile_size):
                width = min(tile_size, self.width - col_off)
                yield Window(col_off, row_off, width, height)

    def for_window(self, window):
        window = self._clipped_window(window, self.width, self.height)
        if window is None:
            raise ValueError("Window does not overlap the parent raster")

        child = self.__class__.__new__(self.__class__)
        child.crs = self.crs
        child.transform = window_transform(window, self.transform)
        child.width = int(window.width)
        child.height = int(window.height)
        child.dtype = self.dtype
        child.count = self.count
        child.window = child._window_from_bounds
        child.bounds = BoundingBox(*window_bounds(window, self.transform))
        child.logger = self.logger
        return child

    @staticmethod
    def _raster_paths(src_path):
        path = Path(src_path)
        if path.is_dir():
            paths = sorted(
                child for child in path.rglob("*")
                if child.is_file() and child.suffix.lower() in {".tif", ".tiff"}
            )
            if not paths:
                raise FileNotFoundError(f"No GeoTIFF files found under {path}")
            return paths
        return [path]

    @staticmethod
    def _rounded_window(window):
        col_off = int(np.floor(window.col_off))
        row_off = int(np.floor(window.row_off))
        col_stop = int(np.ceil(window.col_off + window.width))
        row_stop = int(np.ceil(window.row_off + window.height))

        if col_stop <= col_off or row_stop <= row_off:
            return None

        return Window(col_off, row_off, col_stop - col_off, row_stop - row_off)

    @staticmethod
    def _clipped_window(window, width, height):
        window = GeoTIFFHandler._rounded_window(window)
        if window is None:
            return None

        col_off = max(0, int(window.col_off))
        row_off = max(0, int(window.row_off))
        col_stop = min(width, int(window.col_off + window.width))
        row_stop = min(height, int(window.row_off + window.height))

        if col_stop <= col_off or row_stop <= row_off:
            return None

        return Window(col_off, row_off, col_stop - col_off, row_stop - row_off)

    def _load_raster_path_into(self, src_path, padded, fill_value):
        with rasterio.open(src_path) as src:
            if src.crs != self.crs:
                raise ValueError(f"CRS mismatch for {src_path}; reproject first.")

            left = max(src.bounds.left, self.bounds.left)
            right = min(src.bounds.right, self.bounds.right)
            bottom = max(src.bounds.bottom, self.bounds.bottom)
            top = min(src.bounds.top, self.bounds.top)
            if left >= right or bottom >= top:
                return False

            with WarpedVRT(
                src,
                crs=self.crs,
                transform=self.transform,
                width=self.width,
                height=self.height,
                resampling=Resampling.nearest,
            ) as vrt:
                data = vrt.read(1, masked=True)

            mask = np.ma.getmaskarray(data)
            valid = ~mask
            if not np.any(valid):
                return True

            padded[valid] = data.filled(fill_value)[valid]
            return True

    def save_tiff(self, new_data, output_path: str):
            """
            Save a new TIFF file using the stored properties but with new data.

            :param new_data: 2D NumPy array containing new raster data.
            :param output_path: Path to save the new TIFF file.
            :param compression: Compression type for the TIFF file (default: "LZW").
            """
            if new_data.shape != (self.height, self.width):
                raise ValueError(f"Data shape {new_data.shape} does not match the stored shape ({self.height}, {self.width})")

            # output_dir = os.path.dirname(output_path)
            # if output_dir:
            #     os.makedirs(output_dir, exist_ok=True)

            gdal_options = {
                    'tiled': True,
                    'compress': 'ZSTD',      # <--- Use Zstandard
                    # 'ZSTD_LEVEL': 6,         # <--- Set to lowest level (1=fastest, 22=best)
                    'ZSTD_LEVEL': 1,         # <--- Set to lowest level (1=fastest, 22=best)
                    'NUM_THREADS': 10        # <--- Essential for speed. we have 12 cores.
                }

            with rasterio.open(
                output_path,
                "w",
                driver="GTiff",
                height=self.height,
                width=self.width,
                count=self.count,
                dtype=new_data.dtype,
                crs=self.crs,
                transform=self.transform,
                # compress=compression
                **gdal_options
            ) as dst:
                dst.write(new_data, 1)  # Write new data to band 1

            self.logger.info(f"Saved new TIFF to {output_path}")

    def save_geozarr(self, new_data: np.ndarray, output_path: str, name:str="data"):
        if new_data.shape != (self.height, self.width):
            raise ValueError(f"Data shape {new_data.shape} does not match shape ({self.height}, {self.width})")

        # 1. Generate coordinates from transform (Top-Left + half-pixel offset)
        x_coords = self.transform.c + (np.arange(self.width) + 0.5) * self.transform.a
        y_coords = self.transform.f + (np.arange(self.height) + 0.5) * self.transform.e

        # 2. Wrap in Xarray
        da = xr.DataArray(
            new_data,
            dims=("y", "x"),
            coords={
                "y": y_coords,
                "x": x_coords
            },
            name=name
        )

        # 3. Attach CRS (Essential for GIS)
        da.rio.write_crs(self.crs, inplace=True)

        # 4. Save to Zarr (Creates a directory at output_path)
        da.to_dataset().to_zarr(output_path, mode="w", zarr_format=2)

    def save_geozarr_time(self, new_data: np.ndarray, time, output_path: str, name:str="data"):
        if new_data.shape != (self.height, self.width):
            raise ValueError(f"Data shape {new_data.shape} does not match shape ({self.height}, {self.width})")

        # 1. Generate coordinates from transform (Top-Left + half-pixel offset)
        x_coords = self.transform.c + (np.arange(self.width) + 0.5) * self.transform.a
        y_coords = self.transform.f + (np.arange(self.height) + 0.5) * self.transform.e

        # 2. Wrap in Xarray
        da = xr.DataArray(
            [new_data],
            dims=("time", "y", "x"),
            coords={
                "time": [time],
                "y": y_coords,
                "x": x_coords
            },
            name=name
        )

        # 3. Attach CRS (Essential for GIS)
        da.rio.write_crs(self.crs, inplace=True)

        # 4. Save to Zarr (Creates a directory at output_path)
        if not os.path.exists(output_path):
            da.to_dataset().to_zarr(output_path, mode="w", zarr_format=2)
        else:
            da.to_dataset().to_zarr(output_path, mode="a", zarr_format=2, append_dim="time")

    def save_multiband_tiff(self, output_path:str, data_arrays: list[nptypes.NDArray[Any]], compression="LZW"):
        """
        Save multiple 2D NumPy arrays as bands in a single GeoTIFF.
        data_arrays: list or tuple of 2D numpy arrays with identical shape.
        """
        count = len(data_arrays)
        dtype = data_arrays[0].dtype

        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            height=self.height,
            width=self.width,
            count=count,
            dtype=dtype,
            crs=self.crs,
            transform=self.transform,
            compress=compression
        ) as dst:
            for i, arr in tqdm(enumerate(data_arrays, start=1)):
                dst.write(arr, i)

        self.logger.info("Done writing to " + output_path)

    def load_with_padding_inner(self, crs, data: np.ndarray, src_bounds, fill_value=0):
        if crs != self.crs:
            raise ValueError("CRS mismatch")

        # Use your existing window logic with the bounds from the dict
        ref_window = self.window(*src_bounds)
        row_off, col_off = int(ref_window.row_off), int(ref_window.col_off)

        padded = np.full((self.height, self.width), fill_value, dtype=data.dtype)

        height, width = data.shape

        dest_row0 = max(0, row_off)
        dest_col0 = max(0, col_off)
        dest_row1 = min(self.height, dest_row0 + height)
        dest_col1 = min(self.width, dest_col0 + width)

        # Paste source data if overlapping
        src_row0 = max(0, -row_off)
        src_col0 = max(0, -col_off)
        src_row1 = src_row0 + (dest_row1 - dest_row0)
        src_col1 = src_col0 + (dest_col1 - dest_col0)

        padded[dest_row0:dest_row1, dest_col0:dest_col1] = data[src_row0:src_row1, src_col0:src_col1]

        return padded

    def load_with_padding(self, src_path, fill_value=0):
        paths = self._raster_paths(src_path)

        with rasterio.open(paths[0]) as first:
            padded = np.full((self.height, self.width), fill_value, dtype=first.dtypes[0])

        loaded = 0
        for path in paths:
            if self._load_raster_path_into(path, padded, fill_value):
                loaded += 1

        if loaded == 0:
            raise ValueError(f"No raster data from {src_path} overlaps the reference grid")

        self.logger.info("Loaded %s overlapping raster file(s) from %s", loaded, src_path)
        return padded


    def rasterize_by_id(self, shapes, fill_value=0):
        """
        Rasterizes GeoJSON features using their 'id' property as the pixel value.
        Not generic enough, i think.
        """
        # with open(geojson_path, 'r') as f:
        #     geojson_data = json.load(f)

        # 1. Extract (geometry, value) pairs
        # This creates a list like: [(geom1, 817103), (geom2, 818258), ...]

        # shapes = [
        #     (shape(feature['geometry']), feature['properties']['id'])
        #     for feature in geojson_data['features']
        # ]

        # 2. Rasterize
        # Note: Use a dtype large enough for your IDs (e.g., int32 or float32)
        mask = rasterio.features.rasterize(
            shapes=shapes,
            out_shape=(self.height, self.width),
            transform=self.transform,
            fill=fill_value,
            dtype='int32'
        )

        return mask


tif_handler: GeoTIFFHandler = None
