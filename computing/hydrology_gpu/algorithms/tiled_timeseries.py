import csv
import gc
import json
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import cupy as cp
import numpy as np
from shapely.geometry import shape
from tqdm import tqdm

from .. import config as cfg
from ..downloads import rainfall
from . import GenericAlgorithm
from .runoff import Runoff, LegacyCodes


def per_watershed_sum_count(mws, raster):
    watershed = mws.ravel().astype(cp.int32)
    values = cp.asarray(raster, dtype=cp.float32).ravel()

    mask = watershed > 0
    if int(cp.count_nonzero(mask).get()) == 0:
        return [], [], []

    watershed = watershed[mask]
    values = values[mask]
    finite = cp.isfinite(values)
    if int(cp.count_nonzero(finite).get()) == 0:
        return [], [], []

    watershed = watershed[finite]
    values = values[finite]

    sums = cp.bincount(watershed, weights=values)
    counts = cp.bincount(watershed)
    ids = cp.nonzero(counts)[0]

    return ids.get(), sums[ids].get(), counts[ids].get()


class TiledTimeSeries(GenericAlgorithm):
    def __init__(
        self,
        tile_size: int,
        series_1="Rainfall",
        series_2="Runoff",
        *oth_args,
        **kwargs,
    ) -> None:
        super().__init__(*oth_args, **kwargs)
        self.tile_size = tile_size
        self.series_1 = series_1
        self.series_2 = series_2

    def load_inputs(self):
        with open(cfg.MICROWATERSHEDS_PATH, "r") as f:
            self.mws_geojson = json.load(f)

        self.shape_records = []
        for fallback_id, feature in enumerate(self.mws_geojson["features"], start=1):
            properties = feature.setdefault("properties", {})
            try:
                feature_id = int(properties["id"])
            except (KeyError, TypeError, ValueError):
                feature_id = fallback_id
                properties["id"] = feature_id
            properties.pop("timeseries", None)
            geometry = shape(feature["geometry"])
            self.shape_records.append((geometry, feature_id, geometry.bounds))

    def tile_shapes(self, tile_bounds):
        left, bottom, right, top = tile_bounds
        return [
            (geometry, feature_id)
            for geometry, feature_id, bounds in self.shape_records
            if bounds[0] < right and bounds[2] > left and bounds[1] < top and bounds[3] > bottom
        ]

    @staticmethod
    def _empty_series_entry():
        return [0.0, 0]

    def _series_path(self, watershed_id):
        return self.series_dir / f"{watershed_id // 1000:04d}" / f"{watershed_id}.csv"

    def write_tile_series(self, tile_index, data1, data2):
        watershed_ids = sorted(set(data1) | set(data2))
        if not watershed_ids:
            return
        for watershed_id in watershed_ids:
            path = self._series_path(watershed_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", newline="") as f:
                writer = csv.writer(f)
                timestamps = set(data1[watershed_id]) | set(data2[watershed_id])
                for timestamp in sorted(timestamps):
                    rainfall_sum, rainfall_count = data1[watershed_id].get(timestamp, (0.0, 0))
                    runoff_sum, runoff_count = data2[watershed_id].get(timestamp, (0.0, 0))
                    writer.writerow((timestamp, rainfall_sum, rainfall_count, runoff_sum, runoff_count))
        self.logger.info("Stored tile %s series for %s watershed(s) in %s", tile_index, len(watershed_ids), self.series_dir)

    def add_to_series(self, running_data, watershed_raster, raster, name):
        raw_date = name
        dt = datetime.strptime(raw_date, "%Y%m%d_%H").isoformat()
        ids, sums, counts = per_watershed_sum_count(watershed_raster, raster)
        for watershed_id, value_sum, value_count in zip(ids, sums, counts):
            entry = running_data[int(watershed_id)][dt]
            entry[0] += float(value_sum)
            entry[1] += int(value_count)

    def process_tile(self, tile_handler, tile_index, tile_count):
        shapes = self.tile_shapes(tile_handler.bounds)
        if not shapes:
            self.logger.info("Skipping tile %s/%s with no intersecting watershed geometries", tile_index, tile_count)
            return

        watershed_raster = tile_handler.rasterize_by_id(shapes)
        if not np.any(watershed_raster > 0):
            self.logger.info("Skipping tile %s/%s with no watershed pixels", tile_index, tile_count)
            return

        self.logger.info(
            "Processing tile %s/%s: size=%sx%s bounds=%s",
            tile_index,
            tile_count,
            tile_handler.width,
            tile_handler.height,
            tile_handler.bounds,
        )

        watershed_cp = cp.asarray(watershed_raster)
        tile_series_data1 = defaultdict(lambda: defaultdict(self._empty_series_entry))
        tile_series_data2 = defaultdict(lambda: defaultdict(self._empty_series_entry))
        runoff_algo = Runoff()
        runoff_algo.tif_handler = tile_handler

        soil, raw_lulc, slope = runoff_algo.load_sr_inputs()
        current_sr_key = None
        sr1 = sr2 = sr3 = None

        images = []
        p5_sum = None
        rainfall_iter = rainfall.LoadTile_from_database(tile_handler)

        for index, file in enumerate(rainfall_iter.main()):
            img = file["data"]
            np.nan_to_num(img, copy=False)
            self.add_to_series(tile_series_data1, watershed_cp, img, file["timestamp"])

            images.append(img)
            if index == 4:
                p5_sum = cp.zeros_like(cp.asarray(images[0], dtype=cp.float32))
                for previous_img in images[:5]:
                    p5_sum = p5_sum + cp.asarray(previous_img, dtype=cp.float32)
            elif index >= 5:
                new_img = cp.asarray(images[-1], dtype=cp.float32)
                old_img = cp.asarray(images[0], dtype=cp.float32)
                p5_sum = p5_sum - old_img + new_img
                images.pop(0)
                del new_img, old_img

            if index >= 4:
                p_sum = cp.asarray(images[-1], dtype=cp.float32)
                sr_key = runoff_algo.sr_cache_key(file["timestamp"])
                if sr_key != current_sr_key:
                    if sr1 is not None:
                        del sr1, sr2, sr3
                    sr1, sr2, sr3 = runoff_algo.compute_sr_for_timestamp(
                        soil,
                        raw_lulc,
                        slope,
                        file["timestamp"],
                    )
                    current_sr_key = sr_key
                    self.logger.info("Loaded SR for LULC key %s", sr_key)
                m1_cp = LegacyCodes.compute_M(sr1, p5_sum)
                m2_cp = LegacyCodes.compute_M(sr2, p5_sum)
                m3_cp = LegacyCodes.compute_M(sr3, p5_sum)
                runoff = LegacyCodes.calculate_runoff_cupy(p_sum, p5_sum, m1_cp, m2_cp, m3_cp, sr1, sr2, sr3)
                self.add_to_series(tile_series_data2, watershed_cp, runoff, file["timestamp"])
                del p_sum, m1_cp, m2_cp, m3_cp, runoff

        self.write_tile_series(tile_index, tile_series_data1, tile_series_data2)
        if sr1 is not None:
            del sr1, sr2, sr3
        del watershed_cp, soil, raw_lulc, slope, rainfall_iter, images, p5_sum, tile_series_data1, tile_series_data2
        cp.get_default_memory_pool().free_all_blocks()
        gc.collect()

    def main(self):
        output_path = Path(cfg.TIMESERIES_VECTOR)
        self.series_dir = output_path.parent / f"{output_path.stem}_tile_series"
        shutil.rmtree(self.series_dir, ignore_errors=True)
        self.series_dir.mkdir(parents=True, exist_ok=True)

        windows = list(self.tif_handler.iter_windows(self.tile_size))
        self.logger.info("Processing %s tile(s) with tile_size=%s", len(windows), self.tile_size)
        for tile_index, window in enumerate(tqdm(windows, desc="Processing spatial tiles"), start=1):
            tile_handler = self.tif_handler.for_window(window)
            self.process_tile(tile_handler, tile_index, len(windows))

    def save_outputs(self):
        def make_output(watershed_id):
            path = self._series_path(watershed_id)
            if not path.exists():
                return {}
            data = defaultdict(lambda: [0.0, 0, 0.0, 0])
            with path.open(newline="") as f:
                for timestamp, rainfall_sum, rainfall_count, runoff_sum, runoff_count in csv.reader(f):
                    entry = data[timestamp]
                    entry[0] += float(rainfall_sum)
                    entry[1] += int(rainfall_count)
                    entry[2] += float(runoff_sum)
                    entry[3] += int(runoff_count)

            ret = {}
            for timestamp in sorted(data):
                values = {}
                rainfall_sum, rainfall_count, runoff_sum, runoff_count = data[timestamp]
                if rainfall_count:
                    values[self.series_1] = rainfall_sum / rainfall_count
                if runoff_count:
                    values[self.series_2] = runoff_sum / runoff_count
                if values:
                    ret[timestamp] = values
            return ret

        output_path = Path(cfg.TIMESERIES_VECTOR)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as f:
            f.write("{")
            first_key = True
            for key, value in self.mws_geojson.items():
                if key == "features":
                    continue
                if not first_key:
                    f.write(",")
                json.dump(key, f)
                f.write(":")
                json.dump(value, f, separators=(",", ":"))
                first_key = False

            if not first_key:
                f.write(",")
            f.write('"features":[')
            first_feature = True
            for watershed in tqdm(self.mws_geojson["features"], desc="Writing tiled timeseries"):
                watershed_id = int(watershed["properties"]["id"])
                watershed["properties"]["timeseries"] = make_output(watershed_id)
                if not first_feature:
                    f.write(",")
                json.dump(watershed, f, separators=(",", ":"))
                watershed["properties"].pop("timeseries", None)
                first_feature = False
            f.write("]}")

        self.logger.info("Saved file to %s", cfg.TIMESERIES_VECTOR)
