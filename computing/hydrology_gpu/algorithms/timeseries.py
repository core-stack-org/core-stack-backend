import json
import os
import pathlib
from datetime import datetime
from pprint import pprint
from shapely.geometry import shape
from tqdm import tqdm
import cProfile
import pstats
from .runoff import Runoff
from . import GenericAlgorithm, GeoTIFFHandler
from typing import Type, Dict, Tuple, DefaultDict
from collections import defaultdict, OrderedDict
import cupy as cp
from .. import config as cfg

# Not generic enough yet...
class TimeSeries(GenericAlgorithm):
    def __init__(self,
                 algo: Type[GenericAlgorithm] = Runoff,
                 series_1="Rainfall",
                 series_2="Runoff",
                 *oth_args, **kwargs
                 ) -> None:
        super().__init__(*oth_args, **kwargs)
        self.algo = algo()
        self.series_1 = series_1
        self.series_2 = series_2

    def load_inputs(self):
        self.algo.load_inputs()
        # self.mws = self.tif_handler.rasterize_by_id(cfg.MICROWATERSHEDS_PATH)
        # self.mws_geojson = mws.Clip(self.args, self.logger, cfg).main()
        with open(cfg.MICROWATERSHEDS_PATH, 'r') as f:
            self.mws_geojson = json.load(f)

        shapes = []
        for fallback_id, feature in enumerate(self.mws_geojson['features'], start=1):
            properties = feature.setdefault('properties', {})
            try:
                feature_id = int(properties['id'])
            except (KeyError, TypeError, ValueError):
                feature_id = fallback_id
                properties['id'] = feature_id
            shapes.append((shape(feature['geometry']), feature_id))
        self.mws = self.tif_handler.rasterize_by_id(shapes)

    def main(self):
        mws_cp = cp.asarray(self.mws)
        mws_series_data1 = defaultdict(list)
        mws_series_data2 = defaultdict(list)

        def add_to_series(running_data, raster, name):
            avg, ids = per_watershed_avg(mws_cp, raster)
            raw_date = name
            dt = datetime.strptime(raw_date, "%Y%m%d_%H")
            for val, mws_id in zip(avg, ids):
                running_data[int(mws_id)].append((val, dt.isoformat()))

        # profiler = cProfile.Profile()
        # profiler.enable()
        for s1, s2, name in self.algo.main():
            add_to_series(mws_series_data1, s1, name)
            if s2 is not None:
                add_to_series(mws_series_data2, s2, name)

        # profiler.disable()
        #
        # stats = pstats.Stats(profiler)
        # stats.dump_stats("loop_profile.prof")
        #
        # self.logger.info("Profiling data saved to loop_profile.prof")

        self.mws_series_data1 = mws_series_data1
        self.mws_series_data2 = mws_series_data2

    def save_outputs(self):
        def make_output(id):
            ret = dict()    # apparently, dict is an OrderdDict
            for (val, name) in self.mws_series_data1[id]:
                ret[name] = {self.series_1: val}
            for (val, name) in self.mws_series_data2[id]:
                if name not in ret:
                    ret[name] = {self.series_2: val}
                else:
                    ret[name][self.series_2] = val
            return ret

        mws_data = self.mws_geojson

        for mws in tqdm(mws_data['features']):
            id = int(mws['properties']['id'])
            mws['properties']["timeseries"] = make_output(id)

        with open(cfg.TIMESERIES_VECTOR, 'w+') as f:
            json.dump(mws_data, f)

        self.logger.info(f"Saved file to {cfg.TIMESERIES_VECTOR}")



def per_watershed_avg(mws, raster):
    runoff_raster = cp.asarray(raster)
    watershed_raster = mws

    ws_flat = watershed_raster.ravel()
    rf_flat = runoff_raster.ravel()

    unique_ws, inv = cp.unique(ws_flat, return_inverse=True)

    per_ws_sum = cp.bincount(inv, weights=rf_flat, minlength=unique_ws.size)
    per_ws_count = cp.bincount(inv, minlength=unique_ws.size)

    per_ws_mean = per_ws_sum / per_ws_count

    # result = per_ws_mean[inv].reshape(watershed_raster.shape)

    return (per_ws_mean.get(), unique_ws.get())
    # return result
