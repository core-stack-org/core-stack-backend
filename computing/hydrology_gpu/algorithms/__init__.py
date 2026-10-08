from typing import Dict
from time import perf_counter
from .. import config as cfg
from .. import utils

from ..utils import GeoTIFFHandler


def format_elapsed(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.2f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"


class GenericAlgorithm:
    def __init__(self) -> None:
        tif_handler = utils.tif_handler
        self.tif_handler = tif_handler
        self.logger = tif_handler.logger

    def load_inputs(self):
        pass

    def main(self):
        pass

    def save_outputs(self):
        pass

    def run_timed(self, name, fn):
        start_time = perf_counter()
        self.logger.info("Starting %s", name)
        try:
            result = fn()
        except Exception:
            self.logger.exception("Failed %s after %s", name, format_elapsed(perf_counter() - start_time))
            raise
        else:
            self.logger.info("Finished %s in %s", name, format_elapsed(perf_counter() - start_time))
            return result

    def run(self):
        self.run_timed("loading inputs", self.load_inputs)
        self.run_timed("main algorithm", self.main)
        self.run_timed("saving outputs", self.save_outputs)
