"""Pan-India drought tasks for the heavy worker: download the inputs, and make the pan-India layers.

The API views pan-india/drought_inputs/ and pan-india/drought/ queue them. The work is done by
computing/drought/local/download.py and pipeline.py, which the command-line scripts in
utilities/scripts/ also run. Both tasks continue where an earlier run stopped, so a task
cut short (a worker restart) is finished by calling the API again.
"""

import logging
from contextlib import contextmanager

from computing.config_loader import PROJECT_ROOT
from computing.drought.local.download import run_download
from computing.drought.local.pipeline import run_pipeline
from nrm_app.celery import app

LOG_DIR = PROJECT_ROOT / "logs"


@contextmanager
def log_to_file(filename, *logger_names):
    """Copy the named loggers' messages to logs/<filename> while the task runs."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(LOG_DIR / filename)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                                           datefmt="%Y-%m-%d %H:%M:%S"))
    loggers = [logging.getLogger(name) for name in logger_names]
    levels = [lg.level for lg in loggers]
    for lg in loggers:
        lg.addHandler(handler)
        lg.setLevel(logging.INFO)
    try:
        yield
    finally:
        for lg, level in zip(loggers, levels):
            lg.removeHandler(handler)
            lg.setLevel(level)
        handler.close()


@app.task(bind=True)
def download_drought_inputs(self, products=None, end_year=None, dry_run=False):
    """CHIRPS, MOD09A1 and MOD16A2GF up to end_year (default: this year); only what is missing."""
    _ = self
    with log_to_file("drought_inputs_download.log", "drought_inputs"):
        return run_download(products=products, end_year=end_year, dry_run=dry_run)


@app.task(bind=True)
def generate_drought_pan_india(self, seasons=None, stages=None, rerun_stages=None):
    """The pan-India 500 m and MWS drought layers for the seasons whose inputs are on disk."""
    _ = self
    with log_to_file("drought_layer.log", "computing.drought.local"):
        return run_pipeline(seasons=seasons, stages=stages, rerun_stages=rerun_stages)
