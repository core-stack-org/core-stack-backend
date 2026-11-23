"""Pan-India LULC v4 temporal-correction tasks for the heavy worker.

The API views pan-india/lulc_temporal_correction_six_year/ and
pan-india/lulc_temporal_correction_incremental/ queue them. The work is done by runner.py.
"""

from computing.drought.drought_pan_india import log_to_file
from nrm_app.celery import app

from .runner import DEFAULT_WORKERS, run_incremental, run_six_year

LOG_FILE = "lulc_temporal_correction.log"
LOGGER = "computing.lulc.v4.temporal_correction"


@app.task(bind=True)
def lulc_temporal_correction_six_year(self, input_dir, output_dir, start_year=None, workers=DEFAULT_WORKERS):
    """Correct six raw years as one window; the sixth stays provisional."""
    _ = self
    with log_to_file(LOG_FILE, LOGGER):
        return run_six_year(input_dir, output_dir, start_year=start_year, workers=workers)


@app.task(bind=True)
def lulc_temporal_correction_incremental(self, input_dir, output_dir, all_years=False, workers=DEFAULT_WORKERS):
    """Finalise the oldest provisional year (all_years: every year whose successor exists)."""
    _ = self
    with log_to_file(LOG_FILE, LOGGER):
        return run_incremental(input_dir, output_dir, all_years=all_years, workers=workers)
