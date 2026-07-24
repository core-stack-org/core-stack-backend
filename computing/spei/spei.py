from computing.spei.drought_sensitivity.drought_resistance_resilience import (
    generate_drought_resistance,
)
from computing.spei.forestfire_sensitivity.export_fire_index import fire_index
from computing.spei.forestfire_sensitivity.forest_fire_resistance_resilience import (
    forest_fire_sensitivity,
)
from computing.spei.generate_spei.download_base_datasets import download_data_locally
from computing.spei.generate_spei.generate_ppet_multiband import ppet_multiband
from computing.spei.generate_spei.spei_runner import run_spei
from computing.spei.high_wind_sensitivity.export_max_wind_index import max_wind_index
from computing.spei.high_wind_sensitivity.highwind_resistance_resilience import (
    high_wind_sensitivity,
)
from computing.spei.hybrid_tree_mask import generate_hybrid_tree_mask
from computing.spei.rainfall_sensitivity.export_rainfall_index import rainfall_index
from computing.spei.rainfall_sensitivity.rainfall_resistance_resilience import (
    generate_rainfall_resilience,
)
from utilities.gee_utils import ee_initialize, check_task_status, is_gee_asset_exists
from nrm_app.celery import app


@app.task(bind=True)
def generate_spei_pipeline(
    self,
    aez,
    start_year,
    end_year,
    gee_account_id=None,
    overwrite=False,
):
    ee_initialize(gee_account_id)
    start_date = f"{str(start_year)}-01-01"
    end_date = f"{str(end_year)}-12-31"

    download_data_locally(
        aez=aez,
        start_date=start_date,
        end_date=end_date,
        frequency="monthly",
        datasets=None,
        overwrite=overwrite,
    )

    ppet_multiband(
        aez=aez,
        start=start_year,
        end=end_year,
    )

    run_spei(aez, start_year, end_year)


@app.task(bind=True)
def run_drought_resistance_resilience(
    self, aez, start_year=None, end_year=None, gee_account_id=None
):
    task_id, asset_id = generate_hybrid_tree_mask(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    if is_gee_asset_exists(asset_id):
        generate_drought_resistance(
            aez, start_year=2004, end_year=end_year, gee_account_id=gee_account_id
        )


@app.task(bind=True)
def run_rainfall_resistance_resilience(
    self, aez, start_year=None, end_year=None, gee_account_id=None
):
    task_id = rainfall_index(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    task_id, asset_id = generate_hybrid_tree_mask(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    if is_gee_asset_exists(asset_id):
        generate_rainfall_resilience(
            aez, start_year=2004, end_year=end_year, gee_account_id=gee_account_id
        )


@app.task(bind=True)
def run_forest_fire_resistance_resilience(
    self, aez, start_year=None, end_year=None, gee_account_id=None
):
    task_id = fire_index(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    task_id, asset_id = generate_hybrid_tree_mask(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    if is_gee_asset_exists(asset_id):
        forest_fire_sensitivity(
            aez, start_year=2004, end_year=end_year, gee_account_id=gee_account_id
        )


@app.task(bind=True)
def run_high_wind_resistance_resilience(
    self, aez, start_year=None, end_year=None, gee_account_id=None
):
    task_id = max_wind_index(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    task_id, asset_id = generate_hybrid_tree_mask(
        aez, start_year=start_year, end_year=end_year, gee_account_id=gee_account_id
    )
    if task_id:
        check_task_status([task_id])

    if is_gee_asset_exists(asset_id):
        high_wind_sensitivity(
            aez, start_year=2004, end_year=end_year, gee_account_id=gee_account_id
        )
