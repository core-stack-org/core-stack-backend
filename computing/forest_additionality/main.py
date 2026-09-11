import os

from nrm_app.settings import BASE_DIR
from .FC_Change import generate_afforestation_mask
from .FinalClass import RiskMaps
from utilities.gee_utils import ee_initialize
from .afforestation_area_estimation import get_afforestation_area_estimation
from .deforestation_area_estimation import get_deforestation_area_estimation


def forest_additionality(
    start_year, mid_pt, end_year, state_name, district_name, gee_account_id=None
):
    # Initialize the RiskMaps engine
    base_file_path = (
        f"{BASE_DIR}/data/forest_additionality/{state_name}/{district_name}"
    )
    working_directory = os.path.join(
        base_file_path, f"{start_year}_{mid_pt}_{end_year}"
    )

    if not os.path.exists(working_directory):
        os.makedirs(working_directory)

    engine = RiskMaps(
        base_file_path,
        working_directory,
        start_year,
        mid_pt,
        end_year,
        state_name,
        district_name,
    )

    ee_initialize(gee_account_id)

    engine.perform_gee_operations()

    engine.run_wo_gee()

    generate_afforestation_mask(
        district_name, start_year, mid_pt, end_year, working_directory
    )

    get_deforestation_area_estimation(
        district_name, start_year, mid_pt, end_year, working_directory
    )

    get_afforestation_area_estimation(
        district_name, start_year, mid_pt, end_year, working_directory
    )
