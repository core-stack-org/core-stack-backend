import os

from nrm_app.settings import BASE_DIR
from .FC_Change import generate_afforestation_mask
from .FinalClass import RiskMaps
from utilities.gee_utils import ee_initialize
from .area_estimation import get_area_estimation


def forest_additionality(start_year, mid_pt, end_year, state_name):
    # Initialize the RiskMaps engine
    base_file_path = f"{BASE_DIR}/data/forest_additionality/{state_name}"
    working_directory = os.path.join(base_file_path, "outputs")

    if not os.path.exists(working_directory):
        os.makedirs(working_directory)

    engine = RiskMaps(
        base_file_path, working_directory, start_year, mid_pt, end_year, state_name
    )

    ee_initialize(4)

    engine.perform_gee_operations()

    engine.run_wo_gee()

    generate_afforestation_mask(
        state_name, start_year, mid_pt, end_year, working_directory
    )

    get_area_estimation(state_name, start_year, mid_pt, end_year, base_file_path)
