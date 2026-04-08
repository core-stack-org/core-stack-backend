import ee

from nrm_app.settings import BASE_DIR
from .FC_Change import generate_afforestation_mask
from .FinalClass import RiskMaps
from utilities.gee_utils import ee_initialize
from .area_estimation import get_area_estimation


def forest_additionality(start_year, mid_pt, end_year, state_name):
    # Initialize the RiskMaps engine
    dir_path = f"{BASE_DIR}/data/forest_additionality/{state_name}"  # f"/mnt/d/workspaces/BECC/data/GEE_exports_{state_name}"
    print(dir_path)
    # engine = RiskMaps(
    #     dir_path,
    #     start_year,
    #     mid_pt,
    #     end_year,
    #     state_name,
    # )

    # ee.Authenticate(auth_mode="notebook")  # , force=True)
    # ee.Initialize(project="core-stack-dev-2")

    # ee_initialize(3)

    # engine.perform_gee_operations()

    # engine.run_wo_gee()

    # generate_afforestation_mask(state_name, start_year, mid_pt, end_year, dir_path)

    get_area_estimation(state_name, start_year, mid_pt, end_year, dir_path)
