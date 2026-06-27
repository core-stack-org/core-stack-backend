from computing.et_downscale.et_downscale import generate_et_downscale
import ee

from utilities.constants import AEZ
from utilities.gee_utils import ee_initialize


def generate_et_aez(aez_no=None, project_name="corestack-datasets"):
    # ee_initialize(7)
    initialize_gee_and_drive(project_name)
    if aez_no:
        generate_et(aez_no)
    else:
        for aez_no in range(2, 20):
            generate_et(aez_no)


def generate_et(aez_no):
    aez = ee.FeatureCollection(AEZ)
    roi = aez.filter(ee.Filter.eq("ae_regcode", aez_no))
    generate_et_downscale(
        roi=roi,
        asset_suffix=f"AEZ_{str(aez_no)}",
        asset_folder_list=["et_downscale"],
        start_year=2017,
        end_year=2024,
        gee_account_id=7,
        application="pet",
        app_type="MWS",
        aez=aez_no,
    )


def initialize_gee_and_drive(project_name: str):
    """
    Initialize Google Earth Engine (GEE) and authenticate the user.
    This function is called in the constructor of the class.

    Args:
        project_name (str): The name of the GEE project to initialize.
    """
    try:
        ee.Authenticate()
        ee.Initialize(project=project_name)
        print("Google Earth Engine initialized successfully.")
    except Exception as e:
        print(f"Error initializing GEE: {e}")
        print("Please authenticate your Google Earth Engine account.")
    # if False:
    #     try:
    #         drive.mount("/content/drive")
    #         print("Google Drive mounted successfully.")
    #     except Exception as e:
    #         print(
    #             "Google Drive is not available. Please run this code in Google Colab."
    #         )
