from computing.et_downscale.et_downscale import generate_et_downscale
import ee

from utilities.constants import AEZ


def generate_et_aez(
    aez_no=None,
    start_year=2017,
    end_year=None,
    application="aet",
    project_name="corestack-datasets",
):
    """
    We are exporting the assets directly to drive in case of AEZ level generation because they take a lot of space
    which is not manageable on GEE. So here instead of using service_account for GEE authentication,
    we are using browser-based authentication.
    Args:
        aez_no: ae_regcode of the AEZ for which we want to generate the assets
        project_name: GEE project name on which we want to run the computation
        start_year: start year for which we want to run the computation
        end_year: end year for which we want to run the computation
        application: ET application name (aet/pet/gpp, etc.)
    """
    initialize_gee_and_drive(project_name)
    if aez_no:
        generate_et(aez_no, start_year, end_year, application)
    else:
        for aez_no in range(2, 20):
            generate_et(aez_no, start_year, end_year, application)


def generate_et(aez_no, start_year, end_year, application):
    aez = ee.FeatureCollection(AEZ)
    roi = aez.filter(ee.Filter.eq("ae_regcode", aez_no))
    generate_et_downscale(
        roi=roi,
        asset_suffix=f"AEZ_{str(aez_no)}",
        asset_folder_list=["et_downscale"],
        start_year=start_year,
        end_year=end_year,
        application=application,
        aez=aez_no,
    )


def initialize_gee_and_drive(project_name: str):
    """
    Initialize Google Earth Engine (GEE) and authenticate the user through the browser where the GEE account is logged in.

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
