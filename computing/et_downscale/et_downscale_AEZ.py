from computing.et_downscale.et_downscale import generate_et_downscale
import ee

from utilities.constants import AEZ
from utilities.gee_utils import ee_initialize


def generate_et_aez(aez_no=None):
    ee_initialize(7)
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
        application="aet",
        app_type="MWS",
        aez=aez_no,
        asset_root="projects/corestack1-dev-alpha/assets/et_downscale/",  # Change it with the GEE project path where to export the asset
    )
