import os
import subprocess
from nrm_app.settings import USERNAME_GESDISC, PASSWORD_GESDISC
import datetime


def upload_assets_gee():
    """Upload Vector Layers and Raster Layers in the GEE Assets

    Vector layers are directly uploaded to the GEE
    Raster layers first needs to be uploaded to the Google Bucket --> GEE Assets
    """
    pass


def download_et_data(asset_path):
    """
    Download files required for Evapotranspiration from NASA
    """

    # Global Dataset
    globalnames = []

    date = datetime.date(2023, 1, 1)
    year = str(date.year)
    month = str(date.month).zfill(2)
    day = str(date.day).zfill(2)
    full_date = date.strftime("%Y%m")

    base_url = "https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi"
    params = {
        "FILENAME": f"/data/FLDAS/FLDAS_NOAH01_C_GL_M.001/{year}/FLDAS_NOAH01_C_GL_M.A{year}{month}.001.nc",
        "VARIABLES": "Evap_tavg",
        "FORMAT": "Y29nLw",
        "LABEL": f"FLDAS_NOAH01_C_GL_M.A{year}{month}.001.nc.SUB.tif",
        "SERVICE": "L34RS_LDAS",
        "DATASET_VERSION": "001",
        "VERSION": "1.02",
        "SHORTNAME": "FLDAS_NOAH01_C_GL_M",
        "BBOX": "-60,-180,90,180",
    }

    filename = f'{base_url}?{"&".join(f"{k}={v}" for k, v in params.items())}'

    output_name = f"{full_date}.tif"
    print(date.strftime("%Y%m%d"), year, month, day, filename)
    subprocess.call(
        [
            "wget",
            "-O",
            output_name,
            "--user",
            USERNAME_GESDISC,
            "--password",
            PASSWORD_GESDISC,
            filename,
        ]
    )
    date -= datetime.timedelta(days=1)

    final_output_filename = output_name
    final_output_assetid = asset_path + full_date
    globalnames.append(output_name)

    # Central Asia
    central_asia_dataset = []


def download_sm_data():

    pass
