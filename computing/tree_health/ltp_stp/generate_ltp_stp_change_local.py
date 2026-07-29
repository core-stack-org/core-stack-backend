import os
import numpy as np
import rasterio
from computing.config_loader import PROJECT_ROOT
from nrm_app.celery import app

# Local base directory for input and output raster files.
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/base_layers/tree_health/ltp_stp"

# Mapping of agro-climatic zones (ACZs) to their output acronyms.
ACZS = {
    "Eastern Plateau & Hills Region": "EPAHR",
    "Southern Plateau and Hills Region": "SPAHR",
    "East Coast Plains & Hills Region": "ECPHR",
    "Western Plateau and Hills Region": "WPAHR",
    "Central Plateau & Hills Region": "CPAHR",
    "Lower Gangetic Plain Region": "LGPR",
    "Middle Gangetic Plain Region": "MGPR",
    "Upper Gangetic Plain Region": "UGPR",
    "Trans Gangetic Plain Region": "TGPR",
    "Eastern Himalayan Region": "EHR",
    "Western Himalayan Region": "WHR",
    "West Coast Plains & Ghat Region": "WCPGR",
    "Gujarat Plains & Hills Region": "GPHR",
    "Western Dry Region": "WDR",
}


@app.task(bind=True)
def generate_ltp_stp_change_local(self, year_1: int, year_2: int, scale=25):
    """Generate LTP-STP change rasters for the given year pair.

    For each ACZ, this function loads the LTP raster for year_1 and year_2,
    computes the change category, and writes a compact uint8 output raster.
    """

    for acz, acronym in ACZS.items():
        print(f"Processing {acz}")

        # Input file paths for both years.
        ltp_file_1 = os.path.join(
            LOCAL_OUTPUT_BASE_DIR,
            f"{scale}",
            f"ltp_{year_1}",
            acronym,
            f"ltp_{year_1}_{acronym}.tif",
        )
        ltp_file_2 = os.path.join(
            LOCAL_OUTPUT_BASE_DIR,
            f"{scale}",
            f"ltp_{year_2}",
            acronym,
            f"ltp_{year_2}_{acronym}.tif",
        )

        if not (os.path.exists(ltp_file_1) and os.path.exists(ltp_file_2)):
            print("Missing input(s)")
            continue

        # Read first year raster and preserve profile metadata for output.
        with rasterio.open(ltp_file_1) as src1:
            ltp1 = src1.read(1)
            profile = src1.profile.copy()
            nodata = src1.nodata if src1.nodata is not None else 255

        # Read second year raster.
        with rasterio.open(ltp_file_2) as src2:
            ltp2 = src2.read(1)

        # Standardize nodata values for comparison.
        ltp1 = np.where(ltp1 == nodata, -9999, ltp1)
        ltp2 = np.where(ltp2 == nodata, -9999, ltp2)

        # Initialize output change raster with nodata default.
        change = np.full(ltp1.shape, -9999, dtype=np.int16)

        # Define change categories:
        # 0 -> no change, absent in both years
        # 1 -> no change, present in both years
        # 2 -> new presence in year_2
        # 3 -> loss from year_1 to year_2
        # 4 -> year_1 nodata, year_2 absent
        # 5 -> year_1 nodata, year_2 present
        # 6 -> year_1 absent, year_2 nodata
        # 7 -> year_1 present, year_2 nodata
        change[(ltp1 == 0) & (ltp2 == 0)] = 0
        change[(ltp1 == 1) & (ltp2 == 1)] = 1
        change[(ltp1 == 0) & (ltp2 == 1)] = 2
        change[(ltp1 == 1) & (ltp2 == 0)] = 3
        change[(ltp1 == -9999) & (ltp2 == 0)] = 4
        change[(ltp1 == -9999) & (ltp2 == 1)] = 5
        change[(ltp1 == 0) & (ltp2 == -9999)] = 6
        change[(ltp1 == 1) & (ltp2 == -9999)] = 7

        # Convert back to original nodata and write as uint8.
        change = np.where(change == -9999, nodata, change).astype(np.uint8)
        profile.update(
            driver="GTiff", dtype="uint8", count=1, compress="lzw", nodata=nodata
        )

        outdir = os.path.join(LOCAL_OUTPUT_BASE_DIR, f"ltp_change_{year_1}_{year_2}")
        os.makedirs(outdir, exist_ok=True)
        outfile = os.path.join(outdir, f"ltp_change_{year_1}_{year_2}_{acronym}.tif")

        with rasterio.open(outfile, "w", **profile) as dst:
            dst.write(change, 1)

        print("Saved:", outfile)


# To merge the files in the output directory and run the following command in terminal:
# gdal_merge.py \
# -o ltp_stp_change_2017_2024.tif \
# -co COMPRESS=LZW \
# -co TILED=YES \
# -co BIGTIFF=YES \
# -n 255 \
# -a_nodata 255 \
# *.tif
