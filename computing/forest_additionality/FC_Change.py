# if len(sys.argv) > 1:
#     state_name = sys.argv[1]
#     start_year = int(sys.argv[2])
#     mid_pt = int(sys.argv[3])
#     end_year = int(sys.argv[4])
# else:
#     state_name = "Dhenkanal"
#     start_year = 2010
#     mid_pt = 2015
#     end_year = 2020

import rasterio
import numpy as np

import os


def generate_afforestation_mask(state_name, start_year, mid_pt, end_year, dir_path):
    """
    File Locations/Paths
    --------------------
    forest_cover1: Path to the forest cover raster for the start year
    forest_cover2: Path to the forest cover raster for the mid point year
    forest_cover3: Path to the forest cover raster for the end year
    """

    if os.path.exists(dir_path):
        forest_cover1 = f"{dir_path}/{state_name}_{start_year}.tif"
        forest_cover2 = f"{dir_path}/{state_name}_{mid_pt}.tif"
        forest_cover3 = f"{dir_path}/{state_name}_{end_year}.tif"
        # Calculate deforestation and afforestation
        for i in range(2):
            with rasterio.open(forest_cover1) as src:
                data0 = src.read(1)
                meta = src.meta.copy()

            if i == 0:
                change_cover = forest_cover2
            else:
                change_cover = forest_cover3
            with rasterio.open(change_cover) as src1:
                data1 = src1.read(1)

            """
            Change matrix:
            1 -> Deforestation (Forest to Non-forest)
            -1 -> Afforestation (Non-forest to Forest)
            0 -> No change (Forest to Forest or Non-forest to Non-forest)
            """
            change = np.zeros_like(data0, dtype=np.int8)
            change[(data0 == 1) & (data1 == 0)] = -1
            change[(data0 == 0) & (data1 == 1)] = 1
            change[(data0 == 1) & (data1 == 1)] = 0
            change[(data0 == 0) & (data1 == 0)] = 0

            meta.update(dtype=rasterio.int8)

            # Create separate masks for deforestation and afforestation
            deforestation_mask = np.zeros_like(change, dtype=np.int8)
            deforestation_mask[change == 1] = -1
            afforestation_mask = np.zeros_like(change, dtype=np.int8)
            afforestation_mask[change == 1] = 1
            print(change.shape)
            # Save the masks
            if i == 0:
                with rasterio.open(
                    f"{dir_path}/afforestation_{start_year}_{mid_pt}.tif",
                    "w",
                    **meta,
                ) as dst:
                    dst.write(afforestation_mask, 1)
            elif i == 1:
                with rasterio.open(
                    f"{dir_path}/afforestation_{start_year}_{end_year}.tif",
                    "w",
                    **meta,
                ) as dst:
                    dst.write(afforestation_mask, 1)
    else:
        print(
            "Please run Step 1 first and download the appropriate from the drive to the correct location"
        )
