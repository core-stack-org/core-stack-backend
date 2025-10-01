import os
import geopandas as gpd
import re
from nrm_app.celery import app
from utilities.constants import LITHOLOGY_PATH
from utilities.gee_utils import (
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    upload_tif_to_gcs,
    upload_tif_from_gcs_to_gee,
    make_asset_public,
    ee_initialize,
)
from .rasterize_vector import rasterize_vector
import shutil


@app.task(bind=True)
def generate_lithology_layer(self, state):
    asset_id = get_gee_asset_path(state) + valid_gee_text(state.lower()) + "_lithology"

    if not is_gee_asset_exists(asset_id):
        state_lith = gpd.read_file(
            f"""{LITHOLOGY_PATH}inputs/{"_".join(state.split())}/Lithology.shp"""
        )
        state_aquifer = gpd.read_file(
            LITHOLOGY_PATH
            + "yield_aquifer/"
            + ("_".join(state.split()))
            + "/aquifer.shp"
        )
        if state_lith.crs != state_aquifer.crs:
            state_aquifer = state_aquifer.to_crs(state_lith.crs)

        # Perform the clip
        state_lith = gpd.clip(state_lith, state_aquifer)

        def split(s):
            pattern = r"[ ,\/\\()&*-]"
            words = re.split(pattern, s)
            words = [word for word in words if word]
            return words

        def word_wise_matching(s1, s2):
            score = 0
            s1 = s1.lower()
            s2 = s2.lower()
            s2 = split(s2)
            for i in s2:
                if i in s1:
                    score += 1

            return score

        print("Started")
        count = 0
        total = 0
        for index_shapefile, row_shapefile in state_lith.iterrows():
            total += 1
            max_major_acquifer = None
            max_principle_acquifer = None
            max_score = 0

            for index_table, row_table in state_aquifer.iterrows():
                score_1 = word_wise_matching(
                    str(row_table["Principal_"]), str(row_shapefile["GROUP_NAME"])
                )

                score_2 = word_wise_matching(
                    str(row_table["Major_Aqui"]), str(row_shapefile["LITHOLOGIC"])
                )

                score_3 = word_wise_matching(
                    str(row_table["Principal_"]), str(row_shapefile["LITHOLOGIC"])
                )

                score_4 = word_wise_matching(
                    str(row_table["Major_Aqui"]), str(row_shapefile["GROUP_NAME"])
                )

                score_5 = word_wise_matching(
                    str(row_table["Age"]), str(row_shapefile["AGE"])
                )

                if (
                    score_5 > 0
                    and score_1 == 0
                    and score_2 == 0
                    and score_3 == 0
                    and score_4 == 0
                ):
                    continue

                total_score = score_1 + score_2 + score_3 + score_4 + score_5

                if total_score > max_score:
                    max_score = total_score
                    age = row_table["Age"]
                    max_major_acquifer = row_table["Major_Aqui"]
                    max_principle_acquifer = row_table["Principal_"]
                    acquifer_code = row_table["Major_Aq_1"]
                    rif = row_table["Recommende"]

            if max_score != 0:
                count += 1
                state_lith.at[index_shapefile, "Age"] = age
                state_lith.at[index_shapefile, "Major_Aquifer"] = max_major_acquifer
                state_lith.at[index_shapefile, "Principal_Aquifer"] = (
                    max_principle_acquifer
                )
                state_lith.at[index_shapefile, "Major_Aquifer_Code"] = acquifer_code
                state_lith.at[index_shapefile, "Recommended_RIF"] = rif
                lithology_class = None
                if rif < 10:
                    lithology_class = 3
                elif 10 <= rif <= 15:
                    lithology_class = 2
                elif rif > 15:
                    lithology_class = 1

                state_lith.at[index_shapefile, "Lithology_Class"] = lithology_class
            else:
                state_lith.at[index_shapefile, "Age"] = None
                state_lith.at[index_shapefile, "Major_Aquifer"] = None
                state_lith.at[index_shapefile, "Principal_Aquifer"] = None
                state_lith.at[index_shapefile, "Major_Aquifer_Code"] = None
                state_lith.at[index_shapefile, "Recommended_RIF"] = None
                state_lith.at[index_shapefile, "Lithology_Class"] = None

        print("Number of rows classified: ", count)
        print("Total number of rows: ", total)
        print("Number of rows unclassified: ", total - count)
        print("Loop Completed")

        output_path = os.path.join(LITHOLOGY_PATH + "output/", "_".join(state.split()))
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        state_lith.to_file(f"{output_path}/{'_'.join(state.split())}.shp")

        output_raster_path = f"{output_path}/{'_'.join(state.split())}.tif"
        rasterize_vector(output_path, output_raster_path, "Lithology_")

        gcs_path = upload_tif_to_gcs(
            f"{'_'.join(state.split())}.tif", output_raster_path
        )

        task_id = upload_tif_from_gcs_to_gee(gcs_path, asset_id, 30)
        task_list = check_task_status([task_id])
        print("lithology task list ", task_list)

        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)

        path = output_path.split("/")[:-1]
        path = os.path.join(*path)
        shutil.rmtree(path)
