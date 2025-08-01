import geopandas as gpd
import os
from .rasterize_vector import rasterize_vector
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
)
from computing.utils import save_layer_info_to_db


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
        state_aquifer = state_aquifer.to_crs(state_lith.crs)

        joined = gpd.sjoin(
            state_lith, state_aquifer, how="left", predicate="intersects"
        )
        output_path = os.path.join(LITHOLOGY_PATH + "output/", "_".join(state.split()))
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        joined.to_file(f"{output_path}/{'_'.join(state.split())}.shp")
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
    #     save_layer_info_to_db(
    #         state,
    #         district,
    #         block,
    #         layer_name="",
    #         asset_id=asset_id,
    #         dataset_name="Lithology",
    #     )
    #     print("save lithology info at the gee level...")
