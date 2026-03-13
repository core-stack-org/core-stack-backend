from computing.utils import generate_swb_layer_with_max_so_catchment
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
import ee

from waterrejuvenation.utils import add_on_drainage_flag


def waterbody_catchment_streamorder_properties(
    roi=None,
    state=None,
    district=None,
    block=None,
    project_id=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type=None,
    gee_account_id=None,
):
    print(f"asset suffix swb4: {asset_suffix}")
    description = "swb4_" + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
    asset_suffix_swb4 = "swb4_" + asset_suffix
    swb3_asset = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "swb3_"
        + asset_suffix
    )

    swb2_asset = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "swb2_"
        + asset_suffix
    )
    try:
        ee.data.getAsset(swb3_asset)
        water_bodies = ee.FeatureCollection(swb3_asset)
    except Exception as e:
        print("SWB3 does not exist")
        water_bodies = ee.FeatureCollection(swb2_asset)

    print(f"asset_i{water_bodies}")
    swb4_fs = generate_swb_layer_with_max_so_catchment(
        roi=water_bodies,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder_list,
        app_type=app_type,
        gee_account_id=gee_account_id,
    )
    asset_id_dl = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + "drainage_lines_"
        + asset_suffix
    )
    swb4_fs_on_drainage = add_on_drainage_flag(swb4_fs, asset_id_dl)
    task_id = export_vector_asset_to_gee(swb4_fs_on_drainage, description, asset_id)
    return task_id, asset_id
