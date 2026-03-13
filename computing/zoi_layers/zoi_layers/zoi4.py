from computing.utils import sync_fc_to_geoserver
from computing.water_rejuvenation.water_rejuventation import (
    compute_water_score,
    get_lulc_asset_from_year,
    calculate_elevation,
    get_centroid_point,
    wrap_compute_metrics,
    export_and_wait,
)
from gee_computing.utils import mask_landsat_clouds
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    get_gee_dir_path,
    valid_gee_text,
    sync_raster_gcs_to_geoserver,
    export_vector_asset_to_gee,
)
import ee

from waterrejuvenation.utils import delete_asset_on_GEE


def generate_ndmi_layer(
    state=None,
    block=None,
    district=None,
    asset_suffix=None,
    asset_folder_list=None,
    proj_id=None,
    app_type="MWS",
    gee_account_id=None,
):
    print("started generating ndvi")
    ee_initialize(gee_account_id)
    from waterrejuvenation.utils import get_ndvi_data

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )

        asset_folder_list = [state, district, block]
        workspace = "water_bodies"
    else:
        workspace = "zoi_layers"

    description_swb = "swb4_" + asset_suffix
    asset_id_swb = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_swb
    )
    # Step 1: Load SWB Features and Compute Water Score
    swb_fc = ee.FeatureCollection(asset_id_swb)
    scored_fc = swb_fc.map(compute_water_score)
    top_feature = scored_fc.sort("water_score", False).first()
    lulc_year = top_feature.getInfo()["properties"]["water_year"].split("-")
    s_year = f"20{lulc_year[0]}-07-01"
    e_year = f"20{lulc_year[1]}-06-30"

    asset_id_lulc = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + asset_suffix
        + "_"
        + str(s_year)
        + "_"
        + str(e_year)
        + "_LULCmap_10m"
    )

    landsat = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_TOA")
        .filterDate(s_year, e_year)
        .map(mask_landsat_clouds)
    )
    elevation, cropping_mask, ndmi_img = calculate_elevation(landsat, asset_id_lulc)

    # Step 3: NDMI Computation at Centroid of waterbody
    swb_centroids = swb_fc.map(get_centroid_point)
    asset_suffix_ndmi = f"zoi_ndmi_{asset_suffix}"
    asset_id_ndmi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + asset_suffix_ndmi
    )
    layer_name = f"waterbodies_zoi_{asset_suffix}"
    try:
        delete_asset_on_GEE(asset_id_ndmi)
    except Exception:
        print("NDMI asset not present, skipping delete.")

    ndmi_computed_fc = swb_centroids.map(
        wrap_compute_metrics(elevation, ndmi_img, cropping_mask)
    )
    export_and_wait(ndmi_computed_fc, asset_suffix_ndmi, asset_id_ndmi)
    task_id = export_vector_asset_to_gee(
        ndmi_computed_fc, asset_suffix_ndmi, asset_id_ndmi
    )

    res = sync_fc_to_geoserver(
        ndmi_computed_fc, asset_suffix, layer_name, workspace=workspace
    )
