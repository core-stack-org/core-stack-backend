import ee
from computing.utils import (
    sync_fc_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    check_task_status,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from nrm_app.celery import app
from computing.mws.evapotranspiration import merge_assets_chunked_on_year


# Celery task to generate CCD vector data
@app.task(bind=True)
def tree_health_ccd_vector(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year=None,
    end_year=None,
    app_type="MWS",
    gee_account_id=None,
):
    # Initialize Earth Engine
    ee_initialize(gee_account_id)

    # Prepare ROI and asset path
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        # Load ROI from GEE
        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + asset_suffix
            + "_uid"
        )

    yearly_assets = []

    # Process each year
    for year in range(start_year, end_year + 1):

        description = f"ccd_vector_{asset_suffix}_{year}"

        year_asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

        # Create yearly asset if not exists
        if not is_gee_asset_exists(year_asset_id):
            fc = ccd_vector(
                roi,
                year,
                asset_folder_list,
                asset_suffix,
                app_type,
            )

            task_id = export_vector_asset_to_gee(fc, description, year_asset_id)
            check_task_status([task_id])

        # Add existing asset to merge list
        if is_gee_asset_exists(year_asset_id):
            yearly_assets.append(year_asset_id)

    # Final merged asset name
    description = (
        "ccd_vector_" + asset_suffix + "_" + str(start_year) + "_" + str(end_year)
    )

    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    # Merge yearly assets if not exists
    if not is_gee_asset_exists(asset_id):
        task = merge_assets_chunked_on_year(yearly_assets, description, asset_id)
        check_task_status([task])

    layer_at_geoserver = False

    # Publish and sync if asset exists
    if is_gee_asset_exists(asset_id):

        make_asset_public(asset_id)

        # Save layer info in DB
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=description,
            asset_id=asset_id,
            dataset_name="Ccd Vector",
            misc={"star_year": start_year, "end_year": end_year},
        )

        merged_fc = ee.FeatureCollection(asset_id)

        # Sync to GeoServer
        sync_res = sync_fc_to_geoserver(merged_fc, state, description, "ccd")

        # Update DB sync status
        if sync_res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_at_geoserver = True

    return layer_at_geoserver


def ccd_vector(roi, year, asset_folder_list, asset_suffix, app_type):
    """Create vector data from CCD raster."""

    # Density classes
    args = [
        {"value": 0.0, "label": "Low_Density"},
        {"value": 1.0, "label": "High_Density"},
        {"value": 2.0, "label": "Missing_Data"},
    ]

    # Load CCD raster
    raster = ee.Image(
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"ccd_raster_{asset_suffix}_{year}"
    )

    fc = roi

    # Calculate area for each class
    for arg in args:
        raster_cc = raster.select(["cc"])
        mask = raster_cc.eq(ee.Number(arg["value"]))

        pixel_area = ee.Image.pixelArea()
        forest_area = pixel_area.updateMask(mask)

        # Sum area per polygon
        fc = forest_area.reduceRegions(
            collection=fc,
            reducer=ee.Reducer.sum(),
            scale=25,
            crs="EPSG:4326",
        )

        # Convert m² to hectares
        def process_feature(feature):
            area_val = ee.Number(feature.get("sum"))
            area_ha = area_val.multiply(0.0001)
            return feature.set(f"{arg['label']}_{year}", area_ha)

        fc = fc.map(process_feature)

    return fc
