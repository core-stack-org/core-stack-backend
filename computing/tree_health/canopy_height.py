import ee
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    sync_raster_to_gcs,
    check_task_status,
    sync_raster_gcs_to_geoserver,
    export_raster_asset_to_gee,
    make_asset_public,
    get_gee_dir_path,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from computing.STAC_specs import generate_STAC_layerwise
from constants.pan_india_urls import CH_RASTER


# Celery task to generate canopy height raster
@app.task(bind=True)
def tree_health_ch_raster(
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
    print("Inside process Tree health ch raster")

    # Initialize Earth Engine
    ee_initialize(gee_account_id)

    # Prepare ROI and asset folder path
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        # Load ROI FeatureCollection
        roi = ee.FeatureCollection(
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + asset_suffix
            + "_uid"
        )

    layer_at_geoserver = False

    # Loop for each year
    for year in range(start_year, end_year + 1):

        # Create asset name
        description = (
            "ch_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + str(year)
        )

        asset_id = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description
        )

        # Create raster asset if it does not exist
        if not is_gee_asset_exists(asset_id):
            ch_raster = ee.ImageCollection(CH_RASTER + str(year))

            # Filter by ROI and take mean image
            raster = ch_raster.filterBounds(roi.geometry()).mean().clip(roi.geometry())

            # Load LULC map for tree masking
            lulc = ee.Image(
                get_gee_dir_path(
                    asset_folder_list, asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
                )
                + f"{asset_suffix}_{year}-07-01_{year + 1}-06-30_LULCmap_10m"
            )

            # Apply tree mask (class 6 = tree)
            tree_mask = lulc.eq(6).reproject(crs="EPSG:4326", scale=25)
            raster = raster.updateMask(tree_mask)

            # Export raster to GEE asset
            task_id = export_raster_asset_to_gee(
                image=raster,
                description=description,
                asset_id=asset_id,
                scale=25,
                region=roi.geometry(),
            )

            check_task_status([task_id])

        # If asset exists, make public and sync
        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)

            # Save layer metadata in DB
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                description,
                asset_id,
                "Canopy Height Raster",
                misc={"start_year": start_year, "end_year": end_year},
            )

            # Export raster to Google Cloud Storage
            task_id = sync_raster_to_gcs(ee.Image(asset_id), 25, description)

            check_task_status([task_id])

            # Sync raster from GCS to GeoServer
            res = sync_raster_gcs_to_geoserver(
                "canopy_height", description, description, "ch_style"
            )

            if res and layer_id:
                layer_at_geoserver = True

                # layer_STAC_generated = False
                # layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                #     state=state,
                #     district=district,
                #     block=block,
                #     layer_name="ch_raster",
                #     start_year=year,
                # )

                # Update sync flag in DB
                update_layer_sync_status(
                    layer_id=layer_id,
                    sync_to_geoserver=layer_at_geoserver,
                )

    return layer_at_geoserver
