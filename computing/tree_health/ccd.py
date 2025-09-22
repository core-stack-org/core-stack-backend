import ee
from nrm_app.celery import app
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
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status


@app.task(bind=True)
def tree_health_ccd_raster(self, state, district, block, start_year, end_year):
    print("Inside process Tree health ccd raster")
    ee_initialize()

    # Get the block MWS (Micro Watershed) features
    block_mws = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    # Get the block geometry once for consistent clipping
    block_geometry = block_mws.geometry()

    for year in range(start_year, end_year + 1):
        description = (
            "tree_health_ccd_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + str(year)
        )

        asset_id = get_gee_asset_path(state, district, block) + description

        # Skip if asset already exists
        if is_gee_asset_exists(asset_id):
            return

        # Define the path for CCD data based on start year
        if year == 2016 or year == 2022:
            ccd_path = "projects/ee-mtpictd/assets/harsh/ccd_" + str(year)
        else:
            ccd_path = "projects/ee-mtpictd/assets/dhruvi/modal_ccd_" + str(year)

        # Load and properly clip the CCD Image Collection once
        ccd_img = (
            ee.ImageCollection(ccd_path)
            .filterBounds(block_geometry)
            .mean()
            .clip(block_geometry)
        )

        # # Compute CCD statistics function
        # def ccd_stats(feature):
        #     stats = ccd_img.reduceRegion(
        #         reducer=ee.Reducer.frequencyHistogram().unweighted(),
        #         geometry=feature.geometry(),
        #         scale=25,
        #         maxPixels=1e10,
        #     )
        #     pixel_counts = ee.Dictionary(stats.get("classification"))
        #     return feature.set(
        #         {
        #             "ccd_0": pixel_counts.get("0.0", 0),  # Low Density
        #             "ccd_1": pixel_counts.get("1.0", 0),  # High Density
        #             "ccd_2": pixel_counts.get("2.0", 0),  # Missing Data
        #         }
        #     )

        # Apply CCD statistics function to the feature collection
        # block_mws_with_stats = block_mws.map(ccd_stats)
        task_id = export_raster_asset_to_gee(
            image=ccd_img,
            description=description,
            asset_id=asset_id,
            scale=25,
            region=block_geometry,
        )
        task_id_list = check_task_status([task_id])
        print("CCD task_id_list", task_id_list)

        # Sync image to Google Cloud Storage and Geoserver
        layer_name = (
            "tree_health_ccd_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + str(year)
        )
        layer_at_geoserver = False
        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)
            # layer_id = save_layer_info_to_db(
            #     state,
            #     district,
            #     block,
            #     layer_name,
            #     asset_id,
            #     "Ccd Raster",
            #     misc={"start_year": start_year, "end_year": end_year},
            # )
            task_id = sync_raster_to_gcs(ee.Image(asset_id), 25, layer_name)

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to GCS", task_id_list)

            res = sync_raster_gcs_to_geoserver(
                "ccd", layer_name, layer_name, "ccd_style"
            )
            # if res and layer_id:
            #     update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            #     print("sync to geoserver flag is updated")
            if res:
                layer_at_geoserver = True
        return layer_at_geoserver
