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
def tree_health_ch_raster(self, state, district, block, start_year, end_year):
    ee_initialize()
    print("Inside process tree_health_ch_raster")
    # ch_palette = [
    #     "FFA500",
    #     "FFA500",
    #     "DEE64C",
    #     "DEE64C",
    #     "DEE64C",
    #     "DEE64C",
    #     "007500",
    #     "007500",
    #     "000000"
    # ]

    block_mws = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    for year in range(start_year, end_year):
        description = (
            "tree_health_ch_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_"
            + str(year)
        )
        asset_id = get_gee_asset_path(state, district, block) + description

        if is_gee_asset_exists(asset_id):
            return

        # Define the path for CH data based on start year
        if year == 2016 or year == 2022:
            ch_path = "projects/ee-mtpictd/assets/harsh/ch_" + str(year)
        else:
            ch_path = "projects/ee-mtpictd/assets/dhruvi/modal_ch_" + str(year)

        # Load and process the CH Image Collection
        ch_img = (
            ee.ImageCollection(ch_path).filterBounds(block_mws).mean().clip(block_mws)
        )

        # visualization = {"min": 0, "max": 8, "palette": ch_palette}

        ch_composite = ch_img.rename(["classification"])

        # Function to compute the change statistics per block
        def change_stats(feature):
            stats = ch_composite.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram().unweighted(),
                geometry=feature.geometry(),
                scale=25,
                maxPixels=1e10,
            )
            pixelCounts = ee.Dictionary(stats.get("classification"))

            # Return feature with additional properties representing the change statistics
            return feature.set(
                {
                    "ch_0": ee.Number(pixelCounts.get("0.0", 0)).add(
                        ee.Number(pixelCounts.get("1.0", 0))
                    ),  # Short Trees
                    "ch_1": ee.Number(pixelCounts.get("2.0", 0))
                    .add(ee.Number(pixelCounts.get("3.0", 0)))
                    .add(ee.Number(pixelCounts.get("4.0", 0)))
                    .add(ee.Number(pixelCounts.get("5.0", 0))),  # Medium Height Trees
                    "ch_2": ee.Number(pixelCounts.get("6.0", 0)).add(
                        ee.Number(pixelCounts.get("7.0", 0))
                    ),  # Tall Trees
                    "ch_3": pixelCounts.get("8.0", 0),  # Missing Data
                }
            )

        block_mws = block_mws.map(change_stats)

        # Export the result to GEE
        task_id = export_raster_asset_to_gee(
            image=ch_img.clip(block_mws.geometry()),
            description=description,
            asset_id=asset_id,
            scale=25,
            region=block_mws.geometry(),
        )
        task_id_list = check_task_status([task_id])
        print("CH task_id_list", task_id_list)

        layer_at_geoserver = False
        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)
            layer_name = (
                "tree_health_ch_raster_"
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(year)
            )

            # layer_id = save_layer_info_to_db(
            #     state,
            #     district,
            #     block,
            #     layer_name,
            #     asset_id,
            #     "Canopy Height Raster",
            #     misc={"start_year": start_year, "end_year": end_year},
            # )

            # Sync image to Google Cloud Storage and Geoserver
            task_id = sync_raster_to_gcs(ee.Image(asset_id), 30, layer_name)

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to GCS", task_id_list)

            res = sync_raster_gcs_to_geoserver(
                "canopy_height", layer_name, layer_name, "ch_style"
            )
            # if res and layer_id:
            #     update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            #     print("sync to geoserver flag is updated")
            if res:
                layer_at_geoserver = True
        return layer_at_geoserver
