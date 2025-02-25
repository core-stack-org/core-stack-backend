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
)


@app.task(bind=True)
def tree_health_ccd_raster(self, state, district, block, start_year, end_year):
    print("Inside process Tree health ccd raster")
    ee_initialize()

    block_mws = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

    for year in range(start_year, end_year + 1):

        description = (
            "tree_health_ccd_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_" + str(year)
        )

        asset_id = get_gee_asset_path(state, district, block) + description

        if is_gee_asset_exists(asset_id):
            return

        # Define the path for CCD data based on start year
        if year == 2016 or year == 2022:
            ccd_path = "projects/ee-mtpictd/assets/harsh/ccd_" + str(year)
        else:
            ccd_path = "projects/ee-mtpictd/assets/dhruvi/modal_ccd_" + str(year)
        
        # Load and process the CCD Image Collection
        ccd_img = ee.ImageCollection(ccd_path) \
            .filterBounds(block_mws) \
            .mean() \
            .clip(block_mws)

        # Compute CCD statistics function
        def ccd_stats(feature):
            stats = ccd_img.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram().unweighted(),
                geometry=feature.geometry(),
                scale=25,
                maxPixels=1e10
            )
            pixel_counts = ee.Dictionary(stats.get('classification'))
            return feature.set({
                'ccd_0': pixel_counts.get('0.0', 0),  # Low Density
                'ccd_1': pixel_counts.get('1.0', 0),  # High Density
                'ccd_2': pixel_counts.get('2.0', 0)   # Missing Data
            })
        
        # Apply CCD statistics function to the feature collection
        block_mws = block_mws.map(ccd_stats)        
        # Export the result to GEE
        try:
            image_export_task = ee.batch.Export.image.toAsset(
                image=ccd_img.clip(block_mws.geometry()),
                description=description,
                assetId=asset_id,
                pyramidingPolicy={"predicted_label": "mode"},
                scale=25,
                maxPixels=1e13,
                crs="EPSG:4326",
            )
            image_export_task.start()
            print("Successfully started the CCD export task", image_export_task.status())

            task_id_list = check_task_status([image_export_task.status()["id"]])
            print("CCD task_id_list", task_id_list)

            #Sync image to Google Cloud Storage and Geoserver
            layer_name = (
                "tree_health_ccd_raster_"
                +valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_" + str(year)
            )
            task_id = sync_raster_to_gcs(ee.Image(asset_id), 25, layer_name)

            task_id_list = check_task_status([task_id])
            print("task_id_list sync to GCS", task_id_list)

            sync_raster_gcs_to_geoserver("ccd", layer_name, layer_name, "ccd_style")
        except Exception as e:
            print(f"Error occurred in running process_ccd task: {e}")
