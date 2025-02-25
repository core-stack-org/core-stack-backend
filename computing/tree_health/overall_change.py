import ee
from nrm_app.celery import app
from computing.utils import (
    sync_layer_to_geoserver,
)
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
def tree_health_overall_change_raster(self, state, district, block):
    ee_initialize()
    print("Inside process tree_health_overall_change_raster")
    palette = ['FF0000', 'FFA500', 'FFFFFF', '8AFF8A', '007500', 'DEE64C', 'DEE64C', '000000']


    block_mws = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    # Define description and asset ID for the task
    description = (
        "tree_health_overall_change_raster_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    # Check if asset already exists
    if is_gee_asset_exists(asset_id):
        return

    tree_change_path = "projects/ee-mtpictd/assets/dhruvi/overall_change_2017_2021"

    overall_change = ee.ImageCollection(tree_change_path) \
        .filterBounds(block_mws) \
        .mean() \
        .clip(block_mws)

    visualization = {
    'min': -2,
    'max': 5,
    'palette': palette
    }


    change_composite = overall_change.rename(['classification'])

    # Function to compute the change statistics per block
    def change_stats(feature):
        stats = change_composite.reduceRegion(
            reducer=ee.Reducer.frequencyHistogram().unweighted(),
            geometry=feature.geometry(),
            scale=25,
            maxPixels=1e10
        )
        pixelCounts = ee.Dictionary(stats.get('classification'))
        
        # Return feature with additional properties representing the change statistics
        return feature.set({
            'change_0': pixelCounts.get("-2.0", 0),  # Deforestation
            'change_1': pixelCounts.get("-1.0", 0),  # Degradation
            'change_2': pixelCounts.get("0.0", 0),   # No Change
            'change_3': pixelCounts.get("1.0", 0),   # Improvement
            'change_4': pixelCounts.get("2.0", 0),   # Afforestation
            'change_5': ee.Number(pixelCounts.get("3.0", 0)).add(ee.Number(pixelCounts.get("4.0", 0))),  # Partially Degraded
            'change_6': pixelCounts.get("5.0", 0)    # Missing Data
        })

    block_mws = block_mws.map(change_stats)
    print("Updated Feature Collection:")
    try:
        # Export the image to the Asset
        image_export_task = ee.batch.Export.image.toAsset(
            image=overall_change.clip(block_mws.geometry()),
            description=description,
            assetId=asset_id,
            pyramidingPolicy={"predicted_label": "mode"},
            scale=25,
            maxPixels=1e13,
            crs="EPSG:4326",
        )
        image_export_task.start()
        print("Successfully started the tree_health export task", image_export_task.status())

        # Check the task status
        task_id_list = check_task_status([image_export_task.status()["id"]])
        print("CH task_id_list", task_id_list)

        # Sync image to Google Cloud Storage (GCS)
        layer_name = (
            "tree_health_overall_change_raster_"
            +valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        task_id = sync_raster_to_gcs(ee.Image(asset_id), 25, layer_name)

        # Check the task status for GCS sync
        task_id_list = check_task_status([task_id])
        print("task_id_list sync to GCS", task_id_list)

        # Sync raster to GeoServer
        sync_raster_gcs_to_geoserver("tree_overall_ch", layer_name, layer_name, "tree_overall_ch_style")
    
    except Exception as e:
        print(f"Error occurred in running process_ch task: {e}")


