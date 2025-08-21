from computing.utils import sync_fc_to_geoserver, sync_project_fc_to_geoserver
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import ee_initialize, get_gee_asset_path, get_gee_dir_path
import ee
from nrm_app.settings import PAN_INDIA_CATCHMENT_AREA, PAN_INDIA_STREAM_ORDER
from waterrejuvenation.utils import delete_asset_on_GEE, wait_for_task_completion



def compute_max_stream_order_and_catchment_for_swb(swb_layer_asset,  proj_id):
    proj_obj = Project.objects.get(pk = proj_id)
    geoserver_layer_name = 'WaterRejapp-' + str(proj_obj.name) + "_" + str(proj_obj.id)
    ee_initialize()
    swb = ee.FeatureCollection(swb_layer_asset)  # Replace with your asset ID

    # Load the raster image (must contain 'SR_B1' or your band)

    catchemnt_area_raster = ee.Image(PAN_INDIA_CATCHMENT_AREA)  # Replace with your image ID
    catchment_band = catchemnt_area_raster.select('b1')  # Adjust based on the band you need
    stream_order_raster = ee.Image(PAN_INDIA_STREAM_ORDER)
    stream_order_band = stream_order_raster.select('b1')

    # Function to compute max B1 inside each feature
    def make_compute_max(so_band, cm_band, band_name='b1'):
        def compute_max(feature):
            max_val_so = so_band.reduceRegion(
                reducer=ee.Reducer.max(),
                geometry=feature.geometry(),
                scale=30,
                maxPixels=1e13
            ).get(band_name)

            max_val_cm =cm_band.reduceRegion(
                reducer=ee.Reducer.max(),
                geometry=feature.geometry(),
                scale=30,
                maxPixels=1e13
            ).get(band_name)

            return feature.set({
                'max_catchment_area': max_val_cm,
                'max_stream_order': max_val_so
            })

        return compute_max
    # Apply the function to each polygon in the collection
    compute_max = make_compute_max(stream_order_band, catchment_band)
    swb_with_max = swb.map(compute_max)
    asset_name = swb_layer_asset + 'cm_so'
    delete_asset_on_GEE(asset_name)
    # Optional: export to Google Drive as CSV
    task = ee.batch.Export.table.toAsset(
        collection=swb_with_max,
        description='Export_SWB_Max_catchment_area',
        assetId=asset_name  # 🔁 Your desired asset path
    )
    task.start()
    wait_for_task_completion(task)
    sync_project_fc_to_geoserver(swb_with_max, proj_obj.name, geoserver_layer_name, 'waterrej')
    return asset_name