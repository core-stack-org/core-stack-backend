import ee
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    get_gee_dir_path,
    export_vector_asset_to_gee,
    sync_vector_to_geoserver,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status


# Celery task to vectorize forest structure raster
@app.task(bind=True)
def forest_structure_vector(
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
    """
    Vectorize annual forest structure rasters into field/MWS-level polygons.
    
    Each polygon includes:
    - Year
    - Forest structure class (dense, open, scrub)
    - Area (ha)
    - Mean NDVI value
    
    Args:
        state, district, block: Administrative boundaries
        roi: Region of interest (FeatureCollection)
        start_year, end_year: Time period for annual vectorization
        app_type: 'MWS' or 'AoI'
        gee_account_id: Google Earth Engine account ID
    
    Returns:
        Dictionary with status and vector asset path
    """
    print("Inside process Forest Structure vectorization")

    try:
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
            )

        # Convert roi to geometry
        roi_geom = roi.geometry() if isinstance(roi, ee.FeatureCollection) else roi

        # Load the raster asset
        raster_asset_path = get_gee_asset_path(
            asset_folder_list,
            asset_name=f"forest_structure_{asset_suffix}",
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )

        forest_structure_raster = ee.Image(raster_asset_path)

        # Vectorize using reduceToVectors
        vectors = forest_structure_raster.select("forest_structure").reduceToVectors(
            geometry=roi_geom,
            scale=30,
            geometryType="polygon",
            eightConnected=False,
        )

        # Add attributes to vectors
        def add_attributes(feature):
            """Add area and mean NDVI to each polygon"""
            geom = feature.geometry()
            class_value = feature.get("label")

            # Map class values to names
            class_name = ee.Algorithms.Describe(
                ee.Conditional(
                    ee.Eq(class_value, 2),
                    "dense",
                    ee.Conditional(ee.Eq(class_value, 1), "open", "scrub"),
                )
            )

            # Calculate area in hectares (30m pixels, so each pixel = 0.09 ha)
            area_ha = geom.area().divide(10000)

            # Calculate mean NDVI for the polygon
            mean_ndvi = (
                forest_structure_raster.select("NDVI")
                .reduceRegion(
                    reducer=ee.Reducer.mean(), geometry=geom, scale=30
                )
                .get("NDVI")
            )

            return feature.set(
                {
                    "forest_class": class_name,
                    "area_ha": area_ha,
                    "mean_ndvi": mean_ndvi,
                    "year": 2024,  # This should be parameterized for multi-year support
                    "created_date": ee.Date.now().format("YYYY-MM-dd"),
                }
            )

        # Apply attribute function
        vectors_with_attributes = vectors.map(add_attributes)

        # Get vector asset path
        vector_asset_path = get_gee_asset_path(
            asset_folder_list,
            asset_name=f"forest_structure_vector_{asset_suffix}",
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )

        # Check if asset already exists
        if is_gee_asset_exists(vector_asset_path):
            print(f"Vector asset {vector_asset_path} already exists, skipping...")
            return {
                "status": "Vector asset already exists",
                "asset_path": vector_asset_path,
            }

        # Export vectors to GEE asset
        export_task = export_vector_asset_to_gee(
            vectors_with_attributes,
            vector_asset_path,
            description=f"Forest Structure Vectors {state} {district} {block}",
        )

        # Save vector layer info to database
        task_info = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=f"forest_structure_vector_{asset_suffix}",
            algorithm="NDVI_threshold_vectorization",
            dataset_name="Forest Structure Vectors",
            gee_asset_path=vector_asset_path,
            layer_type="vector",
        )

        print(f"Forest structure vector export task initiated: {export_task}")
        return {
            "status": "Task initiated",
            "task_id": export_task,
            "asset_path": vector_asset_path,
        }

    except Exception as e:
        print(f"Exception in forest_structure_vector: {e}")
        raise
