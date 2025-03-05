import ee

from utilities.constants import GEE_PATH_PLANTATION
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    gdf_to_ee_fc,
    create_gee_dir,
    get_gee_dir_path,
    is_gee_asset_exists,
    valid_gee_text,
)
from nrm_app.celery import app
from .lulc_attachment import get_lulc_data
from .ndvi_attachment import get_ndvi_data
from .site_suitability_raster import get_pss
from .plantation_utils import combine_kmls
from utilities.logger import setup_logger
from ..utils import sync_fc_to_geoserver
import geopandas as gpd

logger = setup_logger(__name__)


@app.task(bind=True)
def site_suitability(self, organization, project, state, start_year, end_year):
    """
    Main task for site suitability analysis using Google Earth Engine.

    Args:
        organization: Name of the organization conducting the analysis
        project: Name of the specific project
        state: Geographic state for the analysis
        start_year: Beginning of the temporal analysis range
        end_year: End of the temporal analysis range
    """

    # Initialize Earth Engine connection for the project
    ee_initialize(project="nrm_work")

    # Create a project-specific directory in Google Earth Engine
    create_gee_dir([organization, project], gee_project_path=GEE_PATH_PLANTATION)

    # Construct a unique description and asset ID for the project
    description = organization + "_" + project
    asset_id = (
        get_gee_dir_path([organization, project], asset_path=GEE_PATH_PLANTATION)
        + description
    )

    # Check if the asset already exists and handle accordingly
    if is_gee_asset_exists(asset_id):
        merge_new_kmls(asset_id, description, project)
    else:
        generate_project_roi(asset_id, description, project)

    # Load the region of interest (ROI) feature collection
    roi = ee.FeatureCollection(asset_id)

    # Perform site suitability analysis
    vector_asset_id = check_site_suitability(
        roi, organization, project, state, start_year, end_year
    )

    # Sync the results to GeoServer for visualization
    sync_suitability_to_geoserver(vector_asset_id, organization, project)


def merge_new_kmls(asset_id, description, project):
    """
    Merge new KML files into an existing Google Earth Engine asset.

    Args:
        asset_id: Existing asset identifier
        description: Project description
        project: Project name
    """
    # TODO: Hardcoded path needs to be replaced with a dynamic method
    path = "/home/aman/Downloads/Farmer List and KML/Farmer List and KML/1st phase KML FILE Mandal wise Infosys Donors/Vajrakaruru/Venkatampalli Infosys"

    # Combine KML files into a GeoDataFrame
    gdf = combine_kmls(path)

    # Load existing ROI from Earth Engine
    roi = ee.FeatureCollection(asset_id)
    roi = gpd.GeoDataFrame.from_features(roi.getInfo())

    # Remove duplicate entries based on 'uid'
    gdf = gdf[~gdf["uid"].isin(roi["uid"])]

    # If new entries exist, update the asset
    if gdf.shape[0] > 0:
        ee.data.deleteAsset(asset_id)
        roi = gdf_to_ee_fc(roi)
        fc = gdf_to_ee_fc(gdf)
        asset = ee.FeatureCollection([roi, fc]).flatten()

        try:
            # Export updated feature collection to Earth Engine
            task = ee.batch.Export.table.toAsset(
                **{
                    "collection": asset,
                    "description": description,
                    "assetId": asset_id,
                }
            )
            task.start()
            check_task_status([task.status()["id"]])
            logger.info("ROI for project: %s exported to GEE", project)
        except Exception as e:
            logger.exception("Exception in exporting asset: %s", e)


def generate_project_roi(asset_id, description, project):
    """
    Generate a new region of interest (ROI) for a project.

    Args:
        asset_id: Unique identifier for the asset
        description: Project description
        project: Project name
    """
    # TODO: Hardcoded path needs to be replaced with a dynamic method
    path = "/home/aman/Downloads/Farmer List and KML/Farmer List and KML/1st phase KML FILE Mandal wise Infosys Donors/Vajrakaruru/VPP Thanda infosys"

    # Combine KML files into a feature collection
    gdf = combine_kmls(path)
    fc = gdf_to_ee_fc(gdf)

    try:
        # Export feature collection to Earth Engine
        task = ee.batch.Export.table.toAsset(
            **{"collection": fc, "description": description, "assetId": asset_id}
        )
        task.start()

        check_task_status([task.status()["id"]])
        logger.info("ROI for project: %s exported to GEE", project)
    except Exception as e:
        logger.exception("Exception in exporting asset: %s", e)


def check_site_suitability(roi, org, project, state, start_year, end_year):
    """
    Perform comprehensive site suitability analysis.

    Args:
        roi: Region of Interest feature collection
        org: Organization name
        project: Project name
        state: Geographic state
        start_year: Analysis start year
        end_year: Analysis end year

    Returns:
        Asset ID of the suitability vector
    """

    # Create a unique asset name for the suitability analysis
    asset_name = "site_suitability_" + project

    # Generate Plantation Site Suitability raster
    pss_rasters_asset = get_pss(roi, org, project, state, asset_name)

    # Prepare asset description and path
    description = asset_name + "_vector"
    asset_id = (
        get_gee_dir_path([org, project], asset_path=GEE_PATH_PLANTATION) + description
    )

    # Remove existing asset if it exists
    if is_gee_asset_exists(asset_id):
        ee.data.deleteAsset(asset_id)

    pss_rasters = ee.Image(pss_rasters_asset)

    if pss_rasters is None:
        raise Exception("Failed to calculate PSS rasters")

    def get_max_val(feature):
        """Calculate maximum value and suitability for a feature."""
        if not pss_rasters or not pss_rasters.bandNames().size().gt(0):
            logger.exception("Warning: pss_rasters has no bands")
            return feature

        # Calculate score by clipping raster to feature geometry
        score_clip = pss_rasters.select(["final_score"]).clip(feature.geometry())
        patch_average = score_clip.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e13,
        ).get("final_score")

        # Handle potential None values
        patch_average = ee.Algorithms.If(
            ee.Algorithms.IsEqual(patch_average, None), 0, patch_average
        )

        # Determine suitability based on threshold
        patch_average = ee.Number(patch_average)
        patch_val = patch_average.gte(0.5).int()
        patch_string = ee.Dictionary({0: "Unsuitable", 1: "Suitable"}).get(patch_val)
        patch_conf = ee.Number(0).eq(patch_val).subtract(patch_average).abs()

        # Add suitability metrics to feature properties
        return feature.set(
            {
                "patch_score": patch_val,
                "patch_suitability": patch_string,
                "patch_conf": patch_conf,  # patch_confidence (patch_score-actual value)
                # "GTscore": "-",  # Ground Truth Score
                # "comments": "-",
            }
        )

    # Apply suitability analysis to each feature in ROI
    suitability_vector = roi.map(get_max_val)

    # Add NDVI data for the specified time range
    suitability_vector = get_ndvi_data(suitability_vector, start_year, end_year)
    logger.info("NDVI calculation completed")

    # Add Land Use/Land Cover data
    suitability_vector = get_lulc_data(suitability_vector, start_year, end_year)
    logger.info("LULC calculation completed")

    # Select and prepare final output properties
    final_annotated = suitability_vector.select(
        [
            "Name",
            "uid",
            "source",
            "patch_score",
            "patch_conf",
            "patch_suitability",
            # "GTscore",
            # "comments",
            "LULC",
            "NDVI_values",
            "NDVI_dates",
        ]
    )

    try:
        # Export annotated feature collection to Earth Engine
        task = ee.batch.Export.table.toAsset(
            collection=final_annotated,
            description=description,
            assetId=asset_id,
        )
        task.start()

        logger.info(f"Asset export task started. Asset path: {description}")
        check_task_status([task.status()["id"]])
        logger.info("Suitability vector for project=%s exported to GEE", project)
        return asset_id
    except Exception as e:
        logger.exception("Exception in exporting suitability vector", e)


def sync_suitability_to_geoserver(asset_id, organization, project):
    """
    Synchronize suitability analysis results to GeoServer.

    Args:
        asset_id: Earth Engine asset ID
        organization: Organization name
        project: Project name
    """

    try:
        # Load feature collection from Earth Engine
        fc = ee.FeatureCollection(asset_id)

        # Sync to GeoServer with a generated layer name
        res = sync_fc_to_geoserver(
            fc,
            organization,
            valid_gee_text(organization.lower())
            + "_"
            + valid_gee_text(project.lower())
            + "_suitability",
            workspace="plantation",
        )
        logger.info("Suitability vector synced to geoserver: %s", res)
    except Exception as e:
        logger.exception("Exception in syncing suitability vector to geoserver", e)
