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
    ee_initialize(project="nrm_work")

    create_gee_dir([organization, project], gee_project_path=GEE_PATH_PLANTATION)

    description = organization + "_" + project
    asset_id = (
            get_gee_dir_path([organization, project], asset_path=GEE_PATH_PLANTATION)
            + description
    )
    if is_gee_asset_exists(asset_id):
        merge_new_kmls(asset_id, description, project)
    else:
        generate_project_roi(asset_id, description, project)

    roi = ee.FeatureCollection(asset_id)
    vector_asset_id = check_site_suitability(
        roi, organization, project, state, start_year, end_year
    )

    sync_suitability_to_geoserver(vector_asset_id, organization, project)


def merge_new_kmls(asset_id, description, project):
    path = "/home/aman/Downloads/Farmer List and KML/Farmer List and KML/1st phase KML FILE Mandal wise Infosys Donors/Vajrakaruru/Venkatampalli Infosys"  # TODO Change it
    gdf = combine_kmls(path)

    roi = ee.FeatureCollection(asset_id)
    roi = gpd.GeoDataFrame.from_features(roi.getInfo())

    # Remove rows from gdf which already exists in the asset
    gdf = gdf[~gdf["uid"].isin(roi["uid"])]

    if gdf.shape[0] > 0:
        ee.data.deleteAsset(asset_id)
        roi = gdf_to_ee_fc(roi)
        fc = gdf_to_ee_fc(gdf)
        asset = ee.FeatureCollection([roi, fc]).flatten()

        try:
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
    path = "/home/aman/Downloads/Farmer List and KML/Farmer List and KML/1st phase KML FILE Mandal wise Infosys Donors/Vajrakaruru/VPP Thanda infosys"  # TODO Change it
    gdf = combine_kmls(path)
    fc = gdf_to_ee_fc(gdf)

    try:
        task = ee.batch.Export.table.toAsset(
            **{"collection": fc, "description": description, "assetId": asset_id}
        )
        task.start()

        check_task_status([task.status()["id"]])
        logger.info("ROI for project: %s exported to GEE", project)
    except Exception as e:
        logger.exception("Exception in exporting asset: %s", e)


def check_site_suitability(roi, org, project, state, start_year, end_year):
    asset_name = "site_suitability_" + project

    # Generate Plantation Site Suitability raster
    pss_rasters_asset = get_pss(roi, org, project, state, asset_name)

    description = asset_name + "_vector"
    asset_id = (
            get_gee_dir_path([org, project], asset_path=GEE_PATH_PLANTATION) + description
    )

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

        score_clip = pss_rasters.select(["final_score"]).clip(feature.geometry())
        patch_average = score_clip.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e13,
        ).get("final_score")

        patch_average = ee.Algorithms.If(
            ee.Algorithms.IsEqual(patch_average, None), 0, patch_average
        )

        patch_average = ee.Number(patch_average)
        patch_val = patch_average.gte(0.5).int()
        patch_string = ee.Dictionary({0: "Unsuitable", 1: "Suitable"}).get(patch_val)
        patch_conf = ee.Number(0).eq(patch_val).subtract(patch_average).abs()

        # Set final properties to the feature
        return feature.set(
            {
                "patch_score": patch_val,
                "patch_suitability": patch_string,
                "patch_conf": patch_conf,  # patch_confidence (patch_score-actual value)
                # "GTscore": "-",  # Ground Truth Score
                # "comments": "-",
            }
        )

    # Process features
    suitability_vector = roi.map(get_max_val)

    suitability_vector = get_ndvi_data(suitability_vector, start_year, end_year)
    logger.info("NDVI calculation completed")

    # suitability_vector = suitability_vector.map(get_lulc)
    suitability_vector = get_lulc_data(suitability_vector, start_year, end_year)
    logger.info("LULC calculation completed")

    # Select properties for final output
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
        # Export as Earth Engine Asset
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
    try:
        # Syncing vector asset to geoserver
        fc = ee.FeatureCollection(asset_id)
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
