import ee

from utilities.constants import (
    GEE_PATH_PLANTATION,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    gdf_to_ee_fc,
    create_gee_dir,
    get_gee_dir_path,
    is_gee_asset_exists,
    valid_gee_text,
    make_asset_public,
    get_gee_asset_path,
    export_vector_asset_to_gee,
)
from nrm_app.celery import app
from computing.plantation.utils.plantation_utils import combine_kmls
from utilities.logger import setup_logger
from .site_suitability_vector import check_site_suitability
from ..utils import (
    sync_fc_to_geoserver,
    update_layer_sync_status,
)
import geopandas as gpd
from projects.models import Project, AppType
from plantations.models import KMLFile

logger = setup_logger(__name__)


@app.task(bind=True)
def site_suitability(
    self, gee_account_id, project_id, start_year, end_year, state=None, district=None, block=None
):
    """
    Main task for site suitability analysis using Google Earth Engine.

    Args:
        project_id: Id of the specific project
        state: Geographic state for the analysis
        district: Geographic district
        block: Geographic block
        start_year: Beginning of the temporal analysis range
        end_year: End of the temporal analysis range
    """
    # Initialize Earth Engine connection for the project
    ee_initialize(gee_account_id)

    if project_id:
        project = Project.objects.get(
            id=project_id, app_type=AppType.PLANTATION, enabled=True
        )
        organization = project.organization.name
        project_name = project.name

        kml_files_obj = KMLFile.objects.filter(project=project)
        have_new_sites = False

        # Create a project-specific directory in Google Earth Engine
        create_gee_dir(
            [organization, project_name], gee_project_path=GEE_PATH_PLANTATION
        )

        # Construct a unique description and asset ID for the project
        description = valid_gee_text(organization) + "_" + valid_gee_text(project_name)
        asset_id = (
            get_gee_dir_path(
                [organization, project_name], asset_path=GEE_PATH_PLANTATION
            )
            + description
        )

        # Check if the asset already exists and handle accordingly
        if is_gee_asset_exists(asset_id):
            have_new_sites = merge_new_kmls(
                asset_id, description, project_name, kml_files_obj, have_new_sites
            )
        else:
            have_new_sites = True
            generate_project_roi(asset_id, description, project_name, kml_files_obj)
        print("have_new_sites", have_new_sites)

        roi = ee.FeatureCollection(asset_id)

        # Perform site suitability analysis
        vector_asset_id, asset_name, layer_id = check_site_suitability(
            roi,
            org=organization,
            project=project,
            start_year=start_year,
            end_year=end_year,
            have_new_sites=have_new_sites,
        )
        # Sync the results to GeoServer for visualization
        sync_suitability_to_geoserver(
            vector_asset_id, organization, asset_name, layer_id
        )
    else:
        roi = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )
        vector_asset_id, asset_name, layer_id = check_site_suitability(
            roi,
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
        )

        # Sync the results to GeoServer for visualization
        sync_suitability_to_geoserver(vector_asset_id, state, asset_name, layer_id)


def merge_new_kmls(asset_id, description, project_name, kml_files_obj, have_new_sites):
    """
    Merge new KML files into an existing Google Earth Engine asset.

    Args:
        asset_id: Existing asset identifier
        description: Project description
        project_name: Project name
        kml_files_obj: Queryset of KML_Files model
        have_new_sites: Flag to indicate if new sites are added
    """
    # Combine KML files into a GeoDataFrame
    gdf = combine_kmls(kml_files_obj)

    # Load existing ROI from Earth Engine
    roi = ee.FeatureCollection(asset_id)
    roi = gpd.GeoDataFrame.from_features(roi.getInfo())

    # Remove duplicate entries based on 'uid'
    gdf = gdf[~gdf["uid"].isin(roi["uid"])]

    # If new entries exist, update the asset
    if gdf.shape[0] > 0:
        ee.data.deleteAsset(asset_id)
        have_new_sites = True
        roi = gdf_to_ee_fc(roi)
        fc = gdf_to_ee_fc(gdf)
        asset = ee.FeatureCollection([roi, fc]).flatten()

        # Export updated feature collection to Earth Engine
        task = export_vector_asset_to_gee(asset, description, asset_id)
        check_task_status([task])
        logger.info("ROI for project: %s exported to GEE", project_name)
        if is_gee_asset_exists(asset_id):
            make_asset_public(asset_id)

    return have_new_sites


def generate_project_roi(asset_id, description, project_name, kml_files_obj):
    """
    Generate a new region of interest (ROI) for a project.

    Args:
        asset_id: Unique identifier for the asset
        description: Project description
        project_name: Project name
        kml_files_obj: Queryset of KML_Files model
    """
    # Combine KML files into a feature collection
    gdf = combine_kmls(kml_files_obj)
    fc = gdf_to_ee_fc(gdf)

    try:
        # Export feature collection to Earth Engine
        task = export_vector_asset_to_gee(fc, description, asset_id)
        check_task_status([task])
        make_asset_public(asset_id)

        logger.info("ROI for project: %s exported to GEE", project_name)
    except Exception as e:
        logger.exception("Exception in exporting asset: %s", e)


def sync_suitability_to_geoserver(asset_id, shp_folder, layer_name, layer_id):
    """
    Synchronize suitability analysis results to GeoServer.
    """

    try:
        # Load feature collection from Earth Engine
        fc = ee.FeatureCollection(asset_id)

        # Sync to GeoServer with a generated layer name
        res = sync_fc_to_geoserver(
            fc,
            shp_folder,
            layer_name,
            workspace="plantation",
        )
        logger.info("Suitability vector synced to geoserver: %s", res)
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag is updated")
    except Exception as e:
        logger.exception("Exception in syncing suitability vector to geoserver", e)
