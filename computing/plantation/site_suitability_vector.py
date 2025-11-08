import ee

from gee_computing.models import GEEAccount
from utilities.constants import (
    GEE_PATH_PLANTATION,
    GEE_PATH_PLANTATION_HELPER,
    GEE_ASSET_PATH,
    GEE_HELPER_PATH,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    create_gee_dir,
    get_gee_dir_path,
    is_gee_asset_exists,
    valid_gee_text,
    make_asset_public,
    export_vector_asset_to_gee,
    build_gee_helper_paths,
)
from computing.plantation.utils.lulc_attachment import get_lulc_data
from computing.plantation.utils.ndvi_attachment import get_ndvi_data
from computing.plantation.utils.site_properties import get_site_properties
from .site_suitability_raster import get_pss
from utilities.logger import setup_logger
from ..utils import (
    create_chunk,
    merge_chunks,
    save_layer_info_to_db,
)

logger = setup_logger(__name__)


def check_site_suitability(
    roi,
    start_year,
    end_year,
    org=None,
    project=None,
    state=None,
    district=None,
    block=None,
    have_new_sites=False,
    gee_account_id=None,
):
    """
    Perform comprehensive site suitability analysis.

    Args:
        roi: Region of Interest feature collection
        org: Organization name
        project: Project object
        state: Geographic state
        district: Geographic district
        block: Geographic block
        start_year: Analysis start year
        end_year: Analysis end year
        have_new_sites: Boolean flag for if there are new sites in the ROI
        gee_account_id: GEE account ID

    Returns:
        Asset ID of the suitability vector
    """
    layer_id = None

    # Create a unique asset name for the suitability analysis
    gee_obj = GEEAccount.objects.get(pk=gee_account_id)

    if project:
        project_name = valid_gee_text(project.name)
        asset_name = f"{org}_{project_name}_site_suitability"
        path_list = [org, project_name]
        GEE_PATH = GEE_PATH_PLANTATION
        # GEE_HELPER = GEE_PATH_PLANTATION_HELPER
        state = project.state.state_name
        app_type = "PLANTATION"
    else:
        project_name = f"{valid_gee_text(district)}_{valid_gee_text(block)}"
        asset_name = f"{project_name}_site_suitability"
        path_list = [state, district, block]
        GEE_PATH = GEE_ASSET_PATH
        # GEE_HELPER = GEE_HELPER_PATH
        app_type = "MWS"

    GEE_HELPER = build_gee_helper_paths(app_type, gee_obj.helper_account.name)

    # Generate Plantation Site Suitability raster
    # Here, kept start_year=end_year-2 as in this site assessment script, we are taking into account the data of the latest three years only.
    pss_rasters_asset, is_default_profile = get_pss(
        roi=roi,
        start_year=end_year - 2,
        end_year=end_year,
        asset_name=asset_name,
        org=org,
        project=project,
        have_new_sites=have_new_sites,
        state=state,
        district=district,
        block=block,
    )

    print("is_default_profile= ", is_default_profile)

    # Prepare asset description and path
    description = asset_name + "_vector"
    asset_id = get_gee_dir_path(path_list, asset_path=GEE_PATH) + description

    # Remove existing asset if it exists
    if is_gee_asset_exists(asset_id):
        if have_new_sites:
            ee.data.deleteAsset(asset_id)
        else:
            if state and district and block:
                layer_id = save_layer_info_to_db(
                    state,
                    district,
                    block,
                    layer_name=asset_name,
                    asset_id=asset_id,
                    dataset_name="Site Suitability Vector",
                )
            return asset_id, asset_name, layer_id

    pss_rasters = ee.Image(pss_rasters_asset)

    if pss_rasters is None:
        raise Exception("Failed to calculate PSS rasters")

    if roi.size().getInfo() > 50:
        chunk_size = 30
        rois, descs = create_chunk(roi, description, chunk_size)

        ee_initialize(gee_obj.helper_account.id)
        create_gee_dir(path_list, gee_project_path=GEE_HELPER)

        tasks = []
        for i in range(len(rois)):
            chunk_asset_id = (
                get_gee_dir_path(path_list, asset_path=GEE_HELPER) + descs[i]
            )
            if is_gee_asset_exists(chunk_asset_id):
                ee.data.deleteAsset(chunk_asset_id)
            task_id = generate_vector(
                rois[i],
                start_year,
                end_year,
                pss_rasters,
                is_default_profile,
                descs[i],
                chunk_asset_id,
                state,
                project,
            )
            if task_id:
                tasks.append(task_id)

        check_task_status(tasks, 300)

        for desc in descs:
            make_asset_public(get_gee_dir_path(path_list, asset_path=GEE_HELPER) + desc)

        merge_task_id = merge_chunks(
            roi,
            path_list,
            description,
            chunk_size,
            chunk_asset_path=GEE_HELPER,
            merge_asset_path=GEE_PATH,
        )
        check_task_status([merge_task_id], 120)
    else:
        task_id = generate_vector(
            roi,
            start_year,
            end_year,
            pss_rasters,
            is_default_profile,
            description,
            asset_id,
            state,
            project,
        )
        if task_id:
            check_task_status([task_id], 120)
    if is_gee_asset_exists(asset_id):
        if state and district and block:
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=asset_name,
                asset_id=asset_id,
                dataset_name="Site Suitability Vector",
                misc={"start_year": start_year, "end_year": end_year},
            )
            print("save site suitability info at the gee level...")
        make_asset_public(asset_id)

    return asset_id, asset_name, layer_id


def generate_vector(
    roi,
    start_year,
    end_year,
    pss_rasters,
    is_default_profile,
    description,
    asset_id,
    state,
    project,
):

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
        if is_default_profile and not project:
            map_string = ee.Dictionary(
                {
                    1: "Very Good",
                    2: "Good",
                    3: "Moderate",
                    4: "Marginally Suitable",
                    5: "Unsuitable",
                }
            )
        else:
            map_string = ee.Dictionary({0: "Unsuitable", 1: "Suitable"})

        # Determine suitability based on threshold
        patch_average = ee.Number(patch_average)
        patch_val = patch_average.round().int()

        patch_string = map_string.get(patch_val)
        patch_conf = ee.Number(1).subtract(patch_average.subtract(patch_val).abs())

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
    suitability_vector = get_ndvi_data(
        suitability_vector, start_year, end_year, description, asset_id
    )
    logger.info("NDVI calculation completed")

    # Add Land Use/Land Cover data
    suitability_vector = get_lulc_data(suitability_vector, start_year, end_year)
    logger.info("LULC calculation completed")

    suitability_vector = get_site_properties(
        suitability_vector,
        state,
        end_year - 2,
        end_year,
    )  # start_year = end_year-2, as we don't want to overburden our vector asset and show the relevant data for 3 years only

    try:
        # Export annotated feature collection to Earth Engine
        task = export_vector_asset_to_gee(suitability_vector, description, asset_id)
        logger.info(f"Asset export task started. Asset path: {description}")
        return task
    except Exception as e:
        logger.exception("Exception in exporting suitability vector", e)
        return None
