import ee

from plantations.models import PlantationProfile
from utilities.constants import (
    GEE_PATH_PLANTATION,
    GEE_ASSET_PATH,
    GEE_DATASET_PATH,
)
from utilities.gee_utils import (
    is_gee_asset_exists,
    harmonize_band_types,
    get_gee_dir_path,
    check_task_status,
    valid_gee_text,
    sync_raster_to_gcs,
    sync_raster_gcs_to_geoserver,
    make_asset_public,
    export_raster_asset_to_gee,
)
from .harmonized_ndvi import Get_Padded_NDVI_TS_Image
from .plantation_utils import dataset_paths
from utilities.logger import setup_logger

logger = setup_logger(__name__)


def get_pss(
    roi,
    start_year,
    end_year,
    asset_name,
    org=None,
    project=None,
    have_new_sites=False,
    state=None,
    district=None,
    block=None,
):
    """
    Generate Plantation Site Suitability (PSS) raster.

    Comprehensive multi-layer analysis that combines:
    1. Climate
    2. Soil
    3. Socioeconomic
    4. Ecological
    5. Topographical factors

    Args:
        roi: Region of Interest (Feature Collection)
        org: Organization name
        project: Project object
        state: Geographic state
        district: Geographic district
        block: Geographic block
        asset_name: Name for the output asset
        start_year: Start year
        end_year: End year
        have_new_sites: Boolean flag for if there are new sites in the ROI


    Returns:
        Asset ID of the generated suitability raster
    """
    # Initialize base image with a constant value of 1
    all_layers = ee.Image(1)
    is_default_profile = True

    default_profile = PlantationProfile.objects.get(profile_id=1)
    project_weights = default_profile.config_weight
    project_variables = default_profile.config_variables

    description = f"{asset_name}_raster"

    if project:
        project_name = valid_gee_text(project.name)
        state = project.state.state_name.lower()

        plantation_profile = PlantationProfile.objects.filter(project=project)

        if plantation_profile.exists():
            is_default_profile = False
            plantation_profile = plantation_profile[0]
            project_weights = {**project_weights, **plantation_profile.config_weight}
            project_variables = {
                **project_variables,
                **plantation_profile.config_variables,
            }

        # Prepare asset description and path
        asset_id = (
            get_gee_dir_path([org, project_name], asset_path=GEE_PATH_PLANTATION)
            + description
        )
    else:
        asset_id = (
            get_gee_dir_path([state, district, block], asset_path=GEE_ASSET_PATH)
            + description
        )

    # Remove existing asset if it exists to prevent conflicts
    if is_gee_asset_exists(asset_id):
        if have_new_sites:
            ee.data.deleteAsset(asset_id)
        else:
            return asset_id, is_default_profile

    # Define analysis layers and their processing functions
    # Each subsequent section follows a similar pattern:
    # 1. Define variables
    # 2. Set weights
    # 3. Create sub-layers
    # 4. Combine layers with weighted expression
    # 5. Add to all_layers

    ############### Climate Layer ################
    climate_variables = [
        "annualPrecipitation",
        "meanAnnualTemperature",
        "aridityIndex",
        "referenceEvapoTranspiration",
    ]

    # Create climate sub-layers by classifying each variable
    climate_sub_layers = create_classification(
        project_variables, climate_variables, roi, state, start_year, end_year
    )

    # Prepare weighted expression for climate layer
    climate_expr_vars = {
        "annualPrecip": climate_sub_layers.select(["annualPrecipitation"]),
        "meanAnnualTemp": climate_sub_layers.select(["meanAnnualTemperature"]),
        "aridityIndex": climate_sub_layers.select(["aridityIndex"]),
        "refEvapoTransp": climate_sub_layers.select(["referenceEvapoTranspiration"]),
    }

    climate_expr_weights = {
        "w1": ee.Number(project_weights["annualPrecipitation"]),
        "w2": ee.Number(project_weights["meanAnnualTemperature"]),
        "w3": ee.Number(project_weights["aridityIndex"]),
        "w4": ee.Number(project_weights["referenceEvapoTranspiration"]),
    }

    # Combine climate variables with weighted expression
    climate_layer = climate_sub_layers.expression(
        "w1 * annualPrecip + w2 * meanAnnualTemp + w3 * aridityIndex + w4 * refEvapoTransp",
        {**climate_expr_vars, **climate_expr_weights},
    ).rename("Climate")

    logger.info("------------Climate layer done---------------")

    all_layers = all_layers.addBands(climate_layer)

    ############### Soil Layer ################

    # First we take weighted mean of topsoilNutrients, subsoilNutrients and
    # rootingCondition, then combine them along with AWC and drainage in the final layer
    soil_variables = [
        "topsoilPH",
        "topsoilCEC",
        "topsoilOC",
        "topsoilTexture",
        "topsoilBD",
        "subsoilPH",
        "subsoilCEC",
        "subsoilOC",
        "subsoilTexture",
        "subsoilBD",
        "drainage",
        "AWC",
    ]

    soil_sub_layers = create_classification(
        project_variables, soil_variables, roi, state, start_year, end_year
    )

    # Topsoil Nutrient Layer
    topsoil_nutrient_layer = soil_sub_layers.expression(
        "w1 * topsoilPH + w2 * topsoilOC + w3 * topsoilCEC + w4 * topsoilTexture",
        {
            "topsoilPH": soil_sub_layers.select(["topsoilPH"]),
            "w1": ee.Number(project_weights["tnTopsoilPH"]),
            "topsoilOC": soil_sub_layers.select(["topsoilOC"]),
            "w2": ee.Number(project_weights["tnTopsoilOC"]),
            "topsoilCEC": soil_sub_layers.select(["topsoilCEC"]),
            "w3": ee.Number(project_weights["tnTopsoilCEC"]),
            "topsoilTexture": soil_sub_layers.select(["topsoilTexture"]),
            "w4": ee.Number(project_weights["tnTopsoilTexture"]),
        },
    )
    topsoil_nutrient_layer = topsoil_nutrient_layer.rename("topsoilNutrient")
    logger.info("--------------topsoil_nutrient_layer------------")
    soil_sub_layers = soil_sub_layers.addBands(topsoil_nutrient_layer)

    ################# Subsoil Nutrient Layer ######################
    subsoil_nutrient_layer = soil_sub_layers.expression(
        "w1 * subsoilPH + w2 * subsoilOC + w3 * subsoilCEC + w4 * subsoilTexture",
        {
            "subsoilPH": soil_sub_layers.select(["subsoilPH"]),
            "w1": ee.Number(project_weights["snSubsoilPH"]),
            "subsoilOC": soil_sub_layers.select(["subsoilOC"]),
            "w2": ee.Number(project_weights["snSubsoilOC"]),
            "subsoilCEC": soil_sub_layers.select(["subsoilCEC"]),
            "w3": ee.Number(project_weights["snSubsoilCEC"]),
            "subsoilTexture": soil_sub_layers.select(["subsoilTexture"]),
            "w4": ee.Number(project_weights["snSubsoilTexture"]),
        },
    )
    subsoil_nutrient_layer = subsoil_nutrient_layer.rename("subsoilNutrient")
    soil_sub_layers = soil_sub_layers.addBands(subsoil_nutrient_layer)

    ################ Rooting Condition Layer  ##########################
    rooting_condition_layer = soil_sub_layers.expression(
        "w1 * topsoilPH + w2 * topsoilBD + w3 * subsoilPH + w4 * subsoilBD",
        {
            "topsoilPH": soil_sub_layers.select(["topsoilPH"]),
            "w1": ee.Number(project_weights["rcTopsoilPH"]),
            "topsoilBD": soil_sub_layers.select(["topsoilBD"]),
            "w2": ee.Number(project_weights["rcTopsoilBD"]),
            "subsoilPH": soil_sub_layers.select(["subsoilPH"]),
            "w3": ee.Number(project_weights["rcSubsoilPH"]),
            "subsoilBD": soil_sub_layers.select(["subsoilBD"]),
            "w4": ee.Number(project_weights["rcSubsoilBD"]),
        },
    )
    rooting_condition_layer = rooting_condition_layer.rename("rootingCondition")
    logger.info("---------------rooting_condition_layer-----------------------")
    soil_sub_layers = soil_sub_layers.addBands(rooting_condition_layer)

    ##################### Final Soil Layer###########################

    soil_layer = soil_sub_layers.expression(
        "w1 * topsoilNutrient + w2 * subsoilNutrient + w3 * rootingCondition + w4 * drainage + w5 * AWC",
        {
            "topsoilNutrient": soil_sub_layers.select(["topsoilNutrient"]),
            "w1": ee.Number(project_weights["topsoilNutrient"]),
            "subsoilNutrient": soil_sub_layers.select(["subsoilNutrient"]),
            "w2": ee.Number(project_weights["subsoilNutrient"]),
            "rootingCondition": soil_sub_layers.select(["rootingCondition"]),
            "w3": ee.Number(project_weights["rootingCondition"]),
            "drainage": soil_sub_layers.select(["drainage"]),
            "w4": ee.Number(project_weights["drainage"]),
            "AWC": soil_sub_layers.select(["AWC"]),
            "w5": ee.Number(project_weights["AWC"]),
        },
    )
    soil_layer = soil_layer.rename("Soil")
    logger.info("------------soil_layer----------------")
    all_layers = all_layers.addBands(soil_layer)

    ######################## Socioeconomic Layer  ############################
    socioeconomic_variables = ["distToRoad", "distToDrainage", "distToSettlements"]

    socioeconomic_sub_layers = create_classification(
        project_variables, socioeconomic_variables, roi, state, start_year, end_year
    )

    socioeconomic_layer = socioeconomic_sub_layers.expression(
        "w1 * distToRoad + w2 * distToDrainage + w3 * distToSettlements",
        {
            "distToRoad": socioeconomic_sub_layers.select(["distToRoad"]),
            "w1": ee.Number(project_weights["distToRoad"]),
            "distToDrainage": socioeconomic_sub_layers.select(["distToDrainage"]),
            "w2": ee.Number(project_weights["distToDrainage"]),
            "distToSettlements": socioeconomic_sub_layers.select(["distToSettlements"]),
            "w3": ee.Number(project_weights["distToSettlements"]),
        },
    )

    socioeconomic_layer = socioeconomic_layer.rename("Socioeconomic")
    logger.info("-------------------socioeconomic_layer---------------")
    all_layers = all_layers.addBands(socioeconomic_layer)

    ##################### Ecology Layer  ############################
    ecology_variables = ["NDVI", "LULC"]

    ecology_sub_layers = create_classification(
        project_variables, ecology_variables, roi, state, start_year, end_year
    )

    ecology_layer = ecology_sub_layers.expression(
        "w1 * NDVI + w2 * LULC",
        {
            "NDVI": ecology_sub_layers.select(["NDVI"]),
            "w1": ee.Number(project_weights["NDVI"]),
            "LULC": ecology_sub_layers.select(["LULC"]),
            "w2": ee.Number(project_weights["LULC"]),
        },
    )

    ecology_layer = ecology_layer.rename("Ecology")
    all_layers = all_layers.addBands(ecology_layer)

    ########## Topography Layer ###########################
    topography_variables = ["elevation", "slope", "aspect"]

    topography_sub_layers = create_classification(
        project_variables, topography_variables, roi, state, start_year, end_year
    )

    topography_layer = topography_sub_layers.expression(
        "w1 * elevation + w2 * slope + w3 * aspect",
        {
            "elevation": topography_sub_layers.select(["elevation"]),
            "w1": ee.Number(project_weights["elevation"]),
            "slope": topography_sub_layers.select(["slope"]),
            "w2": ee.Number(project_weights["slope"]),
            "aspect": topography_sub_layers.select(["aspect"]),
            "w3": ee.Number(project_weights["aspect"]),
        },
    )

    topography_layer = topography_layer.rename("Topography")
    all_layers = all_layers.addBands(topography_layer)

    ############### Final layer calculation  ######################
    final_layer = all_layers.expression(
        "w1 * Climate + w2 * Soil + w3 * Topography + w4 * Ecology + w5 * Socioeconomic",
        {
            "Climate": all_layers.select(["Climate"]),
            "w1": ee.Number(project_weights["Climate"]),
            "Soil": all_layers.select(["Soil"]),
            "w2": ee.Number(project_weights["Soil"]),
            "Topography": all_layers.select(["Topography"]),
            "w3": ee.Number(project_weights["Topography"]),
            "Ecology": all_layers.select(["Ecology"]),
            "w4": ee.Number(project_weights["Ecology"]),
            "Socioeconomic": all_layers.select(["Socioeconomic"]),
            "w5": ee.Number(project_weights["Socioeconomic"]),
        },
    )

    final_layer = final_layer.rename("Final")

    # This rounding is specific to a binary score output
    # Round off Plantation Score to 0 or 1 on each pixel and mask
    if is_default_profile:
        # For five suitability classes
        final_plantation_score = (
            ee.Image(0)
            .where(final_layer.lt(1.5), 1)
            .where(final_layer.gte(1.5).And(final_layer.lt(2.5)), 2)
            .where(final_layer.gte(2.5).And(final_layer.lt(3.5)), 3)
            .where(final_layer.gte(3.5).And(final_layer.lt(4.5)), 4)
            .where(final_layer.gte(4.5), 5)
            .clip(roi.geometry())
        )
    else:
        # Pixels with score <= 0.5 are considered unsuitable
        final_plantation_score = ee.Image(1).where(final_layer.lte(0.5), 0)

    # Apply LULC (Land Use/Land Cover) mask to filter suitable areas
    # Considers specific LULC classes as potentially suitable
    # Classes to be masked for in IndiaSat LULC v3 - 5 (Croplands), 6 (Trees/forests),
    # 7 (Barren lands), 8 (Single Kharif Cropping), 9 (Single Non Kharif Cropping),
    # 10 (Double Cropping), 11 (Triple Cropping), 12 (Shrub and Scrub)
    lulc = get_dataset("LULC", state, roi, start_year, end_year)
    if is_default_profile:
        lulc_mask = lulc.eq(5).Or(lulc.gte(7))
    else:
        lulc_mask = lulc.gte(5)

    # Final score with LULC masking and clipping to ROI
    final_plantation_score = (
        final_plantation_score.updateMask(lulc_mask)
        .clip(roi.geometry())
        .rename("final_score")
    )  # Changed to a valid band name

    logger.info("---------------final_plantation_score--------------")
    all_layers = all_layers.addBands(final_plantation_score)

    # Harmonize band types to ensure consistency
    all_layers = harmonize_band_types(all_layers, "Float")

    # Export to GEE asset
    try:
        scale = 30
        task_id = export_raster_asset_to_gee(
            image=all_layers.clip(roi.geometry()),
            description=description,
            asset_id=asset_id,
            scale=scale,
            region=roi.geometry(),
        )
        check_task_status([task_id])

        make_asset_public(asset_id)

        sync_to_gcs_geoserver(asset_id, description, scale)
        return asset_id, is_default_profile

    except Exception as e:
        logger.exception(f"Export failed: {str(e)}")
        raise


# Helper functions for dataset retrieval and classification
def get_dataset(variable, state, roi, start_year, end_year):
    """
    Retrieve and preprocess geospatial dataset for a specific variable.

    Args:
        variable (str): The type of geospatial data to retrieve.
        state (str): The state for which data is being retrieved.
        roi (FeatureCollection): Region of interest
        start_year: Start year
        end_year: End year
    Returns:
        ee.Image: Processed geospatial dataset for the specified variable.

    Supports different processing for various variables like:
    - Slope and Aspect calculation
    - LULC and NDVI retrieval
    - Distance transformations for roads, drainage, settlements
    """

    # List of supported variables with special processing requirements
    diff_variables = [
        "distToRoad",
        "distToDrainage",
        "distToSettlements",
        "slope",
        "aspect",
        "NDVI",
        "LULC",
    ]

    if variable not in diff_variables:
        return ee.Image(dataset_paths[variable])

    # Terrain-related variables (slope and aspect)
    if variable in ["slope", "aspect"]:
        dataset = ee.Image(dataset_paths[variable])
        return (
            ee.Terrain.slope(dataset)
            if variable == "slope"
            else ee.Terrain.aspect(dataset)
        )

    # Land Use and Land Cover (LULC)
    if variable == "LULC":
        s_year = start_year
        lulc_years = []
        while s_year <= end_year:
            asset_id = f"{GEE_DATASET_PATH}/LULC_v3_river_basin/pan_india_lulc_v3_{s_year}_{str(s_year+1)}"
            lulc_img = (
                ee.Image(asset_id).select(["predicted_label"]).clip(roi.geometry())
            )
            lulc_years.append(lulc_img)
            s_year += 1
        return ee.ImageCollection(lulc_years).mode()
    # Normalized Difference Vegetation Index (NDVI)
    if variable == "NDVI":
        start_date = f"{start_year}-07-01"
        end_date = f"{end_year + 1}-06-30"
        final_lsmc_ndvi_ts = Get_Padded_NDVI_TS_Image(start_date, end_date, roi)
        ndvi = final_lsmc_ndvi_ts.select("gapfilled_NDVI_lsc").reduce(ee.Reducer.mean())
        return ndvi
    # Distance to Roads
    if variable == "distToRoad":
        dataset_collection = ee.FeatureCollection(
            f"projects/df-project-iit/assets/datasets/Road_DRRP/{valid_gee_text(state)}"
        )
        dataset = dataset_collection.reduceToImage(
            properties=["STATE_ID"], reducer=ee.Reducer.first()
        )
        return (
            dataset.fastDistanceTransform().sqrt().multiply(ee.Image.pixelArea().sqrt())
        )

    # Distance to Drainage
    if variable == "distToDrainage":
        dataset = ee.Image(dataset_paths[variable])
        # Filter streams with Strahler order between 3 and 7
        strahler3to7 = dataset.select(["b1"]).lte(7).And(dataset.select(["b1"]).gt(2))
        return (
            strahler3to7.fastDistanceTransform()
            .sqrt()
            .multiply(ee.Image.pixelArea().sqrt())
        )

    # Distance to Settlements
    if variable == "distToSettlements":
        s_year = start_year
        lulc_years = []
        while s_year <= end_year:
            asset_id = f"{GEE_DATASET_PATH}/LULC_v3_river_basin/pan_india_lulc_v3_{s_year}_{str(s_year + 1)}"
            lulc_img = (
                ee.Image(asset_id).select(["predicted_label"]).clip(roi.geometry())
            )
            lulc_years.append(lulc_img)
            s_year += 1
        lulc = ee.ImageCollection(lulc_years).mode()
        return (
            lulc.eq(1)
            .fastDistanceTransform()
            .sqrt()
            .multiply(ee.Image.pixelArea().sqrt())
            .reproject(crs="EPSG:4326", scale=10)
        )
    return None


def create_classification(
    project_intervals, variable_list, roi, state, start_year, end_year
):
    """
    Create classification layers for multiple variables.

    Converts continuous datasets into categorical layers
    based on predefined intervals and thresholds.

    Args:
        project_intervals: User-defined intervals
        variable_list: List of variables to classify
        roi: Region of Interest
        state: Geographic state
        start_year: Start year
        end_year: End year
    Returns:
        Multi-band image with classified layers
    """
    sub_layer = ee.Image(1)

    def classify_variable(variable):
        nonlocal sub_layer
        labels = project_intervals[variable]["labels"].split(",")
        thresholds = project_intervals[variable]["thresholds"].split(",")
        dataset = get_dataset(variable, state, roi, start_year, end_year).clip(
            roi.geometry()
        )

        classification = ee.Image(1)
        classification = classification.rename(variable)

        for i in range(len(thresholds)):
            label = ee.Number(float(labels[i]))

            if "-" in thresholds[i]:
                interval = thresholds[i].split("-")
                bottom, top = interval[0], interval[1]

                if top == "posInf":
                    bottom_num = ee.Number(float(bottom))
                    classification = classification.where(
                        dataset.gte(bottom_num), label
                    )
                elif bottom == "negInf":
                    top_num = ee.Number(float(top))
                    classification = classification.where(dataset.lte(top_num), label)
                else:
                    top_num = ee.Number(float(top))
                    bottom_num = ee.Number(float(bottom))
                    classification = classification.where(
                        dataset.lte(top_num).And(dataset.gte(bottom_num)), label
                    )
            else:
                val = ee.Number(float(thresholds[i]))
                classification = classification.where(dataset.eq(val), label)

        sub_layer = sub_layer.addBands(classification).clip(roi.geometry())

    for variable in variable_list:
        classify_variable(variable)

    return sub_layer


def sync_to_gcs_geoserver(asset_id, layer_name, scale):
    image = ee.Image(asset_id)
    task_id = sync_raster_to_gcs(image, scale, layer_name)
    task_id_list = check_task_status([task_id])
    print("task_id sync to gcs ", task_id_list)

    sync_raster_gcs_to_geoserver(
        "plantation",
        layer_name,
        layer_name,
        None,
    )
