import ee

from utilities.constants import GEE_PATH_PLANTATION
from utilities.gee_utils import (
    is_gee_asset_exists,
    harmonize_band_types,
    get_gee_dir_path,
    check_task_status,
    valid_gee_text,
)
from .plantation_utils import dataset_paths, saytrees_weights, saytrees_intervals
from utilities.logger import setup_logger


logger = setup_logger(__name__)


def get_pss(roi, org, project, state, asset_name):
    # Initialize base image
    all_layers = ee.Image(1)
    description = asset_name + "_raster"
    asset_id = (
        get_gee_dir_path([org, project], asset_path=GEE_PATH_PLANTATION) + description
    )

    if is_gee_asset_exists(asset_id):
        ee.data.deleteAsset(asset_id)

    # Climate Layer
    climate_variables = [
        "annualPrecipitation",
        "meanAnnualTemperature",
        "aridityIndex",
        "referenceEvapoTranspiration",
    ]

    climate_variable_weights = {
        "annualPrecipitation": 0.25,
        "meanAnnualTemperature": 0.25,
        "aridityIndex": 0.25,
        "referenceEvapoTranspiration": 0.25,
    }  # TODO Use from DB default config

    climate_variable_weights = get_weights(climate_variable_weights)
    # Validate climate weights
    # if abs(sum(climate_weights.values()) - 1.0) > 1e-6:
    #     raise ValueError("Climate weights must sum to 1")

    climate_sub_layers = create_classification(climate_variables, roi, state)

    climate_expr_vars = {
        "annualPrecip": climate_sub_layers.select(["annualPrecipitation"]),
        "meanAnnualTemp": climate_sub_layers.select(["meanAnnualTemperature"]),
        "aridityIndex": climate_sub_layers.select(["aridityIndex"]),
        "refEvapoTransp": climate_sub_layers.select(["referenceEvapoTranspiration"]),
    }

    climate_expr_weights = {
        "w1": ee.Number(climate_variable_weights["annualPrecipitation"]),
        "w2": ee.Number(climate_variable_weights["meanAnnualTemperature"]),
        "w3": ee.Number(climate_variable_weights["aridityIndex"]),
        "w4": ee.Number(climate_variable_weights["referenceEvapoTranspiration"]),
    }

    climate_layer = climate_sub_layers.expression(
        "w1 * annualPrecip + w2 * meanAnnualTemp + w3 * aridityIndex + w4 * refEvapoTransp",
        {**climate_expr_vars, **climate_expr_weights},
    ).rename("Climate")

    logger.info("------------Climate layer---------------")

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

    # Soil Nutrient Weights
    topsoil_nutrient_weights = {
        "tnTopsoilPH": 0.25,
        "tnTopsoilCEC": 0.25,
        "tnTopsoilOC": 0.25,
        "tnTopsoilTexture": 0.25,
    }  # TODO Use from DB default config

    topsoil_nutrient_weights = get_weights(topsoil_nutrient_weights)

    subsoil_nutrient_weights = {
        "snSubsoilPH": 0.25,
        "snSubsoilCEC": 0.25,
        "snSubsoilOC": 0.25,
        "snSubsoilTexture": 0.25,
    }  # TODO Use from DB default config

    subsoil_nutrient_weights = get_weights(subsoil_nutrient_weights)

    rooting_condition_weights = {
        "rcTopsoilPH": 0.25,
        "rcSubsoilPH": 0.25,
        "rcTopsoilBD": 0.25,
        "rcSubsoilBD": 0.25,
    }  # TODO Use from DB default config

    rooting_condition_weights = get_weights(rooting_condition_weights)

    soil_variable_weights = {
        "topsoilNutrient": 0.20,
        "subsoilNutrient": 0.20,
        "rootingCondition": 0.20,
        "drainage": 0.20,
        "AWC": 0.20,
    }  # TODO Use from DB default config

    soil_variable_weights = get_weights(soil_variable_weights)

    soil_sub_layers = create_classification(soil_variables, roi, state)

    # Topsoil Nutrient Layer
    topsoil_nutrient_layer = soil_sub_layers.expression(
        "w1 * topsoilPH + w2 * topsoilOC + w3 * topsoilCEC + w4 * topsoilTexture",
        {
            "topsoilPH": soil_sub_layers.select(["topsoilPH"]),
            "w1": ee.Number(topsoil_nutrient_weights["tnTopsoilPH"]),
            "topsoilOC": soil_sub_layers.select(["topsoilOC"]),
            "w2": ee.Number(topsoil_nutrient_weights["tnTopsoilOC"]),
            "topsoilCEC": soil_sub_layers.select(["topsoilCEC"]),
            "w3": ee.Number(topsoil_nutrient_weights["tnTopsoilCEC"]),
            "topsoilTexture": soil_sub_layers.select(["topsoilTexture"]),
            "w4": ee.Number(topsoil_nutrient_weights["tnTopsoilTexture"]),
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
            "w1": ee.Number(subsoil_nutrient_weights["snSubsoilPH"]),
            "subsoilOC": soil_sub_layers.select(["subsoilOC"]),
            "w2": ee.Number(subsoil_nutrient_weights["snSubsoilOC"]),
            "subsoilCEC": soil_sub_layers.select(["subsoilCEC"]),
            "w3": ee.Number(subsoil_nutrient_weights["snSubsoilCEC"]),
            "subsoilTexture": soil_sub_layers.select(["subsoilTexture"]),
            "w4": ee.Number(subsoil_nutrient_weights["snSubsoilTexture"]),
        },
    )
    subsoil_nutrient_layer = subsoil_nutrient_layer.rename("subsoilNutrient")
    soil_sub_layers = soil_sub_layers.addBands(subsoil_nutrient_layer)

    ################ Rooting Condition Layer  ##########################
    rooting_condition_layer = soil_sub_layers.expression(
        "w1 * topsoilPH + w2 * topsoilBD + w3 * subsoilPH + w4 * subsoilBD",
        {
            "topsoilPH": soil_sub_layers.select(["topsoilPH"]),
            "w1": ee.Number(rooting_condition_weights["rcTopsoilPH"]),
            "topsoilBD": soil_sub_layers.select(["topsoilBD"]),
            "w2": ee.Number(rooting_condition_weights["rcTopsoilBD"]),
            "subsoilPH": soil_sub_layers.select(["subsoilPH"]),
            "w3": ee.Number(rooting_condition_weights["rcSubsoilPH"]),
            "subsoilBD": soil_sub_layers.select(["subsoilBD"]),
            "w4": ee.Number(rooting_condition_weights["rcSubsoilBD"]),
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
            "w1": ee.Number(soil_variable_weights["topsoilNutrient"]),
            "subsoilNutrient": soil_sub_layers.select(["subsoilNutrient"]),
            "w2": ee.Number(soil_variable_weights["subsoilNutrient"]),
            "rootingCondition": soil_sub_layers.select(["rootingCondition"]),
            "w3": ee.Number(soil_variable_weights["rootingCondition"]),
            "drainage": soil_sub_layers.select(["drainage"]),
            "w4": ee.Number(soil_variable_weights["drainage"]),
            "AWC": soil_sub_layers.select(["AWC"]),
            "w5": ee.Number(soil_variable_weights["AWC"]),
        },
    )
    soil_layer = soil_layer.rename("Soil")
    logger.info("------------soil_layer----------------")
    all_layers = all_layers.addBands(soil_layer)

    ######################## Socioeconomic Layer  ############################
    socioeconomic_variables = ["distToRoad", "distToDrainage", "distToSettlements"]

    socioeconomic_variable_weights = {
        "distToRoad": 0.33,
        "distToDrainage": 0.33,
        "distToSettlements": 0.34,
    }  # TODO Use from DB default config

    socioeconomic_variable_weights = get_weights(socioeconomic_variable_weights)

    socioeconomic_sub_layers = create_classification(
        socioeconomic_variables, roi, state
    )

    socioeconomic_layer = socioeconomic_sub_layers.expression(
        "w1 * distToRoad + w2 * distToDrainage + w3 * distToSettlements",
        {
            "distToRoad": socioeconomic_sub_layers.select(["distToRoad"]),
            "w1": ee.Number(socioeconomic_variable_weights["distToRoad"]),
            "distToDrainage": socioeconomic_sub_layers.select(["distToDrainage"]),
            "w2": ee.Number(socioeconomic_variable_weights["distToDrainage"]),
            "distToSettlements": socioeconomic_sub_layers.select(["distToSettlements"]),
            "w3": ee.Number(socioeconomic_variable_weights["distToSettlements"]),
        },
    )

    socioeconomic_layer = socioeconomic_layer.rename("Socioeconomic")
    logger.info("-------------------socioeconomic_layer---------------")
    all_layers = all_layers.addBands(socioeconomic_layer)

    ##################### Ecology Layer  ############################
    ecology_variables = ["NDVI", "LULC"]

    ecology_variable_weights = {
        "NDVI": 0.5,
        "LULC": 0.5,
    }  # TODO Use from DB default config

    ecology_variable_weights = get_weights(ecology_variable_weights)

    ecology_sub_layers = create_classification(ecology_variables, roi, state)

    ecology_layer = ecology_sub_layers.expression(
        "w1 * NDVI + w2 * LULC",
        {
            "NDVI": ecology_sub_layers.select(["NDVI"]),
            "w1": ee.Number(ecology_variable_weights["NDVI"]),
            "LULC": ecology_sub_layers.select(["LULC"]),
            "w2": ee.Number(ecology_variable_weights["LULC"]),
        },
    )

    ecology_layer = ecology_layer.rename("Ecology")
    all_layers = all_layers.addBands(ecology_layer)

    ########## Topography Layer ###########################
    topography_variables = ["elevation", "slope", "aspect"]

    topography_variable_weights = {
        "elevation": 0.4,
        "slope": 0.4,
        "aspect": 0.2,
    }  # TODO Use from DB default config

    topography_variable_weights = get_weights(topography_variable_weights)

    topography_sub_layers = create_classification(topography_variables, roi, state)

    topography_layer = topography_sub_layers.expression(
        "w1 * elevation + w2 * slope + w3 * aspect",
        {
            "elevation": topography_sub_layers.select(["elevation"]),
            "w1": ee.Number(topography_variable_weights["elevation"]),
            "slope": topography_sub_layers.select(["slope"]),
            "w2": ee.Number(topography_variable_weights["slope"]),
            "aspect": topography_sub_layers.select(["aspect"]),
            "w3": ee.Number(topography_variable_weights["aspect"]),
        },
    )

    topography_layer = topography_layer.rename("Topography")
    all_layers = all_layers.addBands(topography_layer)

    ############### Final layer calculation  ######################
    final_weights = {
        "Climate": 0.25,
        "Soil": 0.20,
        "Topography": 0.30,
        "Ecology": 0.10,
        "Socioeconomic": 0.15,
    }  # TODO Use from DB default config

    final_weights = get_weights(final_weights)

    # # Validate final weights
    # if abs(sum(final_weights.values()) - 1.0) > 1e-6:
    #     raise ValueError("Final weights must sum to 1")

    final_expr_vars = {
        layer: all_layers.select([layer]) for layer in final_weights.keys()
    }

    final_expr_weights = {
        f"w{i+1}": ee.Number(weight) for i, weight in enumerate(final_weights.values())
    }

    final_layer = all_layers.expression(
        "w1 * Climate + w2 * Soil + w3 * Topography + w4 * Ecology + w5 * Socioeconomic",
        {**final_expr_vars, **final_expr_weights},
    ).rename("Final")

    # Final plantation score
    # This rounding is specific to a binary score output
    # Round off Plantation Score to 0 or 1 on each pixel and mask
    final_plantation_score = ee.Image(1).where(final_layer.lte(0.5), 0)

    # LULC mask
    # Classes to be masked for (in Dynamic World) - 1 (Trees), 2 (Grass), 4 (Crops), 5 (Shrub & Scrub), 7 (Bare ground)
    lulc = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterDate("2022-07-01", "2023-06-30")
        .median()
        .select("label")
    )
    # 0	#419bdf	water
    # 1	#397d49	trees
    # 2	#88b053	grass
    # 3	#7a87c6	flooded_vegetation
    # 4	#e49635	crops
    # 5	#dfc35a	shrub_and_scrub
    # 6	#c4281b	built
    # 7	#a59b8f	bare
    # 8	#b39fe1	snow_and_ice

    valid_lulc_values = [1, 2, 4, 5, 7]
    lulc_mask = lulc.eq(valid_lulc_values[0])
    for value in valid_lulc_values[1:]:
        lulc_mask = lulc_mask.Or(lulc.eq(value))

    final_plantation_score = (
        final_plantation_score.updateMask(lulc_mask)
        .clip(roi.geometry())
        .rename("final_score")
    )  # Changed to a valid band name

    logger.info("---------------final_plantation_score--------------")
    all_layers = all_layers.addBands(final_plantation_score)
    logger.info("Final Plantation Score")
    all_layers = harmonize_band_types(all_layers, "Float")

    # Export to GEE asset
    try:
        export_params = {
            "image": all_layers.clip(roi.geometry()),
            "description": description,
            "assetId": asset_id,
            "pyramidingPolicy": {"predicted_label": "mode"},
            "scale": 30,
            "maxPixels": 1e13,
            "crs": "EPSG:4326",
            "region": roi.geometry(),
        }

        export_task = ee.batch.Export.image.toAsset(**export_params)
        export_task.start()

        logger.info(f"Export task started with ID: {export_task.status()['id']}")
        check_task_status([export_task.status()["id"]])
        return asset_id

    except Exception as e:
        logger.exception(f"Export failed: {str(e)}")
        raise


def get_dataset(variable, state):
    """
    Get preprocessed dataset for a specific variable
    """
    diff_variables = [
        "distToRoad",
        "distToDrainage",
        "distToSettlements",
        "slope",
        "aspect",
        "NDVI",
        "LULC",
    ]
    if variable in diff_variables:
        if variable == "slope":
            dataset = ee.Image(dataset_paths[variable])
            return ee.Terrain.slope(dataset)
        elif variable == "aspect":
            dataset = ee.Image(dataset_paths[variable])
            return ee.Terrain.aspect(dataset)
        elif variable == "LULC":
            return (
                ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
                .filterDate("2022-07-01", "2023-06-30")  # TODO Use IndiaSAT
                .median()
                .select("label")
            )
        elif variable == "NDVI":
            return (
                ee.ImageCollection("LANDSAT/COMPOSITES/C02/T1_L2_ANNUAL_NDVI")
                .filterDate("2022-07-01", "2023-06-30")  # TODO Check
                .select("NDVI")
                .reduce(ee.Reducer.mean())
            )
        elif variable == "distToRoad":
            dataset_collection = ee.FeatureCollection(
                "projects/df-project-iit/assets/datasets/Road_DRRP/"
                + valid_gee_text(state)
            )
            dataset = dataset_collection.reduceToImage(
                properties=["STATE_ID"], reducer=ee.Reducer.first()
            )
            return (
                dataset.fastDistanceTransform()
                .sqrt()
                .multiply(ee.Image.pixelArea().sqrt())
            )
        elif variable == "distToDrainage":
            dataset = ee.Image(dataset_paths[variable])
            strahler3to7 = (
                dataset.select(["b1"]).lte(7).And(dataset.select(["b1"]).gt(2))
            )
            return (
                strahler3to7.fastDistanceTransform()
                .sqrt()
                .multiply(ee.Image.pixelArea().sqrt())
            )
        elif variable == "distToSettlements":
            LULC = (
                ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
                .filterDate("2022-07-01", "2023-06-30")
                .median()
                .select("label")
            )
            return (
                LULC.eq(6)
                .fastDistanceTransform()
                .sqrt()
                .multiply(ee.Image.pixelArea().sqrt())
            )
    else:
        return ee.Image(dataset_paths[variable])


def get_weights(weight_dict):
    """
    Get customized weights for variables
    """
    new_dict = {}
    for key in weight_dict:
        weight = saytrees_weights[key]
        new_dict[key] = round(weight, 2)

    return new_dict


def create_classification(variable_list, roi, state):
    sub_layer = ee.Image(1)

    def classify_variable(variable):
        nonlocal sub_layer
        labels = saytrees_intervals[variable]["labels"].split(",")
        thresholds = saytrees_intervals[variable]["thresholds"].split(",")
        dataset = get_dataset(variable, state).clip(roi.geometry())

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
