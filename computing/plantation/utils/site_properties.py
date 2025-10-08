import ee

from utilities.constants import GEE_DATASET_PATH
from utilities.gee_utils import (
    valid_gee_text,
)
from .plantation_utils import dataset_info


def get_site_properties(roi, state, start_year, end_year):
    state_dist_to_road = get_dist_to_road(state)
    dist_to_drainage = get_distance_to_drainage()
    dist_to_settlement = get_distance_to_settlement(start_year, end_year)

    # Function to compute slope/aspect
    def derive_terrain(asset_name, raster):
        if asset_name == "slope":
            return ee.Terrain.slope(raster)
        elif asset_name == "aspect":
            return ee.Terrain.aspect(raster)
        return raster

    def get_properties(feature):
        # Dictionary to hold vectorized outputs
        vectorized_props = {}

        # Iterate through datasets
        for key, value in dataset_info.items():
            path = value["path"]
            unit = value["unit"]
            if key == "distToDrainage":
                prop_value = vectorize_dataset(dist_to_drainage, feature, 10)
            else:
                raster = ee.Image(path)

                # Derive slope/aspect
                raster = derive_terrain(key, raster)

                raster = raster.select(0)

                # Get native scale from raster
                scale = raster.projection().nominalScale()

                vectors = raster.reduceRegions(
                    feature.geometry(), ee.Reducer.mean(), scale, raster.projection()
                )
                prop_value = vectors.first().get("mean")

            if "mapping" in value:
                mapping = ee.Dictionary(value["mapping"])
                prop_value = ee.Algorithms.If(
                    prop_value, mapping.get(ee.Number(prop_value).toInt()), None
                )

            vectorized_props[key] = ee.Algorithms.If(
                prop_value,
                ee.String(prop_value).cat(ee.String(" ")).cat(ee.String(unit)),
                None,
            )

        # Distance to the nearest road
        road_distance = ee.String(vectorize_dataset(state_dist_to_road, feature, 10))
        vectorized_props["distToRoad"] = ee.Algorithms.If(
            road_distance,
            road_distance.cat(ee.String(" ")).cat(ee.String("m")),
            None,
        )

        # Distance to the nearest settlement
        settlement_distance = ee.String(
            vectorize_dataset(dist_to_settlement, feature, 10)
        )
        vectorized_props["distToSettlement"] = (
            settlement_distance,
            settlement_distance.cat(ee.String(" ")).cat(ee.String("m")),
            None,
        )

        # Convert to dictionary and encode as JSON string
        vectorized_dict = ee.Dictionary(vectorized_props)
        vectorized_json = ee.String.encodeJSON(vectorized_dict)

        return feature.set("site_props", vectorized_json)

    site_properties = roi.map(get_properties)
    return site_properties


def get_dist_to_road(state):
    dataset_collection = ee.FeatureCollection(
        f"projects/df-project-iit/assets/datasets/Road_DRRP/{valid_gee_text(state)}"
    )
    dataset = dataset_collection.reduceToImage(
        properties=["STATE_ID"], reducer=ee.Reducer.first()
    )
    return dataset.fastDistanceTransform().sqrt().multiply(ee.Image.pixelArea().sqrt())


def get_distance_to_drainage():
    path = dataset_info["distToDrainage"]["path"]
    dataset = ee.Image(path)
    # Filter streams with Strahler order between 3 and 7
    strahler3to7 = dataset.select(["b1"]).lte(7).And(dataset.select(["b1"]).gt(2))
    return (
        strahler3to7.fastDistanceTransform()
        .sqrt()
        .multiply(ee.Image.pixelArea().sqrt())
    )


def get_distance_to_settlement(start_year, end_year):
    lulc_years = []
    while start_year <= end_year:
        asset_id = f"{GEE_DATASET_PATH}/LULC_v3_river_basin/pan_india_lulc_v3_{start_year}_{str(start_year + 1)}"
        lulc_img = ee.Image(asset_id).select(["predicted_label"])
        lulc_years.append(lulc_img)
        start_year += 1
    lulc = ee.ImageCollection(lulc_years).mode()
    settlement_mask = lulc.eq(1)

    dist_to_settlement = (
        settlement_mask.fastDistanceTransform()
        .sqrt()
        .multiply(ee.Image.pixelArea().sqrt())
    )

    return dist_to_settlement.rename("distance")


def vectorize_dataset(dataset, roi, scale):
    min_distance = dataset.reduceRegion(
        reducer=ee.Reducer.min(),
        geometry=roi.geometry(),
        scale=scale,
        maxPixels=1e12,
    )

    return ee.Algorithms.If(
        min_distance.get("distance"),
        ee.Number(min_distance.get("distance")).multiply(1000).round().divide(1000),
        0,
    )
