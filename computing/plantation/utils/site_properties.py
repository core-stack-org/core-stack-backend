import ee

from utilities.constants import GEE_DATASET_PATH
from utilities.gee_utils import ee_initialize, valid_gee_text
from .plantation_utils import dataset_paths


def get_data():
    ee_initialize(1)
    roi = ee.FeatureCollection(
        "projects/ee-corestackdev/assets/apps/plantation/saytrees/mbrdi_biodiversity_conservation_-_kolar/SayTrees_MBRDI_Biodiversity_Conservation_-_Kolar_site_suitability_vector"
    )
    state = "karnataka"
    start_year = 2021
    end_year = 2023
    fc = get_site_properties(roi, state, start_year, end_year)


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
        for key, path in dataset_paths.items():
            if key == "distToDrainage":
                min_drainage_distance = dist_to_drainage.reduceRegion(
                    reducer=ee.Reducer.min(),
                    geometry=feature.geometry(),
                    scale=10,
                    maxPixels=1e12,
                )

                vectorized_props[key] = (
                    ee.Number(min_drainage_distance.get("distance"))
                    .multiply(1000)
                    .round()
                    .divide(1000)
                )
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

                vectorized_props[key] = vectors.first().get("mean")

        # Distance to the nearest road
        min_road_distance = state_dist_to_road.reduceRegion(
            reducer=ee.Reducer.min(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e12,
        )

        vectorized_props["distToRoad"] = (
            ee.Number(min_road_distance.get("distance"))
            .multiply(1000)
            .round()
            .divide(1000)
        )

        # Distance to the nearest settlement

        min_settlement_distance = dist_to_settlement.reduceRegion(
            reducer=ee.Reducer.min(),
            geometry=feature.geometry(),
            scale=10,
            maxPixels=1e12,
        )

        vectorized_props["distToSettlement"] = (
            ee.Number(min_settlement_distance.get("distance"))
            .multiply(1000)
            .round()
            .divide(1000)
        )

        return feature.set("site_props", vectorized_props)

    site_properties = roi.map(get_properties)
    print(site_properties.getInfo())
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
    path = dataset_paths["distToDrainage"]
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
