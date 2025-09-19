import ee

from utilities.gee_utils import ee_initialize, valid_gee_text
from .plantation_utils import dataset_paths


def get_site_properties():  # roi, state):
    ee_initialize()
    roi = ee.FeatureCollection(
        "projects/ee-corestackdev/assets/apps/plantation/saytrees/mbrdi_biodiversity_conservation_-_kolar/SayTrees_MBRDI_Biodiversity_Conservation_-_Kolar"
    ).limit(2)
    state = "andhra pradesh"
    state_dist_to_road = get_dist_to_road(state)

    # Function to compute slope/aspect
    def derive_terrain(asset_name, raster):
        if asset_name == "slope":
            return ee.Terrain.slope(raster)
        elif asset_name == "aspect":
            return ee.Terrain.aspect(raster)
        return raster

    def get_properties(feature):
        # Dictionary to hold vectorized outputs
        vectorized = {}

        # Iterate through datasets
        for key, path in dataset_paths.items():
            raster = ee.Image(path)

            # Derive slope/aspect
            raster = derive_terrain(key, raster)

            raster = raster.select(0)

            # Get native scale from raster
            scale = raster.projection().nominalScale()

            vectors = raster.reduceRegions(
                feature.geometry(), ee.Reducer.mean(), scale, raster.projection()
            )

            # Store in dictionary
            vectorized[key] = vectors.first().get("mean")

        # Distance to road
        min_distance = state_dist_to_road.reduceRegion(
            reducer=ee.Reducer.min(),
            geometry=roi.geometry(),
            scale=30,  # TODO: Check this
            maxPixels=1e12,
        )

        vectorized["distToRoad"] = min_distance.get("distance")

        return feature.set("site_props", vectorized)

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


# def get_distance_to_drainage(state):
#     dataset = ee.Image(dataset_paths[variable])
#     # Filter streams with Strahler order between 3 and 7
#     strahler3to7 = dataset.select(["b1"]).lte(7).And(dataset.select(["b1"]).gt(2))
#     return (
#         strahler3to7.fastDistanceTransform()
#         .sqrt()
#         .multiply(ee.Image.pixelArea().sqrt())
#     )
