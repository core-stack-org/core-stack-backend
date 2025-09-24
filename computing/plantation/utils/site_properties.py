import ee

from utilities.gee_utils import ee_initialize, valid_gee_text
from .plantation_utils import dataset_paths


def get_site_properties():  # roi, state):
    ee_initialize()
    roi = ee.FeatureCollection(
        "projects/ee-corestackdev/assets/apps/plantation/saytrees/mbrdi_biodiversity_conservation_-_kolar/SayTrees_MBRDI_Biodiversity_Conservation_-_Kolar"
    )
    uids = [
        "940e849351ad64eab340485c2b7309b4",
        "c953dce94e284a4a16c15907661f82cc",
        "2ba17a6977b9a3a4bf534687cfa60051",
    ]
    roi = roi.filter(ee.Filter.inList("uid", uids))
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

        # # Distance to the nearest road
        # min_distance = state_dist_to_road.reduceRegion(
        #     reducer=ee.Reducer.min(),
        #     geometry=feature.geometry(),
        #     scale=30,  # TODO: Check this
        #     maxPixels=1e12,
        # )
        #
        # vectorized["distToRoad"] = (
        #     ee.Number(min_distance.get("distance")).multiply(1000).round().divide(1000)
        # )
        roads = ee.FeatureCollection(
            f"projects/df-project-iit/assets/datasets/Road_DRRP/{valid_gee_text(state)}"
        )

        # Compute distance from plantation polygon to each road
        distances = roads.map(
            lambda f: f.set("dist_m", f.geometry().distance(feature.geometry(), 1))
        )

        # Get the minimum distance
        min_distance = distances.aggregate_min("dist_m")
        vectorized["distToRoad"] = min_distance

        return feature.set("site_props", vectorized)

    site_properties = roi.map(get_properties)
    print(site_properties.getInfo())
    return site_properties


def get_dist_to_road(state):
    dataset_collection = ee.FeatureCollection(
        f"projects/df-project-iit/assets/datasets/Road_DRRP/{valid_gee_text(state)}"
    )
    # dataset = dataset_collection.reduceToImage(
    #     properties=["STATE_ID"], reducer=ee.Reducer.first()
    # )
    # return dataset.fastDistanceTransform().sqrt().multiply(ee.Image.pixelArea().sqrt())
    # Rasterize: give each road a dummy value (1)
    # Rasterize: mark road pixels as 1
    raster = (
        dataset_collection.map(lambda f: f.set("val", 1))
        .reduceToImage(properties=["val"], reducer=ee.Reducer.first())
        .unmask(0)
        .reproject(crs="EPSG:32645", scale=30)
    )  # use UTM projection for Bihar

    # Distance in meters
    dist_m = raster.fastDistanceTransform().sqrt().multiply(30).rename("distance")

    return dist_m


# def get_distance_to_drainage(state):
#     dataset = ee.Image(dataset_paths[variable])
#     # Filter streams with Strahler order between 3 and 7
#     strahler3to7 = dataset.select(["b1"]).lte(7).And(dataset.select(["b1"]).gt(2))
#     return (
#         strahler3to7.fastDistanceTransform()
#         .sqrt()
#         .multiply(ee.Image.pixelArea().sqrt())
#     )
