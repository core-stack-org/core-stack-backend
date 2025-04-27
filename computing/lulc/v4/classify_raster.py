import ee
from utilities.gee_utils import ee_initialize


def classify_raster(state, district, block, start_year, end_year):
    directory = "Area_Peddapally"

    mapping = {"farm": 1, "plantation": 2, "scrubland": 3, "rest": 0}
    reversed_mapping = {v: k for k, v in mapping.items()}
    reversed_ee_mapping = ee.Dictionary(reversed_mapping)

    easy_farm = [
        ee.Filter.gte("rect", 0.67),
        ee.Filter.gt("size", 500),
        ee.Filter.lt("size", 2000),
        ee.Filter.lt("ent", 1),
    ]
    easy_scurbland = [ee.Filter.gte("size", 60000), ee.Filter.gt("red", 0.9)]
    easy_plantation = [ee.Filter.lt("area", 20000), ee.Filter.gt("area", 1000)]

    all = get_feature_collection(
        "projects/ee-raman/assets/" + directory + "_boundaries"
    )
    farm = all.filter(ee.Filter.And(*easy_farm))
    scrubland = all.filter(ee.Filter.And(easy_scurbland))
    plantation = (
        all.filter(ee.Filter.eq("class", "plantation"))
        .map(lambda x: x.set("area", x.geometry().area()))
        .filter(ee.Filter.And(easy_plantation))
    )

    label_image = ee.Image(0).rename("label")
    farm_mask = label_image.clip(farm).mask()
    scrubland_mask = label_image.clip(scrubland).mask()
    plantation_mask = label_image.clip(plantation).mask()

    label_image = (
        label_image.where(farm_mask, mapping["farm"])
        .where(scrubland_mask, mapping["scrubland"])
        .where(plantation_mask, mapping["plantation"])
    )
    # ts_data = ee.Image("projects/ee-raman/assets/ts_data_" + directory) # TODO
    ts_data = ee.Image(
        "projects/ee-corestack-helper/assets/apps/ts_data_Area_Peddapally"
    )
    # Classes to sample (exclude background = 0)
    class_values = [(1, 20000), (2, 20000), (3, 20000)]

    # Empty list to store samples
    samples_list = []

    for class_val, points in class_values:
        # Create a mask for the class
        class_mask = label_image.eq(class_val)

        # Mask the ts_image to only include pixels of this class
        masked_ts = ts_data.updateMask(class_mask)

        # Sample uniformly from the masked image
        class_samples = masked_ts.addBands(
            label_image.rename("class")
        ).stratifiedSample(
            numPoints=points,  # adjust as needed
            classBand="class",
            classValues=[class_val],
            classPoints=[points],  # adjust per class
            scale=10,
            region=ts_data.geometry(),
            seed=42,
            geometries=True,
        )

        samples_list.append(class_samples)

    all_samples = samples_list[0].merge(samples_list[1]).merge(samples_list[2])

    classifier = ee.Classifier.smileRandomForest(50).train(
        features=all_samples, classProperty="class", inputProperties=ts_data.bandNames()
    )
    classified = ts_data.classify(classifier)

    def assign_mode_label(feature):
        class_values = classified.reduceRegion(
            reducer=ee.Reducer.mode(),
            geometry=feature.geometry(),
            scale=30,  # Adjust scale as per resolution
            bestEffort=True,
        )
        return feature.set("class", class_values.get("classification"))

    # Apply function to test features
    all_labels = all.map(assign_mode_label).filter(ee.Filter.notNull(["class"]))
    all_labels = all_labels.map(
        lambda x: x.set(
            "class", reversed_ee_mapping.get(ee.Number(x.get("class")).int())
        )
    )

    task = ee.batch.Export.table.toAsset(
        collection=all_labels,
        description="Classification",
        assetId="projects/ee-corestack-helper/assets/apps/"
        + directory
        + "_boundaries_refined",
    )

    # Start the task
    task.start()


def get_feature_collection(asset_id):
    """Check if an asset exists, and load it as a FeatureCollection if it does.
    Otherwise, return an empty FeatureCollection.

    Args:
        asset_id (str): The Earth Engine asset ID.

    Returns:
        ee.FeatureCollection: The loaded FeatureCollection or an empty one.
    """
    try:
        # Get asset information to check existence
        ee.data.getAsset(asset_id)
        print(f"Asset '{asset_id}' exists. Loading FeatureCollection.")
        return ee.FeatureCollection(asset_id)
    except Exception as e:
        print(f"Asset '{asset_id}' does not exist. Returning empty FeatureCollection.")
        return ee.FeatureCollection([])
