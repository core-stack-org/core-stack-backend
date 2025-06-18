import ee
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)


def create_model_classifier(state, district, block):
    directory = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    description = directory + "_classifier"
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return None

    roi_boundary = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    ).union()

    mapping = {"farm": 1, "plantation": 2, "scrubland": 3, "rest": 0}
    # reversed_mapping = {v: k for k, v in mapping.items()}
    # reversed_ee_mapping = ee.Dictionary(reversed_mapping)

    easy_farm = [
        ee.Filter.gte("rect", 0.67),
        ee.Filter.gt("size", 500),
        ee.Filter.lt("size", 2000),
        ee.Filter.lt("ent", 1),
    ]
    easy_scrubland = [ee.Filter.gte("size", 60000)]
    easy_plantation = [ee.Filter.lt("area", 20000), ee.Filter.gt("area", 1000)]

    all_boundaries = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + directory + "_boundaries"
    )
    farm = all_boundaries.filter(ee.Filter.And(*easy_farm))
    scrubland = all_boundaries.filter(ee.Filter.And(easy_scrubland))
    plantation = (
        all_boundaries.filter(ee.Filter.eq("class", "plantation"))
        .map(lambda x: x.set("area", x.geometry().area()))
        .filter(ee.Filter.And(easy_plantation))
    )

    farm_vectors = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + directory + "_farm_clusters"
    )
    farm = farm.filterBounds(farm_vectors)

    ## Scrubland filtering. Remove large boundaries due to wrong field segmentation from scrubland samples
    # Step 1: Mask for class 8, 9, 10, 11
    lulc_v3 = ee.Image(
        get_gee_asset_path(state, district, block)
        + directory
        + "_2023-07-01_2024-06-30_LULCmap_10m"
    )

    classes_of_interest = [8, 9, 10, 11]
    masked_lulc = lulc_v3.remap(
        classes_of_interest, [1] * len(classes_of_interest)
    )  # 1 where class is of interest, 0 elsewhere

    # Step 2: Function to compute % area of interest inside each feature
    def filter_by_lulc(feature):
        geom = feature.geometry()
        scale = 30  # Set resolution appropriate to your LULC data

        # Area of interest within polygon (masked_lulc == 1)
        interest_area_img = ee.Image.pixelArea().updateMask(masked_lulc)
        interest_area = interest_area_img.reduceRegion(
            reducer=ee.Reducer.sum(), geometry=geom, scale=scale, maxPixels=1e8
        ).get("area")

        # Total area of the polygon
        total_area = (
            ee.Image.pixelArea()
            .reduceRegion(
                reducer=ee.Reducer.sum(), geometry=geom, scale=scale, maxPixels=1e8
            )
            .get("area")
        )

        # Compute % and add it as a property
        percent_interest = (
            ee.Number(interest_area).divide(ee.Number(total_area)).multiply(100)
        )
        return feature.set("percent_interest", percent_interest)

    # Step 3: Apply to FeatureCollection
    scrubland = scrubland.map(filter_by_lulc)

    # Step 4: Filter features with > 50% area of interest
    def strictly_inside_roi(feature):
        return feature.set(
            "inside",
            roi_boundary.geometry().contains(feature.geometry()),
        )

    scrubland = scrubland.filter(ee.Filter.lt("percent_interest", 50)).map(
        strictly_inside_roi
    )
    scrubland = scrubland.filter(ee.Filter.eq("inside", True))

    label_image = ee.Image(0).rename("label")
    farm_mask = label_image.clip(farm).mask()
    scrubland_mask = label_image.clip(scrubland).mask()
    plantation_mask = label_image.clip(plantation).mask()

    label_image = (
        label_image.where(farm_mask, mapping["farm"])
        .where(scrubland_mask, mapping["scrubland"])
        .where(plantation_mask, mapping["plantation"])
    )

    ts_data = ee.Image(
        get_gee_asset_path(state, district, block) + "ts_data_" + directory
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
    try:
        task = ee.batch.Export.classifier.toAsset(
            classifier,
            description,
            asset_id,
        )
        task.start()

        return task.status()["id"]
    except Exception as e:
        print("Exception in export classifier", e)

    return None
