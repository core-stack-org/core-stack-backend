import ee
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)


def classify_raster(state, district, block):
    directory = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    description = f"lulc_v4_{directory}_boundaries_refined"
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return None

    classifier = ee.Classifier.load(
        get_gee_asset_path(state, district, block) + f"lulc_v4_{directory}_classifier"
    )
    ts_data = ee.Image(
        get_gee_asset_path(state, district, block) + f"lulc_v4_ts_data_{directory}"
    )
    classified = ts_data.classify(classifier)

    mapping = {"farm": 1, "plantation": 2, "scrubland": 3, "rest": 0}

    reversed_mapping = {v: k for k, v in mapping.items()}
    reversed_ee_mapping = ee.Dictionary(reversed_mapping)

    def assign_mode_label(feature):
        class_values = classified.reduceRegion(
            reducer=ee.Reducer.mode(),
            geometry=feature.geometry(),
            scale=30,  # Adjust scale as per resolution
            bestEffort=True,
        )
        return feature.set("class", class_values.get("classification"))

    all_boundaries = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + f"lulc_v4_{directory}_boundaries"
    )

    # Apply function to test features
    all_labels = all_boundaries.map(assign_mode_label).filter(
        ee.Filter.notNull(["class"])
    )
    all_labels = all_labels.map(
        lambda x: x.set(
            "class", reversed_ee_mapping.get(ee.Number(x.get("class")).int())
        )
    )

    task_id = export_vector_asset_to_gee(all_labels, description, asset_id)
    return task_id
