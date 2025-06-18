import ee
from computing.lulc.v4.misc import get_points
from utilities.gee_utils import (
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)


def cluster_farm_boundaries(state, district, block):
    directory = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    description = directory + "_farm_clusters"
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

    blocks_df = get_points(roi_boundary, 17, 16, directory)
    points = list(blocks_df["points"])

    roi_boundary = ee.FeatureCollection(
        [
            ee.Feature(
                ee.Geometry.Rectangle(
                    [top_left[1], bottom_right[0], bottom_right[1], top_left[0]]
                )
            )
            for top_left, bottom_right in points
        ]
    )

    easy_farm = [
        ee.Filter.gte("rect", 0.67),
        ee.Filter.gt("size", 500),
        ee.Filter.lt("size", 2000),
        ee.Filter.lt("ent", 1),
    ]

    all_boundaries = ee.FeatureCollection(
        get_gee_asset_path(state, district, block) + directory + "_boundaries"
    )
    farm = all_boundaries.filter(ee.Filter.And(*easy_farm))

    # Filter out farms which doesnot have 3 nearby farms (Removing solo farms inside scrublands)
    farm_buffer = farm.map(lambda x: x.buffer(10))
    farm_image = ee.Image(0)
    farm_mask = farm_image.clip(farm_buffer).mask()

    farm_vectors = farm_mask.toInt().reduceToVectors(
        geometry=roi_boundary,
        scale=10,  # Change based on your resolution
        geometryType="polygon",
        labelProperty="zone",
        reducer=ee.Reducer.countEvery(),
        maxPixels=1e8,
    )
    farm_vectors = (
        farm_vectors.filter(ee.Filter.eq("zone", 1))
        .map(lambda x: x.set("count", farm.filterBounds(x.geometry()).size()))
        .filter(ee.Filter.gt("count", 3))
    )

    task_id = export_vector_asset_to_gee(farm_vectors, description, asset_id)
    return task_id
