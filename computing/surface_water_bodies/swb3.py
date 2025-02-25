import ee
from utilities.gee_utils import valid_gee_text, get_gee_asset_path, is_gee_asset_exists


def calculate_swb3(aoi, state, district, block):
    # Generate a unique description and asset ID for the water body processing
    description = (
        "swb3_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    # Check if the asset already exists to avoid redundant processing
    if is_gee_asset_exists(asset_id):
        return None, asset_id

    # Load census state and water bodies feature collections
    census_state = ee.FeatureCollection(
        "projects/ee-vatsal/assets/WBC_" + state.upper().replace(" ", "") + "_UPD"
    )
    water_bodies = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "swb2_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    # Filter points and polygons within the area of interest (aoi)
    points = census_state.filterBounds(aoi)
    polygons = water_bodies.filterBounds(aoi)

    feature_collection = points

    # Function to handle null or missing area values
    def replace_null_area(feature):
        # Extract water body attributes, handling potential null values
        area = ee.Number(feature.get("water_spread_area_of_water_body"))
        capacity = ee.Number(feature.get("storage_capacity_water_body_original"))
        depth = ee.Number(feature.get("max_depth_water_body_fully_filled"))

        # Ensure non-null values or default to 0
        area = ee.Algorithms.If(area, area, 0)
        capacity = ee.Algorithms.If(capacity, capacity, 0)
        depth = ee.Algorithms.If(depth, depth, 0)
        area = ee.Number(area)
        capacity = ee.Number(capacity)
        depth = ee.Number(depth)
        # Calculate area if not provided, using capacity and depth
        new_area = ee.Algorithms.If(
            area.neq(0),
            area,
            ee.Algorithms.If(
                capacity and depth, capacity.divide(depth).divide(10000), 0
            ),
        )
        return feature.set("water_spread_area_of_water_body", new_area)

    # Apply area replacement to all features
    points = feature_collection.map(replace_null_area)

    # Buffer points to create search areas
    buffered_points = points.map(lambda feature: feature.buffer(90))

    # Create a spatial filter to find intersecting features
    spatial_filter = ee.Filter.intersects(
        leftField=".geo", rightField=".geo", maxError=10
    )

    # Perform a spatial join to find intersecting polygons for each point
    intersect_joined = ee.Join.saveAll("intersections").apply(
        primary=buffered_points, secondary=polygons, condition=spatial_filter
    )

    # Function to select the closest polygon to each point
    def select_closest_polygon(feature):
        # Calculate the spread area of the water body
        spread = ee.Number(feature.get("water_spread_area_of_water_body"))
        spread = spread.multiply(10000)
        intersections = ee.List(feature.get("intersections"))

        # Calculate the difference between intersection areas
        def compute_difference(fc2_feature):
            area_ored = ee.Number(fc2_feature.get("area_ored"))
            difference = area_ored.subtract(spread).abs()
            return ee.Feature(
                None, {"difference": difference, "uid": fc2_feature.get("UID")}
            )

        # Find the polygon with the closest area match
        fc2_intersecting = ee.FeatureCollection(intersections)
        fc2_with_difference = fc2_intersecting.map(compute_difference)
        sorted_fc2 = fc2_with_difference.sort("difference", True)
        closest_feature = sorted_fc2.first()
        return feature.set("closest_polygon_id", closest_feature.get("uid"))

    # Apply closest polygon selection
    fc1_with_closest_polygon = intersect_joined.map(select_closest_polygon)
    fc1 = fc1_with_closest_polygon
    fc2 = water_bodies

    # Join water bodies with points based on closest polygon
    join_filter = ee.Filter.equals(leftField="UID", rightField="closest_polygon_id")
    joined_fc = ee.Join.saveAll(matchesKey="matches").apply(
        primary=fc2, secondary=fc1, condition=join_filter
    )

    # Add census IDs to features
    def add_census_id(feature):
        matches = ee.List(feature.get("matches"))
        census_ids = matches.map(lambda m: ee.Feature(m).get("unique_id"))
        return feature.set("census_id", census_ids.get(0))

    fc2_with_census_id = joined_fc.map(add_census_id)
    fc2 = fc2_with_census_id
    fc1 = water_bodies

    # Remove unnecessary match properties
    def remove_property(feat, prop):
        properties = feat.propertyNames()
        select_properties = properties.filter(ee.Filter.neq("item", prop))
        return feat.select(select_properties)

    new_fc = fc2.map(lambda feat: remove_property(feat, "matches"))
    fc2_without_matrices = new_fc

    # Add census ID to features without matches
    fc1_with_census_id = fc1.map(lambda feat: feat.set("census_id", "NA"))

    # Filter and merge feature collections
    uid_list = fc2_without_matrices.aggregate_array("UID").distinct()
    filtered_fc1 = fc1_with_census_id.filter(ee.Filter.inList("UID", uid_list).Not())

    first_fc = ee.FeatureCollection(filtered_fc1.merge(fc2_without_matrices))

    # Perform a join with census state data
    second_fc = census_state
    field_filter = ee.Filter.equals(leftField="census_id", rightField="unique_id")
    join = ee.Join.saveAll(matchesKey="matches", ordering="unique_id", ascending=True)
    joined = join.apply(primary=first_fc, secondary=second_fc, condition=field_filter)

    # Merge properties from matched features
    def merge_props(feature):
        matches = ee.List(feature.get("matches"))
        census_feature = ee.Feature(matches.get(0))
        return feature.copyProperties(census_feature)

    merged = joined.map(merge_props)
    merged = merged.map(lambda feat: remove_property(feat, "matches"))

    # Identify and set columns unique to the merged dataset
    columns1 = ee.List(merged.first().propertyNames())
    columns2 = ee.List(filtered_fc1.first().propertyNames())
    unique_to_list1 = columns1.removeAll(columns2)

    # Set unique columns to "NA" for features without matches
    def set_columns_to_na(feature):
        return ee.Feature(
            unique_to_list1.iterate(
                lambda column_name, feat: ee.Feature(feature).set(column_name, "NA"),
                feature,
            )
        )

    filtered_fc1 = filtered_fc1.map(set_columns_to_na)
    final_upd = filtered_fc1.merge(merged)

    # Export the final feature collection to Google Earth Engine asset
    try:
        swb_task = ee.batch.Export.table.toAsset(
            **{
                "collection": final_upd,
                "description": description,
                "assetId": asset_id,
                "scale": 30,
                "maxPixels": 1e13,
            }
        )

        swb_task.start()
        print("Successfully started the swb3", swb_task.status())
        return swb_task.status()["id"], asset_id
    except Exception as e:
        print(f"Error occurred in running swb3 task: {e}")
