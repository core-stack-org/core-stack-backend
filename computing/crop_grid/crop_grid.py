import json

import geojson
import geopandas as gpd
from shapely import geometry
import pickle
import os
import ee

from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
)
from .crop_gridXlulc import crop_grids_lulc
from nrm_app.celery import app
from utilities.constants import (
    ADMIN_BOUNDARY_INPUT_DIR,
    CROP_GRID_PATH,
)


@app.task(bind=True)
def create_crop_grids(self, state, district, block):
    ee_initialize()
    description = (
        "crop_grid_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower() + "_with_uid_16ha")
    )
    asset_id = get_gee_asset_path(state, district, block) + description
    if not is_gee_asset_exists(asset_id):
        # Get block coordinates
        block_coords = get_block_coordinates(state, district, block)
        geom_len = len(block_coords)
        state_dir = os.path.join(CROP_GRID_PATH, state)

        if not os.path.exists(state_dir):
            os.mkdir(state_dir)

        path = os.path.join(
            str(state_dir),
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
        )

        if not os.path.exists(path):
            os.mkdir(path)

        path = os.path.join(
            path,
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
        )

        # Generate required files
        gen_geojson_from_coords(path, block_coords)
        gen_grids(path, block_coords)

        task_id = convert_geojson_to_fc(state, district, block, path, geom_len)
        if task_id:
            task_id_list = check_task_status([task_id])
            print("task_id_list", task_id_list)
    crop_grids_lulc(state, district, block, asset_id)


def get_block_coordinates(state, district, block):
    soi = gpd.read_file(ADMIN_BOUNDARY_INPUT_DIR + "/soi_tehsil.geojson")

    soi = soi[(soi["STATE"].str.lower() == state)]
    soi = soi[(soi["District"].str.lower() == district)]
    soi = soi[(soi["TEHSIL"].str.lower() == block)]
    print(soi)
    coordinates = []
    for geometry in soi.geometry:
        if geometry.geom_type == "Polygon":
            # Extract exterior coordinates and convert to list of lists
            poly_coords = [list(coord) for coord in geometry.exterior.coords]
            coordinates.append(poly_coords)
            # Handle MultiPolygon geometries
        elif geometry.geom_type == "MultiPolygon":
            multi_poly_coords = []
            for polygon in geometry.geoms:
                poly_coords = [list(coord) for coord in polygon.exterior.coords]
                multi_poly_coords.append(poly_coords)
            coordinates.extend(multi_poly_coords)

    return coordinates


def gen_geojson_from_coords(path, coords):
    # Create GeoJSON directly without using the Polygon class
    feature = {
        "type": "Feature",
        "properties": {"name": "poly1", "fill": "#FF0000"},
        "geometry": {
            "type": "Polygon",
            "coordinates": coords,  # Use coords directly as it's already in the right format
        },
    }

    feature_collection = {"type": "FeatureCollection", "features": [feature]}

    with open(path + ".geojson", "w") as f:
        geojson.dump(feature_collection, f)


def gen_grids(path, coords_list):
    # Extract the inner coordinate list
    idx = 1
    for coords in coords_list:
        x_ = []
        y_ = []
        for coord in coords:
            x_.append(coord[0])  # longitude
            y_.append(coord[1])  # latitude

        min_x = min(x_)
        max_x = max(x_)
        min_y = min(y_)
        max_y = max(y_)

        grid_size = 0.004
        cover_frac = 0.3
        grid = []

        curr_x = min_x
        curr_y = min_y
        while curr_x <= max_x:
            while curr_y <= max_y:
                grid_cell = [
                    [curr_x, curr_y],
                    [curr_x + grid_size, curr_y],
                    [curr_x + grid_size, curr_y + grid_size],
                    [curr_x, curr_y + grid_size],
                    [curr_x, curr_y],
                ]
                grid.append(grid_cell)
                curr_y += grid_size
            curr_y = min_y
            curr_x += grid_size

        # Create polygon from original coordinates
        poly2 = geometry.Polygon(coords)

        final_grid = []
        features = []

        for box in grid:
            try:
                poly1 = geometry.Polygon(box)
                intersection = poly2.intersection(poly1)
                if (
                    poly1.within(poly2)
                    or intersection.area >= cover_frac * grid_size * grid_size
                ):
                    # Create GeoJSON feature directly
                    features.append(
                        {
                            "type": "Feature",
                            "properties": {},
                            "geometry": {"type": "Polygon", "coordinates": [box]},
                        }
                    )
                    final_grid.append(box)
            except Exception as e:
                print(f"Error processing grid cell: {e}")
                continue

        # with open(path + "_grids_without_LULC_" + str(idx) + ".txt", "w") as f:
        #     for item in final_grid:
        #         f.write("%s," % item)
        #
        # with open(path + "_grids_without_LULC_" + str(idx) + ".pkl", "wb") as f:
        #     pickle.dump(final_grid, f)

        feature_collection = {"type": "FeatureCollection", "features": features}

        with open(path + "_grids_without_LULC_" + str(idx) + ".geojson", "w") as f:
            json.dump(feature_collection, f)

        idx += 1


def gdf_to_ee_fc(gdf):
    features = []
    for i, row in gdf.iterrows():
        properties = row.drop("geometry").to_dict()
        geometry = ee.Geometry(row.geometry.__geo_interface__)
        feature = ee.Feature(geometry, properties)
        features.append(feature)
    return features


def convert_geojson_to_fc(state, district, block, path, geom_len):
    """Converts the GeoJSON to FeatureCollection and pushes the feature collection to the
    GEE Asset
    """
    features = []
    for idx in range(1, geom_len + 1):
        gdf = gpd.read_file(path + "_grids_without_LULC_" + str(idx) + ".geojson")
        unique_ids = []
        for i in range(gdf.shape[0]):
            unique_ids.append(block + "_" + str(i))
        gdf["uid"] = unique_ids
        gdf = gdf.to_crs("EPSG:4326")

        ee_fc = gdf_to_ee_fc(gdf)
        features.extend(ee_fc)
    print("Features' count=", len(features))

    if len(features) > 15000:
        return generate_in_chunks(block, district, features, state)
    else:
        print("Less than 15000 features")
        description = (
            "crop_grid_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower() + "_with_uid_16ha")
        )

        return generate_crop_grid_gee(state, district, block, features, description)


def generate_crop_grid_gee(state, district, block, features, description):
    if not is_gee_asset_exists(
        get_gee_asset_path(state, district, block) + description
    ):
        fc = ee.FeatureCollection(features)

        try:
            task = export_vector_asset_to_gee(
                fc,
                description,
                get_gee_asset_path(state, district, block) + description,
            )
            print("Successfully started the crop_grid")
            return task
        except Exception as e:
            print(f"Error occurred in running crop_grid task: {e}")
    return None


def generate_in_chunks(block, district, features, state):
    print("More than 15000 features")
    chunk_size = 15000
    task_list = []
    asset_ids = []
    for i in range(0, len(features), chunk_size):
        print(i)
        chunk = features[i : i + chunk_size]

        description = (
            "crop_grid_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_with_uid_16ha_"
            + str(i)
        )
        asset_ids.append(get_gee_asset_path(state, district, block) + description)

        task_id = generate_crop_grid_gee(state, district, block, chunk, description)
        if task_id:
            task_list.append(task_id)

    check_task_status(task_list)

    return merge_chunks(state, district, block, asset_ids)


def merge_chunks(state, district, block, asset_ids):
    description = (
        "crop_grid_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower() + "_with_uid_16ha")
    )

    assets = []
    for asset in asset_ids:
        assets.append(ee.FeatureCollection(asset))

    asset = ee.FeatureCollection(assets).flatten()
    asset_id = get_gee_asset_path(state, district, block) + description
    try:
        # Export an ee.FeatureCollection as an Earth Engine asset.
        task = export_vector_asset_to_gee(asset, description, asset_id)
        print("Successfully started the merge crop grid chunk")
        return task
    except Exception as e:
        print(f"Error occurred in running merge crop grid chunk task: {e}")
