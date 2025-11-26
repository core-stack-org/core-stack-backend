import ee
import os
import pandas as pd
import math
from itertools import product
from pathlib import Path
import ast

from utilities.gee_utils import (
    download_csv_from_gcs,
)


# Function to convert latitude to pixel Y at a given zoom level
def lat_to_pixel_y(lat, zoom):
    sin_lat = math.sin(math.radians(lat))
    pixel_y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * (
            2 ** (zoom + 8)
    )
    return pixel_y


# Function to convert longitude to pixel X at a given zoom level
def lon_to_pixel_x(lon, zoom):
    pixel_x = ((lon + 180) / 360) * (2 ** (zoom + 8))
    return pixel_x


# Function to convert pixel X to longitude
def pixel_x_to_lon(pixel_x, zoom):
    lon = (pixel_x / (2 ** (zoom + 8))) * 360 - 180
    return lon


# Function to convert pixel Y to latitude
def pixel_y_to_lat(pixel_y, zoom):
    n = math.pi - 2 * math.pi * pixel_y / (2 ** (zoom + 8))
    lat = math.degrees(math.atan(math.sinh(n)))
    return lat


def lat_lon_from_pixel(lat, lon, zoom, scale):
    """
    Given a starting latitude and longitude, calculate the latitude and longitude
    of the opposite corner of a 256x256 image at a given zoom level.
    """
    pixel_x = lon_to_pixel_x(lon, zoom)
    pixel_y = lat_to_pixel_y(lat, zoom)

    new_lon = pixel_x_to_lon(pixel_x + 256 * scale, zoom)
    new_lat = pixel_y_to_lat(pixel_y + 256 * scale, zoom)

    return new_lat, new_lon


"""

Helper function for dividing an roi into blocks

"""


def get_n_boxes(lat, lon, n, zoom, scale):
    diagonal_lat_lon = [
        (lat, lon),
    ]
    for i in range(n):
        new_lat_lon = lat_lon_from_pixel(lat, lon, zoom, scale)
        diagonal_lat_lon.append(new_lat_lon)
        lat, lon = new_lat_lon
    lats = [i[0] for i in diagonal_lat_lon]
    longs = [i[1] for i in diagonal_lat_lon]
    return list(product(lats, longs))


def latlon_to_tile_xy(lat, lon, zoom):
    """Converts lat/lon to tile x/y at given zoom level"""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    tile_x = int((lon + 180.0) / 360.0 * n)
    tile_y = int(
        (1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n
    )
    return tile_x, tile_y


def tile_xy_to_latlon(tile_x, tile_y, zoom):
    """Converts top-left corner of tile x/y at given zoom level to lat/lon"""
    n = 2.0 ** zoom
    lon_deg = tile_x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def get_points(roi, zoom, scale, directory):
    file_path = os.path.join("data/lulc_v4", directory)
    if not os.path.exists(file_path):
        os.mkdir(file_path)
    download_csv_from_gcs(
        "shapefiles", f"{directory}/status.csv", f"{file_path}/status.csv"
    )
    points_file = Path(file_path + "/status.csv")
    if points_file.is_file():
        df = pd.read_csv(file_path + "/status.csv", index_col=False)
        df["points"] = df["points"].apply(ast.literal_eval)
        return df

    bounds = roi.bounds().coordinates().get(0).getInfo()
    lons = sorted([i[0] for i in bounds])
    lats = sorted([i[1] for i in bounds])

    tile_x, tile_y = latlon_to_tile_xy(lats[-1], lons[0], zoom)
    top_left_lat, top_left_lon = tile_xy_to_latlon(tile_x, tile_y, zoom)

    starting_point = top_left_lat, top_left_lon

    min_, max_ = (
        [lon_to_pixel_x(top_left_lon, zoom), lat_to_pixel_y(lats[0], zoom)],
        [lon_to_pixel_x(lons[-1], zoom), lat_to_pixel_y(top_left_lat, zoom)],
    )
    iterations = math.ceil(
        max(abs(min_[0] - max_[0]), abs(min_[1] - max_[1])) / 256 / 16
    )
    points = get_n_boxes(starting_point[0], starting_point[1], iterations, zoom, scale)
    intersect_list = []
    # print(len(points))
    index = 0
    for point in points:
        top_left = point
        bottom_right = lat_lon_from_pixel(top_left[0], top_left[1], zoom, scale)
        rectangle = ee.Geometry.Rectangle(
            [(top_left[1], top_left[0]), (bottom_right[1], bottom_right[0])]
        )
        # print(top_left, bottom_right)
        intersects = roi.geometry().intersects(rectangle, ee.ErrorMargin(1)).getInfo()
        if intersects:
            intersect_list.append((index, (top_left, bottom_right)))
            index += 1
        # print(intersects)
    df = pd.DataFrame(intersect_list, columns=["index", "points"])
    df.to_csv("data/lulc_v4/" + directory + "/status.csv", index=False)
    return df
