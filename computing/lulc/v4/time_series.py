import ee
import os
import pandas as pd
import math
from itertools import product
from pathlib import Path
import ast
from computing.lulc.v4.cropping_frequency_detection import Get_Padded_NDVI_TS_Image
from utilities.gee_utils import (
    get_gee_asset_path,
    valid_gee_text,
    is_gee_asset_exists,
    export_raster_asset_to_gee,
    download_csv_from_gcs,
)


def time_series(state, district, block, start_year, end_year):
    directory = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    description = "ts_data_" + directory
    asset_id = get_gee_asset_path(state, district, block) + description

    if is_gee_asset_exists(asset_id):
        return

    roi_boundary = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    ).union()

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
        n = 2.0**zoom
        tile_x = int((lon + 180.0) / 360.0 * n)
        tile_y = int(
            (1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi)
            / 2.0
            * n
        )
        return tile_x, tile_y

    def tile_xy_to_latlon(tile_x, tile_y, zoom):
        """Converts top-left corner of tile x/y at given zoom level to lat/lon"""
        n = 2.0**zoom
        lon_deg = tile_x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n)))
        lat_deg = math.degrees(lat_rad)
        return lat_deg, lon_deg

    def get_points(roi, zoom, scale):
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
        points = get_n_boxes(
            starting_point[0], starting_point[1], iterations, zoom, scale
        )
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
            intersects = (
                roi.geometry().intersects(rectangle, ee.ErrorMargin(1)).getInfo()
            )
            if intersects:
                intersect_list.append((index, (top_left, bottom_right)))
                index += 1
            # print(intersects)
        df = pd.DataFrame(intersect_list, columns=["index", "points"])
        df.to_csv("data/lulc_v4/" + directory + "/status.csv", index=False)
        return df

    blocks_df = get_points(roi_boundary, 17, 16)
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

    """ LULC execution for years 2017 onwards with temporal correction """

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-07-01"

    # loopStart = start_date
    # loopEnd = (datetime.strptime(end_date, "%Y-%m-%d")).strftime("%Y-%m-%d")
    #
    # while loopStart != loopEnd:
    #     currStartDate = datetime.strptime(loopStart, "%Y-%m-%d")
    #     currEndDate = currStartDate + relativedelta(years=1) - timedelta(days=1)
    #
    #     loopStart = (currStartDate + relativedelta(years=1)).strftime("%Y-%m-%d")
    #
    #     currStartDate = currStartDate.strftime("%Y-%m-%d")
    #     currEndDate = currEndDate.strftime("%Y-%m-%d")
    #
    #     print(
    #         "\n EXECUTING LULC PREDICTION FOR ",
    #         currStartDate,
    #         " TO ",
    #         currEndDate,
    #         "\n",
    #     )
    #
    #     # curr_filename = directory + "_" + currStartDate + "_" + currEndDate
    #
    #     if datetime.strptime(currStartDate, "%Y-%m-%d").year < 2017:
    #         print(
    #             "To generate LULC output of year ",
    #             datetime.strptime(currStartDate, "%Y-%m-%d").year,
    #             " , go to cell-LULC execution for years before 2017",
    #         )
    #         continue
    ts_data, _ = Get_Padded_NDVI_TS_Image(start_date, end_date, roi_boundary)

    ts_data = ts_data.select(ts_data.bandNames().getInfo()).rename(
        [
            "_".join(i.split("_")[1:]) + "_" + i.split("_")[0]
            for i in ts_data.bandNames().getInfo()
        ]
    )
    task_id = export_raster_asset_to_gee(
        image=ts_data.clip(roi_boundary.geometry()),
        description=description,
        asset_id=asset_id,
        scale=10,
        region=roi_boundary.geometry(),
    )

    return task_id
