import ee
import os
import pandas as pd
from dateutil.relativedelta import relativedelta
import math
from itertools import product
from pathlib import Path
import ast
from computing.lulc.v4.cropping_frequency_detection import Get_Padded_NDVI_TS_Image
from utilities.gee_utils import get_gee_asset_path, valid_gee_text, is_gee_asset_exists


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

    def get_points(roi):
        file_path = os.path.join("data/lulc_v4", directory)
        if not os.path.exists(file_path):
            os.mkdir(file_path)
        points_file = Path(file_path + "/status.csv")
        if points_file.is_file():
            df = pd.read_csv(file_path + "/status.csv", index_col=False)
            df["points"] = df["points"].apply(ast.literal_eval)
            return df
        zoom = 17
        scale = 16
        bounds = roi.bounds().coordinates().get(0).getInfo()
        lons = sorted([i[0] for i in bounds])
        lats = sorted([i[1] for i in bounds])
        starting_point = lats[-1], lons[0]
        min_, max_ = (
            [lon_to_pixel_x(lons[0], zoom), lat_to_pixel_y(lats[0], zoom)],
            [lon_to_pixel_x(lons[-1], zoom), lat_to_pixel_y(lats[-1], zoom)],
        )
        iterations = math.ceil(
            max(abs(min_[0] - max_[0]), abs(min_[1] - max_[1])) / 256 / 16
        )
        points = get_n_boxes(
            starting_point[0], starting_point[1], iterations, zoom, scale
        )
        intersect_list = []
        print(len(points))
        index = 0
        for point in points:
            top_left = point
            bottom_right = lat_lon_from_pixel(top_left[0], top_left[1], zoom, scale)
            rectangle = ee.Geometry.Rectangle(
                [(top_left[1], top_left[0]), (bottom_right[1], bottom_right[0])]
            )
            print(top_left, bottom_right)
            intersects = (
                roi.geometry().intersects(rectangle, ee.ErrorMargin(1)).getInfo()
            )
            if intersects:
                intersect_list.append((index, (top_left, bottom_right)))
                index += 1
            print(intersects)
        df = pd.DataFrame(intersect_list, columns=["index", "points"])
        # df["overall_status"] = False
        # df["download_status"] = False
        # df["model_status"] = False
        # df["segmentation_status"] = False
        # df["postprocessing_status"] = False
        # df["plantation_status"] = False
        df.to_csv("data/lulc_v4/" + directory + "/status.csv", index=False)
        return df

    blocks_df = get_points(roi_boundary)
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
    task = ee.batch.Export.image.toAsset(
        image=ts_data.clip(roi_boundary.geometry()),
        description=description,
        assetId=asset_id,
        # pyramidingPolicy = {'predicted_label': 'mode'},
        scale=10,
        maxPixels=1e13,
        crs="EPSG:4326",
    )
    task.start()

    return task.status()["id"]
