import ee
from computing.lulc.v4.cropping_frequency_detection import Get_Padded_NDVI_TS_Image
from utilities.gee_utils import (
    get_gee_asset_path,
    valid_gee_text,
    is_gee_asset_exists,
    export_raster_asset_to_gee,
)
from .misc import get_points


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

    """ LULC execution for years 2017 onwards with temporal correction """

    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-07-01"

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
