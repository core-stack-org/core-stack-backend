import ee
from computing.lulc.misc import mask_s2cloud


def ndwi_based_builtup_cleaning(
    roi_boundary, prediction_image, start_date, end_date, NDWI_threshold
):
    S2_ic = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterBounds(roi_boundary)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
        .map(mask_s2cloud)
    )

    if S2_ic.size().getInfo() != 0:
        S2_ic = S2_ic.map(
            lambda img: img.addBands(
                img.normalizedDifference(["B3", "B8"]).rename("NDWI")
            )
        )
        NDWI_max_img = S2_ic.select("NDWI").max().clip(roi_boundary.geometry())

        corrected_water_img = prediction_image.select("predicted_label").where(
            prediction_image.select("predicted_label")
            .neq(0)
            .And(NDWI_max_img.gt(NDWI_threshold)),
            0,
        )
        return corrected_water_img
    else:
        print(
            "NDWI based builtup correction cannot be performed due to unavailability of Sentinel-2 data"
        )
        return prediction_image


"""
Function to clean builtup predictions using NDVI.
"""


def ndvi_based_builtup_cleaning(
    roi_boundary, prediction_image, startDate, endDate, NDVI_threshold
):
    S2_ic = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterBounds(roi_boundary)
        .filterDate(startDate, endDate)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
        .map(mask_s2cloud)
    )

    if S2_ic.size().getInfo() != 0:
        S2_ic = S2_ic.map(
            lambda img: img.addBands(
                img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            )
        )
        NDVI_max_img = S2_ic.select("NDVI").max().clip(roi_boundary.geometry())

        corrected_builtup_img = prediction_image.select("predicted_label").where(
            prediction_image.select("predicted_label")
            .neq(0)
            .And(NDVI_max_img.gt(NDVI_threshold)),
            0,
        )
        return corrected_builtup_img
    else:
        print(
            "NDVI based builtup correction cannot be performed due to unavailability of Sentinel-2 data"
        )
        return prediction_image


def get_builtup_prediction(roi_boundary, startDate, endDate):

    print("Inside builtup")
    DW_ic = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(roi_boundary)
        .filterDate(startDate, endDate)
        .select("built", "label")
    )
    print("DW_ic created")

    builtup_img = DW_ic.select("label").mode().rename("predicted_label")
    builtup_img = builtup_img.where(builtup_img.neq(6), 0)
    builtup_img = builtup_img.where(builtup_img.eq(6), 1)

    combined_builtup_img = builtup_img.clip(roi_boundary.geometry())

    ndwi_corrected_builtup_img = ndwi_based_builtup_cleaning(
        roi_boundary, combined_builtup_img, startDate, endDate, 0.25
    )
    ndvi_corrected_builtup_img = ndvi_based_builtup_cleaning(
        roi_boundary, ndwi_corrected_builtup_img, startDate, endDate, 0.5
    )
    print("exiting builtup")
    return ndvi_corrected_builtup_img
