import ee
from .misc import mask_s2cloud

"""
Function to get the first date of the month of input start date and the last date of this month.
It is used to advance the time range by 1 month in future code.
"""


def get_start_and_end_of_month(input_date):
    year = input_date.get("year")
    month = input_date.get("month")

    start_of_month = ee.Date.fromYMD(year, month, 1)
    end_of_month = start_of_month.advance(1, "month").advance(-1, "day")

    return start_of_month, end_of_month


"""
Function to get water body predictions in kharif using Sentinel-1 SAR data.
"""


def get_kharif_bodies(roi_boundary, start_date, end_date):
    SAR_ic = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(roi_boundary)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
    )

    kharif_month1_ic = SAR_ic.filterDate(start_date, end_date)
    kharif_month2_ic = SAR_ic.filterDate(
        start_date.advance(1, "month"), end_date.advance(1, "month")
    )
    kharif_month3_ic = SAR_ic.filterDate(
        start_date.advance(2, "month"), end_date.advance(2, "month")
    )
    kharif_month4_ic = SAR_ic.filterDate(
        start_date.advance(3, "month"), end_date.advance(3, "month")
    )

    ###
    ## Compute water mask
    ###
    kharif_month1_waterImg = (
        kharif_month1_ic.map(
            lambda img: img.addBands(img.select("VV").lt(-16).rename("Water"))
        )
        .select("Water")
        .mode()
    )
    kharif_month2_waterImg = (
        kharif_month2_ic.map(
            lambda img: img.addBands(img.select("VV").lt(-16).rename("Water"))
        )
        .select("Water")
        .mode()
    )
    kharif_month3_waterImg = (
        kharif_month3_ic.map(
            lambda img: img.addBands(img.select("VV").lt(-16).rename("Water"))
        )
        .select("Water")
        .mode()
    )
    kharif_month4_waterImg = (
        kharif_month4_ic.map(
            lambda img: img.addBands(img.select("VV").lt(-16).rename("Water"))
        )
        .select("Water")
        .mode()
    )

    kharif_ic = (
        ee.ImageCollection(kharif_month1_waterImg)
        .merge(kharif_month2_waterImg)
        .merge(kharif_month3_waterImg)
        .merge(kharif_month4_waterImg)
    )
    kharif_water_sum = kharif_ic.reduce(ee.Reducer.sum())
    kharif_water_mask = (
        kharif_water_sum.clip(roi_boundary.geometry()).gte(3).rename("Water")
    )

    return kharif_water_mask


"""
Function to get water body predictions in Rabi using Dynamic World.
"""


def get_rabi_bodies(roi_boundary, start_date, end_date):
    DW_ic = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(roi_boundary)
        .select(["label", "water"])
    )

    rabi_month1_ic = DW_ic.filterDate(
        start_date.advance(4, "month"), end_date.advance(4, "month")
    )
    rabi_month2_ic = DW_ic.filterDate(
        start_date.advance(5, "month"), end_date.advance(5, "month")
    )
    rabi_month3_ic = DW_ic.filterDate(
        start_date.advance(6, "month"), end_date.advance(6, "month")
    )
    rabi_month4_ic = DW_ic.filterDate(
        start_date.advance(7, "month"), end_date.advance(7, "month")
    )

    rabi_month1_img = (
        ee.Image(
            ee.Algorithms.If(
                rabi_month1_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                rabi_month1_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    rabi_month2_img = (
        ee.Image(
            ee.Algorithms.If(
                rabi_month2_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                rabi_month2_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    rabi_month3_img = (
        ee.Image(
            ee.Algorithms.If(
                rabi_month3_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                rabi_month3_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    rabi_month4_img = (
        ee.Image(
            ee.Algorithms.If(
                rabi_month4_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                rabi_month4_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    rabi_ic = (
        ee.ImageCollection(rabi_month1_img)
        .merge(rabi_month2_img)
        .merge(rabi_month3_img)
        .merge(rabi_month4_img)
    )
    rabi_water_mask = rabi_ic.reduce(ee.Reducer.sum()).gte(2).rename("Water")

    return rabi_water_mask


"""
Function to get water body predictions in Zaid using Dynamic World.
"""


def get_zaid_bodies(roi_boundary, start_date, end_date):
    DW_ic = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(roi_boundary)
        .select(["label", "water"])
    )

    zaid_month1_ic = DW_ic.filterDate(
        start_date.advance(8, "month"), end_date.advance(8, "month")
    )
    zaid_month2_ic = DW_ic.filterDate(
        start_date.advance(9, "month"), end_date.advance(9, "month")
    )
    zaid_month3_ic = DW_ic.filterDate(
        start_date.advance(10, "month"), end_date.advance(10, "month")
    )
    zaid_month4_ic = DW_ic.filterDate(
        start_date.advance(11, "month"), end_date.advance(11, "month")
    )

    zaid_month1_img = (
        ee.Image(
            ee.Algorithms.If(
                zaid_month1_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                zaid_month1_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    zaid_month2_img = (
        ee.Image(
            ee.Algorithms.If(
                zaid_month2_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                zaid_month2_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    zaid_month3_img = (
        ee.Image(
            ee.Algorithms.If(
                zaid_month3_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                zaid_month3_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    zaid_month4_img = (
        ee.Image(
            ee.Algorithms.If(
                zaid_month4_ic.size().eq(0),
                ee.Image.constant(0).rename("label"),
                zaid_month4_ic.select("label").mode().add(1),
            )
        )
        .clip(roi_boundary.geometry())
        .select("label")
        .eq(1)
    )

    zaid_ic = (
        ee.ImageCollection(zaid_month1_img)
        .merge(zaid_month2_img)
        .merge(zaid_month3_img)
        .merge(zaid_month4_img)
    )
    zaid_water_mask = zaid_ic.reduce(ee.Reducer.sum()).gte(2).rename("Water")

    return zaid_water_mask


"""
Function to clean water predictions using NDWI.
"""


def ndwi_based_water_cleaning(
    roi_boundary, prediction_image, startDate, endDate, NDWI_threshold
):
    S2_ic = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterBounds(roi_boundary)
        .filterDate(startDate, endDate)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
        .map(mask_s2cloud)
        .select(["B3", "B8"])
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
            .And(NDWI_max_img.lt(NDWI_threshold)),
            0,
        )
        return corrected_water_img
    else:
        print(
            "NDWI based water correction cannot be performed due to unavailability of Sentinel-2 data"
        )
        return prediction_image


"""
Main function to perform water classification
"""


def get_water_prediction(roi_boundary, startDate, endDate):
    start_date, end_date = get_start_and_end_of_month(ee.Date(startDate))

    kharif_water_img = get_kharif_bodies(roi_boundary, start_date, end_date)
    rabi_water_img = get_rabi_bodies(roi_boundary, start_date, end_date)
    zaid_water_img = get_zaid_bodies(roi_boundary, start_date, end_date)

    kharif_water = kharif_water_img.select("Water").rename("predicted_label")
    rabi_water = rabi_water_img.select("Water").rename("predicted_label")
    zaid_water = zaid_water_img.select("Water").rename("predicted_label")
    combined_water_img = (
        kharif_water.where(kharif_water, 2).where(rabi_water, 3).where(zaid_water, 4)
    )

    # Clean the water predictions based on confidence and NDWI
    ndwi_corrected_img = ndwi_based_water_cleaning(
        roi_boundary, combined_water_img, startDate, endDate, 0.15
    )

    return ndwi_corrected_img
