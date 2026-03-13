import ee
from utilities.constants import GEE_DATASET_PATH


def mask_s2cloud(image):
    """
    Function to mask clouds based on the QA60 band of Sentinel SR data.
    param {ee.Image} image Input Sentinel SR image
    return {ee.Image} Cloudmasked Sentinel-2 image
    """
    qa = image.select("QA60")
    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
    return image.updateMask(mask).divide(10000)


def get_barrenland_prediction(roi_boundary, start_date, end_date):
    DW_ic = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(roi_boundary)
        .filterDate(start_date, end_date)
        .select("bare", "label")
    )

    bare_img = DW_ic.select("label").mode().rename("predicted_label")
    bare_img = bare_img.where(bare_img.neq(7), 0)

    bare_img = bare_img.clip(roi_boundary.geometry())

    return bare_img


def dw_based_shrub_cleaning(
    roi_boundary, current_prediction_output, start_date, end_date
):
    DW_ic = (
        ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
        .filterBounds(roi_boundary)
        .filterDate(start_date, end_date)
        .select("shrub_and_scrub", "label")
    )

    bare_img = (
        DW_ic.select("label")
        .mode()
        .rename("predicted_label")
        .clip(roi_boundary.geometry())
    )
    corrected_output = current_prediction_output.select("predicted_label").where(
        (current_prediction_output.select("predicted_label").eq(8)).And(
            bare_img.select("predicted_label").eq(5)
        ),
        12,
    )

    return corrected_output


def remap_values(image):
    remapped = image.remap(
        [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
        [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12],
        0,
        "predicted_label",
    )
    remapped = remapped.select(["remapped"]).rename(["predicted_label"])
    return remapped


def clip_lulc_from_river_basin(
    river_basin, roi, scale, start_date, end_date, version="v3"
):
    # Finding the river basin in which roi lies
    basin = river_basin.filterBounds(roi.geometry())
    basin_size = basin.size().getInfo()
    if basin_size > 1:
        lulc_collection = []
        for i in range(basin_size):
            bs = ee.Feature(basin.toList(basin.size()).get(i))
            lulc_name = (
                str(bs.get("objectid").getInfo())
                + "_"
                + bs.get("ba_name").getInfo()
                + "_"
                + start_date
                + "_"
                + end_date
                + "_LULCmap_"
                + str(scale)
                + ("m_v2" if version == "v2" else "m")
            )

            # Get the image first
            image = ee.Image(
                GEE_DATASET_PATH
                + (
                    "/LULC_v2_river_basin/"
                    if version == "v2"
                    else "/LULC_v3_river_basin/"
                )
                + lulc_name
            )

            # Ensure the geometry is bounded by getting the intersection
            clip_geometry = roi.geometry().intersection(bs.geometry())

            # Clip with the bounded geometry
            clipped_image = image.clip(clip_geometry)
            lulc_collection.append(clipped_image)

        collection = ee.ImageCollection(lulc_collection)
        lulc = collection.mosaic().clip(roi.geometry())
    else:
        lulc_name = (
            str(basin.first().get("objectid").getInfo())
            + "_"
            + basin.first().get("ba_name").getInfo()
            + "_"
            + start_date
            + "_"
            + end_date
            + "_LULCmap_"
            + str(scale)
            + ("m_v2" if version == "v2" else "m")
        )
        # Clipping river basin lulc for a particular geometry
        lulc = ee.Image(
            GEE_DATASET_PATH
            + ("/LULC_v2_river_basin/" if version == "v2" else "/LULC_v3_river_basin/")
            + lulc_name
        ).clip(roi.geometry())
    return lulc
