import ee
from .misc import mask_s2cloud
from datetime import datetime
import pandas as pd


def fill_empty_bands(image):
    band_names = image.bandNames()
    zero_img = image.select(0).multiply(0).rename("constant").toDouble()
    zero_img_masked = zero_img.updateMask(zero_img)
    image = ee.Algorithms.If(
        ee.List(band_names).contains(ee.String("VV")),
        image,
        ee.Image(image).addBands(zero_img_masked.select("constant").rename("VV")),
    )
    image = ee.Algorithms.If(
        ee.List(band_names).contains(ee.String("VH")),
        image,
        ee.Image(image).addBands(zero_img_masked.select("constant").rename("VH")),
    )
    return image


def Get_S1_ImageCollections(inputStartDate, inputEndDate, roi_boundary):
    S1 = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filterDate(inputStartDate, inputEndDate)
        .filterBounds(roi_boundary)
    )

    S1_processed = S1.map(fill_empty_bands)
    return S1_processed


def GetVV_VH_image_datewise(S1_ic):
    def get_VV_VH_datewise(date):
        zero_img = S1_ic.first().select("VV", "VH").multiply(0)
        zero_img_masked = zero_img.updateMask(zero_img)

        subset_ic = S1_ic.select(["VV", "VH"]).filterDate(
            ee.Date(date), ee.Date(date).advance(16, "day")
        )
        image = ee.Algorithms.If(
            ee.Number(subset_ic.size()).gt(0),
            subset_ic.mean().set("system:time_start", ee.Date(date).millis()),
            zero_img.set("system:time_start", ee.Date(date).millis()),
        )

        return image

    return get_VV_VH_datewise


def Get_S1_16Day_VV_VH_TimeSeries(inputStartDate, inputEndDate, S1_ic):
    startDate = datetime.strptime(inputStartDate, "%Y-%m-%d")
    endDate = datetime.strptime(inputEndDate, "%Y-%m-%d")

    date_list = pd.date_range(start=startDate, end=endDate, freq="16D").tolist()
    date_list = ee.List(
        [datetime.strftime(curr_date, "%Y-%m-%d") for curr_date in date_list]
    )

    S1_TS = ee.ImageCollection.fromImages(date_list.map(GetVV_VH_image_datewise(S1_ic)))
    return S1_TS


def add_sarImg_timestamp(image):
    timeImage = image.metadata("system:time_start").rename("timestamp")
    timeImageMasked = timeImage.updateMask(image.mask().select(0))
    return image.addBands(timeImageMasked)


def performInterpolation_sarTS(image):
    image = ee.Image(image)
    beforeImages = ee.List(image.get("before"))
    beforeMosaic = ee.ImageCollection.fromImages(beforeImages).mosaic()
    afterImages = ee.List(image.get("after"))
    afterMosaic = ee.ImageCollection.fromImages(afterImages).mosaic()

    # Interpolation formula
    # y = y1 + (y2-y1)*((t – t1) / (t2 – t1))
    # y = interpolated image
    # y1 = before image
    # y2 = after image
    # t = interpolation timestamp
    # t1 = before image timestamp
    # t2 = after image timestamp

    t1 = beforeMosaic.select("timestamp").rename("t1")
    t2 = afterMosaic.select("timestamp").rename("t2")
    t = image.metadata("system:time_start").rename("t")
    timeImage = ee.Image.cat([t1, t2, t])
    timeRatio = timeImage.expression(
        "(t - t1) / (t2 - t1)",
        {
            "t": timeImage.select("t"),
            "t1": timeImage.select("t1"),
            "t2": timeImage.select("t2"),
        },
    )

    interpolated = beforeMosaic.add(
        (afterMosaic.subtract(beforeMosaic).multiply(timeRatio))
    )
    result = image.unmask(interpolated)

    # Saketh
    # For data points on either end of time-series
    # Before or After mosaics may still have gaps (owing to few/no images in the window)
    # Simply fill with after mosaic (for first few data points) and before mosaic (for last few datapoints)
    fill_value = ee.ImageCollection([beforeMosaic, afterMosaic]).mosaic()
    result = result.unmask(fill_value)

    return result.copyProperties(image, ["system:time_start"])


def interpolate_sar_timeseries(S1_TS):
    filtered = S1_TS.map(add_sarImg_timestamp)

    # Time window in which we are willing to look forward and backward for unmasked pixel in time series
    timeWindow = 120

    # Define a maxDifference filter to find all images within the specified days. Convert days to milliseconds.
    millis = ee.Number(timeWindow).multiply(1000 * 60 * 60 * 24)
    # Filter says that pick only those timestamps which lie between the 2 timestamps not more than millis difference apart
    maxDiffFilter = ee.Filter.maxDifference(
        difference=millis,
        leftField="system:time_start",
        rightField="system:time_start",
    )

    # Filter to find all images after a given image. Compare the image's timstamp against other images.
    # Images ahead of target image should have higher timestamp.
    lessEqFilter = ee.Filter.lessThanOrEquals(
        leftField="system:time_start", rightField="system:time_start"
    )

    # Similarly define this filter to find all images before a given image
    greaterEqFilter = ee.Filter.greaterThanOrEquals(
        leftField="system:time_start", rightField="system:time_start"
    )

    # Apply first join to find all images that are after the target image but within the timeWindow
    filter1 = ee.Filter.And(maxDiffFilter, lessEqFilter)
    join1 = ee.Join.saveAll(
        matchesKey="after", ordering="system:time_start", ascending=False
    )
    join1Result = join1.apply(primary=filtered, secondary=filtered, condition=filter1)

    # Apply first join to find all images that are after the target image but within the timeWindow
    filter2 = ee.Filter.And(maxDiffFilter, greaterEqFilter)
    join2 = ee.Join.saveAll(
        matchesKey="before", ordering="system:time_start", ascending=True
    )
    join2Result = join2.apply(
        primary=join1Result, secondary=join1Result, condition=filter2
    )

    interpolated_S1_TS = ee.ImageCollection(join2Result.map(performInterpolation_sarTS))

    return interpolated_S1_TS


def get_trained_model(training_data_assetpath):
    print("inside create trained model")
    training_data = ee.FeatureCollection(training_data_assetpath)
    print("Featurecollection for training data created")
    training_band_names = [
        "0_VV",
        "1_VV",
        "2_VV",
        "3_VV",
        "4_VV",
        "5_VV",
        "6_VV",
        "7_VV",
        "8_VV",
        "9_VV",
        "10_VV",
        "11_VV",
        "12_VV",
        "13_VV",
        "14_VV",
        "15_VV",
        "16_VV",
        "17_VV",
        "18_VV",
        "19_VV",
        "20_VV",
        "21_VV",
        "22_VV",
        "0_VH",
        "1_VH",
        "2_VH",
        "3_VH",
        "4_VH",
        "5_VH",
        "6_VH",
        "7_VH",
        "8_VH",
        "9_VH",
        "10_VH",
        "11_VH",
        "12_VH",
        "13_VH",
        "14_VH",
        "15_VH",
        "16_VH",
        "17_VH",
        "18_VH",
        "19_VH",
        "20_VH",
        "21_VH",
        "22_VH",
    ]

    trained_model = (
        ee.Classifier.smileRandomForest(numberOfTrees=100, seed=42)
        .setOutputMode("MULTIPROBABILITY")
        .train(
            features=training_data,
            classProperty="class",
            inputProperties=training_band_names,
        )
    )
    print("trained model created")

    return trained_model


def Get_slope(roi_boundary):
    dem = ee.Image("CGIAR/SRTM90_V4")
    slope = ee.Terrain.slope(dem)
    slope_image = slope.clip(roi_boundary.geometry())
    return slope_image


"""
Function to clean cropland predictions using NDVI.
"""


def ndvi_based_cropland_cleaning(
    roi_boundary, prediction_image, startDate, endDate, NDVI_threshold
):
    S2_ic = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterBounds(roi_boundary)
        .filterDate(startDate, endDate)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
        .map(mask_s2cloud)
        .select(["B4", "B8"])
    )

    if S2_ic.size().getInfo():
        S2_ic = S2_ic.map(
            lambda img: img.addBands(
                img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            )
        )
        NDVI_max_img = S2_ic.select("NDVI").max().clip(roi_boundary.geometry())

        # Get barrenlands out as label 7
        corrected_cropland_img = prediction_image.select("predicted_label").where(
            (prediction_image.select("predicted_label").eq(5)).And(
                NDVI_max_img.lt(NDVI_threshold)
            ),
            7,
        )

        return corrected_cropland_img
    else:
        print(
            "NDVI based cropland correction cannot be performed due to unavailability of Sentinel-2 data"
        )
        return prediction_image


"""
Function to clean forest/tree predictions using NDVI.
"""


def ndvi_based_forest_cleaning(
    roi_boundary, prediction_image, startDate, endDate, NDVI_threshold
):
    S2_ic = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterBounds(roi_boundary)
        .filterDate(startDate, endDate)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))
        .map(mask_s2cloud)
        .select(["B4", "B8"])
    )

    if S2_ic.size().getInfo():
        S2_ic = S2_ic.map(
            lambda img: img.addBands(
                img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            )
        )
        NDVI_max_img = S2_ic.select("NDVI").max().clip(roi_boundary.geometry())

        # Get barrenlands out as label 7
        corrected_forest_img = prediction_image.select("predicted_label").where(
            (prediction_image.select("predicted_label").eq(6)).And(
                NDVI_max_img.lt(NDVI_threshold)
            ),
            7,
        )

        return corrected_forest_img
    else:
        print(
            "NDVI based forest correction cannot be performed due to unavailability of Sentinel-2 data"
        )
        return prediction_image


def get_cropland_prediction(startDate, endDate, roi_boundary):
    print("inside cropland")
    training_data_assetpath = "projects/ee-indiasat/assets/Rasterized_Groundtruth/L2_TrainingData_SAR_TimeSeries_1Year"
    trained_model = get_trained_model(training_data_assetpath)
    print("trained model done")
    S1_ic = Get_S1_ImageCollections(startDate, endDate, roi_boundary)
    print("step 2")
    S1_TS = Get_S1_16Day_VV_VH_TimeSeries(startDate, endDate, S1_ic)
    print("step 3")
    interpolated_S1_TS = interpolate_sar_timeseries(S1_TS)
    print("step 4")
    S1_TS_img = interpolated_S1_TS.toBands()
    S1_VV_TS_img = S1_TS_img.select([".*_VV"])
    S1_VH_TS_img = S1_TS_img.select([".*_VH"])

    training_band_names = [
        "0_VV",
        "1_VV",
        "2_VV",
        "3_VV",
        "4_VV",
        "5_VV",
        "6_VV",
        "7_VV",
        "8_VV",
        "9_VV",
        "10_VV",
        "11_VV",
        "12_VV",
        "13_VV",
        "14_VV",
        "15_VV",
        "16_VV",
        "17_VV",
        "18_VV",
        "19_VV",
        "20_VV",
        "21_VV",
        "22_VV",
        "0_VH",
        "1_VH",
        "2_VH",
        "3_VH",
        "4_VH",
        "5_VH",
        "6_VH",
        "7_VH",
        "8_VH",
        "9_VH",
        "10_VH",
        "11_VH",
        "12_VH",
        "13_VH",
        "14_VH",
        "15_VH",
        "16_VH",
        "17_VH",
        "18_VH",
        "19_VH",
        "20_VH",
        "21_VH",
        "22_VH",
    ]

    training_img = (
        S1_VV_TS_img.addBands(S1_VH_TS_img)
        .select(training_band_names)
        .clip(roi_boundary.geometry())
    )
    classified_image = training_img.classify(trained_model)

    roi_label_image = (
        classified_image.select(["classification"])
        .arrayArgmax()
        .arrayFlatten([["predicted_label"]])
    )
    roi_label_image = roi_label_image.add(5).toInt8()

    slope_img = Get_slope(roi_boundary)
    combined_img = roi_label_image.addBands(slope_img)

    # check if the slope is >20 deg, re-classify the pixel from cropland to non-cropland
    final_classified_img = combined_img.select(["predicted_label"]).where(
        combined_img.select("predicted_label")
        .eq(5)
        .And(combined_img.select("slope").gte(30)),
        6,
    )
    print("before cleaning")

    cropland_corrected_img = ndvi_based_cropland_cleaning(
        roi_boundary, final_classified_img, startDate, endDate, NDVI_threshold=0.15
    )
    print("cropland corrected")
    forest_corrected_img = ndvi_based_forest_cleaning(
        roi_boundary, cropland_corrected_img, startDate, endDate, NDVI_threshold=0.3
    )
    print("forest corrected")

    return forest_corrected_img
