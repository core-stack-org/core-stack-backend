import ee
from datetime import datetime, timedelta
import pandas as pd

chastainBandNames = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"]

# Regression model parameters from Table-4. MSI TOA reflectance as a function of OLI TOA reflectance.
msiOLISlopes = [1.0946, 1.0043, 1.0524, 0.8954, 1.0049, 1.0002]
msiOLIIntercepts = [-0.0107, 0.0026, -0.0015, 0.0033, 0.0065, 0.0046]

# Regression model parameters from Table-5. MSI TOA reflectance as a function of ETM+ TOA reflectance.
msiETMSlopes = [1.10601, 0.99091, 1.05681, 1.0045, 1.03611, 1.04011]
msiETMIntercepts = [-0.0139, 0.00411, -0.0024, -0.0076, 0.00411, 0.00861]

# Regression model parameters from Table-6. OLI TOA reflectance as a function of ETM+ TOA reflectance.
oliETMSlopes = [1.03501, 1.00921, 1.01991, 1.14061, 1.04351, 1.05271]
oliETMIntercepts = [-0.0055, -0.0008, -0.0021, -0.0163, -0.0045, 0.00261]

# Construct dictionary to handle all pairwise combos
chastainCoeffDict = {
    "MSI_OLI": [
        msiOLISlopes,
        msiOLIIntercepts,
        1,
    ],  # check what the third item corresponds to
    "MSI_ETM": [msiETMSlopes, msiETMIntercepts, 1],
    "OLI_ETM": [oliETMSlopes, oliETMIntercepts, 1],
    "OLI_MSI": [msiOLISlopes, msiOLIIntercepts, 0],
    "ETM_MSI": [msiETMSlopes, msiETMIntercepts, 0],
    "ETM_OLI": [oliETMSlopes, oliETMIntercepts, 0],
}


"""
Function to mask cloudy pixels in Landsat-7
"""


def maskL7cloud(image):
    qa = image.select("QA_PIXEL")
    mask = qa.bitwiseAnd(1 << 4).eq(0)
    return (
        image.updateMask(mask)
        .select(["B1", "B2", "B3", "B4", "B5", "B7"])
        .rename("BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2")
    )


"""
Function to mask cloudy pixels in Landsat-8
"""


def maskL8cloud(image):
    qa = image.select("QA_PIXEL")
    mask = qa.bitwiseAnd(1 << 4).eq(0)
    return (
        image.updateMask(mask)
        .select(["B2", "B3", "B4", "B5", "B6", "B7"])
        .rename("BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2")
    )


"""
Function to mask clouds using the quality band of Sentinel-2 TOA
"""


def maskS2cloudTOA(image):
    qa = image.select("QA60")
    # Bits 10 and 11 are clouds and cirrus, respectively.
    cloudBitMask = 1 << 10
    cirrusBitMask = 1 << 11
    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
    return (
        image.updateMask(mask)
        .select(["B2", "B3", "B4", "B8", "B11", "B12"])
        .rename(["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])
    )


"""
Get Landsat and Sentinel image collections
"""


def Get_L7_L8_S2_ImageCollections(inputStartDate, inputEndDate, roi_boundary):
    # ------ Landsat 7 TOA
    L7 = (
        ee.ImageCollection("LANDSAT/LE07/C02/T1_TOA")
        .filterDate(inputStartDate, inputEndDate)
        .filterBounds(roi_boundary)
        .map(maskL7cloud)
    )
    # print('\n Original Landsat 7 TOA dataset: \n',L7.limit(1).getInfo())
    # print('Number of images in Landsat 7 TOA dataset: \t',L7.size().getInfo())

    # ------ Landsat 8 TOA
    L8 = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_TOA")
        .filterDate(inputStartDate, inputEndDate)
        .filterBounds(roi_boundary)
        .map(maskL8cloud)
    )
    # print('\n Original Landsat 8 TOA dataset: \n', L8.limit(1).getInfo())
    # print('Number of images in Landsat 8 TOA dataset: \t',L8.size().getInfo())

    # ------ Sentinel-2 TOA
    S2 = (
        ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
        .filterDate(inputStartDate, inputEndDate)
        .filterBounds(roi_boundary)
        .map(maskS2cloudTOA)
    )
    # print('\n Original Sentinel-2 TOA dataset: \n',S2.limit(1).getInfo())
    # print('Number of images in Sentinel 2 TOA dataset: \t',S2.size().getInfo())

    return L7, L8, S2


"""
Function to apply model in one direction
"""


def dir0Regression(img, slopes, intercepts):
    return img.select(chastainBandNames).multiply(slopes).add(intercepts)


"""
Applying the model in the opposite direction
"""


def dir1Regression(img, slopes, intercepts):
    return img.select(chastainBandNames).subtract(intercepts).divide(slopes)


"""
Function to correct one sensor to another
"""


def harmonizationChastain(img, fromSensor, toSensor):
    # Get the model for the given from and to sensor
    comboKey = fromSensor.upper() + "_" + toSensor.upper()
    coeffList = chastainCoeffDict[comboKey]
    slopes = coeffList[0]
    intercepts = coeffList[1]
    direction = ee.Number(coeffList[2])

    # Apply the model in the respective direction
    out = ee.Algorithms.If(
        direction.eq(0),
        dir0Regression(img, slopes, intercepts),
        dir1Regression(img, slopes, intercepts),
    )
    return ee.Image(out).copyProperties(img).copyProperties(img, ["system:time_start"])


"""
Calibrate Landsat-8 (OLI) and Sentinel-2 (MSI) to Landsat-7 (ETM+)
"""


def Harmonize_L7_L8_S2(L7, L8, S2):
    # harmonization
    harmonized_L8 = L8.map(lambda img: harmonizationChastain(img, "OLI", "ETM"))
    harmonized_S2 = S2.map(lambda img: harmonizationChastain(img, "MSI", "ETM"))

    # Merge harmonized landsat-8 and sentinel-2 to landsat-7 image collection
    harmonized_LandsatSentinel_ic = ee.ImageCollection(
        L7.merge(harmonized_L8).merge(harmonized_S2)
    )
    # print(harmonized_LandsatSentinel_ic.size().getInfo())
    return harmonized_LandsatSentinel_ic


"""
Add NDVI band to harmonized image collection
"""


def addNDVI(image):
    return image.addBands(
        image.normalizedDifference(["NIR", "RED"]).rename("NDVI")
    ).float()


"""
Function definitions to get NDVI values at each 16-day composites
"""


def Get_NDVI_image_datewise(harmonized_LS_ic, roi_boundary):
    def get_NDVI_datewise(date):
        empty_band_image = (
            ee.Image(0)
            .float()
            .rename(["NDVI"])
            .updateMask(ee.Image(0).clip(roi_boundary))
        )
        return (
            harmonized_LS_ic.select(["NDVI"])
            .filterDate(ee.Date(date), ee.Date(date).advance(16, "day"))
            .merge(empty_band_image)
            .median()
            .set("system:time_start", ee.Date(date).millis())
        )

    return get_NDVI_datewise


def Get_LS_16Day_NDVI_TimeSeries(
    inputStartDate, inputEndDate, harmonized_LS_ic, roi_boundary
):
    startDate = datetime.strptime(inputStartDate, "%Y-%m-%d")
    endDate = datetime.strptime(inputEndDate, "%Y-%m-%d")

    date_list = pd.date_range(start=startDate, end=endDate, freq="16D").tolist()
    date_list = ee.List(
        [datetime.strftime(curr_date, "%Y-%m-%d") for curr_date in date_list]
    )

    LSC = ee.ImageCollection.fromImages(
        date_list.map(Get_NDVI_image_datewise(harmonized_LS_ic, roi_boundary))
    )

    return LSC


"""
Pair available LSC and modis values for each time stamp.
"""


def pairLSModis(lsRenameBands, roi_boundary):
    def pair(feature):
        date = ee.Date(feature.get("system:time_start"))
        startDateT = date.advance(-8, "day")
        endDateT = date.advance(8, "day")

        # ------ MODIS VI ( We can add EVI to the band list later )
        modis = (
            ee.ImageCollection("MODIS/061/MOD13Q1")
            .filterDate(startDateT, endDateT)
            .select(["NDVI", "SummaryQA"])
            .filterBounds(roi_boundary)
            .median()
            .rename(["NDVI_modis", "SummaryQA_modis"])
        )

        return feature.rename(lsRenameBands).addBands(modis)

    return pair


"""
Function to get Pearson Correlation Coffecient to perform GapFilling
"""


def get_Pearson_Correlation_Coefficients(LSC_modis_paired_ic, roi_boundary, bandList):
    corr = (
        LSC_modis_paired_ic.filterBounds(roi_boundary)
        .select(bandList)
        .toArray()
        .arrayReduce(reducer=ee.Reducer.pearsonsCorrelation(), axes=[0], fieldAxis=1)
        .arrayProject([1])
        .arrayFlatten([["c", "p"]])
    )
    return corr


"""Use print(...) to write to this console.
Fill gaps in LSC timeseries using modis data
"""


def gapfillLSM(LSC_modis_regression_model, LSC_bandName, modis_bandName):
    def peformGapfilling(image):
        offset = LSC_modis_regression_model.select("offset")
        scale = LSC_modis_regression_model.select("scale")
        nodata = -1

        lsc_image = image.select(LSC_bandName)
        modisfit = image.select(modis_bandName).multiply(scale).add(offset)

        mask = (
            lsc_image.mask()
        )  # update mask needs an input (no default input from the API document)
        gapfill = lsc_image.unmask(nodata)
        gapfill = gapfill.where(mask.Not(), modisfit)

        """
    in SummaryQA,
    0: Good data, use with confidence
    1: Marginal data, useful but look at detailed QA for more information
    2: Pixel covered with snow/ice
    3: Pixel is cloudy
    """
        qc_m = image.select("SummaryQA_modis").unmask(
            3
        )  # missing value is grouped as cloud
        w_m = modisfit.mask().rename("w_m").where(qc_m.eq(0), 0.8)  # default is 0.8
        w_m = w_m.where(qc_m.eq(1), 0.5)  # Marginal
        w_m = w_m.where(qc_m.gte(2), 0.2)  # snow/ice or cloudy

        # make sure these modis values are read where there is missing data from LandSat, Sentinel
        w_l = gapfill.mask()  # default is 1
        w_l = w_l.where(mask.Not(), w_m)

        return gapfill.addBands(w_l).rename(
            ["gapfilled_" + LSC_bandName, "SummaryQA"]
        )  # have NDVI from modis and a summary of clarity for each

    return peformGapfilling


"""
Function to combine LSC with Modis data
"""


def Combine_LS_Modis(LSC, roi_boundary):
    lsRenameBands = (
        ee.Image(LSC.first()).bandNames().map(lambda band: ee.String(band).cat("_lsc"))
    )
    LSC_modis_paired_ic = LSC.map(pairLSModis(lsRenameBands, roi_boundary))

    # Output contains scale, offset i.e. two bands
    LSC_modis_regression_model_NDVI = LSC_modis_paired_ic.select(
        ["NDVI_modis", "NDVI_lsc"]
    ).reduce(ee.Reducer.linearFit())

    corr_NDVI = get_Pearson_Correlation_Coefficients(
        LSC_modis_paired_ic, roi_boundary, ["NDVI_modis", "NDVI_lsc"]
    )
    LSMC_NDVI = LSC_modis_paired_ic.map(
        gapfillLSM(LSC_modis_regression_model_NDVI, "NDVI_lsc", "NDVI_modis")
    )

    return LSMC_NDVI


"""
Mask out low quality pixels
"""


def mask_low_QA(lsmc_image):
    low_qa = lsmc_image.select("SummaryQA").neq(0.2)
    return lsmc_image.updateMask(low_qa).copyProperties(
        lsmc_image, ["system:time_start"]
    )


"""
Add image timestamp to each image in time series
"""


def add_timestamp(image):
    timeImage = image.metadata("system:time_start").rename("timestamp")
    timeImageMasked = timeImage.updateMask(image.mask().select(0))
    return image.addBands(timeImageMasked)


"""
Perform linear interpolation on missing values
"""


def performInterpolation(image):
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
    fill_value = ee.ImageCollection([beforeMosaic, afterMosaic]).mosaic()
    result = result.unmask(fill_value)

    return result.copyProperties(image, ["system:time_start"])


def interpolate_timeseries(S1_TS):
    lsmc_masked = S1_TS.map(mask_low_QA)
    filtered = lsmc_masked.map(add_timestamp)

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

    interpolated_S1_TS = ee.ImageCollection(join2Result.map(performInterpolation))

    return interpolated_S1_TS


"""
Function Definition to get Padded NDVI LSMC timeseries image for a given ROI
"""


def Get_Padded_NDVI_TS_Image(startDate, endDate, roi_boundary):
    L7, L8, S2 = Get_L7_L8_S2_ImageCollections(startDate, endDate, roi_boundary)

    harmonized_LS_ic = Harmonize_L7_L8_S2(L7, L8, S2)
    harmonized_LS_ic = harmonized_LS_ic.map(addNDVI)
    LSC = Get_LS_16Day_NDVI_TimeSeries(
        startDate, endDate, harmonized_LS_ic, roi_boundary
    )
    LSMC_NDVI = Combine_LS_Modis(LSC, roi_boundary)
    Interpolated_LSMC_NDVI = interpolate_timeseries(LSMC_NDVI)
    final_LSMC_NDVI_TS = Interpolated_LSMC_NDVI.select(["gapfilled_NDVI_lsc"]).toBands()
    final_LSMC_NDVI_TS = final_LSMC_NDVI_TS.clip(roi_boundary)
    input_bands = final_LSMC_NDVI_TS.bandNames()
    return final_LSMC_NDVI_TS, input_bands


"""
Function definition to compute euclidean distance to each cluster centroid
features ---> clusters
flattened ---> time series image clipped to target area
input_bands ---> band names for time series image
studyarea ---> geometry of region of interest
"""


# Function to get distances as required from each pixel to each cluster centroid
def Get_Euclidean_Distance(
    cluster_centroids, roi_timeseries_img, input_bands, roi_boundary
):

    def wrapper(curr_centroid):
        temp_img = ee.Image()
        curr_centroid = ee.Feature(curr_centroid).setGeometry(roi_boundary)
        temp_fc = ee.FeatureCollection([curr_centroid])
        class_img = (
            temp_fc.select(["class"])
            .reduceToImage(["class"], ee.Reducer.first())
            .rename(["class"])
        )

        def create_img(band_name):
            return (
                temp_fc.select([band_name])
                .reduceToImage([band_name], ee.Reducer.first())
                .rename([band_name])
            )

        temp_img = input_bands.map(create_img)
        empty = ee.Image()
        temp_img = ee.Image(
            temp_img.iterate(lambda img, prev: ee.Image(prev).addBands(img), empty)
        )

        temp_img = temp_img.select(temp_img.bandNames().remove("constant"))
        distance = roi_timeseries_img.spectralDistance(temp_img, "sed")
        confidence = ee.Image(1.0).divide(distance).rename(["confidence"])
        distance = distance.addBands(confidence)
        return distance.addBands(class_img.rename(["class"])).set(
            "class", curr_centroid.get("class")
        )

    return cluster_centroids.map(wrapper)


"""
Function definition to get final prediction image from distance images
"""


def Get_final_prediction_image(distance_imgs_list):
    # Denominator is an image that is sum of all confidences to each cluster
    sum_of_distances = (
        ee.ImageCollection(distance_imgs_list).select(["confidence"]).sum().unmask()
    )
    distance_imgs_ic = ee.ImageCollection(distance_imgs_list).select(
        ["distance", "class"]
    )

    # array is an image where distance band is an array of distances to each cluster centroid and class band is an array of classes associated with each cluster
    array_img = ee.ImageCollection(distance_imgs_ic).toArray()

    axes = {"image": 0, "band": 1}
    sort = array_img.arraySlice(axes["band"], 0, 1)
    sorted = array_img.arraySort(sort)

    # take the first image only
    values = sorted.arraySlice(axes["image"], 0, 1)
    # convert back to an image
    min = values.arrayProject([axes["band"]]).arrayFlatten([["distance", "class"]])
    # Extract the hard classification
    predicted_class_img = min.select(1)
    predicted_class_img = predicted_class_img.rename(["predicted_label"])

    return predicted_class_img


def get_cropping_frequency(roi_boundary, startDate, endDate):
    cluster_centroids = ee.FeatureCollection(
        "projects/ee-indiasat/assets/L3_LULC_Clusters/Final_Level3_PanIndia_Clusters"
    )
    ignore_clusters = [12]  # remove invalid clusters
    cluster_centroids = cluster_centroids.filter(
        ee.Filter.Not(ee.Filter.inList("class", ignore_clusters))
    )

    final_LSMC_NDVI_TS, input_bands = Get_Padded_NDVI_TS_Image(
        startDate, endDate, roi_boundary
    )
    distance_imgs_list = Get_Euclidean_Distance(
        cluster_centroids, final_LSMC_NDVI_TS, input_bands, roi_boundary
    )
    final_classified_img = Get_final_prediction_image(distance_imgs_list)
    ### adding Cluster values after 12
    # cluster_centroids = change_clusters(cluster_centroids)
    distance_imgs_list = Get_Euclidean_Distance(
        cluster_centroids, final_LSMC_NDVI_TS, input_bands, roi_boundary
    )
    final_cluster_classified_img = Get_final_prediction_image(distance_imgs_list)
    final_cluster_classified_img = final_cluster_classified_img.rename(
        ["predicted_cluster"]
    )
    final_classified_img = final_classified_img.addBands(final_cluster_classified_img)
    return final_classified_img
