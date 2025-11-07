import ee

# Chastain band names
chastainBandNames = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"]

# Regression model parameters
msiOLISlopes = [1.0946, 1.0043, 1.0524, 0.8954, 1.0049, 1.0002]
msiOLIIntercepts = [-0.0107, 0.0026, -0.0015, 0.0033, 0.0065, 0.0046]

msiETMSlopes = [1.10601, 0.99091, 1.05681, 1.0045, 1.03611, 1.04011]
msiETMIntercepts = [-0.0139, 0.00411, -0.0024, -0.0076, 0.00411, 0.00861]

oliETMSlopes = [1.03501, 1.00921, 1.01991, 1.14061, 1.04351, 1.05271]
oliETMIntercepts = [-0.0055, -0.0008, -0.0021, -0.0163, -0.0045, 0.00261]

# Coefficient dictionary
chastainCoeffDict = {
    "MSI_OLI": [msiOLISlopes, msiOLIIntercepts, 1],
    "MSI_ETM": [msiETMSlopes, msiETMIntercepts, 1],
    "OLI_ETM": [oliETMSlopes, oliETMIntercepts, 1],
    "OLI_MSI": [msiOLISlopes, msiOLIIntercepts, 0],
    "ETM_MSI": [msiETMSlopes, msiETMIntercepts, 0],
    "ETM_OLI": [oliETMSlopes, oliETMIntercepts, 0],
}


# # Cloud masking function for Landsat-7
# def maskL7cloud(image):
#     qa = image.select("QA_PIXEL")
#     mask = qa.bitwiseAnd(1 << 4).eq(0)
#     return (
#         image.updateMask(mask)
#         .select(["B1", "B2", "B3", "B4", "B5", "B7"])
#         .rename(["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])
#     )
#
#
# # Cloud masking function for Landsat-8
# def maskL8cloud(image):
#     qa = image.select("QA_PIXEL")
#     mask = qa.bitwiseAnd(1 << 4).eq(0)
#     return (
#         image.updateMask(mask)
#         .select(["B2", "B3", "B4", "B5", "B6", "B7"])
#         .rename(["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])
#     )
#
#
# # Cloud masking function for Sentinel-2 TOA
# def maskS2cloudTOA(image):
#     qa = image.select("QA60")
#     cloudBitMask = 1 << 10
#     cirrusBitMask = 1 << 11
#     mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
#     return (
#         image.updateMask(mask)
#         .select(["B2", "B3", "B4", "B8", "B11", "B12"])
#         .rename(["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])
#     )
#
#
# # Function to get Landsat-7, Landsat-8, and Sentinel-2 image collections
# def Get_L7_L8_S2_ImageCollections(inputStartDate, inputEndDate, roi_boundary):
#     L7 = (
#         ee.ImageCollection("LANDSAT/LE07/C02/T1_TOA")
#         .filterDate(inputStartDate, inputEndDate)
#         .filterBounds(roi_boundary)
#         .map(maskL7cloud)
#     )
#
#     L8 = (
#         ee.ImageCollection("LANDSAT/LC08/C02/T1_TOA")
#         .filterDate(inputStartDate, inputEndDate)
#         .filterBounds(roi_boundary)
#         .map(maskL8cloud)
#     )
#
#     S2 = (
#         ee.ImageCollection("COPERNICUS/S2_HARMONIZED")
#         .filterDate(inputStartDate, inputEndDate)
#         .filterBounds(roi_boundary)
#         .map(maskS2cloudTOA)
#     )
#
#     return L7, L8, S2
#
#
# # Function to apply regression model in one direction
# def dir0Regression(img, slopes, intercepts):
#     return img.select(chastainBandNames).multiply(slopes).add(intercepts)
#
#
# # Function to apply regression model in the opposite direction
# def dir1Regression(img, slopes, intercepts):
#     return img.select(chastainBandNames).subtract(intercepts).divide(slopes)
#
#
# # Harmonization function
# def harmonizationChastain(img, fromSensor, toSensor):
#     comboKey = fromSensor.upper() + "_" + toSensor.upper()
#     coeffList = chastainCoeffDict[comboKey]
#     slopes = coeffList[0]
#     intercepts = coeffList[1]
#     direction = ee.Number(coeffList[2])
#
#     out = ee.Algorithms.If(
#         direction.eq(0),
#         dir0Regression(img, slopes, intercepts),
#         dir1Regression(img, slopes, intercepts),
#     )
#     return ee.Image(out).copyProperties(img).copyProperties(img, ["system:time_start"])
#
#
# # Function to harmonize Landsat-8 and Sentinel-2 to Landsat-7
# def Harmonize_L7_L8_S2(L7, L8, S2):
#     # harmonization
#     harmonized_L8 = L8.map(lambda img: harmonizationChastain(img, "OLI", "ETM"))
#     harmonized_S2 = S2.map(lambda img: harmonizationChastain(img, "MSI", "ETM"))
#
#     # Merge harmonized landsat-8 and sentinel-2 to landsat-7 image collection
#     harmonized_LandsatSentinel_ic = ee.ImageCollection(
#         L7.merge(harmonized_L8).merge(harmonized_S2)
#     )
#     # print(harmonized_LandsatSentinel_ic.size().getInfo())
#     return harmonized_LandsatSentinel_ic
#
#
# # Function to add NDVI band
# def addNDVI(image):
#     ndvi = image.normalizedDifference(["NIR", "RED"]).rename("NDVI")
#     return image.addBands(ndvi).float()
#
#
# # Function to create NDVI time series from harmonized image collection
# def Get_NDVI_image_datewise(harmonized_LS_ic, roi_boundary):
#     def get_NDVI_datewise(date):
#         empty_band_image = (
#             ee.Image(0)
#             .float()
#             .rename(["NDVI"])
#             .updateMask(ee.Image(0).clip(roi_boundary))
#         )
#         return (
#             harmonized_LS_ic.select(["NDVI"])
#             .filterDate(ee.Date(date), ee.Date(date).advance(16, "day"))
#             .merge(empty_band_image)
#             .median()
#             .set("system:time_start", ee.Date(date).millis())
#         )
#
#     return get_NDVI_datewise
#

# # Function to create a 16-day NDVI time series
# def Get_LS_16Day_NDVI_TimeSeries(
#     inputStartDate, inputEndDate, harmonized_LS_ic, roi_boundary
# ):
#     startDate = ee.Date(inputStartDate)
#     endDate = ee.Date(inputEndDate)
#
#     dateList = ee.List.sequence(
#         startDate.millis(), endDate.millis(), 16 * 24 * 60 * 60 * 1000
#     )
#     images = dateList.map(Get_NDVI_image_datewise(harmonized_LS_ic, roi_boundary))
#     return ee.ImageCollection.fromImages(images)
#
#
# # Function to pair available LSC and MODIS values for each timestamp
# def pairLSModis(lsRenameBands, roi_boundary):
#     def pair(feature):
#         date = ee.Date(feature.get("system:time_start"))
#         startDateT = date.advance(-8, "day")
#         endDateT = date.advance(8, "day")
#
#         # ------ MODIS VI ( We can add EVI to the band list later )
#         modis = (
#             ee.ImageCollection("MODIS/061/MOD13Q1")
#             .filterDate(startDateT, endDateT)
#             .select(["NDVI", "SummaryQA"])
#             .filterBounds(roi_boundary)
#             .median()
#             .rename(["NDVI_modis", "SummaryQA_modis"])
#         )
#
#         return feature.rename(lsRenameBands).addBands(modis)
#
#     return pair


# # Function to get Pearson Correlation Coffecient to perform GapFilling
# def get_Pearson_Correlation_Coefficients(LSC_modis_paired_ic, roi_boundary, bandList):
#     corr = (
#         LSC_modis_paired_ic.filterBounds(roi_boundary)
#         .select(bandList)
#         .toArray()
#         .arrayReduce(reducer=ee.Reducer.pearsonsCorrelation(), axes=[0], fieldAxis=1)
#         .arrayProject([1])
#         .arrayFlatten([["c", "p"]])
#     )
#     return corr
#
#
# # Function to perform gap filling with MODIS data
# def gapfillLSM(LSC_modis_regression_model, LSC_bandName, modis_bandName):
#     def peformGapfilling(image):
#         offset = LSC_modis_regression_model.select("offset")
#         scale = LSC_modis_regression_model.select("scale")
#         nodata = -1
#
#         lsc_image = image.select(LSC_bandName)
#         modisfit = image.select(modis_bandName).multiply(scale).add(offset)
#
#         mask = (
#             lsc_image.mask()
#         )  # update mask needs an input (no default input from the API document)
#         gapfill = lsc_image.unmask(nodata)
#         gapfill = gapfill.where(mask.Not(), modisfit)
#
#         """
#     in SummaryQA,
#     0: Good data, use with confidence
#     1: Marginal data, useful but look at detailed QA for more information
#     2: Pixel covered with snow/ice
#     3: Pixel is cloudy
#     """
#         qc_m = image.select("SummaryQA_modis").unmask(
#             3
#         )  # missing value is grouped as cloud
#         w_m = modisfit.mask().rename("w_m").where(qc_m.eq(0), 0.8)  # default is 0.8
#         w_m = w_m.where(qc_m.eq(1), 0.5)  # Marginal
#         w_m = w_m.where(qc_m.gte(2), 0.2)  # snow/ice or cloudy
#
#         # make sure these modis values are read where there is missing data from LandSat, Sentinel
#         w_l = gapfill.mask()  # default is 1
#         w_l = w_l.where(mask.Not(), w_m)
#
#         return gapfill.addBands(w_l).rename(
#             ["gapfilled_" + LSC_bandName, "SummaryQA"]
#         )  # have NDVI from modis and a summary of clarity for each
#
#     return peformGapfilling
#
#
# """
# Function to combine LSC with Modis data
# """
#
#
# def Combine_LS_Modis(LSC, roi_boundary):
#     lsRenameBands = (
#         ee.Image(LSC.first()).bandNames().map(lambda band: ee.String(band).cat("_lsc"))
#     )
#     LSC_modis_paired_ic = LSC.map(pairLSModis(lsRenameBands, roi_boundary))
#
#     # Output contains scale, offset i.e. two bands
#     LSC_modis_regression_model_NDVI = LSC_modis_paired_ic.select(
#         ["NDVI_modis", "NDVI_lsc"]
#     ).reduce(ee.Reducer.linearFit())
#
#     # corr_NDVI = get_Pearson_Correlation_Coefficients(
#     #     LSC_modis_paired_ic, roi_boundary, ["NDVI_modis", "NDVI_lsc"]
#     # )
#     LSMC_NDVI = LSC_modis_paired_ic.map(
#         gapfillLSM(LSC_modis_regression_model_NDVI, "NDVI_lsc", "NDVI_modis")
#     )
#
#     return LSMC_NDVI


# Function to mask low quality pixels
def mask_low_QA(lsmc_image):
    low_qa = lsmc_image.select("SummaryQA").neq(0.2)
    return lsmc_image.updateMask(low_qa).copyProperties(
        lsmc_image, ["system:time_start"]
    )


# Function to add timestamp to each image in the time series
def add_timestamp(image):
    timeImage = image.metadata("system:time_start").rename("timestamp")
    timeImageMasked = timeImage.updateMask(image.mask().select(0))
    return image.addBands(timeImageMasked)


# Perform linear interpolation on missing values
def performInterpolation(image):
    beforeImages = ee.List(image.get("before"))
    beforeMosaic = ee.ImageCollection.fromImages(beforeImages).mosaic()
    afterImages = ee.List(image.get("after"))
    afterMosaic = ee.ImageCollection.fromImages(afterImages).mosaic()

    t1 = beforeMosaic.select("timestamp").rename("t1")
    t2 = afterMosaic.select("timestamp").rename("t2")
    t = ee.Image.constant(image.get("system:time_start")).rename("t")
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
        (afterMosaic.subtract(beforeMosaic)).multiply(timeRatio)
    )
    result = ee.Image(image).unmask(interpolated)
    fill_value = ee.ImageCollection([beforeMosaic, afterMosaic]).mosaic()
    result = result.unmask(fill_value)

    return result.copyProperties(image, ["system:time_start"])


# Function to interpolate time series
def interpolate_timeseries(S1_TS):
    lsmc_masked = S1_TS.map(mask_low_QA)
    filtered = lsmc_masked.map(add_timestamp)

    timeWindow = 120
    millis = ee.Number(timeWindow).multiply(1000 * 60 * 60 * 24)
    maxDiffFilter = ee.Filter.maxDifference(
        difference=millis, leftField="system:time_start", rightField="system:time_start"
    )

    lessEqFilter = ee.Filter.lessThanOrEquals(
        leftField="system:time_start", rightField="system:time_start"
    )
    greaterEqFilter = ee.Filter.greaterThanOrEquals(
        leftField="system:time_start", rightField="system:time_start"
    )

    filter1 = ee.Filter.And(maxDiffFilter, lessEqFilter)
    join1 = ee.Join.saveAll(
        matchesKey="after", ordering="system:time_start", ascending=False
    )
    join1Result = join1.apply(primary=filtered, secondary=filtered, condition=filter1)

    filter2 = ee.Filter.And(maxDiffFilter, greaterEqFilter)
    join2 = ee.Join.saveAll(
        matchesKey="before", ordering="system:time_start", ascending=True
    )
    join2Result = join2.apply(
        primary=join1Result, secondary=join1Result, condition=filter2
    )

    interpolated_S1_TS = ee.ImageCollection(join2Result.map(performInterpolation))
    return interpolated_S1_TS


# Function to get padded NDVI LSMC time series image for a given ROI
def get_hls_interpolated_ndvi(start_date, end_date, roi_boundary):
    # L7, L8, S2 = Get_L7_L8_S2_ImageCollections(startDate, endDate, roi_boundary)
    #
    # harmonized_LS_ic = Harmonize_L7_L8_S2(L7, L8, S2)
    # harmonized_LS_ic = harmonized_LS_ic.map(addNDVI)
    # LSC = Get_LS_16Day_NDVI_TimeSeries(
    #     startDate, endDate, harmonized_LS_ic, roi_boundary
    # )
    # LSMC_NDVI = Combine_LS_Modis(LSC, roi_boundary)
    # LSC, LSMC not null

    hls = (
        ee.ImageCollection("NASA/HLS/HLSL30/v002")
        .filterDate(start_date, end_date)
        .filterBounds(roi_boundary)
    )

    def add_ndvi_ndwi(image):
        ndvi_band = image.normalizedDifference(["B5", "B4"]).rename("NDVI")
        # ndwi = image.normalizedDifference(["B3", "B5"]).rename("NDWI")
        return image.addBands(ndvi_band).float()  # .addBands(ndwi).float()

    ndvi = hls.map(add_ndvi_ndwi)

    Interpolated_LSMC_NDVI = interpolate_timeseries(ndvi)

    Interpolated_LSMC_NDVI_clipped = Interpolated_LSMC_NDVI.map(
        lambda image: image.clip(roi_boundary)
    )
    return Interpolated_LSMC_NDVI_clipped
