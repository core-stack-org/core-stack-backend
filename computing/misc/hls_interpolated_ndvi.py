import ee


# Function to create NDVI time series from harmonized image collection
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


# Function to create a 16-day NDVI time series
def Get_LS_16Day_NDVI_TimeSeries(
    inputStartDate, inputEndDate, harmonized_LS_ic, roi_boundary, days
):
    startDate = ee.Date(inputStartDate)
    endDate = ee.Date(inputEndDate)

    dateList = ee.List.sequence(
        startDate.millis(), endDate.millis(), days * 24 * 60 * 60 * 1000
    )
    images = dateList.map(Get_NDVI_image_datewise(harmonized_LS_ic, roi_boundary))
    return ee.ImageCollection.fromImages(images)


# Function to pair available LSC and MODIS values for each timestamp
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


# Function to get Pearson Correlation Coffecient to perform GapFilling
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


# Function to perform gap filling with MODIS data
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

    # corr_NDVI = get_Pearson_Correlation_Coefficients(
    #     LSC_modis_paired_ic, roi_boundary, ["NDVI_modis", "NDVI_lsc"]
    # )
    LSMC_NDVI = LSC_modis_paired_ic.map(
        gapfillLSM(LSC_modis_regression_model_NDVI, "NDVI_lsc", "NDVI_modis")
    )

    return LSMC_NDVI


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


def get_hls_collection(start_date, end_date, roi_boundary):
    hls_l30 = (
        ee.ImageCollection("NASA/HLS/HLSL30/v002")
        .filterDate(start_date, end_date)
        .filterBounds(roi_boundary)
    )

    hls_s30 = (
        ee.ImageCollection("NASA/HLS/HLSS30/v002")
        .filterDate(start_date, end_date)
        .filterBounds(roi_boundary)
    )

    def add_hls_l30_ndvi(image):
        ndvi_band = image.normalizedDifference(["B5", "B4"]).rename("NDVI")
        ndwi = image.normalizedDifference(["B3", "B5"]).rename("NDWI")
        return image.addBands(ndvi_band).float().addBands(ndwi).float()

    def add_hls_s30_ndvi(image):
        ndvi_band = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
        ndwi = image.normalizedDifference(["B3", "B8"]).rename("NDWI")
        return image.addBands(ndvi_band).float().addBands(ndwi).float()

    ndvi_hls_l30 = hls_l30.map(add_hls_l30_ndvi)

    ndvi_hls_s30 = hls_s30.map(add_hls_s30_ndvi)

    hls_merged = ndvi_hls_l30.merge(ndvi_hls_s30)
    return hls_merged


# Function to get padded NDVI LSMC time series image for a given ROI
def get_padded_ndvi_ts_image(startDate, endDate, roi_boundary, days=16):

    harmonized_LS_ic = get_hls_collection(startDate, endDate, roi_boundary)

    LSC = Get_LS_16Day_NDVI_TimeSeries(
        startDate, endDate, harmonized_LS_ic, roi_boundary, days
    )
    LSMC_NDVI = Combine_LS_Modis(LSC, roi_boundary)
    # LSC, LSMC not null

    Interpolated_LSMC_NDVI = interpolate_timeseries(LSMC_NDVI)

    Interpolated_LSMC_NDVI_clipped = Interpolated_LSMC_NDVI.map(
        lambda image: image.clip(roi_boundary)
    )
    return Interpolated_LSMC_NDVI_clipped
