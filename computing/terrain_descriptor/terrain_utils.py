import ee


def generate_terrain_classified_raster(feature):
    dem = ee.Image("USGS/SRTMGL1_003")
    studyArea = feature.geometry()
    demClipped = dem.clip(studyArea)
    dem_std = demClipped.reduceRegion(reducer=ee.Reducer.stdDev(), bestEffort=True).get(
        "elevation"
    )

    small_inner = ee.Number(
        5
    )  # here 5 represent number of pixels. Each pixel is of 30m resolution in SRTM DEM
    small_outer = ee.Number(10)
    small_inner_circle = ee.Kernel.circle(small_inner, "pixels", False, -1)
    small_outer_circle = ee.Kernel.circle(small_outer, "pixels", False, 1)
    small_kernel = small_outer_circle.add(small_inner_circle, True)  # created annulus

    large_inner = ee.Number(62)
    large_outer = ee.Number(67)
    large_inner_circle = ee.Kernel.circle(large_inner, "pixels", False, -1)
    large_outer_circle = ee.Kernel.circle(large_outer, "pixels", False, 1)
    large_kernel = large_outer_circle.add(large_inner_circle, True)  # created annulus

    focalmean_small = demClipped.reduceNeighborhood(ee.Reducer.mean(), small_kernel)
    focalmean_large = demClipped.reduceNeighborhood(ee.Reducer.mean(), large_kernel)

    TPI_small = demClipped.subtract(focalmean_small)
    TPI_large = demClipped.subtract(focalmean_large)

    mean = TPI_small.reduceRegion(reducer=ee.Reducer.mean(), bestEffort=True).get(
        "elevation"
    )
    TPI_small = TPI_small.subtract(ee.Number(mean))
    stdDev = TPI_small.reduceRegion(reducer=ee.Reducer.stdDev(), bestEffort=True).get(
        "elevation"
    )
    TPI_small = (
        TPI_small.divide(ee.Number(stdDev)).multiply(ee.Number(100)).add(ee.Number(0.5))
    )
    mean = TPI_large.reduceRegion(reducer=ee.Reducer.mean(), bestEffort=True).get(
        "elevation"
    )
    TPI_large = TPI_large.subtract(ee.Number(mean))

    stdDev = TPI_large.reduceRegion(reducer=ee.Reducer.stdDev(), bestEffort=True).get(
        "elevation"
    )
    TPI_large = (
        TPI_large.divide(ee.Number(stdDev)).multiply(ee.Number(100)).add(ee.Number(0.5))
    )

    combined_image = TPI_small.addBands(TPI_large)
    stdDev = TPI_large.reduceRegion(reducer=ee.Reducer.stdDev(), bestEffort=True).get(
        "elevation"
    )
    slope = ee.Terrain.slope(dem)
    clippedSlope = slope.clip(studyArea)

    lf300x2k = ee.Image.constant(0).clip(studyArea)

    dem_std = ee.Number(dem_std).add(1)
    a = 3
    b = 1.0
    min_factor = 0.3
    fac_1 = ee.Number(dem_std).log10().multiply(b)
    factor = ee.Number(a).subtract(fac_1)
    factor = factor.max(min_factor)

    right_limit = ee.Number(100).multiply(factor)
    left_limit = ee.Number(-100).multiply(factor)

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.gt(left_limit))
        .And(TPI_large.lt(right_limit))
        .And(clippedSlope.lt(5)),
        5,
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.gt(left_limit))
        .And(TPI_large.lt(right_limit))
        .And(clippedSlope.gte(5))
        .And(clippedSlope.lt(20)),
        6,
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.gte(right_limit))
        .And(clippedSlope.lt(6)),
        7,  # Flat Ridge Tops
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.gt(left_limit))
        .And(TPI_large.lt(right_limit))
        .And(clippedSlope.gte(20)),
        8,  # Upper Slopes
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.gte(right_limit))
        .And(clippedSlope.gte(6)),
        8,  # Upper Slopes
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gt(left_limit)
        .And(TPI_small.lt(right_limit))
        .And(TPI_large.lte(left_limit)),
        4,
    )

    lf300x2k = lf300x2k.where(
        TPI_small.lte(left_limit)
        .And(TPI_large.gt(left_limit))
        .And(TPI_large.lt(right_limit)),
        2,
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gte(right_limit)
        .And(TPI_large.gt(left_limit))
        .And(TPI_large.lt(right_limit)),
        10,
    )

    lf300x2k = lf300x2k.where(
        TPI_small.lte(left_limit).And(TPI_large.gte(right_limit)), 3
    )

    lf300x2k = lf300x2k.where(
        TPI_small.lte(left_limit).And(TPI_large.lte(left_limit)), 1
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gte(right_limit).And(TPI_large.gte(right_limit)), 11
    )

    lf300x2k = lf300x2k.where(
        TPI_small.gte(right_limit).And(TPI_large.lte(left_limit)), 9
    )

    return lf300x2k
