import ee
from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    ee_initialize,
    is_gee_asset_exists,
)


def generate_hybrid_tree_mask(aez, start_year=2004, end_year=None, gee_account_id=None):
    """
    Forest Sensitivity Analysis Pipeline — Script 1
    Hybrid 30m Annual Tree Cover Mask + Contiguous Forest Period

    Produces per-pixel: length (years), start_year, end_year
    of the most recent unbroken forest period (2003–2022).

    Sources:
      1. GLC-FCS30D (2003–2022)
      2. . Dynamic World (2015–present)
      3. IndiaSat LULC (2017–present), Core-stack.

    Union logic: majority vote among active datasets per year.
      GLC-FCS30D: 2003–2022 (classes 51–92)
      Dynamic World: 2015–present (class 1 = Trees)
      IndiaSat LULC: 2017–present (class 6 = Trees)

    Temporal correction: ±2 year window as used in other places too by the team.
    """

    ee_initialize(gee_account_id)

    TEMPORAL_WINDOW = 2

    start_year = 2004
    LULC_START_YEAR = 2017

    OUTPUT_DESC = f"Hybrid_Tree_AEZ_{aez}_{str(start_year)}_{str(end_year)}"
    OUTPUT_ASSET_ID = (
        f"projects/corestack-datasets-alpha/assets/datasets/SPEI/{OUTPUT_DESC}"
    )

    if is_gee_asset_exists(OUTPUT_ASSET_ID):
        return None, OUTPUT_ASSET_ID

    aoi = ee.FeatureCollection(AEZ).filter(ee.Filter.eq("ae_regcode", aez)).geometry()
    # aoi = (
    #     ee.FeatureCollection("projects/ext-datasets/assets/datasets/State_pan_india")
    #     .filter(ee.Filter.eq("Name", "Odisha"))
    #     .geometry()
    # )

    # DATASET PREPARATION :=
    # --- GLC-FCS30D ---
    glcMosaic = ee.ImageCollection(
        "projects/sat-io/open-datasets/GLC-FCS30D/annual"
    ).mosaic()

    # --- Dynamic World ---
    dwCol = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1").filterBounds(aoi)

    # --- IndiaSat LULC ---
    indiaSatList = []
    for year in range(LULC_START_YEAR, end_year + 1):
        indiaSatList.append(
            ee.Image(
                f"projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_{year}_{year+1}"
            ).set("year", year)
        )

    indiaSatCol = ee.ImageCollection(indiaSatList)

    def get_indiaSat_mask(y):
        img = indiaSatCol.filter(ee.Filter.eq("year", y)).first()
        return ee.Image(
            ee.Algorithms.If(
                img,
                ee.Image(img).select("predicted_label").eq(6).unmask(0),
                ee.Image(0),
            )
        ).rename("tree")

    # HYBRID MASK GENERATION :=
    years = ee.List.sequence(start_year, end_year)

    def annual_tree_cover(year):
        year = ee.Number(year)

        # GLC — always active, classes 51–92 are forests
        bandName = ee.String("b").cat(year.subtract(1999).format("%.0f"))

        glcMask = ee.Image(
            ee.Algorithms.If(
                year.lte(2022),
                glcMosaic.select(bandName)
                .gte(51)
                .And(glcMosaic.select(bandName).lte(92)),
                ee.Image(0),
            )
        ).rename("tree")
        proj = glcMask.projection()

        # Dynamic World — active from 2015
        dwYear = dwCol.filter(ee.Filter.calendarRange(year, year, "year")).select(
            "label"
        )
        dwMask = (
            ee.Image(
                ee.Algorithms.If(
                    dwYear.size().gt(0), dwYear.mode().eq(1).unmask(0), ee.Image(0)
                )
            )
            .rename("tree")
            .reproject(crs=proj, scale=30)
        )

        # IndiaSat — active from 2017
        indiaSatMask = get_indiaSat_mask(year).reproject(crs=proj, scale=30)

        # Majority vote
        glcActive = ee.Algorithms.If(year.lte(2022), 1, 0)
        forestSum = glcMask.unmask(0).add(dwMask.unmask(0)).add(indiaSatMask.unmask(0))
        dwActive = ee.Algorithms.If(year.gte(2015), 1, 0)
        indiasatActive = ee.Algorithms.If(year.gte(2017).And(year.lte(2024)), 1, 0)
        activeDatasets = ee.Number(glcActive).add(dwActive).add(indiasatActive)
        requiredVotes = ee.Algorithms.If(activeDatasets.eq(1), 1, 2)
        hybridMask = forestSum.gte(ee.Number(requiredVotes))

        return hybridMask.set("year", year).rename("tree")

    annualTreeCoverMasks = ee.ImageCollection(years.map(annual_tree_cover))

    # TEMPORAL CORRECTION :=
    def corrected_tree_cover(year):
        year = ee.Number(year)
        originalMask = annualTreeCoverMasks.filter(ee.Filter.eq("year", year)).first()

        windowMasks = annualTreeCoverMasks.filter(
            ee.Filter.And(
                ee.Filter.neq("year", year),
                ee.Filter.gte("year", year.subtract(TEMPORAL_WINDOW)),
                ee.Filter.lte("year", year.add(TEMPORAL_WINDOW)),
            )
        )

        corrected = originalMask.unmask(0).where(windowMasks.max().eq(1), 1)
        return corrected.set("year", year).rename("tree")

    correctedTreeCoverMasks = ee.ImageCollection(years.map(corrected_tree_cover))

    # CONTIGUOUS FOREST PERIOD (LENGTH, START, END) :=
    def calculate_consecutive(currentImage, previousState):
        prevCount = ee.Image(ee.List(previousState).get(0))
        prevStop = ee.Image(ee.List(previousState).get(1))
        currentMask = currentImage.select("tree")
        stillCounting = prevStop.Not()
        newCount = prevCount.add(currentMask.multiply(stillCounting))
        newStop = prevStop.Or(currentMask.Not())
        return ee.List([newCount, newStop])

    # Iterate backwards so we get the MOST RECENT contiguous period
    reversedCollection = correctedTreeCoverMasks.sort("year", False)
    initialState = ee.List([ee.Image(0).byte(), ee.Image(0).byte()])
    finalState = ee.List(
        reversedCollection.iterate(calculate_consecutive, initialState)
    )

    recentLength = ee.Image(finalState.get(0)).rename("length")
    forestEndYear = ee.Image(end_year).multiply(recentLength.gt(0)).rename("end_year")
    forestStartYear = (
        forestEndYear.subtract(recentLength)
        .add(1)
        .multiply(recentLength.gt(0))
        .rename("start_year")
    )

    finalOutput = (
        recentLength.addBands(forestStartYear).addBands(forestEndYear).clip(aoi)
    )

    task_id = export_raster_asset_to_gee(
        finalOutput, OUTPUT_DESC, OUTPUT_ASSET_ID, scale=30, region=aoi
    )

    return task_id, OUTPUT_ASSET_ID
