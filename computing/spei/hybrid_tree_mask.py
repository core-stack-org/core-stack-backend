import ee
from datetime import date

from utilities.constants import AEZ
from utilities.gee_utils import (
    export_raster_asset_to_gee,
    ee_initialize,
    is_gee_asset_exists,
)


def generate_hybrid_tree_mask(aez, start_year=2004, end_year=None, gee_account_id=None):
    """
    Forest Sensitivity Analysis Pipeline — Script 1
    Hybrid 30m Agricultural-Year Tree Cover Mask + Contiguous Forest Period

    AGRICULTURAL YEAR CONVENTION:
    Year `y` = Jul 1 of `y` -> Jun 30 of `y+1`, matching Scripts 3a/3b.

    Produces per-pixel: length (ag-years), start_year, end_year (ag-year
    labels) of the most recent unbroken forest period.

    Sources, and how each maps onto an ag-year `y`:
      1. GLC-FCS30D (2003-2022, one classification per CALENDAR year — no
         finer time resolution exists). An ag-year straddles two calendar
         years (Jul-Dec of `y`, Jan-Jun of `y+1`), so the ag-year signal is
         the average/majority of the calendar-year-`y` and calendar-year-
         `y+1` bands (>=50% of the two says forest -> forest). At the
         trailing edge (ag-year 2022, whose `y+1`=2023 doesn't exist in
         GLC's 2003-2022 coverage), only the single available band (2022)
         is used rather than dropping GLC for that year.
      2. Dynamic World (2015-present, near-continuous captures) — windowed
         directly to Jul 1(y) -> Jun 30(y+1), same pattern as CHIRPS/Landsat
         in Scripts 3a/3b.
      3. IndiaSat LULC (2017-present), Core-stack — asset names are already
         `pan_india_lulc_v3_{year}_{year+1}`, i.e. already Jul(year) ->
         Jun(year+1) ag-year aligned. No windowing change needed; `year`
         here already means exactly what our ag-year `y` means.

    Union logic: majority vote among active datasets per ag-year.
      GLC-FCS30D: active while year<=2022 (at least one of the two
        contributing calendar bands exists)
      Dynamic World: active from year>=2015
      IndiaSat LULC: active from year>=2017, no upper cap — relies on the
        per-year missing-asset handling in get_indiaSat_mask to silently
        contribute 0 for any year without a real asset. NOTE: this means
        IndiaSat still counts as "active" (raising requiredVotes to 2) even
        in years where its actual contribution is silently zero — if a
        future year's IndiaSat asset doesn't exist yet, GLC+DW alone would
        then need to agree, a stricter bar than if IndiaSat were excluded.
        Flagging this rather than silently tightening it, since the
        simpler no-cap version was the explicit choice.

    KNOWN PRE-EXISTING QUIRK (not touched): `start_year` is hardcoded to
    2004 below regardless of the caller-supplied `start_year` argument.
    Confirmed 2004 is correct — Script 3b's asset reference was the thing
    that needed fixing (was pointing at a nonexistent "2003" asset name),
    not this. Left as-is beyond that; the parameter-shadowing itself is a
    separate, still-open question if you ever want start_year configurable.

    Temporal correction: +/-2 ag-year window as used elsewhere by the team.
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

    if end_year is None:
        raise ValueError(
            "end_year must be specified explicitly — agricultural-year pipelines "
            "cannot infer a safe default."
        )

    # Dynamic World is a near-real-time product — ag-year end_year isn't
    # complete until Jun 30 of end_year+1 has passed. GLC-FCS30D is a fixed
    # historical release (doesn't need this check) and IndiaSat's missing
    # years are already handled gracefully below, so DW is the only source
    # where "not complete yet" can silently look like "no forest."
    required_complete_by = date(end_year + 1, 6, 30)
    if required_complete_by > date.today():
        raise ValueError(
            f"Agricultural year {end_year} (Jul {end_year} -> Jun {end_year + 1}) "
            f"is not yet complete as of {date.today().isoformat()}. Reduce end_year."
        )

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

    # --- IndiaSat LULC (already ag-year aligned, no windowing needed) ---
    indiaSatList = []
    for year in range(LULC_START_YEAR, end_year + 1):
        indiaSatList.append(
            ee.Image(
                f"projects/corestack-datasets/assets/datasets/LULC_v3_river_basin/pan_india_lulc_v3_{year}_{year + 1}"
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

    def glc_band_for_calendar_year(calYear):
        bandName = ee.String("b").cat(calYear.subtract(1999).format("%.0f"))
        return ee.Image(
            ee.Algorithms.If(
                calYear.lte(2022),
                glcMosaic.select(bandName)
                .gte(51)
                .And(glcMosaic.select(bandName).lte(92)),
                ee.Image(0),
            )
        ).rename("tree")

    def annual_tree_cover(year):
        year = ee.Number(year)

        # GLC — combine the two calendar-year bands this ag-year straddles.
        glcY = glc_band_for_calendar_year(year)
        glcY1 = glc_band_for_calendar_year(year.add(1))
        glcAvailCount = ee.Number(ee.Algorithms.If(year.lte(2022), 1, 0)).add(
            ee.Algorithms.If(year.add(1).lte(2022), 1, 0)
        )
        # average >= 0.5 over whichever of the two bands actually exist;
        # with 2 binary inputs this is "at least one says forest", and
        # gracefully degrades to "use the single available band as-is"
        # when only one calendar year is in GLC's coverage.
        glcMask = ee.Image(
            ee.Algorithms.If(
                glcAvailCount.gt(0),
                glcY.add(glcY1).divide(glcAvailCount.max(1)).gte(0.5),
                ee.Image(0),
            )
        ).rename("tree")
        proj = glcMask.projection()

        # Dynamic World — windowed to the ag-year (Jul 1(year) -> Jun 30(year+1))
        dwStart = ee.Date.fromYMD(year, 7, 1)
        dwEnd = ee.Date.fromYMD(year.add(1), 6, 30).advance(1, "day")
        dwYear = dwCol.filterDate(dwStart, dwEnd).select("label")
        dwMask = (
            ee.Image(
                ee.Algorithms.If(
                    dwYear.size().gt(0), dwYear.mode().eq(1).unmask(0), ee.Image(0)
                )
            )
            .rename("tree")
            .reproject(crs=proj, scale=30)
        )

        # IndiaSat — already ag-year aligned, `year` here already means Jul(year)->Jun(year+1)
        indiaSatMask = get_indiaSat_mask(year).reproject(crs=proj, scale=30)

        # Majority vote
        glcActive = ee.Algorithms.If(year.lte(2022), 1, 0)
        forestSum = glcMask.unmask(0).add(dwMask.unmask(0)).add(indiaSatMask.unmask(0))
        dwActive = ee.Algorithms.If(year.gte(2015), 1, 0)
        indiasatActive = ee.Algorithms.If(year.gte(2017), 1, 0)
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
