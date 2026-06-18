import ee
from computing.et_downscale.helper import (
    build_classifier,
    build_common_pixel_mask,
    ee_annual_total_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    fill_monthly_collection,
    monthly_collection_to_stack,
    get_proj_30m,
    empty_monthly_band,
    cast_monthly_band,
    ensure_monthly_band,
    monthly_valid_mask,
)


# ---------------------------------------------------------------------------
# CONSTANTS - AET pipeline
# ---------------------------------------------------------------------------
FEATURE_BANDS = [
    "MSAVI",
    "NDMI",
    "NDVI",
    "NDWI",
    "SAVI",
    "NDBI",
    "NDIIB7",
    "Albedo",
    "LST",
    "Rainf_tavg",
    "RootMoist_inst",
    "SoilMoi0_10cm_inst",
    "CanopInt_inst",
    "AvgSurfT_inst",
    "Qair_f_inst",
    "Wind_f_inst",
    "Psurf_f_inst",
    "SoilTMP0_10cm_inst",
    "Qsb_acc",
    "Swnet_tavg",
    "Lwnet_tavg",
    "Qg_tavg",
    "Qh_tavg",
    "Qle_tavg",
    "SWdown_f_tavg",
    "Tair_f_inst",
]
# =============================================================================
# CORE LAYER 1 - AET
# =============================================================================


def generate_aet(cfg, region):
    """
    Monthly mean daily AET for every 30 m pixel.
    Output  : aet_<asset_suffix>_<year> GEE asset (13 bands)
    """
    year = cfg["year"]
    asset_suffix = cfg["asset_suffix"]

    classifier = build_classifier(cfg["model_aez"])
    aet_stack = build_aet_stack(region, classifier, year)

    grid_proj = aet_stack.select("ET_01").projection()
    common_mask = build_common_pixel_mask(region, grid_proj)
    aet_monthly = aet_stack.multiply(0.1)

    footprint = monthly_valid_mask(aet_monthly, "ET")

    aet_annual = ee_annual_total_band(
        aet_monthly, "ET", year, band_name="ET_annual"
    ).updateMask(footprint)
    aet_image = finalize_export_image(
        aet_monthly,
        aet_annual,
        region,
        metadata={
            "application": "aet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "year": str(year),
            "asset_suffix": asset_suffix,
            # "roi_path": cfg["roi_path"],
            "model_aez": cfg["model_aez"],
            "description": "Bands 1-12: mean daily AET per month at 30 m; band 13: annual total AET",
        },
        band_descriptions=[f"ET_{abbr}_daily_mm" for abbr in MONTH_ABBR]
        + ["ET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("aet", "AET", aet_image, cfg)
    return aet_stack, common_mask, footprint, grid_proj, spec


def build_aet_stack(
    region: ee.Geometry,
    classifier: ee.Classifier,
    year: int,
    proj: ee.Projection = None,
) -> ee.Image:
    """12-band ET_01...ET_12 stack (0.1 mm/day, mean daily)."""
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 7, 1), ee.Date.fromYMD(year + 1, 7, 1))
    )

    if proj is None:
        proj = get_proj_30m(region, year)

    # months = ee.List.sequence(1, 12)
    start_date = ee.Date.fromYMD(year, 7, 1)

    months = ee.List.sequence(0, 11)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: make_raw_monthly(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                ls_col,
                region,
                classifier,
                year,
                proj,
            )
        )
    )
    interp_col = fill_monthly_collection(raw_monthly, "ET_daily", proj=proj)
    stack = monthly_collection_to_stack(interp_col, "ET_daily", "ET_", region)
    return stack


def calc_landsat_indices(img: ee.Image) -> ee.Image:
    ndvi = img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI")
    savi = img.expression(
        "((NIR-R)/(NIR+R+0.5))*1.5",
        {"NIR": img.select("SR_B5"), "R": img.select("SR_B4")},
    ).rename("SAVI")
    msavi = img.expression(
        "(2*NIR+1-sqrt(pow((2*NIR+1),2)-8*(NIR-R)))/2",
        {"NIR": img.select("SR_B5"), "R": img.select("SR_B4")},
    ).rename("MSAVI")
    ndbi = img.normalizedDifference(["SR_B6", "SR_B5"]).rename("NDBI")
    ndwi = img.normalizedDifference(["SR_B3", "SR_B5"]).rename("NDWI")
    ndmi = img.normalizedDifference(["SR_B5", "SR_B6"]).rename("NDMI")
    ndiib7 = img.normalizedDifference(["SR_B5", "SR_B7"]).rename("NDIIB7")
    albedo = img.expression(
        "((0.356*B1)+(0.130*B2)+(0.373*B3)+(0.085*B4)+(0.072*B5)-0.018)/1.016",
        {
            "B1": img.select("SR_B1"),
            "B2": img.select("SR_B2"),
            "B3": img.select("SR_B3"),
            "B4": img.select("SR_B4"),
            "B5": img.select("SR_B5"),
        },
    ).rename("Albedo")
    lst = img.select("ST_B10").multiply(0.00341802).add(149.0).rename("LST")
    return img.addBands([ndvi, savi, msavi, ndbi, ndwi, ndmi, ndiib7, albedo, lst])


def predict_daily_et(
    ls_img: ee.Image, region: ee.Geometry, classifier: ee.Classifier
) -> ee.Image:
    idx = calc_landsat_indices(ls_img)
    clim = (
        ee.ImageCollection("NASA/GLDAS/V021/NOAH/G025/T3H")
        .filterBounds(region)
        .filterDate(
            ls_img.date().advance(-12, "hour"), ls_img.date().advance(12, "hour")
        )
        .mean()
        .resample("bilinear")
        .reproject(crs=ls_img.select("SR_B5").projection(), scale=30)
    )
    return (
        idx.addBands(clim)
        .select(FEATURE_BANDS)
        .classify(classifier)
        .rename("ET_daily")
        .set("system:time_start", ls_img.date().millis())
    )


def make_raw_monthly(date, agri_month, ls_col, region, classifier, year, proj=None):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()

    month = start.get("month")

    monthly_collection = ls_col.filterDate(start, end)

    source_count = monthly_collection.size()
    et_collection = monthly_collection.map(
        lambda img: cast_monthly_band(
            predict_daily_et(img, region, classifier),
            "ET_daily",
        )
    )
    safe_collection = et_collection.merge(
        ee.ImageCollection.fromImages([empty_monthly_band("ET_daily", proj)])
    )
    et = safe_collection.mean().rename("ET_daily")
    return (
        ensure_monthly_band(et, "ET_daily", proj)
        .set("month", agri_month)
        .set("calendar_month", month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("is_placeholder", source_count.eq(0))
    )
