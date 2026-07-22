import ee

try:
    from .helper import (
        build_classifier,
        cast_monthly_band,
        common_metadata,
        crop_month_abbrs,
        crop_year_start_date,
        crop_year_start_month,
        ee_annual_total_band,
        empty_monthly_band,
        ensure_monthly_band,
        export_product_asset,
        fill_monthly_collection,
        get_proj_30m,
        monthly_collection_to_stack,
        finalize_export_image,
    )
except ImportError:
    from helper import (
        build_classifier,
        cast_monthly_band,
        common_metadata,
        crop_month_abbrs,
        crop_year_start_date,
        crop_year_start_month,
        ee_annual_total_band,
        empty_monthly_band,
        ensure_monthly_band,
        export_product_asset,
        fill_monthly_collection,
        get_proj_30m,
        monthly_collection_to_stack,
        finalize_export_image,
    )


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
GLDAS_FEATURE_BANDS = FEATURE_BANDS[9:]
LANDSAT_COLLECTION = "LANDSAT/LC08/C02/T1_L2"
GLDAS_COLLECTION = "NASA/GLDAS/V021/NOAH/G025/T3H"
MODEL_OUTPUT_SCALE = 0.1


def generate_aet(cfg: dict, region: ee.Geometry):
    """
    Monthly mean daily AET for every 30 m pixel.
    Output: aet_<asset_suffix>_<year> GEE asset (13 bands).
    """
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)

    classifier = build_classifier(cfg["model_aez"])
    aet_stack = build_aet_stack(region, classifier, year, start_month=start_month)

    grid_proj = aet_stack.select("ET_01").projection()
    aet_monthly = aet_stack.multiply(MODEL_OUTPUT_SCALE)
    aet_annual = ee_annual_total_band(
        aet_monthly,
        "ET",
        year,
        band_name="ET_annual",
        start_month=start_month,
    )

    metadata = common_metadata(cfg, "aet")
    metadata.update(
        {
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "model_aez": cfg["model_aez"],
            "model_output_scale": MODEL_OUTPUT_SCALE,
            "interpolation": (
                "+/-45 days for Aug-May; Jul uses Aug only; Jun uses May only"
            ),
            "description": (
                "Bands 1-12: mean daily AET per crop-year month at 30 m; "
                "band 13: crop-year total AET"
            ),
        }
    )
    aet_image = finalize_export_image(
        aet_monthly,
        aet_annual,
        region,
        metadata=metadata,
        band_descriptions=[
            f"ET_{abbr}_daily_mm" for abbr in crop_month_abbrs(start_month)
        ]
        + ["ET_annual_mm"],
        default_proj=grid_proj,
    )
    return export_product_asset("aet", "AET", aet_image, cfg)


def build_aet_stack(
    region: ee.Geometry,
    classifier: ee.Classifier,
    year: int,
    proj: ee.Projection = None,
    start_month: int = 7,
) -> ee.Image:
    """Build ET_01...ET_12 in crop-year order (model units: 0.1 mm/day)."""
    start_date = crop_year_start_date(year, start_month)
    ls_col = (
        ee.ImageCollection(LANDSAT_COLLECTION)
        .filterBounds(region)
        .filterDate(start_date, start_date.advance(12, "month"))
    )

    if proj is None:
        proj = get_proj_30m(region, year, start_month=start_month)

    months = ee.List.sequence(0, 11)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: make_raw_monthly(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                ls_col,
                region,
                classifier,
                proj,
            )
        )
    )
    interp_col = fill_monthly_collection(raw_monthly, "ET_daily", proj=proj)
    return monthly_collection_to_stack(interp_col, "ET_daily", "ET_", region)


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
    """Predict ET for one Landsat observation, or return an empty ET band."""
    idx = calc_landsat_indices(ls_img)
    gldas = (
        ee.ImageCollection(GLDAS_COLLECTION)
        .filterBounds(region)
        .filterDate(
            ls_img.date().advance(-12, "hour"), ls_img.date().advance(12, "hour")
        )
    )
    proj = ls_img.select("SR_B5").projection()
    has_gldas = gldas.size().gt(0)
    empty_clim = (
        ee.Image.constant([0] * len(GLDAS_FEATURE_BANDS))
        .rename(GLDAS_FEATURE_BANDS)
        .updateMask(ee.Image.constant(0))
        .setDefaultProjection(proj)
    )
    clim = (
        ee.Image(
            ee.Algorithms.If(
                has_gldas,
                gldas.mean().select(GLDAS_FEATURE_BANDS),
                empty_clim,
            )
        )
        .resample("bilinear")
        .reproject(crs=proj, scale=30)
    )
    prediction = (
        idx.addBands(clim)
        .select(FEATURE_BANDS)
        .classify(classifier)
        .rename("ET_daily")
    )
    has_source = ee.Number(ee.Algorithms.If(has_gldas, 1, 0))
    return (
        cast_monthly_band(prediction, "ET_daily")
        .set("system:time_start", ls_img.date().millis())
        .set("has_source", has_source)
    )


def make_raw_monthly(date, agri_month, ls_col, region, classifier, proj=None):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()
    calendar_month = start.get("month")
    monthly_collection = ls_col.filterDate(start, end)

    landsat_count = monthly_collection.size()
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
    source_count = ee.Number(
        ee.Algorithms.If(
            landsat_count.gt(0),
            et_collection.aggregate_sum("has_source"),
            0,
        )
    )
    return (
        ensure_monthly_band(et, "ET_daily", proj)
        .set("month", agri_month)
        .set("calendar_month", calendar_month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("landsat_count", landsat_count)
        .set("is_placeholder", source_count.eq(0))
    )


def main():
    try:
        from .et_downscale import main as run_et_application
    except ImportError:
        from et_downscale import main as run_et_application

    run_et_application(default_application="aet")


if __name__ == "__main__":
    main()
