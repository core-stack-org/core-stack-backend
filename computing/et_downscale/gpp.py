import ee

from computing.et_downscale.helper import (
    MCD12Q1_COL,
    fill_monthly_collection,
    crop_year_start_date,
    crop_year_start_month,
    get_proj_30m,
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    cast_monthly_band,
    empty_monthly_band,
    ensure_monthly_band,
    monthly_collection_to_stack,
)

# ---------------------------------------------------------------------------
# CONSTANTS - GPP / WUE (Light Use Efficiency, MOD17 framework)
# ---------------------------------------------------------------------------
# BPLUT: IGBP LC_Type1 class -> (eps_max g_C/MJ, TMIN_min C, TMIN_max C,
#                                VPD_min Pa,   VPD_max Pa)
# Source: MOD17 Collection 6 - Running & Zhao (2015) Table 2.2
BPLUT = {
    1: (0.962, -8.0, 8.31, 650, 4600),  # Evergreen Needleleaf Forest
    2: (1.268, -8.0, 9.09, 800, 3100),  # Evergreen Broadleaf Forest
    3: (1.086, -8.0, 10.44, 650, 2300),  # Deciduous Needleleaf Forest
    4: (1.165, -6.0, 9.94, 650, 1650),  # Deciduous Broadleaf Forest
    5: (1.051, -7.0, 9.50, 650, 2400),  # Mixed Forest
    6: (1.281, -8.0, 8.61, 650, 4700),  # Closed Shrublands
    7: (0.841, -8.0, 8.80, 650, 4800),  # Open Shrublands
    8: (1.239, -8.0, 11.39, 650, 3200),  # Woody Savannas
    9: (1.206, -8.0, 11.39, 650, 3100),  # Savannas
    10: (0.860, -8.0, 12.02, 650, 5300),  # Grasslands - default fallback
    11: (0.860, -8.0, 12.02, 650, 5300),  # Permanent Wetlands -> Grassland
    12: (1.044, -8.0, 12.02, 650, 4300),  # Croplands
    13: (0.860, -8.0, 12.02, 650, 5300),  # Urban/Built-up -> Grassland
    14: (1.044, -8.0, 12.02, 650, 4300),  # Cropland/Natural Veg Mosaic
    15: (0.860, -8.0, 12.02, 650, 5300),  # Permanent Snow/Ice -> Grassland
    16: (0.860, -8.0, 12.02, 650, 5300),  # Barren/Sparsely Vegetated
    17: (0.860, -8.0, 12.02, 650, 5300),  # Water Bodies -> Grassland
}
_BPLUT_DEFAULT_CLASS = 10
LANDSAT_COLLECTION = "LANDSAT/LC08/C02/T1_L2"
GLDAS_COLLECTION = "NASA/GLDAS/V021/NOAH/G025/T3H"
W_M2_TO_MJ_M2_DAY = 0.0864
PAR_FRACTION = 0.45
FAPAR_SLOPE = 1.24
FAPAR_INTERCEPT = -0.168
KELVIN_TO_CELSIUS = 273.15
LANDSAT_SR_SCALE = 0.0000275
LANDSAT_SR_OFFSET = -0.2
GPP_GLDAS_BANDS = [
    "SWdown_f_tavg",
    "Tair_f_inst",
    "Qair_f_inst",
    "Psurf_f_inst",
]
# =============================================================================
# GPP / WUE - GEE IMAGE BUILDERS
# =============================================================================


def generate_gpp(cfg, region):
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)
    asset_suffix = cfg["asset_suffix"]

    proj = get_proj_30m(region, year, start_month=start_month)

    gpp_stack = build_gpp_stack(region, year, proj, start_month=start_month)

    gpp_annual = ee_annual_mean_band(
        gpp_stack, "GPP", band_name="GPP_annual"
    )
    gpp_image = finalize_export_image(
        gpp_stack,
        gpp_annual,
        region,
        metadata={
            "application": "gpp",
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "par_source": "GLDAS SWdown_f_tavg * 0.0864 * 0.45",
            "fapar_source": "QA-masked Landsat 8 L2 scaled SR NDVI -> 1.24*NDVI - 0.168",
            "bplut_source": "MOD17 C6 / MCD12Q1 IGBP LC_Type1",
            "landsat_qa_mask": "QA_PIXEL bits 0-5 clear; QA_RADSAT red/NIR/terrain clear",
            "tmin_source": "GLDAS Tair_f_inst monthly minimum (K-273.15)",
            "vpd_source": "GLDAS Tair+Qair+Psurf Magnus formula",
            "year": str(year),
            "asset_suffix": asset_suffix,
            "roi_path": cfg["roi_path"],
            "description": "Mean daily GPP per month (LUE) + annual mean",
        },
        band_descriptions=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR]
        + ["GPP_annual_mean"],
        default_proj=proj,
    )
    spec = export_product_asset("gpp", "GPP", gpp_image, cfg)
    return spec


def _build_bplut_image(lc_img: ee.Image) -> ee.Image:
    """
    Convert a MCD12Q1 LC_Type1 image to five BPLUT parameter images.

    Returns a 5-band image:
        eps_max   (g C / MJ)
        tmin_min  (C)
        tmin_max  (C)
        vpd_min   (Pa)
        vpd_max   (Pa)
    """
    from_list = list(BPLUT.keys())
    default_values = BPLUT[_BPLUT_DEFAULT_CLASS]

    def _remap_param(idx, name):
        to_list = [BPLUT[k][idx] for k in from_list]
        return (
            lc_img.remap(from_list, to_list, defaultValue=default_values[idx])
            .rename(name)
            .float()
        )

    eps_max = _remap_param(0, "eps_max")
    tmin_min = _remap_param(1, "tmin_min")
    tmin_max = _remap_param(2, "tmin_max")
    vpd_min = _remap_param(3, "vpd_min")
    vpd_max = _remap_param(4, "vpd_max")
    return ee.Image.cat([eps_max, tmin_min, tmin_max, vpd_min, vpd_max])


def build_gpp_stack(
    region: ee.Geometry,
    year: int,
    proj: ee.Projection,
    start_month: int = 7,
) -> ee.Image:
    """Build GPP_01...GPP_12 at 30 m in July-June crop-year order."""
    lc_col = (
        ee.ImageCollection(MCD12Q1_COL)
        .filterBounds(region)
        .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
    )
    lc_raw = ee.Image(
        ee.Algorithms.If(
            lc_col.size().gt(0),
            lc_col.first().select("LC_Type1"),
            empty_monthly_band("LC_Type1", proj),
        )
    )
    lc = lc_raw.reproject(crs=proj, scale=30)
    bplut_img = _build_bplut_image(lc)

    start_date = crop_year_start_date(year, start_month)
    ls_col = (
        ee.ImageCollection(LANDSAT_COLLECTION)
        .filterBounds(region)
        .filterDate(start_date.advance(-1, "month"), start_date.advance(13, "month"))
    )
    months = ee.List.sequence(-1, 12)
    raw_ndvi_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: make_raw_monthly_ndvi(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                ls_col,
                proj,
            )
        )
    )
    ndvi_monthly = fill_monthly_collection(raw_ndvi_monthly, "NDVI", proj=proj)
    ndvi_by_month = {
        month: ee.Image(
            ndvi_monthly.filter(ee.Filter.eq("month", month)).first()
        ).select("NDVI")
        for month in range(0, 14)
    }

    gldas_col = (
        ee.ImageCollection(GLDAS_COLLECTION)
        .filterBounds(region)
        .filterDate(start_date.advance(-1, "month"), start_date.advance(13, "month"))
    )
    monthly_images = []
    for agri_month_idx in range(-1, 13):
        agri_month = agri_month_idx + 1
        monthly_images.append(
            make_raw_monthly_gpp(
                start_date.advance(agri_month_idx, "month"),
                agri_month,
                gldas_col,
                ndvi_by_month[agri_month],
                bplut_img,
                proj,
            )
        )

    raw_gpp_monthly = ee.ImageCollection.fromImages(monthly_images)
    interp_col = fill_monthly_collection(raw_gpp_monthly, "GPP_daily", proj=proj)
    return monthly_collection_to_stack(interp_col, "GPP_daily", "GPP_", region)


def _gldas_mean(gldas: ee.ImageCollection, band: str, proj: ee.Projection) -> ee.Image:
    return (
        gldas.select(band)
        .mean()
        .resample("bilinear")
        .reproject(crs=proj, scale=30)
    )


def _calculate_monthly_gpp(
    gldas: ee.ImageCollection,
    ndvi: ee.Image,
    bplut_img: ee.Image,
    proj: ee.Projection,
) -> ee.Image:
    swdown = _gldas_mean(gldas, "SWdown_f_tavg", proj).multiply(
        W_M2_TO_MJ_M2_DAY
    ).max(0.0)
    par = swdown.multiply(PAR_FRACTION)
    fapar = ndvi.multiply(FAPAR_SLOPE).add(FAPAR_INTERCEPT).clamp(0.0, 1.0)

    tmin_c = (
        gldas.select("Tair_f_inst")
        .min()
        .subtract(KELVIN_TO_CELSIUS)
        .resample("bilinear")
        .reproject(crs=proj, scale=30)
    )
    tair_c = _gldas_mean(gldas, "Tair_f_inst", proj).subtract(KELVIN_TO_CELSIUS)
    qair = _gldas_mean(gldas, "Qair_f_inst", proj)
    psurf = _gldas_mean(gldas, "Psurf_f_inst", proj)

    exponent = tair_c.multiply(17.67).divide(tair_c.add(243.5))
    saturation_vapour_pressure = exponent.exp().multiply(611.2)
    actual_vapour_pressure = psurf.multiply(qair).divide(
        qair.multiply(0.378).add(0.622)
    )
    vpd = saturation_vapour_pressure.subtract(actual_vapour_pressure).max(0.0)

    eps_max = bplut_img.select("eps_max")
    tmin_min = bplut_img.select("tmin_min")
    tmin_max = bplut_img.select("tmin_max")
    vpd_min = bplut_img.select("vpd_min")
    vpd_max = bplut_img.select("vpd_max")
    tmin_scalar = (
        tmin_c.subtract(tmin_min)
        .divide(tmin_max.subtract(tmin_min))
        .clamp(0.0, 1.0)
    )
    vpd_scalar = (
        vpd_max.subtract(vpd)
        .divide(vpd_max.subtract(vpd_min))
        .clamp(0.0, 1.0)
    )
    epsilon = eps_max.multiply(tmin_scalar).multiply(vpd_scalar)
    return par.multiply(fapar).multiply(epsilon).rename("GPP_daily").float()


def make_raw_monthly_gpp(date, agri_month, gldas_col, ndvi, bplut_img, proj=None):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()
    calendar_month = start.get("month")
    gldas = gldas_col.filterDate(start, end)
    source_count = gldas.size()
    empty_gldas = (
        ee.Image.constant([0] * len(GPP_GLDAS_BANDS))
        .rename(GPP_GLDAS_BANDS)
        .updateMask(ee.Image.constant(0))
        .setDefaultProjection(proj)
    )
    safe_gldas = gldas.merge(ee.ImageCollection.fromImages([empty_gldas]))
    gpp = _calculate_monthly_gpp(safe_gldas, ndvi, bplut_img, proj)
    return (
        ensure_monthly_band(gpp, "GPP_daily", proj)
        .set("month", agri_month)
        .set("calendar_month", calendar_month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("is_placeholder", source_count.eq(0))
    )


def make_raw_monthly_ndvi(date, agri_month, ls_col, proj=None):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()

    calendar_month = start.get("month")

    monthly_collection = ls_col.filterDate(start, end)
    source_count = monthly_collection.size()
    ndvi_collection = monthly_collection.map(
        lambda img: cast_monthly_band(
            _landsat_ndvi(img),
            "NDVI",
        )
    )
    safe_collection = ndvi_collection.merge(
        ee.ImageCollection.fromImages([empty_monthly_band("NDVI", proj)])
    )
    ndvi = safe_collection.mean().rename("NDVI")
    return (
        ensure_monthly_band(ndvi, "NDVI", proj)
        .set("month", agri_month)
        .set("calendar_month", calendar_month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("is_placeholder", source_count.eq(0))
    )


def _landsat_ndvi(img: ee.Image) -> ee.Image:
    clean_img = _mask_landsat_l2_sr(img)
    nir = clean_img.select("SR_B5").multiply(LANDSAT_SR_SCALE).add(LANDSAT_SR_OFFSET)
    red = clean_img.select("SR_B4").multiply(LANDSAT_SR_SCALE).add(LANDSAT_SR_OFFSET)
    denominator = nir.add(red)
    reflectance_mask = (
        nir.gte(0.0)
        .multiply(red.gte(0.0))
        .multiply(nir.lte(1.0))
        .multiply(red.lte(1.0))
        .multiply(denominator.gt(0.0))
        .gt(0)
    )
    return (
        nir.subtract(red)
        .divide(denominator)
        .updateMask(reflectance_mask)
        .clamp(-1.0, 1.0)
        .rename("NDVI")
    )


def _mask_landsat_l2_sr(img: ee.Image) -> ee.Image:
    qa = img.select("QA_PIXEL")
    qa_mask = (
        qa.bitwiseAnd(1 << 0)
        .eq(0)
        .multiply(qa.bitwiseAnd(1 << 1).eq(0))
        .multiply(qa.bitwiseAnd(1 << 2).eq(0))
        .multiply(qa.bitwiseAnd(1 << 3).eq(0))
        .multiply(qa.bitwiseAnd(1 << 4).eq(0))
        .multiply(qa.bitwiseAnd(1 << 5).eq(0))
        .gt(0)
    )
    red_nir_clear = img.select("QA_RADSAT").bitwiseAnd(
        (1 << 3) + (1 << 4) + (1 << 11)
    ).eq(0)
    return img.updateMask(qa_mask).updateMask(red_nir_clear)
