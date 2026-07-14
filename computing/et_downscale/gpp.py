import ee

from computing.et_downscale.aet import build_aet_stack
from computing.et_downscale.helper import (
    MCD12Q1_COL,
    fill_monthly_collection,
    build_classifier,
    get_proj_30m,
    build_common_pixel_mask,
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    cast_monthly_band,
    empty_monthly_band,
    ensure_monthly_band,
    monthly_valid_mask,
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
# =============================================================================
# GPP / WUE - GEE IMAGE BUILDERS
# =============================================================================


def generate_gpp(
    cfg, region, common_mask=None, footprint=None, grid_proj=None, proj=None
):
    year = cfg["year"]
    asset_suffix = cfg["asset_suffix"]

    if footprint is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

        grid_proj = aet_stack.select("ET_01").projection()
        common_mask = build_common_pixel_mask(region, grid_proj)
        footprint = monthly_valid_mask(aet_stack, "ET")

        proj = get_proj_30m(region, year)

    gpp_stack = build_gpp_stack(region, year, proj)
    gpp_monthly = gpp_stack.updateMask(footprint)

    gpp_annual = ee_annual_mean_band(
        gpp_monthly, "GPP", band_name="GPP_annual"
    ).updateMask(footprint)
    gpp_image = finalize_export_image(
        gpp_monthly,
        gpp_annual,
        region,
        metadata={
            "application": "gpp",
            "units": "g C / m2 / day",
            "method": "LUE: PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "par_source": "GLDAS SWdown_f_tavg * 0.0864 * 0.45",
            "fapar_source": "Landsat 8 NDVI -> 1.24*NDVI - 0.168",
            "bplut_source": "MOD17 C6 / MCD12Q1 IGBP LC_Type1",
            "tmin_source": "GLDAS Tair_f_inst monthly minimum (K-273.15)",
            "vpd_source": "GLDAS Tair+Qair+Psurf Magnus formula",
            "year": str(year),
            "asset_suffix": asset_suffix,
            "roi_path": cfg["roi_path"],
            "description": "Mean daily GPP per month (LUE) + annual mean",
        },
        band_descriptions=[f"GPP_{abbr}_gC_m2_day" for abbr in MONTH_ABBR]
        + ["GPP_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("gpp", "GPP", gpp_image, cfg)
    return gpp_stack, spec


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


def build_gpp_stack(region: ee.Geometry, year: int, proj: ee.Projection) -> ee.Image:
    """
    Build a 12-band monthly GPP image at 30 m using the documented
    Light Use Efficiency method (Monteith 1972; MOD17 framework).

        GPP_m = PAR_m x fAPAR_m x eps_max x TMIN_scalar_m x VPD_scalar_m

    Band names: GPP_01 ... GPP_12
    Units     : g C / m2 / day  (mean daily GPP for the month)
    """
    lc_raw = (
        ee.ImageCollection(MCD12Q1_COL)
        .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
        .first()
        .select("LC_Type1")
    )
    # For categorical land cover, keep Earth Engine's default nearest-neighbour
    # sampling. Image.resample() only accepts bilinear/bicubic, so calling it
    # with "nearest" breaks the whole GPP graph during download.
    lc = lc_raw.reproject(crs=proj, scale=30)
    bplut_img = _build_bplut_image(lc)
    eps_max = bplut_img.select("eps_max")
    tmin_min = bplut_img.select("tmin_min")
    tmin_max = bplut_img.select("tmin_max")
    vpd_min = bplut_img.select("vpd_min")
    vpd_max = bplut_img.select("vpd_max")

    # year_start = ee.Date.fromYMD(year, 1, 1)
    # year_end = ee.Date.fromYMD(year + 1, 1, 1)

    start_date = ee.Date.fromYMD(year, 7, 1)
    ls_col = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(start_date, start_date.advance(12, "month"))
    )
    # months = ee.List.sequence(1, 12)
    start_date = ee.Date.fromYMD(year, 7, 1)
    months = ee.List.sequence(0, 11)

    raw_ndvi_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: make_raw_monthly_ndvi(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                ls_col,
                year,
                proj,
            )
        )
    )
    ndvi_monthly = fill_monthly_collection(raw_ndvi_monthly, "NDVI", proj=proj)

    ndvi_by_month = {
        month: ee.Image(
            ndvi_monthly.filter(ee.Filter.eq("month", month)).first()
        ).select("NDVI")
        for month in range(1, 13)
    }

    bands = []
    for agri_month_i in range(12):
        start = ee.Date(start_date.advance(agri_month_i, "month"))
        end = start.advance(1, "month")

        gldas = (
            ee.ImageCollection("NASA/GLDAS/V021/NOAH/G025/T3H")
            .filterBounds(region)
            .filterDate(start, end)
        )

        def _gldas_mean_reproj(band):
            return (
                gldas.select(band)
                .mean()
                .resample("bilinear")
                .reproject(crs=proj, scale=30)
            )

        swdown = _gldas_mean_reproj("SWdown_f_tavg").multiply(0.0864)
        par = swdown.multiply(0.45)

        agri_month = agri_month_i + 1
        ndvi = ndvi_by_month[agri_month]

        # ndvi = ndvi_by_month[month]
        fapar = (
            ndvi.multiply(1.24)
            .subtract(0.168)
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )

        tmin_c = (
            gldas.select("Tair_f_inst")
            .min()
            .subtract(273.15)
            .resample("bilinear")
            .reproject(crs=proj, scale=30)
        )

        tair_c = _gldas_mean_reproj("Tair_f_inst").subtract(273.15)
        qair = _gldas_mean_reproj("Qair_f_inst")
        psurf = _gldas_mean_reproj("Psurf_f_inst")

        exponent = tair_c.multiply(17.67).divide(tair_c.add(243.5))
        es = exponent.exp().multiply(611.2)
        ea = psurf.multiply(qair).divide(qair.add(0.622))
        vpd = es.subtract(ea).max(ee.Image.constant(0.0))

        tmin_scalar = (
            tmin_c.subtract(tmin_min)
            .divide(tmin_max.subtract(tmin_min))
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )
        vpd_scalar = (
            vpd_max.subtract(vpd)
            .divide(vpd_max.subtract(vpd_min))
            .max(ee.Image.constant(0.0))
            .min(ee.Image.constant(1.0))
        )

        eps = eps_max.multiply(tmin_scalar).multiply(vpd_scalar)
        gpp = par.multiply(fapar).multiply(eps).rename(f"GPP_{agri_month:02d}").float()
        bands.append(gpp)

    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack.clip(region)


def make_raw_monthly_ndvi(date, agri_month, ls_col, year, proj=None):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()

    month = start.get("month")

    monthly_collection = ls_col.filterDate(start, end)
    source_count = monthly_collection.size()
    ndvi_collection = monthly_collection.map(
        lambda img: cast_monthly_band(
            img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI"),
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
        .set("calendar_month", month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("is_placeholder", source_count.eq(0))
    )
