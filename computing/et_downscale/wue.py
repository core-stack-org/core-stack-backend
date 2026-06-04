import ee

from computing.et_downscale.aet import build_aet_stack
from computing.et_downscale.gpp import build_gpp_stack
from computing.et_downscale.helper import (
    build_classifier,
    get_proj_30m,
    build_common_pixel_mask,
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    monthly_valid_mask,
)


# =============================================================================
# DERIVED APPLICATION 3 - WUE
# =============================================================================


def generate_wue(
    cfg,
    region,
    footprint=None,
    common_mask=None,
    grid_proj=None,
    aet_stack=None,
    gpp_stack=None,
):
    """
    Water Use Efficiency = GPP / AET (g C / kg H2O)
    Output  : wue_<tehsil>_<year> GEE asset (13 bands)
    """
    year = cfg["year"]

    if gpp_stack is None:
        print("  Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
        proj = get_proj_30m(region, year)
        gpp_stack = build_gpp_stack(region, year, proj)

    if aet_stack is None:
        print("  Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

        grid_proj = aet_stack.select("ET_01").projection()
        common_mask = build_common_pixel_mask(region, grid_proj)
        footprint = monthly_valid_mask(aet_stack, "ET")

    wue_monthly = build_wue_image(aet_stack, gpp_stack).updateMask(footprint)
    wue_annual = ee_annual_mean_band(
        wue_monthly, "WUE", band_name="WUE_annual"
    ).updateMask(footprint)
    wue_image = finalize_export_image(
        wue_monthly,
        wue_annual,
        region,
        metadata={
            "application": "wue",
            "units": "g C / kg H2O",
            "formula": "GPP (LUE) / AET (RF downscaled)",
            "gpp_method": "PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "aet_method": "Landsat8 + GLDAS features -> Random Forest",
            "bplut_source": "MOD17 C6 BPLUT / MCD12Q1 IGBP LC_Type1",
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "WUE per month + annual mean at 30 m",
        },
        band_descriptions=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR]
        + ["WUE_annual_mean"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("wue", "WUE", wue_image, cfg)
    return spec


def build_wue_image(aet_stack: ee.Image, gpp_stack: ee.Image) -> ee.Image:
    """WUE_01...12 = GPP / AET_mm (g C / kg H2O)."""
    bands = []
    for month in range(1, 13):
        aet_mm = aet_stack.select(f"ET_{month:02d}").multiply(0.1)
        wue = (
            gpp_stack.select(f"GPP_{month:02d}").divide(aet_mm).updateMask(aet_mm.gt(0))
        )
        wue = wue.updateMask(wue.gte(0)).updateMask(wue.lte(50))
        bands.append(wue.rename(f"WUE_{month:02d}").float())
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack
