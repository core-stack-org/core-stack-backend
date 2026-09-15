import ee

from computing.et_downscale.helper import (
    crop_year_start_month,
    divide_where_valid,
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    interpolate_monthly_stack,
    load_product_monthly_stack,
    product_asset_id,
)


# =============================================================================
# DERIVED APPLICATION 3 - WUE
# =============================================================================


def generate_wue(cfg, region):
    """
    Water Use Efficiency = GPP / AET (g C / kg H2O)
    Output  : wue_<tehsil>_<year> GEE asset (13 bands)
    """
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)

    gpp_stack = load_product_monthly_stack(cfg, "gpp", "GPP")
    aet_stack = load_product_monthly_stack(cfg, "aet", "ET")
    proj = aet_stack.select("ET_01").projection()

    wue_raw = build_wue_image(gpp_stack, aet_stack)
    wue_monthly = interpolate_monthly_stack(
        wue_raw,
        "WUE",
        region,
        year,
        proj=proj,
        start_month=start_month,
    )
    wue_annual = ee_annual_mean_band(
        wue_monthly, "WUE", band_name="WUE_annual"
    )
    wue_image = finalize_export_image(
        wue_monthly,
        wue_annual,
        region,
        metadata={
            "application": "wue",
            "units": "g C / kg H2O",
            "formula": "GPP / AET",
            "gpp_asset": product_asset_id(cfg, "gpp"),
            "aet_asset": product_asset_id(cfg, "aet"),
            "gpp_method": "PAR x fAPAR x eps_max x TMIN_scalar x VPD_scalar",
            "aet_method": "Landsat8 + GLDAS features -> Random Forest",
            "bplut_source": "MOD17 C6 BPLUT / MCD12Q1 IGBP LC_Type1",
            "valid_data_rule": "Calculated where GPP and AET are valid and AET > 0",
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "WUE per month + annual mean at 30 m",
        },
        band_descriptions=[f"WUE_{abbr}_gC_per_kgH2O" for abbr in MONTH_ABBR]
        + ["WUE_annual_mean"],
        default_proj=proj,
    )
    spec = export_product_asset("wue", "WUE", wue_image, cfg)
    return spec


def build_wue_image(gpp_stack: ee.Image, aet_stack: ee.Image) -> ee.Image:
    """WUE_01...12 = GPP / AET (g C / kg H2O)."""
    bands = []
    for month in range(1, 13):
        aet = aet_stack.select(f"ET_{month:02d}")
        gpp = gpp_stack.select(f"GPP_{month:02d}")
        wue = divide_where_valid(gpp, aet)
        bands.append(wue.rename(f"WUE_{month:02d}").float())
    return ee.Image.cat(bands)
