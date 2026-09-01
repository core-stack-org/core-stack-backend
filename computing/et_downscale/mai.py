import ee

from computing.et_downscale.helper import (
    MONTH_ABBR,
    crop_year_start_month,
    divide_where_valid,
    ee_annual_mean_band,
    export_product_asset,
    finalize_export_image,
    interpolate_monthly_stack,
    load_product_monthly_stack,
    product_asset_id,
)


def generate_mai(cfg, region):
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)

    aet_stack = load_product_monthly_stack(cfg, "aet", "ET")
    pet_stack = load_product_monthly_stack(cfg, "pet", "PET")
    proj = aet_stack.select("ET_01").projection()

    mai_raw = build_mai_image(aet_stack, pet_stack)
    mai_monthly = interpolate_monthly_stack(
        mai_raw,
        "MAI",
        region,
        year,
        proj=proj,
        start_month=start_month,
    )
    mai_annual = ee_annual_mean_band(
        mai_monthly, "MAI", band_name="MAI_annual"
    )
    mai_image = finalize_export_image(
        mai_monthly,
        mai_annual,
        region,
        metadata={
            "application": "mai",
            "units": "ratio (AET/PET)",
            "formula": "AET / PET",
            "aet_asset": product_asset_id(cfg, "aet"),
            "pet_asset": product_asset_id(cfg, "pet"),
            "valid_data_rule": "Calculated where AET and PET are valid and PET > 0",
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "Monthly Moisture Adequacy Index from AET/PET + annual mean",
        },
        band_descriptions=[f"MAI_{abbr}" for abbr in MONTH_ABBR] + ["MAI_annual"],
        default_proj=proj,
    )
    spec = export_product_asset("mai", "Moisture Adequacy Index (MAI)", mai_image, cfg)
    return spec


def build_mai_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """MAI_01...12 = AET / PET."""
    bands = []
    for month in range(1, 13):
        mai = divide_where_valid(
            aet_stack.select(f"ET_{month:02d}"),
            pet_stack.select(f"PET_{month:02d}"),
        ).rename(f"MAI_{month:02d}")
        bands.append(mai.float())
    return ee.Image.cat(bands)
