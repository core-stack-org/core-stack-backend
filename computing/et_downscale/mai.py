import ee

try:
    from .helper import (
        common_metadata,
        crop_month_abbrs,
        crop_year_start_month,
        divide_where_valid,
        ee_annual_mean_band,
        export_product_asset,
        finalize_export_image,
        interpolate_monthly_stack,
        load_product_monthly_stack,
        product_asset_id,
    )
except ImportError:
    from helper import (
        common_metadata,
        crop_month_abbrs,
        crop_year_start_month,
        divide_where_valid,
        ee_annual_mean_band,
        export_product_asset,
        finalize_export_image,
        interpolate_monthly_stack,
        load_product_monthly_stack,
        product_asset_id,
    )


def generate_mai(cfg: dict, region: ee.Geometry):
    """Read exported AET/PET assets and export interpolated monthly MAI."""
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
        mai_monthly,
        "MAI",
        band_name="MAI_annual",
    )

    metadata = common_metadata(cfg, "mai")
    metadata.update(
        {
            "units": "ratio (AET/PET)",
            "formula": "AET / PET",
            "aet_asset": product_asset_id(cfg, "aet"),
            "pet_asset": product_asset_id(cfg, "pet"),
            "valid_data_rule": (
                "Calculated independently for each month and pixel where AET "
                "and PET are valid and PET > 0."
            ),
            "interpolation": (
                "+/-45 days for Aug-May; Jul uses Aug only; Jun uses May only"
            ),
            "description": (
                "Monthly Moisture Adequacy Index after pixel-wise temporal "
                "gap filling, plus crop-year mean"
            ),
        }
    )
    mai_image = finalize_export_image(
        mai_monthly,
        mai_annual,
        region,
        metadata=metadata,
        band_descriptions=[
            f"MAI_{abbr}" for abbr in crop_month_abbrs(start_month)
        ]
        + ["MAI_annual"],
        default_proj=proj,
    )
    return export_product_asset(
        "mai",
        "Moisture Adequacy Index (MAI)",
        mai_image,
        cfg,
    )


def build_mai_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """Build raw monthly MAI using a separate valid-pixel intersection per month."""
    bands = []
    for month in range(1, 13):
        aet = aet_stack.select(f"ET_{month:02d}")
        pet = pet_stack.select(f"PET_{month:02d}")
        mai = divide_where_valid(aet, pet).rename(f"MAI_{month:02d}")
        bands.append(mai.float())
    return ee.Image.cat(bands)


def main():
    try:
        from .et_downscale import main as run_et_application
    except ImportError:
        from et_downscale import main as run_et_application

    run_et_application(default_application="mai")


if __name__ == "__main__":
    main()
