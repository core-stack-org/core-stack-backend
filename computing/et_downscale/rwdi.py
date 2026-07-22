import ee

try:
    from .helper import (
        common_metadata,
        crop_month_abbrs,
        crop_year_start_month,
        ee_annual_mean_band,
        export_product_asset,
        finalize_export_image,
        load_product_monthly_stack,
        product_asset_id,
    )
except ImportError:
    from helper import (
        common_metadata,
        crop_month_abbrs,
        crop_year_start_month,
        ee_annual_mean_band,
        export_product_asset,
        finalize_export_image,
        load_product_monthly_stack,
        product_asset_id,
    )


def generate_rwdi(cfg: dict, region: ee.Geometry):
    """Read the exported MAI asset and export RWDI without interpolation."""
    start_month = crop_year_start_month(cfg)

    mai_stack = load_product_monthly_stack(cfg, "mai", "MAI")
    proj = mai_stack.select("MAI_01").projection()

    rwdi_monthly = build_rwdi_image(mai_stack)
    rwdi_annual = ee_annual_mean_band(
        rwdi_monthly,
        "RWDI",
        band_name="RWDI_annual",
    )

    metadata = common_metadata(cfg, "rwdi")
    metadata.update(
        {
            "units": "percent",
            "formula": "max(0, (1 - MAI) * 100)",
            "mai_asset": product_asset_id(cfg, "mai"),
            "valid_data_rule": (
                "Calculated independently for each month and pixel where MAI "
                "is valid; negative RWDI values are set to 0."
            ),
            "interpolation": "none; RWDI uses the already interpolated MAI asset",
            "description": "Monthly RWDI from MAI, plus crop-year mean",
        }
    )
    rwdi_image = finalize_export_image(
        rwdi_monthly,
        rwdi_annual,
        region,
        metadata=metadata,
        band_descriptions=[
            f"RWDI_{abbr}" for abbr in crop_month_abbrs(start_month)
        ]
        + ["RWDI_annual"],
        default_proj=proj,
    )
    return export_product_asset("rwdi", "RWDI", rwdi_image, cfg)


def build_rwdi_image(mai_stack: ee.Image) -> ee.Image:
    """Build RWDI_01...RWDI_12 = max(0, (1 - MAI) x 100)."""
    bands = []
    for month in range(1, 13):
        mai = mai_stack.select(f"MAI_{month:02d}").float()
        rwdi = (
            mai.multiply(-1)
            .add(1)
            .multiply(100)
            .max(0)
            .rename(f"RWDI_{month:02d}")
            .float()
        )
        bands.append(rwdi)
    return ee.Image.cat(bands)


def main():
    try:
        from .et_downscale import main as run_et_application
    except ImportError:
        from et_downscale import main as run_et_application

    run_et_application(default_application="rwdi")


if __name__ == "__main__":
    main()
