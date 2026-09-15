import ee

from computing.et_downscale.helper import (
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    load_product_monthly_stack,
    product_asset_id,
)


# =============================================================================
# DERIVED APPLICATION 1 - RWDI
# =============================================================================


def generate_rwdi(cfg, region):
    year = int(cfg["year"])
    mai_stack = load_product_monthly_stack(cfg, "mai", "MAI")
    proj = mai_stack.select("MAI_01").projection()

    rwdi_monthly = build_rwdi_image(mai_stack)
    rwdi_annual = ee_annual_mean_band(
        rwdi_monthly, "RWDI", band_name="RWDI_annual"
    )

    rwdi_image = finalize_export_image(
        rwdi_monthly,
        rwdi_annual,
        region,
        metadata={
            "application": "rwdi",
            "units": "percent",
            "formula": "max(0, (1 - MAI) * 100)",
            "mai_asset": product_asset_id(cfg, "mai"),
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "RWDI from MAI per month + annual mean",
        },
        band_descriptions=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        default_proj=proj,
    )
    spec = export_product_asset("rwdi", "RWDI", rwdi_image, cfg)
    return spec


def build_rwdi_image(mai_stack: ee.Image) -> ee.Image:
    """RWDI_01...12 = max(0, (1 - MAI) x 100)."""
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
