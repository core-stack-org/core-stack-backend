import ee

from computing.et_downscale.aet import build_aet_stack
from computing.et_downscale.helper import (
    MONTH_ABBR,
    MODIS_COL,
    build_classifier,
    get_proj_30m,
    build_common_pixel_mask,
    ee_annual_mean_band,
    finalize_export_image,
    export_product_asset,
    monthly_valid_mask,
)
from computing.et_downscale.pet import build_pet_stack


def generate_kc(
    cfg,
    region,
    footprint=None,
    common_mask=None,
    grid_proj=None,
    aet_stack=None,
    pet_stack=None,
):
    year = cfg["year"]

    if pet_stack is None:
        print("  Building PET stack (MODIS MOD16A2) ...")
        proj = get_proj_30m(region, year)
        pet_stack = build_pet_stack(region, year, MODIS_COL, proj)

    if aet_stack is None:
        print("  Building AET stack ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

        grid_proj = aet_stack.select("ET_01").projection()
        common_mask = build_common_pixel_mask(region, grid_proj)
        footprint = monthly_valid_mask(aet_stack, "ET")

    kc_monthly = build_kc_image(aet_stack, pet_stack).updateMask(footprint)
    kc_annual = ee_annual_mean_band(kc_monthly, "KC", band_name="KC_annual").updateMask(
        footprint
    )
    kc_image = finalize_export_image(
        kc_monthly,
        kc_annual,
        region,
        metadata={
            "application": "kc",
            "units": "ratio (AET/PET)",
            "formula": "AET / PET",
            "modis_collection": MODIS_COL,
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "Monthly Kc proxy from AET/PET + annual mean",
        },
        band_descriptions=[f"KC_{abbr}" for abbr in MONTH_ABBR] + ["KC_annual"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("kc", "Crop Coefficient (Kc)", kc_image, cfg)
    return spec


def build_kc_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """KC_01...12 = AET / PET (0-1)"""
    bands = []
    for month in range(1, 13):
        kc = (
            aet_stack.select(f"ET_{month:02d}")
            .divide(pet_stack.select(f"PET_{month:02d}"))
            .rename(f"KC_{month:02d}")
            .float()
        )
        bands.append(kc)
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack
