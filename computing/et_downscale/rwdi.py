import ee

from computing.et_downscale.aet import build_aet_stack
from computing.et_downscale.helper import (
    build_classifier,
    get_proj_30m,
    MODIS_COL,
    build_common_pixel_mask,
    ee_annual_mean_band,
    finalize_export_image,
    MONTH_ABBR,
    export_product_asset,
    monthly_valid_mask,
)
from computing.et_downscale.pet import build_pet_stack


# =============================================================================
# DERIVED APPLICATION 1 - RWDI
# =============================================================================


def generate_rwdi(
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

    rwdi_monthly = build_rwdi_image(aet_stack, pet_stack).updateMask(footprint)
    rwdi_annual = ee_annual_mean_band(
        rwdi_monthly, "RWDI", band_name="RWDI_annual"
    ).updateMask(footprint)

    rwdi_image = finalize_export_image(
        rwdi_monthly,
        rwdi_annual,
        region,
        metadata={
            "application": "rwdi",
            "units": "percent",
            "formula": "(1 - AET/PET) * 100",
            "modis_collection": MODIS_COL,
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "RWDI per month + annual mean",
        },
        band_descriptions=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("rwdi", "RWDI", rwdi_image, cfg)
    return spec


def build_rwdi_image(aet_stack: ee.Image, pet_stack: ee.Image) -> ee.Image:
    """RWDI_01...12 = (1 - AET/PET) x 100 (%)"""
    bands = []
    for month in range(1, 13):
        rwdi = (
            ee.Image(1)
            .subtract(
                aet_stack.select(f"ET_{month:02d}").divide(
                    pet_stack.select(f"PET_{month:02d}")
                )
            )
            .multiply(100)
            .rename(f"RWDI_{month:02d}")
            .float()
        )
        bands.append(rwdi)
    stack = bands[0]
    for band in bands[1:]:
        stack = stack.addBands(band)
    return stack


# def run_rwdi(cfg: dict, region: ee.Geometry, aet_stack=None, pet_stack=None) -> str:
#     """
#     RWDI = (1 - AET/PET) x 100 (%)
#     Output  : rwdi_<tehsil>_<year> GEE asset (13 bands)
#     """
#     asset_suffix = cfg["asset_suffix"]
#     year = cfg["year"]
#
#     print(f"\n{'=' * 60}")
#     print(f"  [rwdi]  {asset_suffix}  |  {year}")
#     print(f"{'=' * 60}")
#
#     if aet_stack is None:
#         print("  Building AET stack ...")
#         classifier = build_classifier(cfg["model_aez"])
#         aet_stack = build_aet_stack(region, classifier, year)
#
#     if pet_stack is None:
#         print("  Building PET stack (MODIS MOD16A2) ...")
#         proj = get_proj_30m(region, year)
#         pet_stack = build_pet_stack(region, year, MODIS_COL, proj)
#
#     rwdi_img = build_rwdi_image(aet_stack, pet_stack)
#     grid_proj = aet_stack.select("ET_01").projection()
#     common_mask = build_common_pixel_mask(region, grid_proj)
#     footprint = aet_stack.select("ET_01").mask()
#     rwdi_monthly = rwdi_img.updateMask(footprint)
#     rwdi_annual = ee_annual_mean_band(
#         rwdi_monthly, "RWDI", band_name="RWDI_annual"
#     ).updateMask(footprint)
#     image = finalize_export_image(
#         rwdi_monthly,
#         rwdi_annual,
#         region,
#         metadata={
#             "application": "rwdi",
#             "units": "percent",
#             "formula": "(1 - AET/PET) * 100",
#             "modis_collection": MODIS_COL,
#             "year": str(year),
#             "asset_suffix": asset_suffix,
#             "roi_path": cfg["roi_path"],
#             "description": "Relative Water Deficit Index per month + annual mean",
#         },
#         band_descriptions=[f"RWDI_{abbr}" for abbr in MONTH_ABBR] + ["RWDI_annual"],
#         default_proj=grid_proj,
#         common_mask=common_mask,
#     )
#     task_spec = export_product_asset("rwdi", "RWDI", image, cfg)
#     if cfg.get("wait_exports", True):
#         wait_for_tasks([task_spec], cfg.get("poll_seconds", 30))
#     return task_spec["asset_id"]
