import ee

from computing.et_downscale.aet import build_aet_stack
from computing.et_downscale.helper import (
    build_classifier,
    get_proj_30m,
    MODIS_COL,
    build_common_pixel_mask,
    ee_annual_total_band,
    finalize_export_image,
    export_product_asset,
    MONTH_ABBR,
    fill_monthly_collection,
    monthly_collection_to_stack,
    cast_monthly_band,
    empty_monthly_band,
    ensure_monthly_band,
    monthly_valid_mask,
)


def generate_pet(
    cfg,
    region,
    common_mask=None,
    footprint=None,
    grid_proj=None,
):
    year = cfg["year"]

    if footprint is None:
        print("  Building AET stack (pixel-grid carrier) ...")
        classifier = build_classifier(cfg["model_aez"])
        aet_stack = build_aet_stack(region, classifier, year)

        grid_proj = aet_stack.select("ET_01").projection()
        common_mask = build_common_pixel_mask(region, grid_proj)
        footprint = monthly_valid_mask(aet_stack, "ET")

    proj = get_proj_30m(region, year)
    pet_stack = build_pet_stack(region, year, MODIS_COL, proj)

    pet_monthly = pet_stack.multiply(0.1).updateMask(footprint)
    pet_annual = ee_annual_total_band(
        pet_monthly, "PET", year, band_name="PET_annual"
    ).updateMask(footprint)
    pet_image = finalize_export_image(
        pet_monthly,
        pet_annual,
        region,
        metadata={
            "application": "pet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "modis_collection": MODIS_COL,
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
        band_descriptions=[f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR]
        + ["PET_annual_mm"],
        default_proj=grid_proj,
        common_mask=common_mask,
    )
    spec = export_product_asset("pet", "PET", pet_image, cfg)
    return pet_stack, proj, spec


def _make_raw_monthly_pet(date, agri_month, modis_col, year, proj):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()

    month = start.get("month")

    monthly_collection = modis_col.filterDate(start, end)
    source_count = monthly_collection.size()
    pet_collection = (
        monthly_collection.select("PET")
        .map(
            lambda img: img.divide(8)
            .resample("bilinear")
            .reproject(crs=proj, scale=30)
            .rename("PET_daily")
        )
        .map(lambda img: cast_monthly_band(img, "PET_daily"))
    )
    safe_collection = pet_collection.merge(
        ee.ImageCollection.fromImages([empty_monthly_band("PET_daily", proj)])
    )

    pet = safe_collection.mean().rename("PET_daily")

    return (
        ensure_monthly_band(pet, "PET_daily", proj)
        .set("month", agri_month)
        .set("calendar_month", month)
        .set("system:time_start", mid)
        .set("source_count", source_count)
        .set("is_placeholder", source_count.eq(0))
    )


def build_pet_stack(
    region: ee.Geometry, year: int, modis_col_id: str, proj: ee.Projection
) -> ee.Image:
    """
    12-band PET stack PET_01...PET_12 (0.1 mm/day) at 30 m.
    MOD16A2 is 8-day composite; divide by 8 for daily rate.
    500 m MODIS pixel bilinearly resampled to the 30 m Landsat grid.
    Months with no MODIS composites are filled using a +/-30 day window.
    """
    modis_col = ee.ImageCollection(modis_col_id).filterBounds(region)
    # months = ee.List.sequence(1, 12)
    start_date = ee.Date.fromYMD(year, 7, 1)
    months = ee.List.sequence(0, 11)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: _make_raw_monthly_pet(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                modis_col,
                year,
                proj,
            )
        )
    )
    interp_col = fill_monthly_collection(raw_monthly, "PET_daily", proj=proj)
    stack = monthly_collection_to_stack(interp_col, "PET_daily", "PET_", region)
    return stack
