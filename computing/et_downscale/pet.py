import ee

from computing.et_downscale.helper import (
    crop_year_start_date,
    crop_year_start_month,
    get_proj_30m,
    MODIS_COL,
    ee_annual_total_band,
    finalize_export_image,
    export_product_asset,
    MONTH_ABBR,
    fill_monthly_collection,
    monthly_collection_to_stack,
    cast_monthly_band,
    empty_monthly_band,
    ensure_monthly_band,
)


MODIS_SCALE_FACTOR = 0.1
MODIS_COMPOSITE_DAYS = 8
MODIS_VALID_MAX = 32700


def generate_pet(cfg, region):
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)
    modis_col_id = cfg.get("modis_collection", MODIS_COL)

    proj = get_proj_30m(region, year, start_month=start_month)
    pet_stack = build_pet_stack(region, year, modis_col_id, proj, start_month)

    pet_monthly = pet_stack.multiply(MODIS_SCALE_FACTOR)
    pet_annual = ee_annual_total_band(
        pet_monthly, "PET", year, band_name="PET_annual", start_month=start_month
    )
    pet_image = finalize_export_image(
        pet_monthly,
        pet_annual,
        region,
        metadata={
            "application": "pet",
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "modis_collection": modis_col_id,
            "source_scale_factor": MODIS_SCALE_FACTOR,
            "source_composite_days": MODIS_COMPOSITE_DAYS,
            "interpolation": "+/-45 days for Aug-May; Jul uses Aug only; Jun uses May only",
            "year": str(year),
            "asset_suffix": cfg["asset_suffix"],
            "roi_path": cfg["roi_path"],
            "description": "Bands 1-12: mean daily PET per month at 30 m; band 13: annual total PET",
        },
        band_descriptions=[f"PET_{abbr}_daily_mm" for abbr in MONTH_ABBR]
        + ["PET_annual_mm"],
        default_proj=proj,
    )
    spec = export_product_asset("pet", "PET", pet_image, cfg)
    return spec


def _prepare_pet_image(img: ee.Image, proj: ee.Projection) -> ee.Image:
    """Convert one 8-day PET composite to a 30 m daily-rate image."""
    pet = ee.Image(img).select("PET").float()
    pet = pet.updateMask(pet.gte(0)).updateMask(pet.lte(MODIS_VALID_MAX))
    return (
        pet.divide(MODIS_COMPOSITE_DAYS)
        .resample("bilinear")
        .reproject(crs=proj, scale=30)
        .rename("PET_daily")
    )


def _make_raw_monthly_pet(date, agri_month, modis_col, proj):
    start = ee.Date(date)
    end = start.advance(1, "month")
    mid = start.advance(15, "day").millis()

    month = start.get("month")

    monthly_collection = modis_col.filterDate(start, end)
    source_count = monthly_collection.size()
    pet_collection = (
        monthly_collection.select("PET")
        .map(lambda img: _prepare_pet_image(img, proj))
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
    region: ee.Geometry,
    year: int,
    modis_col_id: str,
    proj: ee.Projection,
    start_month: int = 7,
) -> ee.Image:
    """
    12-band PET stack PET_01...PET_12 (0.1 mm/day) at 30 m.
    MOD16A2 is 8-day composite; divide by 8 for daily rate.
    500 m MODIS pixel bilinearly resampled to the 30 m Landsat grid.
    Months with no MODIS composites are filled by the shared helper.
    """
    modis_col = ee.ImageCollection(modis_col_id).filterBounds(region)
    start_date = crop_year_start_date(year, start_month)
    months = ee.List.sequence(0, 11)
    raw_monthly = ee.ImageCollection.fromImages(
        months.map(
            lambda agri_month_idx: _make_raw_monthly_pet(
                start_date.advance(agri_month_idx, "month"),
                ee.Number(agri_month_idx).add(1),
                modis_col,
                proj,
            )
        )
    )
    interp_col = fill_monthly_collection(raw_monthly, "PET_daily", proj=proj)
    stack = monthly_collection_to_stack(interp_col, "PET_daily", "PET_", region)
    return stack
