import ee

try:
    from .helper import (
        MODIS_COL,
        cast_monthly_band,
        common_metadata,
        crop_month_abbrs,
        crop_year_start_date,
        crop_year_start_month,
        ee_annual_total_band,
        empty_monthly_band,
        ensure_monthly_band,
        export_product_asset,
        fill_monthly_collection,
        get_proj_30m,
        monthly_collection_to_stack,
        finalize_export_image,
    )
except ImportError:
    from helper import (
        MODIS_COL,
        cast_monthly_band,
        common_metadata,
        crop_month_abbrs,
        crop_year_start_date,
        crop_year_start_month,
        ee_annual_total_band,
        empty_monthly_band,
        ensure_monthly_band,
        export_product_asset,
        fill_monthly_collection,
        get_proj_30m,
        monthly_collection_to_stack,
        finalize_export_image,
    )


MODIS_SCALE_FACTOR = 0.1
MODIS_COMPOSITE_DAYS = 8
MODIS_VALID_MAX = 32700


def generate_pet(cfg: dict, region: ee.Geometry):
    """Build and export PET without using AET or any other generated product."""
    year = int(cfg["year"])
    start_month = crop_year_start_month(cfg)
    modis_col_id = cfg.get("modis_collection", MODIS_COL)

    proj = get_proj_30m(region, year, start_month=start_month)
    pet_stack = build_pet_stack(region, year, modis_col_id, proj, start_month)

    pet_monthly = pet_stack.multiply(MODIS_SCALE_FACTOR)
    pet_annual = ee_annual_total_band(
        pet_monthly,
        "PET",
        year,
        band_name="PET_annual",
        start_month=start_month,
    )

    metadata = common_metadata(cfg, "pet")
    metadata.update(
        {
            "units": "bands 1-12: mm/day; band 13: mm/yr",
            "source": "MODIS MOD16A2",
            "modis_collection": modis_col_id,
            "source_pixel_size_m": 500,
            "source_scale_factor": MODIS_SCALE_FACTOR,
            "source_composite_days": MODIS_COMPOSITE_DAYS,
            "resampling": "bilinear to the 30 m output grid",
            "interpolation": (
                "+/-45 days for Aug-May; Jul uses Aug only; Jun uses May only"
            ),
            "description": (
                "Bands 1-12: mean daily PET per crop-year month at 30 m; "
                "band 13: crop-year total PET"
            ),
        }
    )
    pet_image = finalize_export_image(
        pet_monthly,
        pet_annual,
        region,
        metadata=metadata,
        band_descriptions=[
            f"PET_{abbr}_daily_mm" for abbr in crop_month_abbrs(start_month)
        ]
        + ["PET_annual_mm"],
        default_proj=proj,
    )
    return export_product_asset("pet", "PET", pet_image, cfg)


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
    calendar_month = start.get("month")

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
        .set("calendar_month", calendar_month)
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
    12-band PET stack PET_01...PET_12 in crop-year order.
    Values remain in stored MODIS units divided by 8; generate_pet applies the
    0.1 scale factor to produce the exported mm/day bands.
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
    return monthly_collection_to_stack(interp_col, "PET_daily", "PET_", region)


def main():
    try:
        from .et_downscale import main as run_et_application
    except ImportError:
        from et_downscale import main as run_et_application

    run_et_application(default_application="pet")


if __name__ == "__main__":
    main()
