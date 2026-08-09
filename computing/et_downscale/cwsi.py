import ee

from computing.et_downscale.helper import (
    NODATA,
    _asset_token,
    _prepare_asset_target,
    _start_asset_export,
    crop_month_index,
    crop_year_start_month,
    load_product_monthly_stack,
    product_asset_id,
)


DEFAULT_KHARIF_MONTHS = [7, 8, 9, 10]
DEFAULT_DROUGHT_MAI_THRESHOLD = 0.50
CWSI_BAND_NAMES = ["b1", "b2"]


def csv_ints(value) -> list[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [int(item) for item in value]
    return [int(part.strip()) for part in str(value).split(",") if part.strip()]


def _normalise_cfg(cfg: dict) -> dict:
    out = dict(cfg)
    out["start_year"] = int(out.get("start_year", out.get("year")))
    out["end_year"] = int(out.get("end_year", out["start_year"]))
    if out["end_year"] < out["start_year"]:
        raise ValueError("end_year must be greater than or equal to start_year")

    months = csv_ints(out.get("kharif_months", DEFAULT_KHARIF_MONTHS))
    if not months or any(month < 1 or month > 12 for month in months):
        raise ValueError("kharif_months must contain calendar months from 1 to 12")
    out["kharif_months"] = months
    out["drought_mai_threshold"] = float(
        out.get("drought_mai_threshold", DEFAULT_DROUGHT_MAI_THRESHOLD)
    )
    out["crop_year_start_month"] = crop_year_start_month(out)
    out["nodata"] = float(out.get("nodata", NODATA))
    out["output_label"] = out.get("output_label") or "cwsi"
    return out


def build_output_asset_id(cfg: dict) -> str:
    root = str(cfg["asset_root"]).rstrip("/")
    label = _asset_token(cfg.get("output_label", "cwsi"))
    suffix = _asset_token(cfg.get("asset_suffix"))
    return f"{root}/{label}_{suffix}_{cfg['start_year']}_to_{cfg['end_year']}"


def ensure_yearly_mai_assets(cfg: dict, region: ee.Geometry, years: list[int]) -> None:
    """Create every missing yearly MAI asset and wait until it is saved."""
    from computing.et_downscale.et_downscale import ensure_product_asset

    print("\n[cwsi phase 1/2] Creating/checking yearly MAI assets ...")
    for year in years:
        year_cfg = dict(cfg)
        year_cfg["year"] = int(year)
        year_cfg["overwrite_assets"] = False
        ensure_product_asset("mai", year_cfg, region)


def select_kharif_mai_images(cfg: dict, year: int) -> list[ee.Image]:
    """Read valid Kharif MAI bands for one crop year without interpolation."""
    year_cfg = dict(cfg)
    year_cfg["year"] = int(year)
    mai_stack = load_product_monthly_stack(year_cfg, "mai", "MAI")
    start_month = crop_year_start_month(year_cfg)

    images = []
    for calendar_month in cfg["kharif_months"]:
        crop_month = crop_month_index(calendar_month, start_month)
        images.append(
            mai_stack.select(f"MAI_{crop_month:02d}").rename("MAI").float()
        )
    return images


def year_drought_and_mean(cfg: dict, year: int) -> tuple[ee.Image, ee.Image]:
    """Return drought flag and mean valid Kharif MAI for one year."""
    month_images = select_kharif_mai_images(cfg, year)
    month_collection = ee.ImageCollection.fromImages(month_images)
    valid_count = ee.ImageCollection.fromImages(
        [image.mask().gt(0).unmask(0).rename("valid") for image in month_images]
    ).sum()
    usable_year = valid_count.gt(0)

    kharif_mean = month_collection.mean().rename("kharif_mean_mai")
    kharif_mean = kharif_mean.updateMask(usable_year).float()

    drought_flag = (
        month_collection.map(
            lambda image: ee.Image(image)
            .lte(cfg["drought_mai_threshold"])
            .rename("drought")
        )
        .max()
        .updateMask(usable_year)
        .unmask(0)
        .rename("drought_flag")
        .float()
    )
    drought_year_mean = kharif_mean.updateMask(drought_flag.gt(0))
    return drought_flag, drought_year_mean


def build_cwsi_image(cfg: dict, region: ee.Geometry) -> ee.Image:
    """Build return period and drought intensity from saved yearly MAI assets."""
    years = list(range(cfg["start_year"], cfg["end_year"] + 1))
    first_cfg = dict(cfg)
    first_cfg["year"] = years[0]
    first_mai = ee.Image(product_asset_id(first_cfg, "mai"))
    default_proj = first_mai.select("b1").projection()

    drought_flags = []
    drought_year_means = []
    for year in years:
        drought_flag, drought_year_mean = year_drought_and_mean(cfg, year)
        drought_flags.append(drought_flag)
        drought_year_means.append(drought_year_mean)

    drought_count = (
        ee.ImageCollection.fromImages(drought_flags)
        .sum()
        .rename("kharif_drought_year_count")
        .setDefaultProjection(default_proj)
        .float()
    )
    return_period = (
        drought_count.max(1)
        .pow(-1)
        .multiply(len(years))
        .updateMask(drought_count.gt(0))
        .rename("return_period_years")
        .float()
    )
    intensity = (
        ee.ImageCollection.fromImages(drought_year_means)
        .mean()
        .updateMask(drought_count.gt(0))
        .rename("intensity_mai")
        .setDefaultProjection(default_proj)
        .float()
    )

    return (
        ee.Image.cat([return_period, intensity])
        .rename(CWSI_BAND_NAMES)
        .setDefaultProjection(default_proj)
        .clip(region)
        .unmask(cfg["nodata"])
        .float()
        .set("nodata", cfg["nodata"])
    )


def export_cwsi_asset(cfg: dict, region: ee.Geometry) -> dict:
    asset_id = build_output_asset_id(cfg)
    exists = _prepare_asset_target(
        asset_id,
        bool(cfg.get("overwrite_assets", False)),
    )
    print(f"  CWSI asset -> {asset_id}")

    task = None
    if not exists:
        print("\n[cwsi phase 2/2] Reading yearly MAI assets and building CWSI ...")
        image = build_cwsi_image(cfg, region)
        task = _start_asset_export(
            image,
            asset_id,
            description=(
                f"export_cwsi_{_asset_token(cfg['asset_suffix'])}_"
                f"{cfg['start_year']}_to_{cfg['end_year']}"
            ),
        )
    return {"asset_id": asset_id, "task": task, "label": "cwsi"}


def generate_cwsi(cfg: dict, region: ee.Geometry) -> dict:
    cfg = _normalise_cfg(cfg)
    if cfg.get("aez"):
        raise ValueError("CWSI requires saved GEE assets; AEZ Drive export mode is not supported.")

    years = list(range(cfg["start_year"], cfg["end_year"] + 1))
    print("\nCWSI plan")
    print(f"  Years          : {years[0]} to {years[-1]} (N={len(years)})")
    print(f"  Kharif months  : {cfg['kharif_months']}")
    print(f"  MAI threshold  : <= {cfg['drought_mai_threshold']}")
    print(f"  Output         : {build_output_asset_id(cfg)}")

    ensure_yearly_mai_assets(cfg, region, years)
    return export_cwsi_asset(cfg, region)
