#!/usr/bin/env python3
"""Kharif drought return period and intensity from N yearly MAI assets."""

import argparse

import ee

try:
    from .helper import (
        DEFAULT_CROP_YEAR_START_MONTH,
        NODATA,
        _apply_image_properties,
        _asset_token,
        _prepare_asset_target,
        _start_asset_export,
        asset_suffix,
        crop_month_index,
        crop_year_start_month,
        load_product_monthly_stack,
        product_asset_id,
        wait_for_tasks,
    )
except ImportError:
    from helper import (
        DEFAULT_CROP_YEAR_START_MONTH,
        NODATA,
        _apply_image_properties,
        _asset_token,
        _prepare_asset_target,
        _start_asset_export,
        asset_suffix,
        crop_month_index,
        crop_year_start_month,
        load_product_monthly_stack,
        product_asset_id,
        wait_for_tasks,
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


def init_ee(project: str = "") -> None:
    try:
        ee.Initialize(project=project) if project else ee.Initialize()
        print("[EE] Initialised.")
    except Exception:
        print("[EE] Running authenticate ...")
        ee.Authenticate()
        ee.Initialize(project=project) if project else ee.Initialize()


def cfg_from_args(args: argparse.Namespace) -> dict:
    tehsil_asset = args.tehsil_asset or args.roi_asset
    return {
        "gee_project": args.gee_project or "",
        "tehsil_asset": tehsil_asset,
        "model_aez": args.model_aez or "",
        "tehsil_name": args.tehsil_name or "",
        "asset_root": args.asset_root,
        "asset_suffix": args.asset_suffix or "",
        "crop_year_start_month": args.crop_year_start_month,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "kharif_months": csv_ints(args.kharif_months or DEFAULT_KHARIF_MONTHS),
        "drought_mai_threshold": args.drought_mai_threshold,
        "output_label": args.output_label,
        "nodata": args.nodata,
        "overwrite_assets": args.overwrite_assets,
        "wait_exports": not args.no_wait_exports,
        "poll_seconds": args.poll_interval_seconds,
    }


def validate_config(cfg: dict, parser: argparse.ArgumentParser) -> None:
    if not cfg.get("tehsil_asset"):
        parser.error("--tehsil-asset or --roi-asset is required")
    if not cfg.get("asset_root"):
        parser.error("--asset-root is required")
    if cfg.get("start_year") is None or cfg.get("end_year") is None:
        parser.error("--start-year and --end-year are required")

    cfg["start_year"] = int(cfg["start_year"])
    cfg["end_year"] = int(cfg["end_year"])
    if cfg["end_year"] < cfg["start_year"]:
        parser.error("end_year must be greater than or equal to start_year")

    months = csv_ints(cfg.get("kharif_months", DEFAULT_KHARIF_MONTHS))
    if not months or any(month < 1 or month > 12 for month in months):
        parser.error("kharif_months must contain calendar months from 1 to 12")
    cfg["kharif_months"] = months
    cfg["drought_mai_threshold"] = float(
        cfg.get("drought_mai_threshold", DEFAULT_DROUGHT_MAI_THRESHOLD)
    )
    cfg["nodata"] = float(cfg.get("nodata", NODATA))

    try:
        cfg["crop_year_start_month"] = crop_year_start_month(cfg)
    except ValueError as error:
        parser.error(str(error))

    if not cfg.get("tehsil_name"):
        cfg["tehsil_name"] = cfg["tehsil_asset"].rstrip("/").split("/")[-1]
    if not cfg.get("asset_suffix"):
        cfg["asset_suffix"] = cfg["tehsil_name"]
    if not cfg.get("output_label"):
        cfg["output_label"] = "cwsi"


def build_output_asset_id(cfg: dict) -> str:
    root = str(cfg["asset_root"]).rstrip("/")
    label = _asset_token(cfg.get("output_label", "cwsi"))
    return (
        f"{root}/{label}_{asset_suffix(cfg)}_"
        f"{int(cfg['start_year'])}_to_{int(cfg['end_year'])}"
    )


def ensure_yearly_mai_assets(
    cfg: dict,
    region: ee.Geometry,
    years: list[int],
) -> None:
    """Create every missing yearly MAI asset and wait until it is saved."""
    try:
        from .et_downscale import ensure_product_asset
    except ImportError:
        from et_downscale import ensure_product_asset

    print("\n[phase 1/2] Creating/checking yearly MAI assets ...")
    for year in years:
        year_cfg = dict(cfg)
        year_cfg["year"] = int(year)
        year_cfg["overwrite_assets"] = False
        ensure_product_asset("mai", year_cfg, region)


def select_kharif_mai_images(
    cfg: dict,
    year: int,
) -> list[ee.Image]:
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


def year_drought_and_mean(
    cfg: dict,
    year: int,
) -> tuple[ee.Image, ee.Image]:
    """Return the drought flag and mean valid Kharif MAI for one year."""
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
    years = list(range(int(cfg["start_year"]), int(cfg["end_year"]) + 1))
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

    image = (
        ee.Image.cat([return_period, intensity])
        .rename(CWSI_BAND_NAMES)
        .setDefaultProjection(default_proj)
        .clip(region)
        .unmask(cfg["nodata"])
        .float()
    )
    mai_assets = []
    for year in years:
        year_cfg = dict(cfg)
        year_cfg["year"] = year
        mai_assets.append(product_asset_id(year_cfg, "mai"))

    properties = {
        "application": "cwsi",
        "description": "Kharif drought return period and mean drought-year MAI",
        "asset_suffix": asset_suffix(cfg),
        "start_year": str(years[0]),
        "end_year": str(years[-1]),
        "n_years": str(len(years)),
        "kharif_months": ",".join(str(month) for month in cfg["kharif_months"]),
        "drought_mai_threshold": str(cfg["drought_mai_threshold"]),
        "mai_assets": ",".join(mai_assets),
        "drought_year_rule": (
            "A year is a drought year when any valid Kharif month has MAI <= "
            "drought_mai_threshold; a year is usable when at least one Kharif "
            "month is valid."
        ),
        "frequency_formula": "return_period_years = n_years / drought_year_count",
        "intensity_formula": (
            "Mean valid Kharif MAI within each drought year, then mean across "
            "drought years."
        ),
        "valid_data_rule": (
            "NoData months are ignored. Pixels with zero drought years are NoData."
        ),
        "interpolation": "none",
        "nodata": cfg["nodata"],
        "band_1_description": "Kharif drought return period in years",
        "band_2_description": "Mean Kharif MAI intensity across drought years",
    }
    if cfg.get("tehsil_name"):
        properties["tehsil"] = cfg["tehsil_name"]
    if cfg.get("tehsil_asset"):
        properties["tehsil_asset"] = cfg["tehsil_asset"]
    return _apply_image_properties(image, properties)


def export_cwsi_asset(cfg: dict, region: ee.Geometry) -> dict:
    asset_id = build_output_asset_id(cfg)
    exists = _prepare_asset_target(
        asset_id,
        bool(cfg.get("overwrite_assets", False)),
    )
    print(f"  CWSI asset -> {asset_id}")

    task = None
    if not exists:
        print("\n[phase 2/2] Reading yearly MAI assets and building CWSI ...")
        image = build_cwsi_image(cfg, region)
        task = _start_asset_export(
            image,
            asset_id,
            description=(
                f"export_cwsi_{asset_suffix(cfg)}_"
                f"{cfg['start_year']}_to_{cfg['end_year']}"
            ),
        )
    return {"asset_id": asset_id, "task": task, "label": "cwsi"}


def print_plan(cfg: dict) -> None:
    years = list(range(cfg["start_year"], cfg["end_year"] + 1))
    print("\nCWSI plan")
    print(f"  Years          : {years[0]} to {years[-1]} (N={len(years)})")
    print(f"  Kharif months  : {cfg['kharif_months']}")
    print(f"  MAI threshold  : <= {cfg['drought_mai_threshold']}")
    print("  MAI inputs:")
    for year in years:
        year_cfg = dict(cfg)
        year_cfg["year"] = year
        print(f"    {year}: {product_asset_id(year_cfg, 'mai')}")
    print(f"  Output         : {build_output_asset_id(cfg)}")


def generate_cwsi(
    *,
    tehsil_asset: str,
    asset_root: str,
    start_year: int,
    end_year: int,
    model_aez: str | None = None,
    tehsil_name: str | None = None,
    asset_suffix_value: str | None = None,
    crop_year_start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
    kharif_months=None,
    drought_mai_threshold: float = DEFAULT_DROUGHT_MAI_THRESHOLD,
    output_label: str = "cwsi",
    nodata: float = NODATA,
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
    gee_project: str | None = None,
    initialize: bool = True,
) -> str:
    cfg = {
        "gee_project": gee_project or "",
        "tehsil_asset": tehsil_asset,
        "model_aez": model_aez or "",
        "tehsil_name": tehsil_name or "",
        "asset_root": asset_root,
        "asset_suffix": asset_suffix_value or "",
        "crop_year_start_month": crop_year_start_month,
        "start_year": start_year,
        "end_year": end_year,
        "kharif_months": csv_ints(kharif_months or DEFAULT_KHARIF_MONTHS),
        "drought_mai_threshold": drought_mai_threshold,
        "output_label": output_label,
        "nodata": nodata,
        "overwrite_assets": overwrite_assets,
        "wait_exports": wait_exports,
        "poll_seconds": poll_seconds,
    }
    parser = build_parser()
    validate_config(cfg, parser)
    print_plan(cfg)
    if initialize:
        init_ee(cfg.get("gee_project", ""))
    region = ee.FeatureCollection(cfg["tehsil_asset"]).geometry()
    years = list(range(cfg["start_year"], cfg["end_year"] + 1))
    ensure_yearly_mai_assets(cfg, region, years)
    spec = export_cwsi_asset(cfg, region)
    if cfg.get("wait_exports", True):
        wait_for_tasks([spec], cfg.get("poll_seconds", 30), fail_on_error=True)
    return spec["asset_id"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Kharif drought return period and intensity from yearly MAI assets."
    )
    parser.add_argument("--gee-project", default=None)
    parser.add_argument("--tehsil-asset", default=None)
    parser.add_argument("--roi-asset", default=None)
    parser.add_argument("--model-aez", default=None)
    parser.add_argument("--tehsil-name", default=None)
    parser.add_argument("--asset-root", default=None)
    parser.add_argument("--asset-suffix", default=None)
    parser.add_argument(
        "--crop-year-start-month",
        type=int,
        default=DEFAULT_CROP_YEAR_START_MONTH,
    )
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--kharif-months", default=None, metavar="7,8,9,10")
    parser.add_argument(
        "--drought-mai-threshold",
        type=float,
        default=DEFAULT_DROUGHT_MAI_THRESHOLD,
    )
    parser.add_argument("--output-label", default="cwsi")
    parser.add_argument("--nodata", type=float, default=NODATA)
    parser.add_argument("--overwrite-assets", action="store_true", default=False)
    parser.add_argument("--no-wait-exports", action="store_true", default=False)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    cfg = cfg_from_args(args)
    validate_config(cfg, parser)
    print_plan(cfg)

    if args.dry_run:
        return build_output_asset_id(cfg)

    init_ee(cfg.get("gee_project", ""))
    region = ee.FeatureCollection(cfg["tehsil_asset"]).geometry()
    years = list(range(cfg["start_year"], cfg["end_year"] + 1))

    ensure_yearly_mai_assets(cfg, region, years)
    spec = export_cwsi_asset(cfg, region)
    if cfg.get("wait_exports", True):
        wait_for_tasks(
            [spec],
            cfg.get("poll_seconds", 30),
            fail_on_error=True,
        )
    print(f"\nDone. Export asset: {spec['asset_id']}")
    return spec["asset_id"]


if __name__ == "__main__":
    main()
