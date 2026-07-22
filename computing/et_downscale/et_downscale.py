#!/usr/bin/env python3
"""
ET downscaling applications - GEE asset export CLI.

The exported monthly bands follow crop-year order:

    b1..b12 = Jul, Aug, Sep, Oct, Nov, Dec, Jan, Feb, Mar, Apr, May, Jun
    b13     = crop-year annual total or mean

This module is config-independent. Pass all asset paths and run parameters as
CLI arguments, or call ``generate_et_downscale`` with the same values.
"""

import argparse

import ee

try:
    from .aet import generate_aet
    from .gpp import generate_gpp
    from .helper import (
        DEFAULT_CROP_YEAR_START_MONTH,
        MODIS_COL,
        asset_exists,
        asset_suffix,
        crop_month_order,
        product_asset_id,
        wait_for_tasks,
    )
    from .mai import generate_mai
    from .pet import generate_pet
    from .rwdi import generate_rwdi
    from .wue import generate_wue
except ImportError:
    from aet import generate_aet
    from gpp import generate_gpp
    from helper import (
        DEFAULT_CROP_YEAR_START_MONTH,
        MODIS_COL,
        asset_exists,
        asset_suffix,
        crop_month_order,
        product_asset_id,
        wait_for_tasks,
    )
    from mai import generate_mai
    from pet import generate_pet
    from rwdi import generate_rwdi
    from wue import generate_wue


APPLICATIONS = ("all", "aet", "pet", "rwdi", "mai", "gpp", "wue")
APPLICATION_DEPENDENCIES = {
    "mai": ("aet", "pet"),
    "rwdi": ("mai",),
    "wue": ("aet", "gpp"),
}


def init_ee(project: str = "") -> None:
    try:
        ee.Initialize(project=project) if project else ee.Initialize()
        print("[EE] Initialised.")
    except Exception:
        print("[EE] Running authenticate ...")
        ee.Authenticate()
        ee.Initialize(project=project) if project else ee.Initialize()


def build_cfg(
    *,
    tehsil_asset: str,
    asset_root: str,
    model_aez: str | None = None,
    year: int = 2022,
    crop_year_start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
    asset_suffix_value: str | None = None,
    tehsil_name: str | None = None,
    application: str = "all",
    gee_project: str | None = None,
    modis_collection: str = MODIS_COL,
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
    dry_run: bool = False,
) -> dict:
    return {
        "tehsil_asset": tehsil_asset,
        "model_aez": model_aez or "",
        "year": int(year),
        "crop_year_start_month": int(crop_year_start_month),
        "asset_root": asset_root,
        "asset_suffix": asset_suffix_value or tehsil_name or "",
        "gee_project": gee_project or "",
        "tehsil_name": tehsil_name or "",
        "application": application,
        "modis_collection": modis_collection,
        "overwrite_assets": bool(overwrite_assets),
        "wait_exports": bool(wait_exports),
        "poll_seconds": int(poll_seconds),
        "dry_run": bool(dry_run),
    }


def cfg_from_args(args: argparse.Namespace) -> dict:
    tehsil_asset = args.tehsil_asset or args.roi_asset
    return build_cfg(
        tehsil_asset=tehsil_asset,
        model_aez=args.model_aez,
        year=args.year,
        crop_year_start_month=args.crop_year_start_month,
        asset_root=args.asset_root,
        asset_suffix_value=args.asset_suffix,
        gee_project=args.gee_project,
        tehsil_name=args.tehsil_name,
        application=args.application,
        modis_collection=args.modis_collection,
        overwrite_assets=args.overwrite_assets,
        wait_exports=not args.no_wait_exports,
        poll_seconds=args.poll_interval_seconds,
        dry_run=args.dry_run,
    )


def _validate_cfg(cfg: dict) -> None:
    if not cfg.get("tehsil_asset"):
        raise ValueError("--tehsil-asset or --roi-asset is required")
    if not cfg.get("asset_root"):
        raise ValueError("--asset-root is required")
    if not cfg.get("tehsil_name"):
        cfg["tehsil_name"] = cfg["tehsil_asset"].rstrip("/").split("/")[-1].upper()
    if not cfg.get("asset_suffix"):
        cfg["asset_suffix"] = cfg["tehsil_name"]

    start_month = int(cfg.get("crop_year_start_month", DEFAULT_CROP_YEAR_START_MONTH))
    if start_month < 1 or start_month > 12:
        raise ValueError("crop_year_start_month must be between 1 and 12")
    cfg["crop_year_start_month"] = start_month

    app = cfg.get("application", "all")
    if app not in APPLICATIONS:
        raise ValueError(f"application must be one of: {', '.join(APPLICATIONS)}")
    if app in {"aet", "all"} and not cfg.get("model_aez"):
        raise ValueError("--model-aez is required to generate AET")


def validate_config(cfg: dict, parser: argparse.ArgumentParser) -> None:
    try:
        _validate_cfg(cfg)
    except ValueError as error:
        parser.error(str(error))


def print_run_config(cfg: dict) -> None:
    print("\n" + "=" * 68)
    print("  ET Downscale  (Config-independent GEE Asset Export)")
    print("=" * 68)
    for label, key in [
        ("Tehsil/ROI", "tehsil_name"),
        ("Asset suffix", None),
        ("Crop year", "year"),
        ("Crop-year start month", "crop_year_start_month"),
        ("Crop-year month order", "month_order"),
        ("Output mode", "application"),
        ("Asset root", "asset_root"),
        ("Overwrite assets", "overwrite_assets"),
        ("GEE project", "gee_project"),
        ("Model (AEZ)", "model_aez"),
        ("Tehsil/ROI asset", "tehsil_asset"),
        ("Wait for exports", "wait_exports"),
        ("Poll interval (s)", "poll_seconds"),
        ("MODIS collection", "modis_collection"),
        ("Dry run", "dry_run"),
    ]:
        if key is None:
            value = asset_suffix(cfg)
        elif key == "month_order":
            value = crop_month_order(cfg["crop_year_start_month"])
        else:
            value = cfg.get(key, "N/A")
        print(f"  {label:<24}: {value}")
    print("=" * 68 + "\n")


def _generate_application(app: str, cfg: dict, region: ee.Geometry) -> dict:
    """Submit one independent product export and return its export spec."""
    if app == "aet":
        if not cfg.get("model_aez"):
            raise ValueError("model_aez is required because the AET asset is missing")
        return generate_aet(cfg, region)
    if app == "pet":
        return generate_pet(cfg, region)
    if app == "gpp":
        return generate_gpp(cfg, region)
    if app == "mai":
        return generate_mai(cfg, region)
    if app == "rwdi":
        return generate_rwdi(cfg, region)
    if app == "wue":
        return generate_wue(cfg, region)
    raise ValueError(f"Unsupported application: {app}")


def ensure_product_asset(label: str, cfg: dict, region: ee.Geometry) -> str:
    """Recursively create a missing dependency asset, then return its asset ID."""
    asset_id = product_asset_id(cfg, label)
    if asset_exists(asset_id):
        print(f"  OK      {label.upper():<4}: {asset_id}")
        return asset_id

    for dependency in APPLICATION_DEPENDENCIES.get(label, ()):
        ensure_product_asset(dependency, cfg, region)

    print(f"  MISSING {label.upper():<4}: {asset_id}")
    print(f"          Submitting {label.upper()} export ...")
    dep_cfg = dict(cfg)
    dep_cfg["overwrite_assets"] = False
    spec = _generate_application(label, dep_cfg, region)
    wait_for_tasks(
        [spec],
        cfg.get("poll_seconds", 30),
        fail_on_error=True,
    )
    return asset_id


def ensure_single_run_dependencies(app: str, cfg: dict, region: ee.Geometry) -> None:
    dependencies = APPLICATION_DEPENDENCIES.get(app, ())
    if not dependencies:
        return

    print(f"\n[input] Checking exported dependencies for {app.upper()} ...")
    for label in dependencies:
        ensure_product_asset(label, cfg, region)


def run_single(app: str, cfg: dict, region: ee.Geometry) -> str:
    ensure_single_run_dependencies(app, cfg, region)
    spec = _generate_application(app, cfg, region)
    if cfg.get("wait_exports", True):
        wait_for_tasks([spec], cfg.get("poll_seconds", 30), fail_on_error=True)
    return spec["asset_id"]


def run_all(cfg: dict, region: ee.Geometry) -> dict:
    """Export each product independently, using completed assets as inputs."""
    print(f"\n{'=' * 60}")
    print("  [all] Asset-first ET applications ...")
    print(f"{'=' * 60}")

    results = {}

    print("\n  [phase 1/3] Exporting independent AET, PET, and GPP assets ...")
    core_task_specs = []
    for label in ("aet", "pet", "gpp"):
        spec = _generate_application(label, cfg, region)
        core_task_specs.append(spec)
        results[label] = spec["asset_id"]
    wait_for_tasks(core_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True)

    print("\n  [phase 2/3] Reading AET/PET assets and exporting MAI ...")
    mai_spec = _generate_application("mai", cfg, region)
    results["mai"] = mai_spec["asset_id"]

    # Phase 3 reads MAI, so its export must finish before phase 3 starts.
    wait_for_tasks([mai_spec], cfg.get("poll_seconds", 30), fail_on_error=True)

    print("\n  [phase 3/3] Exporting RWDI and WUE from saved assets ...")
    rwdi_spec = _generate_application("rwdi", cfg, region)
    wue_spec = _generate_application("wue", cfg, region)
    results["rwdi"] = rwdi_spec["asset_id"]
    results["wue"] = wue_spec["asset_id"]

    if cfg.get("wait_exports", True):
        wait_for_tasks(
            [rwdi_spec, wue_spec],
            cfg.get("poll_seconds", 30),
            fail_on_error=True,
        )
    else:
        print("\n[exports] Export tasks started. Completion polling skipped.")
    return results


def run_for_region(cfg: dict, region: ee.Geometry):
    app = cfg.get("application", "all")
    return run_all(cfg, region) if app == "all" else run_single(app, cfg, region)


def planned_asset_ids(cfg: dict) -> dict:
    app = cfg.get("application", "all")
    labels = ("aet", "pet", "gpp", "mai", "rwdi", "wue") if app == "all" else (app,)
    return {label: product_asset_id(cfg, label) for label in labels}


def print_dry_run_plan(cfg: dict) -> dict:
    planned = planned_asset_ids(cfg)
    print("\n[dry-run] No Earth Engine initialization or export tasks will be started.")
    print("[dry-run] Planned output assets:")
    for label, asset_id in planned.items():
        print(f"  {label:<5} -> {asset_id}")
    return planned


def _region_from_roi(roi, tehsil_asset: str):
    if roi is not None:
        if isinstance(roi, str):
            return ee.FeatureCollection(roi)
        return ee.FeatureCollection(roi)
    return ee.FeatureCollection(tehsil_asset)


def generate_et_downscale(
    *,
    tehsil_asset: str | None = None,
    roi=None,
    asset_root: str,
    model_aez: str | None = None,
    asset_suffix_value: str | None = None,
    asset_suffix: str | None = None,
    tehsil_name: str | None = None,
    year: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    application: str = "all",
    gee_project: str | None = None,
    crop_year_start_month: int = DEFAULT_CROP_YEAR_START_MONTH,
    modis_collection: str = MODIS_COL,
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
    initialize: bool = True,
    dry_run: bool = False,
):
    """
    Programmatic config-independent entry point.

    ``roi`` may be a FeatureCollection object or a FeatureCollection asset ID.
    ``tehsil_asset``/``roi`` is used only as the processing region; all export
    paths come from ``asset_root`` and ``asset_suffix``.
    """
    suffix = asset_suffix_value or asset_suffix
    roi_asset = tehsil_asset if tehsil_asset is not None else roi if isinstance(roi, str) else ""
    if initialize and not dry_run:
        init_ee(gee_project or "")

    start = int(year if year is not None else start_year if start_year is not None else 2022)
    end = int(year if year is not None else end_year if end_year is not None else start)
    if end < start:
        raise ValueError("end_year must be greater than or equal to start_year")

    region = None
    if not dry_run:
        roi_fc = _region_from_roi(roi, tehsil_asset or roi_asset)
        region = roi_fc.geometry()

    results = {}
    for run_year in range(start, end + 1):
        cfg = build_cfg(
            tehsil_asset=roi_asset,
            model_aez=model_aez,
            year=run_year,
            crop_year_start_month=crop_year_start_month,
            asset_root=asset_root,
            asset_suffix_value=suffix,
            gee_project=gee_project,
            tehsil_name=tehsil_name,
            application=application,
            modis_collection=modis_collection,
            overwrite_assets=overwrite_assets,
            wait_exports=wait_exports,
            poll_seconds=poll_seconds,
            dry_run=dry_run,
        )
        _validate_cfg(cfg)
        print_run_config(cfg)
        if dry_run:
            results[run_year] = print_dry_run_plan(cfg)
            continue
        results[run_year] = run_for_region(cfg, region)
    return results


def build_parser(default_application: str | None = None):
    if default_application is not None and default_application not in APPLICATIONS:
        raise ValueError(f"Unsupported default application: {default_application}")
    parser = argparse.ArgumentParser(
        prog=default_application or "et_downscale",
        description="ET downscaling applications exported as GEE assets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--tehsil-asset", default=None,
                        help="GEE FeatureCollection asset path for the tehsil")
    parser.add_argument("--roi-asset", default=None,
                        help="GEE FeatureCollection asset path for any ROI/AEZ")
    parser.add_argument("--model-aez", default=None,
                        help="GEE asset path for the RF ensemble model")
    parser.add_argument("--year", type=int, default=2022,
                        help="Crop-year start year")
    parser.add_argument("--crop-year-start-month", type=int,
                        default=DEFAULT_CROP_YEAR_START_MONTH,
                        help="First month of the crop year. Default: 7 (July)")
    parser.add_argument("--asset-root", default=None,
                        help="Parent GEE asset path where exports will be created")
    parser.add_argument("--asset-suffix", default=None,
                        help="Suffix used in output asset names")
    parser.add_argument("--overwrite-assets", action="store_true", default=False,
                        help="Delete an existing target asset before exporting")
    parser.add_argument("--no-wait-exports", action="store_true", default=False,
                        help="Start GEE export tasks and exit without polling")
    parser.add_argument("--poll-interval-seconds", type=int, default=30,
                        help="Delay between export task status checks")
    parser.add_argument("--gee-project", default=None)
    parser.add_argument("--tehsil-name", default=None)
    parser.add_argument("--modis-collection", default=MODIS_COL)
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Validate inputs and print planned assets without running GEE")
    if default_application is None:
        parser.add_argument("--application", default="all", choices=APPLICATIONS,
                            help="Which output mode to run")
    else:
        parser.set_defaults(application=default_application)
    return parser


def main(default_application: str | None = None):
    parser = build_parser(default_application)
    args = parser.parse_args()
    cfg = cfg_from_args(args)
    validate_config(cfg, parser)
    print_run_config(cfg)

    if args.dry_run:
        return print_dry_run_plan(cfg)

    init_ee(cfg.get("gee_project", ""))
    region = ee.FeatureCollection(cfg["tehsil_asset"]).geometry()
    result = run_for_region(cfg, region)

    if cfg.get("application") == "all":
        print("\nDone. Export assets:")
        for label, asset_id in result.items():
            print(f"  {label:<5} -> {asset_id}")
    else:
        print(f"\nDone. Export asset: {result}")


if __name__ == "__main__":
    main()
