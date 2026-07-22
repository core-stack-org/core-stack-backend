#!/usr/bin/env python3
"""AEZ wrapper for config-independent ET downscaling exports."""

import argparse

import ee

try:
    from .et_downscale import APPLICATIONS, generate_et_downscale, init_ee
except ImportError:
    from et_downscale import APPLICATIONS, generate_et_downscale, init_ee


def _asset_name(asset_id: str) -> str:
    return str(asset_id).rstrip("/").split("/")[-1]


def generate_et_aez(
    *,
    aez_asset: str,
    asset_root: str,
    model_aez: str | None = None,
    aez_no: int | None = None,
    aez_code_field: str = "ae_regcode",
    start_year: int = 2017,
    end_year: int | None = None,
    application: str = "aet",
    project_name: str = "",
    asset_suffix: str | None = None,
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
    dry_run: bool = False,
):
    """
    Generate ET application assets for one AEZ, or for a supplied AEZ asset.

    Args:
        aez_asset: GEE FeatureCollection asset containing AEZ geometry.
        asset_root: Parent GEE asset folder for outputs.
        model_aez: RF model asset path. Required when AET must be generated.
        aez_no: Optional AEZ code to filter from ``aez_asset``.
        aez_code_field: Property containing the AEZ code.
        start_year: First crop-year start year.
        end_year: Last crop-year start year. Defaults to ``start_year``.
        application: One of aet/pet/gpp/kc/rwdi/wue/all.
        project_name: Earth Engine project used for initialization.
    """
    if not dry_run:
        init_ee(project_name)
    if dry_run:
        roi = None
    else:
        aez_fc = ee.FeatureCollection(aez_asset)
        roi = aez_fc

    if aez_no is None:
        suffix = asset_suffix or _asset_name(aez_asset)
    else:
        if not dry_run:
            roi = roi.filter(ee.Filter.eq(aez_code_field, int(aez_no)))
        suffix = asset_suffix or f"AEZ_{int(aez_no)}"

    return generate_et_downscale(
        roi=roi,
        tehsil_asset=aez_asset,
        asset_root=asset_root,
        model_aez=model_aez,
        asset_suffix_value=suffix,
        tehsil_name=suffix,
        start_year=start_year,
        end_year=end_year or start_year,
        application=application,
        gee_project=project_name,
        overwrite_assets=overwrite_assets,
        wait_exports=wait_exports,
        poll_seconds=poll_seconds,
        initialize=False,
        dry_run=dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run config-independent ET downscaling for AEZ assets."
    )
    parser.add_argument("--aez-asset", required=True)
    parser.add_argument("--aez-no", type=int, default=None)
    parser.add_argument("--aez-code-field", default="ae_regcode")
    parser.add_argument("--asset-root", required=True)
    parser.add_argument("--model-aez", default=None)
    parser.add_argument("--asset-suffix", default=None)
    parser.add_argument("--start-year", type=int, default=2017)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--application", choices=APPLICATIONS, default="aet")
    parser.add_argument("--gee-project", default="")
    parser.add_argument("--overwrite-assets", action="store_true", default=False)
    parser.add_argument("--no-wait-exports", action="store_true", default=False)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser


def main():
    args = build_parser().parse_args()
    return generate_et_aez(
        aez_asset=args.aez_asset,
        aez_no=args.aez_no,
        aez_code_field=args.aez_code_field,
        asset_root=args.asset_root,
        model_aez=args.model_aez,
        asset_suffix=args.asset_suffix,
        start_year=args.start_year,
        end_year=args.end_year,
        application=args.application,
        project_name=args.gee_project,
        overwrite_assets=args.overwrite_assets,
        wait_exports=not args.no_wait_exports,
        poll_seconds=args.poll_interval_seconds,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
