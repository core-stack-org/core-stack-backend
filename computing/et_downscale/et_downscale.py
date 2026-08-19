"""
ET-Applications - Pan-India ET Downscaling CLI  (GEE Asset Export Edition)
===========================================================================
Each output mode now builds its 13-band image fully inside Earth Engine and
exports it directly to a GEE asset. The monthly calculations are performed
entirely server-side in Earth Engine.

  PIXEL CONSISTENCY GUARANTEE
  ----------------------------
  All exported assets produced by the same tehsil + year combination share
  the same bounding box, CRS, and 30 m pixel grid. The AET (Landsat 8)
  pixel grid drives the spatial reference; MODIS-derived bands are
  resampled to this grid before export. Pixels outside the tehsil boundary
  are written as NoData (-9999).

  Band descriptions and valid-pixel counts are stored as Earth Engine image
  properties on the exported assets.

  Core Layers + Derived Applications
  ----------------------------------
  aet           ->  aet_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual total)
  pet           ->  pet_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual total)
  gpp           ->  gpp_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual mean)
  rwdi          ->  rwdi_<TEHSIL>_<YEAR>               13 bands  (12 monthly + annual mean)
  mai           ->  mai_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual mean)
  wue           ->  wue_<TEHSIL>_<YEAR>                13 bands  (12 monthly + annual mean)
  all           ->  three feature layers + three derived applications

  GPP Method (Light Use Efficiency)
  ----------------------------------
  GPP = PAR x fAPAR x eps
    PAR    = 0.45 x SWdown_f_tavg (GLDAS W/m2 -> MJ/m2/day)
    fAPAR  = max(0, 1.24 x NDVI - 0.168) from Landsat 8
    eps    = eps_max x TMIN_scalar x VPD_scalar
    eps_max, TMIN/VPD thresholds from MOD17 BPLUT keyed on MCD12Q1 land cover

  WUE Formula
  -----------
  WUE = GPP / AET   (g C m-2 day-1) / (mm day-1) = g C / kg H2O
  where AET is the Landsat + GLDAS RF-downscaled actual ET.

  Quick-start
  -----------
  python3 et_fixed.py --tehsil-asset ... --model-aez ... --asset-root ...
  python3 et_fixed.py --application aet ...
  python3 et_fixed.py --application all ...

  Or call main() programmatically with the same parameters.
"""

import ee

from computing.et_downscale.aet import generate_aet
from computing.et_downscale.cwsi import generate_cwsi
from computing.et_downscale.gpp import generate_gpp
from computing.et_downscale.helper import (
    asset_exists,
    product_asset_id,
    wait_for_tasks,
)
from computing.et_downscale.mai import generate_mai
from computing.et_downscale.pet import generate_pet
from computing.et_downscale.rwdi import generate_rwdi
from computing.et_downscale.wue import generate_wue
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from utilities.constants import GEE_PATHS, AEZ
from utilities.gee_utils import (
    ee_initialize,
    get_gee_dir_path,
    valid_gee_text,
    sync_raster_to_gcs,
    check_task_status,
    sync_raster_gcs_to_geoserver,
    is_gee_asset_exists,
    make_asset_public,
    gcs_file_exists,
)
from nrm_app.celery import app


APPLICATION_DEPENDENCIES = {
    "mai": ("aet", "pet"),
    "kc": ("aet", "pet"),
    "rwdi": ("mai",),
    "wue": ("gpp", "aet"),
}


@app.task(bind=True)
def generate_et_downscale(
    self,
    state: str | None = None,
    district: str | None = None,
    tehsil: str | None = None,
    roi: str | None = None,
    asset_suffix=None,
    asset_folder_list=None,
    start_year: int = 2017,
    end_year: int = 2024,
    gee_account_id: int = 1,
    application: str = "all",
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
    app_type: str = "MWS",
    aez=None,
    asset_root=None,
    model_aez: str | None = None,
):
    application = (application or "all").lower()
    start_year = int(start_year)
    end_year = int(end_year if end_year is not None else start_year)

    if not aez:
        ee_initialize(account_id=gee_account_id)
    if isinstance(roi, str):
        roi = ee.FeatureCollection(roi)

    if state and district and tehsil:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(tehsil.lower())
        )
        asset_folder_list = [state, district, tehsil]

        roi_path = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(tehsil.lower())
            + "_uid"
        )

        roi = ee.FeatureCollection(roi_path)

    model_aez = model_aez or get_model_aez(roi, aez)

    if not asset_root and not aez:
        asset_root = get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )

    should_sync_backend = bool(state and district and tehsil and not aez)
    specs_list = []
    roi_path = f"{asset_root}/{asset_suffix}"

    if application == "cwsi":
        cfg = _build_cfg(
            aez=str(aez) if aez else None,
            roi_path=roi_path,
            model_aez=model_aez,
            asset_root=asset_root,
            year=start_year,
            start_year=start_year,
            end_year=end_year,
            district_name=district,
            asset_suffix=asset_suffix,
            application=application,
            overwrite_assets=overwrite_assets,
            wait_exports=wait_exports,
            poll_seconds=poll_seconds,
        )

        print("\n" + "=" * 68)
        print("  ET-Applications  (CWSI Backend Asset Export)")
        print("=" * 68)
        for label, key in [
            ("Asset Suffix", "asset_suffix"),
            ("Start year", "start_year"),
            ("End year", "end_year"),
            ("Output mode", "application"),
            ("Asset root", "asset_root"),
            ("Overwrite assets", "overwrite_assets"),
            ("Model (AEZ)", "model_aez"),
            ("ROI path", "roi_path"),
            ("Wait for exports", "wait_exports"),
            ("Poll interval (s)", "poll_seconds"),
        ]:
            print(f"  {label:<22}: {cfg.get(key, 'N/A')}")
        print("=" * 68 + "\n")

        region = roi.geometry()
        spec = _generate_application(application, cfg, region)
        specs_list.append(spec)
        if cfg.get("wait_exports", True):
            wait_for_tasks(specs_list, cfg.get("poll_seconds", 30), fail_on_error=True)
        if should_sync_backend:
            sync_to_db_and_geoserver(state, district, tehsil, specs_list)
        print(f"\nDone. Export asset: {spec['asset_id']}")
        return None

    for year in range(start_year, end_year + 1):
        cfg = _build_cfg(
            aez=str(aez) if aez else None,
            roi_path=roi_path,
            model_aez=model_aez,
            asset_root=asset_root,
            year=year,
            start_year=start_year,
            end_year=end_year,
            district_name=district,
            asset_suffix=asset_suffix,
            application=application,
            overwrite_assets=overwrite_assets,
            wait_exports=wait_exports,
            poll_seconds=poll_seconds,
        )

        app = cfg["application"]

        print("\n" + "=" * 68)
        print("  ET-Applications  (GEE Asset Export Edition - v3.0 with GPP/WUE/MAI)")
        print("=" * 68)
        for label, key in [
            ("Asset Suffix", "asset_suffix"),
            ("Year", "year"),
            ("Output mode", "application"),
            ("Asset root", "asset_root"),
            ("Overwrite assets", "overwrite_assets"),
            ("Model (AEZ)", "model_aez"),
            ("ROI path", "roi_path"),
            ("Wait for exports", "wait_exports"),
            ("Poll interval (s)", "poll_seconds"),
            ("MODIS collection", "modis_collection"),
        ]:
            print(f"  {label:<22}: {cfg.get(key, 'N/A')}")
        print("=" * 68 + "\n")

        region = roi.geometry()

        if app == "all":
            result = run_all(state, district, tehsil, cfg, region)
            print("\nDone. Export assets:")
            for label, asset_id in result.items():
                print(f"  {label:<5} -> {asset_id}")
        else:
            ensure_single_run_dependencies(app, cfg, region)
            specs = _generate_application(app, cfg, region)
            print(f"\nDone. Export asset: {specs}")
            specs_list.append(specs)

    if should_sync_backend and len(specs_list) > 0:
        wait_for_tasks(specs_list)
        sync_to_db_and_geoserver(state, district, tehsil, specs_list)

    return None


def _build_cfg(
    *,
    aez: str,
    roi_path: str,
    model_aez: str,
    asset_root: str,
    year: int = 2022,
    start_year: int | None = None,
    end_year: int | None = None,
    district_name: str | None = None,
    asset_suffix: str | None = None,
    application: str = "all",
    overwrite_assets: bool = False,
    wait_exports: bool = True,
    poll_seconds: int = 30,
) -> dict:
    return {
        "aez": aez,
        "roi_path": roi_path,
        "model_aez": model_aez,
        "asset_root": asset_root,
        "year": int(year),
        "start_year": int(start_year if start_year is not None else year),
        "end_year": int(end_year if end_year is not None else year),
        "district_name": district_name,
        "asset_suffix": asset_suffix or "",
        "application": application,
        "overwrite_assets": overwrite_assets,
        "wait_exports": wait_exports,
        "poll_seconds": poll_seconds,
    }


def _generate_application(app: str, cfg: dict, region: ee.Geometry) -> dict:
    dispatch = {
        "aet": lambda: generate_aet(cfg, region),
        "pet": lambda: generate_pet(cfg, region),
        "rwdi": lambda: generate_rwdi(cfg, region),
        "mai": lambda: generate_mai(cfg, region),
        "kc": lambda: generate_mai(cfg, region),
        "gpp": lambda: generate_gpp(cfg, region),
        "wue": lambda: generate_wue(cfg, region),
        "cwsi": lambda: generate_cwsi(cfg, region),
    }
    return dispatch[app]()


def ensure_product_asset(label: str, cfg: dict, region: ee.Geometry) -> str:
    asset_id = product_asset_id(cfg, "mai" if label == "kc" else label)
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
    wait_for_tasks([spec], cfg.get("poll_seconds", 30), fail_on_error=True)
    return asset_id


def ensure_single_run_dependencies(app: str, cfg: dict, region: ee.Geometry) -> None:
    dependencies = APPLICATION_DEPENDENCIES.get(app, ())
    if not dependencies:
        return

    print(f"\n[input] Checking exported dependencies for {app.upper()} ...")
    for label in dependencies:
        ensure_product_asset(label, cfg, region)


def get_model_aez(roi, aez=None):
    if not aez:
        aez = (
            ee.FeatureCollection(AEZ)
            .filterBounds(roi.geometry())
            .first()
            .get("ae_regcode")
            .getInfo()
        )
    if aez == 1:
        raise RuntimeError("No model for AEZ 1")

    return f"projects/corestack-datasets-beta/assets/models_downscaling_et/rf_aez{aez}_final"


def run_all(
    state: str, district: str, tehsil: str, cfg: dict, region: ee.Geometry
) -> dict:
    """
    Export core assets first, then build derived assets from saved assets.

    Export sequencing guarantee:
      1. Start AET, PET, and GPP export tasks.
      2. Read completed AET/PET assets and export MAI.
      3. Read completed MAI/GPP/AET assets and export RWDI/WUE.
    """

    print(f"\n{'=' * 60}")
    print("  [all] Asset-first ET applications ...")
    print(f"{'=' * 60}")

    results = {}
    should_sync_backend = bool(state and district and tehsil)

    print("\n  [phase 1/3] Exporting independent AET, PET, and GPP assets ...")
    core_task_specs = []
    for label in ("aet", "pet", "gpp"):
        spec = _generate_application(label, cfg, region)
        core_task_specs.append(spec)
        results[label] = spec["asset_id"]
    wait_for_tasks(core_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True)
    if should_sync_backend:
        sync_to_db_and_geoserver(state, district, tehsil, core_task_specs)

    print("\n  [phase 2/3] Reading AET/PET assets and exporting MAI ...")
    mai_specs = _generate_application("mai", cfg, region)
    results["mai"] = mai_specs["asset_id"]
    wait_for_tasks([mai_specs], cfg.get("poll_seconds", 30), fail_on_error=True)
    if should_sync_backend:
        sync_to_db_and_geoserver(state, district, tehsil, [mai_specs])

    print("\n  [phase 3/3] Exporting RWDI and WUE from saved assets ...")
    rwdi_specs = _generate_application("rwdi", cfg, region)
    wue_specs = _generate_application("wue", cfg, region)
    results["rwdi"] = rwdi_specs["asset_id"]
    results["wue"] = wue_specs["asset_id"]
    final_specs = [rwdi_specs, wue_specs]

    if cfg.get("wait_exports", True):
        wait_for_tasks(
            final_specs, cfg.get("poll_seconds", 30), fail_on_error=True
        )
        if should_sync_backend:
            sync_to_db_and_geoserver(state, district, tehsil, final_specs)
    else:
        print(
            "\n[exports] Final export tasks started. Completion polling skipped (wait_exports=false)."
        )
    return results


def sync_to_db_and_geoserver(state, district, tehsil, specs_list):
    gcs_tasks = []
    layer_ids = []
    for layer in specs_list:
        asset_id = layer["asset_id"]
        layer_name = asset_id.split("/")[-1]

        if is_gee_asset_exists(asset_id):
            layer_id = save_layer_info_to_db(
                state,
                district,
                tehsil,
                layer_name=layer_name,
                asset_id=asset_id,
                dataset_name="ET Downscale",
            )
            layer_ids.append(layer_id)

            make_asset_public(asset_id)

            """Sync image to google cloud storage and then to geoserver"""
            image = ee.Image(asset_id)
            if not gcs_file_exists(layer_name):
                task_id = sync_raster_to_gcs(image, 30, layer_name)
                gcs_tasks.append(task_id)

    task_id_list = check_task_status(gcs_tasks)
    print("task_id_list sync to gcs ", task_id_list)

    for i in range(0, len(specs_list)):
        layer = specs_list[i]
        asset_id = layer["asset_id"]
        layer_name = asset_id.split("/")[-1]
        res = sync_raster_gcs_to_geoserver("ET", layer_name, layer_name)

        if res and layer_ids[i]:
            print("layer_ids[i]", layer_ids[i])
            update_layer_sync_status(layer_id=layer_ids[i], sync_to_geoserver=True)
            print("sync to geoserver flag updated")
