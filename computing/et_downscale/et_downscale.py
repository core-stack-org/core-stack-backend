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
  kc            ->  kc_<TEHSIL>_<YEAR>                 13 bands  (12 monthly + annual mean)
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
from computing.et_downscale.gpp import generate_gpp
from computing.et_downscale.helper import (
    wait_for_tasks,
)
from computing.et_downscale.kc import generate_kc
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
):
    if not aez:
        ee_initialize(account_id=gee_account_id)

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

    model_aez = get_model_aez(roi, aez)

    if not asset_root and not aez:
        asset_root = get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )

    specs_list = []
    roi_path = f"{asset_root}/{asset_suffix}"
    for year in range(start_year, end_year + 1):
        cfg = _build_cfg(
            aez=str(aez),
            roi_path=roi_path,
            model_aez=model_aez,
            asset_root=asset_root,
            year=year,
            district_name=district,
            asset_suffix=asset_suffix,
            application=application,
            overwrite_assets=overwrite_assets,
            wait_exports=wait_exports,
            poll_seconds=poll_seconds,
        )

        app = cfg["application"]

        print("\n" + "=" * 68)
        print("  ET-Applications  (GEE Asset Export Edition - v3.0 with GPP/WUE/Kc)")
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

        dispatch = {
            "aet": lambda: generate_aet(cfg, region),
            "pet": lambda: generate_pet(cfg, region),
            "rwdi": lambda: generate_rwdi(cfg, region),
            "kc": lambda: generate_kc(cfg, region),
            "gpp": lambda: generate_gpp(cfg, region),
            "wue": lambda: generate_wue(cfg, region),
            "all": lambda: run_all(state, district, tehsil, cfg, region),
        }

        result = dispatch[app]()
        if app == "all":
            print("\nDone. Export assets:")
            for label, asset_id in result.items():
                print(f"  {label:<5} -> {asset_id}")
        else:
            print(f"\nDone. Export asset: {result}")
            specs = result[-1]
            specs_list.append(specs)

    if not aez and len(specs_list) > 0:
        wait_for_tasks(specs_list)
        sync_to_db_and_geoserver(state, district, tehsil, specs_list)

    return None  # TODO return something for sync run


def _build_cfg(
    *,
    aez: str,
    roi_path: str,
    model_aez: str,
    asset_root: str,
    year: int = 2022,
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
        "district_name": district_name,
        "asset_suffix": asset_suffix or "",
        "application": application,
        "overwrite_assets": overwrite_assets,
        "wait_exports": wait_exports,
        "poll_seconds": poll_seconds,
    }


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
    Run the three core layers first, wait for them to finish exporting,
    then export the three derived applications.

    Export sequencing guarantee:
      1. Start AET, PET, and GPP export tasks.
      2. Wait until all three core exports reach a terminal state.
      3. Only then start RWDI, KC, and WUE export tasks.

    Pixel consistency guarantee:
      All outputs are derived from the same AET/PET/GPP stacks.
      They are therefore spatially identical - same CRS, transform, and
      pixel grid. No post-processing alignment is needed.
    """

    print(f"\n{'=' * 60}")
    print("  [all] Building shared GEE stacks ...")
    print(f"{'=' * 60}")

    core_task_specs = []
    derived_task_specs = []
    results = {}

    print("\n  [1/3] Building AET stack (Landsat 8 + GLDAS -> RF model) ...")
    aet_stack, common_mask, footprint, grid_proj, aet_specs = generate_aet(cfg, region)
    results["aet"] = aet_specs["asset_id"]
    core_task_specs.append(aet_specs)

    print("\n  [2/3] Building PET stack (MODIS MOD16A2) ...")
    pet_stack, proj, pet_specs = generate_pet(
        cfg=cfg,
        region=region,
        common_mask=common_mask,
        footprint=footprint,
        grid_proj=grid_proj,
    )
    results["pet"] = pet_specs["asset_id"]
    core_task_specs.append(pet_specs)

    print("\n  [3/3] Building GPP stack (LUE: GLDAS + Landsat NDVI + MCD12Q1) ...")
    gpp_stack, gpp_specs = generate_gpp(
        cfg=cfg,
        region=region,
        common_mask=common_mask,
        footprint=footprint,
        grid_proj=grid_proj,
        proj=proj,
    )

    results["gpp"] = gpp_specs["asset_id"]
    core_task_specs.append(gpp_specs)

    print(
        "\n  [phase 1/2] Waiting for core exports (AET, PET, GPP) before derived exports ..."
    )
    wait_for_tasks(core_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True)
    sync_to_db_and_geoserver(state, district, tehsil, core_task_specs)

    print(
        "\n  [phase 2/2] Starting derived exports (RWDI, KC, WUE) from the shared core stacks ..."
    )

    rwdi_specs = generate_rwdi(
        cfg,
        region,
        footprint=footprint,
        common_mask=common_mask,
        grid_proj=grid_proj,
        aet_stack=aet_stack,
        pet_stack=pet_stack,
    )
    derived_task_specs.append(rwdi_specs)

    kc_specs = generate_kc(
        cfg,
        region,
        footprint=footprint,
        common_mask=common_mask,
        grid_proj=grid_proj,
        aet_stack=aet_stack,
        pet_stack=pet_stack,
    )
    derived_task_specs.append(kc_specs)

    wue_specs = generate_wue(
        cfg,
        region,
        footprint=footprint,
        common_mask=common_mask,
        grid_proj=grid_proj,
        aet_stack=aet_stack,
        gpp_stack=gpp_stack,
    )
    derived_task_specs.append(wue_specs)

    if cfg.get("wait_exports", True):
        wait_for_tasks(
            derived_task_specs, cfg.get("poll_seconds", 30), fail_on_error=True
        )

        sync_to_db_and_geoserver(state, district, tehsil, derived_task_specs)
    else:
        print(
            "\n[exports] Derived export tasks started. Final completion polling skipped (wait_exports=false)."
        )
    return results


def sync_to_geoserver(asset_id, layer_name, workspace, scale=30):
    """Sync image to google cloud storage and then to geoserver"""
    image = ee.Image(asset_id)
    if not gcs_file_exists(layer_name):
        task_id = sync_raster_to_gcs(image, scale, layer_name)

        task_id_list = check_task_status([task_id])
        print("task_id_list sync to gcs ", task_id_list)

    res = sync_raster_gcs_to_geoserver(workspace, layer_name, layer_name)
    return res


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
