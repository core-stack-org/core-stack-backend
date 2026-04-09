from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    LULC_BASE_DIR,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_raster_path,
    clip_raster_with_roi,
    load_precomputed_roi,
    push_local_raster_to_geoserver,
    read_validated_vector_file,
    resolve_lulc_raster_paths,
)
from computing.models import Dataset
from computing.utils import save_layer_info_to_db, update_layer_sync_status


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/lulc/lulc_v3_local"
GEOSERVER_WORKSPACE = "LULC_v3"
GEOSERVER_STYLE = "lulc_level_3_style"
LOCAL_ALGORITHM = "local_lulc_v3_clip"
LOCAL_ALGORITHM_VERSION = "local-1.0"


def _slug(value, fallback):
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if not text:
        return fallback
    return valid_gee_text(text) or fallback


def _resolve_roi(state, district, block, roi_path, precomputed_roi_dir):
    if state and district and block:
        return load_precomputed_roi(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )

    if not roi_path:
        raise ValueError(
            "For non state/district/block runs, `roi_path` must be provided."
        )

    return read_validated_vector_file(
        roi_path,
        f"ROI file has no valid geometries: {roi_path}",
    )


def _resolve_filename_prefix(district, block, asset_suffix):
    if district and block:
        return (
            f"{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}"
        )
    if asset_suffix is None or not str(asset_suffix).strip():
        raise ValueError(
            "For non state/district/block runs, `asset_suffix` must be provided."
        )
    return _slug(asset_suffix, "custom")


def _build_output_stub(filename_prefix, start_year):
    return (
        f"{filename_prefix}_{start_year}-07-01_"
        f"{start_year + 1}-06-30_LULCmap_10m"
    )


def _build_layer_name(start_year, end_year, filename_prefix):
    return f"LULC_{start_year}_{end_year}_{filename_prefix}_level_3"


def _resolve_dataset_name():
    return "LULC_v3" if Dataset.objects.filter(name="LULC_v3").exists() else "LULC_level_3"


def _sync_lulc_stac(layer_id, state, district, block, start_year):
    from computing.STAC_specs import generate_STAC_layerwise

    layer_stac_generated = generate_STAC_layerwise.generate_raster_stac(
        state=state,
        district=district,
        block=block,
        layer_name="land_use_land_cover_raster",
        start_year=str(start_year),
    )
    update_layer_sync_status(
        layer_id=layer_id,
        is_stac_specs_generated=layer_stac_generated,
    )


def run_lulc_v3_local(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    roi_path=None,
    asset_suffix=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    lulc_dir=LULC_BASE_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None
    start_year = int(start_year)
    end_year = int(end_year)

    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    roi_gdf = _resolve_roi(
        state=state,
        district=district,
        block=block,
        roi_path=roi_path,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    filename_prefix = _resolve_filename_prefix(district, block, asset_suffix)
    raster_paths = resolve_lulc_raster_paths(
        start_year=start_year,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )

    layer_at_geoserver = True
    is_admin_run = bool(state and district and block)

    for current_year, raster_path in zip(range(start_year, end_year + 1), raster_paths):
        year_start = str(current_year)[2:]
        year_end = str(current_year + 1)[2:]
        output_stub = _build_output_stub(filename_prefix, current_year)
        output_path = build_output_raster_path(
            layer_name=output_stub,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
            custom_subdir=_slug(asset_suffix, "custom"),
            block_fallback="unknown_block",
        )
        clipped_raster_path = clip_raster_with_roi(
            roi_gdf=roi_gdf,
            raster_path=raster_path,
            output_path=output_path,
            raster_label=f"LULC raster for {current_year}-{current_year + 1}",
        )
        print(f"Saved local LULC raster: {clipped_raster_path}")

        layer_name = _build_layer_name(
            start_year=year_start,
            end_year=year_end,
            filename_prefix=filename_prefix,
        )
        print(f"Prepared local LULC layer {GEOSERVER_WORKSPACE}: {layer_name}")

        layer_id = None
        if sync_layer_metadata and is_admin_run:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=layer_name,
                asset_id=clipped_raster_path,
                dataset_name=_resolve_dataset_name(),
                misc={
                    "start_year": start_year,
                    "end_year": end_year,
                },
                algorithm=LOCAL_ALGORITHM,
                algorithm_version=LOCAL_ALGORITHM_VERSION,
            )

        if not push_to_geoserver:
            continue

        try:
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=clipped_raster_path,
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=GEOSERVER_STYLE,
            )
            print(f"GeoServer upload response for {layer_name}: {upload_res}")
            print(f"GeoServer style response for {layer_name}: {style_res}")
        except Exception as error:
            print(f"Failed to sync local LULC raster {layer_name}: {error}")
            layer_at_geoserver = False
            continue

        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            _sync_lulc_stac(
                layer_id=layer_id,
                state=state,
                district=district,
                block=block,
                start_year=current_year,
            )

    return layer_at_geoserver if push_to_geoserver else True


def _clip_lulc_v3_local_task(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    roi_path=None,
    asset_folder=None,
    asset_suffix=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    app_type="MWS",
):
    _ = gee_account_id, asset_folder, app_type
    return run_lulc_v3_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        roi_path=roi_path,
        asset_suffix=asset_suffix,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def clip_lulc_v3(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    roi_path=None,
    asset_folder=None,
    asset_suffix=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    app_type="MWS",
):
    _ = self
    return _clip_lulc_v3_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        roi_path=roi_path,
        asset_folder=asset_folder,
        asset_suffix=asset_suffix,
        precomputed_roi_dir=precomputed_roi_dir,
        app_type=app_type,
    )
