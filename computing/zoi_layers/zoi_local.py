"""
Local version of the ZOI pipeline (zoi1 + zoi2) plus GEE NDVI enrichment (zoi3).

``run_zoi_local`` runs purely locally (no GEE needed): it reads the local SWB
GeoPackage, builds ZOI rings (zoi1), computes cropping intensity from local LULC
rasters (zoi2), writes the GeoPackage, records DB metadata, and queues a
GeoServer push on ``swb:waterbodies_zoi_{district}_{block}``. If zoi2 fails,
the zoi1 rings are saved and pushed to GeoServer as a fallback.

When NDVI enrichment is run later (via ``sync_local_zoi_to_gee_and_run_ndvi`` /
``sync_zoi_ndvi_gee`` management command, or one-shot ``run_ndvi=True``), the
local ZOI GeoPackage is uploaded to GEE, NDVI is computed on GEE, the enriched
output is saved back to the local GeoPackage, and the same GeoServer layer is
updated in place.
"""

import logging
import math
from pathlib import Path

import ee
import geopandas as gpd
import numpy as np
import pandas as pd

from computing.config_loader import (
    SWB_VECTOR_OUTPUT_DIR as SWB_OUTPUT_BASE_DIR,
    ZOI_OUTPUT_DIR,
    layer_dir,
)
from computing.cropping_intensity.cropping_intensity_local import (
    _coerce_year,
    _compute_cropping_intensity_columns,
    _compute_total_croppable_area,
    _compute_yearly_area_columns,
    INITIAL_YEAR,
)
from computing.local_compute_helper import (
    LULC_BASE_DIR,
    _slug,
    build_output_vector_path,
    push_local_vector_to_geoserver,
    queue_local_vector_for_geoserver,
    resolve_lulc_raster_paths,
    write_vector_output,
)
from computing.surface_water_bodies.swb_local import (
    _export_gdf_to_gee_in_chunks,
    _layer_name as _swb_layer_name,
    _prepare_gdf_for_gee,
    _resolve_asset_suffix,
)
from computing.utils import (
    fix_invalid_geometry_in_gdf,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    check_task_status,
    create_gee_dir,
    ee_initialize,
    get_gee_dir_path,
    get_geojson_from_gcs,
    is_gee_asset_exists,
    make_asset_public,
    sync_vector_to_gcs,
)
from waterrejuvenation.utils import delete_asset_on_GEE

logger = logging.getLogger(__name__)

ZOI_OUTPUT_BASE_DIR = ZOI_OUTPUT_DIR
GEOSERVER_WORKSPACE = "swb"
DATASET_NAME = "Surface Water Bodies"
LOCAL_ALGORITHM = "local_zoi"
LOCAL_ALGORITHM_VERSION = "local-1.0"
AREA_CRS = "EPSG:6933"  # equal-area, for hectares when area_ored is missing


def zoi_layer_name(asset_suffix):
    return f"waterbodies_zoi_{asset_suffix}"


def zoi_output_path(state, district, block, asset_suffix=None, create_dir=False):
    """Return the local GeoPackage path for a tehsil's ZOI layer."""
    state = str(state).strip().lower() if state else state
    district = str(district).strip().lower() if district else district
    block = str(block).strip().lower() if block else block
    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    layer_name = zoi_layer_name(asset_suffix)

    if create_dir:
        return build_output_vector_path(
            layer_name=layer_name,
            state=state,
            district=district,
            block=block,
            output_base_dir=ZOI_OUTPUT_BASE_DIR,
            custom_subdir=asset_suffix,
            block_fallback="unknown_block",
        )

    base_dir = Path(ZOI_OUTPUT_BASE_DIR)
    if state and district and block:
        output_dir = layer_dir(
            base_dir,
            _slug(state, "unknown_state"),
            _slug(district, "unknown_district"),
            _slug(block, "unknown_block"),
        )
    else:
        output_dir = base_dir / asset_suffix
    return output_dir / f"{layer_name}.gpkg"


def _gee_zoi_asset_id(asset_folder_list, asset_suffix, app_type="MWS"):
    return (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + f"zoi_{asset_suffix}"
    )


def _gee_ndvi_asset_id(asset_folder_list, asset_suffix, app_type="MWS"):
    return (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + asset_suffix
    )


def _stored_area_ha(gdf):
    """area_ored as float; NaN where missing, 'N/A' or non-numeric (same as GEE)."""
    if "area_ored" not in gdf.columns:
        return pd.Series(np.nan, index=gdf.index)
    return pd.to_numeric(gdf["area_ored"], errors="coerce")


def _waterbody_area_ha(gdf):
    """area_ored when > 0, else geometry area in ha (waterrejuvenation.utils._waterbody_area_ha)."""
    geom_area_ha = gdf.to_crs(AREA_CRS).geometry.area / 10_000
    stored = _stored_area_ha(gdf)
    return stored.where(stored > 0, geom_area_ha)


def compute_zoi_radius_m(area_ha):
    """Logistic blend of the small / large waterbody fits (same as zoi1.compute_zoi)."""
    area_ha = np.asarray(area_ha, dtype=float)
    s = 1.0 / (1.0 + np.exp(-50.0 * (area_ha - 0.2)))
    y_small = 126.84 * np.log(area_ha + 0.05) + 383.57
    y_large = 140.0 * np.log(area_ha + 0.05) + 500.0
    return (1.0 - s) * y_small + np.round(s * y_large)


def _first_valid(gdf, columns):
    result = pd.Series(None, index=gdf.index, dtype=object)
    for column in columns:
        if column not in gdf.columns:
            continue
        values = gdf[column]
        usable = values.notna() & (values.astype(str).str.strip() != "")
        result = result.where(result.notna(), values.where(usable))
    return result


def build_zoi_rings(swb_gdf):
    """One circular ring per waterbody, buffered from its centroid by the ZOI radius."""
    gdf = swb_gdf.copy()
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    gdf["zoi_wb"] = compute_zoi_radius_m(_waterbody_area_ha(gdf))
    gdf = gdf[gdf["zoi_wb"] > 0].copy()

    uid = _first_valid(gdf, ["UID", "uid", "MWS_UID"])
    wb_id = _first_valid(gdf, ["wb_id"])
    gdf["UID"] = uid.where(uid.notna(), wb_id)
    gdf["wb_id"] = wb_id.where(wb_id.notna(), gdf["UID"])

    # Buffer in a local metric CRS so the radius is in metres, then back to 4326.
    metric_crs = gdf.estimate_utm_crs()
    centroids = gdf.to_crs(metric_crs).geometry.centroid
    rings = centroids.buffer(gdf["zoi_wb"].to_numpy()).to_crs("EPSG:4326")

    out = gdf.drop(columns="geometry").copy()
    out["zoi"] = gdf["zoi_wb"].to_numpy()
    out["zoi_area"] = (math.pi * out["zoi"] ** 2 / 10_000).round(2)
    return gpd.GeoDataFrame(out, geometry=rings.values, crs="EPSG:4326")


def add_zoi_cropping_intensity(rings_gdf, start_year, end_year, lulc_dir=LULC_BASE_DIR):
    """Same columns as the watershed CI, computed over the ZOI rings."""
    yearly_rasters = resolve_lulc_raster_paths(
        start_year=start_year, end_year=end_year, lulc_dir=lulc_dir
    )
    denominator_rasters = resolve_lulc_raster_paths(
        start_year=INITIAL_YEAR, end_year=end_year, lulc_dir=lulc_dir
    )
    result = _compute_yearly_area_columns(
        result_gdf=rings_gdf.copy(),
        raster_paths=yearly_rasters,
        start_year=start_year,
    )
    result = _compute_total_croppable_area(
        result_gdf=result,
        denominator_raster_paths=denominator_rasters,
        end_year=end_year,
    )
    return _compute_cropping_intensity_columns(
        result_gdf=result, start_year=start_year, end_year=end_year
    )


def _swb_input_path(state, district, block, asset_suffix):
    return build_output_vector_path(
        layer_name=_swb_layer_name(asset_suffix),
        state=state,
        district=district,
        block=block,
        output_base_dir=SWB_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
        block_fallback="unknown_block",
    )


def _fc_to_gdf(fc, layer_name):
    """Fetch a GEE FeatureCollection into a validated EPSG:4326 GeoDataFrame."""
    try:
        geojson_fc = fc.getInfo()
    except Exception as exc:
        logger.warning(
            "fc.getInfo() failed for %s (%s); falling back to GCS GeoJSON export",
            layer_name,
            exc,
        )
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])
        geojson_fc = get_geojson_from_gcs(layer_name)

    features = geojson_fc.get("features") if isinstance(geojson_fc, dict) else None
    if not features:
        raise RuntimeError(
            f"ZOI NDVI GEE FeatureCollection has no features: {layer_name}"
        )

    gdf = gpd.GeoDataFrame.from_features(features)
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
    return fix_invalid_geometry_in_gdf(gdf)


def _publish(
    gdf,
    layer_name,
    state,
    district,
    block,
    asset_suffix,
    misc,
    sync_layer_metadata,
    sync_geoserver_inline=False,
):
    output_path = zoi_output_path(
        state, district, block, asset_suffix=asset_suffix, create_dir=True
    )
    local_path = write_vector_output(
        gdf=gdf, output_path=output_path, layer_name=layer_name
    )

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=str(local_path),
            dataset_name=DATASET_NAME,
            misc={
                "is_generated_locally": True,
                "local_vector_path": str(local_path),
                **misc,
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )

    base_path = str(Path(local_path).with_suffix(""))
    if sync_geoserver_inline:
        response = push_local_vector_to_geoserver(
            path=base_path,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        synced = isinstance(response, dict) and response.get("status_code") in (
            200,
            201,
            202,
        )
        if not synced:
            raise RuntimeError(
                f"Failed to sync ZOI layer to GeoServer: {layer_name} ({response})"
            )
        if layer_id and not response.get("skipped"):
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=True,
                is_stac_specs_generated=False,
            )
    else:
        response = queue_local_vector_for_geoserver(
            path=base_path,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
            layer_id=layer_id,
        )
        queued_or_synced = isinstance(response, dict) and response.get(
            "status_code"
        ) in (200, 201, 202)
        if not queued_or_synced:
            raise RuntimeError(
                f"Failed to queue ZOI layer for GeoServer: {layer_name} ({response})"
            )

    return Path(local_path), layer_id


def export_zoi_to_gee_for_ndvi(rings_gdf, asset_suffix, asset_folder_list, app_type):
    """Upload the local ZOI GeoDataFrame to GEE as the input asset for zoi3 NDVI."""
    create_gee_dir(
        asset_folder_list, gee_project_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    description = f"zoi_{asset_suffix}"
    asset_id = _gee_zoi_asset_id(asset_folder_list, asset_suffix, app_type)
    delete_asset_on_GEE(asset_id)  # same as zoi1: replace any previous run
    _export_gdf_to_gee_in_chunks(_prepare_gdf_for_gee(rings_gdf), description, asset_id)
    if not is_gee_asset_exists(asset_id):
        raise RuntimeError(f"ZOI GEE asset was not created: {asset_id}")
    make_asset_public(asset_id)
    return asset_id


def run_zoi_local(
    state,
    district,
    block,
    start_year,
    end_year,
    start_date=None,
    end_date=None,
    asset_suffix=None,
    app_type="MWS",
    sync_layer_metadata=True,
    lulc_dir=LULC_BASE_DIR,
):
    """
    Run local ZOI (zoi1 + zoi2) without touching GEE.

    If zoi2 (cropping intensity) succeeds, writes and queues the zoi2 GeoPackage
    once (``source_stage="zoi2_local"``). If zoi2 fails, logs the exception and
    falls back to writing and queueing the zoi1 rings GeoPackage
    (``source_stage="zoi1_local"``).

    Returns the local GeoPackage ``Path``.
    """
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()
    start_year = _coerce_year(start_year, "start_year")
    end_year = _coerce_year(end_year, "end_year")

    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    swb_path = _swb_input_path(state, district, block, asset_suffix)
    if not swb_path.exists():
        raise FileNotFoundError(
            f"Local SWB layer not found, run generate_swb first: {swb_path}"
        )

    swb_gdf = gpd.read_file(swb_path)
    logger.info("Loaded %s SWB features from %s", len(swb_gdf), swb_path)

    base_misc = {
        "start_year": start_date.split("-")[0] if start_date else str(start_year),
        "end_year": end_date.split("-")[0] if end_date else str(end_year),
    }
    layer_name = zoi_layer_name(asset_suffix)

    rings = build_zoi_rings(swb_gdf)
    logger.info("Built %s ZOI rings for %s", len(rings), asset_suffix)

    try:
        rings_with_ci = add_zoi_cropping_intensity(
            rings, start_year, end_year, lulc_dir=lulc_dir
        )
    except Exception:
        logger.exception(
            "ZOI2 cropping intensity failed for %s; falling back to ZOI1 rings",
            asset_suffix,
        )
        local_path, _ = _publish(
            rings,
            layer_name,
            state,
            district,
            block,
            asset_suffix,
            {**base_misc, "source_stage": "zoi1_local"},
            sync_layer_metadata,
        )
        return local_path

    local_path, _ = _publish(
        rings_with_ci,
        layer_name,
        state,
        district,
        block,
        asset_suffix,
        {**base_misc, "source_stage": "zoi2_local"},
        sync_layer_metadata,
    )
    return local_path


def sync_local_zoi_to_gee_and_run_ndvi(
    state,
    district,
    block,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    asset_suffix=None,
    app_type="MWS",
    gee_account_id=None,
    upload_only=False,
    overwrite_gee_upload=False,
    skip_existing_ndvi=False,
    fast_ndvi=False,
    sync_layer_metadata=True,
    sync_geoserver_inline=False,
):
    """
    Upload a locally generated ZOI GeoPackage (zoi1/zoi2) to GEE, compute NDVI
    (zoi3) on GEE, save the NDVI-enriched output back to the local GeoPackage,
    update DB metadata in place, and update the GeoServer layer.
    """
    from computing.zoi_layers import zoi as zoi_gee
    from computing.zoi_layers.zoi3 import get_ndvi_for_zoi
    from computing.zoi_layers.zoi_ndvi_from_timeseries import (
        get_ndvi_for_zoi_from_timeseries_compute,
    )

    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()
    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    layer_name = zoi_layer_name(asset_suffix)
    asset_folder_list = [state, district, block]

    local_path = zoi_output_path(
        state, district, block, asset_suffix=asset_suffix, create_dir=False
    )
    if not local_path.exists():
        raise FileNotFoundError(
            f"Local ZOI layer not found, run local generate_zoi first: {local_path}"
        )

    if not upload_only:
        start_date, end_date, start_year, end_year = zoi_gee._resolve_zoi_time_window(
            start_date, end_date, start_year, end_year
        )

    ee_initialize(gee_account_id)

    zoi_asset_id = _gee_zoi_asset_id(asset_folder_list, asset_suffix, app_type)
    if overwrite_gee_upload or not is_gee_asset_exists(zoi_asset_id):
        rings_gdf = gpd.read_file(local_path)
        logger.info(
            "Uploading %s local ZOI features from %s to GEE asset %s",
            len(rings_gdf),
            local_path,
            zoi_asset_id,
        )
        zoi_asset_id = export_zoi_to_gee_for_ndvi(
            rings_gdf, asset_suffix, asset_folder_list, app_type
        )
    else:
        logger.info("Reusing existing GEE ZOI input asset: %s", zoi_asset_id)

    if upload_only:
        return {
            "stage": "uploaded",
            "zoi_asset_id": zoi_asset_id,
            "ndvi_asset_id": None,
            "local_path": str(local_path),
            "layer_id": None,
        }

    ndvi_asset_id = _gee_ndvi_asset_id(asset_folder_list, asset_suffix, app_type)
    if skip_existing_ndvi and is_gee_asset_exists(ndvi_asset_id):
        logger.info("Reusing existing GEE ZOI NDVI asset: %s", ndvi_asset_id)
    else:
        ndvi_runner = (
            get_ndvi_for_zoi_from_timeseries_compute
            if fast_ndvi
            else get_ndvi_for_zoi
        )
        ndvi_runner(
            state=state,
            district=district,
            block=block,
            zoi_roi=zoi_asset_id,
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            start_date=start_date,
            end_date=end_date,
            start_year=start_year,
            end_year=end_year,
            app_type=app_type,
            gee_account_id=gee_account_id,
            sync_to_geoserver=False,
        )
        if not is_gee_asset_exists(ndvi_asset_id):
            raise RuntimeError(
                f"ZOI NDVI GEE asset was not created: {ndvi_asset_id}"
            )

    make_asset_public(ndvi_asset_id)

    ndvi_gdf = _fc_to_gdf(ee.FeatureCollection(ndvi_asset_id), layer_name)
    misc = {
        "source_stage": "zoi3_ndvi",
        "start_year": start_date.split("-")[0],
        "end_year": end_date.split("-")[0],
        "start_date": start_date,
        "end_date": end_date,
        "gee_zoi_asset_id": zoi_asset_id,
        "gee_ndvi_asset_id": ndvi_asset_id,
        "ndvi_mode": "timeseries" if fast_ndvi else "standard",
    }
    updated_path, layer_id = _publish(
        ndvi_gdf,
        layer_name,
        state,
        district,
        block,
        asset_suffix,
        misc,
        sync_layer_metadata=sync_layer_metadata,
        sync_geoserver_inline=sync_geoserver_inline,
    )
    return {
        "stage": "zoi3_ndvi",
        "zoi_asset_id": zoi_asset_id,
        "ndvi_asset_id": ndvi_asset_id,
        "local_path": str(updated_path),
        "layer_id": layer_id,
    }


@app.task()
def generate_zoi(
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
    proj_id=None,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    run_ndvi=False,
    fast_ndvi=False,
):
    """
    Local ZOI (zoi1 + zoi2) by default (purely local, no GEE calls).

    Pass ``run_ndvi=True`` to also upload the local ZOI layer to GEE, compute
    NDVI (zoi3), save the NDVI-enriched output locally, and update GeoServer in
    one shot. Project (``proj_id``) runs stay on the GEE path.
    """
    from computing.zoi_layers import zoi as zoi_gee

    if not (state and district and block):
        return zoi_gee.generate_zoi(
            state=state,
            district=district,
            block=block,
            roi=roi,
            asset_suffix=asset_suffix,
            asset_folder_list=asset_folder_list,
            app_type=app_type,
            gee_account_id=gee_account_id,
            proj_id=proj_id,
            start_date=start_date,
            end_date=end_date,
            start_year=start_year,
            end_year=end_year,
        )

    start_date, end_date, start_year, end_year = zoi_gee._resolve_zoi_time_window(
        start_date, end_date, start_year, end_year
    )
    run_zoi_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        start_date=start_date,
        end_date=end_date,
        asset_suffix=asset_suffix,
        app_type=app_type,
    )

    if run_ndvi:
        sync_local_zoi_to_gee_and_run_ndvi(
            state=state,
            district=district,
            block=block,
            start_date=start_date,
            end_date=end_date,
            start_year=start_year,
            end_year=end_year,
            asset_suffix=asset_suffix,
            app_type=app_type,
            gee_account_id=gee_account_id,
            overwrite_gee_upload=True,
            fast_ndvi=fast_ndvi,
        )
    return True


@app.task(name="computing.zoi_layers.zoi_local.sync_zoi_ndvi_gee")
def sync_zoi_ndvi_gee(
    state,
    district,
    block,
    start_date=None,
    end_date=None,
    start_year=None,
    end_year=None,
    asset_suffix=None,
    app_type="MWS",
    gee_account_id=None,
    upload_only=False,
    overwrite_gee_upload=False,
    skip_existing_ndvi=False,
    fast_ndvi=False,
):
    """Celery task wrapping ``sync_local_zoi_to_gee_and_run_ndvi``."""
    return sync_local_zoi_to_gee_and_run_ndvi(
        state=state,
        district=district,
        block=block,
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
        asset_suffix=asset_suffix,
        app_type=app_type,
        gee_account_id=gee_account_id,
        upload_only=upload_only,
        overwrite_gee_upload=overwrite_gee_upload,
        skip_existing_ndvi=skip_existing_ndvi,
        fast_ndvi=fast_ndvi,
    )
