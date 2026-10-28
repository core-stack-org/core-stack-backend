"""
Local version of the ZOI pipeline (zoi1 + zoi2).

Reads the local SWB GeoPackage (no swb3_ GEE asset needed), builds the ZOI rings,
computes cropping intensity on them from the local LULC rasters and pushes both
to GeoServer. Only the NDVI step (zoi3) still runs on GEE: the final rings are
exported once to GEE as its input.
"""

import logging
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from computing.config_loader import SWB_VECTOR_OUTPUT_DIR as SWB_OUTPUT_BASE_DIR
from computing.cropping_intensity.cropping_intensity_local import (
    _coerce_year,
    _compute_cropping_intensity_columns,
    _compute_total_croppable_area,
    _compute_yearly_area_columns,
    INITIAL_YEAR,
)
from computing.local_compute_helper import (
    LULC_BASE_DIR,
    build_output_vector_path,
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
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from nrm_app.celery import app
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    create_gee_dir,
    ee_initialize,
    get_gee_dir_path,
    is_gee_asset_exists,
    make_asset_public,
)
from waterrejuvenation.utils import delete_asset_on_GEE

logger = logging.getLogger(__name__)

ZOI_OUTPUT_BASE_DIR = SWB_OUTPUT_BASE_DIR.parent / "zoi"
GEOSERVER_WORKSPACE = "swb"
DATASET_NAME = "Surface Water Bodies"
LOCAL_ALGORITHM = "local_zoi"
LOCAL_ALGORITHM_VERSION = "local-1.0"
AREA_CRS = "EPSG:6933"  # equal-area, for hectares when area_ored is missing


def zoi_layer_name(asset_suffix):
    return f"waterbodies_zoi_{asset_suffix}"


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


def _publish(
    gdf, layer_name, state, district, block, asset_suffix, misc, sync_layer_metadata
):
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=ZOI_OUTPUT_BASE_DIR,
        custom_subdir=asset_suffix,
        block_fallback="unknown_block",
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
            misc={"is_generated_locally": True, **misc},
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )

    response = push_shape_to_geoserver(
        str(Path(local_path).with_suffix("")),
        workspace=GEOSERVER_WORKSPACE,
        layer_name=layer_name,
        file_type="gpkg",
    )
    synced = isinstance(response, dict) and response.get("status_code") in (200, 201)
    if not synced:
        raise RuntimeError(f"Failed to sync ZOI layer to GeoServer: {layer_name}")
    if layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
    return local_path


def export_zoi_to_gee_for_ndvi(rings_gdf, asset_suffix, asset_folder_list, app_type):
    """NDVI still runs on GEE, so the finished rings are uploaded once as its input."""
    create_gee_dir(
        asset_folder_list, gee_project_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
    )
    description = f"zoi_{asset_suffix}"
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )
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
    """Returns the GEE asset id of the finished rings (input for the NDVI step)."""
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

    misc = {}
    if start_date and end_date:
        misc = {"start_year": start_date.split("-")[0], "end_year": end_date.split("-")[0]}
    layer_name = zoi_layer_name(asset_suffix)
    publish = lambda gdf: _publish(  # noqa: E731
        gdf, layer_name, state, district, block, asset_suffix, misc, sync_layer_metadata
    )

    rings = build_zoi_rings(swb_gdf)
    logger.info("Built %s ZOI rings", len(rings))
    publish(rings)

    rings = add_zoi_cropping_intensity(rings, start_year, end_year, lulc_dir=lulc_dir)
    publish(rings)

    asset_folder_list = [state, district, block]
    return export_zoi_to_gee_for_ndvi(rings, asset_suffix, asset_folder_list, app_type)


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
):
    """Local ZOI + CI, then NDVI on GEE. Project (proj_id) runs stay on the GEE path."""
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

    from computing.zoi_layers.zoi3 import get_ndvi_for_zoi

    ee_initialize(gee_account_id)  # needed for the ring upload below as well as NDVI
    start_date, end_date, start_year, end_year = zoi_gee._resolve_zoi_time_window(
        start_date, end_date, start_year, end_year
    )
    zoi_asset_id = run_zoi_local(
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

    suffix = _resolve_asset_suffix(
        str(state).lower(), str(district).lower(), str(block).lower(), asset_suffix
    )
    get_ndvi_for_zoi(
        state=state,
        district=district,
        block=block,
        zoi_roi=zoi_asset_id,
        asset_suffix=suffix,
        asset_folder_list=[state, district, block],
        app_type=app_type,
        gee_account_id=gee_account_id,
        start_date=start_date,
        end_date=end_date,
        start_year=start_year,
        end_year=end_year,
    )
    return True
