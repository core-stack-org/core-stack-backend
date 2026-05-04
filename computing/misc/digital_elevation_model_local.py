# dem_local.py
import os

import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_raster_path,
    build_output_vector_path,
    get_union_geometry,
    load_precomputed_roi,
    load_precomputed_watersheds,
    push_local_raster_to_geoserver,
    validate_geometry,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.STAC_specs import generate_STAC_layerwise

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/dem/dem_local"
GEOSERVER_WORKSPACE = "digital_elevation_model"
FABDEM_LOCAL_PATH = PROJECT_ROOT / "data/fabdem/fabdem_pan_india.tif"

ZERO_NODATA = -9999  # FABDEM elevation nodata — 0 is a valid elevation (sea level)


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _base_description(district, block):
    return f"{_slug(district, 'unknown_district')}_" f"{_slug(block, 'unknown_block')}"


def _raster_layer_name(district, block):
    return f"{_base_description(district, block)}_dem_raster"


def _vector_layer_name(district, block):
    return f"{_base_description(district, block)}_dem_vector"


# ---------------------------------------------------------------------------
# Stage 1 helpers — raster
# ---------------------------------------------------------------------------


def _load_and_clip_fabdem(roi_gdf):
    """
    Clips the pan-India FABDEM raster to the ROI geometry.

    GEE equivalent:
        fabdem_img.mosaic()
            .setDefaultProjection("EPSG:3857", None, 30)
            .rename("elevation")
            .clip(roi_boundary.geometry())

    No tile merging needed — single pan-India file covers all of India.
    """
    roi_gdf = validate_geometry(roi_gdf)
    if roi_gdf.empty:
        raise ValueError("ROI GeoDataFrame is empty — cannot clip FABDEM.")

    if not FABDEM_LOCAL_PATH.exists():
        raise FileNotFoundError(
            f"Pan-India FABDEM raster not found at {FABDEM_LOCAL_PATH}. "
            "Ensure the file is downloaded and placed at that path."
        )

    with rasterio.open(str(FABDEM_LOCAL_PATH)) as src:
        roi_in_raster_crs = (
            roi_gdf if roi_gdf.crs == src.crs else roi_gdf.to_crs(src.crs)
        )
        roi_union_shape = mapping(get_union_geometry(roi_in_raster_crs))

        clipped_array, clipped_transform = mask(
            src,
            shapes=[roi_union_shape],
            crop=True,
            filled=True,
            nodata=ZERO_NODATA,
        )

        out_meta = src.meta.copy()
        raster_crs = src.crs

    clipped_array = clipped_array[0].astype(np.float32)
    out_meta.update(
        {
            "driver": "GTiff",
            "height": clipped_array.shape[0],
            "width": clipped_array.shape[1],
            "transform": clipped_transform,
            "count": 1,
            "dtype": "float32",
            "compress": "lzw",
            "nodata": ZERO_NODATA,
        }
    )
    return clipped_array, clipped_transform, raster_crs, out_meta


def _write_dem_raster(array, output_path, meta):
    """Write float32 elevation array to a GeoTIFF."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with rasterio.open(output_path, "w", **meta) as dst:
        dst.write(array.astype(np.float32), 1)
    return str(output_path)


# ---------------------------------------------------------------------------
# Stage 2 helpers — vectorization
# ---------------------------------------------------------------------------


def _compute_watershed_dem_stats(watersheds_gdf, raster_path):
    """
    Computes per-watershed DEM statistics using rasterstats.

    GEE equivalent:
        pixel_area = ee.Image.pixelArea()
        combined = dem.addBands(pixel_area)
        fc = combined.reduceRegions(
            reducer = min + max + mean + sum,
            scale   = nominalScale,
        )

    Local mapping:
        zonal_stats(min / max / mean / count) where:
            count × pixel_area_ha  ≈  GEE pixel_area band sum → area_in_ha

    Output columns match GEE's process() selector:
        uid, area_in_ha, min_elevation, max_elevation, mean_elevation
    """
    from rasterstats import zonal_stats

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        pixel_width = abs(src.res[0])
        pixel_height = abs(src.res[1])

    # pixel area in m² → hectares (works for projected CRS like EPSG:32643 etc.)
    pixel_area_ha = (pixel_width * pixel_height) / 10_000.0

    watersheds_for_stats = (
        watersheds_gdf
        if watersheds_gdf.crs == raster_crs
        else watersheds_gdf.to_crs(raster_crs)
    )

    stats = zonal_stats(
        watersheds_for_stats,
        raster_path,
        stats=["min", "max", "mean", "count"],
        nodata=ZERO_NODATA,
        all_touched=False,
    )

    result_gdf = watersheds_gdf.copy()
    result_gdf["min_elevation"] = [s.get("min") for s in stats]
    result_gdf["max_elevation"] = [s.get("max") for s in stats]
    result_gdf["mean_elevation"] = [s.get("mean") for s in stats]
    result_gdf["area_in_ha"] = [(s.get("count") or 0) * pixel_area_ha for s in stats]

    # Mirror GEE's .select(["uid", "area_in_ha", "min_elevation", ...])
    keep_cols = [
        "uid",
        "area_in_ha",
        "min_elevation",
        "max_elevation",
        "mean_elevation",
        "geometry",
    ]
    available_cols = [c for c in keep_cols if c in result_gdf.columns]
    return result_gdf[available_cols]


# ---------------------------------------------------------------------------
# Stage 1 — DEM raster  (= dem_raster_generation() on GEE)
# ---------------------------------------------------------------------------


def run_dem_raster_local(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    GEE equivalent: dem_raster_generation()

    GEE flow:                           Local flow:
    ─────────────────────────────────   ──────────────────────────────────────
    ee.ImageCollection(FABDEM)          rasterio.open(FABDEM_LOCAL_PATH)
    .mosaic().clip(roi.geometry())  →   rasterio.mask.mask(roi_union_shape)
    export_raster_asset_to_gee()    →   _write_dem_raster()
    sync_raster_to_gcs()            →   (no cloud step needed)
    sync_raster_gcs_to_geoserver()  →   push_local_raster_to_geoserver()
    save_layer_info_to_db()         →   save_layer_info_to_db()
    generate_STAC_layerwise         →   generate_STAC_layerwise
    """
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()

    roi_gdf = load_precomputed_roi(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )

    clipped_array, clipped_transform, raster_crs, out_meta = _load_and_clip_fabdem(
        roi_gdf
    )
    print(f"Clipped FABDEM to ROI — shape: {clipped_array.shape}, CRS: {raster_crs}")

    layer_name = _raster_layer_name(district, block)
    output_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
        block_fallback="unknown_block",
    )

    raster_path = _write_dem_raster(clipped_array, str(output_path), out_meta)
    print(f"Saved local DEM raster: {raster_path}")

    layer_at_geoserver = False

    if push_to_geoserver:
        upload_res, style_res = push_local_raster_to_geoserver(
            file_path=raster_path,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name="digital_elevation_model",
        )
        print(f"GeoServer upload response: {upload_res}")
        print(f"GeoServer style  response: {style_res}")
        layer_at_geoserver = True

    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=raster_path,
            dataset_name="DEM Raster",
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            layer_stac_generated = generate_STAC_layerwise.generate_raster_stac(
                state=state,
                district=district,
                block=block,
                layer_name="dem_raster",
            )
            update_layer_sync_status(
                layer_id=layer_id,
                is_stac_specs_generated=layer_stac_generated,
            )

    # Return raster_path so the orchestrator can pass it directly to Stage 2
    # without reconstructing the path from naming helpers again.
    return layer_at_geoserver, raster_path


# ---------------------------------------------------------------------------
# Stage 2 — DEM vectorization  (= vectorize_fabdem() on GEE)
# ---------------------------------------------------------------------------


def run_dem_vector_local(
    state,
    district,
    block,
    raster_path,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    GEE equivalent: vectorize_fabdem()

    GEE flow:                               Local flow:
    ───────────────────────────────────     ──────────────────────────────────────
    ee.Image(asset_id).select("elevation")  raster_path from Stage 1
    ee.Image.pixelArea()                    pixel_width × pixel_height / 10_000
    combined.reduceRegions(min/max/mean) →  rasterstats.zonal_stats()
    export_vector_asset_to_gee()         →  write_vector_output() → .gpkg
    sync_layer_to_geoserver() (GeoJSON)  →  push_shape_to_geoserver() (.gpkg)
    save_layer_info_to_db()              →  save_layer_info_to_db()
    """
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    print(f"Watershed boundary source: {watershed_source}")

    result_gdf = _compute_watershed_dem_stats(watersheds_gdf, raster_path)
    print(f"Computed DEM stats for {len(result_gdf)} watersheds")

    layer_name = _vector_layer_name(district, block)
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        block_fallback="unknown_block",
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local DEM vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer vector response: {geoserver_response}")
        if isinstance(geoserver_response, dict) and geoserver_response.get(
            "status_code"
        ) in (200, 201):
            layer_at_geoserver = True

    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="DEM Vector",
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return layer_at_geoserver


# ---------------------------------------------------------------------------
# Orchestrator  (= generate_dem_raster() on GEE)
# ---------------------------------------------------------------------------


def run_dem_local(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Full local DEM pipeline — mirrors generate_dem_raster() on the server.

    Stage 1: Clip pan-India FABDEM → write GeoTIFF → push raster to GeoServer
    Stage 2: Zonal stats per watershed → write GeoPackage → push vector to GeoServer
    """
    layer_at_geoserver, raster_path = run_dem_raster_local(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    run_dem_vector_local(
        state=state,
        district=district,
        block=block,
        raster_path=raster_path,  # Stage 1 output fed directly into Stage 2
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    return layer_at_geoserver


# ---------------------------------------------------------------------------
# Internal task wrapper
# ---------------------------------------------------------------------------


def _get_dem_local_task(
    state,
    district,
    block,
    gee_account_id=None,  # ignored locally, kept for call-site compatibility
):
    _ = gee_account_id
    return run_dem_local(
        state=state,
        district=district,
        block=block,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


# ---------------------------------------------------------------------------
# Celery task — same signature as server's generate_dem_raster()
# ---------------------------------------------------------------------------


@app.task(bind=True)
def generate_dem_raster(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    proj_id=None,  # unused locally
    roi_path=None,  # unused locally
    asset_suffix=None,  # unused locally
    asset_folder=None,  # unused locally
    app_type="MWS",  # unused locally
):
    _ = self
    return _get_dem_local_task(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
    )
