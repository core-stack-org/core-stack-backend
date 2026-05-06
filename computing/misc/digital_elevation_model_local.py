import os
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app
from computing.utils import push_shape_to_geoserver

from computing.local_compute_helper import (
    PROJECT_ROOT,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_raster_path,
    build_output_vector_path,
    get_union_geometry,
    load_precomputed_roi,
    load_precomputed_watersheds,
    push_local_raster_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)

# ---------------------------------------------------------------------------
# Fix broken PROJ installation BEFORE any pyproj/geopandas import uses it
# ---------------------------------------------------------------------------
try:
    import pyproj

    os.environ["PROJ_DATA"] = pyproj.datadir.get_data_dir()
    os.environ["PROJ_LIB"] = pyproj.datadir.get_data_dir()
except Exception:
    pass

LOCAL_OUTPUT_BASE_DIR = str(PROJECT_ROOT / "data/fabdem/fabdem_local")
TERRAIN_RASTER_PATH = str(PROJECT_ROOT / "data/fabdem/fabdem_pan_india.tif")
GEOSERVER_STYLE = "Terrain_Style_11_Classes"
GEOSERVER_WORKSPACE = "digital_elevation_model"
ZERO_NODATA = -9999  # FABDEM nodata — 0 is valid elevation (sea level)


# ---------------------------------------------------------------------------
# Internal clip helper — reprojects ROI to raster CRS using correct PROJ
# ---------------------------------------------------------------------------


def _clip_fabdem_with_roi(roi_gdf, output_path):
    """
    Clips pan-India FABDEM raster to ROI.
    Reprojects ROI to match raster CRS (EPSG:3857) using pyproj's own data dir,
    bypassing the broken system PROJ installation.
    """
    import pyproj
    import geopandas as gpd

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with rasterio.open(TERRAIN_RASTER_PATH) as src:
        raster_crs = src.crs

        # Reproject ROI to raster CRS — use pyproj data dir to avoid broken PROJ
        if roi_gdf.crs != raster_crs:
            roi_in_raster_crs = roi_gdf.to_crs("EPSG:3857")
        else:
            roi_in_raster_crs = roi_gdf

        roi_union = get_union_geometry(roi_in_raster_crs)
        if roi_union is None or roi_union.is_empty:
            raise ValueError("ROI union geometry is empty — cannot clip FABDEM.")

        roi_shape = mapping(roi_union)

        clipped_array, clipped_transform = mask(
            src,
            shapes=[roi_shape],
            crop=True,
            filled=True,
            nodata=ZERO_NODATA,
        )
        out_meta = src.meta.copy()
        out_meta.update(
            {
                "driver": "GTiff",
                "height": clipped_array.shape[1],
                "width": clipped_array.shape[2],
                "transform": clipped_transform,
                "nodata": ZERO_NODATA,
                "compress": "lzw",
            }
        )

    with rasterio.open(output_path, "w", **out_meta) as dst:
        dst.write(clipped_array)

    print(f"Local clipped FABDEM raster written to: {output_path}")
    return str(output_path)


# ---------------------------------------------------------------------------
# Stage 1 — Raster
# ---------------------------------------------------------------------------


def run_raster_fabdem_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):
    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_dem_raster"
        )
        roi_gdf = load_precomputed_roi(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_dem_raster".lower()
        roi_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )

    output_raster_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
    )

    clipped_raster_path = _clip_fabdem_with_roi(
        roi_gdf=roi_gdf,
        output_path=str(output_raster_path),
    )

    if push_to_geoserver:
        try:
            from utilities.geoserver_utils import Geoserver

            # Step 1 — Pre-delete stale store from any workspace it may exist in
            geo = Geoserver()
            # for ws in ("ne", GEOSERVER_WORKSPACE):
            #     try:
            geo.delete_raster_store(GEOSERVER_WORKSPACE, layer_name)
            # print(
            #     f"Deleted stale raster store '{layer_name}' from workspace '{ws}'"
            # )
            # except Exception:
            #     pass

            # Step 2 — Upload raster → creates coveragestore
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=clipped_raster_path,
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=GEOSERVER_STYLE,
            )
            print(f"GeoServer upload response: {upload_res}")
            print(f"GeoServer style  response: {style_res}")

            # # Step 3 — Explicitly publish coverage as layer → appears in Layer Preview
            # try:
            #     geo.publish_layer(
            #         layer_name=layer_name,
            #         workspace=GEOSERVER_WORKSPACE,
            #         store_name=layer_name,
            #         store_type="coverageStore",
            #     )
            #     print(f"Published raster layer '{layer_name}' to Layer Preview.")
            # except Exception as publish_err:
            #     print(f"publish_layer warning (non-blocking): {publish_err}")

            # # Step 4 — Apply style to the published layer
            # try:
            #     geo.publish_style(
            #         layer_name=layer_name,
            #         style_name=GEOSERVER_STYLE,
            #         workspace=GEOSERVER_WORKSPACE,
            #     )
            #     print(f"Style '{GEOSERVER_STYLE}' applied to '{layer_name}'.")
            # except Exception as style_err:
            #     print(f"publish_style warning (non-blocking): {style_err}")

        except Exception as error:
            print(f"Failed to sync local FABDEM raster to GeoServer: {error}")
            return False, None

    if sync_layer_metadata and state and district and block:
        from computing.STAC_specs import generate_STAC_layerwise
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=clipped_raster_path,
            dataset_name="Terrain Raster",
            algorithm="FABDEM",
            algorithm_version="2.0",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated")

            try:
                layer_stac_generated = generate_STAC_layerwise.generate_raster_stac(
                    state=state,
                    district=district,
                    block=block,
                    layer_name="terrain_raster",
                )
                update_layer_sync_status(
                    layer_id=layer_id,
                    is_stac_specs_generated=layer_stac_generated,
                )
            except Exception as stac_err:
                print(f"STAC generation warning (non-blocking): {stac_err}")

    return True, clipped_raster_path


# ---------------------------------------------------------------------------
# Stage 2 helpers — per-watershed DEM stats
# ---------------------------------------------------------------------------


def _compute_watershed_dem_stats(watersheds_gdf, raster_path):
    """
    GEE equivalent: vectorize_fabdem() reduceRegions(min/max/mean + pixelArea sum)

    GEE                                     Local
    ──────────────────────────────────────  ──────────────────────────────────────
    ee.Image.pixelArea()                    pixel_width × pixel_height / 10_000
    Reducer.min/max/mean/sum per polygon    rasterstats.zonal_stats(min/max/mean/count)
    count × pixel_area_ha                → area_in_ha

    Output columns mirror GEE's .select():
        uid, area_in_ha, min_elevation, max_elevation, mean_elevation
    """
    from rasterstats import zonal_stats

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        pixel_area_ha = (abs(src.res[0]) * abs(src.res[1])) / 10_000.0

    # Reproject watersheds to raster CRS for accurate zonal stats
    watersheds_for_stats = (
        watersheds_gdf
        if watersheds_gdf.crs == raster_crs
        else watersheds_gdf.to_crs(raster_crs.to_epsg())
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

    keep_cols = [
        "uid",
        "area_in_ha",
        "min_elevation",
        "max_elevation",
        "mean_elevation",
        "geometry",
    ]
    return result_gdf[[c for c in keep_cols if c in result_gdf.columns]]


# ---------------------------------------------------------------------------
# Stage 2 — Vector  (= vectorize_fabdem() on GEE)
# ---------------------------------------------------------------------------


def run_vector_fabdem_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    raster_path=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):
    """
    GEE equivalent: vectorize_fabdem()

    GEE flow:                               Local flow:
    ──────────────────────────────────────  ──────────────────────────────────────
    ee.Image(asset_id).select("elevation")  raster_path from Stage 1
    reduceRegions(min/max/mean/pixelArea) →  rasterstats.zonal_stats()
    export_vector_asset_to_gee()          →  write_vector_output() → .gpkg
    sync_layer_to_geoserver() (GeoJSON)   →  push_shape_to_geoserver() (.gpkg)
    """
    if not raster_path:
        raise ValueError(
            "`raster_path` is required for vector stage — pass Stage 1 output."
        )

    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_dem_vector"
        )
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, `asset_suffix` is required."
            )
        layer_name = f"{asset_suffix}_dem_vector".lower()
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")

    result_gdf = _compute_watershed_dem_stats(watersheds_gdf, raster_path)
    print(f"Computed DEM stats for {len(result_gdf)} watersheds")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
    )
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local DEM vector: {asset_id}")

    if push_to_geoserver:
        try:
            geoserver_response = push_shape_to_geoserver(
                os.path.splitext(asset_id)[0],
                workspace=GEOSERVER_WORKSPACE,
                layer_name=layer_name,
                file_type="gpkg",
            )
            print(f"GeoServer vector response: {geoserver_response}")
            if not isinstance(geoserver_response, dict) or geoserver_response.get(
                "status_code"
            ) not in (200, 201):
                return False
        except Exception as error:
            print(f"Failed to sync local FABDEM vector to GeoServer: {error}")
            return False

    if sync_layer_metadata and state and district and block:
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="DEM Vector",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for DEM vector")

    return True


# ---------------------------------------------------------------------------
# Orchestrator — runs Stage 1 then Stage 2
# ---------------------------------------------------------------------------


def run_fabdem_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):
    """
    Full local FABDEM pipeline.
    Stage 1: Clip pan-India raster → GeoTIFF → GeoServer
    Stage 2: Zonal stats per watershed → GeoPackage → GeoServer
    """
    raster_ok, clipped_raster_path = run_raster_fabdem_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    if not raster_ok or not clipped_raster_path:
        print("Raster stage failed — skipping vector stage.")
        return False

    vector_ok = run_vector_fabdem_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        raster_path=clipped_raster_path,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )

    return raster_ok and vector_ok


# ---------------------------------------------------------------------------
# Internal task wrapper
# ---------------------------------------------------------------------------


def _generate_febdem_raster_clip_task(
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    asset_suffix=None,
    asset_folder=None,
    proj_id=None,
    roi=None,
    precomputed_roi_dir=None,
    app_type="MWS",
):
    _ = gee_account_id, asset_folder, proj_id, app_type
    return run_fabdem_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


# ---------------------------------------------------------------------------
# Celery task — same signature as before, now runs both stages
# ---------------------------------------------------------------------------


@app.task(bind=True)
def generate_febdem_raster_clip(
    self,
    state=None,
    district=None,
    block=None,
    gee_account_id=None,
    asset_suffix=None,
    asset_folder=None,
    proj_id=None,
    roi=None,
    precomputed_roi_dir=None,
    app_type="MWS",
):
    _ = self
    return _generate_febdem_raster_clip_task(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        proj_id=proj_id,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        app_type=app_type,
    )
