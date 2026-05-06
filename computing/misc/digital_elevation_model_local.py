import os
import sys
import tempfile
import subprocess


# ---------------------------------------------------------------------------
# Fix broken PROJ installation BEFORE any GDAL/rasterio/pyproj imports
# ---------------------------------------------------------------------------
def fix_proj_environment():
    """Fix PROJ environment to avoid version conflicts and CRS issues"""
    try:
        import pyproj

        proj_data_dir = pyproj.datadir.get_data_dir()
        os.environ["PROJ_DATA"] = proj_data_dir
        os.environ["PROJ_LIB"] = proj_data_dir

        # Try to find GDAL data directory
        try:
            gdal_config = subprocess.run(
                ["gdal-config", "--datadir"], capture_output=True, text=True
            )
            if gdal_config.returncode == 0:
                os.environ["GDAL_DATA"] = gdal_config.stdout.strip()
        except FileNotFoundError:
            # If gdal-config not found, try common paths
            common_paths = [
                "/usr/share/gdal",
                "/usr/local/share/gdal",
                os.path.join(sys.prefix, "share", "gdal"),
                os.path.join(sys.prefix, "Library", "share", "gdal"),
            ]
            for path in common_paths:
                if os.path.exists(path):
                    os.environ["GDAL_DATA"] = path
                    break

        # Force GDAL to use EPSG registry over GeoTIFF keys
        os.environ["GDAL_GEOTIFF_SRS_SOURCE"] = "EPSG"
        os.environ["GTIFF_SRS_SOURCE"] = "EPSG"

        # Suppress PROJ warnings
        os.environ["PROJ_DEBUG"] = "0"
        os.environ["CPL_LOG"] = "/dev/null"  # Suppress GDAL logs

        print(f"✓ PROJ data directory: {proj_data_dir}")
        print(f"✓ GDAL data directory: {os.environ.get('GDAL_DATA', 'Not set')}")

    except Exception as e:
        print(f"⚠ Warning: Could not fully fix PROJ environment: {e}")


# Apply the fix immediately
fix_proj_environment()

# Now import all required modules
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import mapping
from osgeo import gdal, ogr, osr

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
# Constants
# ---------------------------------------------------------------------------
LOCAL_OUTPUT_BASE_DIR = str(PROJECT_ROOT / "data/fabdem/fabdem_local")
TERRAIN_RASTER_PATH = str(PROJECT_ROOT / "data/fabdem/fabdem_pan_india.tif")
GEOSERVER_STYLE = "Terrain_Style_11_Classes"
GEOSERVER_WORKSPACE = "digital_elevation_model"
ZERO_NODATA = -9999  # FABDEM nodata — 0 is valid elevation (sea level)


# ---------------------------------------------------------------------------
# Helper: Validate raster file before GeoServer upload
# ---------------------------------------------------------------------------
def validate_raster_file(file_path):
    """Validate raster file is readable and has correct format"""
    if not os.path.exists(file_path):
        return False, f"File does not exist: {file_path}"

    if not os.access(file_path, os.R_OK):
        return False, f"File not readable: {file_path}"

    if os.path.getsize(file_path) == 0:
        return False, f"File is empty: {file_path}"

    # Try to open with rasterio to validate
    try:
        with rasterio.open(file_path) as src:
            print(f"✓ Raster valid: {src.width}x{src.height}, CRS: {src.crs}")
            return True, "Raster is valid"
    except Exception as e:
        return False, f"Cannot read raster with rasterio: {e}"


# ---------------------------------------------------------------------------
# Improved clip helper with better PROJ handling
# ---------------------------------------------------------------------------
def _clip_fabdem_with_roi(roi_gdf, output_path):
    """
    Clips pan-India FABDEM raster to ROI with improved PROJ compatibility.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Create temporary file for initial clip
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        temp_clip_path = tmp.name

    try:
        # Step 1: Clip with rasterio
        with rasterio.open(TERRAIN_RASTER_PATH) as src:
            raster_crs = src.crs

            # Reproject ROI to raster CRS if needed
            if roi_gdf.crs != raster_crs:
                print(f"Reprojecting ROI from {roi_gdf.crs} to {raster_crs}")
                roi_in_raster_crs = roi_gdf.to_crs(raster_crs)
            else:
                roi_in_raster_crs = roi_gdf

            roi_union = get_union_geometry(roi_in_raster_crs)
            if roi_union is None or roi_union.is_empty:
                raise ValueError("ROI union geometry is empty — cannot clip FABDEM.")

            roi_shape = mapping(roi_union)

            # Perform the clip
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
                }
            )

        # Write to temporary file
        with rasterio.open(temp_clip_path, "w", **out_meta) as dst:
            dst.write(clipped_array)

        print(f"✓ Initial clip written to temp file: {temp_clip_path}")

        # Step 2: Use GDAL Warp to ensure compatibility
        print("Reprocessing with GDAL for GeoServer compatibility...")

        # Get the CRS from the clipped raster
        with rasterio.open(temp_clip_path) as src:
            src_crs = src.crs

        # Create Warp options
        warp_options = gdal.WarpOptions(
            format="GTiff",
            creationOptions=[
                "COMPRESS=LZW",
                "TILED=YES",
                "BIGTIFF=IF_SAFER",
                "BLOCKXSIZE=256",
                "BLOCKYSIZE=256",
            ],
            dstSRS=src_crs.to_wkt() if src_crs else "EPSG:4326",
            resampleAlg="bilinear",
            options=["-co", "GDAL_GEOTIFF_SRS_SOURCE=EPSG"],
        )

        # Reprocess the file
        gdal.Warp(output_path, temp_clip_path, **warp_options)

        # Verify the output
        valid, msg = validate_raster_file(output_path)
        if not valid:
            raise RuntimeError(f"Output raster validation failed: {msg}")

        print(f"✓ Local clipped FABDEM raster written to: {output_path}")
        return str(output_path)

    except Exception as e:
        print(f"✗ Error in _clip_fabdem_with_roi: {e}")
        raise

    finally:
        # Clean up temp file
        if os.path.exists(temp_clip_path):
            os.unlink(temp_clip_path)


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
    try:
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

        # Clip the raster
        clipped_raster_path = _clip_fabdem_with_roi(
            roi_gdf=roi_gdf,
            output_path=str(output_raster_path),
        )

        if push_to_geoserver:
            try:
                from utilities.geoserver_utils import Geoserver

                # Validate raster before upload
                valid, msg = validate_raster_file(clipped_raster_path)
                if not valid:
                    raise RuntimeError(f"Raster validation failed: {msg}")

                # Step 1 — Delete stale store from any workspace
                geo = Geoserver()
                for ws in ("ne", GEOSERVER_WORKSPACE):
                    try:
                        geo.delete_raster_store(layer_name, workspace=ws)
                        print(
                            f"✓ Deleted stale raster store '{layer_name}' from workspace '{ws}'"
                        )
                    except Exception as e:
                        print(f"Note: Could not delete from {ws}: {e}")

                # Step 2 — Upload raster → creates coveragestore
                upload_res, style_res = push_local_raster_to_geoserver(
                    file_path=clipped_raster_path,
                    layer_name=layer_name,
                    workspace=GEOSERVER_WORKSPACE,
                    style_name=GEOSERVER_STYLE,
                )
                print(f"✓ GeoServer upload response: {upload_res}")
                print(f"✓ GeoServer style response: {style_res}")

                # Step 3 — Publish coverage as layer
                try:
                    geo.publish_layer(
                        layer_name=layer_name,
                        workspace=GEOSERVER_WORKSPACE,
                        store_name=layer_name,
                        store_type="coverageStore",
                    )
                    print(f"✓ Published raster layer '{layer_name}' to Layer Preview.")
                except Exception as publish_err:
                    print(f"⚠ publish_layer warning: {publish_err}")

                # Step 4 — Apply style
                try:
                    geo.publish_style(
                        layer_name=layer_name,
                        style_name=GEOSERVER_STYLE,
                        workspace=GEOSERVER_WORKSPACE,
                    )
                    print(f"✓ Style '{GEOSERVER_STYLE}' applied to '{layer_name}'.")
                except Exception as style_err:
                    print(f"⚠ publish_style warning: {style_err}")

            except Exception as error:
                print(f"✗ Failed to sync local FABDEM raster to GeoServer: {error}")
                return False, None

        # Update metadata if needed
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
                print("✓ Sync to GeoServer flag updated")

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
                    print(f"⚠ STAC generation warning: {stac_err}")

        return True, clipped_raster_path

    except Exception as e:
        print(f"✗ Error in run_raster_fabdem_local: {e}")
        import traceback

        traceback.print_exc()
        return False, None


# ---------------------------------------------------------------------------
# Helper: compute watershed DEM stats
# ---------------------------------------------------------------------------
def _compute_watershed_dem_stats(watersheds_gdf, raster_path):
    """
    Compute zonal statistics for each watershed polygon.
    """
    from rasterstats import zonal_stats

    # Validate raster
    valid, msg = validate_raster_file(raster_path)
    if not valid:
        raise RuntimeError(f"Raster validation failed: {msg}")

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        pixel_area_ha = (abs(src.res[0]) * abs(src.res[1])) / 10000.0

    # Reproject watersheds to raster CRS for accurate zonal stats
    if watersheds_gdf.crs != raster_crs:
        print(f"Reprojecting watersheds from {watersheds_gdf.crs} to {raster_crs}")
        watersheds_for_stats = watersheds_gdf.to_crs(raster_crs)
    else:
        watersheds_for_stats = watersheds_gdf

    # Compute zonal statistics
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
    final_gdf = result_gdf[[c for c in keep_cols if c in result_gdf.columns]]

    print(f"✓ Computed DEM stats for {len(final_gdf)} watersheds")
    return final_gdf


# ---------------------------------------------------------------------------
# Stage 2 — Vector
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
    Compute vector statistics (zonal stats per watershed).
    """
    try:
        if not raster_path:
            raise ValueError(
                "`raster_path` is required for vector stage — pass Stage 1 output."
            )

        # Validate raster
        valid, msg = validate_raster_file(raster_path)
        if not valid:
            raise RuntimeError(f"Raster validation failed: {msg}")

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
            print(f"✓ Watershed boundary source: {watershed_source}")
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
            print(f"✓ Watershed boundary source: {watershed_source}")

        # Compute statistics
        result_gdf = _compute_watershed_dem_stats(watersheds_gdf, raster_path)

        # Save to file
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
        print(f"✓ Saved local DEM vector: {asset_id}")

        # Push to GeoServer if requested
        if push_to_geoserver:
            try:
                geoserver_response = push_shape_to_geoserver(
                    os.path.splitext(asset_id)[0],
                    workspace=GEOSERVER_WORKSPACE,
                    layer_name=layer_name,
                    file_type="gpkg",
                )
                print(f"✓ GeoServer vector response: {geoserver_response}")
                if not isinstance(geoserver_response, dict) or geoserver_response.get(
                    "status_code"
                ) not in (200, 201):
                    print("⚠ Warning: GeoServer response indicates failure")
                    return False
            except Exception as error:
                print(f"✗ Failed to sync local FABDEM vector to GeoServer: {error}")
                return False

        # Update metadata
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
                print("✓ Sync to GeoServer flag updated for DEM vector")

        return True

    except Exception as e:
        print(f"✗ Error in run_vector_fabdem_local: {e}")
        import traceback

        traceback.print_exc()
        return False


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
    print("=" * 60)
    print("Starting local FABDEM pipeline")
    print("=" * 60)

    # Stage 1: Raster
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
        print("✗ Raster stage failed — skipping vector stage.")
        return False

    print(f"✓ Raster stage complete: {clipped_raster_path}")

    # Stage 2: Vector
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

    if vector_ok:
        print("=" * 60)
        print("✓ Local FABDEM pipeline completed successfully")
        print("=" * 60)
    else:
        print("✗ Vector stage failed")

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
# Celery task
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
