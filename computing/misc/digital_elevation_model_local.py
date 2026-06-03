import os
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from utilities.gee_utils import valid_gee_text
import pyproj

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
    os.environ["PROJ_DATA"] = pyproj.datadir.get_data_dir()
    os.environ["PROJ_LIB"] = pyproj.datadir.get_data_dir()
except Exception:
    pass

from computing.config_loader import (
    PAN_INDIA_FABDEM_PATH,
    LOCAL_FABDEM_OUTPUT,
)

GEOSERVER_STYLE = None
GEOSERVER_WORKSPACE = "dem"
ZERO_NODATA = -9999

def _clip_fabdem_with_roi(roi_gdf, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with rasterio.open(PAN_INDIA_FABDEM_PATH) as src:
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
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_dem_raster"
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
        output_base_dir=LOCAL_FABDEM_OUTPUT,
        state=state,
        district=district,
        block=block,
    )

    clipped_raster_path = _clip_fabdem_with_roi(
        roi_gdf=roi_gdf,
        output_path=str(output_raster_path),
    )

    # if push_to_geoserver:
    #     try:
    #         upload_res, style_res = push_local_raster_to_geoserver(
    #             file_path=clipped_raster_path,
    #             layer_name=layer_name,
    #             workspace=GEOSERVER_WORKSPACE,
    #             style_name=GEOSERVER_STYLE,
    #         )
    #         print(f"GeoServer upload response: {upload_res}")
    #         print(f"GeoServer style  response: {style_res}")
    #     except Exception as error:
    #         print(f"Failed to sync local FABDEM raster to GeoServer: {error}")
    #         return False, None

    if sync_layer_metadata and state and district and block:
        from computing.STAC_specs import generate_STAC_layerwise
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=clipped_raster_path,
            dataset_name="DEM Raster",
            algorithm="FABDEM",
            algorithm_version="1.0",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated")

    return True, clipped_raster_path


def _compute_watershed_dem_stats(watersheds_gdf, raster_path):
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

    keep_cols = [
        "uid",
        "min_elevation",
        "max_elevation",
        "mean_elevation",
        "geometry",
    ]
    return result_gdf[[c for c in keep_cols if c in result_gdf.columns]]


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
    if not raster_path:
        raise ValueError(
            "`raster_path` is required for vector stage — pass Stage 1 output."
        )

    if state and district and block:
        layer_name = f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_dem_vector"
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
        output_base_dir=LOCAL_FABDEM_OUTPUT,
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

    # if push_to_geoserver:
    #     try:
    #         geoserver_response = push_shape_to_geoserver(
    #             os.path.splitext(asset_id)[0],
    #             workspace=GEOSERVER_WORKSPACE,
    #             layer_name=layer_name,
    #             file_type="gpkg",
    #         )
    #         print(f"GeoServer vector response: {geoserver_response}")
    #         if not isinstance(geoserver_response, dict) or geoserver_response.get(
    #             "status_code"
    #         ) not in (200, 201):
    #             return False
    #     except Exception as error:
    #         print(f"Failed to sync local FABDEM vector to GeoServer: {error}")
    #         return False

    if sync_layer_metadata and state and district and block:
        from computing.utils import save_layer_info_to_db, update_layer_sync_status

        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="DEM Vector",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for DEM vector")

    return True


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
    raster_ok, clipped_raster_path = run_raster_fabdem_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        precomputed_roi_dir=precomputed_roi_dir,
        push_to_geoserver=True,
        sync_layer_metadata=True,
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
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )

    return raster_ok and vector_ok
