import os
from pathlib import Path
import logging

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from shapely.geometry import mapping

from computing.STAC_specs import generate_STAC_layerwise
from computing.config_loader import LULC_BASE_DIR
from computing.soil_health.soil_health_helper import nutrient_stats_for_geometries
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    PROJECT_ROOT,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    build_output_raster_path,
    compute_mode_lulc_array,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
    push_local_vector_to_geoserver,
    push_local_raster_to_geoserver,
    load_precomputed_roi,
)

from nrm_app.celery import app

logger = logging.getLogger(__name__)

LOCAL_OUTPUT_BASE_DIR = "data/soil_health"
GEOSERVER_STYLE = ""
GEOSERVER_RASTER_WORKSPACE = "soil_health_raster"
GEOSERVER_VECTOR_WORKSPACE = "soil_health_vector"
NUTRIENTS = ["N", "K", "P", "OC", "OLM"]
NUTRIENT_PERCENTILES = (5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95)
LOCAL_ALGORITHM = "local_soil_health"
LOCAL_ALGORITHM_VERSION = "local-1.0"


def _get_lulc_mask_classes(nutrient):
    nutrient = str(nutrient).strip().upper()
    if nutrient == "OLM":
        return {6, 12}
    if nutrient in {
        "N",
        "P",
        "K",
        "OC",
    }:
        return {8, 9, 10, 11}
    raise ValueError(f"Unsupported nutrient for LULC masking: {nutrient}")


def _pick_output_nodata(dtype, source_nodata):
    if source_nodata is not None:
        source_nodata = float(source_nodata)
        if not np.isnan(source_nodata):
            return source_nodata

    dtype = np.dtype(dtype)
    if np.issubdtype(dtype, np.floating):
        return -9999.0

    info = np.iinfo(dtype)
    if np.issubdtype(dtype, np.signedinteger):
        return info.min
    return info.max


def _resolve_latest_lulc_raster_paths(count=3, lulc_dir=LULC_BASE_DIR):
    lulc_dir = Path(lulc_dir)
    if not lulc_dir.exists():
        return []

    candidates = sorted(lulc_dir.glob("lulc_v3_*.tif"))
    if not candidates:
        return []
    selected = candidates[-count:] if len(candidates) >= count else candidates
    return [str(path) for path in selected]


def _clip_and_mask_soil_health_raster(
    roi_gdf,
    soil_raster_path,
    output_path,
    nutrient,
    lulc_paths=None,
):
    with rasterio.open(soil_raster_path) as soil_src:
        roi_gdf = validate_geometry(roi_gdf)
        if roi_gdf.empty:
            raise ValueError(
                "No valid ROI geometry available for soil health clipping."
            )
        if roi_gdf.crs is None:
            raise ValueError(
                "ROI CRS is missing; cannot align with soil health raster."
            )
        if soil_src.crs and roi_gdf.crs != soil_src.crs:
            roi_gdf = roi_gdf.to_crs(soil_src.crs)

        shapes = [
            mapping(geom)
            for geom in roi_gdf.geometry
            if geom is not None and not geom.is_empty
        ]
        if not shapes:
            raise ValueError(
                "No valid ROI geometry available for soil health clipping."
            )

        band_index = 1
        nodata = _pick_output_nodata(
            dtype=soil_src.dtypes[band_index - 1],
            source_nodata=soil_src.nodata,
        )
        clipped_data, clipped_transform = mask(
            soil_src,
            shapes=shapes,
            crop=True,
            filled=True,
            nodata=nodata,
            indexes=band_index,
        )
        if clipped_data.ndim == 3:
            clipped_data = clipped_data[0]

        clipped_meta = soil_src.meta.copy()
        clipped_meta.update(
            {
                "driver": "GTiff",
                "height": clipped_data.shape[0],
                "width": clipped_data.shape[1],
                "transform": clipped_transform,
                "count": 1,
                "dtype": clipped_data.dtype,
                "nodata": nodata,
                "compress": "lzw",
            }
        )

    if lulc_paths:
        reprojected_arrays = []
        for lulc_path in lulc_paths:
            lulc_array = np.zeros(
                (clipped_meta["height"], clipped_meta["width"]),
                dtype=np.float32,
            )
            with rasterio.open(lulc_path) as lulc_src:
                reproject(
                    source=rasterio.band(lulc_src, 1),
                    destination=lulc_array,
                    src_transform=lulc_src.transform,
                    src_crs=lulc_src.crs,
                    src_nodata=lulc_src.nodata,
                    dst_transform=clipped_meta["transform"],
                    dst_crs=clipped_meta["crs"],
                    dst_nodata=0,
                    resampling=Resampling.mode,
                )
            reprojected_arrays.append(lulc_array)

        lulc_mode_array = compute_mode_lulc_array(reprojected_arrays)
        allowed_mask_classes = _get_lulc_mask_classes(nutrient)
        valid_pixels = np.isin(lulc_mode_array, list(allowed_mask_classes))
        valid_soil_pixels = clipped_data != nodata
        output_array = np.where(valid_pixels & valid_soil_pixels, clipped_data, nodata)
    else:
        output_array = clipped_data

    with rasterio.open(output_path, "w", **clipped_meta) as dst:
        dst.write(output_array.astype(clipped_meta["dtype"], copy=False), 1)

    return str(output_path)


def clip_soil_health_raster(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):

    asset_suffix, roi_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    layer_name = f"{asset_suffix}_soil_health_raster"
    lulc_paths = _resolve_latest_lulc_raster_paths()
    geoserver_statuses = []
    for nutrient in NUTRIENTS:
        SOIL_MAP_PATH = str(
            PROJECT_ROOT / f"data/base_layers/soil_health/soil_health_{nutrient}.tif"
        )
        output_raster_path = build_output_raster_path(
            layer_name=f"{layer_name}_{nutrient}",
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
        )

        asset_id = _clip_and_mask_soil_health_raster(
            roi_gdf=roi_gdf,
            soil_raster_path=SOIL_MAP_PATH,
            output_path=output_raster_path,
            nutrient=nutrient,
            lulc_paths=lulc_paths,
        )

        if push_to_geoserver:
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=output_raster_path,
                layer_name=f"{layer_name}_{nutrient}",
                workspace=GEOSERVER_RASTER_WORKSPACE,
            )
            print(f"GeoServer upload response for {nutrient}: {upload_res}")
            geoserver_statuses.append(True)

        if sync_layer_metadata:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=layer_name,
                asset_id=asset_id,
                dataset_name="Soil Health Raster",
                misc={"is_generated_locally": True},
                algorithm=LOCAL_ALGORITHM,
                algorithm_version=LOCAL_ALGORITHM_VERSION,
            )
            logger.info("Saved layer metadata to DB: layer_id=%s", layer_id)
            if layer_id:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("Sync to GeoServer flag updated for Soil health raster")

                try:
                    layer_STAC_generated = generate_STAC_layerwise.generate_raster_stac(
                        state=state,
                        district=district,
                        block=block,
                        layer_name=layer_name,
                    )
                    update_layer_sync_status(
                        layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
                    )
                    print("STAC metadata updated for Soil health raster")
                except Exception as e:
                    print(f"Error generating STAC: {e}")

    return all(geoserver_statuses) if push_to_geoserver else True


def get_roi(asset_suffix, block, district, roi, state, precomputed_roi_dir):
    if state and district and block:
        asset_suffix = f"{valid_gee_text(str(district).lower())}_{valid_gee_text(str(block).lower())}"
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

        roi_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
    roi_gdf = validate_geometry(roi_gdf)
    return asset_suffix, roi_gdf


def vectorize_soil_health(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):

    asset_suffix, roi_gdf = get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    )
    layer_name = f"{asset_suffix}_soil_health"
    geoserver_statuses = []
    for nutrient in NUTRIENTS:
        # This produces one output feature per ROI geometry with Nitrogen summary columns.
        raster_path = build_output_raster_path(
            layer_name=f"{layer_name}_raster_{nutrient}",
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
        )

        result_gdf = nutrient_stats_for_geometries(
            roi_gdf=roi_gdf,
            raster_path=raster_path,
            percentiles=tuple(NUTRIENT_PERCENTILES),
            nutrient=nutrient,
        )
        output_layer_name = f"{layer_name}_vector_{nutrient}"
        output_path = build_output_vector_path(
            layer_name=output_layer_name,
            state=state,
            district=district,
            block=block,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        )
        asset_id = write_vector_output(
            gdf=result_gdf,
            output_path=output_path,
            layer_name=output_layer_name,
        )
        print(f"Saved soil health vector: {output_path}")

        if push_to_geoserver:
            geoserver_response = push_local_vector_to_geoserver(
                path=os.path.splitext(output_path)[0],
                layer_name=output_layer_name,
                workspace=GEOSERVER_VECTOR_WORKSPACE,
                file_type="gpkg",
            )
            print(f"GeoServer response for {nutrient}: {geoserver_response}")

            if not isinstance(geoserver_response, dict) or geoserver_response.get(
                "status_code"
            ) not in (200, 201):
                return False
            geoserver_statuses.append(True)

        if sync_layer_metadata:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=output_layer_name,
                asset_id=asset_id,
                dataset_name="Soil Health Vector",
                misc={"is_generated_locally": True},
                algorithm=LOCAL_ALGORITHM,
                algorithm_version=LOCAL_ALGORITHM_VERSION,
            )
            logger.info("Saved layer metadata to DB: layer_id=%s", layer_id)
            if layer_id and push_to_geoserver:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                print("Sync to GeoServer flag updated for Soil health vector")

                try:
                    layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                        state=state,
                        district=district,
                        block=block,
                        layer_name="soil_health_vector",
                    )
                    update_layer_sync_status(
                        layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
                    )
                    print("STAC metadata updated for Soil health vector")
                except Exception as e:
                    print(f"Error generating STAC: {e}")

    return all(geoserver_statuses) if push_to_geoserver else True


@app.task(bind=True)
def soil_health_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    clip_soil_health_raster(
        state,
        district,
        block,
        asset_suffix,
        roi,
        precomputed_roi_dir,
        push_to_geoserver,
        sync_layer_metadata,
    )

    vectorize_soil_health(
        state,
        district,
        block,
        asset_suffix,
        roi,
        push_to_geoserver,
        sync_layer_metadata,
    )
