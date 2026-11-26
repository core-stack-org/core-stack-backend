import logging
import os
from contextlib import ExitStack

import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.config_loader import (
    LULC_PLAIN_CLUSTER_OUTPUT_DIR as LOCAL_OUTPUT_BASE_DIR,
)
from computing.config_loader import TERRAIN_RASTER_PATH
from computing.local_compute_helper import (
    AEZ_VECTOR_PATH,
    LULC_BASE_DIR,
    MIN_WATERSHED_AREA_HA,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path as _build_output_vector_path,
    compute_mode_lulc_array as _compute_mode_lulc_array,
    compute_terrain_properties_for_watersheds,
    ensure_file_exists as _ensure_file_exists,
    filter_large_watersheds as _filter_large_watersheds,
    load_precomputed_watersheds as _load_precomputed_watersheds,
    queue_local_vector_for_geoserver,
    get_roi as _get_roi,
    resolve_aez_code as _resolve_aez_code,
    resolve_lulc_raster_paths as _resolve_lulc_raster_paths,
    validate_geometry as _validate_geometry,
    write_vector_output as _write_output_vector,
)
from computing.utils import save_layer_info_to_db

from .utils import aez_lulcXterrain_cluster_centroids

logger = logging.getLogger(__name__)

GEOSERVER_WORKSPACE = "terrain_lulc"

PLAIN_LULC_FIELD_MAPPING = {
    "barren": 7,
    "double_crop": 10,
    "shrubs_scrubs": 12,
    "sing_crop": 8,
    "sing_non_kharif_crop": 9,
    "forest": [6, 13],
    "triple_crop": 11,
}


def _compute_plain_lulc_properties_for_feature(geometry, terrain_src, lulc_sources):
    try:
        terrain_data, terrain_transform = mask(
            terrain_src,
            [geometry.__geo_interface__],
            crop=True,
            filled=False,
        )
    except ValueError:
        return None

    band = terrain_data[0]
    if band.size == 0:
        return None

    raw_terrain = np.asarray(band, dtype=np.float64)
    finite_mask = np.isfinite(raw_terrain)
    terrain_values = np.rint(np.where(finite_mask, raw_terrain, 0)).astype(
        np.int16, copy=False
    )

    terrain_valid_mask = (~np.ma.getmaskarray(band)) & finite_mask
    if terrain_src.nodata is not None and not np.isnan(terrain_src.nodata):
        terrain_valid_mask &= terrain_values != int(round(terrain_src.nodata))
    terrain_valid_mask &= terrain_values > 0

    if not terrain_valid_mask.any():
        return None

    reprojected_lulc = []
    for lulc_src in lulc_sources:
        destination = np.zeros(terrain_values.shape, dtype=np.float32)
        reproject(
            source=rasterio.band(lulc_src, 1),
            destination=destination,
            src_transform=lulc_src.transform,
            src_crs=lulc_src.crs,
            src_nodata=lulc_src.nodata,
            dst_transform=terrain_transform,
            dst_crs=terrain_src.crs,
            dst_nodata=0,
            resampling=Resampling.nearest,
        )
        reprojected_lulc.append(destination)

    lulc_mode = _compute_mode_lulc_array(reprojected_lulc)
    plains_mask = terrain_valid_mask & (terrain_values == 5)
    slopy_mask = terrain_valid_mask & (terrain_values == 6)
    plain_plus_slope_pixels = int((plains_mask | slopy_mask).sum())

    if plain_plus_slope_pixels == 0:
        return {
            "LxP_cluster": -1,
            "clust_name": "No plain or slope pixels",
            "barren": 0.0,
            "double_crop": 0.0,
            "shrubs_scrubs": 0.0,
            "sing_crop": 0.0,
            "sing_non_kharif_crop": 0.0,
            "forest": 0.0,
            "triple_crop": 0.0,
        }

    proportions = {}
    for field_name, lulc_class in PLAIN_LULC_FIELD_MAPPING.items():
        if isinstance(lulc_class, list):
            class_pixels = int((plains_mask & np.isin(lulc_mode, lulc_class)).sum())
        else:
            class_pixels = int((plains_mask & (lulc_mode == lulc_class)).sum())
        proportions[field_name] = class_pixels / float(plain_plus_slope_pixels)

    return proportions


def _assign_plain_clusters(
    plain_watersheds_gdf,
    terrain_raster_path,
    plain_centroids,
    lulc_raster_paths,
):
    centroid_vectors = np.array(
        [
            plain_centroids[str(index)]["cluster_vector"]
            for index in range(len(plain_centroids))
        ],
        dtype=np.float64,
    )

    computed_rows = []
    with rasterio.open(terrain_raster_path) as terrain_src, ExitStack() as stack:
        working_gdf = plain_watersheds_gdf
        if working_gdf.crs is None:
            raise ValueError(
                "Watershed CRS is missing; cannot align with terrain raster."
            )
        if terrain_src.crs and working_gdf.crs != terrain_src.crs:
            working_gdf = working_gdf.to_crs(terrain_src.crs)

        lulc_sources = [
            stack.enter_context(rasterio.open(path)) for path in lulc_raster_paths
        ]
        total = len(working_gdf)

        for index, row in enumerate(
            working_gdf.itertuples(index=False),
            start=1,
        ):
            properties = _compute_plain_lulc_properties_for_feature(
                geometry=row.geometry,
                terrain_src=terrain_src,
                lulc_sources=lulc_sources,
            )

            if properties is None:
                computed_rows.append(
                    {
                        "LxP_cluster": -1,
                        "clust_name": "No valid terrain pixels",
                        "barren": 0.0,
                        "double_crop": 0.0,
                        "shrubs_scrubs": 0.0,
                        "sing_crop": 0.0,
                        "sing_non_kharif_crop": 0.0,
                        "forest": 0.0,
                        "triple_crop": 0.0,
                    }
                )
            elif properties.get("LxP_cluster") == -1:
                computed_rows.append(properties)
            else:
                feature_vector = np.array(
                    [
                        properties["barren"],
                        properties["double_crop"],
                        properties["shrubs_scrubs"],
                        properties["sing_crop"],
                        properties["sing_non_kharif_crop"],
                        properties["forest"],
                        properties["triple_crop"],
                    ],
                    dtype=np.float64,
                )
                distances = np.sum(
                    (centroid_vectors - feature_vector) ** 2,
                    axis=1,
                )
                cluster_index = int(np.argmin(distances))

                computed_rows.append(
                    {
                        "LxP_cluster": cluster_index,
                        "clust_name": plain_centroids[str(cluster_index)][
                            "cluster_name"
                        ],
                        "barren": properties["barren"] * 100.0,
                        "double_crop": properties["double_crop"] * 100.0,
                        "shrubs_scrubs": properties["shrubs_scrubs"] * 100.0,
                        "sing_crop": properties["sing_crop"] * 100.0,
                        "sing_non_kharif_crop": (
                            properties["sing_non_kharif_crop"] * 100.0
                        ),
                        "forest": properties["forest"] * 100.0,
                        "triple_crop": properties["triple_crop"] * 100.0,
                    }
                )

            if index % 200 == 0 or index == total:
                logger.info(
                    "Computed plain LULC clusters for %d/%d watersheds", index, total
                )

    result = plain_watersheds_gdf.copy()
    computed_df = pd.DataFrame(computed_rows)
    for column in computed_df.columns:
        result[column] = computed_df[column].values
    return result


def run_lulc_on_plain_cluster_local(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    aez_vector_path=AEZ_VECTOR_PATH,
    lulc_dir=LULC_BASE_DIR,
    terrain_raster_path=TERRAIN_RASTER_PATH,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    asset_suffix=None,
    roi=None,
):
    if state and district and block:
        state = str(state).strip()
        district = str(district).strip()
        block = str(block).strip()
    start_year = int(start_year)
    end_year = int(end_year)

    if start_year > end_year:
        raise ValueError("start_year cannot be greater than end_year")

    _ensure_file_exists(terrain_raster_path, "Terrain raster")
    lulc_raster_paths = _resolve_lulc_raster_paths(
        start_year=start_year,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )

    # Uses precomputed tehsil ROI when state/district/block are given,
    # otherwise falls back to the provided `roi` file and `asset_suffix`.
    asset_suffix, watersheds_gdf = _get_roi(
        asset_suffix,
        block,
        district,
        roi,
        state,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    watersheds_gdf = _validate_geometry(watersheds_gdf)
    if watersheds_gdf.empty:
        raise ValueError("No valid watershed geometries found for local processing.")

    aez_code = _resolve_aez_code(
        watersheds_gdf,
        aez_vector_path=aez_vector_path,
    )
    plain_centroids = aez_lulcXterrain_cluster_centroids[f"aez{aez_code}"]["plains"]

    filtered_watersheds = _filter_large_watersheds(watersheds_gdf)
    if filtered_watersheds.empty:
        raise ValueError(
            f"No watersheds larger than {MIN_WATERSHED_AREA_HA} ha found for {asset_suffix}."
        )
    filtered_watersheds.drop(columns=["area"], inplace=True)

    terrain_classified = compute_terrain_properties_for_watersheds(
        watersheds_gdf=filtered_watersheds,
        raster_path=str(terrain_raster_path),
    )
    terrain_classified["terrain_cluster"] = terrain_classified[
        "terrainClusters"
    ].astype(int)
    plain_watersheds = terrain_classified.loc[
        terrain_classified["terrain_cluster"] != 2
    ].copy()
    if plain_watersheds.empty:
        raise ValueError(
            f"No plain-cluster watersheds found for {asset_suffix}."
        )

    temp_columns = [
        "terrainClusters",
        "plain_area",
        "valley_area",
        "hill_slopes_area",
        "ridge_area",
        "slopy_area",
    ]
    plain_watersheds.drop(
        columns=[
            column for column in temp_columns if column in plain_watersheds.columns
        ],
        inplace=True,
    )

    result_gdf = _assign_plain_clusters(
        plain_watersheds_gdf=plain_watersheds,
        terrain_raster_path=str(terrain_raster_path),
        plain_centroids=plain_centroids,
        lulc_raster_paths=lulc_raster_paths,
    )

    layer_name = f"{valid_gee_text(str(asset_suffix).strip().lower())}_lulc_plain"
    output_path = _build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        block_fallback="unknown_block",
    )
    asset_id = _write_output_vector(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    logger.info("Saved local plain-cluster vector: %s", asset_id)
    logger.info("Resolved AEZ code: %s", aez_code)

    layer_id = None
    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Terrain LULC",
            misc={
                "start_year": start_year,
                "end_year": end_year,
                "is_generated_locally": True,
            },
        )
        logger.info("Saved layer metadata to DB: layer_id=%s", layer_id)

    geoserver_ok = False
    if push_to_geoserver:
        geoserver_response = queue_local_vector_for_geoserver(
            path=os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
            layer_id=layer_id,
        )
        geoserver_ok = (
            isinstance(geoserver_response, dict)
            and geoserver_response.get("status_code") == 202
        )
        if geoserver_ok:
            logger.info("GeoServer upload queued for layer %s", layer_name)
        else:
            logger.error(
                "GeoServer queueing failed for layer %s: %s",
                layer_name,
                geoserver_response,
            )

    return geoserver_ok if push_to_geoserver else True


def _generate_lulc_on_plain_cluster_local_task(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    asset_suffix=None,
    roi=None,
):
    _ = gee_account_id
    return run_lulc_on_plain_cluster_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
        asset_suffix=asset_suffix,
        roi=roi,
    )


@app.task(bind=True)
def lulc_on_plain_cluster_local(
    self,
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    gee_account_id=None,
    asset_suffix=None,
    roi=None,
):
    _ = self
    return _generate_lulc_on_plain_cluster_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        asset_suffix=asset_suffix,
        roi=roi,
    )
