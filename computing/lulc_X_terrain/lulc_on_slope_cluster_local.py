import os

import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.local_compute_helper import (
    AEZ_VECTOR_PATH,
    LULC_BASE_DIR,
    MIN_WATERSHED_AREA_HA,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    TERRAIN_RASTER_PATH,
    build_output_vector_path as _build_output_vector_path,
    compute_mode_lulc_array as _compute_mode_lulc_array,
    compute_terrain_properties_for_watersheds,
    ensure_file_exists as _ensure_file_exists,
    filter_large_watersheds as _filter_large_watersheds,
    load_precomputed_watersheds as _load_precomputed_watersheds,
    resolve_aez_code as _resolve_aez_code,
    resolve_lulc_raster_paths as _resolve_lulc_raster_paths,
    validate_geometry as _validate_geometry,
    write_vector_output as _write_output_vector,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

from .utils import aez_lulcXterrain_cluster_centroids


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/lulc_X_terrain/lulc_slope_clusters_local"
GEOSERVER_WORKSPACE = "terrain_lulc"
SLOPE_LULC_FIELD_MAPPING = {
    "barren": 7,
    "double": 10,
    "shrub_scrub": 12,
    "sing_kharif": 8,
    "sing_non_kharif": 9,
    "forests": 6,
    "triple": 11,
}


def _compute_slope_lulc_properties_for_feature(geometry, terrain_src, lulc_sources):
    try:
        terrain_data, terrain_transform = mask(
            terrain_src,
            [geometry.__geo_interface__],
            crop=True,
            filled=True,
        )
    except ValueError:
        return None

    terrain_values = np.rint(terrain_data[0]).astype(np.int16, copy=False)
    if terrain_values.size == 0:
        return None

    terrain_valid_mask = np.ones(terrain_values.shape, dtype=bool)
    if terrain_src.nodata is not None:
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
            "LxS_cluster": -1,
            "clust_name": "No plain or slope pixels",
            "barren": 0.0,
            "double": 0.0,
            "shrub_scrub": 0.0,
            "sing_kharif": 0.0,
            "sing_non_kharif": 0.0,
            "forests": 0.0,
            "triple": 0.0,
        }

    proportions = {}
    for field_name, lulc_class in SLOPE_LULC_FIELD_MAPPING.items():
        class_pixels = int((slopy_mask & (lulc_mode == lulc_class)).sum())
        proportions[field_name] = class_pixels / float(plain_plus_slope_pixels)

    return proportions


def _assign_slope_clusters(
    slope_watersheds_gdf,
    terrain_raster_path,
    slope_centroids,
    lulc_raster_paths,
):
    centroid_vectors = np.array(
        [
            slope_centroids[str(index)]["cluster_vector"]
            for index in range(len(slope_centroids))
        ],
        dtype=np.float64,
    )

    computed_rows = []
    with rasterio.open(terrain_raster_path) as terrain_src:
        lulc_sources = [rasterio.open(path) for path in lulc_raster_paths]
        try:
            total = len(slope_watersheds_gdf)
            for index, row in enumerate(
                slope_watersheds_gdf.itertuples(index=False),
                start=1,
            ):
                properties = _compute_slope_lulc_properties_for_feature(
                    geometry=row.geometry,
                    terrain_src=terrain_src,
                    lulc_sources=lulc_sources,
                )

                if properties is None:
                    computed_rows.append(
                        {
                            "LxS_cluster": -1,
                            "clust_name": "No valid terrain pixels",
                            "barren": 0.0,
                            "double": 0.0,
                            "shrub_scrub": 0.0,
                            "sing_kharif": 0.0,
                            "sing_non_kharif": 0.0,
                            "forests": 0.0,
                            "triple": 0.0,
                        }
                    )
                elif properties.get("LxS_cluster") == -1:
                    computed_rows.append(properties)
                else:
                    feature_vector = np.array(
                        [
                            properties["barren"],
                            properties["shrub_scrub"],
                            properties["forests"],
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
                            "LxS_cluster": cluster_index,
                            "clust_name": slope_centroids[str(cluster_index)][
                                "cluster_name"
                            ],
                            "barren": properties["barren"] * 100.0,
                            "double": properties["double"] * 100.0,
                            "shrub_scrub": properties["shrub_scrub"] * 100.0,
                            "sing_kharif": properties["sing_kharif"] * 100.0,
                            "sing_non_kharif": (
                                properties["sing_non_kharif"] * 100.0
                            ),
                            "forests": properties["forests"] * 100.0,
                            "triple": properties["triple"] * 100.0,
                        }
                    )

                if index % 200 == 0 or index == total:
                    print(
                        f"Computed slope LULC clusters for {index}/{total} watersheds"
                    )
        finally:
            for lulc_src in lulc_sources:
                lulc_src.close()

    result = slope_watersheds_gdf.copy()
    computed_df = pd.DataFrame(computed_rows)
    for column in computed_df.columns:
        result[column] = computed_df[column].values
    return result


def run_lulc_on_slope_cluster_local(
    state,
    district,
    block,
    start_year,
    end_year,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    aez_vector_path=AEZ_VECTOR_PATH,
    lulc_dir=LULC_BASE_DIR,
    terrain_raster_path=TERRAIN_RASTER_PATH,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
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

    watersheds_gdf, watershed_source = _load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    watersheds_gdf = _validate_geometry(watersheds_gdf)
    if watersheds_gdf.empty:
        raise ValueError("No valid watershed geometries found for local processing.")

    aez_code = _resolve_aez_code(
        watersheds_gdf,
        aez_vector_path=aez_vector_path,
    )
    slope_centroids = aez_lulcXterrain_cluster_centroids[f"aez{aez_code}"]["slopes"]

    filtered_watersheds = _filter_large_watersheds(watersheds_gdf)
    if filtered_watersheds.empty:
        raise ValueError(
            f"No watersheds larger than {MIN_WATERSHED_AREA_HA} ha found for {state}/{district}/{block}."
        )

    terrain_classified = compute_terrain_properties_for_watersheds(
        watersheds_gdf=filtered_watersheds,
        raster_path=str(terrain_raster_path),
    )
    terrain_classified["terrain_cluster"] = terrain_classified[
        "terrainClusters"
    ].astype(int)
    slope_watersheds = terrain_classified.loc[
        terrain_classified["terrain_cluster"].isin([0, 3])
    ].copy()
    if slope_watersheds.empty:
        raise ValueError(
            f"No slope-cluster watersheds found for {state}/{district}/{block}."
        )

    temp_columns = [
        "terrainClusters",
        "plain_area",
        "valley_area",
        "hill_slopes_area",
        "ridge_area",
        "slopy_area",
    ]
    slope_watersheds.drop(
        columns=[
            column
            for column in temp_columns
            if column in slope_watersheds.columns
        ],
        inplace=True,
    )

    result_gdf = _assign_slope_clusters(
        slope_watersheds_gdf=slope_watersheds,
        terrain_raster_path=str(terrain_raster_path),
        slope_centroids=slope_centroids,
        lulc_raster_paths=lulc_raster_paths,
    )

    layer_name = (
        f"{valid_gee_text(str(district).strip().lower())}_"
        f"{valid_gee_text(str(block).strip().lower())}_lulc_slope"
    )
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
    print(f"Saved local slope-cluster vector: {asset_id}")
    print(f"Watershed boundary source: {watershed_source}")
    print(f"Resolved AEZ code: {aez_code}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            str(output_path.with_suffix("")),
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")

        if not isinstance(geoserver_response, dict) or geoserver_response.get(
            "status_code"
        ) not in (200, 201):
            return False

    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Terrain LULC",
            misc={"start_year": start_year, "end_year": end_year},
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=True,
            )

    return True


def _generate_lulc_on_slope_cluster_local_task(
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_lulc_on_slope_cluster_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def lulc_on_slope_cluster_local(
    self,
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = self
    return _generate_lulc_on_slope_cluster_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )
