import os
import geopandas as gpd

from computing.soil_health.soil_health_helper import nutrient_stats_for_geometries
from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    PROJECT_ROOT,
    build_output_vector_path,
    clip_raster_with_roi,
    build_output_raster_path,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
    push_local_vector_to_geoserver,
    push_local_raster_to_geoserver,
)

ROI_PATH = str(PROJECT_ROOT / "data/yalburga_mws.json")
LOCAL_OUTPUT_BASE_DIR = "data/soil_health"
GEOSERVER_STYLE = ""
GEOSERVER_WORKSPACE = "soil_health"
NUTRIENTS = ["N", "K", "P", "OC"]
NUTRIENT_PERCENTILES = (5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95)


def clip_soil_health_raster(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=False,
):

    asset_suffix, roi_gdf = get_roi(asset_suffix, block, district, roi, state)
    layer_name = f"{asset_suffix}_soil_health_raster"
    geoserver_statuses = []
    for nutrient in NUTRIENTS:
        SOIL_MAP_PATH = str(
            PROJECT_ROOT
            / f"data/soil_health/AEZ_3_{nutrient}_20260311.tif"  # TODO Replace with pan India local path
        )
        output_raster_path = build_output_raster_path(
            layer_name=f"{layer_name}_{nutrient}",
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
        )

        clip_raster_with_roi(
            roi_gdf, SOIL_MAP_PATH, output_raster_path, raster_label="Raster"
        )

        if push_to_geoserver:
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=output_raster_path,
                layer_name=f"{layer_name}_{nutrient}",
                workspace=GEOSERVER_WORKSPACE,
            )
            print(f"GeoServer upload response for {nutrient}: {upload_res}")
            geoserver_statuses.append(True)

    return all(geoserver_statuses) if push_to_geoserver else True  # TODO Add Stac specs


def get_roi(asset_suffix, block, district, roi, state):
    if state and district and block:
        asset_suffix = f"{valid_gee_text(str(district).lower())}_{valid_gee_text(str(block).lower())}"
        # roi_gdf = load_precomputed_roi(
        #     state=state,
        #     district=district,
        #     block=block,
        #     precomputed_roi_dir=precomputed_roi_dir,
        # )
        roi_gdf = gpd.read_file(ROI_PATH)
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
    percentiles=NUTRIENT_PERCENTILES,
    push_to_geoserver=True,
):

    asset_suffix, roi_gdf = get_roi(asset_suffix, block, district, roi, state)
    layer_name = f"{asset_suffix}_soil_health"

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
            percentiles=tuple(percentiles),
            nutrient=nutrient,
        )

        output_path = build_output_vector_path(
            layer_name=f"{layer_name}_vector_{nutrient}",
            state=state,
            district=district,
            block=block,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        )
        write_vector_output(
            gdf=result_gdf,
            output_path=output_path,
            layer_name=layer_name,
        )
        print(f"Saved soil health vector: {output_path}")

        if push_to_geoserver:
            geoserver_response = push_local_vector_to_geoserver(
                path=os.path.splitext(output_path)[0],
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                file_type="gpkg",
            )
            print(f"GeoServer response: {geoserver_response}")

        # TODO Add Stac specs
