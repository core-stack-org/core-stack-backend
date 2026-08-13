import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping

from computing.config_loader import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    SOIL_TYPE_OUTPUT_DIR,
    SOIL_TYPE_RASTER_PATHS,
)
from computing.local_compute_helper import (
    build_output_vector_path,
    ensure_file_exists,
    load_precomputed_watersheds,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

logger = logging.getLogger(__name__)

GEOSERVER_WORKSPACE = "soil_type"
LOCAL_ALGORITHM = "local_soil_type_zonal_summary"
LOCAL_ALGORITHM_VERSION = "local-1.0"
DATASET_NAME = "Soil Type"

# TODO: water capacity could be a mean instead of dominant
AVAILABLE_WATER_CAPACITY_CLASSES = {
    1: 150,
    2: 125,
    3: 100,
    4: 75,
    5: 50,
    6: 15,
    7: 0,
}
SOIL_DRAINAGE_CLASSES = {
    0: "Excessively drained",
    1: "Somewhat excessively drained",
    2: "Well drained",
    3: "Moderately well drained",
    4: "Imperfectly drained",
    5: "Poorly drained",
    6: "Very poorly drained",
}
TOPSOIL_TEXTURE_CLASSES = {
    1: "Coarse",
    2: "Medium",
    3: "Fine",
}
SUBSOIL_TEXTURE_CLASSES = {
    1: "Clay (heavy)",
    2: "Silty clay",
    3: "Clay",
    4: "Silty clay loam",
    5: "Clay loam",
    6: "Silt",
    7: "Silt loam",
    8: "Sandy clay",
    9: "Loam",
    10: "Sandy clay loam",
    11: "Sandy loam",
    12: "Loamy sand",
    13: "Sand",
}

SOIL_PROPERTY_SPECS = (
    {
        "column": "available_water_capacity",
        "aggregation": "mode",
        "mapping": AVAILABLE_WATER_CAPACITY_CLASSES,
        "zero_is_nodata": True,
    },
    {
        "column": "soil_drainage_classes",
        "aggregation": "mode",
        "mapping": SOIL_DRAINAGE_CLASSES,
        "zero_is_nodata": False,
    },
    {"column": "subsoil_bulk_density", "aggregation": "mean"},
    {"column": "subsoil_exchange_capacity", "aggregation": "mean"},
    {"column": "subsoil_organic_carbon", "aggregation": "mean"},
    {"column": "subsoil_ph", "aggregation": "mean"},
    {
        "column": "subsoil_texture",
        "aggregation": "mode",
        "mapping": SUBSOIL_TEXTURE_CLASSES,
        "zero_is_nodata": True,
    },
    {"column": "topsoil_bulk_density", "aggregation": "mean"},
    {"column": "topsoil_exchange_capacity", "aggregation": "mean"},
    {"column": "topsoil_organic_carbon", "aggregation": "mean"},
    {"column": "topsoil_ph", "aggregation": "mean"},
    {
        "column": "topsoil_texture",
        "aggregation": "mode",
        "mapping": TOPSOIL_TEXTURE_CLASSES,
        "zero_is_nodata": True,
    },
)


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _layer_name(district=None, block=None, asset_suffix=None):
    if asset_suffix:
        return f"{_slug(asset_suffix, 'custom')}_soil_type"
    return (
        f"soil_type_{_slug(district, 'unknown_district')}_"
        f"{_slug(block, 'unknown_block')}"
    )


def _aggregate_values(values, spec):
    values = np.asarray(values, dtype=np.float64)
    values = values[np.isfinite(values)]
    if spec.get("zero_is_nodata", True):
        values = values[values != 0]
    if values.size == 0:
        return None

    if spec["aggregation"] == "mean":
        return round(float(values.mean()), 4)

    class_values = np.rint(values).astype(np.int32)
    unique_values, counts = np.unique(class_values, return_counts=True)
    mode = int(unique_values[np.argmax(counts)])
    return spec["mapping"].get(mode)


def compute_soil_properties_for_geometries(
    geometries_gdf,
    raster_paths=SOIL_TYPE_RASTER_PATHS,
):
    if geometries_gdf.crs is None:
        raise ValueError("Input geometry CRS is missing.")

    result = geometries_gdf.copy()
    for spec in SOIL_PROPERTY_SPECS:
        column = spec["column"]
        raster_path = Path(raster_paths[column])
        ensure_file_exists(raster_path, f"Soil property raster '{column}'")

        with rasterio.open(raster_path) as src:
            working_gdf = (
                geometries_gdf
                if not src.crs or geometries_gdf.crs == src.crs
                else geometries_gdf.to_crs(src.crs)
            )
            values = []
            for geom in working_gdf.geometry:
                if geom is None or geom.is_empty:
                    values.append(None)
                    continue
                try:
                    clipped, _ = mask(
                        src,
                        [mapping(geom)],
                        crop=True,
                        filled=False,
                    )
                except ValueError:
                    values.append(None)
                    continue

                band = clipped[0]
                valid = ~np.ma.getmaskarray(band)
                data = np.asarray(band, dtype=np.float64)
                if src.nodata is not None and np.isfinite(src.nodata):
                    valid &= data != src.nodata
                values.append(_aggregate_values(data[valid], spec))

        result[column] = pd.Series(values, index=result.index)
        logger.info("Computed soil property column '%s'.", column)

    return result


def run_soil_type_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    raster_paths=SOIL_TYPE_RASTER_PATHS,
    output_base_dir=SOIL_TYPE_OUTPUT_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    is_tehsil_run = bool(state and district and block)
    if is_tehsil_run:
        state = str(state).strip().lower()
        district = str(district).strip().lower()
        block = str(block).strip().lower()
        geometries_gdf, geometry_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi_path or not asset_suffix:
            raise ValueError(
                "Custom runs require both `roi_path` and `asset_suffix`."
            )
        geometries_gdf = read_validated_vector_file(
            roi_path,
            f"Custom ROI file has no valid geometries: {roi_path}",
        )
        geometry_source = str(roi_path)

    logger.info("Soil type geometry source: %s", geometry_source)
    layer_name = _layer_name(district, block, asset_suffix)
    result_gdf = compute_soil_properties_for_geometries(
        geometries_gdf=geometries_gdf,
        raster_paths=raster_paths,
    )
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=output_base_dir,
    )
    asset_id = write_vector_output(result_gdf, output_path, layer_name)
    logger.info("Saved local soil type vector: %s", asset_id)

    geoserver_ok = False
    geoserver_response = None
    if push_to_geoserver:
        geoserver_response = push_local_vector_to_geoserver(
            path=os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        geoserver_ok = (
            isinstance(geoserver_response, dict)
            and geoserver_response.get("status_code") in (200, 201)
        )
        if not geoserver_ok:
            logger.error(
                "GeoServer upload failed for %s: %s",
                layer_name,
                geoserver_response,
            )
            return False

    if sync_layer_metadata and is_tehsil_run:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name=DATASET_NAME,
            misc={
                "is_generated_locally": True,
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=True,
            )

    return geoserver_ok if push_to_geoserver else True


@app.task(bind=True)
def generate_soil_type_local(self, state=None, district=None, block=None):
    _ = self
    return run_soil_type_local(
        state=state,
        district=district,
        block=block,
    )
