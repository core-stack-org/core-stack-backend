import os

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    LULC_BASE_DIR,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_vector_path,
    compute_categorical_raster_areas_for_watersheds,
    load_precomputed_watersheds,
    resolve_lulc_raster_paths,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/lulc/lulc_vector_local"
GEOSERVER_WORKSPACE = "lulc_vector"
LOCAL_ALGORITHM = "local_lulc_vector"
LOCAL_ALGORITHM_VERSION = "local-1.0"

LULC_VECTOR_CLASS_DEFINITIONS = (
    {"value": 1, "label": "built-up_area_"},
    {"value": 2, "label": "k_water_area_"},
    {"value": 3, "label": "kr_water_area_"},
    {"value": 4, "label": "krz_water_area_"},
    {"value": 5, "label": "cropland_area_"},
    {"value": 6, "label": "tree_forest_area_"},
    {"value": 7, "label": "barrenlands_area_"},
    {"value": 8, "label": "single_kharif_cropped_area_"},
    {"value": 9, "label": "single_non_kharif_cropped_area_"},
    {"value": 10, "label": "doubly_cropped_area_"},
    {"value": 11, "label": "triply_cropped_area_"},
    {"value": 12, "label": "shrub_scrub_area_"},
)


def _slug(value, fallback):
    text = str(value).strip().lower()
    return valid_gee_text(text) or fallback


def _layer_name(district, block):
    return f"lulc_vector_{_slug(district, 'unknown_district')}_{_slug(block, 'unknown_block')}"


def _year_class_definitions(year):
    return [
        {
            "value": class_definition["value"],
            "label": f"{class_definition['label']}{year}",
        }
        for class_definition in LULC_VECTOR_CLASS_DEFINITIONS
    ]


def run_lulc_vector_local(
    state,
    district,
    block,
    start_year,
    end_year,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    lulc_dir=LULC_BASE_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()
    start_year = int(start_year)
    end_year = int(end_year)

    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    print(f"Watershed boundary source: {watershed_source}")

    layer_name = _layer_name(district, block)
    result_gdf = watersheds_gdf.copy()
    raster_paths = resolve_lulc_raster_paths(
        start_year=start_year,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )

    for year, raster_path in zip(range(start_year, end_year + 1), raster_paths):
        print(f"Computing local LULC vector properties for {year}-{year + 1}: {raster_path}")
        year_result = compute_categorical_raster_areas_for_watersheds(
            watersheds_gdf=result_gdf,
            raster_path=raster_path,
            class_definitions=_year_class_definitions(year),
        )
        for column in year_result.columns:
            if column not in result_gdf.columns:
                result_gdf[column] = year_result[column]

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
    print(f"Saved local LULC vector: {asset_id}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
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
            dataset_name="LULC",
            misc={
                "start_year": start_year,
                "end_year": end_year,
            },
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
        )
        if layer_id and push_to_geoserver:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return True


def _vectorise_lulc_local_task(
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_lulc_vector_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def vectorise_lulc(
    self,
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = self
    return _vectorise_lulc_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )
