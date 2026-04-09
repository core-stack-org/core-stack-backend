import os

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_raster_path,
    build_output_vector_path,
    compute_categorical_raster_areas_for_watersheds,
    ensure_file_exists,
    load_precomputed_watersheds,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)


CHANGE_RASTER_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/change_detection/change_detection_local"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/change_detection/change_detection_vector_local"
GEOSERVER_WORKSPACE = "change_detection"

CHANGE_VECTOR_CLASS_DEFINITIONS = {
    "Afforestation": [
        {"value": 1, "label": "fo_fo"},
        {"value": 2, "label": "bu_fo"},
        {"value": 3, "label": "fa_fo"},
        {"value": 4, "label": "ba_fo"},
        {"value": 5, "label": "sc_fo"},
        {"value": [2, 3, 4, 5], "label": "total_aff"},
    ],
    "Deforestation": [
        {"value": 1, "label": "fo_fo"},
        {"value": 2, "label": "fo_bu"},
        {"value": 3, "label": "fo_fa"},
        {"value": 4, "label": "fo_ba"},
        {"value": 5, "label": "fo_sc"},
        {"value": [2, 3, 4, 5], "label": "total_def"},
    ],
    "Degradation": [
        {"value": 1, "label": "f_f"},
        {"value": 2, "label": "f_bu"},
        {"value": 3, "label": "f_ba"},
        {"value": 4, "label": "f_sc"},
        {"value": [2, 3, 4], "label": "total_deg"},
    ],
    "Urbanization": [
        {"value": 1, "label": "bu_bu"},
        {"value": 2, "label": "w_bu"},
        {"value": 3, "label": "tr_bu"},
        {"value": 4, "label": "b_bu"},
        {"value": [2, 3, 4], "label": "total_urb"},
    ],
    "CropIntensity": [
        {"value": 1, "label": "do_si"},
        {"value": 2, "label": "tr_si"},
        {"value": 3, "label": "tr_do"},
        {"value": 4, "label": "si_do"},
        {"value": 5, "label": "si_tr"},
        {"value": 6, "label": "do_tr"},
        {"value": 7, "label": "si_si"},
        {"value": 8, "label": "do_do"},
        {"value": 9, "label": "tr_tr"},
        {"value": [1, 2, 3, 4, 5, 6], "label": "total_change"},
    ],
}


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _published_layer_name(district, block, param_name):
    return (
        f"change_vector_{_slug(district, 'unknown_district')}_"
        f"{_slug(block, 'unknown_block')}_{param_name}"
    )


def _output_stub(district, block, param_name, start_year, end_year):
    return f"{_published_layer_name(district, block, param_name)}_{start_year}_{end_year}"


def _resolve_local_change_raster_path(state, district, block, param_name, start_year, end_year):
    raster_path = build_output_raster_path(
        layer_name=(
            f"change_{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}_{param_name}_{int(start_year)}_{int(end_year)}"
        ),
        output_base_dir=CHANGE_RASTER_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
        block_fallback="unknown_block",
    )
    ensure_file_exists(raster_path, f"Local change detection raster for {param_name}")
    return str(raster_path)


def run_change_detection_vector_local(
    state,
    district,
    block,
    start_year,
    end_year,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()
    start_year = int(start_year)
    end_year = int(end_year)

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    print(f"Watershed boundary source: {watershed_source}")

    geoserver_statuses = []

    for param_name, class_definitions in CHANGE_VECTOR_CLASS_DEFINITIONS.items():
        raster_path = _resolve_local_change_raster_path(
            state=state,
            district=district,
            block=block,
            param_name=param_name,
            start_year=start_year,
            end_year=end_year,
        )
        print(f"Using local change raster for {param_name}: {raster_path}")

        result_gdf = compute_categorical_raster_areas_for_watersheds(
            watersheds_gdf=watersheds_gdf,
            raster_path=raster_path,
            class_definitions=class_definitions,
        )

        output_stub = _output_stub(district, block, param_name, start_year, end_year)
        output_path = build_output_vector_path(
            layer_name=output_stub,
            state=state,
            district=district,
            block=block,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            block_fallback="unknown_block",
        )
        asset_id = write_vector_output(
            gdf=result_gdf,
            output_path=output_path,
            layer_name=output_stub,
        )
        print(f"Saved local change detection vector: {asset_id}")

        published_layer_name = _published_layer_name(district, block, param_name)
        if push_to_geoserver:
            geoserver_response = push_shape_to_geoserver(
                os.path.splitext(asset_id)[0],
                workspace=GEOSERVER_WORKSPACE,
                layer_name=published_layer_name,
                file_type="gpkg",
            )
            print(f"GeoServer response for {param_name}: {geoserver_response}")
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
                layer_name=published_layer_name,
                asset_id=asset_id,
                dataset_name="Change Detection Vector",
            )
            if layer_id and push_to_geoserver:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return all(geoserver_statuses) if push_to_geoserver else True


def _vectorise_change_detection_local_task(
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_change_detection_vector_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def vectorise_change_detection(
    self,
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = self
    return _vectorise_change_detection_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )
