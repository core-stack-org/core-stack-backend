import os

import pandas as pd
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text

from computing.local_compute_helper import (
    LULC_BASE_DIR,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_vector_path,
    compute_categorical_raster_areas_for_watersheds,
    compute_union_categorical_area_across_rasters_for_watersheds,
    load_precomputed_watersheds,
    resolve_lulc_raster_paths,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)


LOCAL_OUTPUT_BASE_DIR = (
    PROJECT_ROOT / "data/cropping_intensity/cropping_intensity_local"
)
GEOSERVER_WORKSPACE = "crop_intensity"
LOCAL_ALGORITHM = "local_cropping_intensity"
LOCAL_ALGORITHM_VERSION = "local-1.0"
INITIAL_YEAR = 2017

SINGLE_KHARIF = 8
SINGLE_NON_KHARIF = 9
DOUBLE = 10
TRIPLE = 11

YEARLY_CLASS_DEFINITIONS = (
    {"value": SINGLE_KHARIF, "label_prefix": "single_kharif_cropped_area_"},
    {"value": SINGLE_NON_KHARIF, "label_prefix": "single_non_kharif_cropped_area_"},
    {"value": DOUBLE, "label_prefix": "doubly_cropped_area_"},
    {"value": TRIPLE, "label_prefix": "triply_cropped_area_"},
)


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _layer_name(asset_suffix, zoi_ci_asset=False):
    if zoi_ci_asset:
        return f"{asset_suffix}_intensity_ZOI"
    return f"{asset_suffix}_intensity"


def _resolve_asset_suffix(state, district, block, asset_suffix):
    if asset_suffix:
        return asset_suffix
    if state and district and block:
        return (
            f"{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}"
        )
    raise ValueError("state, district, and block are required for local cropping intensity.")


def _coerce_year(year_value, label):
    try:
        return int(year_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a valid integer year.") from exc


def _compute_yearly_area_columns(result_gdf, raster_paths, start_year):
    for year, raster_path in zip(range(start_year, start_year + len(raster_paths)), raster_paths):
        print(f"Computing local cropping intensity inputs for {year}-{year + 1}: {raster_path}")
        year_definitions = [
            {
                "value": class_definition["value"],
                "label": f"{class_definition['label_prefix']}{year}",
            }
            for class_definition in YEARLY_CLASS_DEFINITIONS
        ]
        year_result = compute_categorical_raster_areas_for_watersheds(
            watersheds_gdf=result_gdf,
            raster_path=raster_path,
            class_definitions=year_definitions,
        )
        for definition in year_definitions:
            result_gdf[definition["label"]] = year_result[definition["label"]].astype(
                float
            )
        result_gdf[f"single_cropped_area_{year}"] = (
            result_gdf[f"single_kharif_cropped_area_{year}"]
            + result_gdf[f"single_non_kharif_cropped_area_{year}"]
        )
    return result_gdf


def _compute_total_croppable_area(result_gdf, denominator_raster_paths, end_year):
    output_column = f"total_cropable_area_ever_hydroyear_{INITIAL_YEAR}_{end_year}"
    print(
        "Computing local ever-croppable denominator "
        f"for {INITIAL_YEAR}-{end_year} using {len(denominator_raster_paths)} rasters"
    )
    return compute_union_categorical_area_across_rasters_for_watersheds(
        watersheds_gdf=result_gdf,
        raster_paths=denominator_raster_paths,
        class_values=[SINGLE_KHARIF, SINGLE_NON_KHARIF, DOUBLE, TRIPLE],
        output_column=output_column,
    )


def _compute_cropping_intensity_columns(result_gdf, start_year, end_year):
    denominator_column = f"total_cropable_area_ever_hydroyear_{INITIAL_YEAR}_{end_year}"
    denominator = pd.to_numeric(result_gdf[denominator_column], errors="coerce").fillna(0.0)

    for year in range(start_year, end_year + 1):
        single_area = pd.to_numeric(
            result_gdf[f"single_cropped_area_{year}"], errors="coerce"
        ).fillna(0.0)
        double_area = pd.to_numeric(
            result_gdf[f"doubly_cropped_area_{year}"], errors="coerce"
        ).fillna(0.0)
        triple_area = pd.to_numeric(
            result_gdf[f"triply_cropped_area_{year}"], errors="coerce"
        ).fillna(0.0)

        nonzero_denominator = denominator > 0
        intensity = pd.Series(0.0, index=result_gdf.index, dtype=float)
        intensity.loc[nonzero_denominator] = (
            single_area.loc[nonzero_denominator] / denominator.loc[nonzero_denominator]
            + 2.0 * double_area.loc[nonzero_denominator] / denominator.loc[nonzero_denominator]
            + 3.0 * triple_area.loc[nonzero_denominator] / denominator.loc[nonzero_denominator]
        )
        result_gdf[f"cropping_intensity_{year}"] = intensity.astype(float)

    return result_gdf


def run_cropping_intensity_local(
    state,
    district,
    block,
    start_year,
    end_year,
    asset_suffix=None,
    zoi_ci_asset=False,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    lulc_dir=LULC_BASE_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower()
    district = str(district).strip().lower()
    block = str(block).strip().lower()
    start_year = _coerce_year(start_year, "start_year")
    end_year = _coerce_year(end_year, "end_year")

    if start_year > end_year:
        raise ValueError("start_year cannot be greater than end_year")
    if start_year < INITIAL_YEAR:
        raise ValueError(
            f"start_year must be greater than or equal to {INITIAL_YEAR} for local cropping intensity."
        )

    asset_suffix = _resolve_asset_suffix(state, district, block, asset_suffix)
    layer_name = _layer_name(asset_suffix, zoi_ci_asset=zoi_ci_asset)

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    print(f"Watershed boundary source: {watershed_source}")

    yearly_raster_paths = resolve_lulc_raster_paths(
        start_year=start_year,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )
    denominator_raster_paths = resolve_lulc_raster_paths(
        start_year=INITIAL_YEAR,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )

    result_gdf = watersheds_gdf.copy()
    result_gdf = _compute_yearly_area_columns(
        result_gdf=result_gdf,
        raster_paths=yearly_raster_paths,
        start_year=start_year,
    )
    result_gdf = _compute_total_croppable_area(
        result_gdf=result_gdf,
        denominator_raster_paths=denominator_raster_paths,
        end_year=end_year,
    )
    result_gdf = _compute_cropping_intensity_columns(
        result_gdf=result_gdf,
        start_year=start_year,
        end_year=end_year,
    )

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
    print(f"Saved local cropping intensity vector: {asset_id}")

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
            dataset_name="Cropping Intensity",
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


def _generate_cropping_intensity_local_task(
    state=None,
    district=None,
    block=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    gee_account_id=None,
    zoi_ci_asset=None,
):
    _ = roi_path, asset_folder_list, app_type, gee_account_id
    return run_cropping_intensity_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        asset_suffix=asset_suffix,
        zoi_ci_asset=bool(zoi_ci_asset),
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def generate_cropping_intensity(
    self,
    state=None,
    district=None,
    block=None,
    roi_path=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    start_year=None,
    end_year=None,
    gee_account_id=None,
    zoi_ci_asset=None,
):
    _ = self
    return _generate_cropping_intensity_local_task(
        state=state,
        district=district,
        block=block,
        roi_path=roi_path,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder_list,
        app_type=app_type,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
        zoi_ci_asset=zoi_ci_asset,
    )
