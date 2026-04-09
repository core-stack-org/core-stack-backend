import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from shapely.geometry import mapping
from utilities.gee_utils import valid_gee_text

from nrm_app.celery import app

from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
    build_output_raster_path,
    get_union_geometry,
    load_precomputed_roi,
    push_local_raster_to_geoserver,
    resolve_lulc_raster_paths,
    validate_geometry,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status


LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/change_detection/change_detection_local"
GEOSERVER_WORKSPACE = "change_detection"

CHANGE_STAC_LAYER_NAMES = {
    "Urbanization": "change_urbanization_raster",
    "Degradation": "change_cropping_reduction_raster",
    "Deforestation": "change_tree_cover_loss_raster",
    "Afforestation": "change_tree_cover_gain_raster",
    "CropIntensity": "change_cropping_intensity_raster",
}

BUILT_UP_REMAP = {
    1: 1,
    2: 2,
    3: 2,
    4: 2,
    6: 3,
    7: 4,
    8: 3,
    9: 3,
    10: 3,
    11: 3,
    12: 4,
}

DEGRADATION_REMAP = {
    1: 1,
    2: 2,
    3: 2,
    4: 2,
    6: 4,
    7: 5,
    8: 3,
    9: 3,
    10: 3,
    11: 3,
    12: 6,
}

DEFORESTATION_AFFORESTATION_REMAP = {
    1: 1,
    2: 2,
    3: 2,
    4: 2,
    6: 3,
    7: 5,
    8: 4,
    9: 4,
    10: 4,
    11: 4,
    12: 6,
}

CROP_INTENSITY_REMAP = {
    1: 1,
    2: 2,
    3: 2,
    4: 2,
    6: 3,
    7: 4,
    8: 5,
    9: 5,
    10: 6,
    11: 7,
    12: 8,
}


def _build_lookup_table(mapping, size=13):
    lookup = np.zeros(size, dtype=np.int16)
    for source_value, mapped_value in mapping.items():
        lookup[source_value] = mapped_value
    return lookup


BUILT_UP_LOOKUP = _build_lookup_table(BUILT_UP_REMAP)
DEGRADATION_LOOKUP = _build_lookup_table(DEGRADATION_REMAP)
DEFORESTATION_AFFORESTATION_LOOKUP = _build_lookup_table(
    DEFORESTATION_AFFORESTATION_REMAP
)
CROP_INTENSITY_LOOKUP = _build_lookup_table(CROP_INTENSITY_REMAP)

CHANGE_PARAM_FUNCTIONS = {
    "Urbanization": "_compute_built_up_change",
    "Degradation": "_compute_degradation_change",
    "Deforestation": "_compute_deforestation_change",
    "Afforestation": "_compute_afforestation_change",
    "CropIntensity": "_compute_crop_intensity_change",
}

ZERO_NODATA = 0


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def _as_lulc_int_array(array):
    array = np.asarray(array)
    if np.issubdtype(array.dtype, np.integer):
        return array.astype(np.int16, copy=False)

    array = np.asarray(array, dtype=np.float64)
    array = np.where(np.isfinite(array), array, 0)
    return np.rint(array).astype(np.int16, copy=False)


def _remap_array(array, mapping):
    source = _as_lulc_int_array(array)
    if isinstance(mapping, np.ndarray):
        return mapping[source]

    remapped = np.zeros(source.shape, dtype=np.int16)
    for source_value, mapped_value in mapping.items():
        remapped[source == source_value] = mapped_value
    return remapped


def _combine_transitions(shape, transitions):
    result = np.zeros(shape, dtype=np.uint8)
    for value, condition in transitions:
        result += condition.astype(np.uint8) * np.uint8(value)
    return result


def _base_description(district, block):
    return f"change_{_slug(district, 'unknown_district')}_{_slug(block, 'unknown_block')}"


def _published_layer_name(district, block, param_name):
    return f"{_base_description(district, block)}_{param_name}"


def _output_stub(district, block, param_name, start_year, end_year):
    return f"{_published_layer_name(district, block, param_name)}_{start_year}_{end_year}"


def _select_change_detection_raster_paths(start_year, end_year):
    raster_paths = resolve_lulc_raster_paths(start_year=start_year, end_year=end_year)
    if len(raster_paths) < 6:
        raise ValueError(
            "Local change detection requires at least six yearly LULC rasters to compare the first three years against the last three years."
        )
    return raster_paths[:3] + raster_paths[-3:]


def _build_roi_shapes_by_crs(roi_gdf, raster_paths):
    roi_shapes_by_crs = {}
    roi_union = get_union_geometry(roi_gdf)
    if roi_union is None or roi_union.is_empty:
        raise ValueError("ROI union geometry is empty for local change detection.")

    roi_shapes_by_crs[str(roi_gdf.crs)] = mapping(roi_union)
    for raster_path in raster_paths:
        with rasterio.open(raster_path) as src:
            crs_key = str(src.crs)
            if crs_key in roi_shapes_by_crs:
                continue
            clip_gdf = roi_gdf if src.crs is None else roi_gdf.to_crs(src.crs)
            clip_union = get_union_geometry(clip_gdf)
            if clip_union is None or clip_union.is_empty:
                raise ValueError("ROI union geometry is empty for local change detection.")
            roi_shapes_by_crs[crs_key] = mapping(clip_union)

    return roi_shapes_by_crs


def _load_masked_lulc_array(index, raster_path, roi_shape_by_crs):
    with rasterio.open(raster_path) as src:
        clipped_data, clipped_transform = mask(
            src,
            shapes=[roi_shape_by_crs[str(src.crs)]],
            crop=True,
            filled=True,
            nodata=ZERO_NODATA,
        )
        clipped_array = _as_lulc_int_array(clipped_data[0])
        meta = src.meta.copy()
        return {
            "index": index,
            "raster_path": raster_path,
            "array": clipped_array,
            "transform": clipped_transform,
            "crs": src.crs,
            "meta": meta,
        }


def _load_masked_lulc_arrays(roi_gdf, raster_paths):
    roi_gdf = validate_geometry(roi_gdf)
    if roi_gdf.empty:
        raise ValueError("No valid ROI geometry available for local change detection.")
    if roi_gdf.crs is None:
        raise ValueError("ROI CRS is missing; cannot align LULC rasters.")

    roi_shapes_by_crs = _build_roi_shapes_by_crs(roi_gdf, raster_paths)
    max_workers = min(len(raster_paths), max(os.cpu_count() or 1, 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _load_masked_lulc_array, index, raster_path, roi_shapes_by_crs
            )
            for index, raster_path in enumerate(raster_paths, start=1)
        ]
        results = [future.result() for future in futures]

    results.sort(key=lambda item: item["index"])

    reference = results[0]
    output_meta = reference["meta"]
    output_meta.update(
        {
            "driver": "GTiff",
            "height": reference["array"].shape[0],
            "width": reference["array"].shape[1],
            "transform": reference["transform"],
            "crs": reference["crs"],
            "count": 1,
            "dtype": "uint8",
            "nodata": ZERO_NODATA,
            "compress": "lzw",
        }
    )

    arrays = []
    for result in results:
        clipped_array = result["array"]
        needs_alignment = (
            clipped_array.shape != (output_meta["height"], output_meta["width"])
            or result["transform"] != output_meta["transform"]
            or result["crs"] != output_meta["crs"]
        )
        if needs_alignment:
            aligned_array = np.zeros(
                (output_meta["height"], output_meta["width"]),
                dtype=np.int16,
            )
            reproject(
                source=clipped_array,
                destination=aligned_array,
                source_transform=result["transform"],
                source_crs=result["crs"],
                destination_transform=output_meta["transform"],
                destination_crs=output_meta["crs"],
                src_nodata=ZERO_NODATA,
                dst_nodata=ZERO_NODATA,
                resampling=Resampling.nearest,
            )
            clipped_array = aligned_array

        arrays.append(clipped_array.astype(np.int16, copy=False))
        print(
            f"Loaded local LULC raster {result['index']}/{len(raster_paths)}: {result['raster_path']}"
        )

    return arrays, output_meta


def _write_change_raster(array, output_path, output_meta):
    raster = np.asarray(array, dtype=np.uint8)
    meta = output_meta.copy()
    meta.update(
        {
            "driver": "GTiff",
            "count": 1,
            "dtype": "uint8",
            "nodata": ZERO_NODATA,
            "compress": "lzw",
        }
    )
    with rasterio.open(output_path, "w", **meta) as dst:
        dst.write(raster, 1)
    return str(output_path)


def _compute_mode_lulc_three(arrays):
    first, second, third = (_as_lulc_int_array(array) for array in arrays)
    mode = np.zeros(first.shape, dtype=np.int16)

    same_first = ((first == second) | (first == third)) & (first > 0)
    same_second = (second == third) & (second > 0)
    mode = np.where(same_first, first, mode)
    mode = np.where((mode == 0) & same_second, second, mode)

    unresolved = mode == 0
    if np.any(unresolved):
        sentinel = np.int16(np.iinfo(np.int16).max)
        positive_stack = np.stack(
            [
                np.where(first > 0, first, sentinel),
                np.where(second > 0, second, sentinel),
                np.where(third > 0, third, sentinel),
            ],
            axis=0,
        )
        fallback = np.min(positive_stack, axis=0)
        fallback = np.where(fallback == sentinel, 0, fallback)
        mode = np.where(unresolved, fallback, mode)

    return mode


def _compute_then_now_modes(remapped_arrays):
    if len(remapped_arrays) < 6:
        raise ValueError(
            "Local change detection requires at least six selected yearly LULC rasters to compare the first three years against the last three years."
        )
    then = _compute_mode_lulc_three(remapped_arrays[:3])
    now = _compute_mode_lulc_three(remapped_arrays[-3:])
    return now, then


def _compute_built_up_change(lulc_arrays):
    remapped_arrays = [_remap_array(array, BUILT_UP_LOOKUP) for array in lulc_arrays]
    now, then = _compute_then_now_modes(remapped_arrays)
    return _combine_transitions(
        then.shape,
        [
            (1, (then == 1) & (now == 1)),
            (2, (then == 2) & (now == 1)),
            (3, (then == 3) & (now == 1)),
            (4, (then == 4) & (now == 1)),
        ],
    )


def _compute_degradation_change(lulc_arrays):
    remapped_arrays = [
        _remap_array(array, DEGRADATION_LOOKUP) for array in lulc_arrays
    ]
    now, then = _compute_then_now_modes(remapped_arrays)
    return _combine_transitions(
        then.shape,
        [
            (1, (then == 3) & (now == 3)),
            (2, (then == 3) & (now == 1)),
            (3, (then == 3) & (now == 5)),
            (4, (then == 3) & (now == 6)),
        ],
    )


def _compute_deforestation_afforestation_modes(lulc_arrays):
    remapped_arrays = [
        _remap_array(array, DEFORESTATION_AFFORESTATION_LOOKUP)
        for array in lulc_arrays
    ]
    return _compute_then_now_modes(remapped_arrays)


def _build_deforestation_change(now, then):
    return _combine_transitions(
        then.shape,
        [
            (1, (then == 3) & (now == 3)),
            (2, (then == 3) & (now == 1)),
            (3, (then == 3) & (now == 4)),
            (4, (then == 3) & (now == 5)),
            (5, (then == 3) & (now == 6)),
        ],
    )


def _compute_deforestation_change(lulc_arrays):
    now, then = _compute_deforestation_afforestation_modes(lulc_arrays)
    return _build_deforestation_change(now, then)


def _build_afforestation_change(now, then):
    return _combine_transitions(
        then.shape,
        [
            (1, (then == 3) & (now == 3)),
            (2, (then == 1) & (now == 3)),
            (3, (then == 4) & (now == 3)),
            (4, (then == 5) & (now == 3)),
            (5, (then == 6) & (now == 3)),
        ],
    )


def _compute_afforestation_change(lulc_arrays):
    now, then = _compute_deforestation_afforestation_modes(lulc_arrays)
    return _build_afforestation_change(now, then)


def _compute_crop_intensity_change(lulc_arrays):
    remapped_arrays = [
        _remap_array(array, CROP_INTENSITY_LOOKUP) for array in lulc_arrays
    ]
    now, then = _compute_then_now_modes(remapped_arrays)
    return _combine_transitions(
        then.shape,
        [
            (1, (then == 6) & (now == 5)),
            (2, (then == 7) & (now == 5)),
            (3, (then == 7) & (now == 6)),
            (4, (then == 5) & (now == 6)),
            (5, (then == 5) & (now == 7)),
            (6, (then == 6) & (now == 7)),
            (7, (then == 5) & (now == 5)),
            (8, (then == 6) & (now == 6)),
            (9, (then == 7) & (now == 7)),
        ],
    )


def _compute_single_change_output(param_name, lulc_arrays):
    print(f"Computing local change detection raster: {param_name}")
    return param_name, globals()[CHANGE_PARAM_FUNCTIONS[param_name]](lulc_arrays)


def _compute_forest_change_outputs(lulc_arrays):
    print("Computing local change detection raster: Deforestation")
    print("Computing local change detection raster: Afforestation")
    now, then = _compute_deforestation_afforestation_modes(lulc_arrays)
    return {
        "Deforestation": _build_deforestation_change(now, then),
        "Afforestation": _build_afforestation_change(now, then),
    }


def _compute_change_outputs(lulc_arrays):
    outputs = {}
    cpu_workers = min(max(os.cpu_count() or 1, 1), 4)
    with ThreadPoolExecutor(max_workers=cpu_workers) as executor:
        futures = [
            executor.submit(_compute_single_change_output, param_name, lulc_arrays)
            for param_name in ("Urbanization", "Degradation", "CropIntensity")
        ]
        futures.append(executor.submit(_compute_forest_change_outputs, lulc_arrays))

        for future in futures:
            result = future.result()
            if isinstance(result, dict):
                outputs.update(result)
            else:
                param_name, change_array = result
                outputs[param_name] = change_array

    return outputs


def run_change_detection_local(
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

    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    roi_gdf = load_precomputed_roi(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
    )
    lulc_raster_paths = _select_change_detection_raster_paths(
        start_year=start_year,
        end_year=end_year,
    )
    lulc_arrays, output_meta = _load_masked_lulc_arrays(roi_gdf, lulc_raster_paths)
    change_outputs = _compute_change_outputs(lulc_arrays)

    geoserver_statuses = []

    for param_name, change_array in change_outputs.items():
        output_stub = _output_stub(district, block, param_name, start_year, end_year)
        output_path = build_output_raster_path(
            layer_name=output_stub,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
            block_fallback="unknown_block",
        )
        raster_path = _write_change_raster(
            array=change_array,
            output_path=output_path,
            output_meta=output_meta,
        )
        print(f"Saved local change detection raster: {raster_path}")

        published_layer_name = _published_layer_name(district, block, param_name)
        if push_to_geoserver:
            upload_res, style_res = push_local_raster_to_geoserver(
                file_path=raster_path,
                layer_name=published_layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=param_name.lower(),
            )
            print(f"GeoServer upload response for {param_name}: {upload_res}")
            print(f"GeoServer style response for {param_name}: {style_res}")
            geoserver_statuses.append(True)

        if sync_layer_metadata:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=published_layer_name,
                asset_id=raster_path,
                dataset_name="Change Detection Raster",
                misc={
                    "start_year": start_year,
                    "end_year": end_year,
                },
            )
            if layer_id and push_to_geoserver:
                update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
                from computing.STAC_specs import generate_STAC_layerwise

                layer_stac_generated = generate_STAC_layerwise.generate_raster_stac(
                    state=state,
                    district=district,
                    block=block,
                    layer_name=CHANGE_STAC_LAYER_NAMES[param_name],
                )
                update_layer_sync_status(
                    layer_id=layer_id,
                    is_stac_specs_generated=layer_stac_generated,
                )

    return all(geoserver_statuses) if push_to_geoserver else True


def _get_change_detection_local_task(
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = gee_account_id
    return run_change_detection_local(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )


@app.task(bind=True)
def get_change_detection(
    self,
    state,
    district,
    block,
    start_year,
    end_year,
    gee_account_id=None,
):
    _ = self
    return _get_change_detection_local_task(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        gee_account_id=gee_account_id,
    )
