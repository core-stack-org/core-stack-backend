from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from shapely.geometry import mapping

from computing.config_loader import CHANGE_DETECTION_RASTER_OUTPUT_DIR, PROJECT_ROOT
from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_raster_path,
    get_union_geometry,
    load_precomputed_roi,
    push_local_raster_to_geoserver,
    read_validated_vector_file,
    validate_geometry,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text


LOCAL_TREE_CHANGE_BASE_DIR = PROJECT_ROOT / "data/base_layers/tree_health/overall_change"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/tree_health/overall_change_local"
GEOSERVER_WORKSPACE = "tree_overall_ch"
GEOSERVER_STYLE = "tree_overall_ch_style"
BACKGROUND = -9999


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_tree_change_raster(
    start_year,
    end_year,
    tree_change_dir=LOCAL_TREE_CHANGE_BASE_DIR,
):
    # Local equivalent of TREE_OVERALL_CHANGE image collection.
    tree_change_dir = Path(tree_change_dir)
    possible_names = [
        f"overall_change_{start_year}_{end_year}.tif",
        f"overall_change_{start_year}_{int(end_year) + 1}.tif",
        "overall_change.tif",
        "TREE_OVERALL_CHANGE.tif",
    ]

    for name in possible_names:
        path = tree_change_dir / name
        if path.exists():
            return str(path)

    matches = sorted(tree_change_dir.glob("*.tif"))
    if matches:
        return str(matches[0])

    raise FileNotFoundError(f"Local overall change raster not found in {tree_change_dir}.")


def _resolve_change_detection_raster(
    state,
    district,
    block,
    asset_suffix,
    param_name,
    start_year,
    end_year,
    change_dir=CHANGE_DETECTION_RASTER_OUTPUT_DIR,
):
    # Original GEE code reads change outputs ending at end_year + 1.
    candidate_end_years = [int(end_year) + 1, int(end_year)]

    for candidate_end_year in candidate_end_years:
        raster_name = f"change_{asset_suffix}_{param_name}_{start_year}_{candidate_end_year}.tif"
        path = (
            Path(change_dir)
            / _slug(state, "unknown_state")
            / _slug(district, "unknown_district")
            / _slug(block, "unknown_block")
            / raster_name
        )
        if path.exists():
            return str(path)

    raise FileNotFoundError(
        f"Local {param_name} change raster not found for "
        f"{asset_suffix}, start_year={start_year}, end_year={end_year}."
    )


def _clip_tree_change(tree_change_path, roi_gdf):
    with rasterio.open(tree_change_path) as src:
        roi_gdf = validate_geometry(roi_gdf)
        if roi_gdf.empty:
            raise ValueError("No valid ROI geometry available for overall change clipping.")
        if roi_gdf.crs is None:
            raise ValueError("ROI CRS is missing; cannot align overall change raster.")
        if src.crs and roi_gdf.crs != src.crs:
            roi_gdf = roi_gdf.to_crs(src.crs)

        roi_union = get_union_geometry(roi_gdf)
        if roi_union is None or roi_union.is_empty:
            raise ValueError("ROI union geometry is empty for overall change clipping.")

        tree_change, transform = mask(
            src,
            shapes=[mapping(roi_union)],
            crop=True,
            filled=True,
            nodata=BACKGROUND,
            indexes=1,
        )
        if tree_change.ndim == 3:
            tree_change = tree_change[0]

        meta = src.meta.copy()
        meta.update(
            {
                "driver": "GTiff",
                "height": tree_change.shape[0],
                "width": tree_change.shape[1],
                "transform": transform,
                "count": 1,
                "dtype": "int16",
                "nodata": BACKGROUND,
                "compress": "lzw",
            }
        )

    tree_change = np.where(np.isfinite(tree_change), tree_change, BACKGROUND)
    return np.rint(tree_change).astype(np.int16), meta


def _reproject_to_match(raster_path, meta):
    output = np.full((meta["height"], meta["width"]), BACKGROUND, dtype=np.int16)
    with rasterio.open(raster_path) as src:
        reproject(
            source=rasterio.band(src, 1),
            destination=output,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=meta["transform"],
            dst_crs=meta["crs"],
            dst_nodata=BACKGROUND,
            resampling=Resampling.nearest,
        )
    return output


def _build_overall_change_raster(
    tree_change_path,
    deforestation_path,
    afforestation_path,
    roi_gdf,
    output_path,
):
    tree_change, output_meta = _clip_tree_change(
        tree_change_path=tree_change_path,
        roi_gdf=roi_gdf,
    )
    deforestation = _reproject_to_match(deforestation_path, output_meta)
    afforestation = _reproject_to_match(afforestation_path, output_meta)

    # Same class priority as mask_raster() in the GEE implementation.
    output = np.full(tree_change.shape, BACKGROUND, dtype=np.int16)

    no_change_mask = afforestation == 1
    output[no_change_mask] = 0

    deforestation_mask = (deforestation >= 2) & (deforestation <= 5)
    output[deforestation_mask] = -2

    afforestation_mask = (afforestation >= 2) & (afforestation <= 5)
    output[afforestation_mask] = 2

    allowed_inside_no_change = np.isin(tree_change, [-1, 1, 3, 4, 5])
    output[no_change_mask & allowed_inside_no_change] = tree_change[
        no_change_mask & allowed_inside_no_change
    ]

    with rasterio.open(output_path, "w", **output_meta) as dst:
        dst.write(output, 1)
        dst.set_band_description(1, "constant")

    return str(output_path)

@app.task(bind=True)
def tree_health_overall_change_raster_local(
    state=None,
    district=None,
    block=None,
    start_year=None,
    end_year=None,
    roi=None,
    asset_suffix=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    tree_change_dir=LOCAL_TREE_CHANGE_BASE_DIR,
    change_dir=CHANGE_DETECTION_RASTER_OUTPUT_DIR,
    deforestation_path=None,
    afforestation_path=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None
    start_year = int(start_year)
    end_year = int(end_year)

    if state and district and block:
        asset_suffix = (
            f"{_slug(district, 'unknown_district')}_"
            f"{_slug(block, 'unknown_block')}"
        )
        roi_gdf = load_precomputed_roi(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and "
                "`asset_suffix` are required."
            )
        asset_suffix = _slug(asset_suffix, "custom")
        roi_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )

    layer_name = f"overall_change_raster_{asset_suffix}"
    tree_change_path = _resolve_tree_change_raster(
        start_year=start_year,
        end_year=end_year,
        tree_change_dir=tree_change_dir,
    )
    if state and district and block:
        deforestation_path = deforestation_path or _resolve_change_detection_raster(
            state=state,
            district=district,
            block=block,
            asset_suffix=asset_suffix,
            param_name="Deforestation",
            start_year=start_year,
            end_year=end_year,
            change_dir=change_dir,
        )
        afforestation_path = afforestation_path or _resolve_change_detection_raster(
            state=state,
            district=district,
            block=block,
            asset_suffix=asset_suffix,
            param_name="Afforestation",
            start_year=start_year,
            end_year=end_year,
            change_dir=change_dir,
        )
    elif not deforestation_path or not afforestation_path:
        raise ValueError(
            "For custom overall change runs, `deforestation_path` and "
            "`afforestation_path` are required."
        )
    output_path = build_output_raster_path(
        layer_name=layer_name,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
        state=state,
        district=district,
        block=block,
        custom_subdir=asset_suffix,
    )

    raster_path = _build_overall_change_raster(
        tree_change_path=tree_change_path,
        deforestation_path=deforestation_path,
        afforestation_path=afforestation_path,
        roi_gdf=roi_gdf,
        output_path=output_path,
    )
    print(f"Saved local overall tree change raster: {raster_path}")

    layer_id = None
    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=raster_path,
            dataset_name="Tree Overall Change Raster",
            misc={"is_generated_locally": True},
            algorithm="local_tree_overall_change",
            algorithm_version="local-1.0",
        )

    if push_to_geoserver:
        upload_res, style_res = push_local_raster_to_geoserver(
            file_path=raster_path,
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            style_name=GEOSERVER_STYLE,
        )
        print(f"GeoServer upload response for {layer_name}: {upload_res}")
        print(f"GeoServer style response for {layer_name}: {style_res}")

    if layer_id and push_to_geoserver:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return True
