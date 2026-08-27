from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from shapely.geometry import mapping

from computing.config_loader import LULC_BASE_DIR, PROJECT_ROOT
from computing.local_compute_helper import (
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_raster_path,
    get_union_geometry,
    load_precomputed_roi,
    queue_local_raster_for_geoserver,
    read_validated_vector_file,
    resolve_lulc_raster_paths,
    validate_geometry,
)
from computing.utils import save_layer_info_to_db
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text


LOCAL_CH_BASE_DIR = PROJECT_ROOT / "data/base_layers/tree_health/ch"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/tree_health"
GEOSERVER_WORKSPACE = "tree_ch_raster"
GEOSERVER_STYLE = "tree_ch_style"

# LULC class 6 is tree cover. CH values are retained only on tree pixels.
TREE_LULC_CLASS = 6


def _slug(value, fallback):
    if value is None:
        return fallback
    return valid_gee_text(str(value).strip().lower()) or fallback


def _resolve_ch_raster(year, ch_dir=LOCAL_CH_BASE_DIR):
    # Local canopy height files are expected to be pre-downloaded.
    ch_dir = Path(ch_dir)
    possible_names = [
        f"CH_raster_{year}.tif",
        f"ch_raster_{year}.tif",
        f"canopy_height_{year}.tif",
        f"{year}.tif",
    ]

    for name in possible_names:
        path = ch_dir / name
        if path.exists():
            return str(path)

    matches = sorted(ch_dir.glob(f"*{year}*.tif"))
    if matches:
        return str(matches[0])

    raise FileNotFoundError(
        f"Local canopy height raster for {year} not found in {ch_dir}."
    )


def _pick_output_nodata(dtype, source_nodata):
    # CH class values are 0, 1, 2 and 3, so avoid using those as nodata.
    if source_nodata is not None:
        source_nodata = float(source_nodata)
        if not np.isnan(source_nodata) and source_nodata not in (0.0, 1.0, 2.0, 3.0):
            return source_nodata

    dtype = np.dtype(dtype)
    if np.issubdtype(dtype, np.floating):
        return -9999.0

    info = np.iinfo(dtype)
    if np.issubdtype(dtype, np.signedinteger):
        return info.min
    return info.max


def _clip_and_mask_ch(ch_path, lulc_path, roi_gdf, output_path):
    with rasterio.open(ch_path) as ch_src:
        # Align ROI with the canopy height raster before clipping.
        roi_gdf = validate_geometry(roi_gdf)
        if roi_gdf.empty:
            raise ValueError("No valid ROI geometry available for local CH clipping.")
        if roi_gdf.crs is None:
            raise ValueError("ROI CRS is missing; cannot align canopy height raster.")
        if ch_src.crs and roi_gdf.crs != ch_src.crs:
            roi_gdf = roi_gdf.to_crs(ch_src.crs)

        roi_union = get_union_geometry(roi_gdf)
        if roi_union is None or roi_union.is_empty:
            raise ValueError("ROI union geometry is empty for local CH clipping.")

        # Prefer the class band if the source raster has band descriptions.
        band_index = 1
        for index, description in enumerate(ch_src.descriptions, start=1):
            if description and description.strip().lower() in ("ch_class", "ch"):
                band_index = index
                break

        nodata = _pick_output_nodata(
            dtype=ch_src.dtypes[band_index - 1],
            source_nodata=ch_src.nodata,
        )
        ch_array, ch_transform = mask(
            ch_src,
            shapes=[mapping(roi_union)],
            crop=True,
            filled=True,
            nodata=nodata,
            indexes=band_index,
        )
        if ch_array.ndim == 3:
            ch_array = ch_array[0]

        output_meta = ch_src.meta.copy()
        output_meta.update(
            {
                "driver": "GTiff",
                "height": ch_array.shape[0],
                "width": ch_array.shape[1],
                "transform": ch_transform,
                "count": 1,
                "dtype": ch_array.dtype,
                "nodata": nodata,
                "compress": "lzw",
            }
        )

    # Reproject LULC to the clipped CH grid and use it as the tree mask.
    lulc_array = np.zeros((output_meta["height"], output_meta["width"]), dtype=np.uint8)
    with rasterio.open(lulc_path) as lulc_src:
        reproject(
            source=rasterio.band(lulc_src, 1),
            destination=lulc_array,
            src_transform=lulc_src.transform,
            src_crs=lulc_src.crs,
            src_nodata=lulc_src.nodata,
            dst_transform=output_meta["transform"],
            dst_crs=output_meta["crs"],
            dst_nodata=0,
            resampling=Resampling.mode,
        )

    tree_mask = lulc_array == TREE_LULC_CLASS
    valid_ch = ch_array != nodata
    output_array = np.where(tree_mask & valid_ch, ch_array, nodata).astype(
        ch_array.dtype,
        copy=False,
    )

    with rasterio.open(output_path, "w", **output_meta) as dst:
        dst.write(output_array, 1)
        dst.set_band_description(1, "ch_class")

    return str(output_path)


@app.task(bind=True)
def tree_health_ch_raster_local(
    self,
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    start_year=None,
    end_year=None,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    ch_dir=LOCAL_CH_BASE_DIR,
    lulc_dir=LULC_BASE_DIR,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = str(state).strip().lower() if state else None
    district = str(district).strip().lower() if district else None
    block = str(block).strip().lower() if block else None
    start_year = int(start_year)
    end_year = int(end_year)

    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    # Admin runs use the precomputed watershed boundary. Custom runs use the ROI path.
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

    lulc_paths = resolve_lulc_raster_paths(
        start_year=start_year,
        end_year=end_year,
        lulc_dir=lulc_dir,
    )

    layer_at_geoserver = True

    for year, lulc_path in zip(range(start_year, end_year + 1), lulc_paths):
        layer_name = f"ch_raster_{asset_suffix}_{year}"
        ch_path = _resolve_ch_raster(year=year, ch_dir=ch_dir)
        output_path = build_output_raster_path(
            layer_name=layer_name,
            output_base_dir=LOCAL_OUTPUT_BASE_DIR,
            state=state,
            district=district,
            block=block,
            custom_subdir=asset_suffix,
        )

        # Local replacement for:
        # ImageCollection(CH).mean().clip(roi).updateMask(lulc.eq(6))
        raster_path = _clip_and_mask_ch(
            ch_path=ch_path,
            lulc_path=lulc_path,
            roi_gdf=roi_gdf,
            output_path=output_path,
        )
        print(f"Saved local canopy height raster: {raster_path}")

        layer_id = None
        if sync_layer_metadata and state and district and block:
            layer_id = save_layer_info_to_db(
                state=state,
                district=district,
                block=block,
                layer_name=layer_name,
                asset_id=raster_path,
                dataset_name="Canopy Height Raster",
                misc={
                    "start_year": start_year,
                    "end_year": end_year,
                    "is_generated_locally": True,
                },
                algorithm="local_ch_clip_tree_mask",
                algorithm_version="local-1.0",
            )

        if not push_to_geoserver:
            continue

        try:
            geoserver_response = queue_local_raster_for_geoserver(
                file_path=raster_path,
                layer_name=layer_name,
                workspace=GEOSERVER_WORKSPACE,
                style_name=GEOSERVER_STYLE,
                layer_id=layer_id,
            )
            print(f"GeoServer response for {layer_name}: {geoserver_response}")
            if geoserver_response.get("status_code") != 202:
                raise RuntimeError(str(geoserver_response))
        except Exception as error:
            print(f"Failed to sync local CH raster {layer_name}: {error}")
            layer_at_geoserver = False
            continue

    return layer_at_geoserver if push_to_geoserver else True