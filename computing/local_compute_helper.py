import os
from contextlib import ExitStack
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping
from utilities.gee_utils import valid_gee_text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRECOMPUTED_TEHSIL_WATERSHED_DIR = (
    PROJECT_ROOT / "data/base_layers/tehsil_watersheds"
)
PRECOMPUTED_ROI_EXTENSIONS = (".gpkg", ".geojson")
AEZ_VECTOR_PATH = (
    PROJECT_ROOT / "data/base_layers/AEZs/Agro_Ecological_Regions.shp"
)
LULC_BASE_DIR = PROJECT_ROOT / "data/base_layers/lulc"
TERRAIN_RASTER_PATH = (
    PROJECT_ROOT / "data/base_layers/terrain_raster_fabdam_pan_india.tif"
)
VALID_COMPUTE_TYPES = {"gee", "local"}
MIN_WATERSHED_AREA_HA = 400.0
LULC_CLASSES = np.arange(1, 13, dtype=np.int16)

TERRAIN_CLUSTER_CENTROIDS = np.array(
    [
        [0.36255426, 0.21039965, 0.12161905, 0.17393119, 0.13149585],
        [0.09171062, 0.84299211, 0.035222, 0.02172654, 0.00834873],
        [0.08497599, 0.01051893, 0.23763531, 0.37992855, 0.28694122],
        [0.22301813, 0.5611825, 0.08511123, 0.07314189, 0.05754624],
    ],
    dtype=np.float64,
)

PLAIN_CLASSES = {5}
VALLEY_CLASSES = {1, 2, 4, 9}
HILL_SLOPES_CLASSES = {8}
RIDGE_CLASSES = {3, 7, 10, 11}
SLOPY_CLASSES = {6}


def _slug(value, fallback):
    return valid_gee_text(str(value).strip().lower()) or fallback


def ensure_file_exists(path, label):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def validate_geometry(gdf):
    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        return gdf
    invalid = ~gdf.is_valid
    if invalid.any():
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)
    return gdf[~gdf.geometry.is_empty].copy()


def read_validated_vector_file(path, empty_message):
    gdf = validate_geometry(gpd.read_file(path))
    if gdf.empty:
        raise ValueError(empty_message)
    return gdf


def resolve_precomputed_vector_file(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    extensions=PRECOMPUTED_ROI_EXTENSIONS,
    missing_file_label="Precomputed vector file",
):
    roi_dir = Path(precomputed_roi_dir or PRECOMPUTED_TEHSIL_WATERSHED_DIR)
    state_slug = _slug(state, "unknown_state")
    district_slug = _slug(district, "unknown_district")
    block_slug = _slug(block, "unknown_tehsil")

    expected_paths = [
        roi_dir / state_slug / district_slug / f"{block_slug}{ext}"
        for ext in extensions
    ]
    for path in expected_paths:
        if path.exists():
            return path

    raise FileNotFoundError(
        f"{missing_file_label} not found. "
        f"state={state}, district={district}, block={block}. "
        f"Expected one of: {[str(path) for path in expected_paths]}"
    )


def load_precomputed_watersheds(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    watershed_path = resolve_precomputed_vector_file(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
        missing_file_label="Precomputed watershed boundary file",
    )
    watersheds_gdf = read_validated_vector_file(
        watershed_path,
        f"Precomputed watershed file has no valid geometries: {watershed_path}",
    )
    print(f"Loaded watershed boundaries: {watershed_path}")
    return watersheds_gdf, str(watershed_path)


def load_precomputed_roi(
    state,
    district,
    block,
    precomputed_roi_dir=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    roi_path = resolve_precomputed_vector_file(
        state=state,
        district=district,
        block=block,
        precomputed_roi_dir=precomputed_roi_dir,
        missing_file_label="Precomputed tehsil watershed file",
    )
    roi_gdf = read_validated_vector_file(
        roi_path,
        f"Precomputed ROI file has no valid geometries: {roi_path}",
    )
    print(f"Loaded precomputed ROI file: {roi_path}")
    return roi_gdf


def _build_output_dir(
    output_base_dir,
    state=None,
    district=None,
    block=None,
    custom_subdir="custom",
    block_fallback="unknown_tehsil",
):
    output_base_dir = Path(output_base_dir)
    if state and district and block:
        output_dir = (
            output_base_dir
            / _slug(state, "unknown_state")
            / _slug(district, "unknown_district")
            / _slug(block, block_fallback)
        )
    else:
        output_dir = output_base_dir / custom_subdir
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_output_vector_path(
    layer_name,
    state,
    district,
    block,
    output_base_dir,
    block_fallback="unknown_block",
):
    output_dir = _build_output_dir(
        output_base_dir=output_base_dir,
        state=state,
        district=district,
        block=block,
        block_fallback=block_fallback,
    )
    return output_dir / f"{layer_name}.gpkg"


def build_output_raster_path(
    layer_name,
    output_base_dir,
    state=None,
    district=None,
    block=None,
    custom_subdir="custom",
    block_fallback="unknown_tehsil",
):
    output_dir = _build_output_dir(
        output_base_dir=output_base_dir,
        state=state,
        district=district,
        block=block,
        custom_subdir=custom_subdir,
        block_fallback=block_fallback,
    )
    return output_dir / f"{layer_name}.tif"


def write_vector_output(gdf, output_path, layer_name):
    gdf.to_file(output_path, driver="GPKG", layer=layer_name)
    return str(output_path)


def clip_raster_with_roi(roi_gdf, raster_path, output_path, raster_label="Raster"):
    ensure_file_exists(raster_path, raster_label)

    with rasterio.open(raster_path) as src:
        clip_gdf = roi_gdf
        if src.crs and clip_gdf.crs and clip_gdf.crs != src.crs:
            clip_gdf = clip_gdf.to_crs(src.crs)

        clip_gdf = validate_geometry(clip_gdf)
        shapes = [
            mapping(geom)
            for geom in clip_gdf.geometry
            if geom is not None and not geom.is_empty
        ]
        if not shapes:
            raise ValueError("No valid ROI geometry available for raster clipping.")

        clipped_data, clipped_transform = mask(src, shapes=shapes, crop=True)
        clipped_meta = src.meta.copy()
        clipped_meta.update(
            {
                "driver": "GTiff",
                "height": clipped_data.shape[1],
                "width": clipped_data.shape[2],
                "transform": clipped_transform,
                "compress": "lzw",
            }
        )

    with rasterio.open(output_path, "w", **clipped_meta) as dst:
        dst.write(clipped_data)

    return str(output_path)


def push_local_raster_to_geoserver(file_path, layer_name, workspace, style_name=None):
    from utilities.geoserver_utils import Geoserver

    geo = Geoserver()
    geo.delete_raster_store(workspace=workspace, store=layer_name)
    upload_response = geo.create_coveragestore(
        path=file_path,
        workspace=workspace,
        layer_name=layer_name,
    )
    style_response = None
    if style_name:
        style_response = geo.publish_style(
            layer_name=layer_name,
            style_name=style_name,
            workspace=workspace,
        )
    return upload_response, style_response



def compute_pixel_area_grid(transform, height, width, crs):
    if crs is None:
        raise ValueError("Raster CRS is missing; cannot compute pixel areas.")

    if getattr(crs, "is_geographic", False):
        lon_width_radians = np.deg2rad(abs(transform.a))
        row_indices = np.arange(height, dtype=np.float64)
        lat_top = transform.f + (row_indices * transform.e)
        lat_bottom = lat_top + transform.e
        earth_radius_m = 6378137.0
        row_areas = (earth_radius_m**2) * lon_width_radians * np.abs(
            np.sin(np.deg2rad(lat_top)) - np.sin(np.deg2rad(lat_bottom))
        )
        return np.broadcast_to(row_areas[:, None], (height, width))

    pixel_area = abs(transform.a * transform.e)
    return np.full((height, width), pixel_area, dtype=np.float64)



def compute_categorical_raster_areas_for_watersheds(
    watersheds_gdf,
    raster_path,
    class_definitions,
):
    ensure_file_exists(raster_path, "Categorical raster")

    with rasterio.open(raster_path) as src:
        working_gdf = watersheds_gdf.copy()
        if working_gdf.crs is None:
            raise ValueError(
                "Watershed CRS is missing; cannot align with categorical raster."
            )
        if src.crs and working_gdf.crs != src.crs:
            working_gdf = working_gdf.to_crs(src.crs)

        nodata = src.nodata
        computed_rows = []
        empty_result = {
            class_definition["label"]: 0.0
            for class_definition in class_definitions
        }

        total = len(working_gdf)
        for index, row in enumerate(working_gdf.itertuples(index=False), start=1):
            geom = row.geometry
            if geom is None or geom.is_empty:
                computed_rows.append(empty_result.copy())
                continue

            try:
                clipped, clipped_transform = mask(
                    src,
                    [mapping(geom)],
                    crop=True,
                    filled=False,
                )
            except ValueError:
                computed_rows.append(empty_result.copy())
                continue

            data = clipped[0]
            if data.size == 0:
                computed_rows.append(empty_result.copy())
                continue

            values = np.asarray(data, dtype=np.float64)
            values = np.where(np.isfinite(values), values, 0)
            values = np.rint(values).astype(np.int16, copy=False)
            valid_mask = ~np.ma.getmaskarray(data)
            if nodata is not None and not np.isnan(nodata):
                valid_mask &= values != int(round(nodata))

            pixel_area_ha = (
                compute_pixel_area_grid(
                    transform=clipped_transform,
                    height=values.shape[0],
                    width=values.shape[1],
                    crs=src.crs,
                )
                * 0.0001
            )

            row_result = {}
            for class_definition in class_definitions:
                raw_values = class_definition.get("values", class_definition.get("value"))
                if isinstance(raw_values, (list, tuple, set, np.ndarray)):
                    class_values = list(raw_values)
                else:
                    class_values = [raw_values]
                class_mask = valid_mask & np.isin(values, class_values)
                row_result[class_definition["label"]] = float(
                    pixel_area_ha[class_mask].sum()
                )

            computed_rows.append(row_result)

            if index % 200 == 0 or index == total:
                print(
                    f"Computed categorical raster areas for {index}/{total} watersheds"
                )

    result = watersheds_gdf.copy()
    computed_df = pd.DataFrame(computed_rows)
    for column in computed_df.columns:
        result[column] = computed_df[column].values
    return result


def compute_union_categorical_area_across_rasters_for_watersheds(
    watersheds_gdf,
    raster_paths,
    class_values,
    output_column,
):
    if not raster_paths:
        raise ValueError("At least one raster path is required for union area computation.")

    for raster_path in raster_paths:
        ensure_file_exists(raster_path, "Categorical raster")

    if isinstance(class_values, (list, tuple, set, np.ndarray)):
        class_values = list(class_values)
    else:
        class_values = [class_values]

    with ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(path)) for path in raster_paths]
        aligned_gdfs = []
        for src in sources:
            aligned = watersheds_gdf.copy()
            if aligned.crs is None:
                raise ValueError(
                    "Watershed CRS is missing; cannot align with categorical raster."
                )
            if src.crs and aligned.crs != src.crs:
                aligned = aligned.to_crs(src.crs)
            aligned_gdfs.append(aligned)

        computed_rows = []
        total = len(watersheds_gdf)

        for index in range(total):
            union_mask = None
            union_pixel_area_ha = None

            for src, aligned_gdf in zip(sources, aligned_gdfs):
                geom = aligned_gdf.iloc[index].geometry
                if geom is None or geom.is_empty:
                    continue

                try:
                    clipped, clipped_transform = mask(
                        src,
                        [mapping(geom)],
                        crop=True,
                        filled=False,
                    )
                except ValueError:
                    continue

                data = clipped[0]
                if data.size == 0:
                    continue

                values = np.asarray(data, dtype=np.float64)
                values = np.where(np.isfinite(values), values, 0)
                values = np.rint(values).astype(np.int16, copy=False)

                valid_mask = ~np.ma.getmaskarray(data)
                nodata = src.nodata
                if nodata is not None and not np.isnan(nodata):
                    valid_mask &= values != int(round(nodata))

                class_mask = valid_mask & np.isin(values, class_values)
                if union_mask is None:
                    union_mask = class_mask
                    union_pixel_area_ha = (
                        compute_pixel_area_grid(
                            transform=clipped_transform,
                            height=values.shape[0],
                            width=values.shape[1],
                            crs=src.crs,
                        )
                        * 0.0001
                    )
                else:
                    if union_mask.shape != class_mask.shape:
                        raise ValueError(
                            "Local categorical rasters do not align for union area computation."
                        )
                    union_mask |= class_mask

            if union_mask is None or union_pixel_area_ha is None:
                computed_rows.append({output_column: 0.0})
            else:
                computed_rows.append(
                    {output_column: float(union_pixel_area_ha[union_mask].sum())}
                )

            if (index + 1) % 200 == 0 or (index + 1) == total:
                print(
                    f"Computed union categorical raster areas for {index + 1}/{total} watersheds"
                )

    result = watersheds_gdf.copy()
    computed_df = pd.DataFrame(computed_rows)
    result[output_column] = computed_df[output_column].values
    return result


def get_union_geometry(gdf):
    if hasattr(gdf.geometry, "union_all"):
        return gdf.geometry.union_all()
    return gdf.geometry.unary_union


def resolve_clipped_terrain_raster_path(
    state,
    district,
    block,
    clipped_raster_dir,
):
    layer_stub = f"{_slug(district, 'unknown_district')}_{_slug(block, 'unknown_tehsil')}_terrain_raster"
    raster_path = (
        Path(clipped_raster_dir)
        / _slug(state, "unknown_state")
        / _slug(district, "unknown_district")
        / _slug(block, "unknown_tehsil")
        / f"{layer_stub}.tif"
    )
    ensure_file_exists(raster_path, "Clipped terrain raster")
    return str(raster_path)


def _compute_cluster_id(
    slopy_prop,
    plain_prop,
    ridge_prop,
    valley_prop,
    hill_slopes_prop,
):
    feature_vector = np.array(
        [slopy_prop, plain_prop, ridge_prop, valley_prop, hill_slopes_prop],
        dtype=np.float64,
    )
    distances = np.sum((TERRAIN_CLUSTER_CENTROIDS - feature_vector) ** 2, axis=1)
    return int(np.argmin(distances))


def _fraction(values, valid_mask, class_values):
    total = int(valid_mask.sum())
    if total == 0:
        return 0.0
    class_mask = np.isin(values, list(class_values))
    return float((class_mask & valid_mask).sum()) / float(total)


def compute_terrain_properties_for_watersheds(watersheds_gdf, raster_path):
    with rasterio.open(raster_path) as src:
        working_gdf = watersheds_gdf.copy()
        if working_gdf.crs is None:
            raise ValueError(
                "Watershed CRS is missing; cannot align with raster CRS."
            )
        if src.crs and working_gdf.crs != src.crs:
            working_gdf = working_gdf.to_crs(src.crs)

        nodata = src.nodata
        computed_rows = []

        total = len(working_gdf)
        for index, row in enumerate(working_gdf.itertuples(index=False), start=1):
            geom = row.geometry
            if geom is None or geom.is_empty:
                computed_rows.append(
                    {
                        "plain_area": 0.0,
                        "valley_area": 0.0,
                        "hill_slopes_area": 0.0,
                        "ridge_area": 0.0,
                        "slopy_area": 0.0,
                        "terrainClusters": -1,
                    }
                )
                continue

            try:
                clipped, _ = mask(src, [mapping(geom)], crop=True, filled=True)
            except ValueError:
                computed_rows.append(
                    {
                        "plain_area": 0.0,
                        "valley_area": 0.0,
                        "hill_slopes_area": 0.0,
                        "ridge_area": 0.0,
                        "slopy_area": 0.0,
                        "terrainClusters": -1,
                    }
                )
                continue

            values = clipped[0]
            if values.size == 0:
                computed_rows.append(
                    {
                        "plain_area": 0.0,
                        "valley_area": 0.0,
                        "hill_slopes_area": 0.0,
                        "ridge_area": 0.0,
                        "slopy_area": 0.0,
                        "terrainClusters": -1,
                    }
                )
                continue

            values = np.rint(values).astype(np.int16, copy=False)
            valid_mask = np.ones(values.shape, dtype=bool)
            if nodata is not None and not np.isnan(nodata):
                valid_mask &= values != int(round(nodata))
            valid_mask &= values != 0

            plain_prop = _fraction(values, valid_mask, PLAIN_CLASSES)
            valley_prop = _fraction(values, valid_mask, VALLEY_CLASSES)
            hill_slopes_prop = _fraction(
                values, valid_mask, HILL_SLOPES_CLASSES
            )
            ridge_prop = _fraction(values, valid_mask, RIDGE_CLASSES)
            slopy_prop = _fraction(values, valid_mask, SLOPY_CLASSES)

            if valid_mask.sum() == 0:
                cluster_id = -1
            else:
                cluster_id = _compute_cluster_id(
                    slopy_prop=slopy_prop,
                    plain_prop=plain_prop,
                    ridge_prop=ridge_prop,
                    valley_prop=valley_prop,
                    hill_slopes_prop=hill_slopes_prop,
                )

            computed_rows.append(
                {
                    "plain_area": plain_prop * 100.0,
                    "valley_area": valley_prop * 100.0,
                    "hill_slopes_area": hill_slopes_prop * 100.0,
                    "ridge_area": ridge_prop * 100.0,
                    "slopy_area": slopy_prop * 100.0,
                    "terrainClusters": cluster_id,
                }
            )

            if index % 200 == 0 or index == total:
                print(
                    f"Computed terrain properties for {index}/{total} watersheds"
                )

    result = watersheds_gdf.copy()
    computed_df = pd.DataFrame(computed_rows)
    for column in computed_df.columns:
        result[column] = computed_df[column].values
    return result


def resolve_lulc_raster_paths(
    start_year,
    end_year,
    lulc_dir=LULC_BASE_DIR,
):
    raster_paths = []
    for year in range(int(start_year), int(end_year) + 1):
        raster_path = Path(lulc_dir) / f"lulc_v3_{year}_{year + 1}.tif"
        ensure_file_exists(raster_path, f"LULC raster for {year}-{year + 1}")
        raster_paths.append(str(raster_path))
    return raster_paths


def get_watershed_areas_in_hectares(watersheds_gdf):
    if "area_in_ha" in watersheds_gdf.columns:
        area_in_ha = pd.to_numeric(
            watersheds_gdf["area_in_ha"], errors="coerce"
        )
        if area_in_ha.notna().any():
            return area_in_ha
    projected = watersheds_gdf.to_crs("EPSG:6933")
    return projected.geometry.area / 10000.0


def filter_large_watersheds(
    watersheds_gdf,
    min_watershed_area_ha=MIN_WATERSHED_AREA_HA,
):
    area_in_ha = get_watershed_areas_in_hectares(watersheds_gdf)
    filtered = watersheds_gdf.loc[area_in_ha > min_watershed_area_ha].copy()
    filtered["area"] = area_in_ha.loc[filtered.index].astype(float) * 10000.0
    return filtered.reset_index(drop=True)


def resolve_aez_code(
    watersheds_gdf,
    aez_vector_path=AEZ_VECTOR_PATH,
):
    ensure_file_exists(aez_vector_path, "AEZ vector")

    aez_gdf = read_validated_vector_file(
        aez_vector_path,
        f"AEZ vector has no valid geometries: {aez_vector_path}",
    )

    study_area = watersheds_gdf[["geometry"]].copy()
    if study_area.crs is None:
        raise ValueError("Watershed CRS is missing; cannot resolve AEZ.")
    if aez_gdf.crs and study_area.crs != aez_gdf.crs:
        study_area = study_area.to_crs(aez_gdf.crs)

    study_union = get_union_geometry(study_area)
    intersecting_aez = aez_gdf.loc[aez_gdf.intersects(study_union)].copy()
    if intersecting_aez.empty:
        raise ValueError("No AEZ polygon intersects the watershed study area.")

    intersecting_aez["overlap_area"] = intersecting_aez.geometry.intersection(
        study_union
    ).area
    return int(
        intersecting_aez.sort_values("overlap_area", ascending=False).iloc[0][
            "ae_regcode"
        ]
    )


def compute_mode_lulc_array(reprojected_arrays, lulc_classes=LULC_CLASSES):
    stack = np.rint(np.stack(reprojected_arrays, axis=0)).astype(
        np.int16,
        copy=False,
    )
    valid_mask = np.isfinite(stack) & (stack > 0)
    counts = np.zeros(
        (len(lulc_classes), stack.shape[1], stack.shape[2]),
        dtype=np.uint16,
    )

    for index, lulc_class in enumerate(lulc_classes):
        counts[index] = np.sum((stack == lulc_class) & valid_mask, axis=0)

    mode_index = np.argmax(counts, axis=0)
    mode_values = lulc_classes[mode_index]
    max_counts = np.max(counts, axis=0)
    mode_values[max_counts == 0] = 0
    return mode_values


def get_compute_mode(request, default="gee"):
    compute = str(request.data.get("compute") or default).strip().lower()
    if compute not in VALID_COMPUTE_TYPES:
        raise ValueError("compute must be either 'gee' or 'local'")
    return compute


def select_compute_task(compute, gee_task, local_task):
    return gee_task if compute == "gee" else local_task
