"""Generate Pan-India soil-type raster and vector layers.

Run from the repository root with::

    python -m computing.scripts.generate_pan_india_soil_type

The raster is a 12-band GeoTIFF assembled from the configured soil-property
rasters. The vector retains the Pan-India microwatershed geometries and IDs,
enriched with zonal values from all 12 bands. Both outputs are written directly
under ``data/`` by default.
"""

import argparse
import importlib.util
import logging
import os
import sqlite3
from contextlib import ExitStack
from pathlib import Path


def _configure_proj_data():
    """Use the PROJ database bundled for Rasterio's GDAL/PROJ build."""
    rasterio_spec = importlib.util.find_spec("rasterio")
    if not rasterio_spec or not rasterio_spec.submodule_search_locations:
        return

    rasterio_package_dir = Path(next(iter(rasterio_spec.submodule_search_locations)))
    bundled_proj_dir = rasterio_package_dir / "proj_data"
    if (bundled_proj_dir / "proj.db").is_file():
        os.environ["PROJ_DATA"] = str(bundled_proj_dir)
        os.environ["PROJ_LIB"] = str(bundled_proj_dir)


_configure_proj_data()
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

import django
import fiona
import numpy as np
import rasterio
from django.apps import apps
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window, rasterize
from rasterio.transform import guard_transform
from rasterio.windows import Window, transform as window_transform
from shapely.geometry import mapping

if not apps.ready:
    django.setup()

from computing.config_loader import (
    MICROWATERSHED_PATH,
    PAN_INDIA_OUTER_BOUNDARY_PATH,
    PAN_INDIA_SOIL_TYPE_OUTPUT_DIR,
    SOIL_TYPE_RASTER_PATHS,
)
from computing.local_compute_helper import (
    ensure_file_exists,
    read_validated_vector_file,
)
from computing.soil_type.soil_type_local import (
    SOIL_PROPERTY_SPECS,
)

logger = logging.getLogger(__name__)

DEFAULT_RASTER_NAME = "pan_india_soil_type.tif"
DEFAULT_VECTOR_NAME = "pan_india_soil_type.gpkg"
VECTOR_LAYER_NAME = "pan_india_soil_type"
OUTPUT_NODATA = -9999.0


def _soil_field_schema():
    properties = {}
    for spec in SOIL_PROPERTY_SPECS:
        mapping_values = list(spec.get("mapping", {}).values())
        if spec["aggregation"] == "mean":
            field_type = "float"
        elif mapping_values and all(isinstance(value, int) for value in mapping_values):
            field_type = "int"
        else:
            field_type = "str"
        properties[spec["column"]] = field_type
    return properties


def _iter_spatial_batches(mws_source, raster, batch_size, max_window_pixels):
    batch = []
    batch_bounds = None
    x_resolution = abs(raster.transform.a)
    y_resolution = abs(raster.transform.e)

    for feature in mws_source:
        feature_bounds = fiona.bounds(feature)
        candidate_bounds = (
            feature_bounds
            if batch_bounds is None
            else (
                min(batch_bounds[0], feature_bounds[0]),
                min(batch_bounds[1], feature_bounds[1]),
                max(batch_bounds[2], feature_bounds[2]),
                max(batch_bounds[3], feature_bounds[3]),
            )
        )
        estimated_pixels = (
            max(1, int((candidate_bounds[2] - candidate_bounds[0]) / x_resolution) + 2)
            * max(
                1,
                int((candidate_bounds[3] - candidate_bounds[1]) / y_resolution) + 2,
            )
        )
        if batch and (
            len(batch) >= batch_size or estimated_pixels > max_window_pixels
        ):
            yield batch
            batch = []
            batch_bounds = feature_bounds
        else:
            batch_bounds = candidate_bounds
        batch.append(feature)

    if batch:
        yield batch


def _aggregate_mws_batch(features, raster):
    try:
        window = geometry_window(
            raster,
            [feature["geometry"] for feature in features],
        )
    except WindowError:
        return []

    transform = window_transform(window, raster.transform)
    labels = rasterize(
        (
            (feature["geometry"], index)
            for index, feature in enumerate(features, start=1)
        ),
        out_shape=(int(window.height), int(window.width)),
        transform=transform,
        fill=0,
        dtype="int32",
    )
    band_values = raster.read(window=window)
    flat_labels = labels.ravel()
    feature_count = len(features)
    valid_profile = np.all(np.isfinite(band_values), axis=0)
    if raster.nodata is not None:
        valid_profile &= ~np.any(band_values == raster.nodata, axis=0)
    valid_profile &= np.any(band_values != 0, axis=0)
    profile_pixel_counts = np.bincount(
        flat_labels[valid_profile.ravel()],
        minlength=feature_count + 1,
    )

    aggregated_columns = {}
    for band_index, spec in enumerate(SOIL_PROPERTY_SPECS):
        values = band_values[band_index].ravel()
        valid = (flat_labels > 0) & np.isfinite(values)
        if raster.nodata is not None:
            valid &= values != raster.nodata
        if spec.get("zero_is_nodata", True):
            valid &= values != 0

        if spec["aggregation"] == "mean":
            counts = np.bincount(
                flat_labels[valid],
                minlength=feature_count + 1,
            )
            sums = np.bincount(
                flat_labels[valid],
                weights=values[valid],
                minlength=feature_count + 1,
            )
            results = np.full(feature_count + 1, np.nan, dtype=np.float64)
            np.divide(sums, counts, out=results, where=counts > 0)
            aggregated_columns[spec["column"]] = [
                None if not np.isfinite(value) else round(float(value), 4)
                for value in results[1:]
            ]
            continue

        rounded_values = np.rint(values).astype(np.int32, copy=False)
        best_counts = np.zeros(feature_count + 1, dtype=np.int64)
        best_classes = np.full(feature_count + 1, -1, dtype=np.int32)
        for class_value in sorted(spec["mapping"]):
            class_mask = valid & (rounded_values == class_value)
            class_counts = np.bincount(
                flat_labels[class_mask],
                minlength=feature_count + 1,
            )
            better = class_counts > best_counts
            best_counts[better] = class_counts[better]
            best_classes[better] = class_value
        aggregated_columns[spec["column"]] = [
            spec["mapping"].get(int(class_value)) if count else None
            for class_value, count in zip(best_classes[1:], best_counts[1:])
        ]

    records = []
    for index, feature in enumerate(features):
        if profile_pixel_counts[index + 1] == 0:
            continue
        properties = dict(feature["properties"])
        for column, values in aggregated_columns.items():
            properties[column] = values[index]
        records.append(
            {
                "geometry": feature["geometry"],
                "properties": properties,
            }
        )
    return records


def _source_raster_paths(source_dir=None):
    if source_dir is None:
        return dict(SOIL_TYPE_RASTER_PATHS)

    source_dir = Path(source_dir)
    return {
        property_name: source_dir / configured_path.name
        for property_name, configured_path in SOIL_TYPE_RASTER_PATHS.items()
    }


def _validate_raster_stack(sources, raster_paths):
    reference = sources[0]
    if reference.crs is None:
        raise ValueError(f"Raster CRS is missing: {raster_paths[0]}")
    if reference.count != 1:
        raise ValueError(f"Expected a single-band raster: {raster_paths[0]}")

    for source, raster_path in zip(sources[1:], raster_paths[1:]):
        if source.count != 1:
            raise ValueError(f"Expected a single-band raster: {raster_path}")
        if (
            source.crs != reference.crs
            or source.transform != reference.transform
            or source.width != reference.width
            or source.height != reference.height
        ):
            raise ValueError(
                "Soil rasters must use the same CRS, transform, width, and "
                f"height; {raster_path} does not match {raster_paths[0]}."
            )


def _source_window(output_window, crop_window):
    return Window(
        col_off=int(crop_window.col_off + output_window.col_off),
        row_off=int(crop_window.row_off + output_window.row_off),
        width=int(output_window.width),
        height=int(output_window.height),
    )


def build_pan_india_soil_raster(
    boundary_gdf,
    raster_paths,
    output_path,
    overwrite=False,
):
    """Stack aligned soil rasters and clip every band to the India boundary."""
    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Output already exists: {output_path}. Pass --overwrite to replace it."
        )

    property_names = [spec["column"] for spec in SOIL_PROPERTY_SPECS]
    missing_properties = [name for name in property_names if name not in raster_paths]
    if missing_properties:
        raise ValueError(
            "Missing soil raster paths for: " + ", ".join(missing_properties)
        )

    ordered_paths = [Path(raster_paths[name]) for name in property_names]
    for property_name, raster_path in zip(property_names, ordered_paths):
        ensure_file_exists(raster_path, f"Soil property raster '{property_name}'")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    if temporary_path.exists():
        temporary_path.unlink()

    try:
        with ExitStack() as stack:
            sources = [
                stack.enter_context(rasterio.open(raster_path))
                for raster_path in ordered_paths
            ]
            _validate_raster_stack(sources, ordered_paths)
            reference = sources[0]

            working_boundary = (
                boundary_gdf
                if boundary_gdf.crs == reference.crs
                else boundary_gdf.to_crs(reference.crs)
            )
            shapes = [
                mapping(geometry)
                for geometry in working_boundary.geometry
                if geometry is not None and not geometry.is_empty
            ]
            if not shapes:
                raise ValueError("No valid Pan-India geometry is available for clipping.")

            crop_window = geometry_window(reference, shapes).round_offsets().round_lengths()
            crop_window = crop_window.intersection(
                Window(0, 0, reference.width, reference.height)
            )
            output_transform = guard_transform(
                window_transform(crop_window, reference.transform)
            )
            output_dtype = np.result_type(*[source.dtypes[0] for source in sources])
            profile = reference.profile.copy()
            profile.update(
                driver="GTiff",
                count=len(sources),
                width=int(crop_window.width),
                height=int(crop_window.height),
                transform=output_transform,
                dtype=np.dtype(output_dtype).name,
                nodata=OUTPUT_NODATA,
                compress="lzw",
                tiled=True,
                blockxsize=512,
                blockysize=512,
                BIGTIFF="YES",
            )

            with rasterio.open(temporary_path, "w", **profile) as destination:
                for band_index, property_name in enumerate(property_names, start=1):
                    destination.set_band_description(band_index, property_name)
                    destination.update_tags(
                        band_index,
                        soil_property=property_name,
                        source=str(ordered_paths[band_index - 1]),
                    )

                for _, output_window in destination.block_windows(1):
                    source_window = _source_window(output_window, crop_window)
                    inside_boundary = geometry_mask(
                        shapes,
                        out_shape=(
                            int(output_window.height),
                            int(output_window.width),
                        ),
                        transform=window_transform(
                            source_window,
                            reference.transform,
                        ),
                        invert=True,
                    )
                    for band_index, source in enumerate(sources, start=1):
                        data = source.read(
                            1,
                            window=source_window,
                            masked=True,
                        ).filled(OUTPUT_NODATA)
                        data = np.asarray(data, dtype=output_dtype)
                        data[~np.isfinite(data) | ~inside_boundary] = OUTPUT_NODATA
                        destination.write(data, band_index, window=output_window)

        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise

    logger.info("Created Pan-India soil raster: %s", output_path)
    return output_path


def build_pan_india_soil_vector(
    raster_path,
    mws_path,
    output_path,
    overwrite=False,
    batch_size=500,
    max_window_pixels=2_000_000,
):
    """Attach 12-band soil zonal summaries to Pan-India MWS geometries."""
    raster_path = Path(raster_path)
    mws_path = Path(mws_path)
    ensure_file_exists(raster_path, "Combined Pan-India soil raster")
    ensure_file_exists(mws_path, "Pan-India microwatershed boundary")
    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Output already exists: {output_path}. Pass --overwrite to replace it."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f".{output_path.name}.tmp.gpkg")
    if temporary_path.exists():
        temporary_path.unlink()

    try:
        with rasterio.open(raster_path) as source, fiona.open(mws_path) as mws_source:
            expected_bands = [spec["column"] for spec in SOIL_PROPERTY_SPECS]
            if source.count != len(expected_bands):
                raise ValueError(
                    f"Expected {len(expected_bands)} soil bands in {raster_path}; "
                    f"found {source.count}."
                )
            if list(source.descriptions) != expected_bands:
                raise ValueError(
                    "Combined soil raster band descriptions do not match the "
                    "configured soil properties."
                )
            if source.crs is None:
                raise ValueError(f"Raster CRS is missing: {raster_path}")
            mws_crs = rasterio.crs.CRS.from_user_input(
                mws_source.crs_wkt or mws_source.crs
            )
            if not mws_crs:
                raise ValueError(f"MWS CRS is missing: {mws_path}")
            if mws_crs != source.crs:
                raise ValueError(
                    f"MWS CRS {mws_crs} does not match soil raster CRS {source.crs}."
                )
            if "uid" not in mws_source.schema["properties"]:
                raise ValueError(f"MWS source does not contain a 'uid' field: {mws_path}")

            output_schema = {
                "geometry": mws_source.schema["geometry"],
                "properties": {
                    **dict(mws_source.schema["properties"]),
                    **_soil_field_schema(),
                },
            }
            index_columns = [
                column
                for column in ("uid", "id")
                if column in output_schema["properties"]
            ]
            feature_count = 0
            scanned_count = 0

            with fiona.open(
                temporary_path,
                "w",
                driver="GPKG",
                layer=VECTOR_LAYER_NAME,
                schema=output_schema,
                crs_wkt=source.crs.to_wkt(),
            ) as destination:
                for batch in _iter_spatial_batches(
                    mws_source=mws_source,
                    raster=source,
                    batch_size=batch_size,
                    max_window_pixels=max_window_pixels,
                ):
                    records = _aggregate_mws_batch(batch, source)
                    destination.writerecords(records)
                    scanned_count += len(batch)
                    feature_count += len(records)
                    if scanned_count % 10_000 < len(batch):
                        logger.info(
                            "Processed %s MWS features; wrote %s.",
                            scanned_count,
                            feature_count,
                        )

        with sqlite3.connect(temporary_path) as connection:
            for column in index_columns:
                connection.execute(
                    f'CREATE INDEX IF NOT EXISTS '
                    f'"idx_{VECTOR_LAYER_NAME}_{column}" '
                    f'ON "{VECTOR_LAYER_NAME}" ("{column}")'
                )
            connection.commit()

        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()
        raise

    logger.info(
        "Created Pan-India soil vector with %s MWS features: %s",
        feature_count,
        output_path,
    )
    return output_path


def generate_pan_india_soil_type(
    boundary_path=PAN_INDIA_OUTER_BOUNDARY_PATH,
    mws_path=MICROWATERSHED_PATH,
    source_dir=None,
    output_dir=PAN_INDIA_SOIL_TYPE_OUTPUT_DIR,
    raster_output=None,
    vector_output=None,
    overwrite=False,
    generate_raster=True,
    generate_vector=True,
):
    """Generate one Pan-India multiband raster and MWS-indexed vector."""
    if not generate_raster and not generate_vector:
        raise ValueError("At least one output type must be selected.")

    boundary_path = Path(boundary_path)
    ensure_file_exists(boundary_path, "Pan-India boundary")
    boundary_gdf = read_validated_vector_file(
        boundary_path,
        f"Pan-India boundary has no valid geometries: {boundary_path}",
    )
    if boundary_gdf.crs is None:
        raise ValueError(f"Pan-India boundary CRS is missing: {boundary_path}")

    raster_paths = _source_raster_paths(source_dir)
    output_dir = Path(output_dir)
    raster_output = Path(raster_output or output_dir / DEFAULT_RASTER_NAME)
    vector_output = Path(vector_output or output_dir / DEFAULT_VECTOR_NAME)
    outputs = {}

    if generate_raster:
        outputs["raster"] = str(
            build_pan_india_soil_raster(
                boundary_gdf=boundary_gdf,
                raster_paths=raster_paths,
                output_path=raster_output,
                overwrite=overwrite,
            )
        )
    if generate_vector:
        outputs["vector"] = str(
            build_pan_india_soil_vector(
                raster_path=raster_output,
                mws_path=mws_path,
                output_path=vector_output,
                overwrite=overwrite,
            )
        )

    return outputs


def _build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Generate Pan-India soil-type GeoTIFF and GeoPackage outputs from "
            "the configured soil-property rasters."
        )
    )
    parser.add_argument(
        "--boundary",
        type=Path,
        default=PAN_INDIA_OUTER_BOUNDARY_PATH,
        help="Pan-India clipping boundary GeoJSON/GPKG.",
    )
    parser.add_argument(
        "--mws-boundary",
        type=Path,
        default=MICROWATERSHED_PATH,
        help="Pan-India microwatershed vector containing the uid field.",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        help="Directory containing the 12 soil-property rasters.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PAN_INDIA_SOIL_TYPE_OUTPUT_DIR,
        help="Directory for default raster and vector outputs.",
    )
    parser.add_argument("--raster-output", type=Path)
    parser.add_argument("--vector-output", type=Path)
    parser.add_argument(
        "--raster-only",
        action="store_true",
        help="Generate only the multiband GeoTIFF.",
    )
    parser.add_argument(
        "--vector-only",
        action="store_true",
        help="Generate only the MWS-indexed GeoPackage from the combined raster.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing output files.",
    )
    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.raster_only and args.vector_only:
        parser.error("--raster-only and --vector-only cannot be used together.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    outputs = generate_pan_india_soil_type(
        boundary_path=args.boundary,
        mws_path=args.mws_boundary,
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        raster_output=args.raster_output,
        vector_output=args.vector_output,
        overwrite=args.overwrite,
        generate_raster=not args.vector_only,
        generate_vector=not args.raster_only,
    )
    for output_type, output_path in outputs.items():
        print(f"{output_type}: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
