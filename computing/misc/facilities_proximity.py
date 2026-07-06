"""
Facilities Proximity Layer Generator

Filters village facilities data from GEE by tehsil boundary and exports to GEE asset + GeoServer.
Uses admin boundary clipping (spatial filtering) for fast server-side processing.

GEE Asset: projects/corestack-datasets/assets/datasets/pan_india_facilities
"""

import logging
import time
import tempfile
import zipfile
from datetime import datetime, timezone
from functools import lru_cache
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import pandas as pd
from django.conf import settings

from computing.models import Dataset, LayerType
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    sync_fc_to_geoserver,
)

logger = logging.getLogger(__name__)
from utilities.constants import FACILITIES_GEOSERVER_WORKSPACE, FACILITIES_DATASET_NAME

ADMIN_BOUNDARY_SOURCE_FIELDS = ["state", "district", "tehsil", "vill_ID", "vill_name"]
ADMIN_BOUNDARY_EXPORT_FIELDS = ["state", "district", "tehsil", "censuscode2011", "censusname"]
FACILITIES_STATIC_EXPORT_FIELDS = ["core_admin_uid", "shrid2"]


def _get_facilities_export_fields(facilities_fc):
    """Return the facilities fields that should be copied to the output layer."""
    facilities_property_names = ee.List(
        ee.Feature(facilities_fc.first()).propertyNames()
    )
    distance_fields = facilities_property_names.filter(
        ee.Filter.stringEndsWith("item", "_distance")
    )
    return ee.List(FACILITIES_STATIC_EXPORT_FIELDS).cat(distance_fields).distinct()


def _dissolve_admin_boundary(admin_boundary):
    """
    Merge repeated admin rows with the same village properties into one geometry.

    This preserves full village shapes while preventing split polygon parts from
    producing repeated output rows with identical attributes. Includes a schema
    validation check to discard malformed village rows missing expected properties.
    """
    # Filter out any feature that does not contain ALL required source fields
    def filter_complete_schemas(feature):
        props = feature.propertyNames()
        has_all_fields = ee.List(ADMIN_BOUNDARY_SOURCE_FIELDS).map(
            lambda field: props.contains(field)
        ).reduce(ee.Reducer.min()) 
        return feature.set('has_complete_schema', has_all_fields)

    filtered_admin = admin_boundary.map(filter_complete_schemas).filter(
        ee.Filter.eq('has_complete_schema', 1)
    )

    admin_export_fc = filtered_admin.select(
        ADMIN_BOUNDARY_SOURCE_FIELDS,
        ADMIN_BOUNDARY_EXPORT_FIELDS,
    )
    unique_admin_fc = admin_export_fc.distinct(ADMIN_BOUNDARY_EXPORT_FIELDS)

    def merge_duplicate_geometries(feature):
        feature = ee.Feature(feature)
        duplicate_filter = ee.Filter.And(
            ee.Filter.eq("state", feature.get("state")),
            ee.Filter.eq("district", feature.get("district")),
            ee.Filter.eq("tehsil", feature.get("tehsil")),
            ee.Filter.eq("censuscode2011", feature.get("censuscode2011")),
            ee.Filter.eq("censusname", feature.get("censusname")),
        )
        dissolved_geometry = admin_export_fc.filter(duplicate_filter).geometry()
        return ee.Feature(dissolved_geometry).copyProperties(
            feature,
            ADMIN_BOUNDARY_EXPORT_FIELDS,
        )

    return unique_admin_fc.map(merge_duplicate_geometries)


def _build_facilities_output_fc(admin_boundary, facilities_fc):
    """
    Preserve admin-boundary geometry and attach facilities metrics after a fast
    spatial clip.

    The exported layer keeps polygon shapes and core hierarchy columns from the
    admin-boundary asset, while copying the facilities distance metrics plus the
    requested identifier fields from the pan-India facilities asset.
    """
    facilities_export_fields = _get_facilities_export_fields(facilities_fc)
    clipped_facilities = facilities_fc.filterBounds(admin_boundary.geometry()).select(
        ee.List(["censuscode2011"]).cat(facilities_export_fields)
    )
    admin_export_fc = _dissolve_admin_boundary(admin_boundary)
    admin_census_codes = ee.List(admin_export_fc.aggregate_array("censuscode2011")).distinct()
    clipped_facilities = clipped_facilities.filter(
        ee.Filter.inList("censuscode2011", admin_census_codes)
    )
    join_filter = ee.Filter.equals(
        leftField="censuscode2011",
        rightField="censuscode2011",
    )
    joined_fc = ee.FeatureCollection(
        ee.Join.saveFirst(matchKey="facility_match", outer=True).apply(
            admin_export_fc,
            clipped_facilities,
            join_filter,
        )
    )

    def attach_facilities_metrics(feature):
        feature = ee.Feature(feature)
        facility_match = feature.get("facility_match")
        admin_feature = feature.select(ADMIN_BOUNDARY_EXPORT_FIELDS)
        return ee.Feature(
            ee.Algorithms.If(
                facility_match,
                admin_feature.copyProperties(
                    ee.Feature(facility_match),
                    facilities_export_fields,
                ),
                admin_feature,
            )
            first = False

        if first:
            raise ValueError("No non-empty facilities outputs were produced")
        shutil.move(str(temp_gpkg_path), gpkg_path)

    return gpkg_path, row_counts


def _write_tehsil_summary_csv(
    *,
    output_dir: Path,
    bundle_stem: str,
    selected_outputs: tuple[str, ...],
    output_results: dict[str, dict[str, Any]],
    row_counts: dict[str, int],
    source_path: Path,
    facilities_source: str | None,
    gpkg_path: Path,
    resolved_state: str,
    resolved_district: str,
    resolved_block: str,
    source_village_geometry: str,
) -> Path:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    for output_key in selected_outputs:
        result = output_results[output_key]
        rows.append(
            {
                "output_key": output_key,
                "local_gpkg_layer": result["local_layer"],
                "row_count": row_counts.get(output_key, 0),
                "geoserver_layer_name": result["geoserver_layer_name"],
                "local_gpkg_path": gpkg_path.as_posix(),
                "source_proximity_gpkg": source_path.as_posix(),
                "facilities_source": facilities_source,
                "source_village_layer": SOURCE_VILLAGE_LAYER,
                "source_village_geometry": source_village_geometry,
                "state_name": resolved_state,
                "district_name": resolved_district,
                "tehsil": resolved_block,
                "generated_at_utc": generated_at,
            }
        )
    csv_path = output_dir / f"{bundle_stem}.summary.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    return csv_path


def _write_single_layer_gpkg(gdf, output_dir: Path, layer_name: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = output_dir / f"{layer_name}.gpkg"
    zip_path = gpkg_path.with_suffix(".zip")
    for path in (gpkg_path, zip_path):
        if path.exists():
            path.unlink()
    with tempfile.TemporaryDirectory(prefix="facilities_geoserver_gpkg_") as temp_dir:
        temp_gpkg_path = Path(temp_dir) / gpkg_path.name
        _write_layer(gdf, temp_gpkg_path, layer_name, "w")
        shutil.move(str(temp_gpkg_path), gpkg_path)
    _zip_gpkg(gpkg_path)
    return gpkg_path


def _publish_to_geoserver(gpkg_path: Path, layer_name: str, overwrite: bool) -> dict[str, Any]:
    try:
        geoserver = Geoserver()
        try:
            geoserver.get_workspace(FACILITIES_GEOSERVER_WORKSPACE)
        except GeoserverException as exc:
            if exc.status != 404:
                raise
            geoserver.create_workspace(FACILITIES_GEOSERVER_WORKSPACE)

        zip_path = gpkg_path.with_suffix(".zip")
        if not zip_path.exists():
            _zip_gpkg(gpkg_path)
        if overwrite:
            geoserver.delete_vector_store(
                workspace=FACILITIES_GEOSERVER_WORKSPACE,
                store=layer_name,
            )
        response = geoserver.create_shp_datastore(
            path=str(zip_path),
            store_name=layer_name,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            file_extension="gpkg",
        )
        return {"ok": True, "response": response}
    except Exception as exc:
        logger.exception("Facilities GeoServer publish failed")
        return {
            "ok": False,
            "error_type": exc.__class__.__name__,
            "error": str(exc)[:500],
        }


def _publish_outputs_to_geoserver(
    output_layers: dict[str, Any],
    district: str,
    block: str,
    overwrite: bool,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="facilities_geoserver_layers_") as temp_dir:
        publish_dir = Path(temp_dir)
        for output_key, gdf in output_layers.items():
            layer_name = _geoserver_layer_name(output_key, district, block)
            if gdf.empty:
                results[output_key] = {
                    "ok": False,
                    "layer_name": layer_name,
                    "error": "Output layer is empty",
                }
                continue
            gpkg_path = _write_single_layer_gpkg(gdf, publish_dir, layer_name)
            result = _publish_to_geoserver(gpkg_path, layer_name, overwrite)
            result["layer_name"] = layer_name
            results[output_key] = result
    return results


def _create_or_refresh_layer_group(layer_names: list[str], district: str, block: str) -> bool:
    if len(layer_names) <= 1:
        return False
    group_name = _layer_name(district, block)
    geoserver = Geoserver()
    try:
        try:
            geoserver.delete_layergroup(group_name, workspace=FACILITIES_GEOSERVER_WORKSPACE)
        except Exception:
            pass
        geoserver.create_layergroup(
            name=group_name,
            mode="named",
            title=f"Facilities {district} {block}",
            abstract_text="Facilities inventory, nearest-service, and village service layers.",
            layers=layer_names,
            workspace=FACILITIES_GEOSERVER_WORKSPACE,
            formats="json",
            keywords=["facilities", "inventory", "nearest", "village_service"],
        )
        return True
    except Exception:
        logger.exception("Facilities GeoServer layer group creation failed")
        return False


def _register_layer(
    state: str,
    district: str,
    block: str,
    layer_name: str,
    geoserver_url: str,
    output: dict[str, Any],
    overwrite: bool,
) -> tuple[int | None, dict[str, Any] | None]:
    try:
        Dataset.objects.get_or_create(
            name=FACILITIES_DATASET_NAME,
            defaults={
                "layer_type": LayerType.VECTOR,
                "workspace": FACILITIES_GEOSERVER_WORKSPACE,
                "is_active": True,
            },
        )
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=geoserver_url,
            dataset_name=FACILITIES_DATASET_NAME,
            algorithm=ALGORITHM,
            algorithm_version=ALGORITHM_VERSION,
            misc=output,
            is_override=_bool(overwrite),
            is_gee_asset=False,
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        return layer_id, None
    except Exception as exc:
        logger.exception("Facilities DB registration failed")
        return None, {"error_type": exc.__class__.__name__, "error": str(exc)[:500]}


def _publish_and_register_outputs(
    *,
    output_layers: dict[str, Any],
    output_dir: Path,
    district: str,
    block: str,
    resolved_state: str,
    resolved_district: str,
    resolved_block: str,
    source_path: Path,
    facilities_source: str | None,
    gpkg_path: Path,
    row_counts: dict[str, int],
    output_results: dict[str, dict[str, Any]],
    overwrite: bool,
    register_layers: bool,
) -> tuple[dict[str, dict[str, Any]], bool]:
    geoserver = _publish_outputs_to_geoserver(
        output_layers=output_layers,
        district=district,
        block=block,
        overwrite=_bool(overwrite),
    )
    successful_layers: list[str] = []
    for output_key, publish_result in geoserver.items():
        if not publish_result.get("ok"):
            output_results[output_key]["registration_error"] = publish_result
            continue
        geoserver_layer_name = publish_result["layer_name"]
        geoserver_url = (
            f"{settings.GEOSERVER_URL.rstrip('/')}/{FACILITIES_GEOSERVER_WORKSPACE}/ows"
            "?service=WFS&version=1.0.0&request=GetFeature"
            f"&typeName={FACILITIES_GEOSERVER_WORKSPACE}:{geoserver_layer_name}"
            "&outputFormat=application/json"
        )
        layer_output = {
            "is_generated_locally": True,
            "source": source_path.as_posix(),
            "facilities_source": facilities_source,
            "gpkg_path": gpkg_path.as_posix(),
            "output_dir": output_dir.as_posix(),
            "row_counts": row_counts,
            "output_key": output_key,
            "local_gpkg_layer": LOCAL_GPKG_LAYER_NAMES[output_key],
            "geoserver_workspace": FACILITIES_GEOSERVER_WORKSPACE,
            "geoserver_layer_name": geoserver_layer_name,
            "geoserver_url": geoserver_url,
        }
        layer_id = None
        registration_error = None
        if register_layers:
            layer_id, registration_error = _register_layer(
                resolved_state,
                resolved_district,
                resolved_block,
                geoserver_layer_name,
                geoserver_url,
                layer_output,
                _bool(overwrite),
            )
        output_results[output_key].update(
            {
                "geoserver_url": geoserver_url,
                "layer_id": layer_id,
                "registration_error": registration_error,
            }
        )
        successful_layers.append(geoserver_layer_name)
    layer_group_created = _create_or_refresh_layer_group(
        successful_layers,
        district,
        block,
    )
    return geoserver, layer_group_created


def generate_facilities_proximity(
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    outputs: Any = "all",
    register_layers: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    timings: dict[str, float] = {}
    layer_name = _layer_name(district, block)
    selected_outputs = _parse_outputs(outputs)
    bundle_stem = _bundle_stem(layer_name, selected_outputs)
    output_dir = _output_dir(state, district, block)
    source_path = _source_path()
    logger.info(
        "Facilities proximity started: %s/%s/%s outputs=%s",
        state,
        district,
        block,
        selected_outputs,
    )

    resolved_state = _canonical_asset_name(state)
    resolved_district = _canonical_asset_name(district)
    resolved_block = _canonical_asset_name(block)
    try:
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading villages: %s/%s/%s", resolved_state, resolved_district, resolved_block)
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    except ValueError:
        resolved_state, resolved_district, resolved_block = _resolve_location(state, district, block)
        logger.info("Facilities proximity resolved location: %s/%s/%s", resolved_state, resolved_district, resolved_block)
        villages = _read_villages(resolved_state, resolved_district, resolved_block)
    timings["read_villages_seconds"] = round(time.perf_counter() - t0, 3)
    logger.info("Facilities proximity read %d villages in %.3fs", len(villages), timings["read_villages_seconds"])

    village_ids = villages[VILLAGE_ID_COL].dropna().astype(str).drop_duplicates().tolist()
    output_layers: dict[str, Any] = {}
    proximity_outputs = {OUTPUT_NEAREST, OUTPUT_VILLAGE_SERVICE}
    l3_attributes = pd.DataFrame()
    l2_attributes = pd.DataFrame()

    if OUTPUT_INVENTORY in selected_outputs:
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading inventory rows for %d villages", len(village_ids))
        output_layers[OUTPUT_INVENTORY] = _read_inventory_facilities(villages)
        timings["read_inventory_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read inventory rows in %.3fs: inventory=%d",
            timings["read_inventory_seconds"],
            len(output_layers[OUTPUT_INVENTORY]),
        )

    if proximity_outputs.intersection(selected_outputs):
        t0 = time.perf_counter()
        logger.info("Facilities proximity reading L3/L2 rows for %d villages", len(village_ids))
        l3_attributes = _read_l3_attributes(source_path, village_ids)
        l2_attributes = _read_l2_attributes(source_path, village_ids)
        timings["read_proximity_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read proximity rows in %.3fs: l3=%d l2=%d",
            timings["read_proximity_seconds"],
            len(l3_attributes),
            len(l2_attributes),
        )

    if OUTPUT_NEAREST in selected_outputs:
        t0 = time.perf_counter()
        nearest_facility_uids = (
            pd.concat(
                [
                    l3_attributes["nearest_facility_uid"],
                    l2_attributes["nearest_facility_uid"],
                ],
                ignore_index=True,
            )
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )
        nearest_facilities = _read_nearest_facilities(source_path, nearest_facility_uids)
        timings["read_nearest_facilities_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "Facilities proximity read distinct nearest facilities in %.3fs: nearest=%d",
            timings["read_nearest_facilities_seconds"],
            len(nearest_facilities),
        )
        t0 = time.perf_counter()
        output_layers[OUTPUT_NEAREST] = _build_nearest_service(
            villages,
            l3_attributes,
            l2_attributes,
            nearest_facilities,
        )
        timings["build_nearest_seconds"] = round(time.perf_counter() - t0, 3)

    if OUTPUT_VILLAGE_SERVICE in selected_outputs:
        t0 = time.perf_counter()
        output_layers[OUTPUT_VILLAGE_SERVICE] = _build_village_service(
            villages,
            l3_attributes,
            l2_attributes,
        )
        timings["build_village_service_seconds"] = round(time.perf_counter() - t0, 3)

    t0 = time.perf_counter()
    logger.info("Facilities proximity writing output GPKG: %s", output_dir / f"{bundle_stem}.gpkg")
    gpkg_path, row_counts = _write_tehsil_gpkg(
        output_layers=output_layers,
        output_dir=output_dir,
        layer_name=bundle_stem,
    )
    timings["write_gpkg_seconds"] = round(time.perf_counter() - t0, 3)
    logger.info("Facilities proximity wrote GPKG in %.3fs: %s", timings["write_gpkg_seconds"], gpkg_path)
    source_village_geometry = str(villages.geometry.geom_type.mode().iloc[0])

    geoserver: dict[str, dict[str, Any]] | None = None
    geoserver_layer_group_created = False
    facilities_source = _facilities_path().as_posix() if OUTPUT_INVENTORY in selected_outputs else None
    output_results = _output_results_template(selected_outputs, row_counts, district, block)
    summary_csv_path = _write_tehsil_summary_csv(
        output_dir=output_dir,
        bundle_stem=bundle_stem,
        selected_outputs=selected_outputs,
        output_results=output_results,
        row_counts=row_counts,
        source_path=source_path,
        facilities_source=facilities_source,
        gpkg_path=gpkg_path,
        resolved_state=resolved_state,
        resolved_district=resolved_district,
        resolved_block=resolved_block,
        source_village_geometry=source_village_geometry,
    )
    for output in output_results.values():
        output["local_summary_csv"] = summary_csv_path.as_posix()
    if _bool(sync_to_geoserver):
        t0 = time.perf_counter()
        logger.info("Facilities proximity publishing selected outputs to GeoServer: %s", selected_outputs)
        geoserver, geoserver_layer_group_created = _publish_and_register_outputs(
            output_layers=output_layers,
            output_dir=output_dir,
            district=district,
            block=block,
            resolved_state=resolved_state,
            resolved_district=resolved_district,
            resolved_block=resolved_block,
            source_path=source_path,
            facilities_source=facilities_source,
            gpkg_path=gpkg_path,
            row_counts=row_counts,
            output_results=output_results,
            overwrite=_bool(overwrite),
            register_layers=_bool(register_layers),
        )
        timings["publish_geoserver_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info("Facilities proximity GeoServer publish finished in %.3fs: %s", timings["publish_geoserver_seconds"], geoserver)

    elapsed = round(time.perf_counter() - started, 3)
    timings["total_seconds"] = elapsed
    logger.info("Facilities proximity completed %s in %.3fs", layer_name, elapsed)
    return {
        "status": "success",
        "layer_name": layer_name,
        "bundle_name": bundle_stem,
        "selected_outputs": list(selected_outputs),
        "outputs": output_results,
        "row_counts": row_counts,
        "source": source_path.as_posix(),
        "facilities_source": facilities_source,
        "source_village_layer": SOURCE_VILLAGE_LAYER,
        "source_village_geometry": source_village_geometry,
        "gpkg_path": gpkg_path.as_posix(),
        "summary_csv_path": summary_csv_path.as_posix(),
        "output_dir": output_dir.as_posix(),
        "state_name": resolved_state,
        "district_name": resolved_district,
        "tehsil": resolved_block,
        "sync_to_geoserver": _bool(sync_to_geoserver),
        "register_layers": _bool(register_layers),
        "geoserver": geoserver,
        "geoserver_layer_group_created": geoserver_layer_group_created,
        "timings": timings,
        "elapsed_seconds": elapsed,
    }


@app.task(bind=True)
def generate_facilities_proximity_task(
    self,
    state: str,
    district: str,
    block: str,
    sync_to_geoserver: bool = True,
    overwrite: bool = False,
    outputs: Any = "all",
    register_layers: bool = True,
    *_ignored_args: Any,
    **_ignored: Any,
) -> dict[str, Any]:
    """Celery wrapper for the local facilities proximity export."""
    return generate_facilities_proximity(
        state=state,
        district=district,
        block=block,
        sync_to_geoserver=sync_to_geoserver,
        overwrite=overwrite,
        outputs=outputs,
        register_layers=register_layers,
    )
