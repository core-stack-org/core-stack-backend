import csv
import datetime as dt
import json
import math
import os
from collections import Counter, defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from nrm_app.celery import app
from rasterio.features import geometry_mask
from rasterio.windows import Window, from_bounds
from shapely.geometry import box, mapping

from computing.config_loader import (
    AQUIFER_VECTOR_PATH,
    HYDROLOGY_LOCAL_OUTPUT_DIR,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    PROJECT_ROOT,
)
from computing.hydrology_gpu.watershed_boundary import (
    find_pan_india_watersheds,
    find_tehsil_watershed,
)
from computing.hydrology_gpu.et_download import (
    FLDAS_CA_DAILY_PATCH_FILLED_SOURCE,
    FLDAS_CA_DAILY_SOURCE,
    FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE,
    FLDAS_GLOBAL_MONTHLY_SOURCE,
)
from computing.local_compute_helper import (
    build_output_vector_path,
    load_precomputed_watersheds,
    push_local_vector_to_geoserver,
    read_validated_vector_file,
    write_vector_output,
)
from computing.misc.aquifer_vector_local import (
    _compute_aquifer_properties_for_watersheds,
    _prepare_aquifers_for_intersection,
)
from computing.mws.runoff_gpu import (
    HYDROLOGY_OUTPUT_ROOT,
    PAN_INDIA_RUNOFF_OUTPUT_ROOT,
    PAN_INDIA_RUNOFF_TIMESERIES_DIR_NAME,
)
from computing.mws.et_download import PAN_INDIA_ET_OUTPUT_ROOT
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import valid_gee_text


GEOSERVER_WORKSPACE = "mws_layers"
LOCAL_ALGORITHM = "local_hydrology"
LOCAL_ALGORITHM_VERSION = "local-1.0"
SECONDS_PER_DAY = 86400.0
CACHE_UID_COLUMN = "uid"
CACHE_ET_SOURCE_COLUMN = "et_source"
CACHE_ET_SOURCE_SIGNATURE_COLUMN = "et_source_signature"
CACHE_ET_ERROR_COLUMN = "et_error"
SOURCE_YEARS_COLUMN = "source_years"
FORTNIGHT_ANCHOR_DATE = dt.date(2017, 7, 1)
HYDROLOGY_BASE_LAYER_ROOT = PROJECT_ROOT / "data" / "base_layers" / "hydrology"


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _normalize_location(value, field_name):
    if value is None or not str(value).strip():
        raise ValueError(f"{field_name} is required")
    return str(value).strip().lower()


def _year_key(year):
    return f"{year}_{year + 1}"


def _source_year_for_date(day):
    boundary = dt.date(day.year, 7, 1)
    return day.year if day >= boundary else day.year - 1


def _source_years_for_period(start_date, end_date):
    years = set()
    current = start_date
    while current < end_date:
        years.add(_source_year_for_date(current))
        current += dt.timedelta(days=1)
    return sorted(years)


def _source_years_for_periods(periods):
    years = set()
    for start_date, end_date, _ in periods:
        years.update(_source_years_for_period(start_date, end_date))
    return sorted(years)


def _cache_root(output_base_dir):
    return Path(output_base_dir) / "cache"


def _et_cache_root(year):
    return Path(PAN_INDIA_ET_OUTPUT_ROOT) / _year_key(year) / "cache"


def _aquifer_cache_path(output_base_dir):
    return (
        Path(HYDROLOGY_BASE_LAYER_ROOT)
        / "aquifer"
        / "cache"
        / "aquifer_by_uid.parquet"
    )


def _et_cache_path(output_base_dir, year, is_annual):
    period = "annual" if is_annual else "fortnight"
    return _et_cache_root(year) / f"{period}.parquet"


def _et_aggregate_root(output_base_dir, year, is_annual):
    period = "annual" if is_annual else "fortnight"
    return _et_cache_root(year) / "rasters" / period


def _base_layer_period_name(is_annual):
    return "annual" if is_annual else "fortnightly"


def _base_layer_name(year, is_annual):
    return f"hydrology_{_base_layer_period_name(is_annual)}_{_year_key(year)}"


def _base_layer_path(base_layer_root, year, is_annual):
    return (
        Path(base_layer_root)
        / _base_layer_period_name(is_annual)
        / f"{_base_layer_name(year, is_annual)}.gpkg"
    )


def _write_parquet_atomic(frame, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    frame.to_parquet(temporary_path, index=False)
    temporary_path.replace(path)


def _layer_name(district, block, is_annual):
    suffix = "_".join(
        [
            valid_gee_text(district.lower()),
            valid_gee_text(block.lower()),
        ]
    )
    prefix = "deltaG_well_depth_" if is_annual else "deltaG_fortnight_"
    return prefix + suffix


def _build_periods(year, is_annual):
    start = dt.date(year, 7, 1)
    end = dt.date(year + 1, 7, 1)
    if is_annual:
        return [(start, end, _year_key(year))]
    if year < FORTNIGHT_ANCHOR_DATE.year:
        raise ValueError(
            "Fortnightly hydrology starts from the 2017 agricultural year"
        )

    periods = []
    current = FORTNIGHT_ANCHOR_DATE
    for previous_year in range(FORTNIGHT_ANCHOR_DATE.year, year):
        previous_end = dt.date(previous_year + 1, 7, 1)
        while current + dt.timedelta(days=14) <= previous_end:
            current += dt.timedelta(days=14)

    while current + dt.timedelta(days=14) <= end:
        period_end = current + dt.timedelta(days=14)
        periods.append((current, period_end, current.isoformat()))
        current = period_end
    return periods


def _resolve_base_layer_year_bounds(year=None, start_year=None, end_year=None):
    if year is not None and (start_year is not None or end_year is not None):
        raise ValueError("Provide start_year/end_year or year, not both")

    if year is not None:
        start_year = int(year)
        end_year = start_year + 1
    else:
        if start_year is None or end_year is None:
            raise ValueError("start_year and end_year are required")
        start_year = int(start_year)
        end_year = int(end_year)

    if start_year < FORTNIGHT_ANCHOR_DATE.year:
        raise ValueError("Hydrology base layers can only be generated from 2017")
    if end_year <= start_year:
        raise ValueError("end_year must be greater than start_year")
    if end_year != start_year + 1:
        raise ValueError(
            "Hydrology base-layer generation supports one hydrological year "
            "at a time; use end_year=start_year+1"
        )
    return start_year, end_year


def _resolve_year_inputs(
    year,
    hydrology_output_root=HYDROLOGY_OUTPUT_ROOT,
):
    year_key = _year_key(year)
    runoff_roots = [
        Path(PAN_INDIA_RUNOFF_OUTPUT_ROOT),
        Path(hydrology_output_root) / "pan_india",
    ]
    timeseries_dir = None
    for runoff_root in runoff_roots:
        candidate = (
            runoff_root
            / year_key
            / PAN_INDIA_RUNOFF_TIMESERIES_DIR_NAME
            / "pan_india_timeseries_tile_series"
        )
        if candidate.exists():
            timeseries_dir = candidate
            break

    et_roots = [
        Path(PAN_INDIA_ET_OUTPUT_ROOT) / year_key,
        Path(hydrology_output_root) / "pan_india" / year_key / "et",
    ]
    et_root = next((path for path in et_roots if path.exists()), None)

    if timeseries_dir is None:
        available_years = sorted(
            {
                path.name
                for runoff_root in runoff_roots
                for path in runoff_root.glob("*_*")
                if (
                    path
                    / PAN_INDIA_RUNOFF_TIMESERIES_DIR_NAME
                    / "pan_india_timeseries_tile_series"
                ).is_dir()
            }
        )
        raise FileNotFoundError(
            "Pan-India rainfall/runoff timeseries not found for "
            f"{year_key}: "
            f"{PAN_INDIA_RUNOFF_OUTPUT_ROOT / year_key / PAN_INDIA_RUNOFF_TIMESERIES_DIR_NAME / 'pan_india_timeseries_tile_series'}. "
            f"Available Pan-India runoff years: {available_years or 'none'}."
        )
    if et_root is None:
        available_et_years = sorted(
            path.name
            for path in Path(PAN_INDIA_ET_OUTPUT_ROOT).glob("*_*")
            if path.is_dir()
        )
        raise FileNotFoundError(
            "Pan-India ET folder not found for "
            f"{year_key}: {PAN_INDIA_ET_OUTPUT_ROOT / year_key}. "
            f"Available Pan-India ET years: {available_et_years or 'none'}. "
            "Run et_download for this hydrological year first."
        )
    return timeseries_dir, et_root


def _resolve_source_year_inputs(
    *,
    output_year,
    periods,
    hydrology_output_root=HYDROLOGY_OUTPUT_ROOT,
):
    source_inputs = {}
    for source_year in _source_years_for_periods(periods):
        try:
            source_inputs[source_year] = _resolve_year_inputs(
                year=source_year,
                hydrology_output_root=hydrology_output_root,
            )
        except FileNotFoundError as error:
            raise FileNotFoundError(
                f"Hydrology output year {_year_key(output_year)} requires "
                f"source year {_year_key(source_year)} because fortnight "
                f"windows are anchored at {FORTNIGHT_ANCHOR_DATE.isoformat()}."
            ) from error
    return source_inputs


def _pan_india_watershed_offset(
    state,
    district,
    block,
    watershed_root=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    root = Path(watershed_root)
    target_path, target_row = find_tehsil_watershed(
        root,
        state,
        district,
        block,
    )
    target_path = target_path.resolve()
    offset = 0

    matches = find_pan_india_watersheds(root)
    for match_index, (source_path, row) in enumerate(matches):
        feature_count = int(row.get("feature_count") or 0)
        if source_path.resolve() == target_path:
            return (
                matches,
                match_index,
                offset,
                feature_count,
                str(target_path),
            )
        offset += feature_count

    raise ValueError(
        "The requested watershed file is not part of the Pan-India runoff "
        f"boundary manifest: {target_path}. Manifest row: {target_row}"
    )


def _pan_india_series_path(series_dir, watershed_id):
    return Path(series_dir) / f"{watershed_id // 1000:04d}" / f"{watershed_id}.csv"


def _pan_india_series_paths_exist(series_dirs, watershed_id):
    return all(
        _pan_india_series_path(series_dir, watershed_id).exists()
        for series_dir in series_dirs
    )


def _read_pan_india_series(series_dir, watershed_id):
    path = _pan_india_series_path(series_dir, watershed_id)
    if not path.exists():
        return {}, path

    data = defaultdict(lambda: [0.0, 0, 0.0, 0])
    with path.open(newline="") as source:
        for row in csv.reader(source):
            if len(row) != 5:
                raise ValueError(
                    f"Invalid Pan-India rainfall/runoff row in {path}: {row}"
                )
            timestamp, rainfall_sum, rainfall_count, runoff_sum, runoff_count = row
            values = data[timestamp]
            values[0] += float(rainfall_sum)
            values[1] += int(rainfall_count)
            values[2] += float(runoff_sum)
            values[3] += int(runoff_count)

    timeseries = {}
    for timestamp in sorted(data):
        rainfall_sum, rainfall_count, runoff_sum, runoff_count = data[timestamp]
        values = {}
        if rainfall_count:
            values["Rainfall"] = rainfall_sum / rainfall_count
        if runoff_count:
            values["Runoff"] = runoff_sum / runoff_count
        if values:
            timeseries[timestamp] = values
    return timeseries, path


def _read_pan_india_series_from_dirs(series_dirs, watershed_id):
    combined = {}
    missing_paths = []
    for series_dir in series_dirs:
        timeseries, path = _read_pan_india_series(series_dir, watershed_id)
        if path.exists():
            combined.update(timeseries)
        else:
            missing_paths.append(path)
    return combined, missing_paths


def _resolve_duplicate_pan_india_ids(
    *,
    matches,
    target_match_index,
    target_offset,
    target_count,
    unresolved_uids,
    series_dirs,
):
    resolved = {}
    remaining = set(unresolved_uids)
    source_offset = target_offset + target_count

    for source_path, row in matches[target_match_index + 1 :]:
        feature_count = int(row.get("feature_count") or 0)
        uid_frame = gpd.read_file(
            source_path,
            columns=["uid"],
            ignore_geometry=True,
        )
        for position, uid in enumerate(uid_frame["uid"].astype(str)):
            if uid not in remaining:
                continue
            watershed_id = source_offset + position + 1
            if _pan_india_series_paths_exist(series_dirs, watershed_id):
                resolved[uid] = watershed_id
                remaining.remove(uid)
        if not remaining:
            break
        source_offset += feature_count

    return resolved


def _attach_pan_india_timeseries(
    watersheds_gdf,
    *,
    state,
    district,
    block,
    series_dirs,
    watershed_root=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    watershed_ids=None,
    uid_to_watershed_id=None,
):
    if "uid" not in watersheds_gdf.columns:
        raise ValueError("Precomputed watershed vector must contain uid")

    series_dirs = [Path(series_dir) for series_dir in series_dirs]
    if not series_dirs:
        raise ValueError(
            "At least one Pan-India rainfall/runoff series folder is required"
        )

    year_gdf = watersheds_gdf.copy()
    year_gdf["uid"] = year_gdf["uid"].astype(str)
    if year_gdf["uid"].duplicated().any():
        raise ValueError("Duplicate uid values found in precomputed watersheds")

    if uid_to_watershed_id is not None:
        watershed_source = "Pan-India watershed manifest"
        watershed_ids = [uid_to_watershed_id.get(uid) for uid in year_gdf["uid"]]
    else:
        (
            matches,
            target_match_index,
            offset,
            expected_count,
            watershed_source,
        ) = _pan_india_watershed_offset(
            state,
            district,
            block,
            watershed_root=watershed_root,
        )
        if len(year_gdf) != expected_count:
            raise ValueError(
                "Precomputed watershed count differs from the Pan-India runoff "
                f"manifest for {watershed_source}: vector={len(year_gdf)}, "
                f"manifest={expected_count}"
            )

    if watershed_ids is None:
        watershed_ids = [offset + position + 1 for position in range(len(year_gdf))]
        unresolved_positions = [
            position
            for position, watershed_id in enumerate(watershed_ids)
            if not _pan_india_series_paths_exist(series_dirs, watershed_id)
        ]
        if unresolved_positions:
            unresolved_uids = {
                year_gdf.iloc[position]["uid"] for position in unresolved_positions
            }
            duplicate_ids = _resolve_duplicate_pan_india_ids(
                matches=matches,
                target_match_index=target_match_index,
                target_offset=offset,
                target_count=expected_count,
                unresolved_uids=unresolved_uids,
                series_dirs=series_dirs,
            )
            for position in unresolved_positions:
                uid = year_gdf.iloc[position]["uid"]
                if uid in duplicate_ids:
                    watershed_ids[position] = duplicate_ids[uid]
    elif len(watershed_ids) != len(year_gdf):
        raise ValueError(
            "Pan-India watershed ID count differs from precomputed "
            f"watersheds: ids={len(watershed_ids)}, "
            f"watersheds={len(year_gdf)}"
        )

    timeseries_values = []
    missing_paths = []
    for uid, watershed_id in zip(year_gdf["uid"], watershed_ids):
        if watershed_id is None:
            timeseries_values.append({})
            missing_paths.append(f"uid={uid} has no Pan-India runoff series")
            continue
        timeseries, missing_for_uid = _read_pan_india_series_from_dirs(
            series_dirs,
            watershed_id,
        )
        timeseries_values.append(timeseries)
        missing_paths.extend(str(path) for path in missing_for_uid)

    year_gdf["timeseries"] = timeseries_values
    return (
        year_gdf,
        watershed_source,
        missing_paths,
        watershed_ids,
    )


def _available_pan_india_series_ids(series_dirs):
    available_ids = None
    for series_dir in series_dirs:
        current_ids = {
            int(path.stem)
            for path in Path(series_dir).glob("*/*.csv")
            if path.stem.isdigit()
        }
        available_ids = (
            current_ids if available_ids is None else available_ids & current_ids
        )
    return available_ids or set()


def _build_pan_india_uid_index(
    series_dirs,
    watershed_root=PRECOMPUTED_TEHSIL_WATERSHED_DIR,
):
    available_ids = _available_pan_india_series_ids(series_dirs)
    if not available_ids:
        raise FileNotFoundError(
            "No common Pan-India rainfall/runoff CSV files were found for "
            f"the requested years: {[str(path) for path in series_dirs]}"
        )

    uid_to_watershed_id = {}
    offset = 0
    matches = find_pan_india_watersheds(Path(watershed_root))
    for source_path, row in matches:
        feature_count = int(row.get("feature_count") or 0)
        uid_frame = gpd.read_file(
            source_path,
            columns=["uid"],
            ignore_geometry=True,
        )
        if len(uid_frame) != feature_count:
            raise ValueError(
                "Watershed count differs from the Pan-India manifest for "
                f"{source_path}: vector={len(uid_frame)}, "
                f"manifest={feature_count}"
            )
        for position, uid in enumerate(uid_frame["uid"].astype(str)):
            watershed_id = offset + position + 1
            if watershed_id in available_ids:
                uid_to_watershed_id[uid] = watershed_id
        offset += feature_count

    print(
        "Built Pan-India rainfall/runoff index for "
        f"{len(uid_to_watershed_id)} unique watersheds"
    )
    return matches, uid_to_watershed_id


def _read_complete_uid_cache(path, required_uids, value_columns):
    path = Path(path)
    if not path.exists():
        return None

    frame = pd.read_parquet(path)
    required_columns = {CACHE_UID_COLUMN, *value_columns}
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        print(
            f"Ignoring incomplete cache {path}; missing columns: "
            f"{sorted(missing_columns)}"
        )
        return None

    frame[CACHE_UID_COLUMN] = frame[CACHE_UID_COLUMN].astype(str)
    if frame[CACHE_UID_COLUMN].duplicated().any():
        print(f"Ignoring cache with duplicate UIDs: {path}")
        return None

    missing_uids = set(required_uids) - set(frame[CACHE_UID_COLUMN])
    if missing_uids:
        print(
            f"Ignoring incomplete cache {path}; "
            f"missing {len(missing_uids)} UIDs"
        )
        return None
    return frame


def _iter_unique_watershed_partitions(
    matches,
    *,
    allowed_uids=None,
    area_limit=None,
):
    seen_uids = set()
    selected_matches = matches if area_limit is None else matches[: int(area_limit)]

    for source_path, row in selected_matches:
        area_gdf = read_validated_vector_file(
            source_path,
            f"Watershed partition has no valid geometries: {source_path}",
        )
        if CACHE_UID_COLUMN not in area_gdf.columns:
            raise ValueError(f"Watershed partition must contain uid: {source_path}")

        area_gdf[CACHE_UID_COLUMN] = area_gdf[CACHE_UID_COLUMN].astype(str)
        keep = ~area_gdf[CACHE_UID_COLUMN].isin(seen_uids)
        if allowed_uids is not None:
            keep &= area_gdf[CACHE_UID_COLUMN].isin(allowed_uids)
        area_gdf = area_gdf.loc[keep].copy()
        seen_uids.update(area_gdf[CACHE_UID_COLUMN])
        if not area_gdf.empty:
            yield source_path, row, area_gdf


def _ensure_pan_india_aquifer_cache(
    *,
    matches,
    required_uids,
    output_base_dir,
    aquifer_vector_path,
    area_limit=None,
):
    cache_path = _aquifer_cache_path(output_base_dir)
    value_column = "weighted_avg_yeild"
    cached = _read_complete_uid_cache(
        cache_path,
        required_uids,
        [value_column],
    )
    if cached is not None:
        print(f"Using Pan-India aquifer cache: {cache_path}")
        return cached.set_index(CACHE_UID_COLUMN), cache_path

    aquifers_gdf = read_validated_vector_file(
        aquifer_vector_path,
        f"Aquifer source file has no valid geometries: " f"{aquifer_vector_path}",
    )
    aquifers_projected = _prepare_aquifers_for_intersection(aquifers_gdf)
    records = []
    processed = 0
    for source_path, _, watersheds_gdf in _iter_unique_watershed_partitions(
        matches,
        allowed_uids=set(required_uids),
        area_limit=area_limit,
    ):
        result = _compute_aquifer_properties_for_watersheds(
            watersheds_gdf=watersheds_gdf[[CACHE_UID_COLUMN, "geometry"]].copy(),
            aquifers_projected=aquifers_projected,
        )
        records.append(
            pd.DataFrame(
                {
                    CACHE_UID_COLUMN: result[CACHE_UID_COLUMN].astype(str),
                    value_column: result["total_weighted_yield"].astype(float),
                }
            )
        )
        processed += len(result)
        print(
            f"Cached aquifer yield for {processed} watersheds "
            f"(latest partition: {source_path})"
        )

    if not records:
        raise ValueError("No watershed records were available for aquifer caching")

    cache_frame = pd.concat(records, ignore_index=True)
    cache_frame = cache_frame.drop_duplicates(
        CACHE_UID_COLUMN,
        keep="last",
    ).sort_values(CACHE_UID_COLUMN)
    if area_limit is None:
        missing_uids = set(required_uids) - set(cache_frame[CACHE_UID_COLUMN])
        if missing_uids:
            raise ValueError(
                "Aquifer cache generation missed "
                f"{len(missing_uids)} required watershed UIDs"
            )
    _write_parquet_atomic(cache_frame, cache_path)
    print(
        f"Saved Pan-India aquifer cache with {len(cache_frame)} records: "
        f"{cache_path}"
    )
    return cache_frame.set_index(CACHE_UID_COLUMN), cache_path


def _decode_timeseries(value, uid):
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        raise ValueError(f"Missing rainfall/runoff timeseries for uid={uid}")
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Invalid rainfall/runoff timeseries JSON for uid={uid}"
        ) from error


def _parse_timestamp(value):
    return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


def _aggregate_rainfall_runoff(timeseries, periods):
    totals = {key: {"Precipitation": 0.0, "RunOff": 0.0} for _, _, key in periods}
    for timestamp, values in timeseries.items():
        if not isinstance(values, dict):
            continue
        day = _parse_timestamp(timestamp)
        for period_start, period_end, key in periods:
            if period_start <= day < period_end:
                totals[key]["Precipitation"] += float(
                    values.get("Rainfall", values.get("rainfall", 0.0)) or 0.0
                )
                totals[key]["RunOff"] += float(
                    values.get("Runoff", values.get("runoff", 0.0)) or 0.0
                )
                break
    return totals


class _RasterZonalGrid:
    def __init__(self, watersheds_gdf, reference_path):
        with rasterio.open(reference_path) as src:
            if src.crs is None:
                raise ValueError(f"Raster CRS is missing: {reference_path}")
            working_gdf = watersheds_gdf.to_crs(src.crs)
            minx, miny, maxx, maxy = working_gdf.total_bounds
            requested = from_bounds(minx, miny, maxx, maxy, src.transform)
            col_start = max(0, math.floor(requested.col_off))
            row_start = max(0, math.floor(requested.row_off))
            col_stop = min(
                src.width,
                math.ceil(requested.col_off + requested.width),
            )
            row_stop = min(
                src.height,
                math.ceil(requested.row_off + requested.height),
            )
            self.window = Window(
                col_start,
                row_start,
                col_stop - col_start,
                row_stop - row_start,
            )
            if self.window.width <= 0 or self.window.height <= 0:
                raise ValueError(
                    f"Watersheds do not overlap ET raster: {reference_path}"
                )
            self.transform = src.window_transform(self.window)
            self.shape = (int(self.window.height), int(self.window.width))
            self.crs = src.crs
            self.reference_transform = src.transform
            self.reference_width = src.width
            self.reference_height = src.height
            self.geometries = list(working_gdf.geometry)
            self._geometry_masks = None

    def read_flux(self, raster_path, negative_as_nodata):
        with rasterio.open(raster_path) as src:
            if (
                src.crs != self.crs
                or src.width != self.reference_width
                or src.height != self.reference_height
                or not src.transform.almost_equals(self.reference_transform)
            ):
                raise ValueError(
                    f"ET raster grid does not match reference raster: {raster_path}"
                )

            data = src.read(1, window=self.window, masked=True)
            values = np.asarray(data.filled(np.nan), dtype=np.float64)
            valid = ~np.ma.getmaskarray(data) & np.isfinite(values)
            if src.nodata is not None:
                valid &= values != src.nodata
            if negative_as_nodata:
                valid &= values >= 0
            else:
                values = np.where(values > 0, values, 0.0)
            return values, valid

    def means(self, values, valid):
        if self._geometry_masks is None:
            self._geometry_masks = []
            for geometry in self.geometries:
                center_mask = geometry_mask(
                    [mapping(geometry)],
                    out_shape=self.shape,
                    transform=self.transform,
                    invert=True,
                    all_touched=False,
                )
                touched_mask = geometry_mask(
                    [mapping(geometry)],
                    out_shape=self.shape,
                    transform=self.transform,
                    invert=True,
                    all_touched=True,
                )
                self._geometry_masks.append((center_mask, touched_mask))

        means = []
        for center_mask, touched_mask in self._geometry_masks:
            selected = center_mask & valid
            if not selected.any():
                selected = touched_mask & valid
            means.append(float(values[selected].mean()) if selected.any() else np.nan)
        return np.asarray(means, dtype=np.float64)


def _aggregate_flux_rasters(
    watersheds_gdf,
    weighted_rasters,
    *,
    negative_as_nodata,
):
    if not weighted_rasters:
        return np.zeros(len(watersheds_gdf), dtype=np.float64)

    grid = _RasterZonalGrid(watersheds_gdf, weighted_rasters[0][0])
    total = np.zeros(grid.shape, dtype=np.float64)
    has_value = np.zeros(grid.shape, dtype=bool)

    for raster_path, day_count in weighted_rasters:
        values, valid = grid.read_flux(
            raster_path,
            negative_as_nodata=negative_as_nodata,
        )
        total[valid] += values[valid] * SECONDS_PER_DAY * float(day_count)
        has_value |= valid

    return grid.means(total, has_value)


def _folder_has_tifs(path):
    path = Path(path)
    return path.is_dir() and next(path.glob("*.tif"), None) is not None


def _preferred_et_source_root(et_root, patch_filled_source, raw_source, raw_subfolder):
    et_root = Path(et_root)
    patch_filled_root = et_root / patch_filled_source
    if _folder_has_tifs(patch_filled_root):
        return patch_filled_root
    return et_root / raw_source / raw_subfolder


def _et_source_name_from_root(root):
    root = Path(root)
    if root.name in {
        FLDAS_CA_DAILY_PATCH_FILLED_SOURCE,
        FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE,
    }:
        return root.name
    if root.name in {"daily", "monthly"}:
        return root.parent.name
    return root.name


def _et_source_names_from_roots(roots_by_year):
    return sorted(
        {
            _et_source_name_from_root(root)
            for root in roots_by_year.values()
        }
    )


def _aggregate_source_folder(source_names):
    source_names = sorted(set(source_names))
    if len(source_names) == 1:
        return source_names[0]
    return "__".join(source_names)


def _cache_et_sources(frame):
    sources = set()
    if CACHE_ET_SOURCE_COLUMN not in frame.columns:
        return sources
    for value in frame[CACHE_ET_SOURCE_COLUMN].dropna().astype(str):
        sources.update(
            source.strip()
            for source in value.split(",")
            if source.strip()
        )
    return sources


def _et_source_signature(et_roots_by_year):
    daily_roots = _daily_roots_by_year(et_roots_by_year)
    monthly_roots = _monthly_roots_by_year(et_roots_by_year)
    return "|".join(
        (
            f"{_year_key(source_year)}:"
            f"daily={_et_source_name_from_root(daily_roots[source_year])};"
            f"monthly={_et_source_name_from_root(monthly_roots[source_year])}"
        )
        for source_year in sorted(et_roots_by_year)
    )


def _daily_roots_by_year(et_roots_by_year):
    return {
        source_year: _preferred_et_source_root(
            et_root,
            FLDAS_CA_DAILY_PATCH_FILLED_SOURCE,
            FLDAS_CA_DAILY_SOURCE,
            "daily",
        )
        for source_year, et_root in et_roots_by_year.items()
    }


def _monthly_roots_by_year(et_roots_by_year):
    return {
        source_year: _preferred_et_source_root(
            et_root,
            FLDAS_GLOBAL_MONTHLY_PATCH_FILLED_SOURCE,
            FLDAS_GLOBAL_MONTHLY_SOURCE,
            "monthly",
        )
        for source_year, et_root in et_roots_by_year.items()
    }


def _source_root_for_date(roots_by_year, day, source_name):
    source_year = _source_year_for_date(day)
    root = roots_by_year.get(source_year)
    if root is None:
        raise FileNotFoundError(
            f"{source_name} folder for source year {_year_key(source_year)} "
            f"is required for {day.isoformat()}"
        )
    return root


def _month_path(monthly_roots_by_year, day):
    monthly_root = _source_root_for_date(
        monthly_roots_by_year,
        day,
        "Monthly global ET",
    )
    path = monthly_root / f"{day:%Y%m}.tif"
    if not path.exists():
        raise FileNotFoundError(f"Monthly global ET raster not found: {path}")
    return path


def _monthly_weights(
    monthly_roots_by_year,
    start_date,
    end_date,
    included_dates=None,
):
    if included_dates is None:
        dates = []
        current = start_date
        while current < end_date:
            dates.append(current)
            current += dt.timedelta(days=1)
    else:
        dates = list(included_dates)

    month_counts = Counter(day.replace(day=1) for day in dates)
    return [
        (_month_path(monthly_roots_by_year, month), day_count)
        for month, day_count in sorted(month_counts.items())
    ]


def _daily_rasters(daily_roots_by_year, start_date, end_date):
    available = []
    missing = []
    current = start_date
    while current < end_date:
        daily_root = _source_root_for_date(
            daily_roots_by_year,
            current,
            "Daily CA ET",
        )
        path = daily_root / f"{current:%Y%m%d}.tif"
        if path.exists():
            available.append((path, 1))
        else:
            missing.append(current)
        current += dt.timedelta(days=1)
    return available, missing


def _uses_daily_et(watersheds_gdf, daily_roots_by_year):
    reference_path = None
    for daily_root in daily_roots_by_year.values():
        reference_path = next(iter(sorted(daily_root.glob("*.tif"))), None)
        if reference_path is not None:
            break
    if reference_path is None:
        return False

    with rasterio.open(reference_path) as src:
        raster_bounds = box(*src.bounds)
        watersheds = watersheds_gdf.to_crs(src.crs)
        return raster_bounds.covers(watersheds.geometry.union_all())


def _calculate_period_et(watersheds_gdf, periods, et_roots_by_year):
    daily_roots = _daily_roots_by_year(et_roots_by_year)
    monthly_roots = _monthly_roots_by_year(et_roots_by_year)
    daily_source = ",".join(_et_source_names_from_roots(daily_roots))
    monthly_source = ",".join(_et_source_names_from_roots(monthly_roots))
    use_daily = _uses_daily_et(watersheds_gdf, daily_roots)
    result = {}

    for period_start, period_end, key in periods:
        if use_daily:
            daily_rasters, missing_dates = _daily_rasters(
                daily_roots,
                period_start,
                period_end,
            )
            values = _aggregate_flux_rasters(
                watersheds_gdf,
                daily_rasters,
                negative_as_nodata=True,
            )
            if missing_dates:
                fallback = _aggregate_flux_rasters(
                    watersheds_gdf,
                    _monthly_weights(
                        monthly_roots,
                        period_start,
                        period_end,
                        included_dates=missing_dates,
                    ),
                    negative_as_nodata=False,
                )
                values = values + fallback
        else:
            values = _aggregate_flux_rasters(
                watersheds_gdf,
                _monthly_weights(monthly_roots, period_start, period_end),
                negative_as_nodata=False,
            )

        if np.isnan(values).any():
            missing_count = int(np.isnan(values).sum())
            raise ValueError(
                f"ET could not be calculated for {missing_count} watersheds "
                f"during period {key}"
            )
        result[key] = values

    return result, daily_source if use_daily else monthly_source


def _write_integrated_flux_raster(
    weighted_rasters,
    *,
    negative_as_nodata,
    output_path,
):
    output_path = Path(output_path)
    if output_path.exists():
        return output_path
    if not weighted_rasters:
        return None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    reference_path = weighted_rasters[0][0]
    with rasterio.open(reference_path) as reference:
        profile = reference.profile.copy()
        shape = (reference.height, reference.width)
        reference_crs = reference.crs
        reference_transform = reference.transform

    total = np.zeros(shape, dtype=np.float64)
    has_value = np.zeros(shape, dtype=bool)
    for raster_path, day_count in weighted_rasters:
        with rasterio.open(raster_path) as src:
            if (
                src.crs != reference_crs
                or src.width != shape[1]
                or src.height != shape[0]
                or not src.transform.almost_equals(reference_transform)
            ):
                raise ValueError(
                    "ET raster grid does not match aggregate reference: "
                    f"{raster_path}"
                )
            data = src.read(1, masked=True)
            values = np.asarray(data.filled(np.nan), dtype=np.float64)
            valid = ~np.ma.getmaskarray(data) & np.isfinite(values)
            if src.nodata is not None:
                valid &= values != src.nodata
            if negative_as_nodata:
                valid &= values >= 0
            else:
                values = np.where(values > 0, values, 0.0)
            total[valid] += values[valid] * SECONDS_PER_DAY * float(day_count)
            has_value |= valid

    nodata = -9999.0
    output = np.where(has_value, total, nodata).astype(np.float32)
    profile.update(
        count=1,
        dtype="float32",
        nodata=nodata,
        compress="deflate",
        predictor=3,
    )
    temporary_path = output_path.with_name(
        f"{output_path.stem}.{os.getpid()}.tmp{output_path.suffix}"
    )
    with rasterio.open(temporary_path, "w", **profile) as dst:
        dst.write(output, 1)
    temporary_path.replace(output_path)
    return output_path


def _ensure_period_et_aggregate_rasters(
    *,
    periods,
    et_roots_by_year,
    aggregate_root,
):
    daily_roots = _daily_roots_by_year(et_roots_by_year)
    monthly_roots = _monthly_roots_by_year(et_roots_by_year)
    daily_source_names = _et_source_names_from_roots(daily_roots)
    monthly_source_names = _et_source_names_from_roots(monthly_roots)
    daily_source = ",".join(daily_source_names)
    monthly_source = ",".join(monthly_source_names)
    daily_aggregate_folder = _aggregate_source_folder(daily_source_names)
    monthly_aggregate_folder = _aggregate_source_folder(monthly_source_names)
    aggregate_root = Path(aggregate_root)
    result = {}

    for period_start, period_end, key in periods:
        file_key = key.replace("-", "")
        daily_rasters, missing_dates = _daily_rasters(
            daily_roots,
            period_start,
            period_end,
        )
        daily_path = _write_integrated_flux_raster(
            daily_rasters,
            negative_as_nodata=True,
            output_path=aggregate_root / daily_aggregate_folder / f"{file_key}.tif",
        )
        global_path = _write_integrated_flux_raster(
            _monthly_weights(
                monthly_roots,
                period_start,
                period_end,
            ),
            negative_as_nodata=False,
            output_path=aggregate_root / monthly_aggregate_folder / f"{file_key}.tif",
        )
        missing_path = None
        if missing_dates:
            missing_path = _write_integrated_flux_raster(
                _monthly_weights(
                    monthly_roots,
                    period_start,
                    period_end,
                    included_dates=missing_dates,
                ),
                negative_as_nodata=False,
                output_path=aggregate_root
                / f"{monthly_aggregate_folder}_missing_daily"
                / f"{file_key}.tif",
            )

        result[key] = {
            "daily": daily_path,
            "global": global_path,
            "missing_daily": missing_path,
            "daily_source": daily_source,
            "global_source": monthly_source,
        }
    return result


def _integrated_raster_means(watersheds_gdf, raster_path, grid=None):
    grid = grid or _RasterZonalGrid(watersheds_gdf, raster_path)
    values, valid = grid.read_flux(
        raster_path,
        negative_as_nodata=True,
    )
    return grid.means(values, valid)


def _raster_covers_watersheds(watersheds_gdf, raster_path):
    if raster_path is None:
        return False
    with rasterio.open(raster_path) as src:
        raster_bounds = box(*src.bounds)
        watersheds = watersheds_gdf.to_crs(src.crs)
        return raster_bounds.covers(watersheds.geometry.union_all())


def _calculate_period_et_from_aggregates(
    watersheds_gdf,
    periods,
    aggregate_rasters,
):
    daily_reference = next(
        (
            aggregate_rasters[key]["daily"]
            for _, _, key in periods
            if aggregate_rasters[key]["daily"] is not None
        ),
        None,
    )
    use_daily = _raster_covers_watersheds(
        watersheds_gdf,
        daily_reference,
    )
    first_key = periods[0][2]
    primary_reference = (
        daily_reference if use_daily else aggregate_rasters[first_key]["global"]
    )
    primary_grid = _RasterZonalGrid(watersheds_gdf, primary_reference)
    missing_daily_grid = None
    result = {}

    for _, _, key in periods:
        paths = aggregate_rasters[key]
        if use_daily:
            if paths["daily"] is None:
                values = np.zeros(len(watersheds_gdf), dtype=np.float64)
            else:
                values = _integrated_raster_means(
                    watersheds_gdf,
                    paths["daily"],
                    grid=primary_grid,
                )
            if paths["missing_daily"] is not None:
                if missing_daily_grid is None:
                    missing_daily_grid = _RasterZonalGrid(
                        watersheds_gdf,
                        paths["missing_daily"],
                    )
                values += _integrated_raster_means(
                    watersheds_gdf,
                    paths["missing_daily"],
                    grid=missing_daily_grid,
                )
        else:
            values = _integrated_raster_means(
                watersheds_gdf,
                paths["global"],
                grid=primary_grid,
            )

        if np.isnan(values).any():
            missing_count = int(np.isnan(values).sum())
            raise ValueError(
                f"Cached ET could not be calculated for {missing_count} "
                f"watersheds during period {key}"
            )
        result[key] = values

    source = (
        aggregate_rasters[first_key]["daily_source"]
        if use_daily
        else aggregate_rasters[first_key]["global_source"]
    )
    return result, source


def _ensure_pan_india_et_cache(
    *,
    matches,
    required_uids,
    year,
    is_annual,
    et_roots_by_year,
    output_base_dir,
    area_limit=None,
):
    periods = _build_periods(year, is_annual)
    period_columns = [key for _, _, key in periods]
    source_year_keys = ",".join(
        _year_key(source_year) for source_year in sorted(et_roots_by_year)
    )
    cache_path = _et_cache_path(output_base_dir, year, is_annual)
    source_signature = _et_source_signature(et_roots_by_year)
    expected_cache_sources = set(
        _et_source_names_from_roots(_daily_roots_by_year(et_roots_by_year))
    ) | set(_et_source_names_from_roots(_monthly_roots_by_year(et_roots_by_year)))
    cached = _read_complete_uid_cache(
        cache_path,
        required_uids,
        [
            *period_columns,
            CACHE_ET_SOURCE_COLUMN,
            CACHE_ET_SOURCE_SIGNATURE_COLUMN,
            SOURCE_YEARS_COLUMN,
        ],
    )
    if cached is not None:
        cached_sources = _cache_et_sources(cached)
        cached_signatures = set(
            cached[CACHE_ET_SOURCE_SIGNATURE_COLUMN].dropna().astype(str)
        )
        if cached_signatures != {source_signature}:
            print(
                f"Ignoring ET cache with stale source signature: {cache_path}; "
                f"cache={sorted(cached_signatures)}, "
                f"current={source_signature}"
            )
        elif cached_sources and not cached_sources.issubset(expected_cache_sources):
            print(
                f"Ignoring ET cache with stale source rasters: {cache_path}; "
                f"cache={sorted(cached_sources)}, "
                f"current={sorted(expected_cache_sources)}"
            )
        else:
            print(f"Using Pan-India ET cache: {cache_path}")
            return cached.set_index(CACHE_UID_COLUMN), cache_path

    aggregate_rasters = _ensure_period_et_aggregate_rasters(
        periods=periods,
        et_roots_by_year=et_roots_by_year,
        aggregate_root=_et_aggregate_root(
            output_base_dir,
            year,
            is_annual,
        ),
    )
    records = []
    processed = 0
    for source_path, _, watersheds_gdf in _iter_unique_watershed_partitions(
        matches,
        allowed_uids=set(required_uids),
        area_limit=area_limit,
    ):
        record = pd.DataFrame(
            {
                CACHE_UID_COLUMN: watersheds_gdf[CACHE_UID_COLUMN].astype(str),
                SOURCE_YEARS_COLUMN: source_year_keys,
                CACHE_ET_SOURCE_SIGNATURE_COLUMN: source_signature,
            }
        )
        try:
            et_by_period, et_source = _calculate_period_et_from_aggregates(
                watersheds_gdf,
                periods,
                aggregate_rasters,
            )
            record[CACHE_ET_SOURCE_COLUMN] = et_source
            record[CACHE_ET_ERROR_COLUMN] = None
            for key in period_columns:
                record[key] = et_by_period[key]
        except Exception as error:
            record[CACHE_ET_SOURCE_COLUMN] = None
            record[CACHE_ET_ERROR_COLUMN] = str(error)
            for key in period_columns:
                record[key] = np.nan
            print(
                f"ET cache unavailable for {len(record)} watersheds in "
                f"{source_path}: {error}"
            )
        records.append(record)
        processed += len(record)
        print(
            f"Cached {len(period_columns)} ET period(s) for "
            f"{processed} watersheds (latest partition: {source_path})"
        )

    if not records:
        raise ValueError("No watershed records were available for ET caching")

    cache_frame = pd.concat(records, ignore_index=True)
    cache_frame = cache_frame.drop_duplicates(
        CACHE_UID_COLUMN,
        keep="last",
    ).sort_values(CACHE_UID_COLUMN)
    if area_limit is None:
        missing_uids = set(required_uids) - set(cache_frame[CACHE_UID_COLUMN])
        if missing_uids:
            raise ValueError(
                "ET cache generation missed "
                f"{len(missing_uids)} required watershed UIDs"
            )
    _write_parquet_atomic(cache_frame, cache_path)
    print(f"Saved Pan-India ET cache with {len(cache_frame)} records: " f"{cache_path}")
    return cache_frame.set_index(CACHE_UID_COLUMN), cache_path


def _add_annual_well_depth(
    result_gdf,
    annual_columns,
    aquifer_vector_path,
    aquifers_gdf=None,
    aquifer_cache=None,
):
    if aquifer_cache is not None:
        uid_values = result_gdf[CACHE_UID_COLUMN].astype(str)
        missing_uids = set(uid_values) - set(aquifer_cache.index)
        if missing_uids:
            raise ValueError(
                "Aquifer cache is missing " f"{len(missing_uids)} watershed UIDs"
            )
        result_gdf["weighted_avg_yeild"] = aquifer_cache.reindex(uid_values)[
            "weighted_avg_yeild"
        ].to_numpy(dtype=float)
    else:
        if aquifers_gdf is None:
            aquifers_gdf = read_validated_vector_file(
                aquifer_vector_path,
                f"Aquifer source file has no valid geometries: "
                f"{aquifer_vector_path}",
            )
        aquifer_result = _compute_aquifer_properties_for_watersheds(
            watersheds_gdf=result_gdf[["uid", "geometry"]].copy(),
            aquifers_gdf=aquifers_gdf,
        )
        result_gdf["weighted_avg_yeild"] = aquifer_result[
            "total_weighted_yield"
        ].to_numpy()

    for index, row in result_gdf.iterrows():
        weighted_yield = row["weighted_avg_yeild"]
        for column in annual_columns:
            values = json.loads(row[column])
            values["WellDepth"] = (
                values["DeltaG"] / (float(weighted_yield) * 1000.0)
                if pd.notna(weighted_yield) and float(weighted_yield) > 0
                else None
            )
            result_gdf.at[index, column] = json.dumps(
                values,
                separators=(",", ":"),
            )

    return _add_annual_net_columns(result_gdf, annual_columns)


def _add_annual_net_columns(result_gdf, annual_columns):
    for start_index in range(len(annual_columns) - 4):
        window = annual_columns[start_index : start_index + 5]
        start_year = window[0].split("_")[0]
        end_year = window[-1].split("_")[1][-2:]
        net_column = f"Net{start_year}_{end_year}"

        def net_value(row):
            well_depths = [
                json.loads(row[column]).get("WellDepth") for column in window
            ]
            if any(value is None for value in well_depths):
                return None
            return sum(float(value) for value in well_depths)

        result_gdf[net_column] = result_gdf.apply(net_value, axis=1)

    return result_gdf


def _run_generate_hydrology_area_local(
    *,
    state,
    district,
    block,
    start_year,
    end_year,
    is_annual=False,
    gee_account_id=None,
    hydrology_output_root=HYDROLOGY_OUTPUT_ROOT,
    output_base_dir=HYDROLOGY_LOCAL_OUTPUT_DIR,
    aquifer_vector_path=AQUIFER_VECTOR_PATH,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    watersheds_gdf=None,
    watershed_source=None,
    uid_to_watershed_id=None,
    aquifers_gdf=None,
    aquifer_cache=None,
    et_cache_by_year=None,
    et_cache_paths_by_year=None,
    layer_name_override=None,
    write_output=True,
):
    _ = gee_account_id
    state = _normalize_location(state, "state")
    district = _normalize_location(district, "district")
    block = _normalize_location(block, "block")
    start_year = int(start_year)
    end_year = int(end_year)
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    result_gdf = None
    uid_to_index = None
    cumulative_g = {}
    period_columns = []
    et_sources = set()
    input_paths = []
    pan_india_watershed_ids = None
    if watersheds_gdf is None:
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
        )
    else:
        watersheds_gdf = watersheds_gdf.copy()
        watershed_source = watershed_source or "provided watershed partition"

    for year in range(start_year, end_year + 1):
        periods = _build_periods(year, is_annual)
        source_inputs = _resolve_source_year_inputs(
            output_year=year,
            periods=periods,
            hydrology_output_root=hydrology_output_root,
        )
        source_years = sorted(source_inputs)
        series_dirs = [
            source_inputs[source_year][0] for source_year in source_years
        ]
        et_roots_by_year = {
            source_year: source_inputs[source_year][1]
            for source_year in source_years
        }
        (
            year_gdf,
            runoff_watershed_source,
            missing_series_paths,
            pan_india_watershed_ids,
        ) = _attach_pan_india_timeseries(
            watersheds_gdf,
            state=state,
            district=district,
            block=block,
            series_dirs=series_dirs,
            watershed_ids=pan_india_watershed_ids,
            uid_to_watershed_id=uid_to_watershed_id,
        )
        if missing_series_paths:
            raise FileNotFoundError(
                "Pan-India rainfall/runoff remains unavailable for "
                f"{len(missing_series_paths)} of {len(year_gdf)} watersheds "
                f"in {_year_key(year)} after checking duplicate watershed "
                f"IDs. First missing files: {missing_series_paths[:3]}"
            )

        if result_gdf is None:
            result_gdf = year_gdf.drop(columns=["timeseries"]).copy()
            uid_to_index = {uid: index for index, uid in result_gdf["uid"].items()}
            cumulative_g = {uid: 0.0 for uid in uid_to_index}
        elif set(year_gdf["uid"]) != set(uid_to_index):
            raise ValueError(
                "Watershed uid set differs between yearly inputs: "
                f"{[str(path) for path in series_dirs]}"
            )

        if et_cache_by_year is not None and year in et_cache_by_year:
            et_cache = et_cache_by_year[year]
            uid_values = year_gdf[CACHE_UID_COLUMN].astype(str)
            missing_uids = set(uid_values) - set(et_cache.index)
            if missing_uids:
                raise ValueError(
                    f"ET cache for {_year_key(year)} is missing "
                    f"{len(missing_uids)} watershed UIDs"
                )
            selected_et = et_cache.reindex(uid_values)
            invalid_et = selected_et[
                [key for _, _, key in periods]
            ].isna().any(axis=1)
            if invalid_et.any():
                errors = []
                if CACHE_ET_ERROR_COLUMN in selected_et.columns:
                    errors = (
                        selected_et.loc[invalid_et, CACHE_ET_ERROR_COLUMN]
                        .dropna()
                        .astype(str)
                        .unique()
                        .tolist()
                    )
                error_suffix = f" First cache errors: {errors[:3]}" if errors else ""
                raise ValueError(
                    f"ET cache for {_year_key(year)} has no usable value for "
                    f"{int(invalid_et.sum())} watershed UIDs.{error_suffix}"
                )
            et_by_period = {
                key: selected_et[key].to_numpy(dtype=float) for _, _, key in periods
            }
            selected_sources = set(
                selected_et[CACHE_ET_SOURCE_COLUMN].dropna().astype(str)
            )
            et_sources.update(selected_sources)
            et_source = ",".join(sorted(selected_sources))
        else:
            et_by_period, et_source = _calculate_period_et(
                year_gdf,
                periods,
                et_roots_by_year,
            )
            et_sources.add(et_source)
        input_paths.append(
            {
                "year": _year_key(year),
                "source_years": [
                    _year_key(source_year) for source_year in source_years
                ],
                "rainfall_runoff": [str(path) for path in series_dirs],
                "rainfall_runoff_watersheds": runoff_watershed_source,
                "missing_rainfall_runoff_series": len(missing_series_paths),
                "et": [
                    str(et_roots_by_year[source_year])
                    for source_year in source_years
                ],
                "et_source": et_source,
                "et_cache": (
                    str(et_cache_paths_by_year[year])
                    if et_cache_paths_by_year and year in et_cache_paths_by_year
                    else None
                ),
            }
        )

        for source_index, row in enumerate(year_gdf.itertuples(index=False)):
            uid = str(row.uid)
            target_index = uid_to_index[uid]
            timeseries = _decode_timeseries(row.timeseries, uid)
            water_balance = _aggregate_rainfall_runoff(timeseries, periods)

            for _, _, key in periods:
                values = water_balance[key]
                values["ET"] = float(et_by_period[key][source_index])
                values["DeltaG"] = (
                    values["Precipitation"] - values["RunOff"] - values["ET"]
                )
                cumulative_g[uid] += values["DeltaG"]
                values["G"] = cumulative_g[uid]
                result_gdf.at[target_index, key] = json.dumps(
                    values,
                    separators=(",", ":"),
                )

        period_columns.extend(key for _, _, key in periods)

    if is_annual:
        result_gdf = _add_annual_well_depth(
            result_gdf,
            annual_columns=period_columns,
            aquifer_vector_path=aquifer_vector_path,
            aquifers_gdf=aquifers_gdf,
            aquifer_cache=aquifer_cache,
        )

    if not write_output:
        return {
            "gdf": result_gdf,
            "is_annual": bool(is_annual),
            "start_year": start_year,
            "end_year": end_year,
            "period_columns": period_columns,
            "period_count": len(period_columns),
            "watershed_count": len(result_gdf),
            "et_sources": sorted(et_sources),
            "inputs": input_paths,
        }

    layer_name = layer_name_override or _layer_name(
        district,
        block,
        is_annual,
    )
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=output_base_dir,
        block_fallback="unknown_block",
    )
    if output_path.exists():
        output_path.unlink()
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local hydrology vector: {asset_id}")

    geoserver_synced = False
    if push_to_geoserver:
        response = push_local_vector_to_geoserver(
            path=os.path.splitext(asset_id)[0],
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        print(f"GeoServer response: {response}")
        geoserver_synced = isinstance(response, dict) and response.get(
            "status_code"
        ) in (200, 201, 202)

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Hydrology",
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
            misc={
                "start_date": f"{start_year}-07-01",
                "end_date": f"{end_year + 1}-06-30",
                "is_annual": bool(is_annual),
                "is_generated_locally": True,
                "et_sources": sorted(et_sources),
                "inputs": input_paths,
            },
        )
        if layer_id and geoserver_synced:
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=True,
            )

    return {
        "output": asset_id,
        "layer_name": layer_name,
        "is_annual": bool(is_annual),
        "start_year": start_year,
        "end_year": end_year,
        "period_count": len(period_columns),
        "watershed_count": len(result_gdf),
        "et_sources": sorted(et_sources),
        "geoserver_synced": geoserver_synced,
        "layer_id": layer_id,
    }


def _pan_india_run_root(output_base_dir, start_year, end_year, is_annual):
    period = "annual" if is_annual else "fortnight"
    return Path(output_base_dir) / "pan_india" / f"{start_year}_{end_year + 1}" / period


def _pan_india_area_output_path(
    *,
    output_base_dir,
    state,
    district,
    block,
    is_annual,
):
    layer_name = _layer_name(
        district,
        block,
        is_annual,
    )
    return build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=output_base_dir,
        block_fallback="unknown_block",
    )


def _run_generate_hydrology_pan_india_local(
    *,
    start_year,
    end_year,
    is_annual,
    hydrology_output_root,
    output_base_dir,
    aquifer_vector_path,
    push_to_geoserver,
    sync_layer_metadata,
    overwrite=False,
    area_limit=None,
):
    series_dirs = []
    et_roots_by_output_year = {}
    for year in range(start_year, end_year + 1):
        periods = _build_periods(year, is_annual)
        source_inputs = _resolve_source_year_inputs(
            output_year=year,
            periods=periods,
            hydrology_output_root=hydrology_output_root,
        )
        source_years = sorted(source_inputs)
        series_dirs.extend(
            source_inputs[source_year][0] for source_year in source_years
        )
        et_roots_by_output_year[year] = {
            source_year: source_inputs[source_year][1]
            for source_year in source_years
        }
    series_dirs = list(dict.fromkeys(series_dirs))

    matches, uid_to_watershed_id = _build_pan_india_uid_index(
        series_dirs=series_dirs,
    )
    required_uids = set(uid_to_watershed_id)
    aquifer_cache = None
    aquifer_cache_path = None
    if is_annual:
        aquifer_cache, aquifer_cache_path = _ensure_pan_india_aquifer_cache(
            matches=matches,
            required_uids=required_uids,
            output_base_dir=output_base_dir,
            aquifer_vector_path=aquifer_vector_path,
            area_limit=area_limit,
        )

    et_cache_by_year = {}
    et_cache_paths_by_year = {}
    for year in range(start_year, end_year + 1):
        et_cache, et_cache_path = _ensure_pan_india_et_cache(
            matches=matches,
            required_uids=required_uids,
            year=year,
            is_annual=is_annual,
            et_roots_by_year=et_roots_by_output_year[year],
            output_base_dir=output_base_dir,
            area_limit=area_limit,
        )
        et_cache_by_year[year] = et_cache
        et_cache_paths_by_year[year] = et_cache_path

    run_root = _pan_india_run_root(
        output_base_dir,
        start_year,
        end_year,
        is_annual,
    )
    layers_root = run_root / "layers"
    manifest_path = run_root / "manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "state",
        "district",
        "block",
        "source",
        "watershed_count",
        "duplicate_count",
        "status",
        "output",
        "layer_name",
        "geoserver_synced",
        "layer_id",
        "error",
    ]
    seen_uids = set()
    written_count = 0
    skipped_count = 0
    failed_count = 0
    watershed_count = 0

    selected_matches = matches if area_limit is None else matches[: int(area_limit)]
    with manifest_path.open("w", newline="") as manifest_file:
        writer = csv.DictWriter(manifest_file, fieldnames=fieldnames)
        writer.writeheader()

        for source_path, row in selected_matches:
            state = str(row.get("state") or "").strip().lower()
            district = str(row.get("district") or "").strip().lower()
            block = str(row.get("tehsil") or "").strip().lower()
            manifest_row = {
                "state": state,
                "district": district,
                "block": block,
                "source": str(source_path),
                "watershed_count": 0,
                "duplicate_count": 0,
                "status": "failed",
                "output": "",
                "layer_name": "",
                "geoserver_synced": False,
                "layer_id": "",
                "error": "",
            }

            try:
                area_gdf = read_validated_vector_file(
                    source_path,
                    f"Watershed partition has no valid geometries: {source_path}",
                )
                if "uid" not in area_gdf.columns:
                    raise ValueError(
                        f"Watershed partition must contain uid: {source_path}"
                    )
                area_gdf["uid"] = area_gdf["uid"].astype(str)
                duplicate_mask = area_gdf["uid"].isin(seen_uids)
                duplicate_count = int(duplicate_mask.sum())
                manifest_row["duplicate_count"] = duplicate_count
                manifest_row["watershed_count"] = len(area_gdf)
                seen_uids.update(area_gdf["uid"])

                output_path = _pan_india_area_output_path(
                    output_base_dir=layers_root,
                    state=state,
                    district=district,
                    block=block,
                    is_annual=is_annual,
                )
                if (
                    output_path.exists()
                    and not overwrite
                    and not push_to_geoserver
                    and not sync_layer_metadata
                ):
                    manifest_row["status"] = "skipped_existing"
                    manifest_row["output"] = str(output_path)
                    watershed_count += len(area_gdf)
                    skipped_count += 1
                    writer.writerow(manifest_row)
                    manifest_file.flush()
                    continue

                layer_name = _layer_name(
                    district,
                    block,
                    is_annual,
                )
                result = _run_generate_hydrology_area_local(
                    state=state,
                    district=district,
                    block=block,
                    start_year=start_year,
                    end_year=end_year,
                    is_annual=is_annual,
                    hydrology_output_root=hydrology_output_root,
                    output_base_dir=layers_root,
                    aquifer_vector_path=aquifer_vector_path,
                    push_to_geoserver=push_to_geoserver,
                    sync_layer_metadata=sync_layer_metadata,
                    watersheds_gdf=area_gdf,
                    watershed_source=str(source_path),
                    uid_to_watershed_id=uid_to_watershed_id,
                    aquifer_cache=aquifer_cache,
                    et_cache_by_year=et_cache_by_year,
                    et_cache_paths_by_year=et_cache_paths_by_year,
                    layer_name_override=layer_name,
                )
                if push_to_geoserver and not result["geoserver_synced"]:
                    raise RuntimeError(
                        f"GeoServer upload did not succeed for {layer_name}"
                    )
                if sync_layer_metadata and not result["layer_id"]:
                    raise RuntimeError(
                        f"Layer metadata sync did not succeed for {layer_name}"
                    )
                manifest_row["status"] = "written"
                manifest_row["output"] = result["output"]
                manifest_row["layer_name"] = result["layer_name"]
                manifest_row["geoserver_synced"] = result["geoserver_synced"]
                manifest_row["layer_id"] = result["layer_id"] or ""
                watershed_count += len(area_gdf)
                written_count += 1
            except Exception as error:
                manifest_row["error"] = str(error)
                failed_count += 1

            writer.writerow(manifest_row)
            manifest_file.flush()

    return {
        "scope": "pan_india",
        "manifest": str(manifest_path),
        "output_root": str(layers_root),
        "is_annual": bool(is_annual),
        "start_year": start_year,
        "end_year": end_year,
        "partition_count": len(selected_matches),
        "written_count": written_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "watershed_count": watershed_count,
        "runoff_index_size": len(uid_to_watershed_id),
        "aquifer_cache": (str(aquifer_cache_path) if aquifer_cache_path else None),
        "et_caches": {
            _year_key(year): str(path) for year, path in et_cache_paths_by_year.items()
        },
    }


def _run_generate_hydrology_base_layer_local(
    *,
    year=None,
    start_year=None,
    end_year=None,
    is_annual,
    hydrology_output_root=HYDROLOGY_OUTPUT_ROOT,
    output_base_dir=HYDROLOGY_BASE_LAYER_ROOT,
    cache_base_dir=HYDROLOGY_LOCAL_OUTPUT_DIR,
    aquifer_vector_path=AQUIFER_VECTOR_PATH,
    overwrite=False,
    area_limit=None,
):
    year, hydrology_end_year = _resolve_base_layer_year_bounds(
        year=year,
        start_year=start_year,
        end_year=end_year,
    )

    overwrite = _parse_bool(overwrite)
    layer_name = _base_layer_name(year, is_annual)
    output_path = _base_layer_path(output_base_dir, year, is_annual)
    manifest_path = output_path.with_name(f"{output_path.stem}_manifest.csv")
    if output_path.exists() and not overwrite:
        return {
            "scope": "pan_india_base_layer",
            "status": "skipped_existing",
            "output": str(output_path),
            "manifest": str(manifest_path) if manifest_path.exists() else None,
            "layer_name": layer_name,
            "start_year": year,
            "end_year": hydrology_end_year,
            "year_key": _year_key(year),
            "is_annual": bool(is_annual),
        }

    periods = _build_periods(year, is_annual)
    source_inputs = _resolve_source_year_inputs(
        output_year=year,
        periods=periods,
        hydrology_output_root=hydrology_output_root,
    )
    source_years = sorted(source_inputs)
    series_dirs = [source_inputs[source_year][0] for source_year in source_years]
    et_roots_by_year = {
        source_year: source_inputs[source_year][1]
        for source_year in source_years
    }
    matches, uid_to_watershed_id = _build_pan_india_uid_index(
        series_dirs=series_dirs,
    )
    required_uids = set(uid_to_watershed_id)
    aquifer_cache = None
    aquifer_cache_path = None
    if is_annual:
        aquifer_cache, aquifer_cache_path = _ensure_pan_india_aquifer_cache(
            matches=matches,
            required_uids=required_uids,
            output_base_dir=cache_base_dir,
            aquifer_vector_path=aquifer_vector_path,
            area_limit=area_limit,
        )

    et_cache, et_cache_path = _ensure_pan_india_et_cache(
        matches=matches,
        required_uids=required_uids,
        year=year,
        is_annual=is_annual,
        et_roots_by_year=et_roots_by_year,
        output_base_dir=cache_base_dir,
        area_limit=area_limit,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "state",
        "district",
        "block",
        "source",
        "watershed_count",
        "status",
        "error",
    ]
    records = []
    written_count = 0
    failed_count = 0
    watershed_count = 0
    selected_matches = matches if area_limit is None else matches[: int(area_limit)]

    with manifest_path.open("w", newline="") as manifest_file:
        writer = csv.DictWriter(manifest_file, fieldnames=fieldnames)
        writer.writeheader()

        for source_path, row, area_gdf in _iter_unique_watershed_partitions(
            selected_matches,
            allowed_uids=required_uids,
        ):
            state = str(row.get("state") or "unknown_state").strip().lower()
            district = str(row.get("district") or "unknown_district").strip().lower()
            block = str(row.get("tehsil") or Path(source_path).stem).strip().lower()
            manifest_row = {
                "state": state,
                "district": district,
                "block": block,
                "source": str(source_path),
                "watershed_count": len(area_gdf),
                "status": "failed",
                "error": "",
            }
            try:
                result = _run_generate_hydrology_area_local(
                    state=state,
                    district=district,
                    block=block,
                    start_year=year,
                    end_year=year,
                    is_annual=is_annual,
                    hydrology_output_root=hydrology_output_root,
                    output_base_dir=output_base_dir,
                    aquifer_vector_path=aquifer_vector_path,
                    push_to_geoserver=False,
                    sync_layer_metadata=False,
                    watersheds_gdf=area_gdf,
                    watershed_source=str(source_path),
                    uid_to_watershed_id=uid_to_watershed_id,
                    aquifer_cache=aquifer_cache,
                    et_cache_by_year={year: et_cache},
                    et_cache_paths_by_year={year: et_cache_path},
                    write_output=False,
                )
                records.append(result["gdf"])
                watershed_count += len(result["gdf"])
                written_count += 1
                manifest_row["status"] = "written"
            except Exception as error:
                failed_count += 1
                manifest_row["error"] = str(error)

            writer.writerow(manifest_row)
            manifest_file.flush()

    if not records:
        raise RuntimeError(
            "Hydrology base layer generation produced no watershed records. "
            f"See manifest: {manifest_path}"
        )

    combined = pd.concat(records, ignore_index=True)
    result_gdf = gpd.GeoDataFrame(
        combined,
        geometry="geometry",
        crs=records[0].crs,
    )
    if output_path.exists():
        output_path.unlink()
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local hydrology base layer: {asset_id}")

    return {
        "scope": "pan_india_base_layer",
        "status": "written_with_failures" if failed_count else "written",
        "output": asset_id,
        "manifest": str(manifest_path),
        "layer_name": layer_name,
        "start_year": year,
        "end_year": hydrology_end_year,
        "year_key": _year_key(year),
        "is_annual": bool(is_annual),
        "period_count": len(_build_periods(year, is_annual)),
        "watershed_count": watershed_count,
        "partition_count": len(selected_matches),
        "written_count": written_count,
        "failed_count": failed_count,
        "runoff_index_size": len(uid_to_watershed_id),
        "aquifer_cache": str(aquifer_cache_path) if aquifer_cache_path else None,
        "et_cache": str(et_cache_path),
    }


def _run_clip_hydrology_area_from_base_layers(
    *,
    state,
    district,
    block,
    start_year,
    end_year,
    is_annual,
    output_base_dir=HYDROLOGY_LOCAL_OUTPUT_DIR,
    base_layer_root=HYDROLOGY_BASE_LAYER_ROOT,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    state = _normalize_location(state, "state")
    district = _normalize_location(district, "district")
    block = _normalize_location(block, "block")
    start_year = int(start_year)
    end_year = int(end_year)
    if start_year != FORTNIGHT_ANCHOR_DATE.year:
        raise ValueError(
            "Local hydrology clipping requires start_year=2017 because the "
            "fortnightly cadence is anchored at 2017-07-01"
        )
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    watersheds_gdf, watershed_source = load_precomputed_watersheds(
        state=state,
        district=district,
        block=block,
    )
    if CACHE_UID_COLUMN not in watersheds_gdf.columns:
        raise ValueError("Precomputed watershed vector must contain uid")

    result_gdf = watersheds_gdf.copy()
    result_gdf[CACHE_UID_COLUMN] = result_gdf[CACHE_UID_COLUMN].astype(str)
    if result_gdf[CACHE_UID_COLUMN].duplicated().any():
        raise ValueError("Duplicate uid values found in precomputed watersheds")

    uid_values = result_gdf[CACHE_UID_COLUMN].astype(str)
    period_columns = []
    input_paths = []

    for year in range(start_year, end_year + 1):
        base_path = _base_layer_path(base_layer_root, year, is_annual)
        if not base_path.exists():
            raise FileNotFoundError(
                f"Hydrology base layer not found: {base_path}. "
                "Generate it first using the /api/v1/pan-india/ hydrology API."
            )

        periods = _build_periods(year, is_annual)
        year_columns = [key for _, _, key in periods]
        base_frame = gpd.read_file(base_path, ignore_geometry=True)
        if CACHE_UID_COLUMN not in base_frame.columns:
            raise ValueError(f"Hydrology base layer must contain uid: {base_path}")
        base_frame[CACHE_UID_COLUMN] = base_frame[CACHE_UID_COLUMN].astype(str)
        base_frame = base_frame.drop_duplicates(CACHE_UID_COLUMN, keep="last")
        base_frame = base_frame.set_index(CACHE_UID_COLUMN)

        copy_columns = list(year_columns)
        if (
            is_annual
            and "weighted_avg_yeild" in base_frame.columns
            and "weighted_avg_yeild" not in result_gdf.columns
        ):
            copy_columns.append("weighted_avg_yeild")
        missing_columns = sorted(set(copy_columns) - set(base_frame.columns))
        if missing_columns:
            raise ValueError(
                f"Hydrology base layer {base_path} is missing columns: "
                f"{missing_columns}"
            )

        missing_uids = sorted(set(uid_values) - set(base_frame.index))
        if missing_uids:
            raise FileNotFoundError(
                f"Hydrology base layer {base_path} is missing "
                f"{len(missing_uids)} watershed UIDs for this tehsil. "
                f"First missing UIDs: {missing_uids[:5]}"
            )

        matched = base_frame.reindex(uid_values)
        for column in copy_columns:
            result_gdf[column] = matched[column].to_numpy()

        period_columns.extend(year_columns)
        input_paths.append(
            {
                "year": _year_key(year),
                "path": str(base_path),
                "period_columns": year_columns,
            }
        )

    if is_annual:
        result_gdf = _add_annual_net_columns(result_gdf, period_columns)

    layer_name = _layer_name(
        district,
        block,
        is_annual,
    )
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=output_base_dir,
        block_fallback="unknown_block",
    )
    if output_path.exists():
        output_path.unlink()
    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved clipped local hydrology vector: {asset_id}")

    geoserver_synced = False
    if push_to_geoserver:
        response = push_local_vector_to_geoserver(
            path=os.path.splitext(asset_id)[0],
            layer_name=layer_name,
            workspace=GEOSERVER_WORKSPACE,
            file_type="gpkg",
        )
        print(f"GeoServer response: {response}")
        geoserver_synced = isinstance(response, dict) and response.get(
            "status_code"
        ) in (200, 201, 202)

    layer_id = None
    if sync_layer_metadata:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Hydrology",
            algorithm=LOCAL_ALGORITHM,
            algorithm_version=LOCAL_ALGORITHM_VERSION,
            misc={
                "start_date": f"{start_year}-07-01",
                "end_date": f"{end_year + 1}-06-30",
                "is_annual": bool(is_annual),
                "is_generated_locally": True,
                "source": "base_layer_clip",
                "watershed_source": watershed_source,
                "inputs": input_paths,
            },
        )
        if layer_id and geoserver_synced:
            update_layer_sync_status(
                layer_id=layer_id,
                sync_to_geoserver=True,
            )

    return {
        "output": asset_id,
        "layer_name": layer_name,
        "is_annual": bool(is_annual),
        "start_year": start_year,
        "end_year": end_year,
        "period_count": len(period_columns),
        "watershed_count": len(result_gdf),
        "source": "base_layer_clip",
        "geoserver_synced": geoserver_synced,
        "layer_id": layer_id,
    }


def run_generate_hydrology_local(
    *,
    state=None,
    district=None,
    block=None,
    pan_india=False,
    start_year,
    end_year,
    is_annual=False,
    gee_account_id=None,
    hydrology_output_root=HYDROLOGY_OUTPUT_ROOT,
    output_base_dir=HYDROLOGY_LOCAL_OUTPUT_DIR,
    aquifer_vector_path=AQUIFER_VECTOR_PATH,
    push_to_geoserver=True,
    sync_layer_metadata=True,
    overwrite=False,
):
    pan_india = _parse_bool(pan_india)
    start_year = int(start_year)
    end_year = int(end_year)
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    if pan_india:
        raise ValueError(
            "pan_india=true is not supported on the tehsil hydrology API. "
            "Use the /api/v1/pan-india/ hydrology API to generate "
            "Pan-India outputs."
        )

    return _run_clip_hydrology_area_from_base_layers(
        state=state,
        district=district,
        block=block,
        start_year=start_year,
        end_year=end_year,
        is_annual=is_annual,
        output_base_dir=output_base_dir,
        push_to_geoserver=push_to_geoserver,
        sync_layer_metadata=sync_layer_metadata,
    )


@app.task(bind=True)
def generate_hydrology(
    self,
    state=None,
    district=None,
    block=None,
    pan_india=False,
    start_year=None,
    end_year=None,
    is_annual=False,
    gee_account_id=None,
    overwrite=False,
):
    _ = self
    return run_generate_hydrology_local(
        state=state,
        district=district,
        block=block,
        pan_india=pan_india,
        start_year=start_year,
        end_year=end_year,
        is_annual=is_annual,
        gee_account_id=gee_account_id,
        push_to_geoserver=True,
        sync_layer_metadata=True,
        overwrite=overwrite,
    )


@app.task(bind=True)
def generate_hydrology_base_layer(
    self,
    year=None,
    start_year=None,
    end_year=None,
    is_annual=False,
    overwrite=False,
):
    _ = self
    return _run_generate_hydrology_base_layer_local(
        year=year,
        start_year=start_year,
        end_year=end_year,
        is_annual=is_annual,
        overwrite=overwrite,
    )
