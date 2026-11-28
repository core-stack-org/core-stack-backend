import difflib
import ee
import os
import json
import math
import tempfile
import requests
import pandas as pd
import numpy as np
from rest_framework.response import Response
from rest_framework import status
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
)
from rest_framework.response import Response
from rest_framework import status
from nrm_app.settings import EXCEL_PATH
import json
import requests
import pandas as pd
import numpy as np
import geopandas as gpd
from stats_generator.mws_indicators import generate_mws_data_for_kyl_filters
from nrm_app.settings import EXCEL_PATH, GEOSERVER_URL, GEE_HELPER_ACCOUNT_ID
from geoadmin.models import StateSOI, DistrictSOI, TehsilSOI
from computing.models import Layer, LayerType
from stats_generator.utils import get_url
from nrm_app.settings import GEOSERVER_URL
from nrm_app.settings import EXCEL_PATH, GEE_HELPER_ACCOUNT_ID
from utilities.renderers import round_floats
from django.conf import settings
from django.db.models import Q
from shapely.geometry import Point
from computing.config_loader import DATA_DIR

# Create your views here.


def is_valid_string(value):
    if not value:
        return True

    cleaned = (
        value.replace(" ", "")
        .replace("_", "")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
        .replace("-", "")
    )

    return cleaned.isalpha()


def is_valid_mws_id(value):
    if not value:
        return True
    return all(c.isdigit() or c == "_" for c in value)


def excel_file_exists(state, district, tehsil):
    base_path = os.path.join(EXCEL_PATH, "data/stats_excel_files")
    state_path = os.path.join(base_path, state.upper())
    district_path = os.path.join(state_path, district.upper())
    filename = f"{district}_{tehsil}.xlsx"
    file_path = os.path.join(district_path, filename)
    return file_path, os.path.exists(file_path)


def raster_tiff_download_url(workspace, layer_name):
    geotiff_url = f"{GEOSERVER_URL}/{workspace}/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId={workspace}:{layer_name}&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256"
    return geotiff_url


def _soi_name_candidates(name):
    """Match both GEE-normalized (uttar_pradesh) and SOI (Uttar Pradesh) names."""
    raw = str(name or "").strip()
    if not raw:
        return []
    candidates = []
    for value in (raw, raw.replace("_", " "), raw.replace(" ", "_")):
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def _get_soi_by_name(model, field_name, name, **filters):
    for candidate in _soi_name_candidates(name):
        match = model.objects.filter(**{f"{field_name}__iexact": candidate}, **filters).first()
        if match is not None:
            return match
    raise model.DoesNotExist(
        f"{model.__name__} matching query does not exist for {field_name}={name!r}."
    )


def _format_unknown_soi_error(
    label, value, model, field_name, parent_context="", **filters
):
    """Return a readable 'unknown <level>' error message with accepted name hints."""
    available = sorted(
        {
            str(name).strip()
            for name in model.objects.filter(**filters).values_list(
                field_name, flat=True
            )
            if name and str(name).strip()
        }
    )
    query_norm = str(value or "").replace("_", " ").strip().lower()
    lower_to_original = {name.lower(): name for name in available}
    close_keys = difflib.get_close_matches(
        query_norm, list(lower_to_original.keys()), n=3, cutoff=0.6
    )
    suggestions = [lower_to_original[key] for key in close_keys]

    location_suffix = f" {parent_context}" if parent_context else ""
    base = f"Unknown {label} '{value}'{location_suffix}."
    if suggestions:
        if len(suggestions) == 1:
            return f"{base} Accepted name: '{suggestions[0]}'."
        return f"{base} Accepted names: {', '.join(suggestions)}."
    if available:
        preview = available[:10]
        names_str = ", ".join(preview)
        if len(available) > 10:
            names_str += f" (and {len(available) - 10} more)"
        return f"{base} Accepted {label} names: {names_str}."
    return base


class LayerNotPublished(Exception):
    """GeoServer WFS layer does not exist or is not published for this location."""


class UpstreamUnavailable(Exception):
    """Upstream GeoServer/GEE service timed out, returned 5xx, or was unreachable."""


def wfs_get_features(
    workspace,
    type_name,
    *,
    cql_filter=None,
    property_name=None,
    output_format="application/json",
    timeout=30,
    verify=True,
):
    """Fetch GeoServer WFS features, turning XML ServiceExceptions into LayerNotPublished."""
    base_url = f"{GEOSERVER_URL}/{workspace}/ows"
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": type_name,
        "outputFormat": output_format,
    }
    if cql_filter:
        params["CQL_FILTER"] = cql_filter
    if property_name:
        params["propertyName"] = property_name

    try:
        response = requests.get(base_url, params=params, timeout=timeout, verify=verify)
    except requests.exceptions.RequestException as exc:
        raise UpstreamUnavailable(str(exc)) from exc

    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        if status_code >= 500:
            raise UpstreamUnavailable(
                f"GeoServer request failed with status {status_code}"
            )
        if status_code in (400, 404):
            raise LayerNotPublished(
                f"Layer '{type_name}' is not published on GeoServer."
            )

    try:
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise UpstreamUnavailable(str(exc)) from exc

    headers = getattr(response, "headers", None)
    raw_ct = headers.get("Content-Type") if isinstance(headers, dict) else None
    content_type = raw_ct.lower() if isinstance(raw_ct, str) else ""
    raw_text = getattr(response, "text", None)
    body_prefix = raw_text.lstrip()[:256] if isinstance(raw_text, str) else ""
    if (
        "xml" in content_type
        or body_prefix.startswith("<")
        or "ServiceException" in body_prefix
        or "ExceptionReport" in body_prefix
    ):
        raise LayerNotPublished(f"Layer '{type_name}' is not published on GeoServer.")

    try:
        return response.json()
    except ValueError as exc:
        raise UpstreamUnavailable("Invalid JSON response from GeoServer.") from exc


def fetch_generated_layer_urls(state_name, district_name, block_name):
    """
    Fetch all published vector and raster layers for given state, district, and block,
    and return their metadata as JSON.
    """
    state = _get_soi_by_name(StateSOI, "state_name", state_name)
    district = _get_soi_by_name(
        DistrictSOI, "district_name", district_name, state=state
    )
    tehsil = _get_soi_by_name(
        TehsilSOI, "tehsil_name", block_name, district=district
    )

    layers = Layer.objects.filter(
        state=state,
        district=district,
        block=tehsil,
        is_sync_to_geoserver=True,
    )

    EXCLUDE_LAYER_KEYWORDS = [
        "run_off",
        "evapotranspiration",
        "precipitation",
    ]
    for word in EXCLUDE_LAYER_KEYWORDS:
        layers = layers.exclude(
            Q(layer_name__icontains=word) & ~Q(layer_name__icontains="mws_connectivity")
        )

    layer_data = []

    for layer in layers:
        dataset = layer.dataset
        workspace = dataset.workspace
        layer_type = dataset.layer_type
        layer_name = layer.layer_name
        gee_asset_path = layer.gee_asset_path
        style_url = (
            dataset.misc["style_url"]
            if dataset.misc and "style_url" in dataset.misc
            else ""
        )

        if layer_type in [LayerType.VECTOR, LayerType.POINT]:
            layer_url = get_url(workspace, layer_name)
        elif layer_type == LayerType.RASTER:
            layer_url = raster_tiff_download_url(workspace, layer_name)
        else:
            continue  # Skip unknown types

        layer_data.append(
            {
                "layer_name": layer_name,
                "dataset_name": dataset.name,
                "layer_type": layer_type,
                "layer_url": layer_url,
                "layer_version": layer.layer_version,
                "style_url": style_url,
                "gee_asset_path": gee_asset_path,
            }
        )

    latest_layers = {}
    for entry in layer_data:
        name = entry["layer_name"].lower()
        if name not in latest_layers:
            latest_layers[name] = entry
        else:
            current_version = float(latest_layers[name]["layer_version"] or 0)
            new_version = float(entry["layer_version"] or 0)
            if new_version > current_version:
                latest_layers[name] = entry

    return list(latest_layers.values())


def get_location_info_by_lat_lon(lat, lon):
    try:
        data = wfs_get_features(
            "pan_india_asset",
            "pan_india_asset:SOI_tehsil_pan_india_dataset",
            cql_filter=f"INTERSECTS(geom,POINT({lon} {lat}))",  # lon lat order
            timeout=30,
        )
        features = data.get("features", [])
        if not features:
            return Response(
                {
                    "error": "Latitude and longitude is not in SOI boundary.",
                    "code": "UNKNOWN_LOCATION",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        properties = features[0].get("properties", {})
        return {
            "State": properties.get("STATE", ""),
            "District": properties.get("District", ""),
            "Tehsil": properties.get("TEHSIL", ""),
        }
    except LayerNotPublished:
        return Response(
            {
                "error": "SOI admin boundary layer is not available.",
                "code": "LAYER_NOT_PUBLISHED",
            },
            status=status.HTTP_404_NOT_FOUND,
        )
    except UpstreamUnavailable as e:
        print("Exception while getting admin details", str(e))
        return Response(
            {
                "error": "Unable to reach GeoServer for admin boundary lookup.",
                "code": "UPSTREAM_UNAVAILABLE",
            },
            status=status.HTTP_502_BAD_GATEWAY,
        )


PAN_INDIA_MWS_FGB_FILENAME = "Microwatershed_v2_with_details.fgb"


def _resolve_pan_india_mws_fgb_path():
    configured = getattr(settings, "PAN_INDIA_MWS_FGB_PATH", None) or os.environ.get(
        "PAN_INDIA_MWS_FGB_PATH"
    )
    if configured:
        return str(configured)
    candidates = [
        os.path.join(str(DATA_DIR), "mws", PAN_INDIA_MWS_FGB_FILENAME),
        os.path.join(EXCEL_PATH, "data", "mws", PAN_INDIA_MWS_FGB_FILENAME),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


def lookup_mws_uid_from_fgb(lon, lat, fgb_path=None):
    """
    Point-in-polygon lookup against the pan-India MWS FlatGeobuf using its
    R-tree spatial index (bbox pre-filter + exact covers/intersects check).
    """
    path = fgb_path or _resolve_pan_india_mws_fgb_path()
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"Pan-India MWS FGB not found at {path}")

    bbox = (lon, lat, lon, lat)
    try:
        gdf = gpd.read_file(path, bbox=bbox, engine="pyogrio")
    except Exception:
        gdf = gpd.read_file(path, bbox=bbox)

    if gdf.empty:
        return None

    pt = Point(lon, lat)
    matches = gdf[gdf.geometry.covers(pt)]
    if matches.empty:
        matches = gdf[gdf.geometry.intersects(pt)]
    if matches.empty:
        return None

    row = matches.iloc[0]
    for col in ("uid", "UID", "mws_id"):
        if col in row.index and pd.notna(row[col]):
            val = str(row[col]).strip()
            if val:
                return val
    return None


def get_mws_id_by_lat_lon(lon, lat):
    data_dict = get_location_info_by_lat_lon(lat, lon)

    if isinstance(data_dict, Response):
        return data_dict

    district = valid_gee_text((data_dict.get("District") or "").lower())
    tehsil = valid_gee_text((data_dict.get("Tehsil") or "").lower())

    try:
        layer_name = f"mws:mws_{district}_{tehsil}"
        data = wfs_get_features(
            "mws",
            layer_name,
            cql_filter=f"INTERSECTS(geom,POINT({lon} {lat}))",
            timeout=30,
        )
        features = data.get("features", [])
        if not features:
            return Response(
                {
                    "error": "No MWS feature found for the given lat lon location.",
                    "code": "MWS_NOT_FOUND",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        properties = features[0].get("properties", {})
        uid = properties.get("uid") or properties.get("UID") or properties.get("mws_id")

        data_dict["mws_id"] = uid
        data_dict["uid"] = uid
        return data_dict

    except LayerNotPublished:
        print("Per-tehsil MWS layer not published on GeoServer, trying pan-India FGB:", layer_name)
        try:
            uid = lookup_mws_uid_from_fgb(lon, lat)
        except FileNotFoundError:
            return Response(
                {
                    "error": "MWS layer is not published for the given lat lon location.",
                    "code": "LAYER_NOT_PUBLISHED",
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as fgb_exc:
            print("Pan-India MWS FGB lookup failed:", str(fgb_exc))
            return Response(
                {
                    "error": "MWS layer is not published for the given lat lon location.",
                    "code": "LAYER_NOT_PUBLISHED",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        if not uid:
            return Response(
                {
                    "error": "No MWS feature found for the given lat lon location.",
                    "code": "MWS_NOT_FOUND",
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        data_dict["mws_id"] = uid
        data_dict["uid"] = uid
        return data_dict
    except UpstreamUnavailable as e:
        print("Upstream error while getting mws_id by lat long", str(e))
        return Response(
            {
                "error": "MWS GeoServer is temporarily unavailable.",
                "code": "UPSTREAM_UNAVAILABLE",
            },
            status=status.HTTP_502_BAD_GATEWAY,
        )
    except Exception as e:
        print("Exception while getting the mws_id by lat long", str(e))
        return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


def get_mws_time_series_data(state, district, tehsil, mws_id):
    """Fetch and merge water and NDVI time series data for a specific MWS location."""

    def fetch_geoserver_data(workspace, layer_name, mws_id):
        """Generic GeoServer WFS request."""
        data = wfs_get_features(
            workspace,
            layer_name,
            cql_filter=f"uid='{mws_id}'",
            output_format="json",
            timeout=10,
            verify=True,
        )
        return data["features"][0]["properties"] if data.get("features") else {}

    try:
        # Validate geographic hierarchy
        try:
            state_obj = _get_soi_by_name(StateSOI, "state_name", state)
        except StateSOI.DoesNotExist:
            return {
                "error": _format_unknown_soi_error(
                    "state", state, StateSOI, "state_name"
                ),
                "code": "UNKNOWN_LOCATION",
            }
        try:
            district_obj = _get_soi_by_name(
                DistrictSOI, "district_name", district, state=state_obj
            )
        except DistrictSOI.DoesNotExist:
            return {
                "error": _format_unknown_soi_error(
                    "district",
                    district,
                    DistrictSOI,
                    "district_name",
                    parent_context=f"in state '{state_obj.state_name}'",
                    state=state_obj,
                ),
                "code": "UNKNOWN_LOCATION",
            }
        try:
            tehsil_obj = _get_soi_by_name(
                TehsilSOI, "tehsil_name", tehsil, district=district_obj
            )
        except TehsilSOI.DoesNotExist:
            return {
                "error": _format_unknown_soi_error(
                    "tehsil",
                    tehsil,
                    TehsilSOI,
                    "tehsil_name",
                    parent_context=f"in district '{district_obj.district_name}'",
                    district=district_obj,
                ),
                "code": "UNKNOWN_LOCATION",
            }

        # Check if water layer exists
        district = valid_gee_text(district.lower())
        tehsil = valid_gee_text(tehsil.lower())
        layer_name = f"deltaG_fortnight_{district}_{tehsil}"
        water_layer_exists = (
            Layer.objects.filter(
                state=state_obj,
                district=district_obj,
                block=tehsil_obj,
                dataset__name="Hydrology",
                layer_name=layer_name,
            )
            .exclude(
                layer_version="1.0",
                algorithm_version="1.0",
            )
            .exists()
        )

        # Fetch hydrology data
        water_data = fetch_geoserver_data(
            "mws_layers", f"mws_layers:{layer_name}", mws_id
        )

        # Fetch NDVI data only if water layer exists
        ndvi_layers = {}
        if water_layer_exists:
            for veg_type in ["crop", "shrub", "tree"]:
                try:
                    ndvi_layers[veg_type] = fetch_geoserver_data(
                        "ndvi_timeseries",
                        f"ndvi_timeseries:ndvi_timeseries_{district}_{tehsil}_{veg_type}",
                        mws_id,
                    )
                except Exception:
                    ndvi_layers[veg_type] = {}

        # Collect all unique dates
        all_dates = set()
        for key in water_data:
            if "-" in key and key.count("-") == 2:
                all_dates.add(key)

        if water_layer_exists:
            for layer_data in ndvi_layers.values():
                for key in layer_data:
                    if "-" in key and key.count("-") == 2:
                        all_dates.add(key)

        def _parse_optional_number(value):
            if value is None or isinstance(value, bool):
                return None
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
            try:
                num = float(value)
            except (TypeError, ValueError):
                return None
            if math.isnan(num) or math.isinf(num):
                return None
            return round(num, 2)

        time_series = []
        for date in sorted(all_dates):
            # Parse hydrology metrics: missing/unparseable -> None (never 0.0 or '')
            raw_water = water_data.get(date)
            values = {}
            if isinstance(raw_water, dict):
                values = raw_water
            elif isinstance(raw_water, str) and raw_water.strip():
                try:
                    parsed = json.loads(raw_water)
                    if isinstance(parsed, dict):
                        values = parsed
                except (TypeError, ValueError):
                    values = {}
            hydrology_metrix = {
                "et": _parse_optional_number(values.get("ET")),
                "runoff": _parse_optional_number(values.get("RunOff")),
                "precipitation": _parse_optional_number(values.get("Precipitation")),
            }

            entry = {
                "date": date,
                **hydrology_metrix,
            }

            # Add NDVI data if available (None when missing/unparseable, never '')
            if water_layer_exists:
                entry["ndvi_crop"] = _parse_optional_number(
                    ndvi_layers.get("crop", {}).get(date)
                )
                entry["ndvi_shrub"] = _parse_optional_number(
                    ndvi_layers.get("shrub", {}).get(date)
                )
                entry["ndvi_tree"] = _parse_optional_number(
                    ndvi_layers.get("tree", {}).get(date)
                )
            else:
                entry["ndvi_crop"] = None
                entry["ndvi_shrub"] = None
                entry["ndvi_tree"] = None

            time_series.append(entry)

        time_series.sort(key=lambda x: x["date"])
        # Drop trailing periods that have no data yet
        metric_keys = (
            "et",
            "runoff",
            "precipitation",
            "ndvi_crop",
            "ndvi_shrub",
            "ndvi_tree",
        )
        while time_series and all(time_series[-1].get(k) is None for k in metric_keys):
            time_series.pop()
        return {"mws_id": mws_id, "time_series": time_series}

    except LayerNotPublished:
        return {
            "error": "Fortnight hydrology layer is not published for this tehsil.",
            "code": "LAYER_NOT_PUBLISHED",
            "fallback": "get_tehsil_data?data=hydrological_seasonal",
        }
    except UpstreamUnavailable as e:
        return {
            "error": "GeoServer is temporarily unavailable.",
            "code": "UPSTREAM_UNAVAILABLE",
            "http_status": status.HTTP_502_BAD_GATEWAY,
            "details": str(e),
        }
    except Exception as e:
        return {"error": str(e)}


def get_mws_json_from_kyl_indicator(state, district, tehsil, mws_id):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    file_xl_path = os.path.join(
        EXCEL_PATH,
        "data/stats_excel_files",
        state_folder,
        district_folder,
        f"{district}_{tehsil}",
    )
    json_file = file_xl_path + "_KYL_filter_data.json"

    if not os.path.exists(json_file):
        return {
            "error": "KYL indicator data not generated for this tehsil.",
            "code": "DATA_NOT_GENERATED",
        }

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df.columns = [col.strip().lower() for col in df.columns]

        if "mws_id" not in df.columns:
            return {"error": "'mws_id' column not found in JSON file."}

        filtered_df = df[df["mws_id"] == mws_id].copy()
        filtered_df = filtered_df.replace([np.inf, -np.inf], np.nan)
        if "fire_count_per_year" in filtered_df.columns:
            filtered_df["fire_count_per_year"] = (
                filtered_df["fire_count_per_year"]
                .replace([-9999, -9999.0], 0)
                .fillna(0)
            )

        json_compatible = json.loads(
            filtered_df.to_json(orient="records", default_handler=str)
        )

        return json_compatible

    except Exception as e:
        return {"error": f"Error reading or processing file: {str(e)}"}


def _atomic_write_json(target_path, payload):
    """Write JSON to a temp file in the same directory and atomically swap into place."""
    target_dir = os.path.dirname(target_path) or "."
    base_name = os.path.basename(target_path)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target_dir,
            prefix=f".{base_name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp_file:
            tmp_path = tmp_file.name
            json.dump(payload, tmp_file)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        try:
            os.chmod(tmp_path, 0o644)
        except OSError:
            pass
        os.replace(tmp_path, target_path)
    except Exception:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise


def _is_cache_fresh(source_path, cache_path):
    """Return True when cache_path exists and is at least as recent as source_path."""
    if not os.path.exists(cache_path):
        return False
    if os.path.exists(source_path):
        try:
            return os.path.getmtime(cache_path) >= os.path.getmtime(source_path)
        except OSError:
            return False
    return True


def get_tehsil_json(state, district, tehsil, regenerate=False):
    regenerate_bool = (
        regenerate
        if isinstance(regenerate, bool)
        else str(regenerate or "").strip().lower() in {"1", "true", "yes"}
    )
    file_path, file_exists = excel_file_exists(state, district, tehsil)
    json_path = file_path.replace(".xlsx", ".json")

    if not regenerate_bool and _is_cache_fresh(file_path, json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return round_floats(json.load(f))
        except (json.JSONDecodeError, ValueError, OSError):
            # Corrupt or truncated cache file — fall through and rebuild atomically.
            pass

    xls = pd.read_excel(file_path, sheet_name=None)
    json_data = {}

    for sheet_name, df in xls.items():
        df.columns = [col.strip().lower() for col in df.columns]
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)
        json_data[sheet_name] = df.to_dict(orient="records")

    json_data = round_floats(json_data)
    _atomic_write_json(json_path, json_data)
    return json_data


def generate_mws_report_url(state, district, tehsil, mws_id, base_url):
    ee_initialize(GEE_HELPER_ACCOUNT_ID)
    asset_path = get_gee_asset_path(state, district, tehsil)
    mws_asset_id = asset_path + f"filtered_mws_{district}_{tehsil}_uid"

    if not is_gee_asset_exists(mws_asset_id):
        return None, Response(
            {"error": "Mws Layer not found for the given location."},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Filter feature collection by MWS ID
    mws_fc = ee.FeatureCollection(mws_asset_id)
    matching_feature = mws_fc.filter(ee.Filter.eq("uid", mws_id)).first()

    if matching_feature is None or matching_feature.getInfo() is None:
        return None, Response(
            {"error": "Data not found for the given mws_id"},
            status=status.HTTP_404_NOT_FOUND,
        )

    # Check if Excel file exists
    _, exists = excel_file_exists(state, district, tehsil)
    if not exists:
        return None, Response(
            {
                "Message": "Data not found for this state, district, tehsil.",
                "code": "DATA_NOT_GENERATED",
            },
            status=status.HTTP_404_NOT_FOUND,
        )

    # Generate report URL
    report_url = f"{base_url}/api/v1/generate_mws_report/?state={state}&district={district}&block={tehsil}&uid={mws_id}"

    return {"Mws_report_url": report_url}, None


def get_mws_geometry(state, district, tehsil, mws_id=None):
    """
    Fetch GeoJSON geometry from the generated MWS layer.

    When ``mws_id`` is set, return that one feature. When it is omitted, return
    every MWS in the tehsil as a GeoJSON FeatureCollection.
    """
    ee_initialize(GEE_HELPER_ACCOUNT_ID)
    asset_path = get_gee_asset_path(state, district, tehsil)
    mws_asset_id = asset_path + f"filtered_mws_{district}_{tehsil}_uid"

    if not is_gee_asset_exists(mws_asset_id):
        return None, Response(
            {"error": "Mws Layer not found for the given location."},
            status=status.HTTP_404_NOT_FOUND,
        )

    try:
        mws_fc = ee.FeatureCollection(mws_asset_id)
        if mws_id:
            matching_feature = mws_fc.filter(ee.Filter.eq("uid", mws_id)).first()
            feature_info = (
                matching_feature.getInfo() if matching_feature is not None else None
            )
            if feature_info is None:
                return None, Response(
                    {"error": "Data not found for the given mws_id"},
                    status=status.HTTP_404_NOT_FOUND,
                )
            return (
                {
                    "uid": mws_id,
                    "state": state,
                    "district": district,
                    "tehsil": tehsil,
                    "geometry": feature_info.get("geometry"),
                },
                None,
            )

        fc_info = mws_fc.getInfo() or {}
        features = fc_info.get("features") or []
        if not features:
            return None, Response(
                {"error": "No features found in layer"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return (
            {
                "type": "FeatureCollection",
                "features": features,
            },
            None,
        )
    except Exception as e:
        return None, Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


def json_safe_geojson(obj):
    """Convert numpy / tuples to JSON types without rounding coordinates."""
    if isinstance(obj, dict):
        return {key: json_safe_geojson(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe_geojson(item) for item in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    try:
        if isinstance(obj, np.generic):
            if np.issubdtype(type(obj), np.floating):
                val = float(obj)
                return val if math.isfinite(val) else None
            if np.issubdtype(type(obj), np.integer):
                return int(obj)
            if np.issubdtype(type(obj), np.bool_):
                return bool(obj)
    except Exception:
        pass
    return obj


def village_geometry_to_geojson(geometry):
    if geometry is None:
        return None
    if hasattr(geometry, "__geo_interface__"):
        geometry = geometry.__geo_interface__
    return json_safe_geojson(geometry)


def village_rows_to_feature_collection(rows):
    features = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "vill_ID": row.get("village_id"),
                    "vill_name": row.get("village_name"),
                    "state": row.get("state"),
                    "district": row.get("district"),
                    "tehsil": row.get("tehsil"),
                },
                "geometry": village_geometry_to_geojson(row.get("geometry")),
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _village_feature_id(feature):
    props = feature.get("properties") if isinstance(feature, dict) else None
    if not isinstance(props, dict):
        return None
    for key in ("vill_ID", "vill_id", "village_id", "id"):
        if props.get(key) is not None:
            return str(props.get(key))
    return None


def filter_village_feature_collection(geojson, village_id):
    if village_id is None or not isinstance(geojson, dict):
        return geojson
    wanted = str(village_id)
    features = [
        feature
        for feature in (geojson.get("features") or [])
        if _village_feature_id(feature) == wanted
    ]
    return {**geojson, "features": features}


def get_village_geometries(state, district, tehsil, village_id=None):
    """
    Fetch village geometries from panchayat boundaries layer for a block.
    """
    try:
        layer_name = f"{district}_{tehsil}"
        village_gdf = gpd.read_file(get_url("panchayat_boundaries", layer_name))
        if village_gdf.empty:
            return None, Response(
                {"error": "No village boundaries found for the given location."},
                status=status.HTTP_404_NOT_FOUND,
            )

        columns = list(village_gdf.columns)
        col_lookup = {str(c).strip().lower(): c for c in columns}

        id_candidates = [
            "vill_id",
            "villid",
            "village_id",
            "villageid",
            "id",
        ]
        name_candidates = [
            "vill_name",
            "village_name",
            "village",
            "name",
        ]

        id_col = next((col_lookup[k] for k in id_candidates if k in col_lookup), None)
        name_col = next(
            (col_lookup[k] for k in name_candidates if k in col_lookup), None
        )

        if id_col is None or name_col is None or "geometry" not in village_gdf.columns:
            return None, Response(
                {"error": "Village boundary layer schema is missing required fields."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if village_id is not None:
            village_gdf = village_gdf[
                village_gdf[id_col].astype(str) == str(village_id)
            ]
            if village_gdf.empty:
                return None, Response(
                    {"error": "Village not found for the given village_id."},
                    status=status.HTTP_404_NOT_FOUND,
                )

        rows = []
        for _, row in village_gdf.iterrows():
            rows.append(
                {
                    "village_id": str(row.get(id_col))
                    if row.get(id_col) is not None
                    else None,
                    "village_name": row.get(name_col),
                    "state": state,
                    "district": district,
                    "tehsil": tehsil,
                    "geometry": village_geometry_to_geojson(row.get("geometry")),
                }
            )
        return rows, None
    except Exception as e:
        return None, Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


def get_mws_geometries_data(state, district, tehsil):
    try:
        layer_name = f"mws_{district}_{tehsil}"
        geojson_data = wfs_get_features(
            "mws",
            f"mws:{layer_name}",
            property_name="geom,uid",
            timeout=30,
        )

        if not geojson_data.get("features"):
            error_msg = "No features found in layer"
            print(error_msg)
            return False, error_msg

        print(f"Successfully retrieved {len(geojson_data['features'])} MWS geometries")
        return True, geojson_data

    except LayerNotPublished:
        return False, "MWS layer is not published for this tehsil."
    except UpstreamUnavailable as e:
        return False, f"GeoServer unavailable: {str(e)}"
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        return False, error_msg


# def get_village_geometries_data(state, district, tehsil, village_id):
#     try:
#         base_url = (
#             "https://geoserver.core-stack.org:8443/geoserver/panchayat_boundaries/ows"
#         )
#         layer_name = f"{district}_{tehsil}"

#         params = {
#             "service": "WFS",
#             "version": "1.0.0",
#             "request": "GetFeature",
#             "typeName": f"panchayat_boundaries:{layer_name}",
#             "outputFormat": "application/json",
#             "CQL_FILTER": f"vill_ID={village_id}",
#             "propertyName": "the_geom",
#         }

#         response = requests.get(base_url, params=params, timeout=30)

#         if response.status_code != 200:
#             return False, f"GeoServer request failed with status {response.status_code}"

#         geojson_data = response.json()

#         if not geojson_data.get("features") or len(geojson_data["features"]) == 0:
#             return False, f"No village found with ID: {village_id}"

#         geometry = geojson_data["features"][0].get("geometry")

#         if not geometry:
#             return False, "Feature found but no geometry data"

#         return True, geometry

#     except Exception as e:
#         return False, f"Internal error: {str(e)}"


def get_village_geometries_data(state, district, tehsil):
    try:
        layer_name = f"{district}_{tehsil}"
        geojson_data = wfs_get_features(
            "panchayat_boundaries",
            f"panchayat_boundaries:{layer_name}",
            property_name="the_geom,vill_ID,vill_name",
            timeout=30,
        )

        if not isinstance(geojson_data, dict) or not geojson_data.get("features"):
            return False, "No features found in layer"

        return True, json_safe_geojson(geojson_data)

    except LayerNotPublished:
        return False, "Village boundary layer is not published for this tehsil."
    except UpstreamUnavailable as e:
        return False, f"GeoServer unavailable: {str(e)}"
    except Exception as e:
        return False, f"Internal error: {str(e)}"
