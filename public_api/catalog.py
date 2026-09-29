"""Machine-readable catalog of public v2 APIs and the properties they return.

OpenAPI (``/swagger.json``) remains the human/codegen contract. RFC 9727
``/.well-known/api-catalog`` points agents at that spec. This module is the
property list Anupam / MCP clients use before calling ``fields=`` or ``data=``.
"""

from utilities.openmeteo_format import (
    ACTIVE_LOCATION_FIELD_HINTS,
    ADMIN_DETAIL_FIELD_HINTS,
    GENERATED_LAYER_FIELD_UNITS,
    KYL_INDICATOR_UNIT_OVERRIDES,
    MWS_BY_LATLON_FIELD_HINTS,
    MWS_GEOMETRY_FIELD_HINTS,
    MWS_REPORT_FIELD_HINTS,
    VILLAGE_GEOMETRY_FIELD_HINTS,
)

from .dataset_filters import TEHSIL_DATA_TYPE_DOCS
from .tehsil_sheet_units import TEHSIL_SHEET_COLUMN_UNITS

RFC9727_PROFILE = "https://www.rfc-editor.org/info/rfc9727"
RFC9727_CONTENT_TYPE = f'application/linkset+json; profile="{RFC9727_PROFILE}"'

MWS_FORTNIGHT_FIELDS = (
    ("et", "number[]", "mm", "Evapotranspiration for each ~15-day step"),
    ("runoff", "number[]", "mm", "Runoff for each ~15-day step"),
    ("precipitation", "number[]", "mm", "Precipitation for each ~15-day step"),
)
MWS_FORTNIGHT_FIELD_NAMES = {item[0] for item in MWS_FORTNIGHT_FIELDS}

KYL_FIELD_DESCRIPTIONS = {
    "mws_id": "Micro-watershed identifier",
    "terraincluster_id": "Terrain cluster class",
    "avg_precipitation": "Average precipitation",
    "cropping_intensity_trend": "Cropping-intensity trend code",
    "cropping_intensity_avg": "Average cropping intensity",
    "avg_single_cropped": "Average single-cropped area",
    "avg_double_cropped": "Average double-cropped area",
    "avg_triple_cropped": "Average triple-cropped area",
    "avg_wsr_ratio_kharif": "Kharif water-stress ratio",
    "avg_wsr_ratio_rabi": "Rabi water-stress ratio",
    "avg_wsr_ratio_zaid": "Zaid water-stress ratio",
    "avg_kharif_surface_water_mws": "Kharif surface-water extent",
    "avg_rabi_surface_water_mws": "Rabi surface-water extent",
    "avg_zaid_surface_water_mws": "Zaid surface-water extent",
    "trend_swb": "Surface-water-body trend code",
    "trend_g": "Groundwater trend code",
    "drought_category": "Drought category",
    "avg_number_dry_spell": "Average dry-spell count",
    "avg_runoff": "Average runoff",
    "total_nrega_assets": "MGNREGA asset count",
    "mws_intersect_villages": "Villages intersecting this MWS",
    "degradation_land_area": "Degraded land area",
    "increase_in_tree_cover": "Tree-cover increase",
    "decrease_in_tree_cover": "Tree-cover decrease",
    "degradation_cropping_intensity": "Cropping-intensity degradation",
    "urbanization_area": "Urbanization area",
    "lulc_slope_category": "Slope LULC category",
    "lulc_plain_category": "Plain LULC category",
    "area_wide_scale_restoration": "Wide-scale restoration area",
    "area_protection": "Protection area",
    "aquifer_class": "Aquifer class",
    "soge_class": "Stage of groundwater extraction class",
    "lcw_conflict": "Land-conflict flag",
    "mining": "Mining overlay flag",
    "green_credit": "Green-credit flag",
    "factory_csr": "Factory / CSR flag",
}

WATERBODY_PROPERTIES = (
    ("UID", "string", "id", "Waterbody identifier"),
    ("MWS_UID", "string", "id", "Parent micro-watershed identifier"),
    ("water", "number", "flag", "Detected surface water"),
    ("sum", "number", "ha", "Waterbody area"),
    ("zoi", "number", "count", "Zone-of-influence count"),
    ("zoi_area", "number", "ha", "Zone-of-influence area"),
    ("total_cropable_area_ever_hydroyear", "number", "ha", "Cumulative cropable area"),
    ("cropping_intensity", "number[]", "ratio", "Annual cropping intensity"),
    ("single_cropped_area", "number[]", "ha", "Annual single-cropped area"),
    ("doubly_cropped_area", "number[]", "ha", "Annual double-cropped area"),
)


def _prop(name, type_, unit, description, selectable=False):
    return {
        "name": name,
        "type": type_,
        "unit": unit,
        "description": description,
        "selectable": selectable,
    }


def _props_from_hints(hints, type_="string", selectable=False):
    return [
        _prop(name, type_, unit, unit.replace("_", " "), selectable=selectable)
        for name, unit in hints.items()
    ]


def _query(name, type_, required, description):
    return {
        "name": name,
        "in": "query",
        "type": type_,
        "required": required,
        "description": description,
    }


_LATLON_PARAMS = [
    _query("latitude", "number", True, "WGS84 latitude (-90 to 90)"),
    _query("longitude", "number", True, "WGS84 longitude (-180 to 180)"),
]
_ADMIN_PARAMS = [
    _query("state", "string", True, "State name from Get Active Locations"),
    _query("district", "string", True, "District name from Get Active Locations"),
    _query("tehsil", "string", True, "Tehsil / block name from Get Active Locations"),
]
_MWS_PARAMS = _ADMIN_PARAMS + [
    _query("mws_id", "string", True, "Micro-watershed identifier, e.g. 12_208104"),
]


def _kyl_properties():
    return [
        _prop(
            name,
            "string" if unit in {"id", "list", "category", "class", "flag", "code"} else "number",
            unit,
            KYL_FIELD_DESCRIPTIONS.get(name, name.replace("_", " ")),
            selectable=name != "mws_id",
        )
        for name, unit in KYL_INDICATOR_UNIT_OVERRIDES.items()
    ]


def _tehsil_properties():
    return [
        _prop(
            name,
            "object[]",
            dict(TEHSIL_SHEET_COLUMN_UNITS[name]),
            meaning,
            selectable=True,
        )
        for name, meaning in TEHSIL_DATA_TYPE_DOCS
        if name != "all"
    ]


def _mws_data_properties():
    props = [
        _prop("time", "string[]", "iso8601", "Period start date for each ~15-day step"),
        _prop("time_step", "string", "15_days", "Fortnight length"),
        _prop("mws_id", "string", "id", "Micro-watershed identifier in metadata"),
    ]
    props.extend(
        _prop(name, type_, unit, description, selectable=True)
        for name, type_, unit, description in MWS_FORTNIGHT_FIELDS
    )
    return props


PUBLIC_API_CATALOG = (
    {
        "id": "get_admin_details_by_latlon",
        "group": "dataset",
        "path": "/api/v2/get_admin_details_by_latlon/",
        "method": "GET",
        "title": "Get Admin Details by Lat Lon",
        "description": "Resolve a WGS84 coordinate to State, District, and Tehsil names.",
        "parameters": _LATLON_PARAMS,
        "select_param": None,
        "properties": _props_from_hints(ADMIN_DETAIL_FIELD_HINTS),
    },
    {
        "id": "get_mwsid_by_latlon",
        "group": "dataset",
        "path": "/api/v2/get_mwsid_by_latlon/",
        "method": "GET",
        "title": "Get MWSID by Lat Lon",
        "description": "Resolve a WGS84 coordinate to a micro-watershed uid and admin names.",
        "parameters": _LATLON_PARAMS,
        "select_param": None,
        "properties": _props_from_hints(MWS_BY_LATLON_FIELD_HINTS),
    },
    {
        "id": "get_tehsil_data",
        "group": "dataset",
        "path": "/api/v2/get_tehsil_data/",
        "method": "GET",
        "title": "Get Tehsil Data",
        "description": (
            "Download analytical sheets for a tehsil. Filter with data=sheet names. "
            "Each property unit lists that sheet's columns and their units."
        ),
        "parameters": _ADMIN_PARAMS
        + [
            _query(
                "data",
                "string",
                False,
                "Sheet filter: all (default) or names such as drought,stream_order",
            )
        ],
        "select_param": "data",
        "properties": _tehsil_properties(),
    },
    {
        "id": "get_mws_data",
        "group": "dataset",
        "path": "/api/v2/get_mws_data/",
        "method": "GET",
        "title": "Get MWS Time Series Data",
        "description": "Fortnight hydrology time series. Filter metrics with fields=et,runoff.",
        "parameters": _MWS_PARAMS
        + [
            _query(
                "fields",
                "string",
                False,
                "Comma-separated fortnight metrics: et, runoff, precipitation",
            )
        ],
        "select_param": "fields",
        "properties": _mws_data_properties(),
    },
    {
        "id": "get_mws_kyl_indicators",
        "group": "dataset",
        "path": "/api/v2/get_mws_kyl_indicators/",
        "method": "GET",
        "title": "Get MWS KYL Indicators",
        "description": "Single-row KYL indicator snapshot. Filter with fields=avg_runoff,drought_category.",
        "parameters": _MWS_PARAMS
        + [
            _query(
                "fields",
                "string",
                False,
                "Comma-separated indicator names from this catalog",
            )
        ],
        "select_param": "fields",
        "properties": _kyl_properties(),
    },
    {
        "id": "get_generated_layer_urls",
        "group": "dataset",
        "path": "/api/v2/get_generated_layer_urls/",
        "method": "GET",
        "title": "Get Generated Layer Url",
        "description": "GeoServer WFS/WCS URLs for every generated layer in a tehsil.",
        "parameters": _ADMIN_PARAMS,
        "select_param": None,
        "properties": _props_from_hints(GENERATED_LAYER_FIELD_UNITS),
    },
    {
        "id": "get_mws_report",
        "group": "dataset",
        "path": "/api/v2/get_mws_report/",
        "method": "GET",
        "title": "Get MWS Report url",
        "description": "PDF/HTML report URL for one micro-watershed.",
        "parameters": _MWS_PARAMS,
        "select_param": None,
        "properties": _props_from_hints(MWS_REPORT_FIELD_HINTS),
    },
    {
        "id": "get_mws_geometries",
        "group": "dataset",
        "path": "/api/v2/get_mws_geometries/",
        "method": "GET",
        "title": "Get MWS Geometry",
        "description": "Micro-watershed polygons as a GeoJSON FeatureCollection.",
        "parameters": _ADMIN_PARAMS
        + [_query("mws_id", "string", False, "Optional; omit to return every MWS in the tehsil")],
        "select_param": None,
        "properties": _props_from_hints(MWS_GEOMETRY_FIELD_HINTS)
        + [_prop("coordinates", "number[][][]", "degrees", "Polygon rings (lon, lat)")],
    },
    {
        "id": "get_village_geometries",
        "group": "dataset",
        "path": "/api/v2/get_village_geometries/",
        "method": "GET",
        "title": "Get Village Geometries",
        "description": "Village / panchayat polygons as a GeoJSON FeatureCollection.",
        "parameters": _ADMIN_PARAMS
        + [_query("village_id", "string", False, "Optional vill_ID; omit for the whole tehsil")],
        "select_param": None,
        "properties": _props_from_hints(VILLAGE_GEOMETRY_FIELD_HINTS)
        + [_prop("coordinates", "number[][][][]", "degrees", "MultiPolygon rings (lon, lat)")],
    },
    {
        "id": "get_active_locations",
        "group": "dataset",
        "path": "/api/v2/get_active_locations/",
        "method": "GET",
        "title": "Get Active Locations",
        "description": "State → district → tehsil tree where public datasets are already generated.",
        "parameters": [
            _query("state", "string", False, "Optional state filter"),
            _query("district", "string", False, "Optional district filter"),
            _query("tehsil", "string", False, "Optional tehsil filter (alias: block)"),
            _query("block", "string", False, "Optional block filter (same as tehsil)"),
        ],
        "select_param": None,
        "properties": _props_from_hints(ACTIVE_LOCATION_FIELD_HINTS),
    },
    {
        "id": "get_waterbodies_data_by_admin",
        "group": "waterbody",
        "path": "/api/v2/get_waterbodies_data_by_admin/",
        "method": "GET",
        "title": "Get Waterbodies by admin data",
        "description": (
            "Merged remotely sensed waterbodies for a tehsil, detected by the IIT Delhi "
            "method and implemented by the CoRE Stack team."
        ),
        "parameters": _ADMIN_PARAMS
        + [_query("regenerate", "string", False, "Set true to rebuild the merge")],
        "select_param": None,
        "properties": [
            _prop(name, type_, unit, description)
            for name, type_, unit, description in WATERBODY_PROPERTIES
        ],
    },
    {
        "id": "get_waterbody_data",
        "group": "waterbody",
        "path": "/api/v2/get_waterbody_data/",
        "method": "GET",
        "title": "Get Waterbodies by uid",
        "description": "One waterbody from the merged remotely sensed dataset, keyed by UID.",
        "parameters": _ADMIN_PARAMS
        + [
            _query("uid", "string", True, "Waterbody UID, e.g. 12_100174_104"),
            _query("regenerate", "string", False, "Set true to rebuild the merge"),
        ],
        "select_param": None,
        "properties": [
            _prop(name, type_, unit, description)
            for name, type_, unit, description in WATERBODY_PROPERTIES
        ],
    },
)

CATALOG_BY_ID = {entry["id"]: entry for entry in PUBLIC_API_CATALOG}


def list_catalog_entries(group=None):
    entries = PUBLIC_API_CATALOG
    if group:
        wanted = str(group).strip().lower()
        entries = tuple(item for item in entries if item["group"] == wanted)
    return entries


def get_catalog_entry(api_id):
    if not api_id:
        return None
    return CATALOG_BY_ID.get(str(api_id).strip())


def catalog_summary(entry, request=None):
    properties_path = f"/api/v2/catalog/{entry['id']}/"
    item = {
        "id": entry["id"],
        "group": entry["group"],
        "path": entry["path"],
        "method": entry["method"],
        "title": entry["title"],
        "description": entry["description"],
        "select_param": entry["select_param"],
        "property_count": len(entry["properties"]),
        "properties_url": properties_path,
    }
    if request is not None:
        item["properties_url"] = request.build_absolute_uri(properties_path)
        item["href"] = request.build_absolute_uri(entry["path"])
    return item


def catalog_detail(entry, request=None):
    detail = {
        **catalog_summary(entry, request),
        "parameters": list(entry["parameters"]),
        "properties": list(entry["properties"]),
    }
    return detail


def catalog_index_payload(request, group=None):
    origin = request.build_absolute_uri("/").rstrip("/")
    entries = list_catalog_entries(group)
    return {
        "catalog": {
            "version": "1.0",
            "standards": ["openapi", "rfc9727"],
            "service_desc": f"{origin}/swagger.json",
            "service_doc": f"{origin}/redoc/",
            "api_catalog": f"{origin}/.well-known/api-catalog",
        },
        "apis": [catalog_summary(entry, request) for entry in entries],
    }


def rfc9727_linkset(request):
    origin = request.build_absolute_uri("/").rstrip("/")
    return {
        "linkset": [
            {
                "anchor": f"{origin}/api/v2/",
                "service-desc": [
                    {
                        "href": f"{origin}/swagger.json",
                        "type": "application/json",
                        "title": "OpenAPI",
                    }
                ],
                "service-doc": [
                    {
                        "href": f"{origin}/redoc/",
                        "type": "text/html",
                        "title": "ReDoc",
                    }
                ],
            }
        ]
    }


def parse_field_tokens(raw_values):
    tokens = []
    for raw in raw_values or []:
        if raw is None:
            continue
        for part in str(raw).split(","):
            part = part.strip()
            if part:
                tokens.append(part)
    return tokens


def parse_mws_fields_filter(raw_values):
    tokens = [token.lower() for token in parse_field_tokens(raw_values)]
    if not tokens:
        return None
    unknown = [token for token in tokens if token not in MWS_FORTNIGHT_FIELD_NAMES]
    if unknown:
        raise ValueError(
            "Unknown fields value(s): "
            + ", ".join(sorted(set(unknown)))
            + ". Use one of: "
            + ", ".join(sorted(MWS_FORTNIGHT_FIELD_NAMES))
        )
    return set(tokens)


def filter_mws_fortnight_fields(payload, fields):
    if not fields or not isinstance(payload, dict):
        return payload
    fortnight = dict(payload.get("fortnight") or {})
    units = dict(payload.get("fortnight_units") or {})
    keep = {"time"} | set(fields)
    return {
        **payload,
        "fortnight": {key: value for key, value in fortnight.items() if key in keep},
        "fortnight_units": {
            key: value
            for key, value in units.items()
            if key in keep or key == "time_step"
        },
    }


def parse_kyl_fields_filter(raw_values):
    allowed = set(KYL_INDICATOR_UNIT_OVERRIDES)
    tokens = [token.lower() for token in parse_field_tokens(raw_values)]
    if not tokens:
        return None
    unknown = [token for token in tokens if token not in allowed]
    if unknown:
        raise ValueError(
            "Unknown fields value(s): "
            + ", ".join(sorted(set(unknown)))
            + ". See GET /api/v2/catalog/get_mws_kyl_indicators/ for names."
        )
    return set(tokens)


def filter_kyl_fields(payload, fields):
    if not fields or not isinstance(payload, dict):
        return payload
    keep = set(fields) | {"mws_id"}
    indicators = payload.get("indicators")
    units = payload.get("indicator_units") or {}
    if isinstance(indicators, dict):
        filtered = {key: value for key, value in indicators.items() if key in keep}
        filtered_units = {key: value for key, value in units.items() if key in keep}
        return {**payload, "indicators": filtered, "indicator_units": filtered_units}
    if isinstance(indicators, list):
        filtered_rows = []
        for row in indicators:
            if isinstance(row, dict):
                filtered_rows.append(
                    {key: value for key, value in row.items() if key in keep}
                )
            else:
                filtered_rows.append(row)
        filtered_units = {key: value for key, value in units.items() if key in keep}
        return {
            **payload,
            "indicators": filtered_rows,
            "indicator_units": filtered_units,
        }
    return payload
