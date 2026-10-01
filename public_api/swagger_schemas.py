import copy

from drf_yasg import openapi

from .catalog import CATALOG_OUTPUT_DESCRIPTION
from .dataset_filters import tehsil_data_type_help_markdown

# ============= COMMON PARAMETERS =============

# Location Parameters
latitude_param = openapi.Parameter(
    "latitude",
    openapi.IN_QUERY,
    description="Latitude coordinate (-90 to 90)",
    type=openapi.TYPE_NUMBER,
    required=True,
)

longitude_param = openapi.Parameter(
    "longitude",
    openapi.IN_QUERY,
    description="Longitude coordinate (-180 to 180)",
    type=openapi.TYPE_NUMBER,
    required=True,
)

# Administrative Parameters
state_param = openapi.Parameter(
    "state",
    openapi.IN_QUERY,
    description="Name of the state (e.g. 'Uttar Pradesh')",
    type=openapi.TYPE_STRING,
    required=True,
)

district_param = openapi.Parameter(
    "district",
    openapi.IN_QUERY,
    description="Name of the district (e.g. 'Jaunpur')",
    type=openapi.TYPE_STRING,
    required=True,
)

tehsil_param = openapi.Parameter(
    "tehsil",
    openapi.IN_QUERY,
    description="Name of the tehsil (e.g. 'Badlapur')",
    type=openapi.TYPE_STRING,
    required=True,
)

# MWS Parameters
mws_id_param = openapi.Parameter(
    "mws_id",
    openapi.IN_QUERY,
    description="Unique MWS identifier (e.g. '12_234647')",
    type=openapi.TYPE_STRING,
    required=True,
)

mws_id_optional_param = openapi.Parameter(
    "mws_id",
    openapi.IN_QUERY,
    description="Unique MWS identifier (e.g. '12_234647'). Optional; if omitted returns all MWS geometries in the tehsil.",
    type=openapi.TYPE_STRING,
    required=False,
)

village_id_param = openapi.Parameter(
    "village_id",
    openapi.IN_QUERY,
    description="Village identifier (vill_ID). Optional; if omitted returns all villages in the tehsil layer.",
    type=openapi.TYPE_STRING,
    required=False,
)

tehsil_data_filter_param = openapi.Parameter(
    "data",
    openapi.IN_QUERY,
    description=(
        "Omit or pass `all` to return every dataset generated for this tehsil. "
        "Pass one or more sheet names: `data=drought,stream_order` or "
        "`data=drought&data=stream_order`."
    ),
    type=openapi.TYPE_STRING,
    required=False,
)

mws_fields_filter_param = openapi.Parameter(
    "fields",
    openapi.IN_QUERY,
    description=(
        "Optional v2 fortnight metrics. Omit for every metric. Pass one or "
        "more of `et`, `runoff`, `precipitation`: `fields=et,runoff`."
    ),
    type=openapi.TYPE_STRING,
    required=False,
)

kyl_fields_filter_param = openapi.Parameter(
    "fields",
    openapi.IN_QUERY,
    description=(
        "Optional v2 indicator filter. Omit for every indicator. Pass names "
        "from GET /api/v2/catalog/get_mws_kyl_indicators/."
    ),
    type=openapi.TYPE_STRING,
    required=False,
)

catalog_group_param = openapi.Parameter(
    "group",
    openapi.IN_QUERY,
    description="Optional group filter: `dataset` or `waterbody`.",
    type=openapi.TYPE_STRING,
    required=False,
)

catalog_api_id_param = openapi.Parameter(
    "api_id",
    openapi.IN_PATH,
    description="Catalog id such as `get_mws_data`. List ids with GET /api/v2/catalog/.",
    type=openapi.TYPE_STRING,
    required=True,
)

active_locations_state_filter_param = openapi.Parameter(
    "state",
    openapi.IN_QUERY,
    description="Optional state name filter (e.g. 'Rajasthan').",
    type=openapi.TYPE_STRING,
    required=False,
)

active_locations_district_filter_param = openapi.Parameter(
    "district",
    openapi.IN_QUERY,
    description="Optional district name filter (e.g. 'Bhilwara').",
    type=openapi.TYPE_STRING,
    required=False,
)

active_locations_tehsil_filter_param = openapi.Parameter(
    "tehsil",
    openapi.IN_QUERY,
    description="Optional tehsil / block name filter (e.g. 'Mandalgarh'). Alias: `block`.",
    type=openapi.TYPE_STRING,
    required=False,
)

active_locations_block_filter_param = openapi.Parameter(
    "block",
    openapi.IN_QUERY,
    description="Optional block name filter. Same as `tehsil`.",
    type=openapi.TYPE_STRING,
    required=False,
)

# File Type Parameters
file_type_param = openapi.Parameter(
    "file_type",
    openapi.IN_QUERY,
    description="Output format - 'json' or 'excel' (default: 'excel')",
    type=openapi.TYPE_STRING,
    required=False,
)

# Authorization Parameters
authorization_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API Key in format: <your-api-key>",
    type=openapi.TYPE_STRING,
    required=True,
)

# ============= COMMON RESPONSES =============

# Error Responses
bad_request_response = openapi.Response(description="Bad Request - Invalid parameters")

unauthorized_response = openapi.Response(
    description="Unauthorized - Invalid or missing API key"
)

not_found_response = openapi.Response(description="Not Found - Data not found")

internal_error_response = openapi.Response(description="Internal Server Error")

# ============= COMMON EXAMPLES =============
def success_example(data):
    return {"status": "success", "error_message": None, "data": data}


def error_example(message, details=None):
    payload = {"status": "error", "error_message": message, "error": message}
    if details is not None:
        payload["details"] = details
    return payload


def v1_error_example(message):
    return {"error": message}


def _set_json_example(schema, status_code, payload, description=None):
    existing = (schema.get("responses") or {}).get(status_code)
    desc = description
    if desc is None and existing is not None:
        desc = getattr(existing, "description", None) or "Response"
    schema.setdefault("responses", {})[status_code] = openapi.Response(
        description=desc,
        examples={"application/json": payload},
    )


ADMIN_V1_EXAMPLE = {
    "State": "UTTAR PRADESH",
    "District": "JAUNPUR",
    "Tehsil": "BADLAPUR",
}
ADMIN_V2_EXAMPLE = {
    "admin_details": dict(ADMIN_V1_EXAMPLE),
    "admin_field_hints": {
        "State": "state_name",
        "District": "district_name",
        "Tehsil": "tehsil_or_block_name",
    },
}
MWS_LATLON_V1_EXAMPLE = {
    "State": "UTTAR PRADESH",
    "District": "JAUNPUR",
    "Tehsil": "BADLAPUR",
    "mws_id": "12_234647",
    "uid": "12_234647",
}
MWS_LATLON_V2_EXAMPLE = {
    "mws_details": {
        "uid": "12_234647",
        "State": "UTTAR PRADESH",
        "District": "JAUNPUR",
        "Tehsil": "BADLAPUR",
    },
    "mws_field_hints": {
        "uid": "mws_identifier",
        "State": "state_name",
        "District": "district_name",
        "Tehsil": "tehsil_or_block_name",
    },
}
TEHSIL_V1_EXAMPLE = {
    "aquifer_vector": [
        {
            "uid": "12_207597",
            "area_in_ha": 2336.11,
            "aquifer_class": "Alluvium",
        }
    ],
    "Soge_vector": ["..............."],
}
TEHSIL_V2_EXAMPLE = {
    "tehsil_data": {
        "aquifer_vector": [
            {
                "uid": "12_207597",
                "area_in_ha": 2336.11,
                "aquifer_class": "Alluvium",
            }
        ]
    },
    "tehsil_units": {
        "aquifer_vector": {"area_in_ha": "ha"},
    },
}
KYL_V1_EXAMPLE = [
    {
        "mws_id": "12_234647",
        "terraincluster_id": 1,
        "avg_precipitation": 764.45,
        "total_nrega_assets": 550,
    }
]
KYL_V2_EXAMPLE = {
    "indicators": {
        "mws_id": "12_234647",
        "terraincluster_id": 1,
        "avg_precipitation": 764.45,
        "total_nrega_assets": 550,
    },
    "indicator_units": {
        "mws_id": "id",
        "terraincluster_id": "id",
        "avg_precipitation": "mm",
        "total_nrega_assets": "count",
    },
}
LAYER_V1_EXAMPLE = [
    {
        "layer_name": "SOGE",
        "dataset_name": "SOGE",
        "layer_type": "vector",
        "layer_url": "https://geoserver.core-stack.org/geoserver/wfs?...",
        "layer_version": "1.0",
        "style_url": "",
        "gee_asset_path": "projects/ee-.../asset",
    }
]
LAYER_V2_EXAMPLE = {
    "layers": LAYER_V1_EXAMPLE,
    "layer_field_units": {
        "layer_name": "name",
        "dataset_name": "name",
        "layer_type": "vector|raster|point|custom",
        "layer_url": "geoserver_wfs_or_wcs_url",
        "layer_version": "version_label",
        "style_url": "style_url_or_empty",
        "gee_asset_path": "earth_engine_asset_id_or_null",
    },
}
REPORT_V1_EXAMPLE = {
    "Mws_report_url": "http://127.0.0.1:8000/api/v1/generate_mws_report/?state=uttar_pradesh&district=bara_banki&block=fatehpur&uid=12_208104"
}
REPORT_V2_EXAMPLE = {
    "report": dict(REPORT_V1_EXAMPLE),
    "report_field_hints": {"Mws_report_url": "mws_pdf_or_html_report_url"},
}
ACTIVE_LOCATIONS_V1_EXAMPLE = [
    {
        "label": "Rajasthan",
        "value": "1251",
        "state_id": "8",
        "district": [
            {
                "label": "Bhilwara",
                "district_id": "123",
                "blocks": [{"label": "Mandalgarh", "value": 1}],
            }
        ],
    }
]
ACTIVE_LOCATIONS_V2_EXAMPLE = {
    "locations": ACTIVE_LOCATIONS_V1_EXAMPLE,
    "location_field_hints": {
        "label": "display_name",
        "value": "ordinal_code_in_ui_list",
        "state_id": "state_identifier",
        "district_id": "district_identifier",
        "block_id": "block_tehsil_identifier",
        "district": "districts_under_state",
        "blocks": "blocks_tehsils_under_district",
    },
}
MWS_FC_EXAMPLE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"uid": "12_208104"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [75.02716659, 25.2401886],
                        [75.0868641493802, 25.20231618101583],
                        [75.091234567, 25.251234567],
                        [75.02716659, 25.2401886],
                    ]
                ],
            },
        }
    ],
}
VILLAGE_FC_EXAMPLE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "jamui_jamui.1",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [86.11306, 24.75025],
                            [86.11629, 24.74811],
                            [86.12035, 24.74641],
                            [86.11306, 24.75025],
                        ]
                    ]
                ],
            },
            "properties": {"vill_ID": 258411, "vill_name": "Example Village"},
        }
    ],
}


V2_MWS_FORTNIGHT_DESCRIPTION = """
**``/api/v2/get_mws_data/`` only** — ``data`` uses Open-Meteo-style **fortnight** arrays (~15-day steps):

```json
{
  "metadata": { "mws_id": "12_208104" },
  "fortnight": {
    "time": ["2024-01-01", "2024-01-15"],
    "et": [2.5, 3.1],
    "runoff": [1.3, 0.8],
    "precipitation": [10.2, 5.4]
  },
  "fortnight_units": {
    "time": "iso8601",
    "time_step": "15_days",
    "et": "mm",
    "runoff": "mm",
    "precipitation": "mm"
  }
}
```

Query ``fields=et,runoff`` keeps only those metrics (``time`` is always returned).
Query ``regenerate=true`` bypasses MongoDB cache. ``/api/v1/get_mws_data/`` returns legacy ``data.time_series`` rows instead.
"""


def v2_schema_from(base_schema, operation_id, path_suffix):
    schema = dict(base_schema)
    schema["operation_id"] = operation_id
    schema["tags"] = ["Dataset APIs v2"]
    if "responses" in schema:
        schema["responses"] = copy.deepcopy(schema["responses"])
    if "manual_parameters" in schema:
        schema["manual_parameters"] = list(schema["manual_parameters"])
    return schema


# ============= API SCHEMAS =============

# Admin Details by Lat Lon Schema
admin_by_latlon_schema = {
    "method": "get",
    "operation_id": "get_admin_details_by_latlon",
    "operation_summary": "Get Admin Details by Lat Lon",
    "operation_description": """
    Resolve a latitude and longitude to the ``State``, ``District``, and
    ``Tehsil`` names CoRE Stack uses to store datasets.

    Copy those three names into the other dataset APIs (tehsil data,
    micro-watershed geometries, generated layers, and waterbodies).
    Confirm the tehsil is listed in Get Active Locations before you call
    those routes. If it is missing, request it with the
    [Geospatial Data Request Form](https://docs.google.com/forms/d/e/1FAIpQLSesYshZg_HmNc0FgF-JSBye-AeN6mdyrhF2cjGmqLYeD7WgZA/viewform).

    ``latitude`` and ``longitude`` are required WGS84 coordinates. The point
    must fall inside the Survey of India boundary. v1 returns the raw admin
    object. Out-of-boundary points return ``{"error": "..."}``.
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin details.",
            examples={"application/json": ADMIN_V1_EXAMPLE},
        ),
        400: openapi.Response(
            description="Bad Request - Invalid latitude/longitude input.",
            examples={
                "application/json": v1_error_example(
                    "Both 'latitude' and 'longitude' parameters are required."
                )
            },
        ),
        401: unauthorized_response,
        404: openapi.Response(
            description="Not Found - Latitude and longitude is not in SOI boundary.",
            examples={
                "application/json": v1_error_example(
                    "Latitude and longitude is not in SOI boundary."
                )
            },
        ),
        500: internal_error_response,
    },
    "tags": ["Dataset APIs v1"],
}

# MWS ID by Lat Lon Schema
mws_by_latlon_schema = {
    "method": "get",
    "operation_id": "get_mwsid_by_latlon",
    "operation_summary": "Get MWSID by Lat Lon",
    "operation_description": """
    Resolve a latitude and longitude to the micro-watershed that contains that point.

    ``latitude`` and ``longitude`` are required WGS84 coordinates.
    ``uid`` (also called ``mws_id``) is the join key for the time series,
    KYL indicators, geometry, and the watershed report. The response also
    includes the State, District, and Tehsil names for that watershed.

    v1 returns the raw object. There is no status envelope.
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin detail with mws_id.",
            examples={"application/json": MWS_LATLON_V1_EXAMPLE},
        ),
        400: bad_request_response,
        401: unauthorized_response,
        404: not_found_response,
        500: internal_error_response,
    },
    "tags": ["Dataset APIs v1"],
}

# MWS Data Schema
get_mws_data_schema = {
    "method": "get",
    "operation_id": "get_mws_data",
    "operation_summary": "Get MWS Time Series Data",
    "operation_description": """
    Fetch hydrology time series for one micro-watershed: ET, runoff, precipitation, and NDVI.

    Requires ``state``, ``district``, ``tehsil``, and ``mws_id``.
    Names may use spaces or underscores.

    v1 returns the raw payload with ``mws_id`` and a ``time_series`` row list.
    There is no ``{status, data}`` envelope. Missing IDs return ``{"error": "..."}``.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        mws_id_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns MWS time series data",
            examples={
                "application/json": {
                    "mws_id": "12_208104",
                    "time_series": [],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters or invalid format",
            examples={
                "application/json": {
                    "error": "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                }
            },
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - MWS ID not found",
            examples={
                "application/json": {"error": "Data not found for the given mws_id"}
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "Exception": "Error message details",
                }
            },
        ),
    },
    "tags": ["Dataset APIs v1"],
}


get_mws_data_v2_schema = {
    "method": "get",
    "operation_id": "get_mws_data_v2",
    "operation_summary": "Get MWS Time Series Data (v2 fortnight format)",
    "operation_description": """
    Fortnight hydrology for one micro-watershed: evapotranspiration, runoff,
    and precipitation, in steps of about 15 days. Values are in millimetres.

    Requires ``state``, ``district``, ``tehsil``, and ``mws_id`` (the ``uid``
    from Get MWSID by Lat Lon). Optional ``regenerate=true`` skips the cache
    and rereads GeoServer. Optional ``fields=et,runoff`` keeps only those
    metrics. ``time`` is always returned.

    v2 returns ``{status, error_message, data}``. ``data`` has ``metadata``,
    aligned ``fortnight`` arrays, and ``fortnight_units``.
    """
    + "\n\n"
    + V2_MWS_FORTNIGHT_DESCRIPTION.strip(),
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        mws_id_param,
        openapi.Parameter(
            "regenerate",
            openapi.IN_QUERY,
            description="Set true/1/yes to bypass MongoDB cache and refresh from GeoServer",
            type=openapi.TYPE_STRING,
            required=False,
        ),
        mws_fields_filter_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - fortnight-aligned MWS time series in v2 envelope",
            schema=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    "status": openapi.Schema(type=openapi.TYPE_STRING, example="success"),
                    "error_message": openapi.Schema(type=openapi.TYPE_STRING),
                    "data": openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        required=["metadata", "fortnight", "fortnight_units"],
                        properties={
                            "metadata": openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    "mws_id": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="Micro-watershed identifier",
                                    )
                                },
                            ),
                            "fortnight": openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    "time": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_STRING),
                                        description="Period start date for each ~15-day step",
                                    ),
                                    "et": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_NUMBER),
                                        description="Evapotranspiration (mm)",
                                    ),
                                    "runoff": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_NUMBER),
                                        description="Runoff (mm)",
                                    ),
                                    "precipitation": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_NUMBER),
                                        description="Precipitation (mm)",
                                    ),
                                },
                            ),
                            "fortnight_units": openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    "time": openapi.Schema(
                                        type=openapi.TYPE_STRING, example="iso8601"
                                    ),
                                    "time_step": openapi.Schema(
                                        type=openapi.TYPE_STRING, example="15_days"
                                    ),
                                    "et": openapi.Schema(
                                        type=openapi.TYPE_STRING, example="mm"
                                    ),
                                    "runoff": openapi.Schema(
                                        type=openapi.TYPE_STRING, example="mm"
                                    ),
                                    "precipitation": openapi.Schema(
                                        type=openapi.TYPE_STRING, example="mm"
                                    ),
                                },
                            ),
                        },
                    ),
                },
            ),
            examples={
                "application/json": success_example(
                    {
                        "metadata": {"mws_id": "12_208104"},
                        "fortnight": {
                            "time": ["2024-01-01", "2024-01-15"],
                            "et": [2.5, 3.1],
                            "runoff": [1.3, 0.8],
                            "precipitation": [10.2, 5.4],
                        },
                        "fortnight_units": {
                            "time": "iso8601",
                            "time_step": "15_days",
                            "et": "mm",
                            "runoff": "mm",
                            "precipitation": "mm",
                        },
                    }
                )
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters or invalid format",
            examples={
                "application/json": error_example(
                    "'state', 'district', 'tehsil', and 'mws_id' parameters are required."
                )
            },
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - MWS ID not found",
            examples={
                "application/json": error_example("Data not found for the given mws_id")
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": error_example(
                    "Internal server error while fetching MWS data",
                    details="Error message details",
                )
            },
        ),
    },
    "tags": ["Dataset APIs v2"],
}


# Tehsil Data Schema
tehsil_data_schema = {
    "method": "get",
    "operation_id": "get_tehsil_data",
    "operation_summary": "Get Tehsil Data",
    "operation_description": """
    Return the analytical datasets CoRE Stack has generated for one tehsil.

    Requires ``state``, ``district``, and ``tehsil`` — the same names from
    Get Active Locations or Get Admin Details by Lat Lon. The body is every
    sheet in that tehsil's file (drought, hydrology, land use, MGNREGA, and
    others), keyed by sheet name, with one row per micro-watershed.

    v1 returns that raw object. There is no ``data=`` filter and no status envelope.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data for the tehsil.",
            examples={"application/json": TEHSIL_V1_EXAMPLE},
        ),
        400: openapi.Response(
            description="Bad Request - 'state', 'district', and 'tehsil' are required. OR State/District/Tehsil must contain only letters, spaces, and underscores"
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - Data not found for this state, district, tehsil."
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}


# KYL Indicators Schema
kyl_indicators_schema = {
    "method": "get",  # ✅ Changed = to :
    "operation_id": "get_mws_kyl_indicators",
    "operation_summary": "Get MWS KYL Indicators",
    "operation_description": """
    Return a single-row KYL indicator snapshot for one micro-watershed (not a time series).

    Requires ``state``, ``district``, ``tehsil``, and ``mws_id``.
    Use this for terrain class, average rainfall, and asset counts on one watershed.

    v1 returns the raw indicator object for that ``mws_id``.
    There is no status envelope.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        mws_id_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data of the KYL Indicator for the mws_id.",
            examples={"application/json": KYL_V1_EXAMPLE},
        ),
        400: openapi.Response(
            description="Bad Request - 'state', 'district', 'tehsil', and 'mws_id' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores OR MWS id can only contain numbers and underscores"
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - Data not found for this state, district, tehsil. OR Not Found - Data not found for the given mws_id."
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}

# Generated Layer URLs Schema
generated_layer_urls_schema = {
    "method": "get",
    "operation_id": "get_generated_layer_urls",
    "operation_summary": "Get Generated Layer URL",
    "operation_description": """
    Return every generated dataset layer for one tehsil.

    Requires ``state``, ``district``, and ``tehsil`` — the same strings from
    Get Active Locations or Get Admin Details by Lat Lon. Each record has a
    GeoServer ``layer_url`` (WFS for vectors, WCS for rasters). Open that URL
    to read the raw layer and use it in QGIS, a WFS client, or any analysis
    or integration.

    v1 returns the raw layer records. Missing locations return ``{"error": "..."}``.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data for the generated layers.",
            examples={"application/json": LAYER_V1_EXAMPLE},
        ),
        400: openapi.Response(
            description="Bad Request - 'state', 'district', and 'tehsil' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores"
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - Data not found for this state, district, tehsil."
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}


# MWS Report URLs Schema
mws_report_urls_schema = {
    "method": "get",  # ✅ Changed = to :
    "operation_id": "get_mws_report",
    "operation_summary": "Get MWS Report URL",
    "operation_description": """
    Get a URL that opens or generates the MWS PDF/HTML report.

    Requires ``state``, ``district``, ``tehsil``, and ``mws_id``.
    The stats file and MWS layer must already exist.

    v1 returns a raw object with ``Mws_report_url``. There is no status envelope.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        mws_id_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON having mws report url.",
            examples={"application/json": REPORT_V1_EXAMPLE},
        ),
        400: openapi.Response(
            description="Bad Request - 'state', 'district', 'tehsil', and 'mws_id' parameters are required. OR State/District/Tehsil must contain only letters, spaces, and underscores OR MWS id can only contain numbers and underscores"
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - Data not found for the given mws_id OR Data not found for this state, district, tehsil. OR Mws Layer not found for the given location."
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}


# MWS Geometry Schema
mws_geometries_schema = {
    "method": "get",
    "operation_id": "get_mws_geometries",
    "operation_summary": "Get MWS Geometry",
    "operation_description": """
    Return micro-watershed polygons for a tehsil.

    Requires ``state``, ``district``, and ``tehsil``. Omit ``mws_id`` for every MWS;
    pass ``mws_id`` for a single feature.

    v2 wraps the result as ``{status, error_message, data}``. Without ``mws_id``,
    ``data`` is a GeoJSON FeatureCollection with actual vertices. With ``mws_id``,
    ``data`` has ``mws_geometry`` and field hints. Save ``data`` to open the
    collection in QGIS.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        mws_id_optional_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - One MWS geometry, or all tehsil geometries when mws_id is omitted.",
            examples={
                "application/json": success_example(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {"uid": "12_208104"},
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [75.02716659, 25.2401886],
                                            [75.0868641493802, 25.20231618101583],
                                            [75.091234567, 25.251234567],
                                            [75.02716659, 25.2401886],
                                        ]
                                    ],
                                },
                            }
                        ],
                    }
                )
            },
        ),
        400: openapi.Response(
            description="Bad Request - 'state', 'district', and 'tehsil' are required. Invalid mws_id format if provided."
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - MWS layer or mws_id not found for this location"
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}


village_geometries_schema = {
    "method": "get",
    "operation_id": "get_village_geometries",
    "operation_summary": "Get Village Geometries",
    "operation_description": """
    Return village / panchayat polygons for a tehsil.

    Requires ``state``, ``district``, and ``tehsil``. Optional ``village_id``
    keeps a single feature.

    v2 wraps a GeoJSON FeatureCollection in ``data``. Rings are the actual
    GeoServer vertices, not two-decimal points. Save ``data`` to open the
    file in QGIS.
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        village_id_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns a GeoJSON FeatureCollection of village polygons",
            examples={
                "application/json": success_example(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "id": "jamui_jamui.1",
                                "geometry": {
                                    "type": "MultiPolygon",
                                    "coordinates": [
                                        [
                                            [
                                                [86.11306, 24.75025],
                                                [86.11629, 24.74811],
                                                [86.12035, 24.74641],
                                                [86.11306, 24.75025],
                                            ]
                                        ]
                                    ],
                                },
                                "properties": {
                                    "vill_ID": 258411,
                                    "vill_name": "Example Village",
                                },
                            }
                        ],
                    }
                )
            },
        ),
        400: openapi.Response(description="Bad Request - invalid input or layer schema issue"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - layer or village not found"),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs v1"],
}


### Get active locations
generate_active_locations_schema = {
    "method": "get",
    "operation_id": "generate_active_locations",
    "operation_summary": "Get Active Locations",
    "operation_description": """
    Return the state → district → tehsil tree for locations where the full
    public dataset is already generated.

    Building every tehsil takes time, so this list is not all of India.
    Partners request specific tehsils; we generate those first. Use the
    returned names as the exact ``state``, ``district``, and ``tehsil``
    values on other dataset routes.

    To request a new location, submit the
    [Geospatial Data Request Form](https://docs.google.com/forms/d/e/1FAIpQLSesYshZg_HmNc0FgF-JSBye-AeN6mdyrhF2cjGmqLYeD7WgZA/viewform).

    v1 returns the raw nested tree. There is no status envelope and no
    place filter.
    """,
    "manual_parameters": [
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns activated locations data",
            examples={"application/json": ACTIVE_LOCATIONS_V1_EXAMPLE},
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {"Exception": "Error message details"}
            },
        ),
    },
    "tags": ["Dataset APIs v1"],
}


### Get MWS Geometry
### Get MWS Geometry
get_mws_geometries_schema = {
    "method": "get",
    "operation_id": "get_mws_geometries",
    "operation_summary": "Get MWS Geometries",
    "operation_description": """
    Return every micro-watershed boundary in a tehsil as a GeoJSON FeatureCollection.

    Requires ``state``, ``district``, and ``tehsil``. Each feature has
    ``properties.uid`` and a MultiPolygon or Polygon ring.

    v1 returns the FeatureCollection at the top level so QGIS can open the file.
    Vertices are the actual GeoServer coordinates, not two-decimal points.

    **Example response:**
    ```json
    {
        "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "mws_amaravati_achalpur.1",
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": [
                            [
                                [
                                    [77.311209, 21.226113],
                                    [77.311195, 21.22611],
                                    [77.311185, 21.226108],
                                    [77.311552, 21.226182],
                                    [77.311209, 21.226113]
                                ]
                            ]
                        ]
                    },
                    "geometry_name": "the_geom",
                    "properties": {
                        "uid": "1_523"
                    }
                }
            ]
        }
    ```
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns GeoJSON FeatureCollection with all MWS geometries",
            examples={
                "application/json": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": "mws_amaravati_achalpur.1",
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [
                                        [
                                            [77.311209, 21.226113],
                                            [77.311195, 21.22611],
                                            [77.311185, 21.226108],
                                            [77.311552, 21.226182],
                                            [77.311209, 21.226113],
                                        ]
                                    ]
                                ],
                            },
                            "geometry_name": "the_geom",
                            "properties": {"uid": "1_235"},
                        },
                        {
                            "type": "Feature",
                            "id": "mws_amaravati_achalpur.2",
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [
                                        [
                                            [77.312345, 21.227890],
                                            [77.312456, 21.228000],
                                            [77.312567, 21.228111],
                                            [77.312345, 21.227890],
                                        ]
                                    ]
                                ],
                            },
                            "geometry_name": "the_geom",
                            "properties": {"uid": "1_424"},
                        },
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters or invalid format",
            examples={
                "application/json": {
                    "error": "'state', 'district', and 'tehsil' parameters are required."
                }
            },
        ),
        401: openapi.Response(
            description="Unauthorized - Invalid or missing API key",
            examples={
                "application/json": {
                    "error": "Authentication credentials were not provided."
                }
            },
        ),
        404: openapi.Response(
            description="Not Found - No MWS features found in layer",
            examples={"application/json": {"error": "No features found in layer"}},
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={"application/json": {"error": "Internal server error"}},
        ),
    },
    "tags": ["Dataset APIs v1"],
}


### Get Village Geometries
get_village_geometries_schema = {
    "method": "get",
    "operation_id": "get_village_geometries",
    "operation_summary": "Get Village Geometries",
    "operation_description": """
    Return every village / panchayat boundary in a tehsil as a GeoJSON FeatureCollection.

    Requires ``state``, ``district``, and ``tehsil``. Features include
    ``vill_ID``, ``vill_name``, and MultiPolygon rings.

    v1 returns the FeatureCollection at the top level so QGIS can open it.
    Coordinates are the actual vertices from GeoServer.

    **Example response:**
    ```json
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "amaravati_achalpur.3",
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": [
                            [
                                [
                                    [77.311209, 21.226113],
                                    [77.311195, 21.22611],
                                    [77.311185, 21.226108],
                                    [77.311552, 21.226182],
                                    [77.311209, 21.226113]
                                ]
                            ]
                        ]
                    },
                    "geometry_name": "the_geom",
                    "properties": {
                        "vill_ID": 0,
                        "vill_name": "ALIPUR"
                    }
                }
            ]
        }
    ```
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns GeoJSON FeatureCollection with all village geometries",
            examples={
                "application/json": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": "amaravati_achalpur.3",
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [
                                        [
                                            [77.311209, 21.226113],
                                            [77.311195, 21.22611],
                                            [77.311185, 21.226108],
                                            [77.311552, 21.226182],
                                            [77.311209, 21.226113],
                                        ]
                                    ]
                                ],
                            },
                            "geometry_name": "the_geom",
                            "properties": {"vill_ID": 0, "vill_name": "ALIPUR"},
                        },
                        {
                            "type": "Feature",
                            "id": "amaravati_achalpur.4",
                            "geometry": {
                                "type": "MultiPolygon",
                                "coordinates": [
                                    [
                                        [
                                            [77.312345, 21.227890],
                                            [77.312456, 21.228000],
                                            [77.312567, 21.228111],
                                            [77.312345, 21.227890],
                                        ]
                                    ]
                                ],
                            },
                            "geometry_name": "the_geom",
                            "properties": {"vill_ID": 1, "vill_name": "BHAGPUR"},
                        },
                    ],
                }
            },
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters or invalid format",
            examples={
                "application/json": {
                    "error": "'state', 'district', and 'tehsil' parameters are required."
                }
            },
        ),
        401: openapi.Response(
            description="Unauthorized - Invalid or missing API key",
            examples={
                "application/json": {
                    "error": "Authentication credentials were not provided."
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "error": "Internal server error: Unexpected error occurred"
                }
            },
        ),
    },
    "tags": ["Dataset APIs v1"],
}

# ============= V2 API SCHEMAS (distinct operation_id for Swagger/ReDoc) =============

admin_by_latlon_schema_v2 = v2_schema_from(
    admin_by_latlon_schema,
    "get_admin_details_by_latlon_v2",
    "get_admin_details_by_latlon/",
)
_set_json_example(
    admin_by_latlon_schema_v2,
    200,
    success_example(ADMIN_V2_EXAMPLE),
    "Success - admin details in the v2 envelope",
)
_set_json_example(
    admin_by_latlon_schema_v2,
    400,
    error_example("Both 'latitude' and 'longitude' parameters are required."),
    "Bad Request - Invalid latitude/longitude input.",
)
_set_json_example(
    admin_by_latlon_schema_v2,
    404,
    error_example("Latitude and longitude is not in SOI boundary."),
    "Not Found - Latitude and longitude is not in SOI boundary.",
)
admin_by_latlon_schema_v2["operation_description"] = """
Resolve a latitude and longitude to the ``State``, ``District``, and
``Tehsil`` names CoRE Stack uses to store datasets.

Copy those three names into the other dataset APIs (tehsil data,
micro-watershed geometries, generated layers, and waterbodies).
Confirm the tehsil is listed in Get Active Locations before you call
those routes. If it is missing, request it with the
[Geospatial Data Request Form](https://docs.google.com/forms/d/e/1FAIpQLSesYshZg_HmNc0FgF-JSBye-AeN6mdyrhF2cjGmqLYeD7WgZA/viewform).

``latitude`` and ``longitude`` are required WGS84 coordinates. The point
must fall inside the Survey of India boundary. v2 returns
``{status, error_message, data}`` with ``admin_details`` and
``admin_field_hints``.
"""
mws_by_latlon_schema_v2 = v2_schema_from(
    mws_by_latlon_schema,
    "get_mwsid_by_latlon_v2",
    "get_mwsid_by_latlon/",
)
_set_json_example(
    mws_by_latlon_schema_v2,
    200,
    success_example(MWS_LATLON_V2_EXAMPLE),
    "Success - MWS id and admin details in the v2 envelope",
)
mws_by_latlon_schema_v2["operation_description"] = """
Resolve a latitude and longitude to the micro-watershed that contains that point.

``latitude`` and ``longitude`` are required WGS84 coordinates.
Use ``uid`` as ``mws_id`` on the time series, KYL indicators, geometry,
and watershed report. The response also includes the State, District,
and Tehsil names for that watershed.

v2 returns ``{status, error_message, data}`` with ``mws_details`` and
``mws_field_hints`` inside ``data``.
"""
tehsil_data_schema_v2 = v2_schema_from(
    tehsil_data_schema,
    "get_tehsil_data_v2",
    "get_tehsil_data/",
)
tehsil_data_schema_v2["operation_description"] = f"""
Return the analytical datasets CoRE Stack has generated for one tehsil.

Requires ``state``, ``district``, and ``tehsil`` — the same names from
Get Active Locations or Get Admin Details by Lat Lon. Omit ``data``, or
pass ``data=all``, for every sheet in that tehsil's file (drought,
hydrology, land use, MGNREGA, and others). Each sheet is one row per
micro-watershed. ``tehsil_units`` gives the measurement unit of each column.

To fetch fewer sheets, pass names such as ``data=drought,stream_order``.
A tehsil file may contain only some of the sheets listed below.

v2 returns ``{{status, error_message, data}}`` with ``tehsil_data`` and
``tehsil_units``.

{tehsil_data_type_help_markdown()}
"""
tehsil_data_schema_v2["manual_parameters"] = list(
    tehsil_data_schema_v2["manual_parameters"]
) + [tehsil_data_filter_param]
_set_json_example(
    tehsil_data_schema_v2,
    200,
    success_example(TEHSIL_V2_EXAMPLE),
    "Success - tehsil sheets in the v2 envelope",
)
kyl_indicators_schema_v2 = v2_schema_from(
    kyl_indicators_schema,
    "get_mws_kyl_indicators_v2",
    "get_mws_kyl_indicators/",
)
kyl_indicators_schema_v2["operation_description"] = """
Single-row Know Your Landscape snapshot for one micro-watershed.
This is not a time series. It includes terrain class, average rainfall,
drought category, cropping, and asset counts.

Requires ``state``, ``district``, ``tehsil``, and ``mws_id``.
Optional ``fields=avg_runoff,drought_category`` keeps only those indicators.
``mws_id`` is always returned.

v2 returns ``{status, error_message, data}``. ``data`` has ``indicators``
and ``indicator_units`` (mm, ha, count, or a class label).
"""
kyl_indicators_schema_v2["manual_parameters"] = list(
    kyl_indicators_schema_v2["manual_parameters"]
) + [kyl_fields_filter_param]
_set_json_example(
    kyl_indicators_schema_v2,
    200,
    success_example(KYL_V2_EXAMPLE),
    "Success - KYL indicators in the v2 envelope",
)
generated_layer_urls_schema_v2 = v2_schema_from(
    generated_layer_urls_schema,
    "get_generated_layer_urls_v2",
    "get_generated_layer_urls/",
)
generated_layer_urls_schema_v2["operation_description"] = """
Return every generated dataset layer for one tehsil.

Requires ``state``, ``district``, and ``tehsil`` — the same strings from
Get Active Locations or Get Admin Details by Lat Lon. Each record has a
GeoServer ``layer_url`` (WFS for vectors, WCS for rasters). Open that URL
to read the raw layer and use it in QGIS, a WFS client, or any analysis
or integration.

v2 returns ``{status, error_message, data}`` with ``layers`` and
``layer_field_units``.
"""
_set_json_example(
    generated_layer_urls_schema_v2,
    200,
    success_example(LAYER_V2_EXAMPLE),
    "Success - generated layer URLs in the v2 envelope",
)
mws_report_urls_schema_v2 = v2_schema_from(
    mws_report_urls_schema,
    "get_mws_report_urls_v2",
    "get_mws_report/",
)
mws_report_urls_schema_v2["operation_description"] = """
URL of the PDF or HTML report for one micro-watershed.

Requires ``state``, ``district``, ``tehsil``, and ``mws_id``.
The tehsil stats file and the micro-watershed layer must already exist.
Open ``Mws_report_url`` to read the report.

v2 returns ``{status, error_message, data}``. ``data`` has
``report.Mws_report_url`` and ``report_field_hints``.
"""
_set_json_example(
    mws_report_urls_schema_v2,
    200,
    success_example(REPORT_V2_EXAMPLE),
    "Success - MWS report URL in the v2 envelope",
)
mws_geometries_schema_v2 = v2_schema_from(
    mws_geometries_schema,
    "get_mws_geometries_v2",
    "get_mws_geometries/",
)
_set_json_example(
    mws_geometries_schema_v2,
    200,
    success_example(MWS_FC_EXAMPLE),
    "Success - MWS FeatureCollection in the v2 envelope",
)
village_geometries_schema_v2 = v2_schema_from(
    village_geometries_schema,
    "get_village_geometries_v2",
    "get_village_geometries/",
)
_set_json_example(
    village_geometries_schema_v2,
    200,
    success_example(VILLAGE_FC_EXAMPLE),
    "Success - village FeatureCollection in the v2 envelope",
)
generate_active_locations_schema_v2 = v2_schema_from(
    generate_active_locations_schema,
    "get_active_locations_v2",
    "get_active_locations/",
)
generate_active_locations_schema_v2["operation_description"] = """
Return the state → district → tehsil tree for locations where the full
public dataset is already generated.

Building every tehsil takes time, so this list is not all of India.
Partners request specific tehsils; we generate those first. Use the
returned names as the exact ``state``, ``district``, and ``tehsil``
values on other dataset routes.

Optional filters: ``state``, ``district``, and ``tehsil`` (alias ``block``).
Name matches are case-insensitive.

To request a new location, submit the
[Geospatial Data Request Form](https://docs.google.com/forms/d/e/1FAIpQLSesYshZg_HmNc0FgF-JSBye-AeN6mdyrhF2cjGmqLYeD7WgZA/viewform).

v2 returns ``{status, error_message, data}`` with ``locations`` and
``location_field_hints``.
"""
generate_active_locations_schema_v2["manual_parameters"] = list(
    generate_active_locations_schema_v2["manual_parameters"]
) + [
    active_locations_state_filter_param,
    active_locations_district_filter_param,
    active_locations_tehsil_filter_param,
    active_locations_block_filter_param,
]
_set_json_example(
    generate_active_locations_schema_v2,
    200,
    success_example(ACTIVE_LOCATIONS_V2_EXAMPLE),
    "Success - activated locations in the v2 envelope",
)
_set_json_example(
    generate_active_locations_schema_v2,
    500,
    error_example(
        "Internal server error while generating active locations",
        details="Error message details",
    ),
    "Internal Server Error",
)

CATALOG_PROPERTY_ITEM_SCHEMA = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        "name": openapi.Schema(type=openapi.TYPE_STRING, example="et"),
        "type": openapi.Schema(type=openapi.TYPE_STRING, example="number[]"),
        "unit": openapi.Schema(
            description=(
                "Measurement unit, such as mm. For get_tehsil_data this is an "
                "object of column name to unit, matching tehsil_units."
            ),
            type=openapi.TYPE_STRING,
            example="mm",
        ),
        "description": openapi.Schema(
            type=openapi.TYPE_STRING,
            example="Evapotranspiration for each ~15-day step",
        ),
        "selectable": openapi.Schema(
            type=openapi.TYPE_BOOLEAN,
            description="If true, pass this name to fields= or data=",
        ),
    },
)

CATALOG_API_SUMMARY_SCHEMA = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        "id": openapi.Schema(type=openapi.TYPE_STRING, example="get_mws_data"),
        "group": openapi.Schema(type=openapi.TYPE_STRING, example="dataset"),
        "path": openapi.Schema(type=openapi.TYPE_STRING, example="/api/v2/get_mws_data/"),
        "method": openapi.Schema(type=openapi.TYPE_STRING, example="GET"),
        "title": openapi.Schema(type=openapi.TYPE_STRING),
        "description": openapi.Schema(type=openapi.TYPE_STRING),
        "select_param": openapi.Schema(
            type=openapi.TYPE_STRING,
            description="Query param used to pick properties: fields or data",
        ),
        "property_count": openapi.Schema(type=openapi.TYPE_INTEGER, example=6),
        "properties_url": openapi.Schema(
            type=openapi.TYPE_STRING,
            example="/api/v2/catalog/get_mws_data/",
        ),
        "href": openapi.Schema(type=openapi.TYPE_STRING),
    },
)

CATALOG_LIST_EXAMPLE = {
    "catalog": {
        "version": "1.0",
        "description": CATALOG_OUTPUT_DESCRIPTION,
        "standards": ["openapi", "rfc9727"],
        "service_desc": "https://geoserver.core-stack.org/swagger.json",
        "service_doc": "https://geoserver.core-stack.org/redoc/",
        "api_catalog": "https://geoserver.core-stack.org/.well-known/api-catalog",
    },
    "apis": [
        {
            "id": "get_mws_data",
            "group": "dataset",
            "path": "/api/v2/get_mws_data/",
            "method": "GET",
            "title": "Get MWS Time Series Data",
            "description": "Fortnight hydrology time series. Filter metrics with fields=et,runoff.",
            "select_param": "fields",
            "property_count": 6,
            "properties_url": "/api/v2/catalog/get_mws_data/",
        }
    ],
}

CATALOG_ITEM_EXAMPLE = {
    "id": "get_mws_data",
    "group": "dataset",
    "path": "/api/v2/get_mws_data/",
    "method": "GET",
    "title": "Get MWS Time Series Data",
    "description": "Fortnight hydrology time series. Filter metrics with fields=et,runoff.",
    "select_param": "fields",
    "property_count": 6,
    "properties_url": "/api/v2/catalog/get_mws_data/",
    "parameters": [
        {
            "name": "state",
            "in": "query",
            "type": "string",
            "required": True,
            "description": "State name from Get Active Locations",
        }
    ],
    "properties": [
        {
            "name": "et",
            "type": "number[]",
            "unit": "mm",
            "description": "Evapotranspiration for each ~15-day step",
            "selectable": True,
        },
        {
            "name": "runoff",
            "type": "number[]",
            "unit": "mm",
            "description": "Runoff for each ~15-day step",
            "selectable": True,
        },
        {
            "name": "precipitation",
            "type": "number[]",
            "unit": "mm",
            "description": "Precipitation for each ~15-day step",
            "selectable": True,
        },
    ],
}

RFC9727_EXAMPLE = {
    "linkset": [
        {
            "anchor": "https://geoserver.core-stack.org/api/v2/",
            "service-desc": [
                {
                    "href": "https://geoserver.core-stack.org/swagger.json",
                    "type": "application/json",
                    "title": "OpenAPI",
                }
            ],
            "service-doc": [
                {
                    "href": "https://geoserver.core-stack.org/redoc/",
                    "type": "text/html",
                    "title": "ReDoc",
                }
            ],
        }
    ]
}

rfc9727_api_catalog_schema = {
    "method": "get",
    "operation_id": "get_rfc9727_api_catalog",
    "operation_summary": "Get RFC 9727 API Catalog",
    "operation_description": """
    Return the IETF RFC 9727 API catalog for this host.

    Agents fetch ``/.well-known/api-catalog`` first. The body is a linkset
    whose ``service-desc`` points at ``/swagger.json`` and whose
    ``service-doc`` points at ReDoc. No API key is required.

    For the property list used with ``fields=`` / ``data=``, call
    GET /api/v2/catalog/ after you have a key.
    """,
    "manual_parameters": [],
    "responses": {
        200: openapi.Response(
            description="RFC 9727 linkset pointing at OpenAPI and ReDoc",
            schema=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    "linkset": openapi.Schema(
                        type=openapi.TYPE_ARRAY,
                        items=openapi.Schema(
                            type=openapi.TYPE_OBJECT,
                            properties={
                                "anchor": openapi.Schema(type=openapi.TYPE_STRING),
                                "service-desc": openapi.Schema(
                                    type=openapi.TYPE_ARRAY,
                                    items=openapi.Schema(type=openapi.TYPE_OBJECT),
                                ),
                                "service-doc": openapi.Schema(
                                    type=openapi.TYPE_ARRAY,
                                    items=openapi.Schema(type=openapi.TYPE_OBJECT),
                                ),
                            },
                        ),
                    )
                },
            ),
            examples={"application/json": RFC9727_EXAMPLE},
        ),
    },
    "tags": ["Catalog"],
}

catalog_list_schema_v2 = {
    "method": "get",
    "operation_id": "get_public_api_catalog_v2",
    "operation_summary": "Get Public API Catalog",
    "operation_description": f"""
    List every public v2 dataset and waterbody route, with a pointer to the
    properties each one can return.

    {CATALOG_OUTPUT_DESCRIPTION}

    Requires ``X-API-Key``. Optional ``group=dataset`` or ``group=waterbody``.
    v2 returns ``{{status, error_message, data}}``. ``data.catalog`` holds
    version, this description, standards, and links to OpenAPI, ReDoc, and
    the RFC 9727 catalog. ``data.apis`` is the route list.
    """,
    "manual_parameters": [catalog_group_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - catalog index in the v2 envelope",
            schema=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    "status": openapi.Schema(type=openapi.TYPE_STRING, example="success"),
                    "error_message": openapi.Schema(type=openapi.TYPE_STRING),
                    "data": openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        properties={
                            "catalog": openapi.Schema(
                                type=openapi.TYPE_OBJECT,
                                properties={
                                    "version": openapi.Schema(type=openapi.TYPE_STRING),
                                    "description": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="What this catalog lists and how to read each route",
                                    ),
                                    "standards": openapi.Schema(
                                        type=openapi.TYPE_ARRAY,
                                        items=openapi.Schema(type=openapi.TYPE_STRING),
                                    ),
                                    "service_desc": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="OpenAPI URL",
                                    ),
                                    "service_doc": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="ReDoc URL",
                                    ),
                                    "api_catalog": openapi.Schema(
                                        type=openapi.TYPE_STRING,
                                        description="RFC 9727 well-known URL",
                                    ),
                                },
                            ),
                            "apis": openapi.Schema(
                                type=openapi.TYPE_ARRAY,
                                items=CATALOG_API_SUMMARY_SCHEMA,
                            ),
                        },
                    ),
                },
            ),
            examples={"application/json": success_example(CATALOG_LIST_EXAMPLE)},
        ),
        400: openapi.Response(
            description="Bad Request - invalid group filter",
            examples={
                "application/json": error_example("group must be dataset or waterbody.")
            },
        ),
        401: unauthorized_response,
        500: internal_error_response,
    },
    "tags": ["Catalog"],
}

catalog_item_schema_v2 = {
    "method": "get",
    "operation_id": "get_public_api_catalog_item_v2",
    "operation_summary": "Get Public API Catalog Item",
    "operation_description": """
    Return parameters and properties for one public v2 API.

    ``api_id`` is the catalog id from GET /api/v2/catalog/, for example
    ``get_mws_data`` or ``get_tehsil_data``. Requires ``X-API-Key``.

    ``parameters`` lists query arguments (name, type, required, description).
    ``properties`` lists each returned field: ``name``, ``type``, ``unit``,
    ``description``, and ``selectable``. Pass selectable names to ``fields=``
    on MWS fortnight metrics and KYL indicators, or to ``data=`` on tehsil
    sheets. For ``get_tehsil_data``, ``unit`` is an object of column name to
    measurement unit, matching ``tehsil_units`` on that route. A wide sheet
    such as ``antyodaya`` can have hundreds of columns in that object.

    v2 returns ``{status, error_message, data}`` with the route summary plus
    ``parameters`` and ``properties``.
    """,
    "manual_parameters": [catalog_api_id_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - catalog item with properties in the v2 envelope",
            schema=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    "status": openapi.Schema(type=openapi.TYPE_STRING, example="success"),
                    "error_message": openapi.Schema(type=openapi.TYPE_STRING),
                    "data": openapi.Schema(
                        type=openapi.TYPE_OBJECT,
                        properties={
                            "id": openapi.Schema(type=openapi.TYPE_STRING),
                            "path": openapi.Schema(type=openapi.TYPE_STRING),
                            "method": openapi.Schema(type=openapi.TYPE_STRING),
                            "parameters": openapi.Schema(
                                type=openapi.TYPE_ARRAY,
                                items=openapi.Schema(type=openapi.TYPE_OBJECT),
                            ),
                            "properties": openapi.Schema(
                                type=openapi.TYPE_ARRAY,
                                items=CATALOG_PROPERTY_ITEM_SCHEMA,
                            ),
                        },
                    ),
                },
            ),
            examples={"application/json": success_example(CATALOG_ITEM_EXAMPLE)},
        ),
        401: unauthorized_response,
        404: openapi.Response(
            description="Not Found - unknown catalog id",
            examples={
                "application/json": error_example("Unknown catalog id 'not_an_api'.")
            },
        ),
        500: internal_error_response,
    },
    "tags": ["Catalog"],
}
