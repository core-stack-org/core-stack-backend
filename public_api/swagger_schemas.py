from drf_yasg import openapi

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

village_id_param = openapi.Parameter(
    "village_id",
    openapi.IN_QUERY,
    description="Village identifier (vill_ID). Optional; if omitted returns all villages in the tehsil layer.",
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

# ============= API SCHEMAS =============

# Admin Details by Lat Lon Schema
admin_by_latlon_schema = {
    "method": "get",
    "operation_id": "get_admin_details_by_latlon",
    "operation_summary": "Get Admin Details by Lat Lon",
    "operation_description": """
    Retrieve admin details for the provided coordinates.

    Returns a flat payload under ``data``:
    - ``admin_details`` with ``State``, ``District``, ``Tehsil``
    - ``admin_field_hints`` with field semantics
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin details.",
            examples={
                "application/json": success_example(
                    {
                        "admin_details": {
                            "State": "UTTAR PRADESH",
                            "District": "JAUNPUR",
                            "Tehsil": "BADLAPUR",
                        },
                        "admin_field_hints": {
                            "State": "state_name",
                            "District": "district_name",
                            "Tehsil": "tehsil_or_block_name",
                        },
                    }
                )
            },
        ),
        400: openapi.Response(
            description="Bad Request - Invalid latitude/longitude input.",
            examples={
                "application/json": error_example(
                    "Both 'latitude' and 'longitude' parameters are required."
                )
            },
        ),
        401: unauthorized_response,
        404: openapi.Response(
            description="Not Found - Latitude and longitude is not in SOI boundary.",
            examples={
                "application/json": error_example(
                    "Latitude and longitude is not in SOI boundary."
                )
            },
        ),
        500: internal_error_response,
    },
    "tags": ["Dataset APIs"],
}

# MWS ID by Lat Lon Schema
mws_by_latlon_schema = {
    "method": "get",
    "operation_id": "get_mwsid_by_latlon",
    "operation_summary": "Get MWSID by Lat Lon",
    "operation_description": """
    Retrieve MWS ID and admin details for the provided coordinates.

    Returns a flat payload under ``data``:
    - ``mws_details`` with ``uid``, ``State``, ``District``, ``Tehsil``
    - ``mws_field_hints`` with field semantics
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin detail with mws_id.",
            examples={
                "application/json": success_example(
                    {
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
                )
            },
        ),
        400: bad_request_response,
        401: unauthorized_response,
        404: not_found_response,
        500: internal_error_response,
    },
    "tags": ["Dataset APIs"],
}

# MWS Data Schema
get_mws_data_schema = {
    "method": "get",
    "operation_id": "get_mws_data",
    "operation_summary": "Get MWS Time Series Data",
    "operation_description": """
    Retrieve MWS time series data (ET, runoff, precipitation) for a given state, district, tehsil, and MWS ID.
    Values are aggregated on **15-day (~fortnight) steps** from GeoServer — not hourly data.

    **Normalized response (v1 ``/api/v1/get_mws_data`` and envelope):** arrays under ``fortnight``
    with ``fortnight_units.time_step`` = ``15_days``. Each ``date`` is the period start (ISO).

    **Raw upstream shape (GeoServer-style):**
    ```
    {
        "mws_id": "12_208104",
        "time_series": [
            {"date": "2024-01-01", "et": 2.5, "runoff": 1.3, "precipitation": 10.2},
            {"date": "2024-01-15", "et": 3.1, "runoff": 0.8, "precipitation": 5.4}
        ]
    }
    ```
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
                "application/json": success_example(
                    {
                        "mws_id": "12_208104",
                        "time_series": [
                            {
                                "date": "2024-01-01",
                                "et": 2.5,
                                "runoff": 1.3,
                                "precipitation": 10.2,
                            },
                            {
                                "date": "2024-01-15",
                                "et": 3.1,
                                "runoff": 0.8,
                                "precipitation": 5.4,
                            },
                        ],
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
    "tags": ["Dataset APIs"],
}


# Tehsil Data Schema
tehsil_data_schema = {
    "method": "get",
    "operation_id": "get_tehsil_data",
    "operation_summary": "Get Tehsil Data",
    "operation_description": """
    Retrieve tehsil-level JSON data for a given state, district, and tehsil.
    
    **Response dataset details:**
    ```
        [
           "aquifer_vector": [
                {
                    "uid": "MWS_id",
                    "area_in_ha": "Area for the mws",
                    "aquifer_class": "Class for the aquifer",
                    "principle_aq_alluvium_percent": "Total percentage area under aquifer class",
                    "principle_aq_banded gneissic complex_percent": "Total percentage area under aquifer class"
                }
              ]  
        ]
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
            description="Success - It will return JSON data for the tehsil.",
            examples={
                "application/json": success_example(
                    {
                        "aquifer_vector": [
                            {
                                "uid": "12_207597",
                                "area_in_ha": 2336.11,
                                "aquifer_class": "Alluvium",
                            }
                        ],
                        "Soge_vector": ["..............."],
                    }
                )
            },
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
    "tags": ["Dataset APIs"],
}


# KYL Indicators Schema
kyl_indicators_schema = {
    "method": "get",  # ✅ Changed = to :
    "operation_id": "get_mws_kyl_indicators",
    "operation_summary": "Get MWS KYL Indicators",
    "operation_description": """
    Retrieve **flat tabular KYL indicators** for a given MWS (not time series).

    Success payload shape (inside the standard envelope ``data``):

    - ``indicators``: single object when one row matches ``mws_id``, or an array when multiple rows match.
    - ``indicator_units``: map from field name to unit (``mm``, ``ha``, ``ratio``, ``count``, ``code``, etc.).
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
            examples={
                "application/json": success_example(
                    {
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
                )
            },
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
    "tags": ["Dataset APIs"],
}

# Generated Layer URLs Schema
generated_layer_urls_schema = {
    "method": "get",
    "operation_id": "get_generated_layer_urls",
    "operation_summary": "Get Generated Layer Url",
    "operation_description": """
    Retrieve generated GeoServer layer URLs for a given state, district, and tehsil.

    Not time-series data. Success ``data`` contains:

    - ``layers``: array of flat objects (``layer_name``, ``layer_type``, ``layer_url``, …).
    - ``layer_field_units``: map describing each field (URLs, asset ids, version labels).
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
            examples={
                "application/json": success_example(
                    {
                        "layers": [
                            {
                                "layer_name": "SOGE",
                                "layer_type": "vector",
                                "layer_url": "https://example/geoserver/wfs?...",
                                "layer_version": "1.0",
                                "style_url": "",
                                "gee_asset_path": "projects/ee-.../asset",
                            }
                        ],
                        "layer_field_units": {
                            "layer_name": "name",
                            "layer_type": "vector|raster|point|custom",
                            "layer_url": "geoserver_wfs_or_wcs_url",
                            "layer_version": "version_label",
                            "style_url": "style_url_or_empty",
                            "gee_asset_path": "earth_engine_asset_id_or_null",
                        },
                    }
                )
            },
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
    "tags": ["Dataset APIs"],
}


# MWS Report URLs Schema
mws_report_urls_schema = {
    "method": "get",  # ✅ Changed = to :
    "operation_id": "get_mws_report",
    "operation_summary": "Get MWS Report url",
    "operation_description": """
    Retrieve MWS report URL for a given state, district, tehsil, and mws_id.

    Not time-series data. Success ``data`` contains:

    - ``report``: object with ``Mws_report_url``.
    - ``report_field_hints``: field semantics.
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
            examples={
                "application/json": success_example(
                    {
                        "report": {
                            "Mws_report_url": "http://127.0.0.1:8000/api/v1/generate_mws_report/?state=uttar_pradesh&district=bara_banki&block=fatehpur&uid=12_208104"
                        },
                        "report_field_hints": {
                            "Mws_report_url": "mws_pdf_or_html_report_url"
                        },
                    }
                )
            },
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
    "tags": ["Dataset APIs"],
}


# MWS Geometry Schema
mws_geometries_schema = {
    "method": "get",
    "operation_id": "get_mws_geometries",
    "operation_summary": "Get MWS Geometry",
    "operation_description": """
    Retrieve GeoJSON geometry for a given state, district, tehsil, and mws_id.

    Not time-series data. Success ``data`` contains:

    - ``mws_geometry``: object with ``uid``, location fields, and ``geometry``.
    - ``mws_geometry_field_hints``: field semantics.
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
            description="Success - It will return geometry for the requested mws_id.",
            examples={
                "application/json": success_example(
                    {
                        "mws_geometry": {
                            "uid": "12_208104",
                            "state": "rajasthan",
                            "district": "alwar",
                            "tehsil": "alwar",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [[[76.62, 27.55], [76.63, 27.56]]],
                            },
                        },
                        "mws_geometry_field_hints": {
                            "uid": "mws_identifier",
                            "state": "normalized_state_name",
                            "district": "normalized_district_name",
                            "tehsil": "normalized_tehsil_name",
                            "geometry": "geojson_geometry_object",
                        },
                    }
                )
            },
        ),
        400: openapi.Response(
            description="Bad Request - Invalid/missing parameters or invalid mws_id format"
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(
            description="Not Found - MWS layer or mws_id not found for this location"
        ),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs"],
}


village_geometries_schema = {
    "method": "get",
    "operation_id": "get_village_geometries",
    "operation_summary": "Get Village Geometries",
    "operation_description": """
    Retrieve village boundary geometries for a given state, district, and tehsil.
    Optionally filter to one village using ``village_id``.

    Not time-series data. Success ``data`` contains:

    - ``villages``: array of village objects with geometry.
    - ``village_field_hints``: field semantics.
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
            description="Success - Returns village geometry objects",
            examples={
                "application/json": success_example(
                    {
                        "villages": [
                            {
                                "village_id": "12345",
                                "village_name": "Example Village",
                                "state": "rajasthan",
                                "district": "alwar",
                                "tehsil": "alwar",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": [[[76.62, 27.55], [76.63, 27.56]]],
                                },
                            }
                        ],
                        "village_field_hints": {
                            "village_id": "vill_ID_from_layer",
                            "village_name": "vill_name_from_layer",
                            "state": "normalized_state_name",
                            "district": "normalized_district_name",
                            "tehsil": "normalized_tehsil_name",
                            "geometry": "geojson_geometry_object",
                        },
                    }
                )
            },
        ),
        400: openapi.Response(description="Bad Request - invalid input or layer schema issue"),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        404: openapi.Response(description="Not Found - layer or village not found"),
        500: openapi.Response(description="Internal Server Error"),
    },
    "tags": ["Dataset APIs"],
}


### Get active locations
generate_active_locations_schema = {
    "method": "get",
    "operation_id": "generate_active_locations",
    "operation_summary": "Get Active Locations",
    "operation_description": """
    Return the **hierarchical list of activated states → districts → blocks/tehsils** (not time-series).

    Success ``data`` contains:

    - ``locations``: array of state objects, each with ``district`` → ``blocks`` (see live response).
    - ``location_field_hints``: what each repeated key means (ids, labels, nesting).
    """,
    "manual_parameters": [
        authorization_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Returns activated locations data",
            examples={
                "application/json": success_example(
                    {
                        "locations": [
                            {
                                "label": "Rajasthan",
                                "value": "1251",
                                "state_id": "8",
                                "district": [],
                            }
                        ],
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
                )
            },
        ),
        401: openapi.Response(description="Unauthorized - Invalid or missing API key"),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": error_example(
                    "Internal server error while generating active locations",
                    details="Error message details",
                )
            },
        ),
    },
    "tags": ["Dataset APIs"],
}


### Get MWS Geometry
### Get MWS Geometry
get_mws_geometries_schema = {
    "method": "get",
    "operation_id": "get_mws_geometries",
    "operation_summary": "Get MWS Geometries",
    "operation_description": """
    Retrieve MWS geometries for a given state, district, tehsil.

    **Response format:**
    Returns a GeoJSON geometry object containing the polygon coordinates of all the MWS boundary in a tehsil.

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
    "tags": ["Dataset APIs"],
}


### Get Village Geometries
get_village_geometries_schema = {
    "method": "get",
    "operation_id": "get_village_geometries",
    "operation_summary": "Get Village Geometries",
    "operation_description": """
    Retrieve village geometries for a given state, district and tehsil.

    **Response format:**
    Returns a GeoJSON geometry object containing the polygon coordinates of all the village boundary in a tehsil.

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
    "tags": ["Dataset APIs"],
}
