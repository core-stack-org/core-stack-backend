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

# Cache Parameters
regenerate_param = openapi.Parameter(
    "regenerate",
    openapi.IN_QUERY,
    description="Optional: set true/1/yes to bypass cache and regenerate data",
    type=openapi.TYPE_STRING,
    required=False,
)

# MWS Parameters
uuid_id_param = openapi.Parameter(
    "uid",
    openapi.IN_QUERY,
    description="Unique waterbodies identifier (e.g. '12_100174_104')",
    type=openapi.TYPE_STRING,
    required=True,
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

# Response examples
waterbodies_success_example = {
    "status": "success",
    "error_message": None,
    "location": {
        "state": "RAJASTHAN",
        "district": "bhilwara",
        "block": "mandalgarh",
    },
    "data": [
        {
            "UID": "12_100174_104",
            "MWS_UID": "12_100174",
            "waterbody_name": "Talab",
            "water": 1,
            "sum": 2.56,
            "zoi": 3,
            "zoi_area": 10.13,
            "zoi_properties": {
                "cropping_intensity_2017": 1.23,
                "total_cropable_area_ever_hydroyear_2017_2024": 50.02,
            },
        }
    ],
}

waterbodies_success_uid_example = {
    "status": "success",
    "error_message": None,
    "uid": "12_100174_104",
    "location": {
        "state": "RAJASTHAN",
        "district": "bhilwara",
        "block": "mandalgarh",
    },
    "data": {
        "UID": "12_100174_104",
        "MWS_UID": "12_100174",
        "water": 1,
    },
}

waterbodies_error_example = {
    "status": "error",
    "error_message": "Merged dataset not available for given area.",
    "error": "Merged dataset not available for given area.",
}

# ============= API SCHEMAS =============

# Admin Details by Lat Lon Schema
waterbodies_by_admin_schema = {
    "method": "get",
    "operation_id": "get_waterbodies_by_admin_and_uid",
    "operation_summary": "List remotely sensed waterbodies",
    "operation_description": """
    Lists every surface waterbody in a tehsil.

    Waterbodies are detected by a remote-sensing method developed by IIT
    Delhi students and implemented by the Core Stack team.

    **Returns**
    The full inventory. Each record includes ``UID``, ``MWS_UID``,
    ``waterbody_name``, detected-water flag, area (``sum``), zone of
    influence (``zoi``, ``zoi_area``), and ``zoi_properties`` such as
    cropping intensity and cropable area.

    **Related**
    Retrieve a waterbody
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        regenerate_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Waterbody dataset response.",
            examples={"application/json": waterbodies_success_example},
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters.",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "'state', 'district' and 'tehsil' (or 'block') parameters are required.",
                    "error": "'state', 'district' and 'tehsil' (or 'block') parameters are required.",
                }
            },
        ),
        401: unauthorized_response,
        404: openapi.Response(
            description="Not Found - UID does not exist in requested admin area.",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "UID '12_100174_999' not found for state=RAJASTHAN, district=bhilwara, block=mandalgarh.",
                    "error": "UID '12_100174_999' not found for state=RAJASTHAN, district=bhilwara, block=mandalgarh.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "Failed to generate merged dataset.",
                    "error": "Failed to generate merged dataset.",
                    "details": "Upstream source timeout",
                }
            },
        ),
        502: openapi.Response(
            description="Bad Gateway - merged dataset unavailable",
            examples={"application/json": waterbodies_error_example},
        ),
    },
    "tags": ["Waterbody APIs v1"],
}

waterbodies_by_uuid = {
    "method": "get",
    "operation_id": "get_waterbodies_by_uid",
    "operation_summary": "Retrieve a waterbody",
    "operation_description": """
    Retrieves one remotely sensed waterbody by ``uid``.

    Requires ``state``, ``district``, ``tehsil`` (or ``block``), and ``uid``.

    **Returns**
    The raw record for that UID. A missing UID returns 404.

    **Related**
    List remotely sensed waterbodies
    """,
    "manual_parameters": [
        state_param,
        district_param,
        tehsil_param,
        uuid_id_param,
        regenerate_param,
    ],
    "responses": {
        200: openapi.Response(
            description="Success - Waterbody-by-UID response.",
            examples={"application/json": waterbodies_success_uid_example},
        ),
        400: openapi.Response(
            description="Bad Request - Missing required parameters.",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "'state', 'district' and 'tehsil' (or 'block') parameters are required.",
                    "error": "'state', 'district' and 'tehsil' (or 'block') parameters are required.",
                }
            },
        ),
        401: unauthorized_response,
        404: openapi.Response(
            description="Not Found - UID does not exist in requested admin area.",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "UID '12_100174_999' not found for state=RAJASTHAN, district=bhilwara, block=mandalgarh.",
                    "error": "UID '12_100174_999' not found for state=RAJASTHAN, district=bhilwara, block=mandalgarh.",
                }
            },
        ),
        500: openapi.Response(
            description="Internal Server Error",
            examples={
                "application/json": {
                    "status": "error",
                    "error_message": "Internal server error while retrieving waterbody data.",
                    "error": "Internal server error while retrieving waterbody data.",
                    "details": "Unexpected exception",
                }
            },
        ),
        502: openapi.Response(
            description="Bad Gateway - merged dataset unavailable",
            examples={"application/json": waterbodies_error_example},
        ),
    },
    "tags": ["Waterbody APIs v1"],
}


def v2_waterbodies_schema_from(base_schema, operation_id, path_suffix):
    schema = dict(base_schema)
    schema["operation_id"] = operation_id
    schema["tags"] = ["Waterbody APIs v2"]
    return schema


waterbodies_by_admin_schema_v2 = v2_waterbodies_schema_from(
    waterbodies_by_admin_schema,
    "get_waterbodies_by_admin_and_uid_v2",
    "get_waterbodies_data_by_admin/",
)
waterbodies_by_admin_schema_v2["operation_summary"] = (
    "List remotely sensed waterbodies"
)
waterbodies_by_admin_schema_v2["operation_description"] = """
Lists every surface waterbody in a tehsil.

Waterbodies are detected by a remote-sensing method developed by IIT
Delhi students and implemented by the Core Stack team.

**Returns**
``{status, error_message, data}``. ``data`` is the full inventory with
field units. Each record includes ``UID``, ``MWS_UID``,
``waterbody_name``, area, ``zoi``, and ``zoi_properties``.

**Related**
Retrieve a waterbody
"""
waterbodies_by_uuid_schema_v2 = v2_waterbodies_schema_from(
    waterbodies_by_uuid,
    "get_waterbodies_by_uid_v2",
    "get_waterbody_data/",
)
waterbodies_by_uuid_schema_v2["operation_summary"] = "Retrieve a waterbody"
waterbodies_by_uuid_schema_v2["operation_description"] = """
Retrieves one remotely sensed waterbody by ``uid``.

**Returns**
``{status, error_message, data}`` for that UID. A missing UID sets
``error_message`` and 404.

**Related**
List remotely sensed waterbodies
"""
