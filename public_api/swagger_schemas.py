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
    Retrieve admin data based on given latitude and longitude coordinates.
    
    **Response dataset details:**
    ```
    [
        "State": "State name",
        "District": "District name",
        "Tehsil": "Tehsil name"
    ]
    ```
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin details.",
            examples={
                "application/json": success_example(
                    {
                        "State": "UTTAR PRADESH",
                        "District": "JAUNPUR",
                        "Tehsil": "BADLAPUR",
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
    Retrieve MWS ID data based on given latitude and longitude coordinates.
    
    **Response dataset details:**
    ```
    [
        "uid": "MWS_id"
        "State": "State name",
        "District": "District name",
        "Tehsil": "Tehsil name"
    ]
    ```
    """,
    "manual_parameters": [latitude_param, longitude_param, authorization_param],
    "responses": {
        200: openapi.Response(
            description="Success - It will return JSON data having admin detail with mws_id.",
            examples={
                "application/json": success_example(
                    {
                        "uid": "12_234647",
                        "State": "UTTAR PRADESH",
                        "District": "JAUNPUR",
                        "Tehsil": "BADLAPUR",
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
    Retrieve MWS time series data, including ET, Runoff, and Precipitation for a given state, district, tehsil, and MWS ID.
    
    **Response dataset details:**
    ```
    {
        "mws_id": "12_208104",
        "time_series": [
            {
                "date": "2024-01-01",
                "et": 2.5,
                "runoff": 1.3,
                "precipitation": 10.2
            },
            {
                "date": "2024-01-15",
                "et": 3.1,
                "runoff": 0.8,
                "precipitation": 5.4
            }
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
    Retrieve KYL indicator data for a specific MWS ID in a given state, district, and tehsil.
    
    **Example Response:**
    ```
        [
            {
                "mws_id": "MWS id",
                "terraincluster_id": "Cluster id",
                "avg_precipitation": "Average precipitation in mm",
                "cropping_intensity_trend": "Cropping intensity trend value",
                "cropping_intensity_avg": "Average cropping Intensity",
                "avg_single_cropped": "Average Single cropped area",
                "avg_double_cropped": "Average Double cropped area",
                "avg_triple_cropped": "Average Triple cropped area",
                ".................": ".................",
                "avg_number_dry_spell": "Average number of dry spell",
                "avg_runoff": "Average runoff",
                "total_nrega_assets": "Total nrega assets"
            }
        ]
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
            description="Success - It will return JSON data of the KYL Indicator for the mws_id.",
            examples={
                "application/json": success_example(
                    [
                        {
                            "mws_id": "12_234647",
                            "terraincluster_id": 1,
                            "avg_precipitation": 764.4457,
                            "total_nrega_assets": 550,
                        }
                    ]
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
    Retrieve generated layer URLs for a given state, district, and tehsil.
    
    **Example Response:**
    ```
        [
                "layer_name": "Name of the layer",
                "layer_type": "Vector/ Raster",
                "layer_url": "Geoserver url for the layer",
                "layer_version": "Version of the layer",
                "style_url": "Url for the style",
                "gee_asset_path": "GEE Asset path for the layer"
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
            description="Success - It will return JSON data for the generated layers.",
            examples={
                "application/json": success_example(
                    [
                        {
                            "layer_name": "SOGE",
                            "layer_type": "vector",
                            "layer_version": "1.0",
                        }
                    ]
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
    Retrieve MWS report url for a given state, district, tehsil and mws_id.
    
    **Response dataset details:**
    ```
        [
            "Mws_report_url": "Url for the MWS report"
        ]
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
            description="Success - It will return JSON having mws report url.",
            examples={
                "application/json": success_example(
                    {
                        "Mws_report_url": "http://127.0.0.1:8000/api/v1/generate_mws_report/?state=uttar_pradesh&district=bara_banki&block=fatehpur&uid=12_208104"
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


### Get active locations
generate_active_locations_schema = {
    "method": "get",
    "operation_id": "generate_active_locations",
    "operation_summary": "Get Active Locations",
    "operation_description": """
    Return activated locations data.
    
    **Response dataset details:**
    ```
    {
        "state_name": {
            "districts": {
                "district_name": {
                    "tehsils": ["tehsil1", "tehsil2"]
                }
            }
        }
    }
    ```
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
                        "uttar_pradesh": {
                            "districts": {
                                "bara_banki": {"tehsils": ["fatehpur", "nawabganj"]}
                            }
                        }
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
