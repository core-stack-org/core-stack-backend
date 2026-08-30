# Developer Guide: Documentation for the Public APIs

This guide provides a professional walkthrough for documenting new API endpoints in the `public_api` app using `drf-yasg`.

---

## Quick Access

| Resource | URL |
| :--- | :--- |
| **Swagger UI** | [http://127.0.0.1:8000/swagger/](http://127.0.0.1:8000/swagger/) |
| **ReDoc** | [http://127.0.0.1:8000/](http://127.0.0.1:8000/) |

---


## Implementation Workflow

### Step 1 — Define Parameters (`swagger_schemas.py`)

Before creating a new parameter, always check the **COMMON PARAMETERS** section first. Reusing existing parameters prevents duplication and ensures consistency across API documentation.

If your parameter doesn't exist yet, define it using `openapi.Parameter`:

```python
# External definition makes parameters reusable across multiple APIs
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

authorization_param = openapi.Parameter(
    "X-API-Key",
    openapi.IN_HEADER,
    description="API key for authentication",
    type=openapi.TYPE_STRING,
    required=True,
)
```

**Parameter fields explained:**

| Field | Description |
| :--- | :--- |
| `name` | The parameter name as it appears in the request |
| `in` | Location: `openapi.IN_QUERY`, `openapi.IN_HEADER`, `openapi.IN_PATH`, or `openapi.IN_BODY` |
| `description` | Human-readable explanation shown in Swagger UI |
| `type` | Data type: `openapi.TYPE_STRING`, `openapi.TYPE_NUMBER`, `openapi.TYPE_INTEGER`, `openapi.TYPE_BOOLEAN` |
| `required` | `True` if the API cannot function without this parameter |

---

### Step 2 — Create the Schema Dictionary (`swagger_schemas.py`)

The schema dictionary defines the **"contract"** for your API — what it accepts, what it returns, and how it behaves. The example below uses `get_admin_details_by_lat_lon` as a reference implementation.

```python
admin_by_latlon_schema = {
    "method": "get",
    "operation_id": "get_admin_details_by_latlon",
    "operation_summary": "Get Admin Details by Lat Lon",
    "operation_description": "Retrieve administrative hierarchy (State, District, Tehsil) for a coordinate.",
    "manual_parameters": [
        latitude_param,
        longitude_param,
        authorization_param,  # Mandatory for secured APIs
    ],
    "responses": {
        200: openapi.Response(
            description="Success",
            examples={
                "application/json": {
                    "State": "UTTAR PRADESH",
                    "District": "JAUNPUR",
                    "Tehsil": "BADLAPUR",
                }
            },
        ),
        400: bad_request_response,
        401: unauthorized_response,
    },
    "tags": ["Dataset APIs"],
}
```

**Schema dictionary fields explained:**

| Field | Purpose |
| :--- | :--- |
| `method` | HTTP method: `"get"`, `"post"`, `"put"`, `"delete"` |
| `operation_id` | Unique identifier for this operation — must not repeat across the project |
| `operation_summary` | Short, one-line title shown in Swagger UI |
| `operation_description` | Longer explanation of what the API does |
| `manual_parameters` | List of `openapi.Parameter` objects |
| `responses` | Dictionary mapping HTTP status codes to `openapi.Response` objects |
| `tags` | List of category strings used for grouping in the UI |

**Predefined common responses** (import from `swagger_schemas.py`):

```python
bad_request_response = openapi.Response(
    description="Bad Request — Invalid or missing parameters"
)

unauthorized_response = openapi.Response(
    description="Unauthorized — Invalid or missing API key"
)
```

---

### Step 3 — Apply to View (`api.py`)

Import the schema dictionary and unpack it into the `@swagger_auto_schema` decorator using the `**` operator.

```python
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response

from .swagger_schemas import admin_by_latlon_schema
from utilities.auth_check_decorator import api_security_check


@swagger_auto_schema(**admin_by_latlon_schema)
@api_security_check(auth_type="API_key")
def get_admin_details_by_lat_lon(request):
    # Use request.query_params for GET requests in DRF
    lat = request.query_params.get("latitude")
    lon = request.query_params.get("longitude")
    
    # ... fetch data ...
    
    return Response(data)
```

> **Decorator order matters.** `@swagger_auto_schema` must be placed **above** `@api_security_check` and other decorators to ensure Swagger processes the metadata correctly.

---

## File Structure Reference

```
public_api/
├── api.py                  # View functions with @swagger_auto_schema decorators
├── swagger_schemas.py      # All parameter definitions and schema dictionaries
├── views.py                # Heavy business logic and helper functions
└── urls.py                 # URL routing
```

---

## Developer Checklist

When adding a new API, ensure you've completed all of these steps:

- [ ] **Parameter Reuse** — Used existing parameters from `swagger_schemas.py` wherever possible
- [ ] **New Parameters** — Any new parameters are defined at the top of `swagger_schemas.py` in the COMMON PARAMETERS section
- [ ] **Authorization** — Added `authorization_param` to `manual_parameters` if the view uses `@api_security_check`
- [ ] **Unique `operation_id`** — Provided a unique `operation_id` string to avoid Swagger UI conflicts
- [ ] **Realistic Example** — Included a real-world JSON example in the `200` response
- [ ] **Error Responses** — Mapped all relevant error codes (`400`, `401`, `404`, etc.) to responses
- [ ] **Tags** — Assigned a tag to group the API correctly in the Swagger UI
- [ ] **Decorator Order** — Confirmed `@swagger_auto_schema` is above all other decorators on the view

---

## Troubleshooting

### Swagger page fails to load

**Cause:** Syntax errors in `swagger_schemas.py` — most commonly missing commas, unclosed brackets, or incorrect dictionary nesting.

**Fix:** Check your schema dictionary carefully for:
- Missing `,` between list items in `manual_parameters`
- Unclosed `{` or `[` in the `responses` block
- Incorrect indentation in nested `openapi.Response(...)` calls

### `operation_id` conflict warning

**Cause:** Two schema dictionaries share the same `operation_id` value.

**Fix:** Ensure every `operation_id` is globally unique across the entire project. Use a naming convention such as `verb_resource_by_qualifier` (e.g., `get_admin_details_by_latlon`).

### API key not being sent in requests

**Cause:** The Swagger UI prompts for authorization using the header field label, and users may enter the key in the wrong field.

**Fix:** In the Swagger UI, the API key must be provided in the `X-API-Key` **header field**, not as a query parameter. Inform API consumers of this when sharing documentation.

### Parameters not showing up in Swagger UI

**Cause:** The parameter was defined but not added to `manual_parameters` in the schema dictionary.

**Fix:** Ensure the parameter variable is included in the `manual_parameters` list in the relevant schema.

---

## Quick Reference: `openapi` Types

| Constant | Value | Use Case |
| :--- | :--- | :--- |
| `openapi.TYPE_STRING` | `"string"` | Text values, UUIDs, slugs |
| `openapi.TYPE_NUMBER` | `"number"` | Floats (e.g., lat/lon) |
| `openapi.TYPE_INTEGER` | `"integer"` | Whole numbers (e.g., IDs) |
| `openapi.TYPE_BOOLEAN` | `"boolean"` | True/false flags |
| `openapi.IN_QUERY` | `"query"` | URL query string (`?key=value`) |
| `openapi.IN_HEADER` | `"header"` | HTTP request header |
| `openapi.IN_PATH` | `"path"` | URL path segment (`/resource/{id}/`) |
| `openapi.IN_BODY` | `"body"` | POST/PUT request body |

---

