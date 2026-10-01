"""Machine-readable catalog of public v2 APIs and the properties they return.

OpenAPI (``/swagger.json``) remains the human/codegen contract. RFC 9727
``/.well-known/api-catalog`` points agents at that spec. Route text, property
descriptions, and units are loaded from ``catalog.json``.
"""

from .catalog_document import catalog_document, public_properties

RFC9727_PROFILE = "https://www.rfc-editor.org/info/rfc9727"
RFC9727_CONTENT_TYPE = f'application/linkset+json; profile="{RFC9727_PROFILE}"'

_DOCUMENT = catalog_document()
CATALOG_OUTPUT_DESCRIPTION = _DOCUMENT["description"]


def _load_catalog():
    apis = []
    for entry in _DOCUMENT["apis"]:
        apis.append(
            {
                "id": entry["id"],
                "group": entry["group"],
                "path": entry["path"],
                "method": entry["method"],
                "title": entry["title"],
                "description": entry["description"],
                "select_param": entry["select_param"],
                "parameters": list(entry["parameters"]),
                "properties": public_properties(entry["properties"]),
            }
        )
    return tuple(apis)


PUBLIC_API_CATALOG = _load_catalog()
CATALOG_BY_ID = {entry["id"]: entry for entry in PUBLIC_API_CATALOG}
MWS_FORTNIGHT_FIELD_NAMES = {
    item["name"]
    for item in CATALOG_BY_ID["get_mws_data"]["properties"]
    if item["selectable"]
}


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
            "description": CATALOG_OUTPUT_DESCRIPTION,
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
    allowed = {item["name"] for item in CATALOG_BY_ID["get_mws_kyl_indicators"]["properties"]}
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
