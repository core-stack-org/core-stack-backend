"""Sample MCP server for the CoRE Stack public API catalog.

The model lists routes from GET /api/v2/catalog/, reads parameters and column
units from GET /api/v2/catalog/{api_id}/, then calls that route.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

BASE_URL = os.environ.get("CORE_STACK_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
API_KEY = os.environ.get("CORE_STACK_API_KEY", "")
WIDE_UNIT_MAP = 20

mcp = MCPServer(
    name="core-stack-public-api",
    instructions=(
        "CoRE Stack public data. Call list_public_apis, then describe_public_api "
        "for the route you need, then call_public_api. "
        "Tehsil sheets use the data= parameter. MWS time series and KYL indicators "
        "use fields=. Property units are the measurement units of each column. "
        "Start with get_active_locations when you need a real state, district, and tehsil."
    ),
)


def _get(path: str, query: dict | None = None) -> dict:
    if not API_KEY:
        return {
            "status": "error",
            "error_message": "Set the CORE_STACK_API_KEY environment variable.",
            "data": None,
        }
    params = {
        key: str(value)
        for key, value in (query or {}).items()
        if value is not None and str(value) != ""
    }
    url = f"{BASE_URL}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={"X-API-Key": API_KEY, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"status": "error", "error_message": raw, "data": None}
        body["http_status"] = exc.code
        return body
    except urllib.error.URLError as exc:
        return {
            "status": "error",
            "error_message": f"Could not reach {BASE_URL}: {exc.reason}",
            "data": None,
        }


def _unwrap(body: dict):
    if isinstance(body, dict) and body.get("status") == "error":
        raise ToolError(body.get("error_message") or "Request failed")
    if not isinstance(body, dict):
        return body
    return body.get("data", body)


def _shape_property(prop: dict, expand: bool) -> dict:
    unit = prop.get("unit")
    if not isinstance(unit, dict) or expand or len(unit) <= WIDE_UNIT_MAP:
        return prop
    sample = dict(list(unit.items())[:8])
    return {
        **prop,
        "unit": {
            "column_count": len(unit),
            "sample": sample,
            "note": f"Pass property_name={prop['name']} to list every column unit.",
        },
    }


def _shrink_result(data):
    """Keep tool results small enough to read in a chat."""
    if isinstance(data, dict) and "tehsil_data" in data:
        sheets = {}
        units = data.get("tehsil_units") or {}
        for name, rows in (data.get("tehsil_data") or {}).items():
            first = rows[0] if isinstance(rows, list) and rows else rows
            sheets[name] = {
                "row_count": len(rows) if isinstance(rows, list) else None,
                "units": units.get(name),
                "first_row": first,
            }
        return {
            "note": "First row of each sheet. The API itself returns every row.",
            "sheets": sheets,
        }
    text = json.dumps(data, default=str)
    if len(text) <= 12000:
        return data
    return {
        "truncated": True,
        "characters": len(text),
        "preview": text[:4000],
        "note": "Response was shortened for the chat. Call the API with a narrower fields= or data= filter.",
    }


@mcp.tool()
def list_public_apis(group: str = "") -> dict:
    """List CoRE Stack public v2 APIs from GET /api/v2/catalog/.

    group is optional: dataset or waterbody.
    """
    query = {"group": group} if group else None
    data = _unwrap(_get("/api/v2/catalog/", query))
    apis = []
    for item in data.get("apis") or []:
        apis.append(
            {
                "id": item.get("id"),
                "group": item.get("group"),
                "title": item.get("title"),
                "path": item.get("path"),
                "select_param": item.get("select_param"),
                "property_count": item.get("property_count"),
                "description": item.get("description"),
            }
        )
    return {"base_url": BASE_URL, "count": len(apis), "apis": apis}


@mcp.tool()
def describe_public_api(api_id: str, property_name: str = "") -> dict:
    """Parameters, selectable names, and units for one catalog API.

    Use property_name to expand one sheet, for example drought on get_tehsil_data.
    Selectable names are what you pass to fields= or data=.
    """
    data = _unwrap(_get(f"/api/v2/catalog/{api_id}/"))
    if not isinstance(data, dict):
        raise ToolError("Catalog item was not an object.")
    expand = property_name.strip()
    properties = data.get("properties") or []
    if expand and expand not in {item.get("name") for item in properties}:
        names = ", ".join(str(item.get("name")) for item in properties)
        raise ToolError(f"No property named {expand!r} on {api_id}. Names: {names}")
    shown = [
        _shape_property(item, expand == item.get("name"))
        for item in properties
        if not expand or item.get("name") == expand
    ]
    return {
        "id": data.get("id"),
        "path": data.get("path"),
        "description": data.get("description"),
        "select_param": data.get("select_param"),
        "parameters": data.get("parameters"),
        "properties": shown,
    }


@mcp.tool()
def call_public_api(api_id: str, query: dict[str, str] | None = None) -> dict:
    """Call one catalogued public API.

    query is the URL query string as an object, for example
    {"state": "Rajasthan", "district": "Sirohi", "tehsil": "Abu Road", "data": "mws"}.
    Read describe_public_api first. Use data= for tehsil sheets and fields= for
    MWS or KYL metrics.
    """
    described = describe_public_api(api_id)
    path = described.get("path")
    if not path:
        raise ToolError(f"{api_id} has no path in the catalog.")
    params = dict(query or {})
    select_param = described.get("select_param")
    if select_param and params.get(select_param):
        allowed = {
            item["name"]
            for item in described.get("properties") or []
            if item.get("selectable") and isinstance(item.get("name"), str)
        }
        if select_param == "data":
            allowed.add("all")
        unknown = [
            token.strip()
            for token in str(params[select_param]).split(",")
            if token.strip() and token.strip() not in allowed
        ]
        if unknown:
            raise ToolError(
                f"Unknown {select_param} value(s): {', '.join(unknown)}. "
                f"Use one of: {', '.join(sorted(allowed))}."
            )
    data = _shrink_result(_unwrap(_get(path, params)))
    if select_param == "data" and isinstance(data, dict) and "sheets" in data:
        requested = [
            token.strip()
            for token in str(params.get("data") or "").split(",")
            if token.strip() and token.strip() != "all"
        ]
        missing = [token for token in requested if token not in data["sheets"]]
        if missing:
            data["missing_sheets"] = missing
            data["note"] = (
                "The catalog lists these sheets, but this tehsil file did not include them. "
                + data.get("note", "")
            ).strip()
    return {
        "id": api_id,
        "path": path,
        "query": {key: str(value) for key, value in params.items() if value not in (None, "")},
        "data": data,
    }


if __name__ == "__main__":
    mcp.run(transport="stdio")
