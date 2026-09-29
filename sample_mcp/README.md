# CoRE Stack sample MCP server

This is a small [MCP](https://modelcontextprotocol.io) server you can add to Cursor or Claude Desktop. It does not copy the public API. It asks the catalog what exists, then calls those routes.

The backend must already be running. This sample was tried against `http://127.0.0.1:8000`.

## What the model can do

Three tools:

| Tool | What it calls | What you get |
| --- | --- | --- |
| `list_public_apis` | `GET /api/v2/catalog/` | Route id, path, and whether it uses `fields=` or `data=` |
| `describe_public_api` | `GET /api/v2/catalog/{api_id}/` | Parameters, selectable names, and column units |
| `call_public_api` | The path from the catalog | The API result, shortened so a chat can read it |

A normal turn is: list the APIs, describe the one you need, then call it. Tehsil sheets are chosen with `data=`. MWS time series and KYL indicators are chosen with `fields=`.

## Setup

From this folder:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Create an API key for the same host the server will call. For a local backend that is `http://127.0.0.1:8000`. Put the key in the environment, not in this folder.

Check the server before you add it to an editor:

```bash
CORE_STACK_API_KEY=your-key \
CORE_STACK_BASE_URL=http://127.0.0.1:8000 \
  .venv/bin/python try_it.py
```

You should see 10 dataset APIs, drought units in weeks and hectares, and the `mws` sheet for Abu Road (107 rows, area in `ha`).

## Add it to Cursor

Cursor Settings → MCP → Add a new MCP server, or put this in `~/.cursor/mcp.json`. Use the real path to this folder and your own key.

```json
{
  "mcpServers": {
    "core-stack-public-api": {
      "command": "/absolute/path/sample_mcp/.venv/bin/python",
      "args": ["/absolute/path/sample_mcp/server.py"],
      "env": {
        "CORE_STACK_BASE_URL": "http://127.0.0.1:8000",
        "CORE_STACK_API_KEY": "your-key"
      }
    }
  }
}
```

Reload MCP, then start a new chat and ask:

> Using the CoRE Stack tools, how many micro-watersheds are in Abu Road, Sirohi, Rajasthan, and what unit is area in?

The tools should report 107 rows and `area` in `ha`.

The same command and environment variables work in Claude Desktop under Settings → Developer → Edit Config.

## What a local trial showed

These are the things worth saying when you demo it.

The catalog is enough. The server never hardcodes the route list. `list_public_apis` returned the 10 dataset routes, and `describe_public_api` for `get_mws_data` showed `et`, `runoff`, and `precipitation` in millimetres, selected with `fields=`.

Tehsil units are column units. `describe_public_api` for `get_tehsil_data` and `property_name=drought` returned `no_drought` in weeks and `area` in hectares. Pass `property_name` for one sheet. Some sheets, especially `antyodaya`, have hundreds of columns, so the tool shows a sample unless you name the sheet.

Calling a real tehsil works, and the chat only sees the first row. `call_public_api` for Abu Road, `data=mws`, came back with 107 rows, the unit map, and the first watershed (`uid` `14_10589`, `area` about 2336 ha). The tool does this on purpose. The HTTP API still returns every row. Geometry responses are also shortened once they get large.

A bad sheet name is rejected from the catalog. `data=not_a_sheet` failed before the data call, and the error listed the real `data=` names.

An activated place can still have no file, and a listed sheet can be missing for that tehsil. `data=drought` for Abu Road returned success with no drought rows. The catalog describes sheets the platform can produce. A given tehsil file may not contain all of them. For this machine, Abu Road `mws` is a safe demo. Start from `get_active_locations` if you are not sure of the names.

A few units are still the word `value`. Identity columns such as `watershed_code` use that label. Some soil columns keep the unit in the column name (`n_mean_in_kg_per_ha`) because the API did not split it off. The MCP server shows whatever `tehsil_units` says.

The catalog itself needs the API key. `GET /.well-known/api-catalog` is public, but it only points at Swagger and ReDoc. The property list is `GET /api/v2/catalog/`, and that requires `X-API-Key`. This server sends the key on every request.
