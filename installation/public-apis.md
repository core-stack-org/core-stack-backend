# Public APIs

This page is a one-stop path if you simply want to access and use CoRE Stack public data:

---

## 1. Generate An API Key

For public dataset access:

1. Register or sign in at [dashboard.core-stack.org](https://dashboard.core-stack.org/)
2. Generate an API key from the dashboard
3. Send it as the `X-API-Key` header on `public_api` routes

[Register or sign in at dashboard.core-stack.org](https://dashboard.core-stack.org/){ .md-button .md-button--primary }

If you need place names before you use the public routes, start with [GeoAdmin (NoAuth) APIs](geoadmin-noauth.md).

---

## 2. Inspect The Public Surface

Two direct tools are useful here:

- [Swagger](https://geoserver.core-stack.org/swagger/): better for trying requests quickly.
- [ReDoc](https://api-doc.core-stack.org/): better for reading grouped routes and schema details carefully.

---

## 3. Public API Endpoints

This is the practical pattern for moving from discovery to analysis with the public data APIs.

| Route | What it does | Source |
|------|---------------|--------|
| `GET /api/v1/get_admin_details_by_latlon/` | state, district, tehsil for a coordinate | [get_admin_details_by_lat_lon()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L43-L88) |
| `GET /api/v1/get_mwsid_by_latlon/` | MWS identifier for a coordinate | [get_mws_by_lat_lon()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L91-L131) |
| `GET /api/v1/get_tehsil_data/` | tehsil JSON derived from stats spreadsheets | [generate_tehsil_data()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L186-L235) |
| `GET /api/v1/get_mws_data/` | MWS time-series data | [get_mws_data()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L134-L183) |
| `GET /api/v1/get_mws_kyl_indicators/` | KYL indicator subset for one MWS | [get_mws_json_by_kyl_indicator()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L238-L296) |
| `GET /api/v1/get_generated_layer_urls/` | generated vector or raster layer URLs | [get_generated_layer_urls()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L299-L329) |
| `GET /api/v1/get_mws_report/` | report URL for one MWS | [get_mws_report_urls()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L343-L399) |
| `GET /api/v1/get_active_locations/` | activated location inventory | [generate_active_locations()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L404-L423) |
| `GET /api/v1/get_mws_geometries/` | MWS geometries via GeoServer | [get_mws_geometries()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L428-L455) |
| `GET /api/v1/get_village_geometries/` | village geometries via GeoServer | [get_village_geometries()](https://github.com/core-stack-org/core-stack-backend/blob/main/public_api/api.py#L460-L487) |

---

## 4. Recommended API Workflow

### Discover a real place

Use either:

- [GeoAdmin (NoAuth) APIs](geoadmin-noauth.md) if you need state or district names

<details>
  <summary>Reveal a quick location-finding route</summary>

Start with your state:

```bash
curl https://geoserver.core-stack.org/api/v1/get_states/
```

Once you find your state (eg: 29), start with:

```bash
curl https://geoserver.core-stack.org/api/v1/get_districts/29/
```

Using the district code in your state, follow with:

```bash
curl https://geoserver.core-stack.org/api/v1/get_blocks/566/
```

That lets you copy the exact district and block names before you try the computation route locally.

</details>

- `GET /api/v1/get_active_locations/` if you want only places currently active in the public surface

### Inventory the published layers

Call `GET /api/v1/get_generated_layer_urls/` and inspect:

- `dataset_name`
- `layer_type`
- `layer_url`
- `style_url`
- `gee_asset_path`

The output url provides several ways to download and utilise the data:

- direct download
- QGIS loading
- GeoServer-backed clients
- Earth Engine-oriented follow-up work

### Fetch stable geometries

Use:

- `GET /api/v1/get_mws_geometries/`
- `GET /api/v1/get_village_geometries/`

The main practical join key to watch for is `uid` on micro-watershed-aligned outputs.

### Fetch analytical tables

Use:

- `GET /api/v1/get_tehsil_data/` for tehsil-wide analytical tables keyed by watershed identifiers
- `GET /api/v1/get_mws_data/` for one watershed time series
- `GET /api/v1/get_mws_kyl_indicators/` for a compact watershed snapshot
- `GET /api/v1/get_mws_report/` for report handoff URLs

### Join by `uid`

That is the central reuse pattern:

```python
geometries = fetch_mws_geometries(...)
tehsil_data = fetch_tehsil_data(...)
metric_table = build_metric_table(tehsil_data)
joined_geojson = join_metrics_to_geojson(geometries, metric_table)
```

In other words:

1. choose a stable geometry layer
2. choose an analytical table
3. join through the watershed identifier
4. visualize, rank, or export

---

## 5. CLI Helper For Validation And Bulk Download

The repository also ships a local helper at [`installation/public_api_client.py`](/mnt/y/core-stack-org/backend-test-2/installation/public_api_client.py). It is useful when you want a guided workflow instead of composing each raw request yourself.

Key commands:

- `smoke-test`: minimal verification against `get_generated_layer_urls` and `get_mws_geometries`
- `locations`: inspect the activated state/district/tehsil hierarchy
- `resolve`: validate spellings and return canonical names or closest matches
- `download`: bulk-download tehsil, district, state, village, and MWS payloads

Helpful `download` patterns:

- `--state --district --tehsil`: one tehsil package
- `--state --district`: expand across all activated tehsils in that district
- `--state`: expand across all activated tehsils in that state
- `--latitude --longitude`: resolve the containing tehsil automatically

For district and state downloads, the helper now writes both:

- per-tehsil output folders, so each tehsil keeps its own raw public payloads
- root-level aggregated metadata, including selected tehsils, merged layer catalogs, and deduplicated MWS geometry indexes where applicable

Helpful `smoke-test` patterns:

- no location flags: use the built-in verified sample tehsil
- `--state --district --tehsil`: test exactly one tehsil
- `--state --district` or `--state`: pick the first activated tehsil in that scope and report which one was chosen
- broad-scope smoke tests stay intentionally lightweight; they validate one representative activated tehsil instead of downloading the whole district or state
- `--latitude --longitude`: resolve the containing tehsil automatically

Preferred dataset controls:

- `--datasets ...`: explicit dataset names
- `--bundle metadata`: catalogs and tehsil tables
- `--bundle layers`: layer files plus geometry helpers
- `--bundle watersheds`: MWS-focused outputs
- `--bundle full`: the default package

Examples:

```bash
# Minimal smoke test
python installation/public_api_client.py smoke-test

# Smoke test the first activated tehsil in a district
python installation/public_api_client.py smoke-test \
  --state assam \
  --district cachar

# Resolve misspelled names using activated locations
python installation/public_api_client.py resolve \
  --state bihar \
  --district jamu \
  --tehsil jami

# Download all available public artifacts for a tehsil
python installation/public_api_client.py download \
  --state assam \
  --district cachar \
  --tehsil lakhipur

# Download one district in bulk by expanding all activated tehsils
python installation/public_api_client.py download \
  --state assam \
  --district cachar \
  --bundle metadata

# Download one state in bulk, but cap the run to the first 5 tehsils
python installation/public_api_client.py download \
  --state assam \
  --bundle watersheds \
  --tehsil-limit 5

# Download only MWS-level payloads for one lat/lon lookup
python installation/public_api_client.py download \
  --latitude 24.7387057899787 \
  --longitude 86.30411868979151 \
  --streams point_lookup,mws_data,mws_kyl,mws_report
```

The helper uses `data/activated_locations/active_locations.json` by default and can refresh it from the live public API with `--refresh-active-locations`.
