# Core Stack GEE Ingestion

This is the standard ingestion path for large Core Stack GeoPackage and
GeoJSON vector assets.

The script writes a local tab-delimited table with one GeoJSON geometry column,
stages the same bytes to GCS with a `.csv` object suffix, then starts an Earth
Engine table-ingestion task from a manifest.

## Files

- Script: `utilities/scripts/gee/core_stack_gee_ingest.py`
- Config: `utilities/scripts/gee/core_stack_gee_assets.yaml`
- Service account: `data/gee_confs/core-stack-learn-818963fa8f26.json`
- Local outputs: `data/gee/core_stack/`
- Manifests: `data/gee/core_stack/manifests/`
- Log: `logs/core_stack_gee_ingest.log`

## Configured Assets

- `cs_admin_standard`
- `cs_village_facility_proximity`
- `cs_pan_india_facilities`
- `cs_village_livestock_census_20`
- `cs_antyodaya_2020`

`cs_pan_india_facilities` currently uses the existing source file
`data/facilities/outputs/pan_india_facilities.gpkg`; it is named with the `cs_`
asset key in the GEE config without making a duplicate 2 GB GeoPackage.

## Commands

Inspect configured sources:

```bash
uv run --with pyyaml --with shapely \
  python utilities/scripts/gee/core_stack_gee_ingest.py inspect --asset all
```

Smoke-build all configured assets:

```bash
uv run --with pyyaml --with shapely \
  python utilities/scripts/gee/core_stack_gee_ingest.py build \
  --asset all --limit 10 --jobs 2 --output-suffix .smoke \
  --overwrite --max-rss-mb 5000
```

Dry-run manifests for the smoke outputs:

```bash
uv run --with pyyaml --with shapely \
  python utilities/scripts/gee/core_stack_gee_ingest.py upload \
  --asset all --output-suffix .smoke --dry-run --jobs 2
```

Build full local ingestion tables:

```bash
nohup uv run --with pyyaml --with shapely \
  python utilities/scripts/gee/core_stack_gee_ingest.py build \
  --asset all --jobs 2 --overwrite --max-rss-mb 5000 \
  > logs/core_stack_gee_build_full_$(date -u +%Y%m%dT%H%M%SZ).log 2>&1 &
```

Start GEE ingestion after full tables are built:

```bash
nohup uv run --with earthengine-api --with google-cloud-storage --with pyyaml --with shapely \
  python utilities/scripts/gee/core_stack_gee_ingest.py upload \
  --asset all --jobs 2 --replace-existing \
  > logs/core_stack_gee_upload_$(date -u +%Y%m%dT%H%M%SZ).log 2>&1 &
```

Check task status later:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml \
  python utilities/scripts/gee/core_stack_gee_ingest.py status
```

The status command also writes `data/gee/core_stack/core_stack_gee_status.yaml`
by default.

Make existing ingested assets public after tasks succeed:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml \
  python utilities/scripts/gee/core_stack_gee_ingest.py make-public --asset all
```

Verify completed GEE assets and sample schemas:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml \
  python utilities/scripts/gee/core_stack_gee_ingest.py verify --asset all
```

## Notes

- The runtime memory guard defaults to `5000` MB and is checked at chunk
  boundaries.
- Special exporters accept either plain column names or `{source, target}`
  column mappings in YAML. The facilities point asset aliases raw
  `latitude`/`longitude` to `facility_latitude`/`facility_longitude` so Earth
  Engine keeps them as ordinary properties while still using the GeoJSON
  geometry column.
- For large GeoJSON FeatureCollections, install `ijson` or convert to GeoJSONL:
  `uv run --with ijson --with pyyaml --with shapely ...`.
- `upload --make-public` is best-effort and waits for ingestion to finish
  before applying ACLs. For long overnight uploads, submit tasks first, monitor
  them, then use `make-public` or make assets public manually in Earth Engine if
  IAM blocks the script.
- `verify` writes `data/gee/core_stack/core_stack_gee_verify_summary.yaml`.
