# `gee_gen_utils.py`

`utilities/gee_gen_utils.py` is the CoRE Stack Backend utility for uploading vector datasets into Google Earth Engine (GEE) table assets in a consistent, reproducible, and backend-friendly way.

It is designed to solve a practical backend need:

- we often generate GeoJSON-based point layers inside the backend
- those layers need to be uploaded to GEE as table assets
- many datasets are large
- we want to avoid converting to shapefiles because shapefiles truncate field names
- we want one standard upload path that works for both single files and bulk directories

This utility provides that standard path.

## Why This Utility Matters

This is an important infrastructure component for CoRE Stack Backend because it standardizes how backend-generated vector layers move into Earth Engine.

Without a shared uploader, every dataset pipeline would need its own custom upload logic, auth handling, GCS staging, failure handling, and Earth Engine task polling. That creates duplication, inconsistent behavior, and fragile operational flows.

`gee_gen_utils.py` centralizes that work so that:

- backend-produced GeoJSON layers can be promoted into GEE assets reliably
- large files can be handled without loading everything into memory
- asset naming and folder creation follow a predictable convention
- upload status and ingestion failures are surfaced clearly
- batch uploads behave consistently across domains like facilities and future vector pipelines

In short, it is the backend bridge between local/generated vector data and Earth Engine-hosted assets.

## What It Supports

Supported input formats:

- `.geojson`
- `.json`
- `.geojsonl`
- `.jsonl`
- `.ndjson`

Supported usage patterns:

- upload a single vector file to a specific GEE asset
- upload all supported vector files from a directory into a GEE asset folder
- use a Django-managed `GEEAccount`
- use a direct service-account JSON file path
- optionally wait for Earth Engine ingestion completion
- optionally make the resulting asset public
- optionally delete the staged GCS object after upload

## Why We Do Not Use Shapefiles Here

This uploader intentionally works with GeoJSON-like formats and converts them into an Earth Engine-friendly CSV staging format internally.

That is important because shapefiles have field-name length restrictions and can silently damage backend schemas by truncating column names. For CoRE Stack datasets, preserving descriptive field names is often necessary for downstream use in analytics, APIs, and map layers.

## How It Works

The upload flow is:

1. Resolve credentials.
2. Initialize an Earth Engine session.
3. Validate or create the target Earth Engine asset folder.
4. Read the source vector file.
5. Convert the source into a temporary CSV where geometry is stored as GeoJSON text in a geometry column.
6. Upload that CSV to a GCS staging location.
7. Verify the staged object is readable.
8. Start Earth Engine table ingestion from the staged GCS file.
9. Optionally wait for the ingestion task to complete.
10. Optionally make the created asset public.
11. Optionally try to clean up the staged GCS object.

This staging-based flow is important because Earth Engine table uploads are most reliable when driven from GCS rather than ad hoc local conversions.

## Authentication Modes

The utility supports two auth models.

### 1. Django-backed auth

If you pass `--gee-account-id` or call the Python helpers with `gee_account_id=...`, the utility loads credentials from the Django `GEEAccount` model.

This mode is useful when the upload is being triggered as part of the application runtime and should align with the project’s stored GEE account configuration.

### 2. Direct service-account JSON

If you pass `--service-account-json /path/to/key.json`, the utility reads credentials directly from that file.

This mode keeps the uploader operationally independent and is the simplest approach for standalone data movement tasks.

## Large File Handling

Large GeoJSON and JSON files are handled differently from smaller files.

- files up to the small-file threshold are loaded normally
- files larger than the threshold are streamed using `ijson`

This matters for CoRE Stack because some facility layers are hundreds of megabytes and contain millions of features. Streaming avoids loading the entire FeatureCollection into memory at once.

Large-file notes:

- install `ijson` for large `.geojson` and `.json` uploads
- the utility normalizes streamed numeric values so that `Decimal` values do not break JSON serialization
- the conversion is intentionally done in streaming passes so wide or very large files can still be uploaded predictably

Install:

```bash
pip install ijson
```

## Schema and Geometry Preservation

The uploader preserves backend-friendly schemas by:

- reading all property names across features
- choosing a geometry column name that avoids collisions
- serializing nested JSON values safely
- preserving long field names that would be unsafe in shapefile-based workflows

This makes it especially suitable for point-layer outputs generated by scripts like [build_facilities_point_layer.py](/mnt/y/core-stack-org/core-stack-backend/data/facilities/build_facilities_point_layer.py).

## CLI Usage

The module is runnable directly:

```bash
python utilities/gee_gen_utils.py --help
```

You can also run it as a module:

```bash
python -m utilities.gee_gen_utils --help
```

### Upload a directory of vector files

```bash
python utilities/gee_gen_utils.py \
  --directory data/facilities/facilities_point_files \
  --asset-parent projects/corestack-datasets/assets/facilities \
  --service-account-json /abs/path/to/service-account.json \
  --replace-existing \
  --wait
```

### Upload a single file to a specific asset

```bash
python utilities/gee_gen_utils.py \
  --file data/facilities/facilities_point_files/advanced_health.geojson \
  --asset-id projects/corestack-datasets/assets/facilities/advanced_health \
  --service-account-json /abs/path/to/service-account.json \
  --replace-existing \
  --wait
```

### Use facilities defaults

```bash
python utilities/gee_gen_utils.py \
  --facilities-defaults \
  --service-account-json /abs/path/to/service-account.json \
  --replace-existing \
  --wait
```

## Python Usage

Single file:

```python
from utilities.gee_gen_utils import upload_vector_file_to_gee

result = upload_vector_file_to_gee(
    source_path="data/facilities/facilities_point_files/advanced_health.geojson",
    asset_parent="projects/corestack-datasets/assets/facilities",
    service_account_json_path="/abs/path/to/service-account.json",
    replace_existing=True,
    wait_for_completion=True,
)
```

Batch upload:

```python
from utilities.gee_gen_utils import upload_vector_files_to_gee

results = upload_vector_files_to_gee(
    input_dir="data/facilities/facilities_point_files",
    asset_parent="projects/corestack-datasets/assets/facilities",
    service_account_json_path="/abs/path/to/service-account.json",
    replace_existing=True,
    wait_for_completion=True,
)
```

Facilities shortcut:

```python
from utilities.gee_gen_utils import upload_facilities_point_files

results = upload_facilities_point_files(
    service_account_json_path="/abs/path/to/service-account.json",
    replace_existing=True,
    wait_for_completion=True,
)
```

## Important Operational Notes

### GCS permissions

The upload account needs enough access to the staging bucket to:

- upload the staged CSV object
- read the staged object back
- optionally delete it if `cleanup_gcs=True`

If delete permission is missing, the upload can still succeed. The utility now reports cleanup failure as a warning rather than treating the Earth Engine asset creation as a failed upload.

### Earth Engine permissions

The account also needs permission to create assets under the target asset parent, for example:

```text
projects/corestack-datasets/assets/facilities
```

### Public asset setting

`--make-public` only works when the active account is allowed to update the target asset ACL.

## Error Handling Philosophy

The utility is meant to fail clearly and helpfully.

Examples of guarded failure points include:

- missing input files
- unsupported vector formats
- invalid asset ids or asset parents
- missing service-account JSON files
- unreadable staged GCS objects
- Earth Engine ingestion failures
- optional GCS cleanup failures reported as warnings

For batch uploads, it can continue across files and report per-file status.

## Why It Is a Good Fit for CoRE Stack Backend

CoRE Stack Backend regularly creates geospatial outputs that need to move from Python data pipelines into Earth Engine for serving, analysis, and downstream map use.

`gee_gen_utils.py` is a strong fit for that environment because it is:

- reusable across domains, not tied only to facilities
- safe for large vector datasets
- schema-preserving
- compatible with both backend-managed and standalone auth flows
- isolated enough to be used as an operational utility without forcing changes across the rest of the repo

That combination makes it a foundational utility for standardized Earth Engine vector publishing in the backend.
