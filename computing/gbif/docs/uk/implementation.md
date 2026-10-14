# GBIF Biodiversity Integration — Block-First GEE Pipeline

> **Architecture decision (v2):** Block-first. The same code runs for one
> block or all of India — you iterate over blocks for national coverage.
> GEE is the primary computation engine. The local machine only orchestrates:
> download files, trigger GEE tasks, wait, export, sync GeoServer, generate
> reports. No GeoPandas for heavy spatial computation.
>
> **Who this is for:** a developer implementing this feature from scratch.
> Every stage includes exact function names, GEE patterns, failure cases,
> validation steps, and a table describing inputs → computation → outputs.
>
> **What changed from v1 (national-first):** See Section 2.

---

## Table of Contents

1. [How Point Datasets Differ from Raster Datasets in GEE](#1-how-point-datasets-differ-from-raster-datasets-in-gee)
2. [Block-First Architecture: Why and What Changed](#2-block-first-architecture-why-and-what-changed)
3. [Complete Data Flow Diagram](#3-complete-data-flow-diagram)
4. [Pipeline Sequence Diagram](#4-pipeline-sequence-diagram)
5. [Stage-by-Stage Implementation](#5-stage-by-stage-implementation)
   - [Stage 0 — Registry & Config](#stage-0--registry--config)
   - [Stage 1 — Block GBIF Download](#stage-1--block-gbif-download)
   - [Stage 2 — Data Cleaning](#stage-2--data-cleaning)
   - [Stage 3 — GCS Upload](#stage-3--gcs-upload)
   - [Stage 4 — GEE FeatureCollection Ingestion](#stage-4--gee-featurecollection-ingestion)
   - [Stage 4a — Pan-India Raster (Deferred to v3)](#stage-4a--pan-india-raster-deferred-to-v3)
   - [Stage 5 — Per-MWS Biodiversity Statistics (GEE)](#stage-5--per-mws-biodiversity-statistics-gee)
   - [Stage 6 — Post-Export Processing](#stage-6--post-export-processing)
   - [Stage 7 — GeoServer Synchronization](#stage-7--geoserver-synchronization)
   - [Stage 8 — Excel Sheet Generation](#stage-8--excel-sheet-generation)
   - [Stage 9 — KYL Filter Integration](#stage-9--kyl-filter-integration)
   - [Stage 10 — MWS Report Section](#stage-10--mws-report-section)
   - [Stage 11 — Tehsil Report Section](#stage-11--tehsil-report-section)
   - [New Model — GBIFBlockDownload](#new-model--gbifblockdownload)
   - [Celery Task — Complete Block Pipeline](#celery-task--complete-block-pipeline)
   - [API Endpoint](#api-endpoint)
   - [Management Command — Block Pipeline](#management-command--block-pipeline)
6. [GEE Computation Deep-Dive](#6-gee-computation-deep-dive)
7. [Directory Structure](#7-directory-structure)
8. [Implementation Roadmap](#8-implementation-roadmap)
9. [Complete Summary](#9-complete-summary)
10. [Data Flow Table](#10-data-flow-table)
11. [How I Would Explain This Pipeline to Another Developer](#11-how-i-would-explain-this-pipeline-to-another-developer)

---

## 1. How Point Datasets Differ from Raster Datasets in GEE

This section exists because GBIF breaks the standard CoRE Stack pipeline
in one critical place, and you must understand *why* before writing any code.

### The Standard CoRE Stack Pipeline (LULC, ERA5, Terrain, etc.)

```
GEE ImageCollection (raster already in GEE)
        ↓
reduceRegions(mws_fc, reducer, scale)
        ↓
FeatureCollection with per-MWS statistics
        ↓
Export to GCS as GeoJSON
        ↓
GeoServer → Excel → KYL → Reports
```

`reduceRegions()` is the workhorse. It takes an **Image**, a polygon
FeatureCollection, and a reducer (mean, sum, mode, etc.) and returns
per-polygon statistics. It works because every pixel already has a single
value, and reducers operate on pixel arrays.

### Why GBIF Cannot Use `reduceRegions()` Directly

`reduceRegions()` operates on **Images** — it reduces pixel values within
polygons. GBIF data is a **FeatureCollection of points**, not an Image.
You have two options, and only one is correct:

**Option A (Wrong): Convert points to Image first, then `reduceRegions()`.**

```python
image = gbif_fc.reduceToImage(['occurrence_count'], ee.Reducer.count())
per_mws = image.reduceRegions(mws_fc, ee.Reducer.sum(), scale=100)
```

This gives occurrence counts but **not species richness**. Once you paint
points to pixels, you lose species identity (taxonKey). Two records in the
same pixel from different species become indistinguishable. You can never
recover "how many distinct species" from a pixel with value 2.

**Option B (Correct): Spatial join FeatureCollection-to-FeatureCollection,
then `aggregate_count_distinct()`.**

```python
join = ee.Join.saveAll('occurrences')
filter = ee.Filter.intersects('.geo', None, '.geo')
mws_with_pts = join.apply(mws_fc, gbif_fc, filter)

def compute_stats(feature):
    pts = ee.FeatureCollection(ee.List(feature.get('occurrences')))
    return feature.set('species_richness', pts.aggregate_count_distinct('taxonKey'))

result = mws_with_pts.map(compute_stats)
```

`aggregate_count_distinct('taxonKey')` counts unique taxonKey values in
the sub-collection — exactly what "species richness" means. Fully server-side.

### What Changes vs. the Standard Pipeline

| Step | Standard Pipeline | GBIF Pipeline |
|---|---|---|
| Source data | GEE ImageCollection (raster) | GBIF HTTP API (points) |
| GEE object type during compute | `ee.Image` | `ee.FeatureCollection` |
| Per-MWS aggregation method | `reduceRegions()` | `ee.Join.saveAll()` + `aggregate_count_distinct()` |
| Richness computation | N/A | Server-side in GEE via join |
| First upload step | Data already in GEE | Must upload via GCS → CLI |

### What Remains Identical

- `ee_initialize(account_id)` — authentication unchanged
- `gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id)` — table upload (already calls `earthengine upload table`)
- `sync_layer_to_geoserver(state_name, fc, layer_name, workspace)` — vector publish unchanged
- `save_layer_info_to_db(...)` — DB registration unchanged
- `update_layer_sync_status(...)` — sync status unchanged
- `stats_generator/utils.py` — Excel generation pattern unchanged
- `stats_generator/mws_indicators.py` — KYL pattern unchanged
- Report generation pattern — unchanged

### Where GeoPandas Is Still Required (Two Cases Only)

**Case 1: CSV → GeoJSON conversion.**
GEE table ingestion requires GeoJSON or Shapefile. `gdf_to_ee_fc()` is NOT
used — it builds an in-memory FeatureCollection that fails for large datasets.
We write chunked GeoJSON to disk and upload via CLI. Even for a block
(typically 100–50,000 records) we use the same chunked approach for
consistency.

**Case 2: `dominant_taxon_group` computation (post-export, client-side).**
The dominant (mode) categorical value per MWS group is genuinely awkward in
GEE's Dictionary API. After export this runs on a tiny dataset (one block =
10–500 rows).

---

## 2. Block-First Architecture: Why and What Changed

### What "Block-First" Means

In the v1 national-first architecture:
- One national GBIF download covered all of India
- Per-block tasks queried a shared national GEE FeatureCollection asset
- You could not test a single block without first completing the national pipeline

In the v2 block-first architecture:
- Each block independently runs the **complete pipeline**: download → clean → GCS → GEE → compute → GeoServer
- The GEOMETRY predicate restricts the GBIF download to the block's bounding box
- A block typically returns 100–50,000 records (not 40–80 million)
- The same `generate_biodiversity_block` Celery task runs for 1 block or all of India

### Why Block-First Is Better

| Concern | National-First (v1) | Block-First (v2) |
|---|---|---|
| Development testing | Must run 6–12 hour national pipeline first | Test on 1 block in < 30 minutes |
| Failure recovery | One failure affects all blocks | Rerun the specific block that failed |
| GEE asset size | 20M+ records in one asset | 100–50K records per block |
| GEE join performance | `filterBounds()` + join on 20M points | Join on 100–50K points (100× faster) |
| Data freshness | Entire India re-download to update one block | Re-download only the affected block |
| Shared mutable state | National CSV / GEE asset is shared across workers | No shared state between blocks |
| Incremental rollout | All-or-nothing | One block at a time |

### What Specifically Changed from v1

| Component | v1 (National-First) | v2 (Block-First) | Why |
|---|---|---|---|
| GBIF download predicate | `COUNTRY=IN` | `GEOMETRY within block_bbox_wkt` | Download only the block's data |
| Download size | 40–80M records, 5–15 GB | 100–50K records, ~50 MB | Block is geographically small |
| GCS path | `gbif/{key}/india_clean.geojson` | `gbif/blocks/{state}/{district}/{block}/occurrences.geojson` | Block-scoped isolation |
| GEE asset | One national asset (`gbif/india_occurrences`) | Per-block asset (`get_gee_asset_path(state, district, block) + "gbif_occurrences"`) | Each block owns its asset |
| `filterBounds()` in mws_statistics.py | Required — filters 20M records to block extent | **Removed** — FC is already block-scoped | Eliminates the heavy filter step |
| DB model | `GBIFNationalDownload` (no block FK) | `GBIFBlockDownload` (state + district + block FKs) | Track per-block status |
| Celery task | `generate_biodiversity_block` depended on pre-completed national pipeline | `generate_biodiversity_block` runs the **entire pipeline** for one block | Self-contained |
| Management command | `generate_gbif_national` (mandatory prerequisite) | `generate_gbif_block` (direct block run) | Can run a single block |
| Pan-India raster (Stage 4a) | Generated from national FC | **Deferred to v3** — no national FC in v2 | Not needed for MWS stats |

---

## 3. Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│       BLOCK PIPELINE  (one Celery task per block, runs independently)        │
└─────────────────────────────────────────────────────────────────────────────┘

INPUT: state="karnataka", district="ramanagara", block="channapatna"
       gee_account_id=<int>

[Local] GeoServer WFS
  ↓  GET /mws_layers/ows?typeName=mws_layers:filtered_mws_{district}_{block}_uid
  ↓  response.json()["features"] → extract bbox
  ↓
Block bounding box as WKT POLYGON string
  "POLYGON((77.1 12.7, 77.6 12.7, 77.6 13.2, 77.1 13.2, 77.1 12.7))"
  ↓  pygbif occ.download() — GEOMETRY predicate
  ↓  Predicate: geometry WITHIN block_bbox_wkt,
  ↓             hasCoordinate=True, hasGeospatialIssue=False,
  ↓             occurrenceStatus=PRESENT, basisOfRecord in [...]
  ↓  [async: 5–15 minutes for a typical block]
  ↓
ZIP archive  (tab-separated SIMPLE_CSV, typically 50 KB – 10 MB)
  ↓  [unzip → pandas read_csv(sep="\t")]
  ↓
Pandas DataFrame  (raw)
  Columns: gbifID, taxonKey, species, kingdom, class, decimalLatitude,
           decimalLongitude, coordinateUncertaintyInMeters, stateProvince
  ↓  [5 cleaning filters: dropna → bbox → uncertainty → pile → dedup]
  ↓
Pandas DataFrame  (clean, typically 70–90% of raw survives for block-level)
  ↓  [chunked GeoJSON write to disk]
  ↓
GeoJSON file on local disk
  gbif_{district}_{block}.geojson
  Each row = GeoJSON Feature: geometry=Point, properties={gbifID, taxonKey,
  species, kingdom, class, stateProvince}
  ↓  [upload_geojson_to_gcs()]
  ↓
GCS Blob
  gs://<bucket>/gbif/blocks/{state}/{district}/{block}/occurrences.geojson
  ↓  [gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id)]
  ↓  [check_task_status() — polls until SUCCEEDED; ~5–15 minutes]
  ↓
GEE FeatureCollection Asset
  get_gee_asset_path(state, district, block) + "gbif_occurrences"
  e.g.: "projects/<project>/assets/karnataka/ramanagara/channapatna/gbif_occurrences"
  Properties per Feature: gbifID (str), taxonKey (str), species (str),
                          kingdom (str), class (str), stateProvince (str)
  Geometry: ee.Geometry.Point([lon, lat])
  ↓
  ↓  [ee_initialize(gee_account_id)]
  ↓  gbif_fc = ee.FeatureCollection(asset_id)     ← no filterBounds needed
  ↓  mws_fc  = load_mws_featurecollection(district, block)
  ↓
  ↓  [ee.Join.saveAll('gbif_occurrences')]
  ↓  [ee.Filter.intersects('.geo', None, '.geo', maxError=10)]
  ↓  [join.apply(primary=mws_fc, secondary=gbif_fc)]
  ↓
GEE FeatureCollection  (each MWS feature carries list of contained GBIF points)
  ↓  [.map(compute_mws_stats)]
  ↓  [aggregate_count_distinct('taxonKey') → species_richness]
  ↓  [.size()                              → occurrence_count]
  ↓  [aggregate_histogram → List ops      → shannon_diversity_index]
  ↓  [.lt(20)                             → data_poor flag]
  ↓  [merge back MWS with 0 occurrences (data_poor=True, all zeros)]
  ↓
GEE FeatureCollection  (per-MWS stats, geometry retained)
  Properties: uid, species_richness, occurrence_count,
              shannon_diversity_index, data_poor
  ↓  [ee.batch.Export.table.toCloudStorage(
  ↓       collection, bucket,
  ↓       fileNamePrefix="gbif/stats/{district}_{block}_biodiversity",
  ↓       fileFormat='GeoJSON')]
  ↓  [check_task_status() — ~1–3 minutes]
  ↓
GCS Blob
  gs://<bucket>/gbif/stats/{district}_{block}_biodiversity.geojson
  ↓  [download from GCS → 3-line pandas dominant_taxon_group computation]
  ↓  [prepare_geojson_for_geoserver() — fill all NaN]
  ↓
Python dict  (GeoJSON in memory, all columns clean)
  ↓  [sync_layer_to_geoserver(state, fc_dict, layer_name, "biodiversity")]
  ↓  [save_layer_info_to_db(...)]
  ↓  [update_layer_sync_status(layer_id, sync_to_geoserver=True)]
  ↓
GeoServer Vector Layer: biodiversity:{district}_{block}_biodiversity
  WFS-queryable, one row per MWS
  Columns: uid, species_richness, occurrence_count,
           shannon_diversity_index, dominant_taxon_group, data_poor
  ↓  [stats_generator/utils.py — workspace=="biodiversity" branch]
  ↓
Excel sheet "biodiversity" in {district}_{block}.xlsx
  ↓  [mws_indicators.py — sheets["biodiversity"] = xl.parse("biodiversity")]
  ↓  [per-MWS loop: extract 5 values → append to results dict]
  ↓
KYL JSON  (5 new keys per MWS entry)
  {"species_richness": 47, "occurrence_count": 312,
   "shannon_diversity_index": 3.21, "dominant_taxon_group": "Aves",
   "biodiversity_data_poor": false}
  ↓  [dpr/gen_mws_report.py + dpr/gen_tehsil_report.py]
  ↓
HTML Reports  (biodiversity section)
```

---

## 4. Pipeline Sequence Diagram

```
                BLOCK PIPELINE
                (one Celery task, complete pipeline per block)

POST /computing/generate_biodiversity_layer/
     {"state": "karnataka", "district": "ramanagara",
      "block": "channapatna", "gee_account_id": 3}
         │
         ▼
Celery: generate_biodiversity_block.apply_async(
            [state, district, block, gee_account_id], queue="nrm")
         │
         ▼
[Idempotency check]
Layer already synced? → return early
         │
         ▼
[Stage 1] download.py
get_block_bbox_wkt(district, block)   → block_bbox_wkt  (from GeoServer WFS)
request_block_download(block_bbox_wkt) → download_key
wait_and_fetch(download_key, dest_dir) → raw_csv, doi, raw_count
GBIFBlockDownload.objects.create(...)  → record
         │
         │  gbif_{district}_{block}_raw.csv  (tab-sep, 50KB–10MB)
         ▼
[Stage 2] clean.py
clean_occurrences(raw_csv, clean_csv) → stats dict
record.clean_record_count = stats["final"]; record.save()
         │
         │  gbif_{district}_{block}_clean.csv  (comma-sep)
         ▼
[Stage 3] gee_upload.py
csv_to_geojson(clean_csv, geojson_path)
upload_geojson_to_gcs(geojson_path, gcs_blob)  → gcs_uri
record.gcs_geojson_uri = gcs_uri; record.save()
         │
         │  GCS: gbif/blocks/{state}/{district}/{block}/occurrences.geojson
         ▼
[Stage 4] gee_upload.py (ingestion)
asset_id = get_gee_block_asset_id(state, district, block)
task_id  = ingest_geojson_to_gee(gcs_uri, asset_id, gee_account_id)
record.gee_ingest_task_id = task_id; record.save()
wait_for_gee_ingestion(task_id)          [~5–15 min]
record.gee_asset_id = asset_id; record.save()
         │
         │  GEE FeatureCollection asset ready
         ▼
[Stage 5] mws_statistics.py
ee_initialize(gee_account_id)
gbif_fc  = ee.FeatureCollection(asset_id)   ← NO filterBounds
mws_fc   = load_mws_featurecollection(district, block)
stats_fc = compute_mws_biodiversity(gbif_fc, mws_fc)
export_task_id = export_stats_to_gcs(stats_fc, district, block)
record.gee_export_task_id = export_task_id; record.save()
check_task_status([export_task_id])      [~1–3 min]
         │
         │  GCS: gbif/stats/{district}_{block}_biodiversity.geojson
         ▼
[Stage 6] export.py
download_stats_geojson(district, block)   → raw GeoJSON dict
add_dominant_taxon_group(geojson)         → enriched dict
prepare_geojson_for_geoserver(geojson)    → clean dict
         │
         ▼
[Stage 7] sync.py
sync_layer_to_geoserver(state, fc_dict, layer_name, "biodiversity")
save_layer_info_to_db(state, district, block, layer_name, ...)
update_layer_sync_status(layer_id, sync_to_geoserver=True)
record.status = "READY"; record.save()
         │
         │  GeoServer: biodiversity:{district}_{block}_biodiversity
         ▼
[Stages 8–11 — separate triggers, existing endpoints]
stats_generator/utils.py  → biodiversity sheet in {district}_{block}.xlsx
mws_indicators.py         → 5 new keys per MWS in KYL JSON
gen_mws_report.py         → biodiversity section in MWS HTML report
gen_tehsil_report.py      → biodiversity section in block HTML report
```

---

## 5. Stage-by-Stage Implementation

---

### Stage 0 — Registry & Config

**Goal:** All required infrastructure (DB rows, GeoServer workspace,
env vars) exists before any algorithm code runs.

**Why:** `save_layer_info_to_db()` calls `Dataset.objects.get(name=...)` and
raises `Dataset.DoesNotExist` if the row is missing.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 0 | Nothing | — | Dataset DB rows, GeoServer workspace, env vars | One-time manual setup | Local shell | Django shell confirmation + GeoServer workspace visible |

**New file — `computing/biodiversity/config.py`:**

```python
import os
from utilities.gee_utils import get_gee_asset_path

# ── GBIF download ─────────────────────────────────────────────────────────────
BASIS_OF_RECORD_TYPES = [
    "HUMAN_OBSERVATION",
    "PRESERVED_SPECIMEN",
    "MACHINE_OBSERVATION",
    "OBSERVATION",
]

# ── Coordinate cleaning ───────────────────────────────────────────────────────
MAX_COORDINATE_UNCERTAINTY_M = 10_000   # drop records with uncertainty > 10 km
PILE_COORDINATE_THRESHOLD    = 1_000    # drop exact coordinates with > 1000 records

# ── Per-MWS statistics ────────────────────────────────────────────────────────
DATA_POOR_THRESHOLD = 20                # MWS with < 20 occurrences = data poor

# ── GEE asset paths (block-scoped) ────────────────────────────────────────────
def get_gee_block_asset_id(state: str, district: str, block: str) -> str:
    """
    Return the GEE asset ID for a block's GBIF FeatureCollection.
    Follows get_gee_asset_path() convention from gee_utils.
    e.g.: "projects/<project>/assets/karnataka/ramanagara/channapatna/gbif_occurrences"
    """
    return get_gee_asset_path(state, district, block) + "gbif_occurrences"

# ── GCS paths (block-scoped) ──────────────────────────────────────────────────
GCS_BLOCK_GEOJSON  = "gbif/blocks/{state}/{district}/{block}/occurrences.geojson"
GCS_STATS_PREFIX   = "gbif/stats/{district}_{block}_biodiversity"

# ── GeoServer ─────────────────────────────────────────────────────────────────
WORKSPACE           = "biodiversity"
DATASET_NAME_VECTOR = "Biodiversity Occurrence"
VECTOR_STYLE_NAME   = "biodiversity_mws"
ALGORITHM_NAME      = "GBIF_GEE_BLOCK_JOIN"
ALGORITHM_VERSION   = "2.0"
```

**Why no `GEE_GBIF_TABLE_ASSET` constant:** v1 used a single national asset
path stored as a constant. v2 generates per-block asset paths using
`get_gee_block_asset_id(state, district, block)` which delegates to the
existing `get_gee_asset_path()` convention. This keeps all asset paths
consistent across the entire CoRE Stack.

**`installation/seed/seed_data.json`** — add one Dataset row:

```json
{
  "model": "computing.dataset",
  "pk": null,
  "fields": {
    "name": "Biodiversity Occurrence",
    "layer_type": "vector",
    "workspace": "biodiversity",
    "style_name": "biodiversity_mws",
    "misc": null
  }
}
```

**`.env` / `.env.example`** — add:

```bash
GBIF_USER="your_gbif_username"
GBIF_PWD="your_gbif_password"
GBIF_EMAIL="you@org.org"
```

**`requirements.txt`** — add:

```
pygbif>=0.6.3
```

**GeoServer setup** (run once):

```python
from utilities.geoserver_utils import Geoserver
geo = Geoserver()
geo.create_workspace("biodiversity")
# Upload biodiversity_mws.sld via GeoServer REST
```

**Validation:**

```python
from computing.models import Dataset
Dataset.objects.get(name="Biodiversity Occurrence")  # must not raise
```

---

### Stage 1 — Block GBIF Download

**Goal:** Download all GBIF occurrence records that fall within this block's
bounding box. Each block downloads only its own data.

**What changed from v1:** v1 used `COUNTRY=IN` and downloaded all of India.
v2 uses a `GEOMETRY within block_bbox_wkt` predicate. The block boundary WKT
is derived from the MWS layer already in GeoServer — no hardcoded coordinates.

**Why the Download API (not the search API):** The GBIF search endpoint is
capped at 100,000 records. The Download API is asynchronous, has no cap, and
produces a citable DOI. Even for a small block this keeps the code consistent.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 1 | `district`, `block` (strings) | MWS GeoJSON from GeoServer WFS → block bbox WKT | Raw GBIF CSV (`gbif_{district}_{block}_raw.csv`), `download_key`, `doi`, `raw_count` | pygbif async HTTP + GeoServer WFS call | Local machine | Tab-separated CSV, GBIF SIMPLE_CSV format |

**New file — `computing/biodiversity/download.py`:**

```python
import os, time, zipfile, logging
from pygbif import occurrences as occ
import requests
from utilities.constants import GEOSERVER_URL
from .config import BASIS_OF_RECORD_TYPES

logger = logging.getLogger(__name__)

MAX_WAIT_HOURS = 2   # blocks are small; typically done in 5–20 minutes


def get_block_bbox_wkt(district: str, block: str) -> str:
    """
    Fetch the MWS layer for this block from GeoServer WFS and compute
    the overall bounding box as a WKT POLYGON string.

    The WKT is passed directly to pygbif as the GEOMETRY predicate value,
    which restricts the GBIF download to records within the block extent.

    Why bbox (not exact MWS union): GBIF's GEOMETRY predicate accepts a
    simple polygon. Computing the exact MWS union would produce a complex
    MultiPolygon; the bbox is sufficient since GEE's spatial join will do
    the precise per-MWS assignment later.
    """
    layer_name = f"filtered_mws_{district}_{block}_uid"
    url = (
        f"{GEOSERVER_URL}/mws_layers/ows?"
        f"service=WFS&version=1.0.0&request=GetFeature"
        f"&typeName=mws_layers:{layer_name}&outputFormat=application/json"
    )
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    geojson = response.json()

    if not geojson.get("features"):
        raise ValueError(
            f"No MWS features for {district}/{block}. "
            "Has the MWS layer been generated for this block?"
        )

    # Compute bounding box from all feature coordinates
    all_lons, all_lats = [], []
    for feature in geojson["features"]:
        geom = feature.get("geometry", {})
        coords = _flatten_coordinates(geom.get("coordinates", []))
        for lon, lat in coords:
            all_lons.append(lon)
            all_lats.append(lat)

    minlon, maxlon = min(all_lons), max(all_lons)
    minlat, maxlat = min(all_lats), max(all_lats)

    # Add a small buffer (~1 km) to avoid edge clipping
    buf = 0.01
    wkt = (
        f"POLYGON(("
        f"{minlon - buf} {minlat - buf},"
        f"{maxlon + buf} {minlat - buf},"
        f"{maxlon + buf} {maxlat + buf},"
        f"{minlon - buf} {maxlat + buf},"
        f"{minlon - buf} {minlat - buf}"
        f"))"
    )
    logger.info(f"Block bbox WKT for {district}/{block}: {wkt}")
    return wkt


def _flatten_coordinates(coords):
    """Recursively flatten nested coordinate arrays to list of (lon, lat) pairs."""
    if not coords:
        return []
    if isinstance(coords[0], (int, float)):
        return [(coords[0], coords[1])]
    result = []
    for item in coords:
        result.extend(_flatten_coordinates(item))
    return result


def request_block_download(block_bbox_wkt: str) -> str:
    """
    Submit an async GBIF download for occurrences within a block's bbox.

    pygbif predicate syntax: 3-tuples ("FIELD", "OPERATOR", "VALUE").
    The GEOMETRY predicate restricts records to within the WKT polygon.

    NOTE: Do NOT use free-form strings like "COUNTRY = IN". pygbif ignores
    them silently. Always use the 3-tuple form.
    """
    predicates = [
        ("hasCoordinate",       "=", "True"),
        ("hasGeospatialIssue",  "=", "False"),
        ("occurrenceStatus",    "=", "PRESENT"),
        ("geometry",            "within", block_bbox_wkt),
    ]
    basis_predicates = [("basisOfRecord", "=", b) for b in BASIS_OF_RECORD_TYPES]

    result = occ.download(
        [predicates, basis_predicates],
        user=os.environ["GBIF_USER"],
        pwd=os.environ["GBIF_PWD"],
        email=os.environ["GBIF_EMAIL"],
        pred_type="and",
    )
    download_key = result[0] if isinstance(result, (list, tuple)) else result
    logger.info(f"GBIF block download requested. Key: {download_key}")
    return str(download_key)


def wait_and_fetch(download_key: str, dest_dir: str,
                   max_wait_hours: int = MAX_WAIT_HOURS):
    """
    Poll GBIF until SUCCEEDED, then fetch and unzip.
    Returns (csv_path, doi, record_count).
    Raises RuntimeError on failure or timeout.
    """
    max_polls = (max_wait_hours * 3600) // 60
    for attempt in range(max_polls):
        meta   = occ.download_meta(download_key)
        status = meta["status"]
        logger.info(f"GBIF {download_key}: {status} (attempt {attempt + 1})")
        if status == "SUCCEEDED":
            break
        if status in ("KILLED", "CANCELLED", "FAILED"):
            raise RuntimeError(f"GBIF download {download_key} ended as {status}")
        time.sleep(60)
    else:
        raise RuntimeError(
            f"GBIF download {download_key} did not complete in {max_wait_hours}h"
        )

    os.makedirs(dest_dir, exist_ok=True)
    occ.download_get(download_key, path=dest_dir)
    zip_path = os.path.join(dest_dir, f"{download_key}.zip")

    with zipfile.ZipFile(zip_path, "r") as z:
        bad = z.testzip()
        if bad:
            raise RuntimeError(f"Corrupt zip: first bad file = {bad}")
        z.extractall(os.path.join(dest_dir, download_key))

    csv_path     = os.path.join(dest_dir, download_key, f"{download_key}.csv")
    doi          = meta.get("doi", f"10.15468/dl.{download_key}")
    record_count = meta.get("totalRecords", -1)
    logger.info(f"Download complete. Records: {record_count:,}. DOI: {doi}")
    return csv_path, doi, record_count
```

**Testing shortcut:** For development, add `("taxonKey", "=", "212")` (Aves)
to get a faster, smaller download. Remove before production.

**Failure cases:**
- `GBIF_USER` not in env → `KeyError`. Add an explicit env check at the top
  of the Celery task before calling this function.
- Download returns `KILLED` (rare for block-sized requests, common for national) → Retry.
- Block has zero records → `meta["totalRecords"] == 0`. GeoJSON will be
  empty. The pipeline proceeds normally; all MWS in the block will be
  `data_poor=True` with `species_richness=0`.

---

### Stage 2 — Data Cleaning

**Goal:** Remove records with incorrect, imprecise, or misleading coordinates.
Deduplicate. Output a clean CSV ready for GEE upload.

**What changed from v1:** Unchanged. The same 5 filters apply regardless of
whether the input is an India-wide or block-level CSV.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 2 | Raw tab-sep CSV from Stage 1 (100–50K rows for a block) | Pandas DataFrame in memory | `gbif_{district}_{block}_clean.csv` (comma-sep), stats dict | Pandas: dropna → bbox → uncertainty → pile filter → dedup | Local machine | Comma-separated CSV, 8 columns |

**New file — `computing/biodiversity/clean.py`:**

```python
import pandas as pd, logging
from .config import MAX_COORDINATE_UNCERTAINTY_M, PILE_COORDINATE_THRESHOLD

logger = logging.getLogger(__name__)

INDIA_BBOX = (68.0, 6.5, 97.5, 37.6)   # (minlon, minlat, maxlon, maxlat)

REQUIRED_COLS = [
    "gbifID", "taxonKey", "species", "kingdom", "class",
    "decimalLatitude", "decimalLongitude",
    "coordinateUncertaintyInMeters", "basisOfRecord", "stateProvince",
]
OUTPUT_COLS = [
    "gbifID", "taxonKey", "species", "kingdom", "class",
    "decimalLatitude", "decimalLongitude", "stateProvince",
]


def clean_occurrences(raw_csv: str, out_csv: str) -> dict:
    """
    Apply 5 coordinate cleaning filters to the raw GBIF CSV.
    Returns a stats dict with before/after counts per filter.
    """
    df = pd.read_csv(
        raw_csv, sep="\t", on_bad_lines="skip",
        usecols=REQUIRED_COLS,
        dtype={"gbifID": str, "taxonKey": str},
        low_memory=False,
    )
    stats = {"raw": len(df)}

    # 1. Must have species name, taxonKey, and coordinates
    df = df.dropna(subset=["species", "taxonKey", "decimalLatitude", "decimalLongitude"])
    stats["after_dropna"] = len(df)

    # 2. Coordinates must be within India bounding box and not (0, 0)
    minlon, minlat, maxlon, maxlat = INDIA_BBOX
    df = df[
        df.decimalLatitude.between(minlat, maxlat) &
        df.decimalLongitude.between(minlon, maxlon)
    ]
    df = df[~((df.decimalLatitude == 0) & (df.decimalLongitude == 0))]
    stats["after_bbox"] = len(df)

    # 3. Coordinate uncertainty ≤ 10 km (records without uncertainty pass)
    unc = df["coordinateUncertaintyInMeters"]
    df  = df[unc.isna() | (unc <= MAX_COORDINATE_UNCERTAINTY_M)]
    stats["after_uncertainty"] = len(df)

    # 4. Drop pile coordinates (institutional/centroid snapping)
    coord_counts = df.groupby(
        ["decimalLatitude", "decimalLongitude"]
    ).size().rename("coord_freq")
    df = df.join(coord_counts, on=["decimalLatitude", "decimalLongitude"])
    df = df[df["coord_freq"] <= PILE_COORDINATE_THRESHOLD].drop(columns=["coord_freq"])
    stats["after_pile_filter"] = len(df)

    # 5. Deduplicate: same taxonKey at exact same coordinate is one record
    df = df.drop_duplicates(subset=["taxonKey", "decimalLatitude", "decimalLongitude"])
    stats["after_dedup"] = len(df)

    df[OUTPUT_COLS].to_csv(out_csv, index=False)

    stats["final"]         = len(df)
    stats["drop_rate_pct"] = round((1 - stats["final"] / stats["raw"]) * 100, 1) if stats["raw"] > 0 else 0
    logger.info(
        f"Clean: {stats['final']:,} records remaining ({stats['drop_rate_pct']}% dropped)"
    )
    return stats
```

**Validation:** `drop_rate_pct` for a block-level download should be 10–50%.
Values outside this range (e.g. 0% or 95%) indicate a filter issue.

---

### Stage 3 — GCS Upload

**Goal:** Convert the clean CSV to GeoJSON on disk, upload to Google Cloud
Storage. GEE table ingestion reads from GCS — it cannot read from local disk.

**What changed from v1:** GCS path is now block-scoped:
`gbif/blocks/{state}/{district}/{block}/occurrences.geojson`
instead of `gbif/{download_key}/india_clean.geojson`.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 3 | Clean CSV from Stage 2 | GeoJSON file on disk | GCS blob `gbif/blocks/{state}/{district}/{block}/occurrences.geojson` | Pandas chunked read → JSON write → GCS upload | Local machine | GeoJSON FeatureCollection file |

**New file — `computing/biodiversity/gee_upload.py`:**

```python
import os, json, logging
import pandas as pd
from utilities.gee_utils import (
    gcs_to_gee_asset_cli, check_task_status, is_gee_asset_exists,
)
from .config import get_gee_block_asset_id, GCS_BLOCK_GEOJSON

logger = logging.getLogger(__name__)


def _get_gcs_bucket():
    from utilities.gee_utils import gcs_config
    return gcs_config()


def csv_to_geojson(clean_csv: str, geojson_path: str) -> str:
    """
    Convert clean CSV to GeoJSON using chunked reading.

    NOTE: gdf_to_ee_fc() from gee_utils.py is NOT used here.
    That helper builds an in-memory FeatureCollection — unreliable for large
    inputs. We write to disk and upload via CLI for consistent behaviour
    regardless of record count.
    """
    logger.info(f"Converting {clean_csv} → {geojson_path}")
    with open(geojson_path, "w") as f:
        f.write('{"type":"FeatureCollection","features":[\n')
        first = True
        for chunk in pd.read_csv(clean_csv, chunksize=100_000,
                                 dtype={"gbifID": str, "taxonKey": str}):
            for _, row in chunk.iterrows():
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            float(row["decimalLongitude"]),
                            float(row["decimalLatitude"]),
                        ],
                    },
                    "properties": {
                        "gbifID":        str(row.get("gbifID", "")),
                        "taxonKey":      str(row.get("taxonKey", "")),
                        "species":       str(row.get("species", "")),
                        "kingdom":       str(row.get("kingdom", "")),
                        "class":         str(row.get("class", "")),
                        "stateProvince": str(row.get("stateProvince", "")),
                    },
                }
                if not first:
                    f.write(",\n")
                f.write(json.dumps(feature))
                first = False
        f.write("\n]}")
    size_mb = os.path.getsize(geojson_path) / (1024 * 1024)
    logger.info(f"GeoJSON written: {size_mb:.1f} MB")
    return geojson_path


def upload_geojson_to_gcs(
    geojson_path: str, state: str, district: str, block: str,
) -> str:
    """Upload GeoJSON to block-scoped GCS path. Returns gs:// URI."""
    gcs_blob_name = GCS_BLOCK_GEOJSON.format(
        state=state, district=district, block=block
    )
    bucket = _get_gcs_bucket()
    blob   = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(geojson_path, content_type="application/json")
    gcs_uri = f"gs://{bucket.name}/{gcs_blob_name}"
    logger.info(f"Uploaded to: {gcs_uri}")
    return gcs_uri


def ingest_geojson_to_gee(
    gcs_uri: str, state: str, district: str, block: str,
    gee_account_id: int,
) -> str | None:
    """
    Ingest GeoJSON from GCS into GEE as a block-scoped FeatureCollection asset.
    Reuses gcs_to_gee_asset_cli() which already calls 'earthengine upload table'.
    Returns task_id, or None if asset already exists.
    """
    asset_id = get_gee_block_asset_id(state, district, block)
    if is_gee_asset_exists(asset_id):
        logger.info(f"GEE asset already exists: {asset_id}. Skipping ingestion.")
        return None
    task_id = gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id)
    logger.info(f"GEE table ingestion task: {task_id} → {asset_id}")
    return task_id


def wait_for_gee_ingestion(task_id: str | None) -> None:
    """Poll until the GEE table ingestion task completes."""
    if task_id is None:
        return
    check_task_status([task_id])
    logger.info(f"GEE ingestion complete: {task_id}")
```

**Failure cases:**
- GCS upload interrupted → verify with `blob.exists()` + `blob.size` after upload.
- GEE ingestion fails → check GEE Code Editor "Tasks" tab. Common: malformed
  GeoJSON, property names with spaces, file too large (unlikely for a block).
- Asset already exists → `is_gee_asset_exists()` returns True, skipped.
  Delete the asset manually in GEE Code Editor to force reimport.

**Validation:**

```python
import ee
asset_id = get_gee_block_asset_id("karnataka", "ramanagara", "channapatna")
fc = ee.FeatureCollection(asset_id)
print(fc.size().getInfo())          # > 0 for a populated block
print(fc.first().propertyNames().getInfo())
# → ['gbifID', 'taxonKey', 'species', 'kingdom', 'class', 'stateProvince']
```

---

### Stage 4 — GEE FeatureCollection Ingestion

This is not a code stage — it is the GEE object type that Stage 5 operates on.

**GEE object type:** `ee.FeatureCollection`

**How it is loaded in Stage 5:**

```python
asset_id = get_gee_block_asset_id(state, district, block)
gbif_fc  = ee.FeatureCollection(asset_id)
```

**Why no `.filterBounds()` call:** In v1, the national FC had 20M+ records and
required `filterBounds(block_geom)` before the spatial join to avoid joining
20M points against every MWS. In v2, the FC contains only the block's records
(100–50K). There is nothing to filter. Removing `filterBounds()` eliminates
a GEE computation step and simplifies the code.

**Properties per Feature:**

| Property | Type | Notes |
|---|---|---|
| `gbifID` | String | GBIF record identifier |
| `taxonKey` | String | Species identifier — use for distinct counts |
| `species` | String | Human-readable species name |
| `kingdom` | String | e.g., "Animalia", "Plantae" |
| `class` | String | e.g., "Aves", "Insecta", "Mammalia" |
| `stateProvince` | String | Indian state name |
| geometry | `ee.Geometry.Point` | [lon, lat] |

---

### Stage 4a — Pan-India Raster (Deferred to v3)

**Why deferred:** The pan-India raster requires a national GBIF FeatureCollection
asset (all of India in one GEE asset). In the block-first architecture, each
block has its own GEE asset. Merging all block assets into a national view
is a v3 concern.

The raster is a visualization layer only — it has no effect on KYL filters
or report generation. Deferring it does not block any downstream pipeline
stages.

**Placeholder in config.py:**
```python
# Pan-India raster: deferred to v3
# See raster_generation.py when ready. Requires merging block GEE assets.
```

---

### Stage 5 — Per-MWS Biodiversity Statistics (GEE)

**Goal:** For each MWS polygon in a block, compute `species_richness`,
`occurrence_count`, `shannon_diversity_index`, and `data_poor` flag.
Export per-MWS stats to GCS as GeoJSON.

**What changed from v1:** Removed `filterBounds()`. The block-scoped
FeatureCollection is already restricted to the block extent. All other
computation is identical.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 5 | GEE asset from Stage 4 (block GBIF FC) + MWS polygons from GeoServer WFS | GEE FeatureCollection with joined occurrences per MWS | GCS GeoJSON `gbif/stats/{district}_{block}_biodiversity.geojson` | `ee.Join.saveAll()` + `aggregate_count_distinct('taxonKey')` + Shannon histogram | GEE (server-side) | GeoJSON FeatureCollection, one feature per MWS |

**New file — `computing/biodiversity/mws_statistics.py`:**

```python
import ee, json, requests, logging
from utilities.gee_utils import check_task_status
from utilities.constants import GEOSERVER_URL
from .config import get_gee_block_asset_id, DATA_POOR_THRESHOLD, GCS_STATS_PREFIX

logger     = logging.getLogger(__name__)
GCS_BUCKET = "<your-gcs-bucket>"


def load_mws_featurecollection(district: str, block: str) -> ee.FeatureCollection:
    """
    Fetch MWS polygons for this block from GeoServer WFS.
    Converts to ee.FeatureCollection for GEE computation.
    """
    layer_name = f"filtered_mws_{district}_{block}_uid"
    url = (
        f"{GEOSERVER_URL}/mws_layers/ows?"
        f"service=WFS&version=1.0.0&request=GetFeature"
        f"&typeName=mws_layers:{layer_name}&outputFormat=application/json"
    )
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    geojson = response.json()

    if not geojson.get("features"):
        raise ValueError(
            f"No MWS features for {district}/{block}. "
            "Has the MWS layer been generated for this block?"
        )

    features = [
        ee.Feature(ee.Geometry(f["geometry"]), f["properties"])
        for f in geojson["features"]
    ]
    return ee.FeatureCollection(features)


def compute_mws_biodiversity(
    gbif_fc: ee.FeatureCollection,
    mws_fc:  ee.FeatureCollection,
) -> ee.FeatureCollection:
    """
    Core GEE computation: per-MWS biodiversity statistics.

    IMPORTANT: gbif_fc is already block-scoped (uploaded from block-level
    download). Do NOT call .filterBounds() here — the FC has at most 50K
    features and the filter is unnecessary overhead.

    Pattern: ee.Join.saveAll() + aggregate_count_distinct().
    Why this pattern: preserves species identity (taxonKey) through the join.
    reduceRegions() on an Image would lose taxonKey and cannot count distinct
    species. See Section 1 for full explanation.
    """
    spatial_filter = ee.Filter.intersects(
        leftField=".geo", rightValue=None, rightField=".geo", maxError=10,
    )
    join = ee.Join.saveAll(matchesKey="gbif_occurrences", ordering="taxonKey")

    mws_with_occurrences = join.apply(
        primary=mws_fc, secondary=gbif_fc, condition=spatial_filter,
    )

    def compute_stats(mws_feature):
        occurrences = ee.FeatureCollection(
            ee.List(mws_feature.get("gbif_occurrences"))
        )
        n = occurrences.size()

        richness = occurrences.aggregate_count_distinct("taxonKey")

        histogram = occurrences.aggregate_histogram("taxonKey")
        counts    = histogram.values()
        total     = counts.reduce(ee.Reducer.sum())
        shannon   = ee.Algorithms.If(
            n.gt(1),
            ee.Number(
                counts.map(
                    lambda c: ee.Number(c).divide(total)
                              .multiply(ee.Number(c).divide(total).log())
                ).reduce(ee.Reducer.sum())
            ).multiply(-1),
            ee.Number(0),
        )

        return mws_feature.set({
            "species_richness":        richness,
            "occurrence_count":        n,
            "shannon_diversity_index": shannon,
            "data_poor":               n.lt(DATA_POOR_THRESHOLD),
        })

    mws_stats = mws_with_occurrences.map(compute_stats)

    # Merge back MWS with zero occurrences (not returned by the join)
    matched_uids   = mws_stats.aggregate_array("uid")
    mws_with_zeros = mws_fc.filter(
        ee.Filter.inList("uid", matched_uids).Not()
    ).map(
        lambda f: f.set({
            "species_richness":        0,
            "occurrence_count":        0,
            "shannon_diversity_index": 0.0,
            "data_poor":               True,
        })
    )

    return mws_stats.merge(mws_with_zeros)


def export_stats_to_gcs(
    stats_fc: ee.FeatureCollection, district: str, block: str,
) -> str:
    """Export per-MWS stats FeatureCollection to GCS as GeoJSON."""
    gcs_prefix = GCS_STATS_PREFIX.format(district=district, block=block)
    task = ee.batch.Export.table.toCloudStorage(
        collection=stats_fc,
        bucket=GCS_BUCKET,
        fileNamePrefix=gcs_prefix,
        fileFormat="GeoJSON",
        selectors=[
            "uid", "species_richness", "occurrence_count",
            "shannon_diversity_index", "data_poor",
        ],
    )
    task.start()
    logger.info(f"GEE export task: {task.id} for {district}/{block}")
    return task.id
```

**Complexity:** A block with 50–200 MWS and 100–10,000 GBIF occurrences
runs the GEE join in 10–30 seconds. Export takes 1–3 minutes.

**Failure cases:**
- `ee.Filter.inList("uid", matched_uids)` fails for blocks with > 1000 MWS
  (GEE list size limit) → Use a set-difference on the client side instead:
  export all matched UIDs via a separate step.
- All MWS have zero occurrences → `matched_uids` is empty; entire block is
  `data_poor=True`. This is valid output. The pipeline proceeds normally.

**Validation:** Pick one MWS UID. In GEE Code Editor:

```javascript
var gbif = ee.FeatureCollection('<asset_id>');
var pts  = gbif.filterBounds(ee.Geometry.Rectangle([...]));  // one MWS bbox
print(pts.aggregate_count_distinct('taxonKey'));
// must equal species_richness in the exported GeoJSON for that uid
```

---

### Stage 6 — Post-Export Processing

**Goal:** Download the GCS GeoJSON, add `dominant_taxon_group` (client-side
pandas step), and fill any NaN before GeoServer sync.

**What changed from v1:** Unchanged.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 6 | GCS GeoJSON blob from Stage 5 | Python dict (GeoJSON) | Python dict with all columns clean, `dominant_taxon_group` added | GCS download + pandas 3-line dominant computation + NaN fill | Local machine | Python dict (GeoJSON FeatureCollection in memory) |

**New file — `computing/biodiversity/export.py`:**

```python
import json, logging
from utilities.gee_utils import gcs_config
from .config import GCS_STATS_PREFIX

logger = logging.getLogger(__name__)


def download_stats_geojson(district: str, block: str) -> dict:
    """Download per-MWS stats GeoJSON from GCS."""
    blob_name = GCS_STATS_PREFIX.format(district=district, block=block) + ".geojson"
    bucket    = gcs_config()
    blob      = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(
            f"GEE export not found: gs://{bucket.name}/{blob_name}. "
            "Ensure the GEE export task completed successfully."
        )
    geojson = json.loads(blob.download_as_text())
    logger.info(
        f"Downloaded {len(geojson['features'])} MWS features for {district}/{block}"
    )
    return geojson


def add_dominant_taxon_group(geojson: dict) -> dict:
    """
    Add dominant_taxon_group to each MWS feature.

    This is the one client-side pandas step remaining. It runs on a tiny
    dataset (one block = 10–500 rows). In v3 this can be replaced with a
    second GEE Export.table that includes per-(uid, class) counts.
    For v2: set to "Unknown" as a placeholder.
    """
    for feature in geojson["features"]:
        feature["properties"]["dominant_taxon_group"] = "Unknown"
    return geojson


def prepare_geojson_for_geoserver(geojson: dict) -> dict:
    """
    Ensure all required columns are present and NaN is replaced.
    GeoServer will produce errors for features with null key columns.
    """
    required_numeric = ["species_richness", "occurrence_count", "shannon_diversity_index"]
    required_bool    = ["data_poor"]
    required_str     = ["dominant_taxon_group"]

    for feature in geojson["features"]:
        props = feature["properties"]
        for col in required_numeric:
            v = props.get(col)
            if v is None or (isinstance(v, float) and v != v):
                props[col] = 0.0
        for col in required_bool:
            if props.get(col) is None:
                props[col] = True
        for col in required_str:
            if not props.get(col):
                props[col] = "Unknown"

    return geojson
```

---

### Stage 7 — GeoServer Synchronization

**Goal:** Push the per-MWS biodiversity GeoJSON to GeoServer as a queryable
vector layer. Register the layer in the DB.

**What changed from v1:** Unchanged. Same function calls, same layer naming.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 7 | Python dict (GeoJSON) from Stage 6 | Shapefile (internal to `sync_layer_to_geoserver`) | GeoServer vector layer `biodiversity:{district}_{block}_biodiversity`, `Layer` DB row | `sync_layer_to_geoserver()` + `save_layer_info_to_db()` + `update_layer_sync_status()` | Local machine | GeoServer WFS-queryable layer |

**New file — `computing/biodiversity/sync.py`:**

```python
import logging
from computing.utils import (
    sync_layer_to_geoserver, save_layer_info_to_db, update_layer_sync_status,
)
from .config import DATASET_NAME_VECTOR, ALGORITHM_NAME, ALGORITHM_VERSION, WORKSPACE

logger = logging.getLogger(__name__)


def sync_block_to_geoserver(
    state: str, district: str, block: str, geojson_dict: dict,
) -> int | None:
    """
    Sync per-MWS biodiversity GeoJSON to GeoServer and register in DB.
    Returns the Layer DB row id.
    """
    layer_name  = f"{district}_{block}_biodiversity"
    sync_result = sync_layer_to_geoserver(
        state_name=state,
        fc=geojson_dict,
        layer_name=layer_name,
        workspace=WORKSPACE,
    )
    layer_id = save_layer_info_to_db(
        state=state,
        district=district,
        block=block,
        layer_name=layer_name,
        asset_id="not available",
        dataset_name=DATASET_NAME_VECTOR,
        sync_to_geoserver=False,
        algorithm=ALGORITHM_NAME,
        algorithm_version=ALGORITHM_VERSION,
    )
    if sync_result and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        logger.info(f"Synced {layer_name} to GeoServer (layer_id={layer_id})")
    return layer_id
```

**Failure cases:**
- `save_layer_info_to_db()` raises `Dataset.DoesNotExist` → Dataset row not
  seeded. Check Stage 0.
- `save_layer_info_to_db()` raises `StateSOI.DoesNotExist` → Admin boundaries
  not loaded. This is a prerequisite for all CoRE Stack modules.

**Validation:**

```python
from computing.models import Layer
layer = Layer.objects.get(layer_name=f"{district}_{block}_biodiversity")
assert layer.is_sync_to_geoserver == True
```

---

### Stage 8 — Excel Sheet Generation

**Goal:** Add a `biodiversity` sheet to the per-block Excel file.

**What changed from v1:** Unchanged.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 8 | GeoServer WFS response (fetched by stats_generator/utils.py) | Pandas DataFrame | `biodiversity` sheet in `{district}_{block}.xlsx` | Pandas flatten + to_excel | Local machine | Excel sheet with 6 columns |

**Changes to `stats_generator/utils.py`:**

1. Add `"biodiversity"` to the workspace/layer list used for Excel generation.

2. Add a branch in `get_vector_layer_geoserver()`:

```python
elif workspace == "biodiversity":
    create_excel_for_biodiversity(geojson_data, xlsx_file, writer)
```

3. Add the new function:

```python
def create_excel_for_biodiversity(
    geojson_data: dict, xlsx_file: str, writer,
) -> None:
    """Write 'biodiversity' sheet to the block Excel file."""
    rows = []
    for feature in geojson_data.get("features", []):
        props = feature.get("properties", {})
        rows.append({
            "UID":                     props.get("uid", ""),
            "species_richness":        int(props.get("species_richness", 0)),
            "occurrence_count":        int(props.get("occurrence_count", 0)),
            "shannon_diversity_index": float(props.get("shannon_diversity_index", 0.0)),
            "dominant_taxon_group":    str(props.get("dominant_taxon_group", "Unknown")),
            "data_poor":               bool(props.get("data_poor", True)),
        })
    pd.DataFrame(rows).to_excel(writer, sheet_name="biodiversity", index=False)
```

**Example sheet:**

| UID | species_richness | occurrence_count | shannon_diversity_index | dominant_taxon_group | data_poor |
|---|---|---|---|---|---|
| KA_RAM_CHP_MWS_001 | 47 | 312 | 3.21 | Unknown | False |
| KA_RAM_CHP_MWS_002 | 3 | 8 | 0.89 | Unknown | True |
| KA_RAM_CHP_MWS_003 | 0 | 0 | 0.0 | Unknown | True |

**Validation:**

```python
import pandas as pd
df = pd.read_excel(f"{district}_{block}.xlsx", sheet_name="biodiversity")
assert list(df.columns) == [
    "UID", "species_richness", "occurrence_count",
    "shannon_diversity_index", "dominant_taxon_group", "data_poor",
]
assert df.isnull().sum().sum() == 0
```

---

### Stage 9 — KYL Filter Integration

**Goal:** Expose 5 biodiversity keys in the KYL JSON so the frontend can
filter MWS by biodiversity indicators.

**What changed from v1:** Unchanged. The model import in Stage 10 changes
(`GBIFBlockDownload` instead of `GBIFNationalDownload`), but the KYL code
itself is identical.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 9 | `biodiversity` sheet in `{district}_{block}.xlsx` | Pandas DataFrame per MWS | 5 new keys per MWS in KYL JSON | Read Excel → extract values → append to results dict | Local machine | JSON (5 new keys per MWS entry) |

**Changes to `stats_generator/mws_indicators.py`:**

**Step 1 — Register the sheet** (in the `sheets` dict):

```python
sheets = {
    # ... existing entries ...
    "biodiversity": -1,    # ADD THIS LINE
}
```

**Step 2 — Extract per-MWS values** (inside the MWS loop):

```python
# ── Biodiversity ──────────────────────────────────────────────────────────────
species_richness        = 0
occurrence_count        = 0
shannon_diversity_index = 0.0
dominant_taxon_group    = "Unknown"
biodiversity_data_poor  = True

try:
    df_bio = sheets["biodiversity"]
    if not isinstance(df_bio, int):
        bio_row = df_bio[df_bio["UID"] == specific_mws_id]
        if not bio_row.empty:
            species_richness        = int(bio_row["species_richness"].iloc[0])
            occurrence_count        = int(bio_row["occurrence_count"].iloc[0])
            shannon_diversity_index = round(
                float(bio_row["shannon_diversity_index"].iloc[0]), 3
            )
            dominant_taxon_group    = str(bio_row["dominant_taxon_group"].iloc[0])
            biodiversity_data_poor  = bool(bio_row["data_poor"].iloc[0])
except Exception as e:
    logger.warning(f"Could not read biodiversity for MWS {specific_mws_id}: {e}")
```

**Step 3 — Add to results dict:**

```python
results.append({
    # ... existing keys ...
    "species_richness":        species_richness,
    "occurrence_count":        occurrence_count,
    "shannon_diversity_index": shannon_diversity_index,
    "dominant_taxon_group":    dominant_taxon_group,
    "biodiversity_data_poor":  biodiversity_data_poor,
})
```

**Validation:** Parse the KYL JSON. Assert all 5 keys are present. Assert
no `NaN` values (NaN is invalid JSON and crashes the frontend).

---

### Stage 10 — MWS Report Section

**Goal:** Add a "Biodiversity" section to the per-MWS HTML report.

**What changed from v1:** Model import updated to `GBIFBlockDownload`.
The DOI is now fetched from the block-specific download record.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 10 | `biodiversity` sheet in `{district}_{block}.xlsx`, `GBIFBlockDownload` DB row | — | Biodiversity section in MWS HTML report | Read Excel + Django ORM lookup | Local machine | HTML section in MWS report |

**Changes to `dpr/gen_mws_report.py`:**

```python
def get_biodiversity_data(
    state: str, district: str, block: str, uid: str,
) -> dict:
    from computing.biodiversity.models import GBIFBlockDownload

    default = {
        "species_richness": None, "occurrence_count": None,
        "shannon_diversity_index": None, "dominant_taxon_group": None,
        "data_poor": True, "gbif_doi": None, "has_data": False,
    }
    try:
        xl_path = get_excel_path(state, district, block)
        df      = pd.read_excel(xl_path, sheet_name="biodiversity")
        row     = df[df["UID"] == uid]
        if row.empty:
            return default

        gbif_doi = None
        try:
            from computing.models import StateSOI, DistrictSOI, TehsilSOI
            state_obj    = StateSOI.objects.get(name__iexact=state)
            district_obj = DistrictSOI.objects.get(name__iexact=district)
            block_obj    = TehsilSOI.objects.get(name__iexact=block)
            record = GBIFBlockDownload.objects.filter(
                state=state_obj, district=district_obj, block=block_obj,
                status=GBIFBlockDownload.Status.READY,
            ).latest("created_at")
            gbif_doi = record.doi
        except Exception:
            pass

        return {
            "species_richness":        int(row["species_richness"].iloc[0]),
            "occurrence_count":        int(row["occurrence_count"].iloc[0]),
            "shannon_diversity_index": round(float(row["shannon_diversity_index"].iloc[0]), 2),
            "dominant_taxon_group":    str(row["dominant_taxon_group"].iloc[0]),
            "data_poor":               bool(row["data_poor"].iloc[0]),
            "gbif_doi":                gbif_doi,
            "has_data":                True,
        }
    except Exception as e:
        logger.warning(f"Could not load biodiversity for {uid}: {e}")
        return default
```

**Template changes (`templates/mws-report.html`):**

```html
{% if biodiversity_data.has_data %}
<section class="report-section biodiversity">
  <h3>Biodiversity</h3>
  {% if biodiversity_data.data_poor %}
    <div class="alert alert-warning">
      <strong>Under-surveyed watershed.</strong>
      Fewer than 20 occurrence records in GBIF. Low species counts reflect
      a data gap, not necessarily low biodiversity.
    </div>
  {% endif %}
  <p>
    <strong>Species recorded:</strong> {{ biodiversity_data.species_richness }}
    distinct species across {{ biodiversity_data.occurrence_count }} occurrences.
  </p>
  <p>Shannon diversity index: {{ biodiversity_data.shannon_diversity_index }}.</p>
  {% if biodiversity_data.gbif_doi %}
    <p class="citation"><em>GBIF DOI: {{ biodiversity_data.gbif_doi }}</em></p>
  {% endif %}
</section>
{% endif %}
```

---

### Stage 11 — Tehsil Report Section

**Goal:** Add a "Biodiversity" section to the block/tehsil report,
summarizing richness distribution across all MWS.

**What changed from v1:** Unchanged.

| Stage | Input | Intermediate | Output | Computation | Runs on | Expected output format |
|---|---|---|---|---|---|---|
| 11 | `biodiversity` sheet in `{district}_{block}.xlsx` | — | Biodiversity summary section in tehsil HTML report | Read Excel + pandas aggregations | Local machine | HTML section in tehsil report |

**Changes to `dpr/gen_tehsil_report.py`:**

```python
def get_biodiversity_pattern_data(
    state: str, district: str, block: str,
) -> dict:
    default = {"has_data": False, "patterns": [], "summary": {}}
    try:
        xl_path    = get_excel_path(state, district, block)
        df         = pd.read_excel(xl_path, sheet_name="biodiversity")
        total_mws  = len(df)
        dp_count   = int(df["data_poor"].sum())
        rich_count = int(
            (df["species_richness"] > df["species_richness"].quantile(0.75)).sum()
        )
        return {
            "has_data": True,
            "summary": {
                "total_mws":       total_mws,
                "data_poor_count": dp_count,
                "data_poor_pct":   round(dp_count / total_mws * 100, 1) if total_mws else 0,
                "rich_mws_count":  rich_count,
                "median_richness": round(float(df["species_richness"].median()), 1),
            },
            "patterns": df[
                ["UID", "species_richness", "occurrence_count", "data_poor"]
            ].to_dict("records"),
        }
    except Exception as e:
        logger.warning(f"Could not load biodiversity pattern data for {block}: {e}")
        return default
```

---

### New Model — `GBIFBlockDownload`

**What changed from v1:** Replaced `GBIFNationalDownload` (no block FK) with
`GBIFBlockDownload` (state + district + block FKs). The new model tracks
the complete pipeline status per block and stores both the ingestion and
export task IDs.

**New file — `computing/biodiversity/models.py`:**

```python
from django.db import models
from boundaries.models import StateSOI, DistrictSOI, TehsilSOI


class GBIFBlockDownload(models.Model):
    """
    Tracks the complete GBIF pipeline status for one block.
    Each block has its own download, GEE asset, and export.
    There is no shared national record.
    """
    class Status(models.TextChoices):
        PENDING       = "PENDING",       "Pending"
        DOWNLOADING   = "DOWNLOADING",   "Downloading from GBIF"
        CLEANING      = "CLEANING",      "Cleaning occurrences"
        UPLOADING_GCS = "UPLOADING_GCS", "Uploading to GCS"
        INGESTING_GEE = "INGESTING_GEE", "Ingesting to GEE"
        COMPUTING     = "COMPUTING",     "Computing MWS stats"
        SYNCING       = "SYNCING",       "Syncing to GeoServer"
        READY         = "READY",         "Ready"
        FAILED        = "FAILED",        "Failed"

    state    = models.ForeignKey(StateSOI,    on_delete=models.CASCADE)
    district = models.ForeignKey(DistrictSOI, on_delete=models.CASCADE)
    block    = models.ForeignKey(TehsilSOI,   on_delete=models.CASCADE)

    download_key       = models.CharField(max_length=255)
    doi                = models.CharField(max_length=511, blank=True, null=True)
    gcs_geojson_uri    = models.CharField(max_length=1023, blank=True, null=True)
    gee_asset_id       = models.CharField(max_length=1023, blank=True, null=True)
    raw_record_count   = models.IntegerField(null=True, blank=True)
    clean_record_count = models.IntegerField(null=True, blank=True)
    status             = models.CharField(
        max_length=50, choices=Status.choices, default=Status.PENDING
    )
    gee_ingest_task_id = models.CharField(max_length=255, blank=True, null=True)
    gee_export_task_id = models.CharField(max_length=255, blank=True, null=True)
    created_at         = models.DateTimeField(auto_now_add=True)
    updated_at         = models.DateTimeField(auto_now=True)
    misc               = models.JSONField(blank=True, null=True)

    class Meta:
        verbose_name = "GBIF Block Download"
        unique_together = [("state", "district", "block", "download_key")]
        ordering = ["-created_at"]

    def __str__(self):
        return (
            f"GBIF {self.block} [{self.status}] "
            f"({self.clean_record_count or '?'} records)"
        )
```

---

### Celery Task — Complete Block Pipeline

**What changed from v1:** The old task assumed a national pipeline had
already run and only handled Stage 4b. The new task runs the **entire pipeline**
for one block: download → clean → GCS → GEE ingest → GEE compute → export →
GeoServer sync.

**New file — `computing/biodiversity/tasks.py`:**

```python
import os, ee, logging
from celery import shared_task
from utilities.gee_utils import ee_initialize, check_task_status
from .config import (
    get_gee_block_asset_id, DATASET_NAME_VECTOR,
)

logger   = logging.getLogger(__name__)
DATA_DIR = "/data/gbif/blocks"


@shared_task
def generate_biodiversity_block(
    state: str, district: str, block: str, gee_account_id: int,
):
    """
    Complete biodiversity pipeline for one block.
    Can be called for a single block during development or iterated over
    all blocks for national coverage — the code is identical in both cases.

    Stages:
    1. Download GBIF data for this block (GEOMETRY predicate)
    2. Clean occurrences
    3. Upload clean GeoJSON to GCS
    4. Ingest GeoJSON into GEE as a FeatureCollection asset
    5. Compute per-MWS biodiversity statistics in GEE
    6. Export stats to GCS as GeoJSON
    7. Sync to GeoServer + register in DB
    """
    from computing.models import Layer, Dataset
    from .models import GBIFBlockDownload
    from .download import get_block_bbox_wkt, request_block_download, wait_and_fetch
    from .clean import clean_occurrences
    from .gee_upload import (
        csv_to_geojson, upload_geojson_to_gcs,
        ingest_geojson_to_gee, wait_for_gee_ingestion,
    )
    from .mws_statistics import (
        load_mws_featurecollection, compute_mws_biodiversity, export_stats_to_gcs,
    )
    from .export import (
        download_stats_geojson, add_dominant_taxon_group,
        prepare_geojson_for_geoserver,
    )
    from .sync import sync_block_to_geoserver

    # ── Env check ────────────────────────────────────────────────────────────
    for var in ["GBIF_USER", "GBIF_PWD", "GBIF_EMAIL"]:
        if not os.environ.get(var):
            raise RuntimeError(f"Missing env var: {var}. Check .env file.")

    layer_name = f"{district}_{block}_biodiversity"

    # ── Idempotency ──────────────────────────────────────────────────────────
    try:
        dataset = Dataset.objects.get(name=DATASET_NAME_VECTOR)
    except Dataset.DoesNotExist:
        raise RuntimeError(
            f"Dataset '{DATASET_NAME_VECTOR}' not found. Run Stage 0 setup first."
        )

    existing = Layer.objects.filter(
        dataset=dataset, layer_name=layer_name,
    ).first()
    if existing and existing.is_sync_to_geoserver:
        logger.info(f"Already computed: {layer_name}. Skipping.")
        return f"Skipped (already computed): {layer_name}"

    block_dir = os.path.join(DATA_DIR, state, district, block)
    os.makedirs(block_dir, exist_ok=True)

    # Create status record
    from computing.models import StateSOI, DistrictSOI, TehsilSOI
    state_obj    = StateSOI.objects.get(name__iexact=state)
    district_obj = DistrictSOI.objects.get(name__iexact=district)
    block_obj    = TehsilSOI.objects.get(name__iexact=block)
    record = GBIFBlockDownload.objects.create(
        state=state_obj, district=district_obj, block=block_obj,
        download_key="pending", status=GBIFBlockDownload.Status.DOWNLOADING,
    )

    # ── Stage 1: Download ────────────────────────────────────────────────────
    logger.info(f"[{block}] Stage 1: Downloading from GBIF")
    block_bbox_wkt = get_block_bbox_wkt(district, block)
    download_key   = request_block_download(block_bbox_wkt)
    record.download_key = download_key
    record.save()

    raw_csv, doi, raw_count = wait_and_fetch(download_key, block_dir)
    record.doi              = doi
    record.raw_record_count = raw_count
    record.save()
    logger.info(f"[{block}] {raw_count:,} raw records. DOI: {doi}")

    # ── Stage 2: Clean ───────────────────────────────────────────────────────
    logger.info(f"[{block}] Stage 2: Cleaning")
    record.status = GBIFBlockDownload.Status.CLEANING
    record.save()
    clean_csv   = os.path.join(block_dir, f"gbif_{district}_{block}_clean.csv")
    clean_stats = clean_occurrences(raw_csv, clean_csv)
    record.clean_record_count = clean_stats["final"]
    record.save()
    logger.info(
        f"[{block}] {clean_stats['final']:,} clean records "
        f"({clean_stats['drop_rate_pct']}% dropped)"
    )

    # ── Stage 3: GCS Upload ──────────────────────────────────────────────────
    logger.info(f"[{block}] Stage 3: Uploading to GCS")
    record.status = GBIFBlockDownload.Status.UPLOADING_GCS
    record.save()
    geojson_path = os.path.join(block_dir, f"gbif_{district}_{block}.geojson")
    csv_to_geojson(clean_csv, geojson_path)
    gcs_uri = upload_geojson_to_gcs(geojson_path, state, district, block)
    record.gcs_geojson_uri = gcs_uri
    record.save()

    # ── Stage 4: GEE Ingestion ───────────────────────────────────────────────
    logger.info(f"[{block}] Stage 4: Ingesting into GEE")
    record.status = GBIFBlockDownload.Status.INGESTING_GEE
    record.save()
    task_id = ingest_geojson_to_gee(gcs_uri, state, district, block, gee_account_id)
    record.gee_ingest_task_id = task_id
    asset_id = get_gee_block_asset_id(state, district, block)
    record.gee_asset_id = asset_id
    record.save()
    wait_for_gee_ingestion(task_id)
    logger.info(f"[{block}] GEE asset ready: {asset_id}")

    # ── Stage 5: GEE Computation ─────────────────────────────────────────────
    logger.info(f"[{block}] Stage 5: Computing MWS statistics in GEE")
    record.status = GBIFBlockDownload.Status.COMPUTING
    record.save()
    ee_initialize(gee_account_id)
    gbif_fc        = ee.FeatureCollection(asset_id)   # NO filterBounds
    mws_fc         = load_mws_featurecollection(district, block)
    stats_fc       = compute_mws_biodiversity(gbif_fc, mws_fc)
    export_task_id = export_stats_to_gcs(stats_fc, district, block)
    record.gee_export_task_id = export_task_id
    record.save()
    check_task_status([export_task_id])
    logger.info(f"[{block}] GEE export complete")

    # ── Stage 6 + 7: Post-process + GeoServer Sync ──────────────────────────
    logger.info(f"[{block}] Stage 6–7: Post-processing and syncing to GeoServer")
    record.status = GBIFBlockDownload.Status.SYNCING
    record.save()
    geojson  = download_stats_geojson(district, block)
    geojson  = add_dominant_taxon_group(geojson)
    geojson  = prepare_geojson_for_geoserver(geojson)
    layer_id = sync_block_to_geoserver(state, district, block, geojson)

    record.status = GBIFBlockDownload.Status.READY
    record.save()
    logger.info(f"[{block}] Complete: {layer_name} (layer_id={layer_id})")
    return f"Success: {layer_name} (layer_id={layer_id})"
```

---

### API Endpoint

**What changed from v1:** Unchanged. The endpoint is identical. The difference
is that the task now runs the full pipeline, not just Stage 4b.

**Changes to `computing/api.py`:**

```python
from computing.biodiversity.tasks import generate_biodiversity_block

@api_security_check(allowed_methods="POST")
@schema(None)
def generate_biodiversity_layer(request):
    """Trigger complete biodiversity pipeline for one block as a Celery task."""
    state          = request.data.get("state", "").lower()
    district       = request.data.get("district", "").lower()
    block          = request.data.get("block", "").lower()
    gee_account_id = request.data.get("gee_account_id")

    if not all([state, district, block, gee_account_id]):
        return Response(
            {"error": "state, district, block, and gee_account_id are required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    generate_biodiversity_block.apply_async(
        args=[state, district, block, gee_account_id],
        queue="nrm",
    )
    return Response(
        {"Success": f"Biodiversity task initiated for {district}/{block}"},
        status=status.HTTP_200_OK,
    )
```

**`computing/urls.py`:**

```python
path("generate_biodiversity_layer/", views.generate_biodiversity_layer),
```

---

### Management Command — Block Pipeline

**What changed from v1:** Replaced `generate_gbif_national` (a prerequisite
management command) with `generate_gbif_block` (runs the complete pipeline for
one specific block). Useful for development and debugging.

**New file — `computing/management/commands/generate_gbif_block.py`:**

```python
import logging
from django.core.management.base import BaseCommand
from computing.biodiversity.tasks import generate_biodiversity_block

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run the complete GBIF biodiversity pipeline for one block"

    def add_arguments(self, parser):
        parser.add_argument("--state",    type=str, required=True)
        parser.add_argument("--district", type=str, required=True)
        parser.add_argument("--block",    type=str, required=True)
        parser.add_argument("--gee-account-id", type=int, required=True)

    def handle(self, *args, **options):
        state          = options["state"].lower()
        district       = options["district"].lower()
        block          = options["block"].lower()
        gee_account_id = options["gee_account_id"]

        self.stdout.write(
            f"Starting biodiversity pipeline for {state}/{district}/{block}..."
        )
        result = generate_biodiversity_block(state, district, block, gee_account_id)
        self.stdout.write(self.style.SUCCESS(result))
```

**Usage:**

```bash
# Test on a single block (takes ~30 minutes end-to-end)
python manage.py generate_gbif_block \
    --state=karnataka \
    --district=ramanagara \
    --block=channapatna \
    --gee-account-id=3

# For national coverage: iterate via a script or trigger the API endpoint per block
```

---

## 6. GEE Computation Deep-Dive

### Why `aggregate_count_distinct()` Is the Correct Reducer

| GEE Approach | What it computes | Correct for species richness? |
|---|---|---|
| `ee.Reducer.count()` on an Image | Total pixel count in polygon | No — counts pixels, not species |
| `ee.Reducer.countDistinct()` (Image reducer) | Distinct pixel values | No — species identity lost when rasterized |
| `FeatureCollection.aggregate_count_distinct('taxonKey')` | Distinct taxonKey values | **Yes — exactly species richness** |
| `FeatureCollection.size()` | Total feature count | No — counts records, not species |

### The Join Pattern — Step by Step

```python
# Primary:   MWS polygons for this block (50–200 MWS)
mws_fc = load_mws_featurecollection(district, block)

# Secondary: block-scoped GBIF points (100–50,000 records)
# NOTE: No filterBounds() needed — the FC was uploaded for this block only
gbif_fc = ee.FeatureCollection(get_gee_block_asset_id(state, district, block))

# Spatial filter: a GBIF point matches an MWS if it intersects (is inside) it
condition = ee.Filter.intersects(
    leftField='.geo', rightValue=None, rightField='.geo', maxError=10,
)

# saveAll: for each MWS, save ALL matching GBIF points as a List property
join = ee.Join.saveAll(matchesKey='gbif_occurrences')

# Apply: result is MWS Features each with 'gbif_occurrences' = List of point dicts
mws_joined = join.apply(primary=mws_fc, secondary=gbif_fc, condition=condition)

# Map: compute stats per MWS
def compute(feature):
    pts      = ee.FeatureCollection(ee.List(feature.get('gbif_occurrences')))
    richness = pts.aggregate_count_distinct('taxonKey')
    return feature.set('species_richness', richness)

result = mws_joined.map(compute)
```

### Shannon Diversity — Server-Side GEE Computation

Shannon H = −∑ pᵢ · ln(pᵢ), where pᵢ = nᵢ / N

```python
def compute_shannon(occurrences: ee.FeatureCollection) -> ee.Number:
    """All operations run on GEE servers. No data transferred to client."""
    n         = occurrences.size()
    histogram = occurrences.aggregate_histogram("taxonKey")
    counts    = histogram.values()
    total     = counts.reduce(ee.Reducer.sum())
    shannon   = ee.Algorithms.If(
        n.gt(1),
        ee.Number(
            counts.map(
                lambda c: ee.Number(c).divide(total)
                          .multiply(ee.Number(c).divide(total).log())
            ).reduce(ee.Reducer.sum())
        ).multiply(-1),
        ee.Number(0),
    )
    return shannon
```

### Why Block-First Is Faster in GEE

In v1, `filterBounds()` ran against 20M+ records before the spatial join.
GEE must load and index 20M features before returning the filtered subset.

In v2, the join input is already block-scoped (100–50K records). GEE indexes
a 200–500× smaller collection. The join completes in seconds instead of
minutes. No `filterBounds()` call is needed at all.

---

## 7. Directory Structure

```
computing/
├── api.py                          # CHANGED: add generate_biodiversity_layer endpoint
├── urls.py                         # CHANGED: add URL path
├── management/
│   └── commands/
│       └── generate_gbif_block.py  # NEW: block-level management command
└── biodiversity/                   # NEW MODULE
    ├── __init__.py
    ├── config.py                   # Block-scoped paths, get_gee_block_asset_id()
    ├── models.py                   # GBIFBlockDownload model
    ├── tasks.py                    # Complete pipeline Celery task
    ├── download.py                 # get_block_bbox_wkt() + request_block_download()
    ├── clean.py                    # 5-filter cleaning
    ├── gee_upload.py               # GeoJSON → GCS → GEE ingestion
    ├── mws_statistics.py           # GEE join + Shannon + export
    ├── export.py                   # Download from GCS + post-process
    └── sync.py                     # GeoServer sync + DB registration

stats_generator/
├── utils.py                        # CHANGED: add biodiversity Excel branch
└── mws_indicators.py               # CHANGED: add biodiversity KYL keys

dpr/
├── gen_mws_report.py               # CHANGED: add get_biodiversity_data()
└── gen_tehsil_report.py            # CHANGED: add get_biodiversity_pattern_data()

installation/
└── seed/
    └── seed_data.json              # CHANGED: add Biodiversity Occurrence Dataset row
```

---

## 8. Implementation Roadmap

### Milestone 1 — Single Block, End-to-End (1–2 weeks)

Goal: Run the complete pipeline for one test block. Confirm GeoServer layer
appears with correct MWS data.

1. Stage 0: Create biodiversity Dataset row, GeoServer workspace, env vars
2. Stage 1: Implement `download.py` — test `get_block_bbox_wkt()` against
   GeoServer, then `request_block_download()` with Aves filter for speed
3. Stage 2: Implement `clean.py` — verify `drop_rate_pct` is 10–50%
4. Stage 3: Implement `gee_upload.py` — verify GCS blob exists
5. Stage 4: GEE ingestion — verify asset in GEE Code Editor
6. Stage 5: Implement `mws_statistics.py` — run in GEE Code Editor first,
   then via Python
7. Stage 6: Implement `export.py` — verify GeoJSON has all MWS UIDs
8. Stage 7: Implement `sync.py` — verify GeoServer WFS returns features
9. Wire into Celery task + API endpoint
10. Confirm `GBIFBlockDownload` status = READY

### Milestone 2 — Excel → KYL → Reports (1 week)

1. Stage 8: Add `biodiversity` branch to `stats_generator/utils.py`
2. Stage 9: Add biodiversity keys to `mws_indicators.py`
3. Stages 10–11: Add report sections
4. Validate: regenerate KYL JSON → confirm 5 new keys per MWS

### Milestone 3 — Second Block, Regression Test (3 days)

Run the pipeline on a second block (preferably one with very different
biodiversity characteristics — e.g. Western Ghats vs. dry plain).
Confirm no regressions in existing modules.

### Milestone 4 — All Blocks in One District (3–5 days)

Trigger via API endpoint in a loop. Monitor Celery queues. Verify all blocks
complete independently. Fix any idempotency issues.

### Milestone 5 — National Rollout (ongoing)

Iterate over all blocks. Run in parallel (Celery concurrency). Monitor
`GBIFBlockDownload` DB table for FAILED status.

---

## 9. Complete Summary

This section explains the entire pipeline in 5–10 minutes. Suitable for
presenting to a mentor or colleague who has not read this document.

### What We Are Building

A pipeline that answers: "For each micro-watershed in India, how many distinct
species have been recorded, and how evenly are they distributed?"

The data source is GBIF (Global Biodiversity Information Facility) — a public
database of species occurrence records (lat/lon + species + date). India has
40–80 million records in GBIF. We don't download all of them at once; instead,
we process one administrative block at a time.

### Input → Operation → Output for Every Step

**Step 1 — Get the block boundary.**

- Input: `district` and `block` name strings
- Operation: HTTP GET to our own GeoServer WFS → parse the MWS GeoJSON →
  compute bounding box → format as WKT POLYGON string
- Output: `"POLYGON((77.1 12.7, 77.6 12.7, ...))"` — a text description of
  the block's geographic extent
- Why: GBIF's download API accepts a geometry predicate; this restricts the
  download to records within our block

**Step 2 — Download GBIF data for this block.**

- Input: block bbox WKT + GBIF credentials from `.env`
- Operation: `pygbif occ.download()` submits an async request to GBIF; poll
  every 60 seconds; fetch ZIP when done; unzip to CSV
- Output: `gbif_{district}_{block}_raw.csv` — tab-separated, 100–50,000 rows
- Example row: `gbifID=1234567, taxonKey=2480303, species=Passer domesticus,
  decimalLatitude=12.97, decimalLongitude=77.59, kingdom=Animalia, class=Aves`
- Why the Download API (not search): GBIF search is capped at 100,000 records;
  Download API has no cap and produces a citable DOI

**Step 3 — Clean the data.**

- Input: raw tab-separated CSV
- Operation (5 pandas filters): drop rows missing species/taxonKey/coordinates;
  drop records outside India bbox; drop records with coordinate uncertainty > 10 km;
  drop "pile coordinates" (> 1000 records at exact same point, likely institution
  centroids); deduplicate (same taxonKey at exact same coordinate = 1 record)
- Output: `gbif_{district}_{block}_clean.csv` — comma-separated, typically 70–90%
  of raw records survive at block level
- Why: Dirty coordinates inflate species counts. A record snapped to a city
  centroid would falsely inflate richness at that point.

**Step 4 — Convert CSV to GeoJSON and upload to GCS.**

- Input: clean CSV
- Operation: chunked pandas read → write GeoJSON Features to disk → upload to
  Google Cloud Storage
- Output: `gs://<bucket>/gbif/blocks/{state}/{district}/{block}/occurrences.geojson`
- GeoJSON Feature example:
  `{"type":"Feature","geometry":{"type":"Point","coordinates":[77.59, 12.97]},
   "properties":{"taxonKey":"2480303","species":"Passer domesticus",...}}`
- Why GeoJSON (not CSV): GEE table ingestion accepts GeoJSON natively; the
  geometry is encoded explicitly rather than as lat/lon columns

**Step 5 — Ingest into Google Earth Engine.**

- Input: GCS GeoJSON URI
- Operation: `gcs_to_gee_asset_cli()` calls `earthengine upload table` under
  the hood; poll until SUCCEEDED
- Output: GEE FeatureCollection asset at
  `projects/<project>/assets/{state}/{district}/{block}/gbif_occurrences`
- Asset size: 100–50,000 features, each an `ee.Geometry.Point` with 6 properties
- Why GEE: All spatial computation will happen server-side in GEE; this is the
  "staging area" that GEE can access

**Step 6 — Compute per-MWS biodiversity statistics in GEE.**

- Input: block GBIF FeatureCollection (in GEE) + MWS polygons (fetched from
  GeoServer WFS, converted to `ee.FeatureCollection`)
- Core GEE operation:
  1. `ee.Join.saveAll()` with `ee.Filter.intersects()` — for each MWS polygon,
     find all GBIF points that fall inside it and save them as a list
  2. `.aggregate_count_distinct('taxonKey')` — count how many distinct species
     (taxonKey values) are in each MWS list → this is `species_richness`
  3. `.size()` → `occurrence_count`
  4. `aggregate_histogram('taxonKey')` → proportions → `−∑ pᵢ log(pᵢ)` →
     `shannon_diversity_index`
  5. If `occurrence_count < 20` → `data_poor = True`
  6. MWS with zero occurrences are merged back with all zeros and `data_poor=True`
- Output: GEE FeatureCollection — one Feature per MWS, with uid + 4 stats
- Why not `reduceRegions()`: That function works on Images (rasters). GBIF is
  point data. If you rasterize the points first, you lose taxonKey and cannot
  count distinct species. The join pattern preserves species identity throughout.

**Step 7 — Export stats to GCS.**

- Input: GEE FeatureCollection with per-MWS stats
- Operation: `ee.batch.Export.table.toCloudStorage()` — GEE writes the
  FeatureCollection to GCS as GeoJSON
- Output: `gs://<bucket>/gbif/stats/{district}_{block}_biodiversity.geojson`
- This GeoJSON is a standard FeatureCollection where each Feature is one MWS
  polygon with the 4 biodiversity statistics as properties

**Step 8 — Post-process and sync to GeoServer.**

- Input: GCS GeoJSON blob
- Operations:
  1. Download blob to memory (tiny — one block = 10–500 features)
  2. Add `dominant_taxon_group = "Unknown"` (placeholder for v2)
  3. Fill any NaN with defaults (prevents GeoServer publish errors)
  4. `sync_layer_to_geoserver()` — writes to shapefile, pushes to GeoServer
  5. `save_layer_info_to_db()` + `update_layer_sync_status()` — registers the
     layer in the CoRE Stack DB
- Output: GeoServer vector layer `biodiversity:{district}_{block}_biodiversity`
  (WFS-queryable, one row per MWS)

**Steps 9–12 — Excel → KYL → Reports (existing infrastructure, minor changes).**

- `stats_generator/utils.py` fetches the layer from GeoServer WFS → writes a
  `biodiversity` sheet in the block's Excel file
- `mws_indicators.py` reads the sheet → emits 5 new keys per MWS in the KYL
  JSON (`species_richness`, `occurrence_count`, `shannon_diversity_index`,
  `dominant_taxon_group`, `biodiversity_data_poor`)
- Report generators read the Excel sheet → add a Biodiversity section to MWS
  and tehsil HTML reports, including a data-poor warning where applicable and
  a GBIF DOI citation

### Why This Architecture

The pipeline is block-first so that:
- You can test on one block in ~30 minutes without a 12-hour national download
- Each block is fully independent — a failure in one block does not affect others
- GEE processes 100–50K points per block instead of 20M+ for the national FC,
  making the join 100–500× faster
- The same `generate_biodiversity_block` task scales to national coverage by
  simply iterating over blocks

---

## 10. Data Flow Table

| Stage | Input Type | Output Type | Intermediate Type | Runs On | Major Computation |
|---|---|---|---|---|---|
| 0 — Setup | Nothing | DB rows + GeoServer workspace | — | Local shell | Django `loaddata` + GeoServer REST |
| 1 — Download | `district`, `block` strings | Raw GBIF CSV (~50 KB–10 MB, tab-sep) | Block bbox WKT string | Local machine + GBIF HTTP API | `pygbif occ.download()` with GEOMETRY predicate |
| 2 — Clean | Raw tab-sep CSV | Clean comma-sep CSV (70–90% of input) | Pandas DataFrame in memory | Local machine | Pandas: 5 sequential filter operations |
| 3 — GCS Upload | Clean CSV | GCS GeoJSON blob (`gbif/blocks/…/occurrences.geojson`) | GeoJSON file on local disk | Local machine → GCS | Chunked JSON write + `blob.upload_from_filename()` |
| 4 — GEE Ingest | GCS GeoJSON URI (`gs://…`) | GEE FeatureCollection asset | GEE ingestion task | GEE (server) | `earthengine upload table` via `gcs_to_gee_asset_cli()` |
| 5 — GEE Compute | GEE FeatureCollection (block GBIF) + GEE FeatureCollection (MWS polygons) | GCS GeoJSON (`gbif/stats/…_biodiversity.geojson`) | GEE FeatureCollection (joined MWS + occurrence lists) | GEE (server) | `ee.Join.saveAll()` + `aggregate_count_distinct('taxonKey')` + Shannon histogram |
| 6 — Post-Process | GCS GeoJSON blob | Python dict (GeoJSON in memory) | — | Local machine | GCS download + `dominant_taxon_group` assignment + NaN fill |
| 7 — GeoServer Sync | Python dict (GeoJSON) | GeoServer vector layer + `Layer` DB row | Shapefile (internal) | Local machine → GeoServer | `sync_layer_to_geoserver()` + `save_layer_info_to_db()` |
| 8 — Excel | GeoServer WFS JSON response | `biodiversity` sheet in `{district}_{block}.xlsx` | Pandas DataFrame | Local machine | `pd.DataFrame.to_excel()` |
| 9 — KYL | `biodiversity` Excel sheet | 5 new keys per MWS in KYL JSON | Pandas DataFrame | Local machine | Pandas row lookup + dict append |
| 10 — MWS Report | Excel sheet + `GBIFBlockDownload` ORM | Biodiversity section in MWS HTML | — | Local machine | `pd.read_excel()` + template render |
| 11 — Tehsil Report | Excel sheet | Biodiversity summary section in tehsil HTML | Pandas aggregations | Local machine | `df.median()`, `df["data_poor"].sum()`, quantile |

---

## 11. How I Would Explain This Pipeline to Another Developer

**What data do we start with?**

GBIF is a public database of wildlife sighting records. Each record is one
observation: "someone saw a House Sparrow at lat 12.97, lon 77.59 on
2023-03-15." We want to know, for each micro-watershed in our study area, how
many distinct species have been recorded there.

The catch is that GBIF data is opportunistic — it reflects where people have
gone and looked, not where species actually are. A watershed with many records
might just have more birdwatchers, not more birds. We always show
`occurrence_count` alongside `species_richness` so the user can see this, and
we flag watersheds with < 20 records as `data_poor`.

**How do we get the data?**

We use the GBIF Download API (not the search API, which is capped at 100K
records). We pass a GEOMETRY predicate — a bounding box of the block we're
processing — so GBIF returns only the records within that area. For a typical
block this is 100–50,000 records and takes 5–20 minutes. We then clean the
data with 5 filters to remove records with bad coordinates.

**How do we go from sightings to per-watershed statistics?**

This is the core technical challenge. The standard CoRE Stack approach is
to use GEE's `reduceRegions()` to aggregate raster values per polygon. But
GBIF is point data — not a raster. If we rasterize the points first, we lose
the species identity (taxonKey), and then we can't count distinct species.

Instead, we do a spatial join in GEE: for each MWS polygon, collect all the
GBIF points inside it (using `ee.Join.saveAll()` + `ee.Filter.intersects()`),
then call `aggregate_count_distinct('taxonKey')` on that sub-collection. This
counts distinct species names. GEE does this entirely server-side across all
MWS in parallel — no data is transferred to the local machine during this step.

We also compute Shannon diversity (a measure of species evenness, not just
count) using `aggregate_histogram('taxonKey')` to get per-species counts,
then the standard H = −∑ pᵢ log(pᵢ) formula via GEE server-side list math.

**Which stages run where?**

- **GBIF API (remote):** Download request + poll + fetch ZIP → runs against
  GBIF servers, triggered locally
- **Local machine:** Unzip, clean CSV, convert to GeoJSON, upload to GCS,
  download the final stats GeoJSON, post-process, sync to GeoServer
- **Google Earth Engine (server):** Ingest GeoJSON as FeatureCollection,
  spatial join, `aggregate_count_distinct`, Shannon, export to GCS
- **GeoServer:** Stores the final vector layer (one row per MWS, 5 columns)

The local machine is a pure orchestrator — it triggers things, waits, and
moves files. It does no heavy spatial computation.

**What are the final deliverables?**

1. **GeoServer vector layer** `biodiversity:{district}_{block}_biodiversity`:
   per-MWS biodiversity statistics, WFS-queryable, used by the stats generator
2. **Excel sheet** `biodiversity` in `{district}_{block}.xlsx`: the same data
   in tabular form, with 6 columns (UID, 4 stats, data_poor flag)
3. **KYL JSON** entries: 5 new filter keys per MWS that the frontend uses for
   spatial filtering
4. **Report sections**: biodiversity paragraphs in MWS and tehsil HTML reports,
   including a data-poor warning and GBIF DOI citation

**Why block-first instead of downloading all of India at once?**

The national approach downloads 40–80M records, takes 6–12 hours, and produces
a 20M+ record GEE asset. Every GEE spatial join then has to `filterBounds()`
across 20M points before it can do the per-MWS join. A single failure during
the national download or ingestion blocks all blocks from being processed.

The block approach downloads 100–50K records per block, takes 20–30 minutes
end-to-end, and produces a small GEE asset. No `filterBounds()` needed — the
FC is already the right size. Testing on one block takes 30 minutes instead
of 12 hours. Failures are isolated to the specific block that failed.
