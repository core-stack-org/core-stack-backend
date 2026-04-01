# GeoNode and QGIS Distribution Methodology

## Objective

Build one clear publication path for CoRE Stack layers so that:

- computation and provenance remain anchored in Google Earth Engine (GEE)
- served layers remain anchored in GeoServer
- GeoNode becomes the discovery, catalog, and governance layer
- QGIS becomes the easiest desktop client for browsing, styling, downloading, and analysis

The key design choice is to avoid creating a second canonical publishing pipeline. In the current backend, CoRE Stack already computes layers in GEE, syncs layers to GeoServer, stores layer metadata in Django models, and exposes downloadable layer URLs through the public API. This methodology formalizes how GeoNode should sit on top of that existing flow.

## Why this fits the current codebase

The current backend already contains the major building blocks:

- [`utilities/gee_gen_utils.py`](../../../utilities/gee_gen_utils.py): standard GEE vector upload path
- [`computing/utils.py`](../../../computing/utils.py): GeoServer publication helpers such as `sync_fc_to_geoserver` and `push_shape_to_geoserver`
- [`computing/models.py`](../../../computing/models.py): canonical `Dataset` and `Layer` registry
- [`public_api/views.py`](../../../public_api/views.py): `fetch_generated_layer_urls`, which already returns per-layer URLs, versions, style references, and GEE asset paths
- [`stats_generator/utils.py`](../../../stats_generator/utils.py): existing WFS URL construction

So the shortest sustainable path is:

1. Publish once from CoRE Stack to GeoServer.
2. Register and enrich metadata in Django.
3. Expose a normalized manifest for downstream clients.
4. Let GeoNode harvest or register those served resources.
5. Let QGIS consume either GeoNode or the direct OGC service endpoints.

## Recommended architecture

### 1. Canonical system roles

- **GEE**: computation, lineage, reproducibility, public asset references
- **GeoServer**: canonical serving layer for WMS, WFS, and WCS
- **Django models**: canonical metadata and publication registry
- **GeoNode**: catalog, search, permissions, thumbnails, metadata curation, layer landing pages
- **QGIS**: analyst and implementer desktop client

### 2. Publish once, catalog many times

CoRE Stack should continue treating GeoServer as the first served output, not GeoNode.

That keeps the existing backend logic intact:

- vector outputs already flow through GeoPackage or shapefile publication into GeoServer
- raster outputs already flow into GeoServer coverage stores
- API consumers already receive GeoServer-backed download URLs

GeoNode should therefore be used primarily as:

- a searchable catalog of the already-published GeoServer layers
- a metadata and permissions surface
- a stable public landing page for each layer or thematic collection

## Publication methodology

### Step 1. Generate and preserve the canonical layer assets

Every layer should continue to have a clear upstream source:

- GEE asset path for provenance and reproducibility
- GeoServer workspace and layer name for serving
- dataset and layer records in Django for governance

Minimum metadata contract per layer:

- `dataset_name`
- `layer_name`
- `layer_type`
- `workspace`
- `layer_version`
- `state`
- `district`
- `tehsil`
- `gee_asset_path`
- `style_url`
- `is_sync_to_geoserver`

This already matches the fields that exist across [`computing/models.py`](../../../computing/models.py) and [`public_api/views.py`](../../../public_api/views.py).

### Step 2. Keep GeoServer as the canonical service endpoint

GeoServer should remain the source for all public service URLs:

- **Vector and point layers**: WFS for download and query, WMS for visualization
- **Raster layers**: WCS for download, WMS for visualization

Naming should stay deterministic:

- workspaces should map to a stable thematic domain such as `mws_layers`, `swb`, `terrain`, `drought`
- layer names should stay machine-readable and location-aware
- versions should remain attached in Django even if GeoServer layer names stay stable

### Step 3. Normalize a manifest before GeoNode onboarding

Before publishing into GeoNode, export a normalized manifest from the existing public API.

That manifest should contain:

- geographic scope
- dataset and layer names
- layer type and version
- GeoServer workspace
- OWS and WMS service URLs
- `typeName` or `CoverageId`
- QGIS provider hint such as `WFS` or `WCS`
- style URL
- GEE asset path

This repo now includes a helper for that:

- [`utilities/core_stack_layer_manifest.py`](../../../utilities/core_stack_layer_manifest.py)

Use it for either one location or all active locations.

Example for one tehsil:

```bash
python utilities/core_stack_layer_manifest.py \
  --state bihar \
  --district nalanda \
  --tehsil hilsa \
  --api-key "$CORE_STACK_API_KEY" \
  --output data/manifests/nalanda_hilsa.json \
  --csv-output data/manifests/nalanda_hilsa.csv
```

Example for many active locations:

```bash
python utilities/core_stack_layer_manifest.py \
  --all-active-locations \
  --api-key "$CORE_STACK_API_KEY" \
  --max-locations 25 \
  --output data/manifests/core_stack_catalog.json
```

### Step 4. Prefer GeoNode remote-service publishing first

For the first rollout, GeoNode should **catalog** Core-Stack layers from GeoServer instead of re-uploading every file into a second store.

Why:

- it avoids duplicating large vector and raster datasets
- it matches the current backend, which already serves via GeoServer
- it reduces the number of moving parts in sync workflows
- it makes rollback simpler because GeoServer remains authoritative

Recommended GeoNode onboarding mode:

- use **Remote Services** for curated manual registration
- use **Harvesters** for larger-scale periodic synchronization from GeoServer WMS

This is the preferred model for read-mostly public CoRE Stack layers.

Use direct GeoNode uploads only when one of the following is true:

- the layer must have an independent lifecycle inside GeoNode
- the layer must be edited inside GeoNode/MapStore workflows
- the layer must be downloadable from GeoNode as a separately managed local dataset
- the layer must survive independently from the Core-Stack GeoServer deployment

### Step 5. Separate style delivery for GeoNode and QGIS

This is the one place where a split contract is important.

Current CoRE Stack API responses already expose `style_url`, and those examples point to QGIS `.qml` files. However, GeoServer and GeoNode styling workflows are centered around SLD.

So the formal rule should be:

- **GeoServer or GeoNode style**: publish and manage as SLD
- **QGIS analyst style**: publish and manage as QML

Operationally, each dataset should ideally expose both:

- `qgis_style_url`
- `sld_url`

Today, the repo already supports GeoServer-side SLD publication in [`utilities/geoserver_utils.py`](../../../utilities/geoserver_utils.py), while the public API already exposes a QGIS-friendly style link convention through `style_url`.

### Step 6. Provide two supported QGIS entry points

QGIS users should have two supported access paths:

#### Path A. GeoNode-first

Best for:

- discovery
- browsing by metadata
- non-technical users
- public catalog exploration

Flow:

1. User connects QGIS to the GeoNode instance.
2. User browses public datasets in the GeoNode connection panel.
3. User loads WMS or WFS-backed resources into a project.

#### Path B. Manifest-first

Best for:

- analysts
- automation
- scripted downloads
- repeatable project assembly

Flow:

1. User exports a manifest from CoRE Stack.
2. User filters the manifest by location or theme.
3. User loads the returned WFS, WMS, or WCS URLs directly into QGIS.
4. User optionally applies the linked QML style.

This second path is the simplest way to "download and use all of Core-Stack" without depending on GeoNode as the only access surface.

## Download and packaging strategy

To streamline download and reuse, expose three levels of packaging:

### Level 1. OGC service access

Default for most users:

- WMS for map visualization
- WFS for vector query and download
- WCS for raster extraction

### Level 2. Direct downloadable artifacts

Best for users who need local copies:

- GeoJSON or GeoPackage for vector layers
- GeoTIFF for raster layers
- QML sidecar for QGIS styling

### Level 3. Curated project bundles

Best for onboarding and offline work:

- location-specific manifests
- optional QGIS project templates or QLR files in a later phase
- zipped thematic collections for a district or tehsil

The current repo is ready for Levels 1 and 2 now. Level 3 is a good follow-on automation step.

## Governance and operational rules

### Metadata rules

Every published layer should carry:

- human-readable title
- abstract
- thematic category
- geography keywords
- update date
- layer version
- download permissions
- provenance back to GEE asset path

### Sync rules

- GeoServer is updated by Core-Stack compute pipelines
- Django `Layer` records are updated in the same publish workflow
- GeoNode is refreshed from GeoServer on a schedule or curated import cycle
- QGIS consumers should never need to know whether a layer came from a direct upload or a harvested record

### Permission rules

- public layers should be public in GeoServer and GeoNode
- restricted layers should be private in both systems
- GeoNode should be treated as the user-facing permission surface
- service-level auth must stay aligned between GeoNode and GeoServer

## Phased rollout

### Phase 1. Catalog what already exists

- export manifests from the current API
- harvest or register public GeoServer layers into GeoNode
- test QGIS access for public layers

### Phase 2. Tighten the metadata contract

- add `sld_url` alongside current QGIS style links
- standardize dataset titles, abstracts, and keywords
- group layers into thematic collections

### Phase 3. Package for easier reuse

- generate per-location manifests automatically
- generate project-ready QGIS connection or layer-definition artifacts
- optionally publish district and state level bundles

### Phase 4. Automate sync

- schedule GeoNode harvesting against GeoServer WMS
- compare GeoNode catalog counts against Django `Layer` counts
- alert on missing layers, broken styles, or stale metadata

## Recommended decision

The recommended approach is:

1. Keep CoRE Stack publication authoritative in GeoServer.
2. Use the Django layer registry plus manifest export as the canonical catalog feed.
3. Use GeoNode mainly for discovery, permissions, and landing pages.
4. Let QGIS consume either GeoNode-public resources or direct OGC endpoints from the manifest.

That is the most streamlined path because it reuses the architecture this backend already has instead of replacing it.

## Implementation utilities

The following utilities have been created to support the GeoNode integration workflow:

### 1. Django Management Command: `export_manifest`

A Django management command that exports a GeoNode/QGIS-ready manifest directly from the database, without requiring external API calls.

```bash
# Export manifest for a specific location
python manage.py export_manifest --state bihar --district nalanda --tehsil hilsa \
  --output data/manifests/nalanda_hilsa.json \
  --csv-output data/manifests/nalanda_hilsa.csv

# Export manifest for all active locations
python manage.py export_manifest --all-active-locations --max-locations 25 \
  --output data/manifests/core_stack_catalog.json

# Use custom exclusion keywords
python manage.py export_manifest --all-active-locations \
  --exclude-keywords "run_off,evapotranspiration,precipitation,custom_keyword"
```

Key features:
- Queries Django database directly (no API key required)
- Supports both single-location and all-active-locations export
- Outputs JSON and/or CSV format
- Includes both QML style URLs and SLD style URLs
- Includes layer type, version, workspace, service URLs, and QGIS provider hints

### 2. API Endpoint: `get_layer_manifest`

A new API endpoint that serves layer manifests dynamically.

```bash
# Get manifest for a specific location
curl -H "X-API-Key: $CORE_STACK_API_KEY" \
  "https://geoserver.core-stack.org/api/v1/get_layer_manifest/?state=bihar&district=nalanda&tehsil=hilsa"

# Get all active locations
curl -H "X-API-Key: $CORE_STACK_API_KEY" \
  "https://geoserver.core-stack.org/api/v1/get_layer_manifest/?all_active=true"

# Get manifest as CSV
curl -H "X-API-Key: $CORE_STACK_API_KEY" \
  "https://geoserver.core-stack.org/api/v1/get_layer_manifest/?all_active=true&format=csv" \
  -o core_stack_manifest.csv
```

Query parameters:
- `state`: Filter by state (optional)
- `district`: Filter by district (optional)
- `tehsil`: Filter by tehsil/block (optional)
- `all_active`: Return all active locations (optional, boolean)
- `format`: Output format - 'json' or 'csv' (default: json)

### 3. GeoNode Sync Utility: `utilities/geonode_sync.py`

A standalone script for registering Core-Stack layers into GeoNode via Remote Services.

```bash
# Register a single layer
python utilities/geonode_sync.py --layer "mws:mws_bihar_hilsa" \
  --geonode-url "https://geonode.core-stack.org"

# Register all layers from a manifest file
python utilities/geonode_sync.py --manifest data/manifests/nalanda_hilsa.json

# Export and sync all active locations
python utilities/geonode_sync.py --all-active \
  --api-key "$CORE_STACK_API_KEY" \
  --max-locations 25

# Dry run mode (show what would be synced)
python utilities/geonode_sync.py --manifest data/manifests/nalanda_hilsa.json --dry-run
```

Key features:
- Creates Remote Services in GeoNode (no data duplication)
- Supports single layer, manifest file, or API export modes
- Dry run mode for testing
- Uses GeoServer as the authoritative source

### 4. Dataset Model Enhancement

The `Dataset.misc` JSON field now supports both style formats:

```python
# Example: Setting both QML and SLD style URLs
dataset.misc = {
    "style_url": "https://geoserver.core-stack.org/styles/mws_bihar_hilsa.qml",
    "sld_url": "https://geoserver.core-stack.org/styles/mws_bihar_hilsa.sld",
}
dataset.save()
```

## Usage workflow


### Phase 1: Catalog what already exists (recommended starting point)

1. Export the manifest for all active locations:
   ```bash
   python manage.py export_manifest --all-active-locations \
     --output data/manifests/core_stack_catalog.json \
     --csv-output data/manifests/core_stack_layers.csv
   ```

2. Review the CSV in a spreadsheet to plan your GeoNode organization


3. Register the remote services in GeoNode using the sync utility:
   ```bash
   python utilities/geonode_sync.py --manifest data/manifests/core_stack_catalog.json
   ```

4. Test QGIS access by connecting to GeoNode or using direct OGC URLs

### Phase 2: Use the API for dynamic access

For applications that need dynamic layer information:

```python
import requests

api_key = os.environ.get("CORE_STACK_API_KEY")
base_url = "https://geoserver.core-stack.org/api/v1"

# Get all active locations' layers
response = requests.get(
    f"{base_url}/get_layer_manifest/",
    params={"all_active": "true"},
    headers={"X-API-Key": api_key}
)
manifest = response.json()

# Iterate over layers and use them
for layer in manifest["layers"]:
    print(f"{layer['dataset_name']}: {layer['wms_url']}")
```

## External references

GeoNode UI labels differ a bit across versions, but the remote-service, harvesting, and QGIS connection patterns are stable.

- GeoNode remote services: <https://docs.geonode.org/en/3.3.x/usage/managing_layers/using_remote_services.html>
- GeoNode harvesting from remote services: <https://docs.geonode.org/en/master/intermediate/harvesting/index.html>
- GeoNode QGIS connection flow: <https://docs.geonode.org/en/3.3.x/usage/other_apps/qgis/>
- GeoNode style upload notes: <https://docs.geonode.org/en/3.3.x/usage/managing_layers/layer_styling.html>
- GeoNode layer downloads: <https://docs.geonode.org/en/3.3.x/usage/managing_layers/layer_download.html>
- QGIS data source manager and layer-definition guidance: <https://docs.qgis.org/3.44/en/docs/user_manual/managing_data_source/opening_data.html>
