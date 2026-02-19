# Natural Depression Module

**File:** [`computing/misc/naturaldepression.py`](../../computing/misc/naturaldepression.py)

## Overview

This module generates **natural depression raster layers** by clipping pan-India depression data to specific block boundaries. Natural depressions are important for water harvesting and groundwater recharge planning.

## Purpose

Processes natural depression data for:
- Water harvesting site identification
- Groundwater recharge potential assessment
- Watershed management planning
- Distributes to GEE, GeoServer (via GCS), and database

## Architecture

```mermaid
flowchart TD
    A[Celery Task Triggered] --> B[ee_initialize]
    B --> C[Build Description & Asset ID]
    C --> D[Load MWS ROI]
    D --> E[Load Pan-India Depression Raster]
    E --> F[Clip to ROI]
    F --> G[Call Raster Generation Function]
    G --> H{Asset Exists?}
    H -->|No| I[Export Raster to GEE]
    I --> J[Check Task Status]
    J --> K[Sync to GCS]
    K --> L[Sync GCS to GeoServer]
    L --> M[save_layer_info_to_db]
    M --> N[make_asset_public]
    N --> O{Success?}
    O -->|Yes| P[Update Sync Status]
    P --> Q[Return True]
    H -->|Yes| R[Skip Export]
    R --> K
```

## Components

### Main Task: `generate_natural_depression_data()`

**Location:** Line 22

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `state` | str | State name |
| `district` | str | District name |
| `block` | str | Block/tehsil name |
| `gee_account_id` | int | GEE account identifier |

**Returns:** `bool` - Whether layer was successfully synced to GeoServer

### Helper Function: `natural_depression_raster_generation()`

**Location:** Line 47

Handles the raster export and synchronization logic.

## Processing Logic

### 1. ROI Definition
```python
roi = ee.FeatureCollection(
    get_gee_asset_path(state, district, block)
    + "filtered_mws_"
    + valid_gee_text(district.lower())
    + "_"
    + valid_gee_text(block.lower())
    + "_uid"
)
```

### 2. Data Source
```python
natural_depression = ee.Image(NATURAL_DEPRESSION)  # From constants.pan_india_urls
```

### 3. Raster Processing
```python
raster = natural_depression.clip(roi.geometry())
```

## Integration Points

```
computing/misc/naturaldepression.py
├── computing.utils
│   ├── save_layer_info_to_db()   # Database persistence
│   └── update_layer_sync_status() # Status tracking
├── utilities.gee_utils
│   ├── ee_initialize()           # GEE authentication
│   ├── check_task_status()       # Task monitoring
│   ├── valid_gee_text()          # Text sanitization
│   ├── get_gee_asset_path()      # Asset path generation
│   ├── is_gee_asset_exists()     # Asset existence check
│   ├── sync_raster_to_gcs()      # GCS synchronization
│   ├── sync_raster_gcs_to_geoserver() # GeoServer sync
│   ├── export_raster_asset_to_gee() # Raster export
│   └── make_asset_public()       # ACL management
└── constants.pan_india_urls
    └── NATURAL_DEPRESSION        # Pan-India depression URL
```

## Output

| Platform | Asset/Layer Name | Workspace |
|----------|------------------|-----------|
| GEE | `natural_depression_{district}_{block}_raster` | N/A |
| GeoServer | `natural_depression_{district}_{block}_raster` | `natural_depression` |

**Dataset Name:** `Natural Depression`

**Style Name:** `natural_depression`

## Usage

```python
from computing.misc.naturaldepression import generate_natural_depression_data

result = generate_natural_depression_data.delay(
    state="Rajasthan",
    district="Jaipur",
    block="Sanganer",
    gee_account_id=1
)
```

## Pattern Classification

This module follows the **Simple Raster Clip Pattern** - identical to:
- `distancetonearestdrainage.py`
- `slope_percentage.py`
- `catchment_area.py`

## Dependencies

- **ee** (Google Earth Engine Python API)
- **Celery** - Distributed task queue
