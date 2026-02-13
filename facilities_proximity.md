# Facilities Proximity Data Integration

This document describes the integration of facilities proximity data into the CoreStack backend application. The data contains village-level information about distances to various facilities across Indian states.

## Overview

The facilities proximity data provides distance information from each village to 24 different types of facilities, categorized into:

- **Agricultural Facilities**: Processing units, support infrastructure, co-operatives, dairy/animal husbandry, distribution utilities, industrial manufacturing, markets/trading, storage/warehousing, APMC markets
- **Healthcare Facilities**: CHC, district hospitals, PHC, sub-divisional hospitals, health sub-centers
- **Education Facilities**: Colleges, universities, and various types of schools (informal, private aided, market-driven, public advanced, public basic, comprehensive, selective admission, special interest/religious)

## Data Source

The data is stored as state-wise GeoJSON files in:
```
data/statewise_geojsons_facilities/
├── Karnataka.geojson
├── Maharashtra.geojson
├── Rajasthan.geojson
└── ... (other states)
```

Each GeoJSON file contains village features with properties including:
- `censuscode2011`: Village census code (2011)
- `name`: Village name
- `subdistrict` or `block`: Block/Subdistrict/Tehsil name (both column names are supported)
- `district`: District name
- `state`: State name
- 24 distance fields for various facilities (in kilometers)

## API Endpoints

All data endpoints support an `include-geometry` parameter. When set to `true`, the response includes geometry data (as GeoJSON FeatureCollection for lists, or with geometry field for single village), which can be directly used in QGIS or other mapping tools.

### 1. Get Available States

**Endpoint**: `GET /computing/facilities-available-states/`

Returns a list of states for which facilities proximity data is available.

**Response**:
```json
{
  "states": ["Karnataka", "Maharashtra", "Rajasthan"],
  "count": 3
}
```

### 2. Get Village Facilities Data

**Endpoint**: `GET /computing/facilities-village/`

**Query Parameters**:
- `state` (required): State name
- `censuscode` (optional): Village census code (2011) - use either this OR name-based lookup
- `district` (optional): District name (required for name-based lookup)
- `block` (optional): Block/Subdistrict name (required for name-based lookup)
- `village_name` (optional): Village name (required for name-based lookup)
- `include-geometry` (optional): Set to `true` to include geometry (default: `false`)

**Lookup Methods**:
1. **By Census Code**: Provide `state` and `censuscode`
2. **By Name**: Provide `state`, `district`, `block`, and `village_name`

**Response (without geometry)**:
```json
{
  "village_info": {
    "censuscode2011": 598009,
    "name": "Bhistenatti",
    "subdistrict": "Khanapur",
    "district": "Belagavi",
    "state": "Karnataka"
  },
  "facilities": {
    "health_phc_distance": {
      "distance_km": 8.87,
      "display_name": "Primary Health Center (PHC)",
      "category": "health"
    },
    "school_public_basic_education_distance": {
      "distance_km": 0.15,
      "display_name": "Public Basic Education School",
      "category": "education"
    }
    // ... other facilities
  }
}
```

**Response (with `include-geometry=true`)**:
```json
{
  "village_info": { ... },
  "facilities": { ... },
  "geometry": {
    "type": "Polygon",
    "coordinates": [...]
  }
}
```

### 3. Get Block Facilities Data

**Endpoint**: `GET /computing/facilities-block/`

**Query Parameters**:
- `state` (required): State name
- `block` (required): Block/Subdistrict/Tehsil name
- `district` (optional): District name for filtering
- `include-geometry` (optional): Set to `true` to return GeoJSON FeatureCollection (default: `false`)

**Response (without geometry)**:
```json
{
  "villages": [...],
  "summary": {
    "aggregated_stats": {
      "health_phc_distance": {
        "display_name": "Primary Health Center (PHC)",
        "category": "health",
        "min": 2.5,
        "max": 15.3,
        "mean": 7.8,
        "median": 7.2,
        "villages_within_5km": 45,
        "villages_within_10km": 120,
        "villages_within_20km": 150,
        "total_villages": 160
      }
    },
    "category_summary": {
      "health": {
        "facilities": [...],
        "avg_distance": 8.5,
        "total_villages": 160
      },
      "education": {...},
      "agriculture": {...}
    }
  },
  "total_villages": 160
}
```

**Response (with `include-geometry=true`)**: Returns a GeoJSON FeatureCollection that can be directly loaded into QGIS.

### 4. Get District Facilities Data

**Endpoint**: `GET /computing/facilities-district/`

**Query Parameters**:
- `state` (required): State name
- `district` (required): District name
- `include-geometry` (optional): Set to `true` to return GeoJSON FeatureCollection (default: `false`)

**Response**: Similar to block response with aggregated statistics for all villages in the district (summary only, no village list when without geometry).

### 5. Get MWS Facilities Data

**Endpoint**: `POST /computing/facilities-mws/`

**Request Body**:
```json
{
  "state": "Karnataka",
  "village_codes": [598009, 598010, 598011],
  "include-geometry": false
}
```

**Parameters**:
- `state` (required): State name
- `village_codes` (required): List of village census codes
- `include-geometry` (optional): Set to `true` to return GeoJSON FeatureCollection (default: `false`)

**Response**: Returns village-level data and aggregated statistics for the specified village codes, or GeoJSON FeatureCollection if `include-geometry` is `true`.

## Integration with Reports

The facilities proximity data is automatically integrated into:

### Village-Level Reports

When generating village indicators, the following fields are added:
- Individual facility distances: `dist_health_phc`, `dist_school_public_basic_education`, etc.
- Category averages: `avg_dist_health`, `avg_dist_education`, `avg_dist_agriculture`
- Accessibility metrics: `facilities_within_5km`, `facilities_within_10km`, `facilities_within_20km`

### MWS-Level Reports

When generating MWS indicators, the following aggregated fields are added:
- Average, min, max distances per facility type
- Percentage of villages within 5km/10km of each facility
- Category-wise average distances

## Usage Examples

### Python

```python
from computing.misc.facilities_proximity import (
    facilities_data,
    get_facilities_summary,
)

# Get village facilities by census code (without geometry)
village_data = facilities_data.get_village_facilities("Karnataka", censuscode=598009)

# Get village facilities by name (with geometry)
village_data = facilities_data.get_village_facilities(
    state="Karnataka",
    district="Belagavi",
    block="Khanapur",
    village_name="Bhistenatti",
    include_geometry=True
)

# Get block data (without geometry)
villages = facilities_data.get_villages_by_block(
    "Karnataka", "Khanapur", "Belagavi"
)

# Get block data with geometry (for QGIS)
villages_with_geom = facilities_data.get_villages_by_block(
    "Karnataka", "Khanapur", "Belagavi", include_geometry=True
)

# Get aggregated statistics
summary = get_facilities_summary(villages)

# Get GeoJSON directly
geojson = facilities_data.get_facilities_geojson(
    state="Karnataka",
    district="Belagavi",
    block="Khanapur"
)
```

### cURL

```bash
# Get available states
curl -X GET "http://localhost:8000/computing/facilities-available-states/"

# Get village facilities by census code (without geometry)
curl -X GET "http://localhost:8000/computing/facilities-village/?state=Karnataka&censuscode=598009"

# Get village facilities with geometry
curl -X GET "http://localhost:8000/computing/facilities-village/?state=Karnataka&censuscode=598009&include-geometry=true"

# Get village facilities by name
curl -X GET "http://localhost:8000/computing/facilities-village/?state=Karnataka&district=Belagavi&block=Khanapur&village_name=Bhistenatti"

# Get block facilities (without geometry)
curl -X GET "http://localhost:8000/computing/facilities-block/?state=Karnataka&block=Khanapur&district=Belagavi"

# Get block facilities as GeoJSON (for QGIS)
curl -X GET "http://localhost:8000/computing/facilities-block/?state=Karnataka&block=Khanapur&include-geometry=true"

# Get district facilities as GeoJSON
curl -X GET "http://localhost:8000/computing/facilities-district/?state=Karnataka&district=Belagavi&include-geometry=true"

# Get MWS facilities (without geometry)
curl -X POST "http://localhost:8000/computing/facilities-mws/" \
  -H "Content-Type: application/json" \
  -d '{"state": "Karnataka", "village_codes": [598009, 598010]}'

# Get MWS facilities as GeoJSON
curl -X POST "http://localhost:8000/computing/facilities-mws/" \
  -H "Content-Type: application/json" \
  -d '{"state": "Karnataka", "village_codes": [598009, 598010], "include-geometry": true}'
```

## Using with QGIS

To load facilities data into QGIS:

1. **For a block**: 
   ```
   http://localhost:8000/computing/facilities-block/?state=Karnataka&block=Devadurga&include-geometry=true
   ```

2. **For a district**: 
   ```
   http://localhost:8000/computing/facilities-district/?state=Karnataka&district=Raichur&include-geometry=true
   ```

3. **For MWS villages**:
   ```
   POST to: http://localhost:8000/computing/facilities-mws/
   Body: {"state": "Karnataka", "village_codes": [...], "include-geometry": true}
   ```

In QGIS: Layer → Add Layer → Add Vector Layer → Protocol: HTTP, enter the URL

## Facility Categories and Fields

### Agricultural Facilities
| Field | Display Name |
|-------|--------------|
| `agri_industry_agri_processing_distance` | Agricultural Processing Unit |
| `agri_industry_agri_support_infrastructure_distance` | Agricultural Support Infrastructure |
| `agri_industry_co_operatives_societies_distance` | Co-operative Societies |
| `agri_industry_dairy_animal_husbandry_distance` | Dairy/Animal Husbandry |
| `agri_industry_distribution_utilities_distance` | Distribution Utilities |
| `agri_industry_industrial_manufacturing_distance` | Industrial Manufacturing |
| `agri_industry_markets_trading_distance` | Markets/Trading Centers |
| `agri_industry_storage_warehousing_distance` | Storage/Warehousing |
| `apmc_distance` | APMC Market |

### Healthcare Facilities
| Field | Display Name |
|-------|--------------|
| `health_chc_distance` | Community Health Center (CHC) |
| `health_dis_h_distance` | District Hospital |
| `health_phc_distance` | Primary Health Center (PHC) |
| `health_s_t_h_distance` | Sub-Divisional Hospital |
| `health_sub_cen_distance` | Health Sub-Center |

### Education Facilities
| Field | Display Name |
|-------|--------------|
| `college_distance` | College |
| `universities_distance` | University |
| `school_informal_unrecognized_distance` | Informal/Unrecognized School |
| `school_private_aided_distance` | Private Aided School |
| `school_private_market_driven_distance` | Private Market-Driven School |
| `school_public_advanced_education_distance` | Public Advanced Education |
| `school_public_basic_education_distance` | Public Basic Education School |
| `school_public_comprehensive_distance` | Public Comprehensive School |
| `school_public_selective_addmision_distance` | Public Selective Admission School |
| `school_special_interest_religious_distance` | Special Interest/Religious School |

## Error Handling

All API endpoints return appropriate HTTP status codes:
- `200 OK`: Successful request
- `400 Bad Request`: Missing required parameters
- `404 Not Found`: No data found for the specified parameters
- `500 Internal Server Error`: Server-side error

Error responses include an `error` field with a descriptive message:
```json
{
  "error": "Either 'censuscode' OR ('village_name', 'district', 'block') parameters are required"
}
```

## Performance Considerations

- State GeoJSON files are cached in memory after first load
- For large districts, consider using the summary endpoints instead of full village listings
- MWS queries are optimized for batch processing of village codes
- Use `include-geometry=true` only when you need to visualize the data in GIS tools

## Testing

Test scripts are provided in the `tests/` directory:

### Python Test Script
```bash
# Activate conda environment
conda activate corestack-backend

# Run tests
python tests/test_facilities_api.py
```

### Bash Test Script (WSL)
```bash
# Make executable
chmod +x tests/test_facilities_api.sh

# Run tests
./tests/test_facilities_api.sh
```

## Column Name Compatibility

The module handles both `subdistrict` and `block` column names in the GeoJSON files. If your data uses `block` instead of `subdistrict`, the module will automatically detect and use the correct column. The API consistently uses `block` as the parameter name for clarity.

## Celery Integration

The facilities proximity module includes Celery tasks for asynchronous processing, following the patterns established in other `computing/misc` modules.

### Available Tasks

#### 1. `generate_facilities_layer`

Generates facilities proximity layer for a given administrative area and optionally syncs to GeoServer.

```python
from computing.misc.facilities_proximity import generate_facilities_layer

# Generate for a block (async)
result = generate_facilities_layer.delay(
    state="Karnataka",
    district="Raichur",
    block="Devadurga",
    sync_to_geoserver=True
)

# Check task status
task_id = result.id
status = result.status  # PENDING, STARTED, SUCCESS, FAILURE

# Get result (blocks until complete)
layer_info = result.get()
```

**Parameters**:
- `state` (required): State name
- `district` (optional): District name
- `block` (optional): Block name
- `gee_account_id` (optional): GEE account ID (placeholder for future GEE integration)
- `sync_to_geoserver` (optional): Whether to sync to GeoServer (default: `True`)

**Returns**:
```json
{
  "status": "success",
  "state": "Karnataka",
  "district": "Raichur",
  "block": "Devadurga",
  "layer_at_geoserver": true,
  "villages_count": 150,
  "output_path": "/path/to/output/facilities_raichur_devadurga.geojson"
}
```

#### 2. `generate_facilities_report`

Generates facilities proximity report with aggregated statistics.

```python
from computing.misc.facilities_proximity import generate_facilities_report

# Generate summary report for a block
result = generate_facilities_report.delay(
    state="Karnataka",
    district="Raichur",
    block="Devadurga",
    report_type="summary"
)

# Generate detailed report for MWS
result = generate_facilities_report.delay(
    state="Karnataka",
    village_codes=[624783, 624784, 624785],
    report_type="detailed"
)
```

**Parameters**:
- `state` (required): State name
- `district` (optional): District name
- `block` (optional): Block name
- `village_codes` (optional): List of village codes for MWS level
- `report_type` (optional): "summary", "detailed", or "geojson" (default: "summary")

### Integration Pattern

The Celery tasks follow the same pattern as other `computing/misc` modules:

1. **Task Definition**: Uses `@app.task(bind=True)` decorator
2. **Parameters**: Accepts `state`, `district`, `block`, `gee_account_id` like other modules
3. **Return Value**: Returns a dictionary with status and metadata
4. **GeoServer Sync**: Optional sync to GeoServer for visualization
5. **Future GEE Integration**: Placeholder functions for GEE export

### Future Enhancements (Placeholders)

The following functions are hashed out as placeholders for future integration:

```python
# Export to Google Earth Engine
# def _export_to_gee(geojson, state, district, block, gee_account_id):
#     """Export facilities layer to Google Earth Engine."""
#     pass

# Clip from admin boundary
# def _clip_from_admin_boundary(state, district, block):
#     """Clip facilities data from admin boundary layer."""
#     pass

# Integrate with other layers
# def _integrate_with_other_layers(state, district, block, facilities_geojson):
#     """Integrate facilities data with other layers (drainage, roads, etc.)."""
#     pass
```

### Output Directory

Generated files are saved to:
```
data/facilities_output/
├── Karnataka/
│   ├── facilities_raichur_devadurga.geojson
│   ├── facilities_raichur.geojson
│   └── facilities_karnataka.geojson
└── ...
```

### GeoServer Workspace

Layers are published to the `facilities` workspace in GeoServer with layer names like:
- `facilities_raichur_devadurga` (block level)
- `facilities_raichur` (district level)
- `facilities_karnataka` (state level)
