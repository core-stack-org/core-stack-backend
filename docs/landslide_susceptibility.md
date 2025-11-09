# Landslide Susceptibility Mapping

## Overview

The landslide susceptibility module provides comprehensive landslide risk assessment for India at ~100m resolution. It follows the methodology from [Mandal et al. (2024)](https://www.sciencedirect.com/science/article/pii/S0341816223007440) and integrates seamlessly with the CoRE Stack backend.

## Key Features

- ✅ **Tehsil-level processing**: Clip pan-India susceptibility to administrative boundaries
- ✅ **MWS-level vectorization**: Field-level polygons with detailed attributes
- ✅ **Multi-class output**: Low, Moderate, High, Very High susceptibility zones
- ✅ **Rich attributes**: Slope, curvature, LULC, area metrics per MWS
- ✅ **GEE integration**: Leverage Earth Engine for scalable processing
- ✅ **Django REST API**: On-demand generation via HTTP endpoints
- ✅ **GeoServer publishing**: Automatic layer creation for web mapping
- ✅ **Validation tools**: Quality assurance and accuracy assessment

## Quick Start

### 1. Generate Landslide Susceptibility

**Via API:**

```bash
curl -X POST http://localhost/computing/generate_landslide_layer/ \
  -H "Content-Type: application/json" \
  -d '{
    "state": "jharkhand",
    "district": "ranchi",
    "block": "ranchi",
    "gee_account_id": 1
  }'
```

**Via Python:**

```python
from computing.landslide.landslide_vector import vectorise_landslide

vectorise_landslide.apply_async(
    args=["jharkhand", "ranchi", "ranchi", 1],
    queue="nrm"
)
```

### 2. Visualize in GEE Code Editor

Copy `computing/landslide/visualization.js` to the [Earth Engine Code Editor](https://code.earthengine.google.com/) and run it to visualize landslide susceptibility with an interactive map and legend.

### 3. Validate Outputs

```python
from computing.landslide.validation import generate_validation_report
import ee

ee.Initialize()

report = generate_validation_report(
    asset_id="users/corestack/jharkhand_ranchi_landslide_vector",
    aoi=ee.Geometry.Point([85.3, 23.3]).buffer(50000)
)

print(report)
```

## Data Products

### Raster Output

- **Format**: GeoTIFF or Earth Engine Image
- **Resolution**: 100m
- **Values**: 1 (Low), 2 (Moderate), 3 (High), 4 (Very High)
- **Coverage**: India (pan-India) or specific tehsils

### Vector Output (MWS Polygons)

Each micro-watershed (MWS) polygon includes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `low_area_ha` | Float | Area (ha) in low susceptibility class |
| `moderate_area_ha` | Float | Area (ha) in moderate class |
| `high_area_ha` | Float | Area (ha) in high class |
| `very_high_area_ha` | Float | Area (ha) in very high class |
| `total_area_ha` | Float | Total MWS area (ha) |
| `mean_slope_deg` | Float | Mean slope in degrees |
| `mean_curvature` | Float | Mean terrain curvature |
| `dominant_lulc` | Integer | Dominant land use/land cover class |
| `susceptibility_score` | Float | Weighted average score (1-4) |
| `susceptibility_category` | String | Overall category (low/moderate/high/very_high) |

## Methodology

### Data Inputs

1. **Topographic**
   - DEM: USGS SRTM 30m
   - Slope: Derived from DEM
   - Curvature: Second-order derivatives

2. **Land Cover**
   - CoRE Stack LULC (10m) or Copernicus Global (100m)

3. **Hydrological**
   - Flow accumulation (D8 algorithm)
   - Drainage density

4. **Climate**
   - Rainfall: CHIRPS or IMD data

5. **Soil**
   - Texture, moisture, composition (if available)

### Susceptibility Model

The susceptibility score is computed using a weighted linear combination:

```
Score = w₁·slope_norm + w₂·curv_norm + w₃·flow_norm + w₄·lulc_score + w₅·rain_norm
```

Where:
- `slope_norm`: Normalized slope (0-1)
- `curv_norm`: Normalized curvature (0-1)
- `flow_norm`: Normalized flow accumulation (0-1)
- `lulc_score`: LULC susceptibility mapping (0-1)
- `rain_norm`: Normalized rainfall (0-1)

Weights (w₁...w₅) are based on the methodology paper and can be configured.

### Classification Thresholds

| Class | Score Range | Risk Level |
|-------|-------------|------------|
| Low | 0.00 - 0.33 | Minimal risk |
| Moderate | 0.33 - 0.66 | Monitoring recommended |
| High | 0.66 - 0.85 | Mitigation needed |
| Very High | 0.85 - 1.00 | Urgent action required |

## Architecture

```
┌──────────────────────────────────────────────────────┐
│           Pan-India Landslide Asset                  │
│    (projects/ee-corestack/assets/india_ls_100m)      │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│         Tehsil Boundary (from MWS FC)                │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│              Clip to Tehsil                          │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│         Vectorize at MWS Level                       │
│  • Compute area by class (reduceRegions)            │
│  • Add slope, curvature attributes                  │
│  • Add LULC, rainfall metrics                       │
│  • Calculate susceptibility score                   │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│         Export to GEE Asset                          │
└──────────────────┬───────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────┐
│    Sync to Database & Publish to GeoServer          │
└──────────────────────────────────────────────────────┘
```

## API Reference

### Endpoints

#### `POST /computing/generate_landslide_layer/`

Generate landslide susceptibility vectors for a tehsil.

**Request:**
```json
{
  "state": "string",
  "district": "string",
  "block": "string",
  "gee_account_id": "integer"
}
```

**Response:**
```json
{
  "Success": "Landslide susceptibility generation initiated"
}
```

### Python Functions

#### `vectorise_landslide(state, district, block, gee_account_id)`

Main Celery task to generate landslide vectors.

**Parameters:**
- `state` (str): State name
- `district` (str): District name
- `block` (str): Block/Tehsil name
- `gee_account_id` (int): GEE account ID

**Returns:** bool - True if synced to GeoServer

#### `generate_demo_susceptibility()`

Generate demo susceptibility from slope (for testing when pan-India asset is unavailable).

**Returns:** ee.Image - Classified susceptibility

#### `validate_attributes(fc)`

Validate that required attributes are present.

**Parameters:**
- `fc` (ee.FeatureCollection): Collection to validate

**Returns:** dict - Validation results

## Configuration

### Asset Paths

Update in `computing/path_constants.py`:

```python
LANDSLIDE_SUSCEPTIBILITY_INDIA = "projects/ee-corestack/assets/india_landslide_susceptibility_100m"
```

### Weights

Modify in `landslide_vector.py` or pass via configuration:

```python
weights = {
    "slope": 0.4,
    "curvature": 0.1,
    "flow_acc": 0.2,
    "lulc": 0.15,
    "rainfall": 0.15
}
```

## Quality Assurance

### Automated Validation

The module includes comprehensive validation:

1. **Coverage Check**: Ensures raster covers entire AOI (>95%)
2. **Attribute Check**: Verifies all required properties present
3. **Classification Check**: Validates class values and distribution
4. **Geometry Check**: Ensures valid polygon geometry

### Manual Validation

1. **Visual Inspection**: Use GEE Code Editor visualization
2. **Spot Checks**: Compare with known landslide-prone areas
3. **Historical Comparison**: Validate against landslide inventory (if available)
4. **Field Validation**: Ground-truth high-risk areas

## Use Cases

### 1. Disaster Risk Management

Identify high-risk areas for:
- Early warning systems
- Evacuation planning
- Resource allocation

### 2. Land Use Planning

Inform:
- Development restrictions in high-risk zones
- Infrastructure siting decisions
- Conservation priorities

### 3. Climate Adaptation

Support:
- Climate change vulnerability assessment
- Nature-based solutions planning
- Community resilience building

### 4. Research & Monitoring

Enable:
- Landslide inventory mapping
- Temporal change analysis
- Model validation studies

## Performance

### Processing Times

| Tehsil Size | Typical Time |
|-------------|--------------|
| Small (< 500 km²) | 5-10 min |
| Medium (500-1500 km²) | 10-20 min |
| Large (> 1500 km²) | 20-45 min |

### Optimization Tips

- Use `bestEffort: true` for large areas
- Process multiple tehsils in parallel
- Cache intermediate DEM/slope results
- Use appropriate scale (100m for susceptibility)

## Troubleshooting

### Common Issues

**1. "Asset not found" error**

- Verify `LANDSLIDE_SUSCEPTIBILITY_INDIA` path in `path_constants.py`
- Check GEE asset permissions
- Falls back to demo generation from slope if unavailable

**2. "Task failed" in GEE**

- Check memory limits (reduce AOI or increase maxPixels)
- Verify MWS feature collection exists
- Check GEE quota/usage limits

**3. Missing attributes in output**

- Verify LULC asset exists for the tehsil
- Check DEM coverage (SRTM might have gaps)
- Review logs for reducer errors

## Future Enhancements

### Short-term
- [ ] Integrate published pan-India landslide asset
- [ ] Add historical landslide inventory validation
- [ ] Implement sub-100m resolution mapping
- [ ] Add seasonal susceptibility variations

### Long-term
- [ ] Annual recomputation for dynamic factors (rainfall, LULC change)
- [ ] Real-time updates based on rainfall triggers
- [ ] Integration with early warning systems
- [ ] Mobile app for field validation

## References

1. **Methodology Paper**:
   Mandal, K., et al. (2024). "A comprehensive assessment of geospatial modelling techniques for landslide susceptibility mapping." *Catena*, 234, 107440.
   https://www.sciencedirect.com/science/article/pii/S0341816223007440

2. **CoRE Stack Documentation**:
   See `docs/` folder for overall system architecture

3. **Google Earth Engine**:
   https://developers.google.com/earth-engine

## Support

For questions or issues:

1. Review the module README: `computing/landslide/README.md`
2. Check examples: `computing/landslide/examples.py`
3. Contact mentors: @amanodt, @ankit-work7, @kapildadheech

## License

Same as CoRE Stack Backend (see root LICENSE file)
