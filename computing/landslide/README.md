# Landslide Susceptibility Module

This module implements landslide susceptibility mapping for the CoRE Stack, following the methodology from:

**Paper**: [A comprehensive assessment of geospatial modelling techniques for landslide susceptibility mapping](https://www.sciencedirect.com/science/article/pii/S0341816223007440)

## Overview

The landslide susceptibility module provides:

1. **Tehsil-level clipping** of pan-India landslide susceptibility raster
2. **MWS-level vectorization** with detailed attributes
3. **Django API integration** for on-demand processing
4. **GEE visualization** tools for inspection and analysis
5. **Validation utilities** for quality assurance

## Architecture

### Workflow

```
Pan-India Landslide           Tehsil Boundary
Susceptibility Raster    +    (from MWS FC)
        |                            |
        v                            v
    ┌───────────────────────────────────┐
    │    Clip to Tehsil Boundary        │
    └───────────────┬───────────────────┘
                    v
    ┌───────────────────────────────────┐
    │  Vectorize at MWS Level           │
    │  - Compute area by class          │
    │  - Add slope, curvature, LULC     │
    │  - Calculate susceptibility score │
    └───────────────┬───────────────────┘
                    v
    ┌───────────────────────────────────┐
    │  Export to GEE Asset              │
    └───────────────┬───────────────────┘
                    v
    ┌───────────────────────────────────┐
    │  Sync to Database & GeoServer     │
    └───────────────────────────────────┘
```

### Files

- `landslide_vector.py` - Main processing logic (clipping, vectorization, export)
- `utils.py` - Utility functions (statistics, visualization parameters)
- `validation.py` - Quality assurance and validation
- `visualization.js` - GEE Code Editor script for visualization
- `README.md` - This file

## Susceptibility Classification

Based on the methodology paper, landslide susceptibility is classified into 4 classes:

| Class | Value | Description | Color (Viz) |
|-------|-------|-------------|-------------|
| Low | 1 | Minimal landslide risk | Green |
| Moderate | 2 | Moderate risk, monitoring recommended | Yellow |
| High | 3 | High risk, mitigation needed | Orange |
| Very High | 4 | Extreme risk, urgent action required | Red |

## Vector Attributes

Each MWS polygon includes:

### Susceptibility Areas
- `low_area_ha` - Area (hectares) in low susceptibility class
- `moderate_area_ha` - Area in moderate class
- `high_area_ha` - Area in high class
- `very_high_area_ha` - Area in very high class
- `total_area_ha` - Total MWS area

### Derived Metrics
- `susceptibility_score` - Weighted average score (1-4)
- `susceptibility_category` - Overall category (low/moderate/high/very_high)
- `mean_slope_deg` - Mean slope in degrees
- `mean_curvature` - Mean terrain curvature
- `dominant_lulc` - Dominant land use/land cover class

## Usage

### API Endpoint

Generate landslide susceptibility for a tehsil:

```bash
POST /computing/generate_landslide_layer/

{
  "state": "jharkhand",
  "district": "ranchi",
  "block": "ranchi",
  "gee_account_id": 1
}
```

Response:
```json
{
  "Success": "Landslide susceptibility generation initiated"
}
```

### Programmatic Usage

```python
from computing.landslide.landslide_vector import vectorise_landslide

# Generate landslide vectors
vectorise_landslide.apply_async(
    args=["jharkhand", "ranchi", "ranchi", 1],
    queue="nrm"
)
```

### GEE Code Editor Visualization

1. Open the Google Earth Engine Code Editor
2. Copy the contents of `visualization.js`
3. Update the asset path and AOI
4. Run the script to visualize landslide susceptibility

### Validation

```python
from computing.landslide.validation import generate_validation_report
import ee

ee.Initialize()

# Generate validation report
aoi = ee.Geometry.Point([85.3, 23.3]).buffer(50000)
report = generate_validation_report(
    asset_id="users/corestack/jharkhand_ranchi_landslide_vector",
    aoi=aoi,
    inventory_asset="users/corestack/landslide_inventory"  # Optional
)

print(report)
```

## Data Sources

### Primary Input
- **Pan-India Landslide Susceptibility**: `projects/ee-corestack/assets/india_landslide_susceptibility_100m`
  - Resolution: 100m
  - Coverage: India
  - Classes: 1-4 (Low to Very High)

### Ancillary Data
- **DEM**: USGS SRTM 30m (`USGS/SRTMGL1_003`)
- **LULC**: CoRE Stack LULC maps (10m) or Copernicus Global (100m)
- **MWS Boundaries**: CoRE Stack micro-watershed assets

### Optional
- **Historical Landslide Inventory**: For validation (if available)

## Configuration

### Asset Path

Update the pan-India landslide asset path in `landslide_vector.py`:

```python
LANDSLIDE_SUSCEPTIBILITY_ASSET = "projects/ee-corestack/assets/india_landslide_susceptibility_100m"
```

### Susceptibility Classes

Modify class definitions if needed:

```python
SUSCEPTIBILITY_CLASSES = {
    1: "low",
    2: "moderate",
    3: "high",
    4: "very_high"
}
```

## Quality Assurance

### Automated Checks

1. **Coverage Validation**: Ensures raster covers entire AOI
2. **Attribute Validation**: Verifies all required properties present
3. **Classification Validation**: Checks for valid class values
4. **Geometry Validation**: Ensures polygons are valid

### Manual Validation

1. **Visual Inspection**: Use GEE Code Editor visualization
2. **Spot Checks**: Compare with known landslide-prone areas
3. **Historical Comparison**: Validate against landslide inventory
4. **Field Validation**: Ground-truth high-risk areas (when possible)

## Outputs

### GEE Assets

- **Raster**: Clipped susceptibility map (if needed)
- **Vector**: MWS-level polygons with attributes

### GeoServer Layers

- Workspace: `{state}_workspace`
- Layer: `landslide_vector_{district}_{block}`

### Database Records

- Dataset: "Landslide Susceptibility"
- Layer: Includes metadata (methodology, resolution, classes)

## Performance

### Processing Time (Typical)

- Small tehsil (< 500 km²): 5-10 minutes
- Medium tehsil (500-1500 km²): 10-20 minutes
- Large tehsil (> 1500 km²): 20-45 minutes

### Optimization Tips

1. Use `bestEffort: true` for large areas
2. Process multiple tehsils in parallel
3. Cache intermediate results
4. Use appropriate scale (100m for susceptibility)

## Future Enhancements

### Short-term
- [ ] Integrate real pan-India landslide asset (when published)
- [ ] Add historical landslide inventory validation
- [ ] Implement sub-100m resolution mapping
- [ ] Add seasonal susceptibility variations

### Long-term
- [ ] Annual recomputation pipeline for dynamic factors
- [ ] Real-time susceptibility updates based on rainfall
- [ ] Integration with early warning systems
- [ ] Mobile app integration for field validation

## Research & Methodology

The landslide susceptibility methodology is based on:

1. **Topographic Factors**: Slope, aspect, curvature, elevation
2. **Hydrological Factors**: Flow accumulation, drainage density
3. **Land Cover**: Vegetation, built-up areas, bare soil
4. **Soil Properties**: Texture, moisture, composition
5. **Rainfall**: Annual precipitation, intensity patterns

### Weighted Model

Susceptibility score calculation:

```
Score = w₁·slope + w₂·curvature + w₃·flow_acc + w₄·lulc + w₅·rainfall
```

Where weights (w₁...w₅) are derived from the methodology paper.

## References

1. Mandal, K., et al. (2024). "A comprehensive assessment of geospatial modelling techniques for landslide susceptibility mapping." *Catena*, 234, 107440.
2. CoRE Stack Documentation: [Link to docs]
3. Google Earth Engine Guides: https://developers.google.com/earth-engine

## Support

For issues or questions:

1. Check existing LULC module for similar patterns
2. Review GEE documentation for vectorization
3. Contact mentors: @amanodt, @ankit-work7, @kapildadheech

## License

Same as CoRE Stack Backend (see root LICENSE file)
