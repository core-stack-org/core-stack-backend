# Output Images - Landslide Susceptibility Mapping

This directory contains output images and test results from the landslide susceptibility mapping module.

## Contents

### Test Results
- `test_landslide_vector_output.txt` - landslide_vector.py validation results
- `test_tests_output.txt` - tests.py unit test results
- `test_utils_output.txt` - utils.py validation results
- `test_validation_output.txt` - validation.py validation results

### Sample Outputs
This folder is reserved for generated output images, maps, and visualization results from the landslide susceptibility processing pipeline.

## Example Output Data

When the landslide susceptibility module processes a tehsil (administrative block), it generates:

1. **Raster Output** (100m resolution, 4-class susceptibility map)
   - Low susceptibility (green)
   - Moderate susceptibility (yellow)
   - High susceptibility (orange)
   - Very high susceptibility (red)

2. **Vector Output** (MWS-level polygons with attributes)
   - Susceptibility class per polygon
   - Area by class (hectares)
   - Mean slope (degrees)
   - Mean curvature
   - LULC classification
   - Susceptibility score and category

3. **Visualization**
   - Interactive GEE map
   - Color-coded legend
   - Statistics charts
   - Export ready formats (GeoJSON, Shapefile)

## Processing Example

```bash
# Trigger landslide susceptibility generation via API
curl -X POST http://localhost/computing/generate_landslide_layer/ \
  -H "Content-Type: application/json" \
  -d '{
    "state": "jharkhand",
    "district": "ranchi",
    "block": "ranchi",
    "gee_account_id": 1
  }'

# Response:
# {"Success": "Landslide susceptibility generation initiated"}
```

## Output Location

- **Database**: Stored in `computing.Layer` model
- **GEE Assets**: Exported to `projects/ee-corestack/assets/india_landslide_susceptibility_100m`
- **GeoServer**: Published as `landslide_susceptibility:tehsil_name`
- **Files**: Downloaded as GeoJSON or Shapefile

## Quality Metrics

Each output is validated for:
- ✓ Coverage (>95% of AoI)
- ✓ Attributes (10 required fields)
- ✓ Classification (proper class distribution)
- ✓ Accuracy (validated against historical landslides)

## Version

Module Version: 1.0.0
Created: November 9, 2025
Status: Production Ready
