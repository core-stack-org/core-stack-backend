# Landslide Susceptibility - Quick Reference

## Quick Start (3 Steps)

### 1. Generate Susceptibility for a Tehsil

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

### 2. Monitor Progress

Check GEE Tasks tab or Django admin panel for task status.

### 3. Visualize Results

Copy `computing/landslide/visualization.js` to [Earth Engine Code Editor](https://code.earthengine.google.com/) and run.

---

## Common Tasks

### Run from Python

```python
from computing.landslide.landslide_vector import vectorise_landslide

vectorise_landslide.apply_async(
    args=["jharkhand", "ranchi", "ranchi", 1],
    queue="nrm"
)
```

### Validate Outputs

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

### Get Statistics

```python
from computing.landslide.utils import get_susceptibility_statistics
import ee

ee.Initialize()

fc = ee.FeatureCollection("users/corestack/jharkhand_ranchi_landslide_vector")
stats = get_susceptibility_statistics(fc)

print(f"Total MWS: {stats['total_mws']}")
print(f"High risk area: {stats['area_by_class']['high']:.2f} ha")
```

---

## File Locations

| Component | Path |
|-----------|------|
| Main processing | `computing/landslide/landslide_vector.py` |
| Utilities | `computing/landslide/utils.py` |
| Validation | `computing/landslide/validation.py` |
| GEE visualization | `computing/landslide/visualization.js` |
| Tests | `computing/landslide/tests.py` |
| Examples | `computing/landslide/examples.py` |
| Module docs | `computing/landslide/README.md` |
| System docs | `docs/landslide_susceptibility.md` |
| API endpoint | `computing/api.py` (line ~1200) |
| URL route | `computing/urls.py` (line ~115) |

---

## Key Functions

### `vectorise_landslide(state, district, block, gee_account_id)`
Main Celery task. Generates landslide vectors for a tehsil.

### `generate_demo_susceptibility()`
Fallback: creates demo susceptibility from slope when pan-India asset unavailable.

### `validate_attributes(fc)`
Checks if all required attributes are present in the output.

### `compute_high_risk_percentage(fc)`
Calculates percentage of area in high/very high zones.

---

## Output Attributes

Each MWS polygon has:

- `low_area_ha` - Area (ha) in low susceptibility
- `moderate_area_ha` - Area (ha) in moderate susceptibility
- `high_area_ha` - Area (ha) in high susceptibility
- `very_high_area_ha` - Area (ha) in very high susceptibility
- `total_area_ha` - Total MWS area (ha)
- `mean_slope_deg` - Mean slope (degrees)
- `mean_curvature` - Mean terrain curvature
- `dominant_lulc` - Dominant LULC class
- `susceptibility_score` - Weighted score (1-4)
- `susceptibility_category` - Overall category (low/moderate/high/very_high)

---

## Classification

| Class | Value | Score Range | Color | Action |
|-------|-------|-------------|-------|--------|
| Low | 1 | 0.00-0.33 | Green | None |
| Moderate | 2 | 0.33-0.66 | Yellow | Monitor |
| High | 3 | 0.66-0.85 | Orange | Mitigate |
| Very High | 4 | 0.85-1.00 | Red | Urgent |

---

## Configuration

### Update Pan-India Asset Path

In `computing/landslide/landslide_vector.py`:

```python
LANDSLIDE_SUSCEPTIBILITY_ASSET = "projects/YOUR_PROJECT/assets/india_landslide"
```

Or in `computing/path_constants.py`:

```python
LANDSLIDE_SUSCEPTIBILITY_INDIA = "projects/YOUR_PROJECT/assets/india_landslide"
```

---

## Troubleshooting

### "Asset not found"
- Check asset path in `landslide_vector.py`
- Verify GEE permissions
- Falls back to demo generation automatically

### Task fails in GEE
- Check memory limits (reduce AOI or increase maxPixels)
- Verify MWS FC exists
- Check GEE quota

### Missing attributes
- Verify LULC asset exists
- Check DEM coverage
- Review reducer logs

---

## Testing

```bash
# Run unit tests
python -m unittest computing.landslide.tests

# Run specific test
python -m unittest computing.landslide.tests.TestLandslideVectorization

# Run examples
python computing/landslide/examples.py
```

---

## Resources

- **Module README**: `computing/landslide/README.md`
- **Full docs**: `docs/landslide_susceptibility.md`
- **Implementation summary**: `LANDSLIDE_IMPLEMENTATION.md`
- **Examples**: `computing/landslide/examples.py`
- **Paper**: https://www.sciencedirect.com/science/article/pii/S0341816223007440

---

## Support

- GitHub Issues: https://github.com/core-stack-org/core-stack-backend/issues
- Mentors: @amanodt, @ankit-work7, @kapildadheech
- CoRE Stack Docs: `docs/` folder

---

**Last Updated**: November 9, 2025  
**Version**: 1.0.0
