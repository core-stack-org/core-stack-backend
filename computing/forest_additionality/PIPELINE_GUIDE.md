# Forest Additionality Pipeline Guide

This guide traces the forest-additionality workflow implemented in this
directory, beginning with `main.py`. It also explains the UDef-ARP terms used
by the code and records how the implementation relates to VT0007 *Unplanned
Deforestation Allocation v1.0* and the supplied biomass-estimation paper.

## Purpose

The pipeline builds a spatial baseline of expected unplanned deforestation. A
binary forest-cover benchmark map (FCBM) is used to measure past forest loss;
UDef-ARP then allocates an expected quantity of future loss across eligible
forest pixels. The resulting density map can be summed within a project area
to obtain expected deforestation in hectares.

The main entry point is `forest_additionality(start_year, mid_pt, end_year,
state_name)` in `main.py`.

For a common example, the three years have the following UDef-ARP meaning:

| Code variable | UDef-ARP label | Example | Meaning |
|---|---|---:|---|
| `start_year` | T1 | 2005 | Beginning of the historical/calibration record |
| `mid_pt` | T2 | 2010 | End of calibration and start of confirmation |
| `end_year` | T3 | 2015 | End of confirmation and start of the validity period |

## End-to-end flow

```text
main.forest_additionality(T1, T2, T3, jurisdiction)
  |
  +-- GEE: create/download binary forest maps at T1, T2, T3
  +-- GEE: create/download jurisdiction and administrative-division rasters
  |
  +-- RiskMaps.prepare_data()
  |     +-- reproject/resample all inputs to the 25 m analysis grid
  |     +-- create distance-to-forest-edge maps
  |     +-- create binary deforestation maps for T1-T2, T1-T3, T2-T3
  |
  +-- RiskMaps.run_udefarp()
  |     +-- calibration fitting and confirmation prediction (testing stage)
  |     +-- historical-reference fitting and validity-period prediction
  |
  +-- generate_afforestation_mask()
  +-- get_area_estimation()
```

`main.py` calls `perform_gee_operations()` explicitly and then calls
`run_wo_gee()`. Despite its name, `run_wo_gee()` is appropriate here: the GEE
exports have already been generated/downloaded, and it proceeds with
analysis-grid preparation and UDef-ARP processing.

## 1. Earth Engine inputs

`GEE_Manager.create_forest_cover_map()` reads the GLC-FCS30D annual land-cover
collection. It remaps the forest classes to a binary raster:

```text
1 = forest
0 = non-forest
```

It clips the map to the selected GAUL jurisdiction. The maps and jurisdiction
layers are exported through Cloud Storage in EPSG:4326 at the requested GEE
scale. The export includes a rectangular raster extent; pixels beyond the
jurisdiction or outside forest are handled later through masks and value-based
eligibility rules.

Forest classes are explicitly listed in `create_forest_cover_map()`. This is
important because the source is a multi-class land-cover collection, whereas
the rest of the pipeline expects the binary forest/non-forest convention.

## 2. Analysis-grid data preparation

`RiskMaps.prepare_data()` performs the following work.

### Reproject and resample

`GEEManager.resample_raster()` reprojects EPSG:4326 source rasters to the
hard-coded EPSG:32644 UTM zone and resamples them to 25 m by 25 m using nearest
neighbour resampling. The resulting pixel area is:

```text
25 m x 25 m / 10,000 = 0.0625 ha per pixel
```

All inputs used together by UDef-ARP must share this exact CRS, transform and
shape. EPSG:32644 is suitable for the current Odisha/Dhenkanal example; it
must be chosen dynamically or configured for jurisdictions in other UTM zones.

Nearest-neighbour resampling preserves categorical values, so it is appropriate
for binary forest maps, deforestation labels, jurisdiction masks and district
identifiers. It is not an area-conserving operation for a deforestation-density
map: density values are already expressed in hectares per pixel and their sum
depends on the grid on which they were created.

### Forest-edge distance maps

`euclidean_dist_calc()` creates distance rasters for forest conditions at T1,
T2 and T3. Each cell records distance in metres to the relevant forest edge.
These maps are inputs to vulnerability classification.

### Deforestation maps

`generate_deforestation_map(earlier, later, output)` calculates:

```text
deforestation = (forest at earlier year == 1) AND
                (forest at later year == 0)
```

It produces three binary maps:

| File | Period | Role |
|---|---|---|
| `deforestation_map_T1_T2.tif` | T1 to T2 | Calibration observed loss |
| `deforestation_map_T1_T3.tif` | T1 to T3 | Historical Reference Period (HRP) loss |
| `deforestation_map_T2_T3.tif` | T2 to T3 | Confirmation observed loss |

The transition requires forest at the beginning of the interval. A pixel that
is non-forest at the beginning and remains non-forest is not counted as
deforestation, and a non-forest-to-forest transition is handled separately by
`generate_afforestation_mask()` after the risk-map workflow completes.

## 3. UDef-ARP terminology

### HRP — Historical Reference Period

The HRP is the historical period used to characterize long-term deforestation
and establish a baseline. In this implementation it is T1 to T3. Its observed
deforestation map is used to calculate NRT and to fit the application-stage
model.

### CAL — Calibration period

The calibration period is T1 to T2. It is used in the testing stage to fit the
benchmark and alternative models.

### CNF — Confirmation period

The confirmation period is T2 to T3. It is held out from calibration and is
used to test prediction quality. `Acre_Adjucted_Density_Map_CNF.tif` is the
appropriate UDef-ARP map to compare with observed `deforestation_map_T2_T3`.

### VP / BVP — Validity Period / Baseline Validity Period

The VP begins at T3. The VP map is the forward-looking allocated-risk baseline
for a future period, not a prediction map to validate against T1--T2 or
T2--T3 observations. In the code this is
`Acre_Adjucted_Density_Map_VP.tif`.

### NRT — Negligible Risk Threshold

NRT is the distance from non-forest/forest edge beyond which deforestation is
considered negligible. VT0007 defines it as the distance at which the
cumulative histogram of observed HRP deforestation reaches or just exceeds
99.5%. `RMT_FIT_CAL.calculate_nrt()` calculates it using the T1 distance map,
the HRP deforestation map and the jurisdiction mask. The same threshold is
then reused to classify vulnerability for CAL, CNF, HRP and VP.

In practical terms, NRT limits the part of the jurisdiction for which a
distance-based deforestation-risk relationship is established. Pixels farther
from the edge than the threshold are considered to have negligible unplanned
deforestation risk in this benchmark formulation.

### Vulnerability map and modelling regions

A vulnerability map assigns risk classes based on distance from the forest
edge, with zero for excluded/non-forest locations. The pipeline combines that
class with an administrative-division raster to form modelling regions. Each
modelling region has a relative frequency of observed deforestation, which is
used as a fitted frequency or prediction probability.

The distance classes are geometric: classes are narrow near the forest edge,
where deforestation risk is expected to be greatest, and become progressively
wider farther from the edge. Combining a vulnerability class with an
administrative code enables different districts to retain different historical
deforestation frequencies at the same distance class.

### Density map

Relative frequency/probability is dimensionless. UDef-ARP converts it to
deforestation density by multiplying by pixel area. At 25 m, the maximum
density is 0.0625 ha/pixel for a whole-period map. The final application VP
map is an annual density map (`ha/pixel/year`) after conversion to an annual
rate. Values in a density map must be summed directly; do not multiply them by
pixel area again.

### Quantity adjustment

UDef-ARP compares expected deforestation (ED) with modelled deforestation
(MD), the sum of the density-map cells:

```text
adjustment ratio (AR) = ED / MD
adjusted density      = AR x prediction density
```

Values above the maximum pixel density are capped and the process is repeated
until the target quantity is reached. VT0007 requires this adjustment both
when AR is greater than one and when it is less than one.

## 4. Implemented UDef-ARP stages

`RiskMaps.run_udefarp()` executes four stages.

| Code method | UDef-ARP role | Principal outputs |
|---|---|---|
| `testing_stage_fitting_phase_CAL()` | Fit test, calibration period | `Vulnerability_Map_CAL.tif`, fitted CAL density and frequency table |
| `testing_stage_prediction_phase_cnf()` | Prediction test, confirmation period | `Vulnerability_Map_CNF.tif`, `Acre_Adjucted_Density_Map_CNF.tif` |
| `application_stage_fitting_phase_HRP()` | Fit baseline over HRP | `Vulnerability_Map_HRP.tif`, HRP density and frequency table |
| `application_stage_prediction_phase_VP()` | Allocate forward baseline risk | `Acre_Vulnerability_VP.tif`, `Acre_Adjucted_Density_Map_VP.tif` |

The allocation tool outputs density rasters. It uses the administrative
divisions and vulnerability classes to assign the fitted relative frequencies
to each prediction modelling region, then applies the quantity adjustment.

During fitting, the observed binary deforestation values are summarized within
each modelling region. The mean of those values is its relative frequency of
deforestation. During prediction, that table is assigned to the corresponding
new modelling regions, converted to hectares per pixel, and adjusted to the
expected jurisdiction-wide quantity.

## 5. Correct area estimation and validation

### Maintain a common analysis grid

For a 25 m UDef-ARP run, observed deforestation, forest eligibility,
jurisdiction and prediction-density rasters used in the same calculation must
have the same EPSG:32644 CRS, transform and dimensions. This permits direct
cell-by-cell masking and preserves the meaning of a density value as hectares
per pixel. Reprojecting a `ha/pixel` density map to a different grid with
nearest neighbour and summing it changes the reported quantity.

Pixel area should always be derived from the raster transform:

```python
pixel_area_ha = abs(src.transform.a * src.transform.e) / 10_000
```

### Evaluate only eligible forest

For a prediction test over T2 to T3, the evaluation mask is:

```text
eligible = (forest map at T2 == 1)
           AND (inside jurisdiction)
           AND (prediction is finite and not nodata)
```

This avoids treating non-forest cells, outside-jurisdiction cells and the
rectangular bounding-box background as exposure. `area_estimation.py` now
implements this mask and checks that its four rasters have the same grid.

### Correct periods for 2005, 2010, 2015

| Test | Predicted map | Observed map | Eligibility forest map |
|---|---|---|---|
| Fit test | `Acre_Fitted_Density_Map_CAL.tif` | `deforestation_map_2005_2010.tif` | `state_2005.tif` |
| Prediction test | `Acre_Adjucted_Density_Map_CNF.tif` | `deforestation_map_2010_2015.tif` | `state_2010.tif` |
| Application | `Acre_Adjucted_Density_Map_VP.tif` | Future monitored loss only, when available | `state_2015.tif` |

For the testing-stage prediction map, VT0007 specifies setting ED to the
actual observed T2--T3 deforestation. Consequently, its jurisdiction-wide
total is quantity-adjusted by design. The meaningful model-selection test is
spatial allocation: aggregate observed and predicted hectares to the required
coarse grid and calculate median absolute error (MedAE), not just a single
jurisdiction-wide difference.

## 6. Current implementation notes

- `area_estimation.py` presently selects the VP density map while selecting
  the T2--T3 observed map. For the VT0007 prediction test, change the active
  UDef-ARP prediction path to `Acre_Adjucted_Density_Map_CNF.tif`; VP should be
  retained for forward baseline reporting.
- The current CNF quantity-adjustment loop only performs adjustment while
  `AR > 1.00001`. It should also apply the initial adjustment when `AR < 1`;
  otherwise an underprediction is left unadjusted.
- The GEE export and `resample_raster()` should be reviewed whenever the AOI is
  outside UTM zone 44N.
- The spelling `jurisidiction` is part of the current filenames and must remain
  consistent until a coordinated rename is made.

## Sources used for this guide

1. *VT0007-Unplanned-Deforestation-Allocation v1.0*: Sections 5.3--5.6
   (fitting, prediction, quantity adjustment, testing and allocated risk) and
   data-parameter descriptions for NRT and ED.
2. *Evaluating Deforestation Baselines for REDD+* (the supplied
   `biomass_estimation_jayesh.pdf`): its T1/T2/T3-style temporal evaluation,
   distinction between ex-ante and ex-post approaches, and comparison of
   predicted with observed deforestation over a common eligible forest extent.
3. The implementation in `main.py`, `FinalClass.py`, `GEE_Manager.py`,
   `AllocationTool.py`, `area_estimation.py`, and the `RMT_*` / `AT_*` modules.
