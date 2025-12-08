# Change in Well Depth Layer

## Description

Change in well depth data. Input data to compute change in well depth for each microwatershed is done using the vertical water balance and the specific yield obtained from aquifer data and water balance. 

**Important Note:** The water balance only takes the vertical flux into account and hence is likely to suffer from accuracy issues in the absence of accounting for lateral flow through the underlying aquifer.

Details of methodology can be found in the technical manual and the codebase on [GitHub](https://github.com/core-stack-org/core-stack-backend/blob/main/computing/mws/well_depth.py).

---

## Properties

### Identification
| Property | Type | Description |
|----------|------|-------------|
| uid | object | Unique MWS identifier |

### Area Metrics
| Property | Type | Description |
|----------|------|-------------|
| area_in_ha | float64 | Area in hectares |

### Aquifer Characteristics
| Property | Type | Description |
|----------|------|-------------|
| weighted_a | float64 | Weighted average yield of aquifer |

### Annual Well Depth Change
| Property | Type | Description |
|----------|------|-------------|
| 2017_2018 | object | Dictionary of water balance components |
| 2018_2019 | object | Dictionary of water balance components |
| 2019_2020 | object | Dictionary of water balance components |
| 2020_2021 | object | Dictionary of water balance components |
| 2021_2022 | object | Dictionary of water balance components |
| 2022_2023 | object | Dictionary of water balance components |
| 2023_2024 | object | Dictionary of water balance components |
| 2024_2025 | object | Dictionary of water balance components |

### Multi-Year Net Change
| Property | Type | Description |
|----------|------|-------------|
| Net2017_22 | float64 | Net value from 2017 to 2022 |
| Net2018_23 | float64 | Net value from 2018 to 2023 |
| Net2019_24 | float64 | Net value from 2019 to 2024 |
| Net2020_25 | float64 | Net value from 2020 to 2025 |

### Geometry
| Property | Type | Description |
|----------|------|-------------|
| geometry | geometry | Spatial geometry |

---