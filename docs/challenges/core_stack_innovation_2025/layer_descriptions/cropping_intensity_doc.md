# Cropping Intensity Layer

## Description

Cropping intensity data. Cropping intensity is computed on the IndiaSAT Land Use Land Cover outputs by taking the weighted average of the area under single cropping, double cropping, and triple/annual/perennial cropping. The vector map is available from 2017 onwards.

Details of methodology can be found in the technical manual and the codebase on [GitHub](https://github.com/core-stack-org/core-stack-backend/tree/main/computing/cropping_intensity).

---

## Properties

### Identification
| Property | Type | Description |
|----------|------|-------------|
| uid | object | Unique MWS identifier |

### Area Metrics
| Property | Type | Description |
|----------|------|-------------|
| area_in_ha | float64 | Area in hectares identified for analysis |
| total_cropable_area_ever_hydroyear_2017_2023 | float64 | Total area identified suitable for cropping at least once during the specified period (hydroyears 2017 to 2023) |

### Cropping Intensity (Annual)
| Property | Type | Description |
|----------|------|-------------|
| cropping_intensity_2017 | float64 | Cropping intensity for 2017 |
| cropping_intensity_2018 | float64 | Cropping intensity for 2018 |
| cropping_intensity_2019 | float64 | Cropping intensity for 2019 |
| cropping_intensity_2020 | float64 | Cropping intensity for 2020 |
| cropping_intensity_2021 | float64 | Cropping intensity for 2021 |
| cropping_intensity_2022 | float64 | Cropping intensity for 2022 |
| cropping_intensity_2023 | float64 | Cropping intensity for 2023 |

### Single Cropped Area
| Property | Type | Description |
|----------|------|-------------|
| single_cropped_area_2017 | float64 | Single cropped area for 2017 |
| single_cropped_area_2018 | float64 | Single cropped area for 2018 |
| single_cropped_area_2019 | float64 | Single cropped area for 2019 |
| single_cropped_area_2020 | float64 | Single cropped area for 2020 |
| single_cropped_area_2021 | float64 | Single cropped area for 2021 |
| single_cropped_area_2022 | float64 | Single cropped area for 2022 |
| single_cropped_area_2023 | float64 | Single cropped area for 2023 |

### Single Kharif Cropped Area
| Property | Type | Description |
|----------|------|-------------|
| single_kharif_cropped_area_2017 | float64 | Single kharif cropped area for 2017 |
| single_kharif_cropped_area_2018 | float64 | Single kharif cropped area for 2018 |
| single_kharif_cropped_area_2019 | float64 | Single kharif cropped area for 2019 |
| single_kharif_cropped_area_2020 | float64 | Single kharif cropped area for 2020 |
| single_kharif_cropped_area_2021 | float64 | Single kharif cropped area for 2021 |
| single_kharif_cropped_area_2022 | float64 | Single kharif cropped area for 2022 |
| single_kharif_cropped_area_2023 | float64 | Single kharif cropped area for 2023 |

### Single Non-Kharif Cropped Area
| Property | Type | Description |
|----------|------|-------------|
| single_non_kharif_cropped_area_2017 | float64 | Single non kharif cropped area for 2017 |
| single_non_kharif_cropped_area_2018 | float64 | Single non kharif cropped area for 2018 |
| single_non_kharif_cropped_area_2019 | float64 | Single non kharif cropped area for 2019 |
| single_non_kharif_cropped_area_2020 | float64 | Single non kharif cropped area for 2020 |
| single_non_kharif_cropped_area_2021 | float64 | Single non kharif cropped area for 2021 |
| single_non_kharif_cropped_area_2022 | float64 | Single non kharif cropped area for 2022 |
| single_non_kharif_cropped_area_2023 | float64 | Single non kharif cropped area for 2023 |

### Double Cropped Area
| Property | Type | Description |
|----------|------|-------------|
| doubly_cropped_area_2017 | float64 | Double cropped area for 2017 |
| doubly_cropped_area_2018 | float64 | Double cropped area for 2018 |
| doubly_cropped_area_2019 | float64 | Double cropped area for 2019 |
| doubly_cropped_area_2020 | float64 | Double cropped area for 2020 |
| doubly_cropped_area_2021 | float64 | Double cropped area for 2021 |
| doubly_cropped_area_2022 | float64 | Double cropped area for 2022 |
| doubly_cropped_area_2023 | float64 | Double cropped area for 2023 |

### Triple/Annual/Perennial Cropped Area
| Property | Type | Description |
|----------|------|-------------|
| triply_cropped_area_2017 | float64 | Triple cropped area for 2017 |
| triply_cropped_area_2018 | float64 | Triple cropped area for 2018 |
| triply_cropped_area_2019 | float64 | Triple cropped area for 2019 |
| triply_cropped_area_2020 | float64 | Triple cropped area for 2020 |
| triply_cropped_area_2021 | float64 | Triple/annual/perennial cropping area for 2021 |
| triply_cropped_area_2022 | float64 | Triple cropped area for 2022 |
| triply_cropped_area_2023 | float64 | Triple cropped area for 2023 |

### Geometry
| Property | Type | Description |
|----------|------|-------------|
| geometry | geometry | Spatial geometry |

---