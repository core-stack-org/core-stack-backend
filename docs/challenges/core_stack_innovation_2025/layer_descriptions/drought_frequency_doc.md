# Drought Frequency Layer

## Description

Drought frequency data. The Department of Agriculture, Cooperation & Farmers Welfare under the Ministry of Agriculture & Farmers Welfare, Government of India released the "Manual for Drought Management" in 2016 for the prevention, mitigation and management of droughts in India. The manual introduces scientific indices and parameters such as Vegetation Condition Index (VCI) for more accurate determination and assessment of drought.

This data is based on an implementation the methodology of the drought manual using various remote sensing products.

**Output Classes:**
1. Severe
2. Moderate
3. Mild

**Input Data Sources:**
- Rainfall data (CHIRPS)
- Vegetation data (Landsat)
- Crop data (MODIS)
- Land use land cover (IndiaSAT)

Details of methodology can be found in the technical manual and the codebase on [GitHub](https://github.com/core-stack-org/core-stack-backend/tree/main/computing/drought).

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

### Drought Classification by Year
| Property | Type | Description |
|----------|------|-------------|
| drlb_2017 | object | Drought labels 2017 |
| drlb_2018 | object | Drought labels 2018 |
| drlb_2019 | object | Drought labels 2019 |
| drlb_2020 | object | Drought labels 2020 |
| drlb_2021 | object | Drought labels 2021 |
| drlb_2022 | object | Drought labels 2022 |

### Dry Spell Analysis
| Property | Type | Description |
|----------|------|-------------|
| avg_dryspell | float64 | Average dry spell |
| drysp_2017 | int32 | Dryspell length 2017 |
| drysp_2018 | int32 | Dryspell length 2018 |
| drysp_2019 | int32 | Dryspell length 2019 |
| drysp_2020 | int32 | Dryspell length 2020 |
| drysp_2021 | int32 | Dryspell length 2021 |
| drysp_2022 | int32 | Dryspell length 2022 |

### Drought Weeks by Severity
**No Drought**
| Property | Type | Description |
|----------|------|-------------|
| w_no_2017 | int32 | Number of weeks in no drought for 2017 |
| w_no_2018 | int32 | Number of weeks in no drought for 2018 |
| w_no_2019 | int32 | Number of weeks in no drought for 2019 |
| w_no_2020 | int32 | Number of weeks in no drought for 2020 |
| w_no_2021 | int32 | Number of weeks in no drought for 2021 |
| w_no_2022 | int32 | Number of weeks in no drought for 2022 |

**Mild Drought**
| Property | Type | Description |
|----------|------|-------------|
| w_mld_2017 | int32 | Number of weeks in mild drought for 2017 |
| w_mld_2018 | int32 | Number of weeks in mild drought for 2018 |
| w_mld_2019 | int32 | Number of weeks in mild drought for 2019 |
| w_mld_2020 | int32 | Number of weeks in mild drought for 2020 |
| w_mld_2021 | int32 | Number of weeks in mild drought for 2021 |
| w_mld_2022 | int32 | Number of weeks in mild drought for 2022 |

**Moderate Drought**
| Property | Type | Description |
|----------|------|-------------|
| w_mod_2017 | int32 | Number of weeks in moderate drought for 2017 |
| w_mod_2018 | int32 | Number of weeks in moderate drought for 2018 |
| w_mod_2019 | int32 | Number of weeks in moderate drought for 2019 |
| w_mod_2020 | int32 | Number of weeks in moderate drought for 2020 |
| w_mod_2021 | int32 | Number of weeks in moderate drought for 2021 |
| w_mod_2022 | int32 | Number of weeks in moderate drought for 2022 |

**Severe Drought**
| Property | Type | Description |
|----------|------|-------------|
| w_sev_2017 | int32 | Number of weeks in severe drought for 2017 |
| w_sev_2018 | int32 | Number of weeks in severe drought for 2018 |
| w_sev_2019 | int32 | Number of weeks in severe drought for 2019 |
| w_sev_2020 | int32 | Number of weeks in severe drought for 2020 |
| w_sev_2021 | int32 | Number of weeks in severe drought for 2021 |
| w_sev_2022 | int32 | Number of weeks in severe drought for 2022 |

### Total Analysis Period
| Property | Type | Description |
|----------|------|-------------|
| t_wks_2017 | int32 | Total weeks analysed in 2017 |
| t_wks_2018 | int32 | Total weeks analysed in 2018 |
| t_wks_2019 | int32 | Total weeks analysed in 2019 |
| t_wks_2020 | int32 | Total weeks analysed in 2020 |
| t_wks_2021 | int32 | Total weeks analysed in 2021 |
| t_wks_2022 | int32 | Total weeks analysed in 2022 |

### Drought Frequency by Threshold
**Threshold 0**
| Property | Type | Description |
|----------|------|-------------|
| frth0_2017 - frth0_2022 | int32 | Frequency of drought at threshold 0 for each year |

**Threshold 1**
| Property | Type | Description |
|----------|------|-------------|
| frth1_2017 - frth1_2022 | int32 | Frequency of drought at threshold 1 for each year |

**Threshold 2**
| Property | Type | Description |
|----------|------|-------------|
| frth2_2017 - frth2_2022 | int32 | Frequency of drought at threshold 2 for each year |

**Threshold 3**
| Property | Type | Description |
|----------|------|-------------|
| frth3_2017 - frth3_2022 | int32 | Frequency of drought at threshold 3 for each year |

### Drought Intensity by Threshold
**Threshold 0**
| Property | Type | Description |
|----------|------|-------------|
| inth0_2017 - inth0_2022 | float64 | Intensity of drought at threshold 0 for each year |

**Threshold 1**
| Property | Type | Description |
|----------|------|-------------|
| inth1_2017 - inth1_2022 | float64 | Intensity of drought at threshold 1 for each year |

**Threshold 2**
| Property | Type | Description |
|----------|------|-------------|
| inth2_2017 - inth2_2022 | int32/float64 | Intensity of drought at threshold 2 for each year |

**Threshold 3**
| Property | Type | Description |
|----------|------|-------------|
| inth3_2017 - inth3_2022 | int32 | Intensity of drought at threshold 3 for each year |

### Kharif Cropping
| Property | Type | Description |
|----------|------|-------------|
| kh_cr_2017 | float64 | Kharif cropped sq km for 2017 |
| kh_cr_2018 | float64 | Kharif cropped sq km for 2018 |
| kh_cr_2019 | float64 | Kharif cropped sq km for 2019 |
| kh_cr_2020 | float64 | Kharif cropped sq km for 2020 |
| kh_cr_2021 | float64 | Kharif cropped sq km for 2021 |
| kh_cr_2022 | float64 | Kharif cropped sq km for 2022 |

### Percentage Area Cropped (Kharif)
| Property | Type | Description |
|----------|------|-------------|
| pcr_k_2017 | float64 | Percent of area cropped kharif for 2017 |
| pcr_k_2018 | float64 | Percent of area cropped kharif for 2018 |
| pcr_k_2019 | float64 | Percent of area cropped kharif for 2019 |
| pcr_k_2020 | float64 | Percent of area cropped kharif for 2020 |
| pcr_k_2021 | float64 | Percent of area cropped kharif for 2021 |
| pcr_k_2022 | float64 | Percent of area cropped kharif for 2022 |

### Monsoon Onset
| Property | Type | Description |
|----------|------|-------------|
| m_ons_2017 | datetime64[ms] | Monsoon onset for 2017 |
| m_ons_2018 | datetime64[ms] | Monsoon onset for 2018 |
| m_ons_2019 | datetime64[ms] | Monsoon onset for 2019 |
| m_ons_2020 | datetime64[ms] | Monsoon onset for 2020 |
| m_ons_2021 | datetime64[ms] | Monsoon onset for 2021 |
| m_ons_2022 | datetime64[ms] | Monsoon onset for 2022 |

### Geometry
| Property | Type | Description |
|----------|------|-------------|
| geometry | geometry | Spatial geometry |

---