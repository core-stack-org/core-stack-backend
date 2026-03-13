# Surface Water Bodies Layer

## Description

Surface water bodies data. Surface Water is any body of water above ground, including streams, rivers, and lakes. 

To generate this map, water pixels from the Land Use Land Cover model (IndiaSAT) for each year are taken and converted into a vector layer. Given the intra-annual classes for water availability based on India's agriculture calendar in IndiaSAT, water availability in each season of the year (kharif, rabi, zaid) for each of the waterbodies is computed. 

This data is also combined with waterbody census data (2023) to get additional data about the waterbodies, but the matching can have errors - a matching algorithm was built to match the remote sensed waterbodies detected from the IndiaSAT LULC with the closest lat/longs provided in the waterbody census.

Details of methodology can be found in the technical manual and the codebase on [GitHub](https://github.com/core-stack-org/core-stack-backend/tree/main/computing/surface_water_bodies).

---

## Properties

### Identification
| Property | Type | Description |
|----------|------|-------------|
| UID | object | Unique waterbody identifier |
| census_id | object | Waterbody Census ID |
| MWS_UID | object | Unique MWS identifier |

### Hydrological Classification
| Property | Type | Description |
|----------|------|-------------|
| basin_name | object | Basin name |
| sub_basin_name | object | Sub-basin name |

### Surface Water Area (Hectares by Year)
| Property | Type | Description |
|----------|------|-------------|
| area_17-18 | float64 | Area of surface water (in hectares) in 2017-2018 |
| area_18-19 | float64 | Area of surface water (in hectares) in 2018-2019 |
| area_19-20 | float64 | Area of surface water (in hectares) in 2019-2020 |
| area_20-21 | float64 | Area of surface water (in hectares) in 2020-2021 |
| area_21-22 | float64 | Area of surface water (in hectares) in 2021-2022 |
| area_22-23 | float64 | Area of surface water (in hectares) in 2022-2023 |
| area_23-24 | float64 | Area of surface water (in hectares) in 2023-2024 |

### Water Availability - Kharif Season (%)
| Property | Type | Description |
|----------|------|-------------|
| k_17-18 | float64 | Percentage of water availability in kharif season 2017-2018 |
| k_18-19 | float64 | Percentage of water availability in kharif season 2018-2019 |
| k_19-20 | float64 | Percentage of water availability in kharif season 2019-2020 |
| k_20-21 | float64 | Percentage of water availability in kharif season 2020-2021 |
| k_21-22 | float64 | Percentage of water availability in kharif season 2021-2022 |
| k_22-23 | float64 | Percentage of water availability in kharif season 2022-2023 |
| k_23-24 | float64 | Percentage of water availability in kharif season 2023-2024 |

### Water Availability - Kharif & Rabi Seasons (%)
| Property | Type | Description |
|----------|------|-------------|
| kr_17-18 | float64 | Percentage of water availability in kharif and rabi seasons 2017-2018 |
| kr_18-19 | float64 | Percentage of water availability in kharif and rabi seasons 2018-2019 |
| kr_19-20 | float64 | Percentage of water availability in kharif and rabi seasons 2019-2020 |
| kr_20-21 | float64 | Percentage of water availability in kharif and rabi seasons 2020-2021 |
| kr_21-22 | float64 | Percentage of water availability in kharif and rabi seasons 2021-2022 |
| kr_22-23 | float64 | Percentage of water availability in kharif and rabi seasons 2022-2023 |
| kr_23-24 | float64 | Percentage of water availability in kharif and rabi seasons 2023-2024 |

### Water Availability - All Seasons (Kharif, Rabi, Zaid) (%)
| Property | Type | Description |
|----------|------|-------------|
| krz_17-18 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2017-2018 |
| krz_18-19 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2018-2019 |
| krz_19-20 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2019-2020 |
| krz_20-21 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2020-2021 |
| krz_21-22 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2021-2022 |
| krz_22-23 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2022-2023 |
| krz_23-24 | float64 | Percentage of water availability in kharif, rabi and zaid seasons 2023-2024 |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| State Name | object | State name |
| District Name | object | District name |
| Block/Tehsil Name | object | Block/Tehsil name |
| Village Name | object | Village name |
| town_municipalty_name | object | Town/municipality name |
| ward_name | object | Ward name |
| rural_or_urban | object | Rural or urban classification |
| latitude_dec | float64 | Latitude (decimal) |
| longitude_dec | float64 | Longitude (decimal) |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| water_body_name | object | Water body name |
| water_body_loc_name | object | Water body location name |
| ref_water_body_type_id_name | object | Water body type |
| water_body_nature_name | object | Water body nature |
| manmade_water_body_type_name | object | Manmade water body type |
| water_body_ownership_name | object | Water body ownership |
| nature_of_storage | object | Nature of storage |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| water_spread_area_of_water_body | float64 | Water spread area of water body |
| area_ored | float64 | Surveyed water body area |
| max_depth_water_body_fully_filled | float64 | Maximum depth when water body is fully filled |
| storage_capacity_water_body_original | float64 | Original storage capacity of water body |
| storage_capacity_water_body_present | float64 | Present storage capacity of water body |
| category_sq_m | object | Size category in sq m: 5000+, 1000-5000, 500-1000, 0-500 |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| filled_up_storage_name | object | Filled up storage name |
| filled_up_storage_space_name | object | Filled up storage space name |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| construcion_year | float64 | Construction year |
| construction_cost | float64 | Construction cost |
| khasra_number | object | Khasra number |
| si_no_of_water_body_within_village_town | float64 | Serial number of water body within village/town |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| ref_water_body_in_use_id_name | object | Water body in use status |
| ref_reason_water_body_in_use_id1_name | object | Primary reason water body is in use |
| reason_water_body_in_use_name2 | object | Secondary reason water body is in use |
| reason_water_body_in_use_name3 | object | Tertiary reason water body is in use |
| no_people_benefited_by_water_body | float64 | Number of people benefited by water body |
| no_villages_benefited | float64 | Number of villages benefited |
| no_town_cities_benefited | float64 | Number of towns/cities benefited |
| cca_water_body | float64 | CCA water body |
| ipc_water_body | float64 | IPC water body |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| ref_water_body_under_repair_renovation_restoration_id_name | object | Water body under repair/renovation/restoration status |
| scheme_status_reason_name | object | Scheme status reason |
| scheme_under_revival_is_done | object | Scheme under revival completion status |
| ref_selection_id_dip_sip_exists_name | object | DIP/SIP exists |
| ref_selection_id_wua_exists_name | object | WUA (Water Users Association) exists |

### Properties from the Waterbody Census
| Property | Type | Description |
|----------|------|-------------|
| ref_selection_id_water_body_encroached_name | object | Water body encroached status |
| ref_selection_id_encroachment_assessed_name | object | Encroachment assessed status |

### Geometry
| Property | Type | Description |
|----------|------|-------------|
| geometry | geometry | Spatial geometry |

---