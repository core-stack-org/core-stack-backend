# Tehsil Data Analysis Report

Spatial and Temporal Analysis of Key Metrics

Overview of Tehsil characteristics

# Socio-ecological patterns across Tehsil

The following map shows the variation of different socio-ecological stress patterns across microwatersheds and villages in the tehsil. As the number of stress patterns in a microwatershed increases, its gradient moves from green to red color.

![](data/tehsil_reports/.review/markdown/jamui_media/media/image2.jpeg)

> This chart shows the number of stressor patterns in a micro-watershed. Darker shades of red indicate compounding stresses. In this report we highlight areas in tehsil that seems to have various stresses.

# Stress patterns affecting agriculture

## Groundwater Stress

6640.82 hectares out of 253925.38 hectares of total cropping area in the tehsil appears to be groundwater stressed.

Areas where either stage of groundwater stress is unsafe or water balance trend over the years 2017-2024 is negative, are color coded as yellow. Areas with an unsafe groundwater stress and negative water balance trend are shown in red.

![](data/tehsil_reports/.review/markdown/jamui_media/media/image4.jpeg)

> Note: The stage of groundwater extraction refers to the current level or intensity at which groundwater is withdrawn from aquifers compared to its natural replenishment rate. Based on this ratio, agencies like the Central Ground Water Board
>
> (CGWB) in India classify areas into categories indicating the extent of extraction. The water balance for a year is computed by subtracting outgoing vertical fluxes (runoff and evapotranspiration) from incoming water (precipitation). This calculation does not include horizontal fluxes of subsurface flows in the underlying aquifer or surface flows such as canals and rivers within the micro-watershed.

## High Drought Incidence

6734.68 hectares out of 253925.38 hectares of cropping area in the tehsil appears to have a high drought incidence.

Areas where there have been at least two drought years or at least two intensive dry-spell years during the 2017-2024, are color coded as yellow. Areas which experienced both of these conditions are shown in red.

![](data/tehsil_reports/.review/markdown/jamui_media/media/image30.jpg)

> Note: Drought is defined as per the Government of India's Drought Manual and considered moderate or severe if the number of weeks of drought is five or more, which means that rainfall deficit was more than 25% for more than 5 weeks
>
> during the monsoon of that year. Drought weeks are identified based on whether meteorological drought occurred in that week (i.e. the rains were less than usual in that week as compared to previous years, possibly intensified by dry spells
>
> defined as consecutive weeks of low rainfall) and/or agricultural drought occurred in that week (i.e. cropped area or crop health were lower than usual in that week as compared to previous years). Severe drought weeks are those when
>
> meteorological and agricultural drought are both coincident. The occurrence of intensive dry spell is defined across four consecutive weeks with each week of these four weeks incurring a rainfall deviation of more than 50% from the historical average.

<div style="color:#ff0000">
<p><strong>District Level Drought Context:</strong> [[TEHSIL_NAME]] lies in <em>[[DISTRICT_NAME]]</em> district. Following figure shows available Observed IMD series (1981–2024), followed by corresponding RCP4.5 and RCP8.5 modelled-scenario series (2025–2099) for the normalised 6-month drought severity index and longest dry spell (continuous days in the district with less than 1mm rainfall) showing serious watrer scarcity.</p>

<img src="[[DISTRICT_DROUGHT_CHART_PLOT]]" alt="Long-term district drought severity and longest dry spell for [[DISTRICT_NAME]] district" style="max-width:100%">
<p><em>District-level climate context used in this tehsil report.</em></p>

<p>The observed normalized drought-severity value averaged [[DROUGHT_BASELINE_MEAN]] during 1981–2016 and [[DROUGHT_RECENT_MEAN]] during 2017–2024. The modelled segment is shown together, while a separation line shows clear demarcation between them. [[DROUGHT_PROJECTED_TEXT]]</p>

</div>

## Likely stress in cropping yield

Likely stress in cropping yield is assessed based on degradation of farmland into barren or shrub areas and areas with reduced cropping intensity. Micro-watersheds where the extent of either of these conditions over the years 2017-2024 exceed 30 hectares have been highlighted in yellow. Similarly, micro-watersheds where farmland areas that have undergone degradation and reduction in the number of crop cycles both exceeding 30 hectares are marked in red.

5400.62 hectares in the tehsil appears to have had a reduction in cropping area.

253925.38 hectares in the tehsil appears to have had a reduction in cropping intensity.

![](data/tehsil_reports/.review/markdown/jamui_media/media/image9.jpeg)

> Note: Cropping degradation is calculated based on farmland transitions to barren or scrubs and shrub land or reduction in cropping intensity (change in area earlier sown thrice to now sown twice, earlier sown twice to now sown once, earlier sown thrice to now sown once). The degradation in cropping is computed using the IndiaSAT Land Use Land Cover
>
> outputs. Transitions from farmland to barren land, shrubs, or human settlements (built-up) are computed. Similarly, we
>
> use annual land use land cover (LULC), to identify areas under single cropping, double cropping and triple cropping using pixels which are classified as single kharif, single non-kharif, double and triple classes of LULC classifier to determine cropping intensity.

# Stress patterns affecting Tree cover

## Reduction in Tree Cover

2891.67 hectares out of 9325.63 hectares of area under tree cover in the tehsil appears to have been lost.

The data is computed over the years 2017-2024.

![](data/tehsil_reports/.review/markdown/jamui_media/media/image70.jpg)

> Note: The reduction in tree cover is computed using the IndiaSAT Land Use Land Cover outputs. Transitions from tree cover to barren land, shrubs, human settlements (built-up) or farmland are computed.

# Socio-economic patterns

## High density of marginalized caste communities

8,312 population in 13 villages are from marginalized groups.

> ![](data/tehsil_reports/.review/markdown/jamui_media/media/image9.jpg)Note: The demographic details on caste based population density is taken from Census 2011.

## Poor uptake of MGNREGA works

1033 villages out of 1277 have had a low (less than 100 NREGA work) number of NREGA works during 2005-2024.

> Note: The metadata of MGNREGA assets such as work name and work type has been obtained from the NREGA MIS.

# Intervention opportunities

## ![](data/tehsil_reports/.review/markdown/jamui_media/media/image110.jpg)Fishery

Up to 1822.01 hectares of area in the tehsil appears to have good potential for fishery due to a reasonable amount of surface water retained throughout the year. Good water potential in a microwatershed is assessed based on three conditions including the surface water availability during Rabi season being more than a threshold value of 30%, surface water availability during Zaid season similarly being more than 30%, and a non declining trend in annual surface water availability observed over the years 2017-2024. In the map below, the micro-watersheds are mapped to a gradient of green, depending on the number of conditions satisfied for a particular micro-watershed (light blue corresponds to one of the conditions being met, and dark blue corresponds to all the conditions being true).

> ![](data/tehsil_reports/.review/markdown/jamui_media/media/image130.jpg)Note: We use Sentinel-1 (SAR data) VV band for water pixel detection in Kharif season and Dynamic World to detect water pixels in Rabi and Zaid seasons.
>
<div style="color:#ff0000">
<h2>District Climate Context</h2>

<p>[[TEHSIL_NAME]] lies in <em>[[DISTRICT_NAME]]</em> district. While we recognise lack of tehsil level data regarding Climate Change, however we realise its immense importance for Water Resources management needs, therefore this section is added to show publicly available District Level Climate, how it has been changing, how based on best estimations it is projected to change in the future as well, allowing us to plan for mitigation and/or adaption regarding water resources, agriculture, people, livelihoods, governance.</p>

<p>RCP4.5 is an intermediate greenhouse-gas concentration pathway reaching approximately 4.5 W/m² of radiative forcing, while RCP8.5 is a high pathway reaching approximately 8.5 W/m² by 2100. They are shown together to examine climate change under intermediate and high pathways. These are modelled projections rather than predictions, and actual future observations may differ.</p>

[[IF TEMPERATURE_AVAILABLE]]
<img src="[[DISTRICT_TEMPERATURE_CHART_PLOT]]" alt="Single plot of annual maximum, average and minimum temperature for [[DISTRICT_NAME]] district" style="max-width:100%">
<p><em>Annual district maximum, average and minimum temperature. The shaded band connects the minimum and maximum temperature series, while the central line shows average temperature. Observed IMD values continue through 2024, followed by RCP4.5 and RCP8.5 modelled values from 2025 onwards.</em></p>

<p>The observed IMD averages during 2017–2024 were [[MAX_TEMP_OBSERVED_REFERENCE]]°C for maximum temperature, [[AVG_TEMP_OBSERVED_REFERENCE]]°C for average temperature and [[MIN_TEMP_OBSERVED_REFERENCE]]°C for minimum temperature. Under RCP8.5, [[TEMPERATURE_PROJECTED_PHRASE]]. Compared with the modelled 2017–2024 averages, the modelled changes during 2025–2049 are [[MAX_TEMP_NEAR_CHANGE]]°C, [[AVG_TEMP_NEAR_CHANGE]]°C and [[MIN_TEMP_NEAR_CHANGE]]°C for maximum, average and minimum temperature; during 2075–2099 they are [[MAX_TEMP_LATE_CHANGE]]°C, [[AVG_TEMP_LATE_CHANGE]]°C and [[MIN_TEMP_LATE_CHANGE]]°C.</p>
[[END_IF]]

<img src="[[DISTRICT_UNUSUAL_TEMPERATURE_CHART_PLOT]]" alt="Unusual temperature days and nights for [[DISTRICT_NAME]] district" style="max-width:100%">
<p><em>Annual counts of unusually hot days, unusually warm nights, unusually cold days and unusually cold nights. The observed series continues through 2024, followed by the RCP4.5 and RCP8.5 modelled series from 2025 onwards.</em></p>

<p>Compared with 1981–2016, the observed averages during 2017–2024 show [[HOT_OBSERVED_WORD]] unusually hot days ([[HOT_RECENT_MEAN]] from [[HOT_BASELINE_MEAN]]), [[WARM_OBSERVED_WORD]] unusually warm nights ([[WARM_RECENT_MEAN]] from [[WARM_BASELINE_MEAN]]), [[COLD_DAY_OBSERVED_WORD]] unusually cold days ([[COLD_DAY_RECENT_MEAN]] from [[COLD_DAY_BASELINE_MEAN]]), and [[COLD_NIGHT_OBSERVED_WORD]] unusually cold nights ([[COLD_NIGHT_RECENT_MEAN]] from [[COLD_NIGHT_BASELINE_MEAN]]).</p>

<p>As we can see, the district's climate is moving towards [[CLIMATE_OVERALL_PHRASE]], with [[HOT_PROJECTED_PHRASE]] unusually hot days and [[WARM_PROJECTED_PHRASE]] unusually warm nights. It is also projected to have [[COLD_DAY_PROJECTED_PHRASE]] unusually cold days and [[COLD_NIGHT_PROJECTED_PHRASE]] unusually cold nights.</p>

[[IF TEMP_PRIORITY_COUNT >= 2]]
<p>Among these temperature extremes, the two largest modelled changes between 2025–2049 and 2075–2099 are [[TEMP_PRIORITY_1_LABEL]], from [[TEMP_PRIORITY_1_NEAR_MEAN]] to [[TEMP_PRIORITY_1_LATE_MEAN]] a year, and [[TEMP_PRIORITY_2_LABEL]], from [[TEMP_PRIORITY_2_NEAR_MEAN]] to [[TEMP_PRIORITY_2_LATE_MEAN]] a year.</p>
[[END_IF]]

[[IF RAINFALL_AVAILABLE]]
<img src="[[DISTRICT_RAINFALL_CHART_PLOT]]" alt="Annual rainfall for [[DISTRICT_NAME]] district, showing observed, RCP4.5 and RCP8.5 modelled years" style="max-width:100%">
<p><em>Annual district rainfall from 1981–2099. The observed IMD series continues through 2024, followed by the RCP4.5 and RCP8.5 modelled series from 2025 onwards.</em></p>

<p>Annual district rainfall averaged [[RAINFALL_BASELINE_MEAN]] mm during 1981–2016 and [[RAINFALL_RECENT_MEAN]] mm during 2017–2024. Under RCP8.5, annual rainfall [[RAINFALL_PROJECTED_PHRASE]], with the modelled average changing from [[RAINFALL_NEAR_MEAN]] mm during 2025–2049 to [[RAINFALL_LATE_MEAN]] mm during 2075–2099.</p>
[[END_IF]]

<img src="[[DISTRICT_UNUSUAL_RAINFALL_CHART_PLOT]]" alt="Unusually heavy rainy days for [[DISTRICT_NAME]] district" style="max-width:100%">
<p><em>Annual count of unusually heavy rainy days. The observed series continues through 2024, followed by the RCP4.5 and RCP8.5 modelled series from 2025 onwards.</em></p>

<p>Compared with 1981–2016, the observed average during 2017–2024 shows [[RAIN_OBSERVED_WORD]] unusually heavy rainy days ([[RAIN_RECENT_MEAN]] from [[RAIN_BASELINE_MEAN]]). Under RCP8.5, the district is projected to have [[RAIN_PROJECTED_PHRASE]] unusually heavy rainy days, with the modelled annual average changing from [[RAIN_NEAR_MEAN]] during 2025–2049 to [[RAIN_LATE_MEAN]] during 2075–2099.</p>

<p><em>CoRE Stack. Data presented through Climate Resilience Analytics and Visualisation Intelligence System (CRAVIS.ai). 2026. New Delhi: Council on Energy, Environment and Water (CEEW). Available at: https://cravis.ai.</em></p>
</div>

> Report generated on {current_published_date} \| CoRE Stack
>
> Refer to our <u>technical manual</u> for more details on how data was collected and processed.
>
> Do note that while the underlying datasets have been validated against ground-truth in some locations, we need your feedback if the outputs shown here are in agreement with your observations about this area. Please do share your feedback with <u>contact</u>@<u>core-stack.or</u>g.
