"""Single source of truth for tehsil Excel sheet names.

Imported by both ``stats_generator.utils`` (sheet writers) and
``public_api.dataset_filters`` (``data=`` query allowlist) so the
pipeline and API filter cannot drift out of sync.
"""

# (excel_sheet_name, canonical_api_token, description)
TEHSIL_SHEET_SPECS = (
    ("agroecological", "agroecological", "Agroecological zone attributes per MWS"),
    ("antyodaya", "antyodaya", "Antyodaya / SECC socio-economic indicators"),
    ("aquifer_vector", "aquifer_vector", "Aquifer class and area share per MWS"),
    ("canal", "canal", "Canal network intersection with the MWS"),
    ("Canopy_Cover_Density", "canopy_cover_density", "Tree canopy cover density"),
    ("Canopy_height", "canopy_height", "Tree canopy height"),
    (
        "change_detection_cropintensity",
        "change_detection_cropintensity",
        "Change detection: cropping intensity",
    ),
    (
        "change_detection_degradation",
        "change_detection_degradation",
        "Change detection: land degradation",
    ),
    (
        "change_detection_shrubchange",
        "change_detection_shrubchange",
        "Change detection: shrub change",
    ),
    (
        "change_detection_deforestation",
        "change_detection_tree_cover_decrease",
        "Change detection: tree-cover decrease (deforestation)",
    ),
    (
        "change_detection_afforestation",
        "change_detection_tree_cover_increase",
        "Change detection: tree-cover increase (afforestation)",
    ),
    (
        "change_detection_urbanization",
        "change_detection_urbanization",
        "Change detection: urbanization",
    ),
    (
        "croppingDrought_kharif",
        "croppingdrought_kharif",
        "Kharif cropping vs drought years (alias: ``drought``)",
    ),
    (
        "croppingIntensity_annual",
        "croppingintensity_annual",
        "Annual cropping intensity",
    ),
    ("dem", "dem", "Elevation / DEM summary per MWS"),
    ("drainage_density", "drainage_density", "Drainage density"),
    ("drought_causality", "drought_causality", "Drought causality classes"),
    ("facilities_proximity", "facilities_proximity", "Distance to facilities (km)"),
    ("factory_csr", "factory_csr", "Factory / CSR sites intersecting the MWS"),
    ("forest_fire", "forest_fire", "Forest fire frequency and radiative power"),
    ("forest_fringes", "forest_fringes", "Forest fringes overlay"),
    ("green_credit", "green_credit", "Green credit / restoration opportunity"),
    (
        "hydrological_annual",
        "hydrological_annual",
        "Annual hydrology (ET, runoff, precipitation, …)",
    ),
    ("hydrological_seasonal", "hydrological_seasonal", "Seasonal hydrology"),
    ("lcw_conflict", "lcw_conflict", "Land conflict watch overlay"),
    ("livestock", "livestock", "Livestock census indicators"),
    ("lulc_vector", "lulc_vector", "Land use / land cover class shares"),
    ("mining", "mining", "Mining overlay"),
    ("mws", "mws", "Micro-watershed identity and area"),
    ("mws_connectivity", "mws_connectivity", "MWS drainage connectivity"),
    (
        "mws_intersect_swb",
        "mws_intersect_swb",
        "Surface water bodies intersecting the MWS",
    ),
    (
        "mws_intersect_villages",
        "mws_intersect_villages",
        "Villages intersecting the MWS",
    ),
    ("ndvi", "ndvi", "NDVI vegetation indices (alias: ``ndvi_shrub``)"),
    ("nrega_annual", "nrega_annual", "Annual MGNREGA works"),
    ("nrega_assets_village", "nrega_assets_village", "Village-level MGNREGA assets"),
    ("overall_tree_change", "overall_tree_change", "Overall tree-cover change"),
    ("restoration_vector", "restoration_vector", "Restoration opportunity classes"),
    ("river", "river", "River network intersection"),
    (
        "social_economic_indicator",
        "social_economic_indicator",
        "Social-economic indicator table",
    ),
    ("soge_vector", "soge_vector", "Stage of groundwater extraction (SOGE)"),
    ("soil_health", "soil_health", "Soil health parameters"),
    ("soil_type", "soil_type", "Soil type"),
    ("stream_order", "stream_order", "Stream-order length / density"),
    (
        "surfaceWaterBodies_annual",
        "surfacewaterbodies_annual",
        "Annual surface water body extent",
    ),
    ("terrain", "terrain", "Terrain cluster / morphometry"),
    ("terrain_lulc_plain", "terrain_lulc_plain", "Terrain × LULC on plains"),
    ("terrain_lulc_slope", "terrain_lulc_slope", "Terrain × LULC on slopes"),
    ("tree_in_grassland", "tree_in_grassland", "Tree cover in grassland"),
    ("village_GP", "village_gp", "Village to Gram Panchayat mapping"),
    ("solar_power", "solar_power", "India solar power tracker "),
    ("wind_power", "wind_power", "India wind power tracker "),
)

# All Excel sheet names written by the stats pipeline (exact Excel casing).
PIPELINE_EXCEL_SHEETS = tuple(spec[0] for spec in TEHSIL_SHEET_SPECS)

# Lookup from canonical API token -> exact Excel sheet name (used by stats_generator writers).
EXCEL_SHEET_BY_KEY = {
    api_key: excel_name for excel_name, api_key, _ in TEHSIL_SHEET_SPECS
}
