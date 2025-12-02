map_1 = [
    {
        "name": "generate_tehsil_shape_file_data",
        "children": [
            {"name": "clip_nrega_district_block"},
            {"name": "mws_layer"},
        ],
    }
]

map_2_1 = [
    {
        "name": "generate_hydrology",
        "use_global_args": True,
    },
    {
        "name": "generate_hydrology",
        "args": {"is_annual": True},
        "use_global_args": True,
    },
]

map_2_2 = [
    {
        "name": "clip_lulc_v3",
        "use_global_args": True,
        "children": [
            {
                "name": "vectorise_lulc",
                "use_global_args": True,
            },
            {
                "name": "generate_cropping_intensity",
                "use_global_args": True,
            },
            {
                "name": "generate_swb_layer",
                "use_global_args": True,
                "depends_on": [
                    "generate_catchment_area_singleflow",
                    "generate_stream_order",
                    "clip_drainage_lines",
                ],
            },
            {
                "name": "calculate_drought",
                "use_global_args": True,
                "children": [
                    {
                        "name": "drought_causality",
                        "use_global_args": True,
                    }
                ],
            },
            {
                "name": "create_crop_grids",
            },
            {
                "name": "get_change_detection",
                "use_global_args": True,
                "children": [
                    {
                        "name": "vectorise_change_detection",
                    }
                ],
            },
        ],
    },
]

map_3 = [
    {
        "name": "generate_soge_vector",
    },
    {
        "name": "generate_stream_order",
    },
    {
        "name": "generate_restoration_opportunity",
    },
    {
        "name": "generate_aquifer_vector",
    },
    {"name": "generate_natural_depression_data"},
    {"name": "generate_distance_to_nearest_drainage_line"},
    {"name": "generate_catchment_area_singleflow"},
    {"name": "generate_slope_percentage"},
    {"name": "generate_lcw_conflict_data"},
    {"name": "generate_agroecological_data"},
    {"name": "generate_factory_csr_data"},
    {"name": "generate_green_credit_data"},
    {"name": "generate_mining_data"},
]

map_4 = [
    {
        "name": "clip_drainage_lines",
        "children": [
            {
                "name": "generate_clart_layer",
            }
        ],
    },
    {
        "name": "site_suitability",
        "use_global_args": True,
    },
    {
        "name": "terrain_raster",
        "children": [
            {
                "name": "generate_terrain_clusters",
            },
            {
                "name": "lulc_on_slope_cluster",
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
            {
                "name": "lulc_on_plain_cluster",
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
        ],
    },
    {
        "name": "tree_health_ch_raster",
        "use_global_args": True,
        "children": [
            {
                "name": "tree_health_ch_vector",
                "use_global_args": True,
            }
        ],
    },
    {
        "name": "tree_health_ccd_raster",
        "use_global_args": True,
        "children": [
            {
                "name": "tree_health_ccd_vector",
                "use_global_args": True,
            }
        ],
    },
    {
        "name": "tree_health_overall_change_raster",
        "children": [
            {
                "name": "tree_health_overall_change_vector",
            }
        ],
    },
]


end_year_rules = {
    "calculate_drought": 2022,
    "drought_causality": 2022,
    "tree_health_ch_raster": 2022,
    "tree_health_ch_vector": 2022,
    "tree_health_ccd_raster": 2022,
    "tree_health_ccd_vector": 2022,
    "tree_health_overall_change_raster": 2022,
    "tree_health_overall_change_vector": 2022,
}
