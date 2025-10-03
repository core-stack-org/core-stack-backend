map_1 = [
    {
        "name": "generate_tehsil_shape_file_data",
        "children": [
            {"name": "clip_nrega_district_block"},
            {"name": "mws_layer"},
        ],
    }
]

map_2 = [
    {
        "name": "generate_hydrology",
        "use_global_args": True,
    },
    {
        "name": "generate_hydrology",
        "args": {"is_annual": True},
        "use_global_args": True,
    },
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
        "name": "generate_restoration_opportunity",
    },
    {
        "name": "generate_aquifer_vector",
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
]

map_4 = [
    {
        "name": "generate_soge_vector",
    },
    {
        "name": "generate_stream_order",
    },
    {
        "name": "clip_drainage_lines",
        "children": [
            {
                "name": "generate_clart_layer",
            }
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
    "clip_lulc_v3": 2023,
    "vectorise_lulc": 2023,
    "generate_cropping_intensity": 2023,
    "generate_swb_layer": 2023,
    "get_change_detection": 2023,
    "lulc_on_slope_cluster": 2023,
    "lulc_on_plain_cluster": 2023,
}
