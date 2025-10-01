map_1 = [
    {
        "name": "generate_tehsil_shape_file_data",
        "use_multiple_gee": True,
        "children": [
            {"name": "clip_nrega_district_block", "use_multiple_gee": True},
            {"name": "mws_layer", "use_multiple_gee": True},
        ],
    }
]

map_2 = [
    {
        "name": "generate_hydrology",
        # "args": {"start_year": 2017, "end_year": 2023},
        "use_global_args": True,
        "use_multiple_gee": True,
    },
    {
        "name": "generate_hydrology",
        # "args": {"start_year": 2017, "end_year": 2023, "is_annual": True},
        "args": {"is_annual": True},
        "use_global_args": True,
        "use_multiple_gee": True,
    },
    {
        "name": "clip_lulc_v3",
        # "args": {"start_year": 2017, "end_year": 2023},
        "use_global_args": True,
        "use_multiple_gee": True,
        "children": [
            {
                "name": "vectorise_lulc",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "generate_cropping_intensity",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "generate_swb_layer",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "calculate_drought",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "children": [
                    {
                        "name": "drought_causality",
                        "use_multiple_gee": True,
                        # "args": {"start_year": 2017, "end_year": 2023},
                        "use_global_args": True,
                    }
                ],
            },
            {
                "name": "create_crop_grids",
                "use_multiple_gee": True,
            },
            {
                "name": "get_change_detection",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "children": [
                    {
                        "name": "vectorise_change_detection",
                        "use_multiple_gee": True,
                    }
                ],
            },
        ],
    },
]

map_3 = [
    {
        "name": "generate_restoration_opportunity",
        "use_multiple_gee": True,
    },
    {
        "name": "generate_aquifer_vector",
        "use_multiple_gee": True,
    },
    {
        "name": "terrain_raster",
        "use_multiple_gee": True,
        "children": [
            {
                "name": "generate_terrain_clusters",
                "use_multiple_gee": True,
            },
            {
                "name": "lulc_on_slope_cluster",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
            {
                "name": "lulc_on_plain_cluster",
                "use_multiple_gee": True,
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
        ],
    },
]

map_4 = [
    {
        "name": "generate_soge_vector",
        "use_multiple_gee": True,
    },
    {
        "name": "generate_stream_order_vector",
        "use_multiple_gee": True,
    },
    {
        "name": "clip_drainage_lines",
        "use_multiple_gee": True,
        "children": [
            {
                "name": "generate_clart_layer",
                "use_multiple_gee": True,
            }
        ],
    },
    {
        "name": "tree_health_ch_raster",
        "use_multiple_gee": True,
        "children": [
            {
                "name": "tree_health_ch_vector",
                "use_multiple_gee": True,
            }
        ],
    },
    {
        "name": "tree_health_ccd_raster",
        "use_multiple_gee": True,
        "children": [
            {
                "name": "tree_health_ccd_vector",
                "use_multiple_gee": True,
            }
        ],
    },
    {
        "name": "tree_health_overall_change_raster",
        "use_multiple_gee": True,
        "children": [
            {
                "name": "tree_health_overall_change_vector",
                "use_multiple_gee": True,
            }
        ],
    },
]
