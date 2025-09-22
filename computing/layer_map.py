map_1 = [
    {
        "name": "generate_tehsil_shape_file_data",
        "children": [{"name": "clip_nrega_district_block"}, {"name": "mws_layer"}],
    }
]

map_2 = [
    {
        "name": "generate_hydrology",
        # "args": {"start_year": 2017, "end_year": 2023},
        "use_global_args": True,
    },
    {
        "name": "generate_hydrology",
        # "args": {"start_year": 2017, "end_year": 2023, "is_annual": True},
        "args": {"is_annual": True},
        "use_global_args": True,
    },
    {
        "name": "clip_lulc_v3",
        # "args": {"start_year": 2017, "end_year": 2023},
        "use_global_args": True,
        "children": [
            {
                "name": "vectorise_lulc",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "generate_cropping_intensity",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "generate_swb_layer",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
            },
            {
                "name": "calculate_drought",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "children": [
                    {
                        "name": "drought_causality",
                        # "args": {"start_year": 2017, "end_year": 2023},
                        "use_global_args": True,
                    }
                ],
            },
            {"name": "create_crop_grids"},
            {
                "name": "get_change_detection",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "children": [{"name": "vectorise_change_detection"}],
            },
        ],
    },
]

map_3 = [
    {"name": "generate_restoration_opportunity"},
    {"name": "generate_aquifer_vector"},
    {
        "name": "terrain_raster",
        "children": [
            {"name": "generate_terrain_clusters"},
            {
                "name": "lulc_on_slope_cluster",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
            {
                "name": "lulc_on_plain_cluster",
                # "args": {"start_year": 2017, "end_year": 2023},
                "use_global_args": True,
                "depends_on": ["terrain_raster", "clip_lulc_v3"],
            },
        ],
    },
]

map_4 = [
    {
        "name": [
            {"name": "generate_soge_vector"},
            {"name": "generate_stream_order_vector"},
            {
                "name": "clip_drainage_lines",
                "children": [{"name": "generate_clart_layer"}],
            },
            {
                "name": "tree_health_ch_raster",
                "children": [{"name": "tree_health_ch_vector"}],
            },
            {
                "name": "tree_health_ccd_raster",
                "children": [{"name": "tree_health_ccd_vector"}],
            },
            {
                "name": "tree_health_overall_change_raster",
                "children": [{"name": "tree_health_overall_change_vector"}],
            },
        ]
    }
]
