import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add Django project base dir to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# Set Django settings module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")

import django

django.setup()

import requests
import ee
from geoadmin.models import TehsilSOI
from computing.STAC_specs import generate_STAC_layerwise
from computing.models import Layer
from utilities.gee_utils import valid_gee_text


def generate_stac_spec():
    upload_s3 = False
    layer_names_to_generate_rasters = [
        # "change_tree_cover_gain_raster",
        # "change_tree_cover_loss_raster",
        # "change_cropping_reduction_raster",
        # "change_urbanization_raster",
        # "change_cropping_intensity_raster",
        # "land_use_land_cover_raster",
        # "terrain_raster",
        # "clart_raster",
        # "tree_canopy_cover_density_raster",
        # "tree_cover_change_raster",
        # "tree_canopy_height_raster",
        #"wri_restoration_raster",
        # "natural_depressions_raster",
        # "stream_order_raster",
        # "distance_to_upstream_drainage_line_raster",
        "catchment_area_singleflow_raster",

    ]

    layer_names_to_generate_vectors = [
        # "admin_boundaries_vector",
        # "aquifer_vector",
        # "drainage_lines_vector",
        # "surface_water_bodies_vector",
        # "nrega_vector", 
        #"terrain_vector",
        # "cropping_intensity_vector",
        #"stage_of_groundwater_extraction_vector",
        # "drought_frequency_vector",
        #"change_in_well_depth_vector",
    ]

    layer_obj_and_name = {
        "admin_boundaries_vector": {
            "dataset_name": "Admin Boundary",
            "layer_name": "dist_block",
        },
        "aquifer_vector": {
            "dataset_name": "Aquifer",
            "layer_name": "aquifer_vector_dist_block",
        },
        "drainage_lines_vector": {
            "dataset_name": "Drainage",
            "layer_name": "dist_block",
        },
        "surface_water_bodies_vector": {
            "dataset_name": "Surface Water Bodies",
            "layer_name": "surface_waterbodies_dist_block",
        },
        "nrega_vector": {
            "dataset_name": "NREGA Assets",
            "layer_name": "dist_block",
        },
        "cropping_intensity_vector": {
            "dataset_name": "Cropping Intensity",
            "layer_name": "dist_block_intensity",
        },
        "stage_of_groundwater_extraction_vector": {
            "dataset_name": "SOGE",
            "layer_name": "soge_vector_dist_block",
        },
        "drought_frequency_vector": {
            "dataset_name": "Drought",
            "layer_name": "dist_block_drought",
        },
        "change_tree_cover_gain_raster": {
            "dataset_name": "Change Detection Raster",
            "layer_name": "change_dist_block_Afforestation",
        },
        "change_tree_cover_loss_raster": {
            "dataset_name": "Change Detection Raster",
            "layer_name": "change_dist_block_Deforestation",
        },
        "change_cropping_reduction_raster": {
            "dataset_name": "Change Detection Raster",
            "layer_name": "change_dist_block_Degradation",
        },
        "change_urbanization_raster": {
            "dataset_name": "Change Detection Raster",
            "layer_name": "change_dist_block_Urbanization",
        },
        "change_cropping_intensity_raster": {
            "dataset_name": "Change Detection Raster",
            "layer_name": "change_dist_block_CropIntensity",
        },
        "land_use_land_cover_raster": {
            "dataset_name": "LULC_level_3",
            "layer_name": "LULC_start_year_end_year_dist_block_level_3",
        },
        "terrain_raster": {
            "dataset_name": "Terrain Raster",
            "layer_name": "dist_block_terrain_raster",
        },
        "clart_raster": {
            "dataset_name": "CLART",
            "layer_name": "dist_block_clart",
        },
        "wri_restoration_raster": {
            "dataset_name": "Restoration Raster",
            "layer_name": "restoration_dist_block_raster",
        },
        "terrain_vector": {
            "dataset_name": "Terrain Vector",
            "layer_name": "dist_block_cluster",
        },
        "change_in_well_depth_vector": {
            "dataset_name": "Hydrology",
            "layer_name": "deltaG_well_depth_dist_block",
        },
        "natural_depressions_raster": {
            "dataset_name": "Natural Depression",
            "layer_name": "natural_depression_dist_block_raster",
        },
        "stream_order_raster": {
            "dataset_name": "Stream Order",
            "layer_name": "stream_order_dist_block_raster",
        },
        "distance_to_upstream_drainage_line_raster": {
            "dataset_name": "Distance to Drainage Line",
            "layer_name": "distance_to_drainage_line_dist_block_raster",
        },
        "catchment_area_singleflow_raster": {
            "dataset_name": "Catchment Area",
            "layer_name": "catchment_area_dist_block_raster",
        }        
    }

    active_tehsils = TehsilSOI.objects.filter(
        active_status=True,
        district__active_status=True,
        district__state__active_status=True,
    ).select_related("district", "district__state")

    # print(active_tehsils)

    for tehsil in active_tehsils:
        state = tehsil.district.state
        district = tehsil.district
        print(state.state_name, district.district_name, tehsil.tehsil_name)
        for layer_name_to_generate_raster in layer_names_to_generate_rasters:
            if layer_name_to_generate_raster == "land_use_land_cover_raster":
                lulc_year_range = [2023, 2024]
                for year in lulc_year_range:
                    print("year = ",year)

                    #check if STAC already exists for this layer
                    layer_name = layer_obj_and_name[layer_name_to_generate_raster][
                        "layer_name"
                    ]
                    start_year_last_two_digits = str(int(year) % 100)
                    end_year_last_two_digits = str((int(year) + 1) % 100)

                    print("start_year last 2 digits = ",start_year_last_two_digits)
                    print("end_year last 2 digits = ",end_year_last_two_digits)

                    formatted_layer_name = (
                        layer_name.replace(
                            "dist", valid_gee_text(district.district_name.lower())
                        )
                        .replace(
                            "block", valid_gee_text(tehsil.tehsil_name.lower())
                        )
                        .replace("start_year", start_year_last_two_digits)
                        .replace("end_year", end_year_last_two_digits)
                        .replace(" ", "_")
                    )
                    print(f"{formatted_layer_name = }")
                    layer_obj = (
                        Layer.objects.filter(
                            dataset__name=layer_obj_and_name[
                                layer_name_to_generate_raster
                            ]["dataset_name"],
                            layer_name=formatted_layer_name,
                        )
                        .order_by("-layer_version")
                        .first()
                    )

                if (layer_obj.is_stac_specs_generated == True):
                                print(
                                    f"stac spec {layer_name_to_generate_raster} already exists for for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                                )
                else:
                    try:
                        is_raster_stac_generated = (
                            generate_STAC_layerwise.generate_raster_stac(
                                state=state.state_name,
                                district=district.district_name,
                                block=tehsil.tehsil_name,
                                layer_name=layer_name_to_generate_raster,
                                start_year=year,
                                upload_to_s3=upload_s3,
                                generate_stac = True
                            )
                        )

                        if is_raster_stac_generated:
                            print(
                                f"stac spec {layer_name_to_generate_raster} generated for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                            )
                            if layer_obj:
                                layer_obj.is_stac_specs_generated = True
                                layer_obj.save()
                                print("db flag updated.....")
                            else:
                                print("db object not found========")
                        else:
                            print(
                                f"ISSUE IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                            )
                    except Exception as e:
                        print(
                            f"EXCEPTION IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name} and error is:- {e}"
                        )
            # elif layer_name_to_generate_raster in [
            #     "tree_canopy_cover_density_raster",
            #     "tree_canopy_height_raster",
            # ]:
                
            #     tree_year_range = [2017, 2018, 2019, 2020, 2021, 2022]
            #     for year in tree_year_range:
            #            #check if STAC specs are generated 
            #            layer_name = layer_obj_and_name[layer_name_to_generate_raster]["layer_name"]
            #            formatted_layer_name = (
            #                 layer_name.replace(
            #                     "dist", valid_gee_text(district.district_name.lower())
            #                 )
            #                 .replace(
            #                     "block", valid_gee_text(tehsil.tehsil_name.lower())
            #                 )
            #                 .replace(" ", "_"))
            #             layer_obj = (
            #                 Layer.objects.filter(
            #                     dataset__name=layer_obj_and_name[
            #                         layer_name_to_generate_raster
            #                     ]["dataset_name"],
            #                     layer_name=formatted_layer_name,
            #                 )
            #                 .order_by("-layer_version")
            #                 .first()
            #             )
            #             if (layer_obj.is_stac_specs_generated == True):
            #                                 print(
            #                                     f"stac spec {layer_name_to_generate_raster} already exists for for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
            #                                 )                                
            #             else:
            #                 try:
            #                     is_raster_stac_generated = (
            #                         generate_STAC_layerwise.generate_raster_stac(
            #                             state=state.state_name,
            #                             district=district.district_name,
            #                             block=tehsil.tehsil_name,
            #                             layer_name=layer_name_to_generate_raster,
            #                             start_year=year,
            #                             upload_to_s3=upload_s3,
            #                             generate_stac = True
            #                         )
            #                     )
        
            #                     if is_raster_stac_generated:
            #                         print(
            #                             f"stac spec {layer_name_to_generate_raster} generated for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
            #                         )
            #                         if layer_obj:
            #                             layer_obj.is_stac_specs_generated = True
            #                             layer_obj.save()
            #                             print("db flag updated.....")
            #                         else:
            #                             print("db object not found========")
            #                     else:
            #                         print(
            #                             f"ISSUE IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
            #                         )
            #                 except Exception as e:
            #                     print(
            #                         f"EXCEPTION IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name} and error is:- {e}"
            #                     )
            else:
                    layer_name = layer_obj_and_name[layer_name_to_generate_raster][
                        "layer_name"
                    ]
                    formatted_layer_name = (
                        layer_name.replace(
                            "dist", valid_gee_text(district.district_name.lower())
                        )
                        .replace("block", valid_gee_text(tehsil.tehsil_name.lower()))
                        .replace(" ", "_")
                    )
                    print(f"{formatted_layer_name = }")
                    layer_obj = (
                        Layer.objects.filter(
                            dataset__name=layer_obj_and_name[layer_name_to_generate_raster][
                                "dataset_name"
                            ],
                            layer_name=formatted_layer_name,
                        )
                        .order_by("-layer_version")
                        .first()
                    )

                    if (layer_obj.is_stac_specs_generated == True):
                                    print(
                                        f"stac spec {layer_name_to_generate_raster} already exists for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                                    )
                    else:
                        try:
                            is_raster_stac_generated = generate_STAC_layerwise.generate_raster_stac(
                                state=state.state_name,
                                district=district.district_name,
                                block=tehsil.tehsil_name,
                                layer_name=layer_name_to_generate_raster,
                                upload_to_s3=upload_s3,
                                generate_stac = True
                            )

                            if is_raster_stac_generated:
                                print(
                                    f"stac spec {layer_name_to_generate_raster} generated for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                                )
                                if layer_obj:
                                    layer_obj.is_stac_specs_generated = True
                                    layer_obj.save()
                                    print("db flag updated.....")
                                else:
                                    print("db object not found========")
                            else:
                                print(
                                    f"ISSUE IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                                )
                        except Exception as e:
                            print(
                                f"EXCEPTION IN GENERATING {layer_name_to_generate_raster} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name} and error is:- {e}"
                            )

                    
        for layer_name_to_generate_vector in layer_names_to_generate_vectors:
                
                layer_name = layer_obj_and_name[layer_name_to_generate_vector][
                    "layer_name"
                ]

                formatted_layer_name = layer_name.replace(
                    "dist", valid_gee_text(district.district_name.lower())
                ).replace("block", valid_gee_text(tehsil.tehsil_name.lower()))
                layer_obj = (
                    Layer.objects.filter(
                        dataset__name=layer_obj_and_name[layer_name_to_generate_vector][
                            "dataset_name"
                        ],
                        layer_name=formatted_layer_name,
                    )
                    .order_by("-layer_version")
                    .first()
                )
                # print(layer_obj)

                if (layer_obj.is_stac_specs_generated == True):
                                    print(
                                        f"stac spec {layer_name_to_generate_vector} already exists for for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                                    )
                else:
                    try:
                        is_vector_stac_generated = generate_STAC_layerwise.generate_vector_stac(
                            state=state.state_name,
                            district=district.district_name,
                            block=tehsil.tehsil_name,
                            layer_name=layer_name_to_generate_vector,
                            upload_to_s3=upload_s3,
                            generate_stac = True
                        )
                        # print("Vector STAC status =",is_vector_stac_generated)

                        if is_vector_stac_generated:
                            if layer_obj:
                                layer_obj.is_stac_specs_generated = True
                                layer_obj.save()
                                print("db flag updated.....")
                            else:
                                print("db object not found========")
                            print(
                                f"stac spec {layer_name_to_generate_vector} generated for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                            )
                        else:
                            print(
                                f"ISSUE IN GENERATING {layer_name_to_generate_vector} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name}"
                            )
                    except Exception as e:
                        print(
                            f"EXCEPTION IN GENERATING {layer_name_to_generate_vector} for {state.state_name}_{district.district_name}_{tehsil.tehsil_name} and error is:- {e}"
                        )
    print("========== ALL STAC LAYERS GENERATED FOR EXISTING LOCATION ==========")


generate_stac_spec()
