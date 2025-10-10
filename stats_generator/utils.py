import os
import requests, json
from django.http import HttpResponse, Http404
from rest_framework.response import Response
import pandas as pd
import geopandas as gpd
from collections import defaultdict
from datetime import datetime
from nrm_app.settings import GEOSERVER_URL, EXCEL_PATH
import numpy as np
from shapely.geometry import Point, shape
from .models import LayerInfo
from shapely.geometry.base import BaseGeometry


def fetch_layers_for_excel_generation():
    """
    Fetch all vector layers where `excel_to_be_generated` is True.
    """
    layers = LayerInfo.objects.filter(
        layer_type="vector", excel_to_be_generated=True
    ).values("layer_name", "workspace", "start_year", "end_year")
    return list(layers)


def get_url(workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{GEOSERVER_URL}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    print("Geojson url", geojson_url)
    return geojson_url


def get_vector_layer_geoserver(state, district, block):
    """Fetch vector layer data from GeoServer and save it as an Excel file."""
    print(f"Generate Stats excel for {state}_{district}_{block}")
    base_path = os.path.join(EXCEL_PATH, "data/stats_excel_files")
    district_path = os.path.join(
        base_path, state.replace(" ", "_").upper(), district.replace(" ", "_").upper()
    )
    os.makedirs(district_path, exist_ok=True)
    xlsx_file = os.path.join(district_path, f"{district}_{block}.xlsx")

    results = []
    with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
        for layer in fetch_layers_for_excel_generation():
            workspace = layer["workspace"]
            start_year = layer.get("start_year")
            end_year = layer.get("end_year")

            if "{district}" in layer["layer_name"] and "{block}" in layer["layer_name"]:
                layer_name = layer["layer_name"].format(district=district, block=block)
            else:
                layer_name = layer["layer_name"]

            print(f"Processing layer: {layer_name}")
            print(f"Workspace for the layer is: {workspace}")

            geojson_data = None
            try:
                url = get_url(workspace, layer_name)
                response = requests.get(url)
                response.raise_for_status()
                geojson_data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data for {layer_name}: {e}")
                results.append({"layer": layer_name, "status": "failed"})
                continue

            # Process the data based on workspace
            if workspace == "terrain":
                create_excel_for_terrain(geojson_data, xlsx_file, writer)
            elif (
                workspace == "terrain_lulc"
                and layer_name == f"{district}_{block}_lulc_slope"
            ):
                create_excel_for_terrain_lulc_slope(geojson_data, xlsx_file, writer)
            elif (
                workspace == "terrain_lulc"
                and layer_name == f"{district}_{block}_lulc_plain"
            ):
                create_excel_for_terrain_lulc_plain(geojson_data, xlsx_file, writer)
            elif workspace == "swb":
                create_excel_for_swb(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "nrega_assets":
                mws_file_geojson = os.path.join(district_path, "mws_annual.geojson")
                mws_lay_name = f"deltaG_well_depth_{district}_{block}"
                mws_file_url = get_url("mws_layers", mws_lay_name)

                try:
                    response = requests.get(mws_file_url)
                    response.raise_for_status()
                    mws_geojson_datas = response.json()
                except requests.exceptions.RequestException as e:
                    print(f"Failed to fetch MWS data: {e}")
                    continue

                create_excel_for_nrega_assets(
                    geojson_data,
                    mws_geojson_datas,
                    xlsx_file,
                    writer,
                    start_year,
                    end_year,
                )
                fetch_village_asset_count(
                    state, district, block, writer, xlsx_file, start_year, end_year
                )
                create_excel_mws_inters_villages(
                    mws_geojson_datas, xlsx_file, writer, district, block
                )
                # create_excel_village_inters_mwss(mws_geojson_datas, xlsx_file, writer, district, block)

            elif workspace == "crop_intensity":
                create_excel_crop_inten(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "cropping_drought":
                create_excel_crop_drou(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif (
                workspace == "mws_layers"
                and layer_name == f"deltaG_well_depth_{district}_{block}"
            ):
                parsed_data_annual_mws = parse_geojson_annual_mws(geojson_data)
                create_excel_annual_mws(parsed_data_annual_mws, xlsx_file, writer)
            elif (
                workspace == "mws_layers"
                and layer_name == f"deltaG_fortnight_{district}_{block}"
            ):
                processed_data = [
                    process_feature(feature) for feature in geojson_data["features"]
                ]
                create_excel_seas_mws(
                    processed_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "panchayat_boundaries":
                create_excel_for_village_boun(geojson_data, writer)
            elif workspace == "drought_causality":
                create_excel_for_drought_causality(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "ccd":
                create_excel_for_ccd(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "canopy_height":
                create_excel_for_ch(
                    geojson_data, xlsx_file, writer, start_year, end_year
                )
            elif workspace == "tree_overall_ch":
                create_excel_for_overall_tree_change(geojson_data, xlsx_file, writer)
            elif (
                workspace == "change_detection"
                and layer_name == f"change_vector_{district}_{block}_Afforestation"
            ):
                create_excel_chan_detection_afforestation(
                    geojson_data, xlsx_file, writer
                )
            elif (
                workspace == "change_detection"
                and layer_name == f"change_vector_{district}_{block}_CropIntensity"
            ):
                create_excel_chan_detection_cropintensity(
                    geojson_data, xlsx_file, writer
                )
            elif (
                workspace == "change_detection"
                and layer_name == f"change_vector_{district}_{block}_Deforestation"
            ):
                create_excel_chan_detection_deforestation(
                    geojson_data, xlsx_file, writer
                )
            elif (
                workspace == "change_detection"
                and layer_name == f"change_vector_{district}_{block}_Degradation"
            ):
                create_excel_chan_detection_degradation(geojson_data, xlsx_file, writer)
            elif (
                workspace == "change_detection"
                and layer_name == f"change_vector_{district}_{block}_Urbanization"
            ):
                create_excel_chan_detection_urbanization(
                    geojson_data, xlsx_file, writer
                )
            elif workspace == "restoration":
                create_excel_for_restoration(geojson_data, xlsx_file, writer)
            elif workspace == "aquifer":
                create_excel_for_aquifer(geojson_data, xlsx_file, writer)
            elif workspace == "soge":
                create_excel_for_soge(geojson_data, xlsx_file, writer)

            results.append({"layer": layer_name, "status": "success"})

    return results


def create_excel_for_soge(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "soge_dev_percent": properties["sgw_dev_pe"],
            "class_code": properties["code"],
            "class_name": properties["class"],
        }

        df_data.append(row)
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="soge_vector", index=False)
    print(f"Excel file created for soge_vector")


def create_excel_for_aquifer(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    # List of all principal aquifers
    principal_aquifers = [
        "Laterite",
        "Basalt",
        "Sandstone",
        "Shale",
        "Limestone",
        "Granite",
        "Schist",
        "Quartzite",
        "Charnockite",
        "Khondalite",
        "Banded Gneissic Complex",
        "Gneiss",
        "Intrusive",
        "Alluvium",
        "None",
    ]

    # First pass - collect all data
    for feature in features:
        properties = feature["properties"]
        area_aquifer = properties.get("%_area_aquifer") or properties.get("%_area_aqu")

        row = {
            "UID": properties.get("uid"),
            "area_in_ha": properties.get("area_in_ha"),
            "principal_aquifer": properties.get("Principal_"),
            "%_area_aquifer": area_aquifer,
        }

        df_data.append(row)

    df = pd.DataFrame(df_data).sort_values(["UID"])

    # Create empty columns for all aquifer percentages
    for aquifer in principal_aquifers:
        df[f"principle_aq_{aquifer}_percent"] = 0

    # Group by UID
    grouped = df.groupby("UID")

    processed_rows = []
    for uid, group in grouped:
        # Create a base row with common values
        base_row = {
            "UID": uid,
            "area_in_ha": group["area_in_ha"].iloc[
                0
            ],  # Should be same for all rows with same UID
            "aquifer_count": len(group),  # Count of unique aquifers for this UID
        }

        # Initialize all percentage columns
        aquifer_percentages = {
            f"principle_aq_{aq}_percent": 0 for aq in principal_aquifers
        }

        # Process each entry in the group
        for _, row in group.iterrows():
            # Set aquifer percentage - handle None case
            aquifer = (
                row["principal_aquifer"] if row["principal_aquifer"] != "" else "None"
            )
            if aquifer in principal_aquifers:
                aquifer_percentages[f"principle_aq_{aquifer}_percent"] += row[
                    "%_area_aquifer"
                ]

        # Find the dominant aquifer (the one with the highest percentage)
        max_aquifer = None
        max_percentage = 0

        for aquifer in principal_aquifers:
            percentage = aquifer_percentages[f"principle_aq_{aquifer}_percent"]
            if percentage > max_percentage:
                max_percentage = percentage
                max_aquifer = aquifer

        # Set aquifer_class based on the dominant aquifer
        # If the dominant aquifer is Alluvium, class is Alluvium, otherwise Hard Rock
        aquifer_class = "Alluvium" if max_aquifer == "Alluvium" else "Hard Rock"

        # Combine all data for this UID
        combined_row = {
            **base_row,
            "aquifer_class": aquifer_class,
            **aquifer_percentages,
        }

        processed_rows.append(combined_row)

    # Create final dataframe
    final_df = pd.DataFrame(processed_rows)

    # Round numeric values
    numeric_cols = final_df.select_dtypes(include=["number"]).columns
    final_df[numeric_cols] = final_df[numeric_cols].round(2)

    # Reorder columns for better readability
    original_cols = ["UID", "area_in_ha", "aquifer_class"]
    aquifer_cols = sorted(
        [col for col in final_df.columns if col.startswith("principle_aq_")]
    )

    final_df = final_df[original_cols + aquifer_cols]

    final_df.to_excel(writer, sheet_name="aquifer_vector", index=False)
    print(f"Excel file created for aquifer_vector")


def create_excel_for_restoration(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "wide_scale_restoration_area_in_ha": properties["Wide-scale"],
            "protection_area_in_ha": properties["Protection"],
            "mosaic_restoration_area_in_ha": properties["Mosaic Res"],
            "excluded_areas_in_ha": properties["Excluded A"],
        }

        df_data.append(row)
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="restoration_vector", index=False)
    print(f"Excel file created for restoration_vector")


def create_excel_for_overall_tree_change(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "afforestation_area_in_ha": properties["Afforestat"],
            "deforestation_area_in_ha": properties["Deforestat"],
            "degradation_area_in_ha": properties["Degradatio"],
            "improvement_area_in_ha": properties["Improvemen"],
            "missing_data_in_ha": properties["Missing Da"],
            "no_change_area_in_ha": properties["No_Change"],
            "partially_degraded_area_in_ha": properties["Partially_"],
        }

        df_data.append(row)
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="overall_tree_change", index=False)
    print(f"Excel file created for overall_tree_change")


def create_excel_for_ccd(data, xlsx_file, writer, start_year, end_year):
    df_data = []
    features = data["features"]
    for feature in features:
        properties = feature["properties"]

        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
        }

        for year in range(start_year, end_year):
            row["high_density_area_in_ha_" + str(year)] = properties.get(
                "hi_de_" + str(year), None
            )
            row["low_density_area_in_ha_" + str(year)] = properties.get(
                "lo_de_" + str(year), None
            )
            row["missing_data_area_in_ha_" + str(year)] = properties.get(
                "mi_da_" + str(year), None
            )

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="Canopy_Cover_Density", index=False)
    print(f"Excel file created for Canopy_Cover_Density")


def create_excel_for_ch(data, xlsx_file, writer, start_year, end_year):
    df_data = []
    features = data["features"]
    for feature in features:
        properties = feature["properties"]

        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
        }

        for year in range(start_year, end_year):
            row["short_trees_area_in_ha_" + str(year)] = properties.get(
                "sh_tr_" + str(year), None
            )
            row["medium_trees_area_in_ha_" + str(year)] = properties.get(
                "md_tr_" + str(year), None
            )
            row["tall_trees_area_in_ha_" + str(year)] = properties.get(
                "tl_tr_" + str(year), None
            )
            row["missing_data_area_in_ha_" + str(year)] = properties.get(
                "mi_da_" + str(year), None
            )

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="Canopy_height", index=False)
    print(f"Excel file created for Canopy_height")


def create_excel_for_drought_causality(data, xlsx_file, writer, start_year, end_year):
    df_data = []
    features = data["features"]
    for feature in features:
        properties = feature["properties"]

        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
        }

        for year in range(start_year, end_year):
            row["severe_moderate_drought_causality_" + str(year)] = properties.get(
                "se_mo_" + str(year), None
            )
            row["mild_drought_causality_" + str(year)] = properties.get(
                "mild_" + str(year), None
            )

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="drought_causality", index=False)
    print(f"Excel file created for drought_causality")


def create_excel_chan_detection_afforestation(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")
        df_data.append(
            {
                "UID": uid,
                "area_in_ha": properties.get("area_in_ha", None),
                "barren_to_forest_area_in_ha": properties.get("ba_fo", None),
                "built_up_to_forest_area_in_ha": properties.get("bu_fo", None),
                "farm_to_forest_area_in_ha": properties.get("fa_fo", None),
                "forest_to_forest_area_in_ha": properties.get("fo_fo", None),
                "scrub_land_to_forest_area_in_ha": properties.get("sc_fo", None),
                "total_afforestation_area_in_ha": properties.get("total_aff", None),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="change_detection_afforestation", index=False)
    print(f"Excel file created for change_detection_afforestation")


def create_excel_chan_detection_cropintensity(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")
        df_data.append(
            {
                "UID": uid,
                "area_in_ha": properties.get("area_in_ha", None),
                "double_to_single_area_in_ha": properties.get("do_si", None),
                "double_to_triple_area_in_ha": properties.get("do_tr", None),
                "single_to_double_area_in_ha": properties.get("si_do", None),
                "single_to_triple_area_in_ha": properties.get("si_tr", None),
                "triple_to_double_area_in_ha": properties.get("tr_do", None),
                "triple_to_single_area_in_ha": properties.get("tr_si", None),
                "no_change_area_in_ha": properties.get("same", None),
                "total_change_crop_intensity_area_in_ha": properties.get(
                    "total_chan", None
                ),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="change_detection_cropintensity", index=False)
    print(f"Excel file created for change_detection_cropintensity")


def create_excel_chan_detection_deforestation(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")
        df_data.append(
            {
                "UID": uid,
                "area_in_ha": properties.get("area_in_ha", None),
                "forest_to_barren_area_in_ha": properties.get("fo_ba", None),
                "forest_to_built_up_area_in_ha": properties.get("fo_bu", None),
                "forest_to_farm_area_in_ha": properties.get("fo_fa", None),
                "forest_to_forest_area_in_ha": properties.get("fo_fo", None),
                "forest_to_scrub_land_area_in_ha": properties.get("fo_sc", None),
                "total_deforestation_area_in_ha": properties.get("total_def", None),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="change_detection_deforestation", index=False)
    print(f"Excel file created for change_detection_deforestation")


def create_excel_chan_detection_degradation(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")
        df_data.append(
            {
                "UID": uid,
                "area_in_ha": properties.get("area_in_ha", None),
                "farm_to_barren_area_in_ha": properties.get("f_ba", None),
                "farm_to_built_up_area_in_ha": properties.get("f_bu", None),
                "farm_to_farm_area_in_ha": properties.get("f_f", None),
                "farm_to_scrub_land_area_in_ha": properties.get("f_sc", None),
                "total_degradation_area_in_ha": properties.get("total_deg", None),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="change_detection_degradation", index=False)
    print(f"Excel file created for change_detection_degradation")


def create_excel_chan_detection_urbanization(data, xlsx_file, writer):
    df_data = []
    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")
        df_data.append(
            {
                "UID": uid,
                "area_in_ha": properties.get("area_in_ha", None),
                "barren_shrub_to_built_up_area_in_ha": properties.get("b_bu", None),
                "built_up_to_built_up_area_in_ha": properties.get("bu_bu", None),
                "tree_farm_to_built_up_area_in_ha": properties.get("tr_bu", None),
                "water_to_built_up_area_in_ha": properties.get("w_bu", None),
                "total_urbanization_area_in_ha": properties.get("total_urb", None),
            }
        )

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="change_detection_urbanization", index=False)
    print(f"Excel file created for change_detection_urbanization")


def create_excel_mws_inters_villages(mws_geojson, xlsx_file, writer, district, block):
    print("Inside create_excel_mws_inters_villages")
    admin_layer_name = district + "_" + block
    admin_file_url = get_url("panchayat_boundaries", admin_layer_name)

    response = requests.get(admin_file_url)
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}")
        return

    village_geojson = response.json()

    def calculate_intersection_area(village_geom, mws_geom):
        if village_geom.intersects(mws_geom):
            intersection = village_geom.intersection(mws_geom)
            return intersection.area
        return 0

    mws_villages_dict = {}

    for mws_feature in mws_geojson["features"]:
        mws_uid = mws_feature["properties"]["uid"]
        mws_geom = shape(mws_feature["geometry"])
        village_ids = set()

        for village_feature in village_geojson["features"]:
            village_id = village_feature["properties"]["vill_ID"]
            if village_id == 0:
                continue

            village_geom = shape(village_feature["geometry"])
            area_intersected = calculate_intersection_area(village_geom, mws_geom)
            if area_intersected > 0:
                village_ids.add(village_id)
        if village_ids:
            mws_villages_dict[mws_uid] = list(village_ids)

    data = [
        {"MWS UID": mws_uid, "Village IDs": village_ids}
        for mws_uid, village_ids in mws_villages_dict.items()
    ]

    df = pd.DataFrame(data)
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)
    df.to_excel(writer, sheet_name="mws_intersect_villages", index=False)
    print("The data has been saved to mws_intersect_villages.xlsx")


# def create_excel_village_inters_mwss(mws_geojson, xlsx_file, writer, district, block):
#     print("Inside create_excel_village_inters_mwss")
#     admin_layer_name = district + '_' + block
#     admin_file_url = get_url('panchayat_boundaries', admin_layer_name)

#     response = requests.get(admin_file_url)
#     if response.status_code != 200:
#         print(f"Error fetching data: {response.status_code}")
#         return
#     village_geojson = response.json()

#     def calculate_intersection_area(village_geom: BaseGeometry, mws_geom: BaseGeometry) :
#         try:
#             # Check for empty geometries
#             if village_geom.is_empty or mws_geom.is_empty:
#                 return 0.0

#             # Fix invalid geometries if needed
#             if not village_geom.is_valid:
#                 village_geom = village_geom.buffer(0)
#             if not mws_geom.is_valid:
#                 mws_geom = mws_geom.buffer(0)

#             # Calculate intersection
#             if village_geom.intersects(mws_geom):
#                 intersection = village_geom.intersection(mws_geom)
#                 return intersection.area if not intersection.is_empty else 0.0

#             return 0.0

#         except Exception as e:
#             print(f"Error calculating intersection area: {e}")
#             return 0.0


#     data = []

#     processed_villages = set()

#     for village_feature in village_geojson['features']:
#         village_id = village_feature['properties']['vill_ID']
#         village_name = village_feature['properties']['vill_name']

#         village_key = (village_id, village_name)

#         if village_key in processed_villages:
#             continue
#         processed_villages.add(village_key)

#         village_geom = shape(village_feature['geometry'])

#         mws_uids = []
#         intersection_areas = []

#         for mws_feature in mws_geojson['features']:
#             mws_geom = shape(mws_feature['geometry'])
#             area_intersected = calculate_intersection_area(village_geom, mws_geom)
#             if area_intersected > 0:
#                 mws_uids.append(mws_feature['properties']['uid'])
#                 intersection_areas.append(area_intersected)

#         data.append({
#             'Village ID': village_id,
#             'Village Name': village_name,
#             'MWS UIDs': mws_uids,
#         })

#     df = pd.DataFrame(data)

#      ## for roundoff all numeric value upto 2 decimal
#     numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
#     df[numeric_cols] = df[numeric_cols].round(2)

#     df.to_excel(writer, sheet_name='village_intersect_mwss', index=False)
#     print("Excel created for village_intersect_mwss")


def create_excel_for_terrain(data, output_file, writer):
    print("Inside create_excel_for_terrain function")
    df_data = []

    terrain_description = {
        0: "Broad Sloppy and Hilly",
        1: "Mostly Plains",
        2: "Mostly Hills and Valleys",
        3: "Broad Plains and Slopes",
    }

    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "terrain_cluster_id": properties["terrainClu"],
            "terrain_description": terrain_description.get(properties["terrainClu"]),
            "hill_slope_area_percent": properties["hill_slope"],
            "plain_area_percent": properties["plain_area"],
            "ridge_area_percent": properties["ridge_area"],
            "slopy_area_percent": properties["slopy_area"],
            "valley_area_percent": properties["valley_are"],
        }

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="terrain", index=False)
    print(f"Excel file created for terrain vector")


def create_excel_for_terrain_lulc_slope(data, output_file, writer):
    df_data = []

    terrain_description = {
        0: "Broad Sloppy and Hilly",
        1: "Mostly Plains",
        2: "Mostly Hills and Valleys",
        3: "Broad Plains and Slopes",
    }

    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "terrain_cluster_id": properties["terrain_cl"],
            "terrain_description": terrain_description.get(properties["terrain_cl"]),
            "cluster_name": properties["clust_name"],
            "barren_area_percent": properties["barren"],
            "forests_area_percent": properties["forests"],
            "shrub_scrubs_area_percent": properties["shrub_scru"],
            "single_kharif_area_percent": properties["sing_khari"],
            "single_non_kharif_area_percent": properties["sing_non_k"],
            "double_cropping_area_percent": properties["double"],
            "triple_cropping_area_percent": properties["triple"],
        }

        df_data.append(row)
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="terrain_lulc_slope", index=False)
    print("Excel file created for terrain_lulc_slope")


def create_excel_for_terrain_lulc_plain(data, output_file, writer):
    df_data = []

    terrain_description = {
        0: "Broad Sloppy and Hilly",
        1: "Mostly Plains",
        2: "Mostly Hills and Valleys",
        3: "Broad Plains and Slopes",
    }

    features = data["features"]

    for feature in features:
        properties = feature["properties"]
        row = {
            "UID": properties["uid"],
            "area_in_ha": properties["area_in_ha"],
            "terrain_cluster_id": properties["terrain_cl"],
            "terrain_description": terrain_description.get(properties["terrain_cl"]),
            "cluster_name": properties["clust_name"],
            "barren_area_percent": properties["barren"],
            "forests_area_percent": properties["forest"],
            "shrub_scrubs_area_percent": properties["shrubs_scr"],
            "single_non_kharif_area_percent": properties["sing_non_k"],
            "single_kharif_area_percent": properties["sing_crop"],
            "double_cropping_area_percent": properties["double_cro"],
            "triple_cropping_area_percent": properties["triple_cro"],
        }

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="terrain_lulc_plain", index=False)
    print("Excel file created for terrain_lulc_plain")


def create_excel_for_swb(data, output_file, writer, start_year, end_year):
    df_data = []
    features = data.get("features", [])

    for feature in features:
        properties = feature.get("properties", {})
        uid = properties.get("MWS_UID", "Unknown")

        def calculate_area(base_area, percentage):
            if base_area == 0 or percentage == 0:
                return 0
            return base_area * (percentage / 100)

        parts = uid.split("_")
        num_uid_parts_is = [
            f"{parts[i]}_{parts[i + 1]}" for i in range(0, len(parts) - 1, 2)
        ]
        if len(parts) % 2 == 1:  # Check for an unpaired last part
            num_uid_parts_is.append(parts[-1])

        # Generate years dynamically based on start_year and end_year
        years = range(start_year, end_year)

        for num_uid_part in num_uid_parts_is:
            row = {"UID": num_uid_part}

            for year in years:
                short_year = f"{str(year)[-2:]}-{str(year + 1)[-2:]}"

                # Construct keys dynamically using the shortened year format
                total_area_key = f"area_{short_year}"
                kharif_key = f"k_{short_year}"
                rabi_key = f"kr_{short_year}"
                zaid_key = f"krz_{short_year}"

                # Get values from properties
                total_area = properties.get(total_area_key, 0)
                kharif_percentage = properties.get(kharif_key, 0)
                rabi_percentage = properties.get(rabi_key, 0)
                zaid_percentage = properties.get(zaid_key, 0)

                # Calculate areas
                row[f"total_area_in_ha_{year}-{year + 1}"] = total_area / len(
                    num_uid_parts_is
                )
                row[f"kharif_area_in_ha_{year}-{year + 1}"] = calculate_area(
                    total_area, kharif_percentage
                ) / len(num_uid_parts_is)
                row[f"rabi_area_in_ha_{year}-{year + 1}"] = calculate_area(
                    total_area, rabi_percentage
                ) / len(num_uid_parts_is)
                row[f"zaid_area_in_ha_{year}-{year + 1}"] = calculate_area(
                    total_area, zaid_percentage
                ) / len(num_uid_parts_is)

            # Add total SWB area
            row["total_swb_area_in_ha"] = properties.get("area_ored", 0) / len(
                num_uid_parts_is
            )
            df_data.append(row)

    df = pd.DataFrame(df_data)
    agg_dict = {col: "sum" for col in df.columns if col != "UID"}
    grouped_df = df.groupby("UID").agg(agg_dict).reset_index()

    df = grouped_df.sort_values(["UID"])

    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="surfaceWaterBodies_annual", index=False)
    print("Excel file created for surfaceWaterBodies_annual")


def create_excel_for_nrega_assets(
    nrega_data, mws_data, output_file, writer, start_year, end_year
):
    workCategoryMapping = {
        "SWC - Landscape level impact": "Soil and water conservation",
        "Agri Impact - HH, Community": "Land restoration",
        "Plantation": "Plantations",
        "Irrigation - Site level impact": "Irrigation on farms",
        "Irrigation Site level - Non RWH": "Other farm works",
        "Household Livelihood": "Off-farm livelihood assets",
        "Others - HH, Community": "Community assets",
    }

    mws = gpd.GeoDataFrame.from_features(mws_data["features"])
    nrega = gpd.GeoDataFrame.from_features(nrega_data["features"])

    # Set CRS if available in JSON
    if "crs" in mws_data:
        mws.set_crs(mws_data["crs"]["properties"]["name"], inplace=True)
    if "crs" in nrega_data:
        nrega.set_crs(nrega_data["crs"]["properties"]["name"], inplace=True)

    joined = gpd.sjoin(nrega, mws, how="inner", predicate="within")
    counts = {}

    df_data = []
    valid_years = range(start_year, end_year)

    date_formats = [
        "%d-%b-%y %H:%M:%S.%f",
        "%d-%b-%y %H:%M:%S",
        "%d-%m-%y %H:%M:%S.%f",
        "%d-%m-%y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S.%f",
        "%d-%b-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]

    for _, row in joined.iterrows():
        creation_t = row["creation_t"]
        work_category = row["WorkCatego"]
        mws_id = row["uid"]

        if isinstance(creation_t, pd.Timestamp):
            creation_t = creation_t.strftime("%d-%m-%Y %H:%M:%S")

        date_obj = None
        for date_format in date_formats:
            try:
                date_obj = datetime.strptime(creation_t, date_format)
                break
            except ValueError:
                continue

        if date_obj is None:
            continue

        year = date_obj.year
        if year < 100:
            year += 2000

        if year not in valid_years:
            continue

        category = workCategoryMapping.get(work_category, "Others - HH, Community")

        if mws_id not in counts:
            counts[mws_id] = {
                year: {cat: 0 for cat in workCategoryMapping.values()}
                for year in range(start_year, end_year)
            }

        if category not in counts[mws_id][year]:
            counts[mws_id][year][category] = 0
        counts[mws_id][year][category] += 1

    for mws_id, year_data in counts.items():
        row = {"mws_id": mws_id}
        for year, categories in year_data.items():
            for category in workCategoryMapping.values():
                count = categories.get(category, 0)
                row[f"{category}_count_{year}"] = count
        df_data.append(row)

    if not df_data:
        print("No data was collected for the DataFrame.")
    else:
        print(f"Collected {len(df_data)} rows of data for the DataFrame.")

    if df_data:
        df = pd.DataFrame(df_data)
        df.to_excel(writer, sheet_name="nrega_annual", index=False)
        print("Excel file created for nrega_annual")
        return "successfully created"
    else:
        print("No data available to write to Excel.")


def create_excel_village_nrega_assets(
    result_df, output_file, writer, all_villages_df, start_year, end_year
):
    workCategoryMapping = {
        "SWC - Landscape level impact": "Soil and water conservation",
        "Agri Impact - HH,  Community": "Land restoration",
        "Plantation": "Plantations",
        "Irrigation - Site level impact": "Irrigation on farms",
        "Irrigation Site level - Non RWH": "Other farm works",
        "Household Livelihood": "Off-farm livelihood assets",
        "Others - HH, Community": "Community assets",
    }

    # start_year, end_year = 2017, 2022
    year_range = range(start_year, end_year + 1)

    # Initialize all-zero DataFrame for all villages
    rows = []
    for _, row in all_villages_df.iterrows():
        base_row = {"vill_id": row["vill_ID"], "vill_name": row["vill_name"]}
        for year in year_range:
            for cat in workCategoryMapping.values():
                base_row[f"{cat}_count_{year}"] = 0
        rows.append(base_row)

    final_df = pd.DataFrame(rows)

    # Fill counts from assets
    for _, row in result_df.iterrows():
        creation_t = row["creation_t"]
        try:
            date_obj = pd.to_datetime(creation_t, errors="coerce")
            if pd.isnull(date_obj):
                continue
        except:
            continue

        year = date_obj.year
        if year not in year_range:
            continue

        category = workCategoryMapping.get(row["WorkCatego"])
        if not category:
            continue

        mask = (final_df["vill_id"] == row["vill_ID"]) & (
            final_df["vill_name"] == row["vill_name"]
        )
        col_name = f"{category}_count_{year}"
        final_df.loc[mask, col_name] += 1

    # Sort columns for clean layout
    id_cols = ["vill_id", "vill_name"]
    category_cols = sorted(
        [col for col in final_df.columns if col not in id_cols],
        key=lambda x: (int(x.split("_")[-1]), x),
    )
    final_df = final_df[id_cols + category_cols]

    # Save to Excel
    final_df = final_df.drop_duplicates(subset=["vill_id", "vill_name"])
    final_df.to_excel(writer, sheet_name="nrega_assets_village", index=False)
    print("Excel file created successfully with all villages.")


def fetch_village_asset_count(
    state, district, block, writer, output_file, start_year, end_year
):
    # 1. Read village data
    village_gdf = gpd.read_file(get_url("panchayat_boundaries", f"{district}_{block}"))[
        ["vill_ID", "vill_name", "geometry"]
    ].copy()
    print("Village data loaded")

    # 2. Get NREGA data with timeout to prevent hanging
    try:
        nrega_url = get_url("nrega_assets", f"{district}_{block}")
        print(f"Fetching: {nrega_url}")

        # Add timeout to prevent hanging
        response = requests.get(nrega_url, timeout=120)
        nrega_json = response.json()
        print("NREGA data fetched successfully")

    except Exception as e:
        print(f"Failed to get NREGA data: {e}")
        # Return villages with "No Asset" if API fails
        result_df = village_gdf[["vill_ID", "vill_name"]].copy()
        result_df["Asset ID"] = "No Asset"
        result_df["creation_t"] = "No Asset"
        result_df["WorkCatego"] = "No Asset"
        create_excel_village_nrega_assets(
            result_df,
            output_file,
            writer,
            village_gdf[["vill_ID", "vill_name"]],
            start_year,
            end_year,
        )
        return result_df, village_gdf[["vill_ID", "vill_name"]]

    # 3. Process asset features
    points_data = []
    features = nrega_json.get("features", [])
    print(f"Processing {len(features)} features")

    for feature in features:
        try:
            point = Point(feature["geometry"]["coordinates"])
            properties = feature["properties"]
            points_data.append(
                {
                    "geometry": point,
                    "Asset ID": properties.get("Asset ID", "MISSING"),
                    "creation_t": properties.get("creation_t", ""),
                    "WorkCatego": properties.get("WorkCatego", ""),
                }
            )
        except:
            continue

    # If no valid points, return no assets
    if not points_data:
        print("No valid asset points found")
        result_df = village_gdf[["vill_ID", "vill_name"]].copy()
        result_df["Asset ID"] = "No Asset"
        result_df["creation_t"] = "No Asset"
        result_df["WorkCatego"] = "No Asset"
        create_excel_village_nrega_assets(
            result_df,
            output_file,
            writer,
            village_gdf[["vill_ID", "vill_name"]],
            start_year,
            end_year,
        )
        return result_df, village_gdf[["vill_ID", "vill_name"]]

    print("Asset points created")

    # 4. Create points GeoDataFrame and match CRS
    points_gdf = gpd.GeoDataFrame(points_data, geometry="geometry")
    if village_gdf.crs != points_gdf.crs:
        points_gdf.set_crs(village_gdf.crs, inplace=True)

    # 5. Find which village each asset belongs to
    joined_gdf = gpd.sjoin(points_gdf, village_gdf, how="inner", predicate="within")

    # 6. Get asset + village info
    result_df = joined_gdf[
        ["vill_ID", "vill_name", "Asset ID", "creation_t", "WorkCatego"]
    ].copy()

    # 7. Add villages that have no assets
    villages_with_assets = result_df["vill_ID"].unique()
    no_asset_villages = village_gdf[
        ~village_gdf["vill_ID"].isin(villages_with_assets)
    ].copy()
    no_asset_villages["Asset ID"] = "No Asset"
    no_asset_villages["creation_t"] = "No Asset"
    no_asset_villages["WorkCatego"] = "No Asset"

    result_df = pd.concat(
        [
            result_df,
            no_asset_villages[
                ["vill_ID", "vill_name", "Asset ID", "creation_t", "WorkCatego"]
            ],
        ],
        ignore_index=True,
    )

    print("Processing complete")

    # 8. Create Excel file
    create_excel_village_nrega_assets(
        result_df,
        output_file,
        writer,
        village_gdf[["vill_ID", "vill_name"]],
        start_year,
        end_year,
    )

    return result_df, village_gdf[["vill_ID", "vill_name"]]


def analyze_results(village_asset_count, village_gdf):
    villages_with_counts = village_gdf.merge(
        village_asset_count, on="vill_ID", how="left"
    )
    villages_with_counts["asset_count"] = villages_with_counts["asset_count"].fillna(0)
    return villages_with_counts


def create_excel_crop_inten(data, output_file, writer, start_year, end_year):
    df_data = []

    features = data["features"]
    for feature in features:
        properties = feature.get("properties", {})
        uid = properties.get("uid", "Unknown")
        row = {"UID": uid, "area_in_ha": properties.get("area_in_ha", 0)}

        # Process each year in range using new key naming convention
        for year in range(start_year, end_year + 1):
            cropping_key = f"cropping_intensity_{year}"
            single_c_key = f"single_cropped_area_{year}"
            single_k_key = f"single_kharif_cropped_area_{year}"
            single_n_key = f"single_non_kharif_cropped_area_{year}"
            doubly_c_key = f"doubly_cropped_area_{year}"
            triply_c_key = f"triply_cropped_area_{year}"

            row[f"cropping_intensity_unit_less_{year}-{year + 1}"] = properties.get(
                cropping_key, 0
            )
            row[f"single_cropped_area_in_ha_{year}-{year + 1}"] = properties.get(
                single_c_key, 0
            )
            row[f"single_kharif_cropped_area_in_ha_{year}-{year + 1}"] = properties.get(
                single_k_key, 0
            )
            row[f"single_non_kharif_cropped_area_in_ha_{year}-{year + 1}"] = (
                properties.get(single_n_key, 0)
            )
            row[f"doubly_cropped_area_in_ha_{year}-{year + 1}"] = properties.get(
                doubly_c_key, 0
            )
            row[f"triply_cropped_area_in_ha_{year}-{year + 1}"] = properties.get(
                triply_c_key, 0
            )

        row["sum_area_in_ha"] = properties.get("sum", 0) / 10000
        df_data.append(row)

    # Create and format DataFrame
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    # Round numeric columns
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    # Write to Excel
    df.to_excel(writer, sheet_name="croppingIntensity_annual", index=False)
    print("Excel file created for cropping intensity.")


def create_excel_crop_drou(data, output_file, writer, start_year, end_year):
    df_data = []

    features = data["features"]
    for feature in features:
        properties = feature.get("properties", {})
        row = {"UID": properties.get("uid", "Unknown ID")}

        for year in range(start_year, end_year + 1):
            drlb_key = f"drlb_{year}"
            drysp_key = f"drysp_{year}"
            kh_cr_key = f"kh_cr_{year}"
            m_ons_key = f"m_ons_{year}"
            pcr_k_key = f"pcr_k_{year}"
            t_wks_key = f"t_wks_{year}"

            # Get drought levels (drlb) and count occurrences
            drlb_value = properties.get(drlb_key, "")
            row[f"No_Drought_in_weeks_{year}"] = drlb_value.count("0")
            row[f"Mild_in_weeks_{year}"] = drlb_value.count("1")
            row[f"Moderate_in_weeks_{year}"] = drlb_value.count("2")
            row[f"Severe_in_weeks_{year}"] = drlb_value.count("3")

            # Add other properties
            row[f"drysp_unit_4_weeks_{year}"] = properties.get(drysp_key, "0")
            row[f"kharif_cropped_sqkm_{year}"] = properties.get(kh_cr_key, "0")
            row[f"monsoon_onset_{year}"] = properties.get(m_ons_key, "0")
            row[f"kharif_cropped_area_percent_{year}"] = properties.get(pcr_k_key, "0")
            row[f"total_weeks_{year}"] = properties.get(t_wks_key, "0")

        df_data.append(row)

    # Create DataFrame
    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    # Write to Excel
    df.to_excel(writer, sheet_name="croppingDrought_kharif", index=False)
    print("Excel file created for cropping drought.")


def parse_geojson_annual_mws(data):
    features = data["features"]

    all_data = defaultdict(lambda: defaultdict(dict))

    for feature in features:
        properties = feature["properties"]
        uid = properties.get("uid", "Unknown")

        for key, value in properties.items():
            if isinstance(key, str) and isinstance(value, str):
                if key.startswith("20") and len(key) == 9:
                    year = key
                    try:
                        # Attempt to parse the value as JSON
                        year_data = json.loads(value.replace("'", '"'))
                        all_data[uid][year] = year_data
                    except Exception as e:
                        print(f"Couldn't parse data for {uid}, {key}: {e}")

    return all_data


def create_excel_annual_mws(data, output_file, writer):
    df_data = []
    year_columns = ["ET", "RunOff", "G", "DeltaG", "Precipitation", "WellDepth"]

    for uid, years in data.items():
        row = {"UID": uid}

        for year, metrics in years.items():
            start_year = year[:4]
            end_year = str(int(start_year) + 1)
            formatted_year = f"{start_year}-{end_year}"

            for col in year_columns:
                if col == "WellDepth":
                    column_name = f"{col}_in_m_{formatted_year}"
                    row[column_name] = metrics.get(col, "N/A")
                else:
                    column_name = f"{col}_in_mm_{formatted_year}"
                    row[column_name] = metrics.get(col, "N/A")

        df_data.append(row)

    df = pd.DataFrame(df_data)
    df = df.sort_values(["UID"])

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="hydrological_annual", index=False)
    print("Excel file created for hydrological_annual")


def parse_json_seas_mws(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def get_season(month):
    if month in (3, 4, 5, 6):
        return "zaid"
    elif month in (7, 8, 9, 10):
        return "kharif"
    elif month in (11, 12, 1, 2):
        return "rabi"


def process_feature(feature):
    uid = feature["properties"]["uid"]
    results = {
        "UID": uid,
        "precipitation": {"kharif": {}, "rabi": {}, "zaid": {}},
        "et": {"kharif": {}, "rabi": {}, "zaid": {}},
        "runoff": {"kharif": {}, "rabi": {}, "zaid": {}},
        "delta g": {"kharif": {}, "rabi": {}, "zaid": {}},
        "g": {"kharif": {}, "rabi": {}, "zaid": {}},
    }

    variable_mapping = {
        "Precipitation": "precipitation",
        "ET": "et",
        "RunOff": "runoff",
        "DeltaG": "delta g",
        "G": "g",
    }

    for key, value in feature["properties"].items():
        if key.startswith("20"):
            try:
                date = datetime.strptime(key, "%Y-%m-%d")
                year = date.year
                month = date.month
                season = get_season(month)
                if season == "rabi":
                    current_year = year - 1 if month in (1, 2) else year
                elif season == "zaid":
                    current_year = year - 1 if month in (3, 4, 5, 6) else year
                else:
                    current_year = year
                data = json.loads(value)

                for json_var, result_var in variable_mapping.items():
                    if json_var in data:
                        if current_year not in results[result_var][season]:
                            results[result_var][season][current_year] = 0.0
                        results[result_var][season][current_year] += float(
                            data[json_var]
                        )

            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error processing data for date {key}: {e}")
                continue
    return results


def create_excel_seas_mws(processed_data, output_file, writer, start_year, end_year):
    variables = ["precipitation", "et", "runoff", "delta g", "g"]
    seasons = ["kharif", "rabi", "zaid"]

    data = {"UID": []}
    for variable in variables:
        for year in range(start_year, end_year):
            for season in seasons:
                end_to_year = year + 1
                column_name = f"{variable}_{season}_in_mm_{year}-{end_to_year}"
                data[column_name] = []

    for feature_data in processed_data:
        data["UID"].append(feature_data["UID"])
        for variable in variables:
            for year in range(start_year, end_year):
                for season in seasons:
                    end_to_year = year + 1
                    column_name = f"{variable}_{season}_in_mm_{year}-{end_to_year}"
                    value = feature_data[variable].get(season, {}).get(year, 0.0)
                    data[column_name].append(value)

    df = pd.DataFrame(data)
    df = df.sort_values("UID")

    ## for roundoff all numeric value upto 2 decimal
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    df[numeric_cols] = df[numeric_cols].round(2)

    df.to_excel(writer, sheet_name="hydrological_seasonal", index=False)
    print(f"Excel file created hydrological_seasonal")


def create_excel_for_village_boun(old_geojson, writer):
    results = []

    village_data = {}

    for feature in old_geojson["features"]:
        properties = feature["properties"]

        # Extract properties
        state_census_ID = properties.get("state_cen", None)
        dist_census_ID = properties.get("dist_cen", None)
        block_census_ID = properties.get("block_cen", None)
        village_id = properties.get("vill_ID", None)
        village_name = properties.get("vill_name", None)

        # Initialize village data using village_id as the key
        if village_id not in village_data:
            village_data[village_id] = {
                "village_name": village_name,
                "TOT_P": 0,
                "P_LIT": 0,
                "P_SC": 0,
                "P_ST": 0,
                "state_census_ID": state_census_ID,
                "dist_census_ID": dist_census_ID,
                "block_census_ID": block_census_ID,
                "geometry": feature["geometry"],
            }

        village_data[village_id]["TOT_P"] += properties.get("TOT_P", 0)
        village_data[village_id]["P_LIT"] += properties.get("P_LIT", 0)
        village_data[village_id]["P_SC"] += properties.get("P_SC", 0)
        village_data[village_id]["P_ST"] += properties.get("P_ST", 0)

    for village_id, data in village_data.items():
        total_popu = data["TOT_P"]
        literacy_rate = data["P_LIT"] * 100 / total_popu if total_popu > 0 else 0.0
        total_SC_popu = data["P_SC"]
        total_ST_popu = data["P_ST"]
        sc_perce = (data["P_SC"] * 100 / total_popu) if total_popu > 0 else 0.0
        st_perce = (data["P_ST"] * 100 / total_popu) if total_popu > 0 else 0.0

        results.append(
            {
                "state_census_ID": data["state_census_ID"],
                "dist_census_ID": data["dist_census_ID"],
                "block_census_ID": data["block_census_ID"],
                "village_id": village_id,
                "village_name": data["village_name"],
                "total_population_count": total_popu,
                "total_SC_population_count": total_SC_popu,
                "total_ST_population_count": total_ST_popu,
                "literacy_rate_percent": literacy_rate,
                "SC_percent": sc_perce,
                "ST_percent": st_perce,
            }
        )

    results_df = pd.DataFrame(results)
    results_df.to_excel(writer, sheet_name="social_economic_indicator", index=False)

    print(f"Excel file created for social_economic_indicator")


def download_layers_excel_file(state, district, block):
    state_folder = state.replace(" ", "_").upper()
    district_folder = district.replace(" ", "_").upper()
    base_path = EXCEL_PATH + "data/stats_excel_files/"

    state_path = os.path.join(base_path, state_folder)
    if not os.path.exists(state_path):
        os.makedirs(state_path)

    district_path = os.path.join(state_path, district_folder)
    if not os.path.exists(district_path):
        os.makedirs(district_path)

    filename = f"{district}_{block}.xlsx"
    file_path = os.path.join(district_path, filename)
    if os.path.exists(file_path):
        return file_path
    else:
        return None


def read_tehsil_excel_to_json(state, district, block):
    file_path = download_layers_excel_file(state, district, block)

    if not file_path:
        return {"error": "Excel file not found."}

    try:
        xls = pd.read_excel(file_path, sheet_name=None)
        result = {}

        for sheet_name, df in xls.items():
            df.columns = [col.strip().lower() for col in df.columns]
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = df.where(pd.notnull(df), None)
            result[sheet_name] = df.to_dict(orient="records")

        return result

    except Exception as e:
        return {"error": str(e)}
