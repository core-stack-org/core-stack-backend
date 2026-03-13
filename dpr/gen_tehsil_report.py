import re
import os
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import pymannkendall as mk

import json

from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon, Point, LineString
from shapely.ops import unary_union
from scipy.spatial.distance import jensenshannon

from .models import Overpass_Block_Details

from nrm_app.settings import GEOSERVER_URL
from nrm_app.settings import OVERPASS_URL
from utilities.logger import setup_logger
import environ

env = environ.Env()
# reading .env file
environ.Env.read_env()

logger = setup_logger(__name__)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE_PATH = os.path.join(
    os.path.dirname(CURRENT_DIR), "dpr/utils", "block_patterns.json"
)

# TODO: fix the path issue <> shiv and ksheetiz
DATA_DIR_TEMP = env("EXCEL_DIR")


# ? MARK: HELPER FUNCTIONS
def get_geojson(workspace, layer_name):
    """Construct the GeoServer WFS request URL for fetching GeoJSON data."""
    geojson_url = f"{GEOSERVER_URL}/{workspace}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}:{layer_name}&outputFormat=application/json"
    return geojson_url


def create_gdf(feature_list):
    df = pd.DataFrame(feature_list)
    if not df.empty:
        df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return df


def filter_within_boundary(
    gdf, boundary, combined_geometry
):  # filter points and polygons within outer boundary
    polygons_gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]
    filtered_polygons_gdf = gpd.overlay(polygons_gdf, boundary, how="intersection")
    lines_gdf = gdf[gdf.geometry.type.isin(["LineString", "MultiLineString"])]
    filtered_lines_gdf = gpd.overlay(lines_gdf, boundary, how="intersection")
    points_gdf = gdf[gdf.geometry.type == "Point"]
    points_within_boundary = points_gdf[points_gdf.geometry.within(combined_geometry)]
    return filtered_polygons_gdf, filtered_lines_gdf, points_within_boundary


def calculate_river_length(filtered_gdf, target_crs="EPSG:3857"):
    if not filtered_gdf.empty:
        if filtered_gdf.crs.to_string() != target_crs:
            filtered_gdf = filtered_gdf.to_crs(target_crs)  # check polygon vs line
        filtered_gdf["length"] = filtered_gdf.geometry.length
        length_summary = filtered_gdf.groupby("name")["length"].sum().reset_index()

        length_list = []
        for _, row in length_summary.iterrows():
            length_info = {
                "name": row["name"],  # Retrieve the 'name'
                "length": row["length"],  # Summed length
            }
            length_list.append(length_info)

        return length_list
    return []


def calculate_area(filtered_gdf, target_crs="EPSG:3857"):  # calculate polygon area
    if not filtered_gdf.empty:
        if filtered_gdf.crs.to_string() != target_crs:
            filtered_gdf = filtered_gdf.to_crs(target_crs)

        filtered_gdf["area_sq_m"] = filtered_gdf.geometry.area

        area_summary = filtered_gdf  # .groupby('name')['area_sq_m'].sum().reset_index()

        area_list = []
        for _, row in filtered_gdf.iterrows():
            area_info = {
                "name": row["name"],  # Retrieve the 'name'
                "area_sq_m": row["area_sq_m"],  # Summed area in square meters
            }
            area_list.append(area_info)

        return area_list
    return []


def check_point_position(region_gdf, city_point):  # relative position of point
    if not region_gdf.empty:
        centroid = region_gdf.geometry.centroid.iloc[0]
        centroid_latitude = centroid.y
        centroid_longitude = centroid.x

        city_latitude = city_point.y
        city_longitude = city_point.x

        if city_latitude > centroid_latitude and city_longitude < centroid_longitude:
            return "north west"
        elif city_latitude > centroid_latitude and city_longitude > centroid_longitude:
            return "north east"
        elif city_latitude < centroid_latitude and city_longitude > centroid_longitude:
            return "south east"
        elif city_latitude < centroid_latitude and city_longitude < centroid_longitude:
            return "south west"
        else:
            return "centre"
    return "Invalid region geometry"


def load_block_patterns():  # read the json file wherever needed
    try:
        with open(JSON_FILE_PATH, "r") as file:
            block_patterns = json.load(file)
        return block_patterns
    except FileNotFoundError:
        logger.error(f"JSON file not found at {JSON_FILE_PATH}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return {}


# ? MARK: MAIN SECTION
def get_tehsil_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )
        # * Area of the Tehsil
        excel_file = pd.ExcelFile(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="terrain",
        )
        df["area_in_ha"] = pd.to_numeric(df["area_in_ha"], errors="coerce")

        total_area = int(df["area_in_ha"].sum())

        region_gdf = gpd.read_file(
            get_geojson(
                "mws_layers", "deltaG_well_depth" + "_" + district + "_" + block
            )
        )

        if region_gdf.crs != "EPSG:4326":
            region_gdf = region_gdf.to_crs("EPSG:4326")

        minx, miny, maxx, maxy = region_gdf.total_bounds
        overpass_query = f"""
        [out:json];
        (
            way["landuse"="forest"]({miny},{minx},{maxy},{maxx});
            way["boundary"="forest"]({miny},{minx},{maxy},{maxx});
            way["boundary"="forest_compartment"]({miny},{minx},{maxy},{maxx});
            way["natural"="wood"]({miny},{minx},{maxy},{maxx});

            way["natural"="water"]({miny},{minx},{maxy},{maxx});
            way["water"="lake"]({miny},{minx},{maxy},{maxx});
            way["water"="reservoir"]({miny},{minx},{maxy},{maxx});

            relation["natural"="water"]({miny},{minx},{maxy},{maxx});

            node["natural"="hill"]({miny},{minx},{maxy},{maxx});
            way["natural"="ridge"]({miny},{minx},{maxy},{maxx});

            node["place"="city"]({miny},{minx},{maxy},{maxx});
            node["place"="town"]({miny},{minx},{maxy},{maxx});

            way["highway"="motorway"]({miny},{minx},{maxy},{maxx});
            way["highway"="trunk"]({miny},{minx},{maxy},{maxx});
            way["highway"="primary"]({miny},{minx},{maxy},{maxx});
            way["highway"="secondary"]({miny},{minx},{maxy},{maxx});
            way["highway"="tertiary"]({miny},{minx},{maxy},{maxx});
            way["highway"="unclassified"]({miny},{minx},{maxy},{maxx});
            way["highway"="residential"]({miny},{minx},{maxy},{maxx});
            way["highway"="motorway_link"]({miny},{minx},{maxy},{maxx});
            way["highway"="trunk_link"]({miny},{minx},{maxy},{maxx});
            way["highway"="primary_link"]({miny},{minx},{maxy},{maxx});
            way["highway"="secondary_link"]({miny},{minx},{maxy},{maxx});
            way["highway"="tertiary_link"]({miny},{minx},{maxy},{maxx});
            way["highway"="living_street"]({miny},{minx},{maxy},{maxx});
            way["highway"="track"]({miny},{minx},{maxy},{maxx});
            way["highway"="road"]({miny},{minx},{maxy},{maxx});
            way["highway"="proposed"]({miny},{minx},{maxy},{maxx});
            way["highway"="construction"]({miny},{minx},{maxy},{maxx});
            way["highway"="milestone"]({miny},{minx},{maxy},{maxx});
        );
        out body;
        >;
        out skel qt;
        """

        response = {}
        block_detail = Overpass_Block_Details.objects.filter(
            location=f"{district}_{block}"
        ).first()

        if block_detail:
            logger.info(f"Using cached response for location: {district}_{block}")
            response = block_detail.overpass_response
        else:
            logger.info(
                f"No cached data found. Fetching from Overpass API for location: {district}_{block}"
            )

            try:
                response = requests.get(OVERPASS_URL, params={"data": overpass_query})
                response = response.json()

                block_detail = Overpass_Block_Details.objects.create(
                    location=f"{district}_{block}", overpass_response=response
                )
                logger.info(f"Response saved to DB for location: {district}_{block}")

            except Exception as e:
                logger.info("Not able to fetch the Overpass API Info", e)

        # dictionary for storage
        names = {
            "Forests": [],
            "Cities": [],
            "Hills": [],
            "Ridges": [],
            "Lakes": [],
            "Reservoirs": [],
            "Highways": [],
            "Rivers": [],
        }
        node_dict = {}
        if response and "elements" in response and response["elements"]:
            for element in response["elements"]:
                if element["type"] == "node":
                    node_dict[element["id"]] = (element["lon"], element["lat"])

        final_data = {
            "forests": [],
            "forests_mws": [],
            "reservoirs_mws": [],
            "reservoirs": [],
            "cities": [],
            "cities_mws": [],
            "lakes": [],
            "lakes_mws": [],
            "hills": [],
            "hills_mws": [],
            "ridges": [],
            "ridges_mws": [],
            "highway": [],
            "highway_mws": [],
            "river": [],
            "river_mws": [],
        }

        # List to hold the features
        points = []
        lines = []
        polygons = []
        forests = []
        cities = []
        hills = []
        ridges = []
        lakes = []
        reservoirs = []
        highway = []
        rivers = []
        if response and "elements" in response and response["elements"]:
            for element in response["elements"]:
                element_name = element.get("tags", {}).get("name")
                if element_name:
                    if element["type"] == "node":  # Point features
                        point = Point(node_dict[element["id"]])
                        points.append(
                            {
                                "geometry": point,
                                "tags": element.get("tags", {}),
                                "name": element_name,
                            }
                        )

                        # city or town
                        if element.get("tags", {}).get("place") in ["city", "town"]:
                            cities.append(
                                {
                                    "geometry": point,
                                    "tags": element.get("tags", {}),
                                    "name": element_name,
                                }
                            )
                            names["Cities"].append(f"City/Town: {element_name}")
                        # hills
                        if element.get("tags", {}).get("natural") in ["hill"]:
                            hills.append(
                                {
                                    "geometry": point,
                                    "tags": element.get("tags", {}),
                                    "name": element_name,
                                }
                            )
                            names["Hills"].append(f"Hills: {element_name}")

                    elif element["type"] == "way":  # Line or Polygon features
                        try:
                            coordinates = [
                                node_dict[node_id] for node_id in element["nodes"]
                            ]
                            if coordinates[0] == coordinates[-1]:
                                polygon = Polygon(coordinates)
                                polygons.append(
                                    {
                                        "geometry": polygon,
                                        "tags": element.get("tags", {}),
                                        "name": element_name,
                                    }
                                )

                                # Forests
                                if (
                                    element.get("tags", {}).get("landuse") == "forest"
                                    or element.get("tags", {}).get("natural") == "wood"
                                    or element.get("tags", {}).get("boundary")
                                    in ["forest", "forest_compartment"]
                                ):
                                    forests.append(
                                        {
                                            "geometry": polygon,
                                            "area": polygon.area,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Forests"].append(f"Forest: {element_name}")
                                # Lakes
                                if (
                                    (
                                        element.get("tags", {}).get("natural")
                                        == "water"
                                        or element.get("tags", {}).get("water")
                                        == "lake"
                                    )
                                    and not (
                                        element.get("tags", {}).get("landuse")
                                        == "reservoir"
                                    )
                                    and not element.get("tags", {}).get("water")
                                    == "river"
                                ):
                                    lakes.append(
                                        {
                                            "geometry": polygon,
                                            "area": polygon.area,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Lakes"].append(f"Lake: {element_name}")
                                # Reservoirs
                                if (
                                    element.get("tags", {}).get("landuse")
                                    == "reservoir"
                                ):
                                    reservoirs.append(
                                        {
                                            "geometry": polygon,
                                            "area": polygon.area,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Reservoirs"].append(
                                        f"Reservoir: {element_name}"
                                    )
                                # Rivers (if defined as a polygon)
                                if (
                                    element.get("tags", {}).get("natural") == "water"
                                    and element.get("tags", {}).get("water") == "river"
                                ) or element.get("tags", {}).get(
                                    "waterway"
                                ) == "riverbank":
                                    rivers.append(
                                        {
                                            "geometry": polygon,
                                            "area": polygon.area,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Rivers"].append(f"River: {element_name}")
                            else:  # Line
                                line = LineString(coordinates)
                                lines.append(
                                    {
                                        "geometry": line,
                                        "tags": element.get("tags", {}),
                                        "name": element_name,
                                    }
                                )

                                # ridges
                                if element.get("tags", {}).get("natural") == "ridge":
                                    ridges.append(
                                        {
                                            "geometry": line,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Ridges"].append(f"Ridge: {element_name}")
                                # highways
                                if "highway" in element.get("tags", {}):
                                    road_type = element["tags"]["highway"]
                                    if road_type in [
                                        "motorway",
                                        "trunk",
                                        "primary",
                                        "secondary",
                                        "tertiary",
                                        "unclassified",
                                        "residential",
                                        "motorway_link",
                                        "trunk_link",
                                        "primary_link",
                                        "secondary_link",
                                        "tertiary_link",
                                        "living_street",
                                        "track",
                                        "road",
                                        "proposed",
                                        "construction",
                                        "milestone",
                                    ]:
                                        highway.append(
                                            {
                                                "geometry": line,
                                                "tags": element.get("tags", {}),
                                                "name": element_name,
                                            }
                                        )
                                        names["Highways"].append(
                                            (f"Highway: {element_name}")
                                        )
                                if (
                                    (
                                        element.get("tags", {}).get("natural")
                                        == "water"
                                        and element.get("tags", {}).get("water")
                                        == "river"
                                    )
                                    or element.get("tags", {}).get("waterway")
                                    == "river"
                                    or element.get("tags", {}).get("waterway")
                                    == "riverbank"
                                ):
                                    rivers.append(
                                        {
                                            "geometry": line,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Rivers"].append(f"River: {element_name}")

                        except KeyError:
                            pass

        # DataFrames for plotting
        forests_df = create_gdf(forests)
        cities_df = create_gdf(cities)
        hill_df = create_gdf(hills)
        ridges_df = create_gdf(ridges)
        lakes_df = create_gdf(lakes)
        reservoirs_df = create_gdf(reservoirs)
        river_df = create_gdf(rivers)

        buffered_geometries = region_gdf.geometry.buffer(0.005)  # Adjust buffer size

        # Create single outer boundary
        combined_geometry = unary_union(buffered_geometries)

        if isinstance(combined_geometry, MultiPolygon):
            outer_boundary = [
                Polygon(geom.exterior) for geom in combined_geometry.geoms
            ]
        elif isinstance(combined_geometry, Polygon):
            outer_boundary = [combined_geometry]
        else:
            outer_boundary = []

        outer_boundary_gdf = gpd.GeoDataFrame(
            geometry=outer_boundary, crs=region_gdf.crs
        )

        if not forests_df.empty:
            filtered_forests_gdf, forest_lines, forests_points = filter_within_boundary(
                forests_df, outer_boundary_gdf, combined_geometry
            )
            final_data["forests"] = calculate_area(filtered_forests_gdf)

        if not lakes_df.empty:
            filtered_lakes_gdf, lake_lines, lakes_points = filter_within_boundary(
                lakes_df, outer_boundary_gdf, combined_geometry
            )
            final_data["lakes"] = calculate_area(filtered_lakes_gdf)

        if not reservoirs_df.empty:
            filtered_reservoirs_gdf, reservoir_lines, reservoirs_points = (
                filter_within_boundary(
                    reservoirs_df, outer_boundary_gdf, combined_geometry
                )
            )
            final_data["reservoirs"] = calculate_area(filtered_reservoirs_gdf)

        if not cities_df.empty:
            filtered_cities_gdf, city_lines, cities_points = filter_within_boundary(
                cities_df, outer_boundary_gdf, combined_geometry
            )
            if not cities_points.empty:
                for index, city in cities_points.iterrows():
                    city_point = city["geometry"]
                    name = city["tags"]["name"]
                    position = check_point_position(outer_boundary_gdf, city_point)
                    final_data["cities"].append({"name": name, "position": position})

        if not hill_df.empty:
            filtered_hills_gdf, hill_lines, hills_points = filter_within_boundary(
                hill_df, outer_boundary_gdf, combined_geometry
            )
            if not hills_points.empty:
                for index, hill in hills_points.iterrows():
                    hill_point = hill["geometry"]
                    name = hill["tags"]["name"]
                    position = check_point_position(outer_boundary_gdf, hill_point)
                    final_data["hills"].append({"name": name, "position": position})

        if not ridges_df.empty:
            filtered_ridges_gdf, ridge_lines, ridges_points = filter_within_boundary(
                ridges_df, outer_boundary_gdf, combined_geometry
            )
            if not ridge_lines.empty:
                for index, ridge in ridge_lines.iterrows():
                    ridge_point = ridge["geometry"]
                    name = ridge["tags"]["name"]
                    final_data["ridges"].append({"name": name})

        if not river_df.empty:
            filtered_river_gdf, river_lines, river_points = filter_within_boundary(
                river_df, outer_boundary_gdf, combined_geometry
            )
            final_data["river"] = calculate_river_length(river_lines)
            final_data["river"] += calculate_river_length(filtered_river_gdf)

        # Minimum area threshold (1 hectare = 10,000 square meters)
        MIN_AREA_THRESHOLD = 10000  # 1 hectare in square meters

        # ? Block Parameters

        parameter_block = f""

        if final_data["cities"]:
            city_names = [city["name"] for city in final_data["cities"]]
            parameter_block += f" has towns and cities of "
            if len(city_names) == 1:
                parameter_block += city_names[0]
            elif len(city_names) == 2:
                parameter_block += " and ".join(city_names)
            else:
                parameter_block += (
                    ", ".join(city_names[:-1]) + ", and " + city_names[-1]
                )

        if final_data["hills"] or final_data["ridges"]:
            temp = [hill["name"] for hill in final_data["hills"]]
            temp += [hill["name"] for hill in final_data["ridges"]]

            temp_features = ", ".join(temp)
            parameter_block += f". Key natural features such as {temp_features} shape the Tehsil landscape and impact water flow"

        if final_data["forests"]:
            large_forests = [
                f for f in final_data["forests"] if f["area_sq_m"] >= MIN_AREA_THRESHOLD
            ]
            if large_forests:
                parameter_block += (
                    f". Part of {large_forests[0]['name']}, covering roughly "
                    f"{round(large_forests[0]['area_sq_m'] / 10000, 1)} hectares, lies within the Tehsil supporting local wildlife and promoting biodiversity"
                )

        if final_data["lakes"] or final_data["reservoirs"]:
            large_lakes = [
                lake
                for lake in final_data["lakes"]
                if lake["area_sq_m"] >= MIN_AREA_THRESHOLD
            ]
            large_reservoirs = [
                res
                for res in final_data["reservoirs"]
                if res["area_sq_m"] >= MIN_AREA_THRESHOLD
            ]

            if large_lakes or large_reservoirs:
                rname = [temp["name"] for temp in large_lakes]
                rname += [temp["name"] for temp in large_reservoirs]
                rarea = [
                    str(round(temp["area_sq_m"] / 10000, 1)) for temp in large_lakes
                ]
                rarea += [
                    str(round(temp["area_sq_m"] / 10000, 1))
                    for temp in large_reservoirs
                ]

                """
                Additionally, large water bodies such as Lake Alpha, Reservoir Beta, and Pond Gamma span about 120, 85, and 40 hectares respectively within the Tehsil.
                Additionally, waterbody XXX (x area), waterbody YYY (y area), and waterbody ZZZ (z area) flow in this block, providing essential resources for irrigation, fishing, and drinking water.
                """
                parameter_block += ". Additionally, "

                if len(rname) == 1:
                    parameter_block += (
                        f"waterbody {rname[0]} ({rarea[0]} hectares area)"
                    )
                else:
                    for i in range(len(rname)):
                        if i == len(rname) - 1:
                            parameter_block += (
                                f"and waterbody {rname[i]} ({rarea[i]} hectares area)"
                            )
                        else:
                            parameter_block += (
                                f"waterbody {rname[i]} ({rarea[i]} hectares area), "
                            )

                parameter_block += " flow in this block, providing essential resources for irrigation, fishing, and drinking water"

        if final_data["river"]:
            rname = [temp["name"] for temp in final_data["river"]]
            rarea = [
                str(round((temp["length"]) / 1000, 1)) for temp in final_data["river"]
            ]

            parameter_block += f". The "
            if len(rname) == 1:
                parameter_block += rname[0]
            elif len(rname) == 2:
                parameter_block += " and ".join(rname)
            else:
                parameter_block += ", ".join(rname[:-1]) + ", and " + rname[-1]
            parameter_block += f" flowing "
            if len(rname) == 1:
                parameter_block += rarea[0]
            elif len(rname) == 2:
                parameter_block += " and ".join(rarea)
            else:
                parameter_block += ", ".join(rarea[:-1]) + ", and " + rarea[-1]
            parameter_block += f"  kilometers within the tehsil, serve"
            if len(rname) == 1:
                parameter_block += "s"
            parameter_block += (
                f" as a crucial water source for agriculture and daily needs"
            )

        if parameter_block == "":
            parameter_block = f"The Tehsil {block.capitalize()} lies in district {district.capitalize()} in {state.capitalize()}."
        else:
            parameter_block = (
                f"The Tehsil {block.capitalize()} having total area {total_area:,} hectares"
                + parameter_block
                + "."
            )

        return parameter_block

    except Exception as e:
        logger.info("The geojson is empty !", e)
        return "", ""


def get_pattern_intensity(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        block_patterns = load_block_patterns()

        df_temp = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")

        # Initialize dictionaries to track intensity and pattern details
        mws_pattern_intensity = {}
        mws_active_patterns = {}
        active_patterns = set()  # NEW: Track distinct active patterns

        for uid in df_temp["UID"]:
            mws_pattern_intensity[uid] = 0
            mws_active_patterns[uid] = []

        # Iterate through ALL categories (Agriculture, Forest & Livestock, Health, Socio-economic)
        for category_name, patterns in block_patterns.items():

            # Iterate through each pattern in this category
            for pattern in patterns:
                pattern_name = pattern.get("Name")
                indicators = pattern.get("values", [])

                # Dictionary to store UIDs that pass each indicator
                uids_per_indicator = {}

                # Iterate through each indicator for this pattern
                for idx, indicator in enumerate(indicators):
                    sheet_name = indicator.get("sheet_name")
                    indicator_type = indicator.get("type")
                    column = indicator.get("column")
                    comparison = indicator.get("comparison")
                    comp_type = indicator.get("comp_type")
                    trend = indicator.get("trend")
                    percentage = indicator.get("percentage")

                    # Initialize set for this indicator
                    uids_per_indicator[idx] = set()

                    # Handle multiple sheets (Type 7)
                    if isinstance(sheet_name, list):
                        dataframes = {}

                        # Load each sheet
                        try:
                            for sheet in sheet_name:
                                df = pd.read_excel(file_path, sheet_name=sheet)
                                dataframes[sheet] = df

                            # Type 7 logic
                            if indicator_type == 7:
                                # Get the two dataframes
                                df1 = dataframes[sheet_name[0]]
                                df2 = dataframes[sheet_name[1]]

                                # Column names from the indicator
                                sheet1_columns = column[0]
                                sheet2_columns = column[1]

                                mws_uid_col = sheet1_columns[0]
                                village_ids_col = sheet1_columns[1]

                                village_id_col = sheet2_columns[0]
                                metric_col = sheet2_columns[1]

                                # For each MWS UID in the first sheet
                                for index, row in df1.iterrows():
                                    mws_uid = row[mws_uid_col]
                                    village_ids = row[village_ids_col]

                                    # Parse village IDs
                                    if isinstance(village_ids, str):
                                        # Try to parse as list string
                                        try:
                                            import ast

                                            village_ids = ast.literal_eval(village_ids)
                                        except:
                                            # If parsing fails, try JSON
                                            try:
                                                village_ids = json.loads(village_ids)
                                            except:
                                                # Skip if we can't parse
                                                continue

                                    # Ensure village_ids is a list
                                    if not isinstance(village_ids, list):
                                        continue

                                    # Get metric values for each village ID
                                    metric_values = []
                                    for village_id in village_ids:
                                        # Find the row in df2 with this village_id
                                        matching_rows = df2[
                                            df2[village_id_col] == village_id
                                        ]
                                        if not matching_rows.empty:
                                            metric_val = matching_rows.iloc[0][
                                                metric_col
                                            ]
                                            if pd.notna(metric_val):
                                                metric_values.append(float(metric_val))

                                    # Calculate average metric across all villages
                                    if len(metric_values) > 0:
                                        avg_metric = sum(metric_values) / len(
                                            metric_values
                                        )

                                        # Compare using comp_type
                                        passed = False

                                        if comp_type == 1:
                                            passed = avg_metric == comparison
                                        elif comp_type == 2:
                                            passed = avg_metric != comparison
                                        elif comp_type == 3:
                                            passed = avg_metric > comparison
                                        elif comp_type == 4:
                                            passed = avg_metric < comparison
                                        elif comp_type == 5:
                                            passed = avg_metric >= comparison
                                        elif comp_type == 6:
                                            passed = avg_metric <= comparison

                                        if passed:
                                            uids_per_indicator[idx].add(mws_uid)

                        except Exception as e:
                            print(f"    ✗ Error loading sheets: {e}")

                    # Handle single sheet
                    else:
                        try:
                            df = pd.read_excel(file_path, sheet_name=sheet_name)

                            # Type 2: Class/category comparison
                            if indicator_type == 2:
                                for index, row in df.iterrows():
                                    uid = row["UID"]
                                    value = row[column]

                                    passed = False

                                    if comp_type == 2:  # Not Equal to
                                        if isinstance(comparison, str):
                                            passed = (
                                                str(value).lower() != comparison.lower()
                                            )
                                        else:
                                            passed = value == comparison
                                    elif comp_type == 3:  # Greater than
                                        passed = value > comparison
                                    elif comp_type == 4:  # Less than
                                        passed = value < comparison

                                    if passed:
                                        uids_per_indicator[idx].add(uid)

                            # Type 1: Direct column comparison
                            elif indicator_type == 1:
                                # Find all columns that start with the base column name
                                avg_columns = [
                                    col for col in df.columns if col.startswith(column)
                                ]
                                avg_columns.sort()

                                # For each UID, calculate average
                                for index, row in df.iterrows():
                                    uid = row["UID"]

                                    # Collect values from all matching columns
                                    values = []
                                    for col in avg_columns:
                                        val = row[col]
                                        if pd.notna(val):
                                            values.append(float(val))

                                    # Calculate average if we have values
                                    if len(values) > 0:
                                        average = sum(values) / len(values)

                                        # Apply comparison based on comp_type
                                        passed = False

                                        if comp_type == 1:  # Equals to
                                            passed = average == comparison
                                        elif comp_type == 2:  # Not Equals to
                                            passed = average != comparison
                                        elif comp_type == 3:  # Greater than
                                            passed = average > comparison
                                        elif comp_type == 4:  # Less than
                                            passed = average < comparison
                                        elif comp_type == 5:  # Greater than equal to
                                            passed = average >= comparison
                                        elif comp_type == 6:  # Less than equal to
                                            passed = average <= comparison

                                        if passed:
                                            uids_per_indicator[idx].add(uid)

                            # Type 3: Trend analysis
                            elif indicator_type == 3:
                                # Find all columns that start with the base column name
                                trend_columns = [
                                    col for col in df.columns if col.startswith(column)
                                ]

                                # For each UID, analyze the trend
                                for index, row in df.iterrows():
                                    uid = row["UID"]

                                    # Extract values from all trend columns
                                    values = []
                                    for col in trend_columns:
                                        val = row[col]
                                        if pd.notna(val):
                                            values.append(float(val))

                                    if len(values) >= 3:
                                        try:
                                            mk_result = mk.original_test(values)

                                            if (
                                                trend == 1
                                                and mk_result.trend == "increasing"
                                            ):
                                                uids_per_indicator[idx].add(uid)
                                            elif (
                                                trend == 0
                                                and mk_result.trend == "no trend"
                                            ):
                                                uids_per_indicator[idx].add(uid)
                                            elif (
                                                trend == -1
                                                and mk_result.trend == "decreasing"
                                            ):
                                                uids_per_indicator[idx].add(uid)

                                        except Exception as e:
                                            # If Mann-Kendall fails, skip this UID
                                            pass

                            # Type 4: Percentage calculation
                            elif indicator_type == 4:
                                total_base_col = column[0]
                                part_base_col = column[1]

                                # Find all years from the total column
                                total_columns = [
                                    col
                                    for col in df.columns
                                    if col.startswith(total_base_col)
                                ]
                                years = []
                                for col in total_columns:
                                    year = col.replace(total_base_col, "")
                                    if year:
                                        years.append(year)

                                years.sort()

                                # Convert percentage string to float for comparison
                                percentage_threshold = float(percentage)

                                # For each UID, calculate average percentage across years
                                for index, row in df.iterrows():
                                    uid = row["UID"]
                                    yearly_percentages = []

                                    # Calculate percentage for each year
                                    for year in years:
                                        total_col = total_base_col + year
                                        part_col = part_base_col + year

                                        # Check if both columns exist
                                        if (
                                            total_col in df.columns
                                            and part_col in df.columns
                                        ):
                                            total_val = row[total_col]
                                            part_val = row[part_col]

                                            # Calculate percentage if values are valid
                                            if (
                                                pd.notna(total_val)
                                                and pd.notna(part_val)
                                                and total_val > 0
                                            ):
                                                year_percentage = (
                                                    float(part_val) / float(total_val)
                                                ) * 100
                                                yearly_percentages.append(
                                                    year_percentage
                                                )

                                    # Calculate average percentage
                                    if len(yearly_percentages) > 0:
                                        avg_percentage = sum(yearly_percentages) / len(
                                            yearly_percentages
                                        )

                                        # Apply comparison based on comp_type
                                        passed = False

                                        if comp_type == 1:
                                            passed = (
                                                avg_percentage == percentage_threshold
                                            )
                                        elif comp_type == 2:
                                            passed = (
                                                avg_percentage != percentage_threshold
                                            )
                                        elif comp_type == 3:
                                            passed = (
                                                avg_percentage > percentage_threshold
                                            )
                                        elif comp_type == 4:
                                            passed = (
                                                avg_percentage < percentage_threshold
                                            )
                                        elif comp_type == 5:
                                            passed = (
                                                avg_percentage >= percentage_threshold
                                            )
                                        elif comp_type == 6:
                                            passed = (
                                                avg_percentage <= percentage_threshold
                                            )

                                        if passed:
                                            uids_per_indicator[idx].add(uid)

                            # Type 5: Multiple column comparison
                            elif indicator_type == 5:
                                # Find all unique years across all base columns
                                years = set()
                                for base_col in column:
                                    year_columns = [
                                        col
                                        for col in df.columns
                                        if col.startswith(base_col)
                                    ]
                                    for col in year_columns:
                                        year = col.replace(base_col, "")
                                        if year:
                                            years.add(year)

                                years = sorted(list(years))

                                # For each UID, check how many drought years exist
                                for index, row in df.iterrows():
                                    uid = row["UID"]
                                    drought_year_count = 0

                                    # For each year, sum values from all base columns
                                    for year in years:
                                        year_sum = 0
                                        valid_year = True

                                        for base_col in column:
                                            col_name = base_col + year
                                            if col_name in df.columns:
                                                val = row[col_name]
                                                if pd.notna(val):
                                                    year_sum += float(val)
                                                else:
                                                    valid_year = False
                                                    break
                                            else:
                                                valid_year = False
                                                break

                                        # Check if this year qualifies as a drought year (sum > 5)
                                        if valid_year and year_sum > 5:
                                            drought_year_count += 1

                                    # Now check if drought_year_count meets the comparison criteria
                                    passed = False

                                    if comp_type == 1:  # Equals to
                                        passed = drought_year_count == comparison
                                    elif comp_type == 2:  # Not Equals to
                                        passed = drought_year_count != comparison
                                    elif comp_type == 3:  # Greater than
                                        passed = drought_year_count > comparison
                                    elif comp_type == 4:  # Less than
                                        passed = drought_year_count < comparison
                                    elif comp_type == 5:  # Greater than equal to
                                        passed = drought_year_count >= comparison
                                    elif comp_type == 6:  # Less than equal to
                                        passed = drought_year_count <= comparison

                                    if passed:
                                        uids_per_indicator[idx].add(uid)

                            # Type 6: Presence check
                            elif indicator_type == 6:
                                # Get all UIDs present in this sheet
                                if "UID" in df.columns:
                                    present_uids = df["UID"].unique()

                                    # Add all present UIDs to the passing set
                                    for uid in present_uids:
                                        if pd.notna(uid):
                                            uids_per_indicator[idx].add(uid)

                            # Type 8: Count-based logic
                            elif indicator_type == 8:
                                uid_col = column[0]
                                metric_base_cols = column[1:]

                                # For each row/UID
                                for index, row in df.iterrows():
                                    mws_uid = row[uid_col]
                                    total_count = 0

                                    # find all year columns
                                    for base_col in metric_base_cols:
                                        metric_columns = [
                                            col
                                            for col in df.columns
                                            if col.startswith(base_col)
                                        ]

                                        # Sum values across all year columns for this metric
                                        for col in metric_columns:
                                            val = row[col]
                                            if pd.notna(val):
                                                total_count += float(val)

                                    # Compare total count using comp_type
                                    passed = False

                                    if comp_type == 1:  # Equals to
                                        passed = total_count == comparison
                                    elif comp_type == 2:  # Not Equals to
                                        passed = total_count != comparison
                                    elif comp_type == 3:  # Greater than
                                        passed = total_count > comparison
                                    elif comp_type == 4:  # Less than
                                        passed = total_count < comparison
                                    elif comp_type == 5:  # Greater than equal to
                                        passed = total_count >= comparison
                                    elif comp_type == 6:  # Less than equal to
                                        passed = total_count <= comparison

                                    if passed:
                                        uids_per_indicator[idx].add(mws_uid)

                        except Exception as e:
                            print(f"Error loading sheet '{sheet_name}': {e}")

                # Find UIDs that passed ALL indicators (intersection of all sets)
                if uids_per_indicator:
                    # Start with UIDs from first indicator
                    common_uids = uids_per_indicator[0]

                    # Intersect with UIDs from remaining indicators
                    for idx in range(1, len(uids_per_indicator)):
                        common_uids = common_uids.intersection(uids_per_indicator[idx])

                    # Increment intensity for UIDs that satisfied this pattern
                    # NEW: Also track this as an active pattern
                    if common_uids:  # If any UID satisfied this pattern
                        active_patterns.add(pattern_name)

                        for uid in common_uids:
                            if uid in mws_pattern_intensity:
                                mws_pattern_intensity[uid] += 1

                            if uid in mws_active_patterns:
                                if (
                                    pattern_name not in mws_active_patterns[uid]
                                ):  # Avoid duplicates
                                    mws_active_patterns[uid].append(pattern_name)

        PATTERN_DISPLAY_MAPPING = {
            # Agriculture patterns
            "caste": "High density of marginalized caste communities",
            "groundwater_stress": "Groundwater Stress",
            "high_drought_incidence": "High drought incidence",
            "high_irrigation_risk": "High irrigation risk",
            "low_yield": "Likely stress in cropping yield",
            # Forest & Livestock patterns
            "forest_degradation": "Tree Health Degradation",
            # Health patterns
            "mining_presence": "Mining Presence",
            # Socio-economic patterns
            "nrega": "Poor uptake of MGNREGA works",
            # Fishery patterns
            "fishery_potential": "Fishery",
        }

        active_stre_pattern = []
        for str_pattern in active_patterns:
            active_stre_pattern.append(PATTERN_DISPLAY_MAPPING.get(str_pattern))
        # NEW: Return intensity and list of distinct active patterns
        pattern_display_mapping = {}
        for pattern_key in active_patterns:
            pattern_display_mapping[pattern_key] = PATTERN_DISPLAY_MAPPING.get(
                pattern_key, pattern_key
            )

        return {
            "intensity": mws_pattern_intensity,
            "mws_active_patterns": mws_active_patterns,
            "active_patterns": sorted(list(active_stre_pattern)),
            "pattern_display_mapping": pattern_display_mapping,
        }

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity: %s",
            district,
            block,
            e,
        )
        return {"intensity": {}, "active_patterns": []}


def get_agri_water_stress_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_soge = pd.read_excel(file_path, sheet_name="soge_vector")
        df_water = pd.read_excel(file_path, sheet_name="hydrological_annual")

        mws_pattern = {}
        mws_intensity = {}

        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        indicator_weights = {"indicator1": 0.5, "indicator2": 0.5}

        # Indicator 1: class_name != "Safe"
        indicator1_uids = set()
        for index, row in df_soge.iterrows():
            uid = row["UID"]
            class_name = row["class_name"]

            if str(class_name).lower() != "safe":
                indicator1_uids.add(uid)

        # Indicator 2: G_in_mm_ decreasing trend
        indicator2_uids = set()

        g_columns = [col for col in df_water.columns if col.startswith("G_in_mm_")]
        g_columns.sort()

        start_year = 2017
        end_year = start_year + len(g_columns) - 1
        year_range = f"{start_year}-{end_year}"

        for index, row in df_water.iterrows():
            uid = row["UID"]

            values = []
            for col in g_columns:
                val = row[col]
                if pd.notna(val):
                    values.append(float(val))

            if len(values) >= 3:
                try:
                    mk_result = mk.original_test(values)

                    if mk_result.trend == "decreasing":
                        indicator2_uids.add(uid)
                except Exception as e:
                    pass

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            intensity = 0.0

            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            mws_intensity[uid] = round(intensity, 2)

            if intensity >= 1.0:  # Only True if ALL indicators passed
                mws_pattern[uid] = True

        matched_uids = indicator1_uids.intersection(indicator2_uids)

        # Calculate total area of matched MWS
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Calculate total area of ALL MWS (more concise)
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": total_matched_area,
            "total_all_area": total_all_area,
            "year_range": year_range,
        }

        return result

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
            "year_range": "",
        }


def get_agri_water_drought_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_drought = pd.read_excel(file_path, sheet_name="croppingDrought_kharif")

        mws_pattern = {}
        mws_intensity = {}

        # Initialize all UIDs with False and 0 intensity
        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        # Define weights for each indicator
        indicator_weights = {
            "indicator1": 0.5,  # Kharif drought frequency >= 2
            "indicator2": 0.5,  # Average dry spell length >= 2
        }

        # Indicator 1: Kharif drought frequency >= 2
        indicator1_uids = set()

        moderate_cols = [
            col for col in df_drought.columns if col.startswith("Moderate_in_weeks_")
        ]
        severe_cols = [
            col for col in df_drought.columns if col.startswith("Severe_in_weeks_")
        ]

        years = set()
        for col in moderate_cols:
            year = col.replace("Moderate_in_weeks_", "")
            if year:
                years.add(year)

        years_sorted = sorted(list(years))

        for index, row in df_drought.iterrows():
            uid = row["UID"]
            drought_year_count = 0

            for year in years_sorted:
                moderate_col = f"Moderate_in_weeks_{year}"
                severe_col = f"Severe_in_weeks_{year}"

                if (
                    moderate_col in df_drought.columns
                    and severe_col in df_drought.columns
                ):
                    moderate_val = row[moderate_col]
                    severe_val = row[severe_col]

                    if pd.notna(moderate_val) and pd.notna(severe_val):
                        year_sum = float(moderate_val) + float(severe_val)

                        if year_sum >= 5:
                            drought_year_count += 1

            if drought_year_count >= 2:
                indicator1_uids.add(uid)

        # Indicator 2: Average dry spell length >= 2
        indicator2_uids = set()

        drysp_cols = [
            col for col in df_drought.columns if col.startswith("drysp_unit_4_weeks_")
        ]
        drysp_cols.sort()

        for index, row in df_drought.iterrows():
            uid = row["UID"]

            values = []
            for col in drysp_cols:
                val = row[col]
                if pd.notna(val):
                    values.append(float(val))

            if len(values) > 0:
                average = sum(values) / len(values)

                if average >= 2:
                    indicator2_uids.add(uid)

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            intensity = 0.0

            # Add weight if indicator 1 passed
            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            # Add weight if indicator 2 passed
            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            mws_intensity[uid] = round(intensity, 2)

            # Mark as True if ALL indicators passed
            if intensity >= 1.0:
                mws_pattern[uid] = True

        # Find UIDs that passed BOTH indicators (for total_area)
        matched_uids = indicator1_uids.intersection(indicator2_uids)

        # Find UIDs that passed AT LEAST ONE indicator (for timeline denominator)
        any_indicator_uids = indicator1_uids.union(indicator2_uids)

        # Calculate total area of matched MWS (both indicators)
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Calculate weighted average drought incidence timeline
        weighted_drought_timeline = {}

        for year in years_sorted:
            numerator = 0.0
            denominator = 0.0

            for index, row in df_drought.iterrows():
                uid = row["UID"]

                # Include MWS units where AT LEAST ONE indicator matches
                if uid in any_indicator_uids:
                    # Get cropping area for this MWS from df_area
                    area_row = df_area[df_area["UID"] == uid]
                    if not area_row.empty:
                        cropping_area = area_row.iloc[0]["sum_area_in_ha"]

                        if pd.notna(cropping_area):
                            cropping_area = float(cropping_area)

                            # Calculate drought binary for this year
                            moderate_col = f"Moderate_in_weeks_{year}"
                            severe_col = f"Severe_in_weeks_{year}"

                            if (
                                moderate_col in df_drought.columns
                                and severe_col in df_drought.columns
                            ):
                                moderate_val = row[moderate_col]
                                severe_val = row[severe_col]

                                if pd.notna(moderate_val) and pd.notna(severe_val):
                                    year_sum = float(moderate_val) + float(severe_val)
                                    drought_binary = 1 if year_sum > 5 else 0

                                    numerator += drought_binary * cropping_area
                                    denominator += cropping_area

            # Calculate weighted average for this year
            if denominator > 0:
                weighted_drought_timeline[year] = round(numerator / denominator, 4)
            else:
                weighted_drought_timeline[year] = 0.0

        # Calculate total area of ALL MWS
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": total_matched_area,
            "total_all_area": total_all_area,
            "year_range": years_sorted[0] + "-" + years_sorted[-1],
        }

        return result, weighted_drought_timeline

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Drought Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
            "year_range": "",
        }, {}


def get_agri_water_irrigation_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_water = pd.read_excel(file_path, sheet_name="surfaceWaterBodies_annual")

        mws_pattern = {}
        mws_intensity = {}

        # Initialize all UIDs with False and 0 intensity
        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        # Define weights for each indicator
        indicator_weights = {
            "indicator1": 0.5,  # Rabi water availability < 30%
            "indicator2": 0.5,  # Total water body area decreasing trend
        }

        # Indicator 1: Rabi water availability < 30%
        indicator1_uids = set()

        # Find all years from total_area_in_ha_ columns
        total_cols = [
            col for col in df_water.columns if col.startswith("total_area_in_ha_")
        ]
        years = []
        for col in total_cols:
            year = col.replace("total_area_in_ha_", "")
            if year:
                years.append(year)

        years_sorted = sorted(years)
        percentage_threshold = 30.0

        for index, row in df_water.iterrows():
            uid = row["UID"]
            yearly_percentages = []

            # Calculate percentage for each year
            for year in years_sorted:
                total_col = f"total_area_in_ha_{year}"
                rabi_col = f"rabi_area_in_ha_{year}"

                # Check if both columns exist
                if total_col in df_water.columns and rabi_col in df_water.columns:
                    total_val = row[total_col]
                    rabi_val = row[rabi_col]

                    # Calculate percentage if values are valid
                    if pd.notna(total_val) and pd.notna(rabi_val) and total_val > 0:
                        year_percentage = (float(rabi_val) / float(total_val)) * 100
                        yearly_percentages.append(year_percentage)

            # Calculate average percentage
            if len(yearly_percentages) > 0:
                avg_percentage = sum(yearly_percentages) / len(yearly_percentages)

                if avg_percentage < percentage_threshold:
                    indicator1_uids.add(uid)

        # Indicator 2: Total water body area decreasing trend
        indicator2_uids = set()

        # Find all total_area_in_ha_ columns
        total_cols = [
            col for col in df_water.columns if col.startswith("total_area_in_ha_")
        ]
        total_cols.sort()

        for index, row in df_water.iterrows():
            uid = row["UID"]

            # Extract values from all total_area_in_ha_ columns
            values = []
            for col in total_cols:
                val = row[col]
                if pd.notna(val):
                    values.append(float(val))

            if len(values) >= 3:
                try:
                    mk_result = mk.original_test(values)

                    # Check for decreasing trend
                    if mk_result.trend == "decreasing":
                        indicator2_uids.add(uid)
                except Exception as e:
                    pass

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            intensity = 0.0

            # Add weight if indicator 1 passed
            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            # Add weight if indicator 2 passed
            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            mws_intensity[uid] = round(intensity, 2)

            # Mark as True if ALL indicators passed
            if intensity >= 1.0:
                mws_pattern[uid] = True

        # Find UIDs that passed both indicators (for area calculation)
        matched_uids = indicator1_uids.intersection(indicator2_uids)

        # Calculate total area of matched MWS
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Calculate weighted average seasonal water availability timeline
        seasonal_timeline = {"kharif": {}, "rabi": {}, "zaid": {}}

        for year in years_sorted:
            # Initialize accumulators for each season
            kharif_numerator = 0.0
            rabi_numerator = 0.0
            zaid_numerator = 0.0
            denominator = 0.0

            for index, row in df_water.iterrows():
                uid = row["UID"]
                # Only include matched UIDs
                if uid in matched_uids:
                    total_col = f"total_area_in_ha_{year}"
                    kharif_col = f"kharif_area_in_ha_{year}"
                    rabi_col = f"rabi_area_in_ha_{year}"
                    zaid_col = f"zaid_area_in_ha_{year}"

                    # Check if all columns exist
                    if (
                        total_col in df_water.columns
                        and kharif_col in df_water.columns
                        and rabi_col in df_water.columns
                        and zaid_col in df_water.columns
                    ):

                        total_val = row[total_col]
                        kharif_val = row[kharif_col]
                        rabi_val = row[rabi_col]
                        zaid_val = row[zaid_col]

                        # Calculate if all values are valid
                        if (
                            pd.notna(total_val)
                            and pd.notna(kharif_val)
                            and pd.notna(rabi_val)
                            and pd.notna(zaid_val)
                            and total_val > 0
                        ):

                            total_area = float(total_val)

                            # Calculate percentages for each season
                            kharif_percentage = (float(kharif_val) / total_area) * 100
                            rabi_percentage = (float(rabi_val) / total_area) * 100
                            zaid_percentage = (float(zaid_val) / total_area) * 100

                            # Weighted sum
                            kharif_numerator += kharif_percentage * total_area
                            rabi_numerator += rabi_percentage * total_area
                            zaid_numerator += zaid_percentage * total_area
                            denominator += total_area

            # Calculate weighted averages for this year
            if denominator > 0:
                seasonal_timeline["kharif"][year] = round(
                    kharif_numerator / denominator, 2
                )
                seasonal_timeline["rabi"][year] = round(rabi_numerator / denominator, 2)
                seasonal_timeline["zaid"][year] = round(zaid_numerator / denominator, 2)
            else:
                seasonal_timeline["kharif"][year] = 0.0
                seasonal_timeline["rabi"][year] = 0.0
                seasonal_timeline["zaid"][year] = 0.0

        # Calculate total area of ALL MWS (more concise)
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": total_matched_area,
            "total_all_area": total_all_area,
            "year_range": years_sorted[0] + "-" + years_sorted[-1],
        }

        return result, seasonal_timeline

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Irrigation Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
            "year_range": "",
        }, {"kharif": {}, "rabi": {}, "zaid": {}}


def get_agri_low_yield_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_cropIntensity = pd.read_excel(
            file_path, sheet_name="change_detection_cropintensity"
        )
        df_degrade = pd.read_excel(file_path, sheet_name="change_detection_degradation")

        mws_pattern = {}
        mws_intensity = {}

        # Initialize all UIDs with False and 0 intensity
        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        # Define weights for each indicator
        indicator_weights = {
            "indicator1": 0.5,  # Farm to barren/scrub land > 30 ha
            "indicator2": 0.5,  # Total crop intensity change > 30 ha
        }

        # Indicator 1: Farm to barren/scrub land > 30 ha
        indicator1_uids = set()

        for index, row in df_degrade.iterrows():
            uid = row["UID"]

            barren_val = row["farm_to_barren_area_in_ha"]
            scrub_val = row["farm_to_scrub_land_area_in_ha"]

            if pd.notna(barren_val) and pd.notna(scrub_val):
                total_sum = float(barren_val) + float(scrub_val)

                if total_sum > 30:
                    indicator1_uids.add(uid)

        # Indicator 2: Total crop intensity change > 30 ha
        indicator2_uids = set()

        for index, row in df_cropIntensity.iterrows():
            uid = row["UID"]
            value = row["total_change_crop_intensity_area_in_ha"]

            if pd.notna(value) and float(value) > 30:
                indicator2_uids.add(uid)

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            intensity = 0.0

            # Add weight if indicator 1 passed
            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            # Add weight if indicator 2 passed
            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            mws_intensity[uid] = round(intensity, 2)

            # Mark as True if ALL indicators passed
            if intensity >= 1.0:
                mws_pattern[uid] = True

        # Find UIDs that passed BOTH indicators (for area calculation)
        matched_uids = indicator1_uids.intersection(indicator2_uids)

        # Calculate areas for Sankey chart
        total_farmland_area = 0.0
        total_to_barren = 0.0
        total_to_scrub = 0.0

        for uid in matched_uids:

            area_row = df_area[df_area["UID"] == uid]
            if not area_row.empty:
                farmland_area = area_row.iloc[0]["sum_area_in_ha"]
                if pd.notna(farmland_area):
                    total_farmland_area += float(farmland_area)

            degrade_row = df_degrade[df_degrade["UID"] == uid]
            if not degrade_row.empty:
                barren_val = degrade_row.iloc[0]["farm_to_barren_area_in_ha"]
                scrub_val = degrade_row.iloc[0]["farm_to_scrub_land_area_in_ha"]

                if pd.notna(barren_val):
                    total_to_barren += float(barren_val)
                if pd.notna(scrub_val):
                    total_to_scrub += float(scrub_val)

        # Calculate remaining farmland
        remaining_farmland = total_farmland_area - total_to_barren - total_to_scrub

        sankey_data = {
            "nodes": [
                {"name": "Farmlands"},
                {"name": "Barren Land"},
                {"name": "Scrub Land"},
                {"name": "Remaining Farmlands"},
            ],
            "links": [
                {"source": 0, "target": 1, "value": round(total_to_barren, 2)},
                {"source": 0, "target": 2, "value": round(total_to_scrub, 2)},
                {
                    "source": 0,
                    "target": 3,
                    "value": (
                        round(remaining_farmland, 2) if remaining_farmland > 0 else 0
                    ),
                },
            ],
        }

        # Calculate total area of ALL MWS (more concise)
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": round(total_to_barren + total_to_scrub, 2),
            "total_all_area": total_all_area,
        }

        return result, sankey_data

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Low Yield Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
        }, {"nodes": [], "links": []}


def get_forest_degrad_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_degrade = pd.read_excel(
            file_path, sheet_name="change_detection_deforestation"
        )

        mws_pattern = {}
        mws_intensity = {}

        # Initialize all UIDs with False and 0 intensity
        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        # Single indicator (Type 2): Check if total_deforestation_area_in_ha > 50
        # Since there's only one indicator, weight is 1.0
        matched_uids = set()

        for index, row in df_degrade.iterrows():
            uid = row["UID"]
            value = row["total_deforestation_area_in_ha"]

            # Check if value > 50 (comp_type 3 = Greater than)
            if pd.notna(value) and float(value) > 50:
                matched_uids.add(uid)

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            # Since there's only one indicator, intensity is either 0.0 or 1.0
            if uid in matched_uids:
                mws_intensity[uid] = 1.0
                mws_pattern[uid] = True

        # Calculate total deforestation area and forest transition flows
        total_matched_area = 0.0
        total_to_barren = 0.0
        total_to_builtup = 0.0
        total_to_farm = 0.0
        total_to_forest = 0.0
        total_to_scrub = 0.0

        for index, row in df_degrade.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                deforest_val = row["total_deforestation_area_in_ha"]

                if pd.notna(deforest_val):
                    total_matched_area += float(deforest_val)

                # Get transition values
                barren_val = row.get("forest_to_barren_area_in_ha")
                builtup_val = row.get("forest_to_built_up_area_in_ha")
                farm_val = row.get("forest_to_farm_area_in_ha")
                forest_val = row.get("forest_to_forest_area_in_ha")
                scrub_val = row.get("forest_to_scrub_land_area_in_ha")

                # Sum up transitions
                if pd.notna(barren_val):
                    total_to_barren += float(barren_val)
                if pd.notna(builtup_val):
                    total_to_builtup += float(builtup_val)
                if pd.notna(farm_val):
                    total_to_farm += float(farm_val)
                if pd.notna(forest_val):
                    total_to_forest += float(forest_val)
                if pd.notna(scrub_val):
                    total_to_scrub += float(scrub_val)

        # Prepare Sankey data for forest transitions
        forest_sankey = {
            "nodes": [
                {"name": "Forest Cover"},
                {"name": "Barren Land"},
                {"name": "Built-up Area"},
                {"name": "Farmland"},
                {"name": "Remaining Forest"},
                {"name": "Scrub Land"},
            ],
            "links": [
                {
                    "source": 0,  # Forest Cover
                    "target": 1,  # Barren Land
                    "value": round(total_to_barren, 2),
                },
                {
                    "source": 0,  # Forest Cover
                    "target": 2,  # Built-up Area
                    "value": round(total_to_builtup, 2),
                },
                {
                    "source": 0,  # Forest Cover
                    "target": 3,  # Farmland
                    "value": round(total_to_farm, 2),
                },
                {
                    "source": 0,  # Forest Cover
                    "target": 4,  # Remaining Forest
                    "value": round(total_to_forest, 2),
                },
                {
                    "source": 0,  # Forest Cover
                    "target": 5,  # Scrub Land
                    "value": round(total_to_scrub, 2),
                },
            ],
        }

        # Calculate total area of ALL MWS (more concise)
        total_all_area = float(df_degrade["total_deforestation_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": total_matched_area,
            "total_all_area": total_all_area,
        }

        return result, forest_sankey

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Forest Degradation Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
        }, {"nodes": [], "links": []}


def get_mining_presence_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_mining = pd.read_excel(file_path, sheet_name="mining")

        mws_pattern = {}

        # Initialize all UIDs with False
        for uid in df_area["UID"]:
            mws_pattern[uid] = False

        # Type 6 - Presence Check: Check if UID exists in mining sheet
        matched_uids = set()

        # Get all unique UIDs from mining sheet
        mining_uids = df_mining["UID"].unique()

        for uid in mining_uids:
            if pd.notna(uid):
                matched_uids.add(uid)

        # Update mws_pattern with matched UIDs
        for uid in matched_uids:
            if uid in mws_pattern:
                mws_pattern[uid] = True

        # Calculate total area of MWS with mining presence
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Prepare pie chart data - count different types of mines
        mining_types = {}

        # Only include mines from matched UIDs
        for index, row in df_mining.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                sector = row.get("sector_moefcc")

                if pd.notna(sector):
                    sector_str = str(sector).strip()
                    if sector_str:  # Not empty
                        if sector_str in mining_types:
                            mining_types[sector_str] += 1
                        else:
                            mining_types[sector_str] = 1

        # Prepare pie chart data structure
        mining_pie_chart = {
            "labels": list(mining_types.keys()),
            "values": list(mining_types.values()),
        }

        result = {"mws_pattern": mws_pattern, "total_area": total_matched_area}

        return result, mining_pie_chart

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Mining Data: %s",
            district,
            block,
            e,
        )

        return {"mws_pattern": {}, "total_area": 0.0}, {"labels": [], "values": []}


def get_socio_economic_caste_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_social = pd.read_excel(file_path, sheet_name="social_economic_indicator")

        village_pattern = {}
        village_intensity = {}

        # Initialize all village IDs with False and 0 intensity
        for village_id in df_social["village_id"]:
            village_pattern[village_id] = False
            village_intensity[village_id] = 0.0

        # Define weights for each indicator
        indicator_weights = {
            "indicator1": 0.5,  # SC_percent > 17
            "indicator2": 0.5,  # ST_percent > 33
        }

        # Process each village
        indicator1_villages = set()
        indicator2_villages = set()

        for index, row in df_social.iterrows():
            village_id = row["village_id"]
            sc_percent = row.get("SC_percent", 0)
            st_percent = row.get("ST_percent", 0)

            # Indicator 1: SC_percent > 17
            if pd.notna(sc_percent) and float(sc_percent) > 17:
                indicator1_villages.add(village_id)

            # Indicator 2: ST_percent > 33
            if pd.notna(st_percent) and float(st_percent) > 33:
                indicator2_villages.add(village_id)

        # Calculate intensity for each village
        for village_id in village_intensity.keys():
            intensity = 0.0

            # Add weight if indicator 1 passed
            if village_id in indicator1_villages:
                intensity += indicator_weights["indicator1"]

            # Add weight if indicator 2 passed
            if village_id in indicator2_villages:
                intensity += indicator_weights["indicator2"]

            village_intensity[village_id] = round(intensity, 2)

            # Mark as True if ALL indicators passed
            if intensity >= 1.0:
                village_pattern[village_id] = True

        # Find villages that passed BOTH indicators (for population calculation)
        matched_villages = indicator1_villages.intersection(indicator2_villages)

        # Calculate total population and SC/ST populations for matched villages
        total_population = 0
        sc_population = 0
        st_population = 0

        for index, row in df_social.iterrows():
            village_id = row["village_id"]

            if village_id in matched_villages:
                pop = row.get("total_population", 0)

                if pd.notna(pop):
                    pop_value = float(pop)
                    total_population += pop_value

                    # Calculate SC and ST populations
                    sc_percent = row.get("SC_percent", 0)
                    st_percent = row.get("ST_percent", 0)

                    if pd.notna(sc_percent):
                        sc_population += (float(sc_percent) / 100) * pop_value
                    if pd.notna(st_percent):
                        st_population += (float(st_percent) / 100) * pop_value

        # Calculate Others population
        others_population = total_population - sc_population - st_population

        # Prepare pie chart data
        caste_pie_chart = {
            "labels": ["SC (Scheduled Caste)", "ST (Scheduled Tribe)", "Others"],
            "values": [
                round(sc_population, 0),
                round(st_population, 0),
                round(others_population, 0),
            ],
        }

        result = {
            "village_pattern": village_pattern,
            "village_intensity": village_intensity,
            "total_population": round(total_population, 0),
            "total_villages": len(matched_villages),
        }

        return result, caste_pie_chart

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Socio Caste Data: %s",
            district,
            block,
            e,
        )

        return {
            "village_pattern": {},
            "village_intensity": {},
            "total_population": 0,
            "total_villages": 0,
        }, {"labels": [], "values": []}


def get_socio_economic_nrega_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_nrega = pd.read_excel(file_path, sheet_name="nrega_annual")
        df_mws_villages = pd.read_excel(file_path, sheet_name="mws_intersect_villages")
        df_social = pd.read_excel(file_path, sheet_name="social_economic_indicator")

        village_pattern = {}
        village_intensity = {}

        # Initialize all village IDs with False and 0 intensity
        for village_id in df_social["village_id"]:
            village_pattern[village_id] = False
            village_intensity[village_id] = 0.0

        base_columns = [
            "Soil and water conservation_count_",
            "Land restoration_count_",
            "Plantations_count_",
            "Irrigation on farms_count_",
            "Other farm works_count_",
            "Off-farm livelihood assets_count_",
            "Community assets_count_",
        ]

        # Single indicator (Type 8): Total NREGA works < 100
        # Since there's only one indicator, weight is 1.0
        matched_mws_uids = set()

        for index, row in df_nrega.iterrows():
            uid = row["mws_id"]

            grand_total = 0.0

            # For each base column, find all year columns and sum
            for base_col in base_columns:
                # Find all columns that start with this base name
                matching_cols = [
                    col for col in df_nrega.columns if col.startswith(base_col)
                ]

                for col in matching_cols:
                    val = row[col]
                    if pd.notna(val):
                        grand_total += float(val)

            # Check if grand_total < 100 (comp_type 4 = Less than)
            if grand_total < 100:
                matched_mws_uids.add(uid)

        # Map MWS to villages and set village intensity
        matched_villages = set()

        for index, row in df_mws_villages.iterrows():
            mws_uid = row["MWS UID"]

            if mws_uid in matched_mws_uids:
                village_ids = row["Village IDs"]

                if pd.notna(village_ids):
                    try:
                        if isinstance(village_ids, str):
                            import ast
                            import json

                            try:
                                village_list = ast.literal_eval(village_ids)
                            except:
                                try:
                                    village_list = json.loads(village_ids)
                                except:
                                    village_list = [
                                        v.strip() for v in village_ids.split(",")
                                    ]
                        elif isinstance(village_ids, (list, tuple)):
                            village_list = list(village_ids)
                        else:
                            village_list = [village_ids]

                        # Add villages to matched set and update intensity
                        for village_id in village_list:
                            matched_villages.add(village_id)
                            if village_id in village_intensity:
                                village_intensity[village_id] = 1.0
                                village_pattern[village_id] = True

                    except Exception as e:
                        continue

        # Calculate totals for each NREGA work type for pie chart (using matched MWS)
        nrega_totals = {
            "Soil and Water Conservation": 0,
            "Land Restoration": 0,
            "Plantations": 0,
            "Irrigation on Farms": 0,
            "Other Farm Works": 0,
            "Off-farm Livelihood Assets": 0,
            "Community Assets": 0,
        }

        base_to_label = {
            "Soil and water conservation_count_": "Soil and Water Conservation",
            "Land restoration_count_": "Land Restoration",
            "Plantations_count_": "Plantations",
            "Irrigation on farms_count_": "Irrigation on Farms",
            "Other farm works_count_": "Other Farm Works",
            "Off-farm livelihood assets_count_": "Off-farm Livelihood Assets",
            "Community assets_count_": "Community Assets",
        }

        for index, row in df_nrega.iterrows():
            uid = row["mws_id"]

            if uid in matched_mws_uids:
                for base_col, label in base_to_label.items():
                    # Find all columns that start with this base name
                    matching_cols = [
                        col for col in df_nrega.columns if col.startswith(base_col)
                    ]

                    for col in matching_cols:
                        val = row[col]
                        if pd.notna(val):
                            nrega_totals[label] += float(val)

        # Prepare pie chart data
        nrega_pie_chart = {
            "labels": list(nrega_totals.keys()),
            "values": [round(v, 0) for v in nrega_totals.values()],
        }

        # Calculate total number of ALL villages from nrega_assets_village sheet
        # total_all_villages = df_nrega_assets["vill_name"].unique()
        total_all_villages = len(df_social["village_id"].unique())

        result = {
            "village_pattern": village_pattern,
            "village_intensity": village_intensity,
            "total_villages": len(matched_villages),
            "total_all_villages": total_all_villages,
        }

        return result, nrega_pie_chart

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for NREGA Data: %s",
            district,
            block,
            e,
        )

        return {
            "village_pattern": {},
            "village_intensity": {},
            "total_villages": 0,
            "total_all_villages": 0,
        }, {"labels": [], "values": []}


def get_fishery_water_potential_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_water = pd.read_excel(file_path, sheet_name="surfaceWaterBodies_annual")

        mws_pattern = {}
        mws_intensity = {}

        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        indicator_weights = {"indicator1": 0.34, "indicator2": 0.33, "indicator3": 0.33}

        # Find all years from total_area_in_ha_ columns
        total_cols = [
            col for col in df_water.columns if col.startswith("total_area_in_ha_")
        ]
        years = []
        for col in total_cols:
            year = col.replace("total_area_in_ha_", "")
            if year:
                years.append(year)

        years_sorted = sorted(years)
        percentage_threshold = 30.0

        indicator1_uids = set()

        for index, row in df_water.iterrows():
            uid = row["UID"]
            yearly_percentages = []

            for year in years_sorted:
                total_col = f"total_area_in_ha_{year}"
                rabi_col = f"rabi_area_in_ha_{year}"

                if total_col in df_water.columns and rabi_col in df_water.columns:
                    total_val = row[total_col]
                    rabi_val = row[rabi_col]

                    if pd.notna(total_val) and pd.notna(rabi_val) and total_val > 0:
                        year_percentage = (float(rabi_val) / float(total_val)) * 100
                        yearly_percentages.append(year_percentage)

            if len(yearly_percentages) > 0:
                avg_percentage = sum(yearly_percentages) / len(yearly_percentages)

                if avg_percentage > percentage_threshold:
                    indicator1_uids.add(uid)

        indicator2_uids = set()

        for index, row in df_water.iterrows():
            uid = row["UID"]
            yearly_percentages = []

            for year in years_sorted:
                total_col = f"total_area_in_ha_{year}"
                zaid_col = f"zaid_area_in_ha_{year}"

                if total_col in df_water.columns and zaid_col in df_water.columns:
                    total_val = row[total_col]
                    zaid_val = row[zaid_col]

                    if pd.notna(total_val) and pd.notna(zaid_val) and total_val > 0:
                        year_percentage = (float(zaid_val) / float(total_val)) * 100
                        yearly_percentages.append(year_percentage)

            if len(yearly_percentages) > 0:
                avg_percentage = sum(yearly_percentages) / len(yearly_percentages)

                if avg_percentage > percentage_threshold:
                    indicator2_uids.add(uid)

        indicator3_uids = set()

        total_cols = [
            col for col in df_water.columns if col.startswith("total_area_in_ha_")
        ]
        total_cols.sort()

        for index, row in df_water.iterrows():
            uid = row["UID"]

            values = []
            for col in total_cols:
                val = row[col]
                if pd.notna(val):
                    values.append(float(val))

            if len(values) >= 3:
                try:
                    mk_result = mk.original_test(values)

                    if mk_result.trend != "decreasing":
                        indicator3_uids.add(uid)
                except Exception as e:
                    indicator3_uids.add(uid)
            else:
                indicator3_uids.add(uid)

        # Calculate intensity for each MWS
        for uid in mws_intensity.keys():
            intensity = 0.0

            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            if uid in indicator3_uids:
                intensity += indicator_weights["indicator3"]

            mws_intensity[uid] = round(intensity, 2)

            if intensity >= 1.0:
                mws_pattern[uid] = True

        matched_uids = indicator1_uids.intersection(indicator2_uids).intersection(
            indicator3_uids
        )

        # Calculate total area of ALL MWS
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        # Calculate total area of matched MWS
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Calculate percentage
        if total_all_area > 0:
            area_percentage = round((total_matched_area / total_all_area) * 100, 2)
        else:
            area_percentage = 0.0

        # Calculate total zaid area and total SWB area for matched UIDs (using latest year)
        latest_year = years_sorted[-1] if years_sorted else None
        total_zaid_area = 0.0
        total_swb_area = 0.0

        if latest_year:
            zaid_col = f"zaid_area_in_ha_{latest_year}"
            total_col = f"total_area_in_ha_{latest_year}"

            for index, row in df_water.iterrows():
                uid = row["UID"]

                if uid in matched_uids:
                    if zaid_col in df_water.columns:
                        zaid_val = row[zaid_col]
                        if pd.notna(zaid_val):
                            total_zaid_area += float(zaid_val)

                    if total_col in df_water.columns:
                        total_val = row[total_col]
                        if pd.notna(total_val):
                            total_swb_area += float(total_val)

        seasonal_timeline = {"kharif": {}, "rabi": {}, "zaid": {}}

        for year in years_sorted:
            kharif_numerator = 0.0
            rabi_numerator = 0.0
            zaid_numerator = 0.0
            denominator = 0.0

            for index, row in df_water.iterrows():
                uid = row["UID"]

                # Only include matched UIDs
                if uid in matched_uids:
                    total_col = f"total_area_in_ha_{year}"
                    kharif_col = f"kharif_area_in_ha_{year}"
                    rabi_col = f"rabi_area_in_ha_{year}"
                    zaid_col = f"zaid_area_in_ha_{year}"

                    if (
                        total_col in df_water.columns
                        and kharif_col in df_water.columns
                        and rabi_col in df_water.columns
                        and zaid_col in df_water.columns
                    ):

                        total_val = row[total_col]
                        kharif_val = row[kharif_col]
                        rabi_val = row[rabi_col]
                        zaid_val = row[zaid_col]

                        if (
                            pd.notna(total_val)
                            and pd.notna(kharif_val)
                            and pd.notna(rabi_val)
                            and pd.notna(zaid_val)
                            and total_val > 0
                        ):

                            total_area = float(total_val)

                            # Calculate percentages for each season
                            kharif_percentage = (float(kharif_val) / total_area) * 100
                            rabi_percentage = (float(rabi_val) / total_area) * 100
                            zaid_percentage = (float(zaid_val) / total_area) * 100

                            # Weighted sum
                            kharif_numerator += kharif_percentage * total_area
                            rabi_numerator += rabi_percentage * total_area
                            zaid_numerator += zaid_percentage * total_area
                            denominator += total_area

            if denominator > 0:
                seasonal_timeline["kharif"][year] = round(
                    kharif_numerator / denominator, 2
                )
                seasonal_timeline["rabi"][year] = round(rabi_numerator / denominator, 2)
                seasonal_timeline["zaid"][year] = round(zaid_numerator / denominator, 2)
            else:
                seasonal_timeline["kharif"][year] = 0.0
                seasonal_timeline["rabi"][year] = 0.0
                seasonal_timeline["zaid"][year] = 0.0

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": area_percentage,
            "total_zaid_area": round(total_zaid_area, 2),
            "total_swb_area": round(total_swb_area, 2),
            "year_range": years_sorted[0] + "-" + years_sorted[-1],
        }

        return result, seasonal_timeline

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Fishery Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_zaid_area": 0.0,
            "total_swb_area": 0.0,
        }, {"kharif": {}, "rabi": {}, "zaid": {}}


def get_agroforestry_transition_data(state, district, block):
    try:
        file_path = (
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx"
        )

        df_area = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")
        df_cropIntensity = pd.read_excel(
            file_path, sheet_name="change_detection_cropintensity"
        )
        df_degrade = pd.read_excel(file_path, sheet_name="change_detection_degradation")

        mws_pattern = {}
        mws_intensity = {}

        # Initialize all UIDs with False and 0 intensity
        for uid in df_area["UID"]:
            mws_pattern[uid] = False
            mws_intensity[uid] = 0.0

        # Define weights for each indicator
        indicator_weights = {"indicator1": 0.5, "indicator2": 0.5}

        indicator1_uids = set()

        for index, row in df_degrade.iterrows():
            uid = row["UID"]

            barren_val = row["farm_to_barren_area_in_ha"]
            scrub_val = row["farm_to_scrub_land_area_in_ha"]

            if pd.notna(barren_val) and pd.notna(scrub_val):
                total_sum = float(barren_val) + float(scrub_val)

                if total_sum > 30:
                    indicator1_uids.add(uid)

        indicator2_uids = set()

        for index, row in df_cropIntensity.iterrows():
            uid = row["UID"]
            value = row["total_change_crop_intensity_area_in_ha"]

            if pd.notna(value) and float(value) > 30:
                indicator2_uids.add(uid)

        for uid in mws_intensity.keys():
            intensity = 0.0

            # Add weight if indicator 1 passed
            if uid in indicator1_uids:
                intensity += indicator_weights["indicator1"]

            # Add weight if indicator 2 passed
            if uid in indicator2_uids:
                intensity += indicator_weights["indicator2"]

            mws_intensity[uid] = round(intensity, 2)

            # Mark as True if ALL indicators passed
            if intensity >= 1.0:
                mws_pattern[uid] = True

        matched_uids = indicator1_uids.intersection(indicator2_uids)

        # Calculate total area of matched MWS
        total_matched_area = 0.0
        for index, row in df_area.iterrows():
            uid = row["UID"]
            if uid in matched_uids:
                area = row["sum_area_in_ha"]
                if pd.notna(area):
                    total_matched_area += float(area)

        # Initialize transition totals
        total_double_to_single = 0.0
        total_double_to_triple = 0.0
        total_single_to_double = 0.0
        total_single_to_triple = 0.0
        total_triple_to_double = 0.0
        total_triple_to_single = 0.0

        # Also track "staying same" transitions
        total_single_to_single = 0.0
        total_double_to_double = 0.0
        total_triple_to_triple = 0.0

        for index, row in df_cropIntensity.iterrows():
            uid = row["UID"]

            if uid in matched_uids:
                # Get transition values
                double_to_single = row.get("double_to_single_area_in_ha")
                double_to_triple = row.get("double_to_triple_area_in_ha")
                single_to_double = row.get("single_to_double_area_in_ha")
                single_to_triple = row.get("single_to_triple_area_in_ha")
                triple_to_double = row.get("triple_to_double_area_in_ha")
                triple_to_single = row.get("triple_to_single_area_in_ha")

                # Get "staying same" values (if columns exist)
                single_to_single = row.get("single_to_single_area_in_ha", 0)
                double_to_double = row.get("double_to_double_area_in_ha", 0)
                triple_to_triple = row.get("triple_to_triple_area_in_ha", 0)

                # Sum up transitions
                if pd.notna(double_to_single):
                    total_double_to_single += float(double_to_single)
                if pd.notna(double_to_triple):
                    total_double_to_triple += float(double_to_triple)
                if pd.notna(single_to_double):
                    total_single_to_double += float(single_to_double)
                if pd.notna(single_to_triple):
                    total_single_to_triple += float(single_to_triple)
                if pd.notna(triple_to_double):
                    total_triple_to_double += float(triple_to_double)
                if pd.notna(triple_to_single):
                    total_triple_to_single += float(triple_to_single)

                # Sum "staying same"
                if pd.notna(single_to_single):
                    total_single_to_single += float(single_to_single)
                if pd.notna(double_to_double):
                    total_double_to_double += float(double_to_double)
                if pd.notna(triple_to_triple):
                    total_triple_to_triple += float(triple_to_triple)

        # Sankey 1: Single Cropping Transitions
        single_sankey = {
            "nodes": [
                {"name": "Single Cropping (Initial)"},
                {"name": "Single Cropping (Final)"},
                {"name": "Double Cropping (Final)"},
                {"name": "Triple Cropping (Final)"},
            ],
            "links": [
                {"source": 0, "target": 1, "value": round(total_single_to_single, 2)},
                {"source": 0, "target": 2, "value": round(total_single_to_double, 2)},
                {"source": 0, "target": 3, "value": round(total_single_to_triple, 2)},
            ],
        }

        # Sankey 2: Double Cropping Transitions
        double_sankey = {
            "nodes": [
                {"name": "Double Cropping (Initial)"},
                {"name": "Single Cropping (Final)"},
                {"name": "Double Cropping (Final)"},
                {"name": "Triple Cropping (Final)"},
            ],
            "links": [
                {"source": 0, "target": 1, "value": round(total_double_to_single, 2)},
                {"source": 0, "target": 2, "value": round(total_double_to_double, 2)},
                {"source": 0, "target": 3, "value": round(total_double_to_triple, 2)},
            ],
        }

        # Sankey 3: Triple Cropping Transitions
        triple_sankey = {
            "nodes": [
                {"name": "Triple Cropping (Initial)"},
                {"name": "Single Cropping (Final)"},
                {"name": "Double Cropping (Final)"},
                {"name": "Triple Cropping (Final)"},
            ],
            "links": [
                {"source": 0, "target": 1, "value": round(total_triple_to_single, 2)},
                {"source": 0, "target": 2, "value": round(total_triple_to_double, 2)},
                {"source": 0, "target": 3, "value": round(total_triple_to_triple, 2)},
            ],
        }

        # Calculate total area of ALL MWS
        total_all_area = float(df_area["sum_area_in_ha"].sum())

        result = {
            "mws_pattern": mws_pattern,
            "mws_intensity": mws_intensity,
            "total_area": total_matched_area,
            "total_all_area": total_all_area,
        }

        agroforestry_sankey = {
            "single": single_sankey,
            "double": double_sankey,
            "triple": triple_sankey,
        }

        return result, agroforestry_sankey

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Agroforestry Data: %s",
            district,
            block,
            e,
        )

        return {
            "mws_pattern": {},
            "mws_intensity": {},
            "total_area": 0.0,
            "total_all_area": 0.0,
        }, {
            "single": {"nodes": [], "links": []},
            "double": {"nodes": [], "links": []},
            "triple": {"nodes": [], "links": []},
        }


def get_tehsil_pattern_summary(mws_intensity_map):
    """
    mws_intensity_map: The dict returned by your first function {uid: count}
    block_patterns: The JSON/Dict containing the pattern definitions
    """
    # 1. Calculate Legend Basis (Min/Max)
    block_patterns = load_block_patterns()
    counts = mws_intensity_map.values()
    actual_max = max(counts) if counts else 0

    # 2. Get the list of ALL possible patterns from your JSON
    # This ensures the HTML knows what to look for
    all_pattern_names = []
    for category in block_patterns.values():
        for p in category:
            all_pattern_names.append(p.get("Name"))

    # 3. Filter for 'Active' patterns
    # (If any MWS has an intensity > 0, we consider the Tehsil to have active patterns)
    # Alternatively, you can return all_pattern_names to show a full checklist
    active_patterns = all_pattern_names if actual_max > 0 else []

    return active_patterns
