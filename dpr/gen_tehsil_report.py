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
JSON_FILE_PATH = os.path.join(os.path.dirname(CURRENT_DIR), 'dpr/utils', 'block_patterns.json')

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


def load_block_patterns(): # read the json file wherever needed
    try:
        with open(JSON_FILE_PATH, 'r') as file:
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
        file_path = (DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")
        # * Area of the Tehsil
        excel_file = pd.ExcelFile(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx")

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
        block_detail = Overpass_Block_Details.objects.filter(location=f"{district}_{block}").first()

        if block_detail:
            logger.info(f"Using cached response for location: {district}_{block}")
            response = block_detail.overpass_response
        else:
            logger.info(f"No cached data found. Fetching from Overpass API for location: {district}_{block}")
            
            try:
                response = requests.get(OVERPASS_URL, params={"data": overpass_query})
                response = response.json()
                
                block_detail = Overpass_Block_Details.objects.create(
                    location = f"{district}_{block}",
                    overpass_response = response
                )
                logger.info(f"Response saved to DB for location: {district}_{block}")
            
            except Exception as e:
                logger.info("Not able to fetch the Overpass API Info", e)

        #print("Data Processing", datetime.now())
        
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
                                        element.get("tags", {}).get("natural") == "water"
                                        or element.get("tags", {}).get("water") == "lake"
                                    )
                                    and not (
                                        element.get("tags", {}).get("landuse")
                                        == "reservoir"
                                    )
                                    and not element.get("tags", {}).get("water") == "river"
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
                                if element.get("tags", {}).get("landuse") == "reservoir":
                                    reservoirs.append(
                                        {
                                            "geometry": polygon,
                                            "area": polygon.area,
                                            "tags": element.get("tags", {}),
                                            "name": element_name,
                                        }
                                    )
                                    names["Reservoirs"].append(f"Reservoir: {element_name}")
                                # Rivers (if defined as a polygon)
                                if (
                                    element.get("tags", {}).get("natural") == "water"
                                    and element.get("tags", {}).get("water") == "river"
                                ) or element.get("tags", {}).get("waterway") == "riverbank":
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
                                        element.get("tags", {}).get("natural") == "water"
                                        and element.get("tags", {}).get("water") == "river"
                                    )
                                    or element.get("tags", {}).get("waterway") == "river"
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
            parameter_block += f". Key natural features such as {temp} shape the Tehsil landscape and impact water flow"

        if final_data["forests"]:
            large_forests = [f for f in final_data["forests"] if f["area_sq_m"] >= MIN_AREA_THRESHOLD]
            if large_forests:
                parameter_block += (
                    f". Part of {large_forests[0]['name']}, covering roughly "
                    f"{round(large_forests[0]['area_sq_m'] / 10000, 1)} hectares, lies within the Tehsil supporting local wildlife and promoting biodiversity"
                )

        if final_data["lakes"] or final_data["reservoirs"]:
            large_lakes = [lake for lake in final_data["lakes"] if lake["area_sq_m"] >= MIN_AREA_THRESHOLD]
            large_reservoirs = [res for res in final_data["reservoirs"] if res["area_sq_m"] >= MIN_AREA_THRESHOLD]
            
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

                parameter_block += f". Additionally, large water bodies such as "
                if len(rname) == 1:
                    parameter_block += rname[0]
                elif len(rname) == 2:
                    parameter_block += " and ".join(rname)
                else:
                    parameter_block += ", ".join(rname[:-1]) + ", and " + rname[-1]
                parameter_block += f" span about "
                if len(rname) == 1:
                    parameter_block += rarea[0]
                elif len(rname) == 2:
                    parameter_block += " and ".join(rarea)
                else:
                    parameter_block += ", ".join(rarea[:-1]) + ", and " + rarea[-1]
                parameter_block += f"  hectares  respectively within the Tehsil"

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
        else :
            parameter_block = f"The Tehsil {block} having total area {total_area} hectares" + parameter_block + "."

        return parameter_block

    except Exception as e:
        logger.info("The geojson is empty !", e)
        return "", ""


def get_pattern_intensity(state, district, block, mws_pattern_intensity, pattern_count):
    try:
        file_path = DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx"

        block_patterns = load_block_patterns()

        
        
        return mws_pattern_intensity

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity: %s",
            district,
            block,
            e
        )
        return mws_pattern_intensity


def get_agriculture_data(state, district, block):
    try:
        file_path = (DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx")

        df = pd.read_excel(file_path, sheet_name="croppingIntensity_annual")

        # Find all columns
        doubly_cropped_columns = [col for col in df.columns if col.startswith("doubly_cropped_area_in_ha_")]

        # Convert to numeric
        df["sum_area_in_ha"] = pd.to_numeric(df["sum_area_in_ha"], errors="coerce")
        for col in doubly_cropped_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Calculate average percentage for each UID
        result = []

        for index, row in df.iterrows():
            uid = row["UID"]
            sum_area = row["sum_area_in_ha"]
            
            # Skip if sum_area is 0 or NaN
            if sum_area == 0 or pd.isna(sum_area):
                result.append({
                    "uid": uid,
                    "avg_doubly_cropped_percent": 0
                })
                continue
            
            # Calculate percentages for all years
            percentages = []
            for col in doubly_cropped_columns:
                doubly_area = row[col]
                if not pd.isna(doubly_area):
                    percent = (doubly_area / sum_area) * 100
                    percentages.append(percent)
            
            # Calculate average
            avg_percent = sum(percentages) / len(percentages) if percentages else 0
            
            result.append({
                "uid": uid,
                "avg_doubly_cropped_percent": round(avg_percent, 2)
            })
        
        return result

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity",
            district,
            block
        )

        return []
