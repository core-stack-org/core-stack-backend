import re
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


def format_years(year_list):
    if not year_list:
        return ""
    if len(year_list) == 1:
        return year_list[0]
    return "{} and {}".format(", ".join(year_list[:-1]), year_list[-1])


def format_date_monsoon_onset(date_list):
    if not date_list:
        return (None, None)

    standardized_dates = []
    for item in date_list:
        if not item or isinstance(item, (int, float)):
            continue

        s = str(item).strip()
        parts = s.split("-")
        if len(parts) != 3:
            continue

        y, m, d = parts
        try:
            y = int(y); m = int(m); d = int(d)
            standardized_dates.append(f"{y:04d}-{m:02d}-{d:02d}")
        except ValueError:
            continue

    dates = []
    for ds in standardized_dates:
        try:
            dates.append(datetime.strptime(ds, "%Y-%m-%d"))
        except ValueError:
            # Invalid calendar dates get skipped
            continue

    if not dates:
        return (None, None)

    min_date = min(dates)
    max_date = max(dates)

    return min_date.strftime("%m-%d"), max_date.strftime("%m-%d")


def extract_years(items, *, start_only=True):
    years = []
    seen = set()

    for s in map(str, items):
        s = s or ""

        if start_only:
            # Prefer the start of an explicit range YYYY-YYYY
            m = re.search(r'(?<!\d)((?:19|20)\d{2})(?=\s*-\s*(?:19|20)\d{2})', s)
            if m:
                candidates = [m.group(1)]
            else:
                # Otherwise take the first standalone year in the string
                m2 = re.search(r'\b(?:19|20)\d{2}\b', s)
                candidates = [m2.group(0)] if m2 else []
        else:
            # Collect all standalone years
            candidates = [m.group(0) for m in re.finditer(r'\b(?:19|20)\d{2}\b', s)]

        for y in candidates:
            if y not in seen:
                seen.add(y)
                years.append(y)

    return sorted(years, key=int)


def extract_years_single(items):
    years, seen = [], set()
    for s in map(str, items):
        for m in re.finditer(r'(?<!\d)(?:19|20)\d{2}(?!\d)', s):
            y = m.group(0)
            if y not in seen:
                seen.add(y)
                years.append(y)
    return sorted(years, key=int) 


def get_rainfall_type(rainfall):
    if rainfall < 740:
        return "Semi-arid"
    elif rainfall >= 740 and rainfall < 960:
        return "Arid"
    elif rainfall >= 960 and rainfall < 1200:
        return "Moderate"
    elif rainfall >= 1200 and rainfall < 1620:
        return "High"
    else:
        return "Very high"


# ? MARK: MAIN SECTION
def get_osm_data(state, district, block, uid):
    try:
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

        uids_to_filter = [uid]
        mws_gdf = region_gdf[region_gdf["uid"].isin(uids_to_filter)]

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
                
                # if DEBUG: 
                    # Save to file (locally)
                    # with open('overpass_response.json', 'w', encoding='utf-8') as f:
                    #     json.dump(response, f, indent=2, ensure_ascii=False)

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

        if not outer_boundary_gdf.empty:
            mws_area = outer_boundary_gdf.geometry.area

        if not forests_df.empty:
            filtered_forests_gdf, forest_lines, forests_points = filter_within_boundary(
                forests_df, mws_gdf, combined_geometry
            )
            final_data["forests_mws"] = calculate_area(filtered_forests_gdf)
            filtered_forests_gdf, forest_lines, forests_points = filter_within_boundary(
                forests_df, outer_boundary_gdf, combined_geometry
            )
            final_data["forests"] = calculate_area(filtered_forests_gdf)

        if not lakes_df.empty:
            filtered_lakes_gdf, lake_lines, lakes_points = filter_within_boundary(
                lakes_df, mws_gdf, combined_geometry
            )
            final_data["lakes_mws"] = calculate_area(filtered_lakes_gdf)
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
            filtered_reservoirs_gdf, reservoir_lines, reservoirs_points = (
                filter_within_boundary(reservoirs_df, mws_gdf, combined_geometry)
            )
            final_data["reservoirs_mws"] = calculate_area(filtered_reservoirs_gdf)

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

            filtered_cities_gdf, city_lines, cities_points = filter_within_boundary(
                cities_df, mws_gdf, combined_geometry
            )
            if not cities_points.empty:
                for index, city in cities_points.iterrows():
                    city_point = city["geometry"]
                    name = city["tags"]["name"]
                    position = check_point_position(outer_boundary_gdf, city_point)
                    final_data["cities_mws"].append(
                        {"name": name, "position": position}
                    )

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

            filtered_hills_gdf, hill_lines, hills_points = filter_within_boundary(
                hill_df, mws_gdf, combined_geometry
            )
            if not hills_points.empty:
                for index, hill in hills_points.iterrows():
                    hill_point = hill["geometry"]
                    name = hill["tags"]["name"]
                    position = check_point_position(outer_boundary_gdf, hill_point)
                    final_data["hills_mws"].append({"name": name, "position": position})

        if not ridges_df.empty:
            filtered_ridges_gdf, ridge_lines, ridges_points = filter_within_boundary(
                ridges_df, outer_boundary_gdf, combined_geometry
            )
            if not ridge_lines.empty:
                for index, ridge in ridge_lines.iterrows():
                    ridge_point = ridge["geometry"]
                    name = ridge["tags"]["name"]
                    final_data["ridges"].append({"name": name})

            filtered_ridges_gdf, ridge_lines, ridges_points = filter_within_boundary(
                ridges_df, mws_gdf, combined_geometry
            )
            if not ridge_lines.empty:
                for index, ridge in ridge_lines.iterrows():
                    ridge_point = ridge["geometry"]
                    name = ridge["tags"]["name"]
                    final_data["ridges_mws"].append({"name": name})

        if not river_df.empty:
            filtered_river_gdf, river_lines, river_points = filter_within_boundary(
                river_df, outer_boundary_gdf, combined_geometry
            )
            final_data["river"] = calculate_river_length(river_lines)
            final_data["river"] += calculate_river_length(filtered_river_gdf)

            filtered_river_gdf, river_lines, river_points = filter_within_boundary(
                river_df, mws_gdf, combined_geometry
            )
            final_data["river_mws"] = calculate_river_length(river_lines)
            final_data["river_mws"] += calculate_river_length(filtered_river_gdf)

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

        # ? MWS Parameters
        parameter_mws = f""

        if final_data["cities_mws"]:
            city_names = [city["name"] for city in final_data["cities_mws"]]
            parameter_mws += f", which has towns and cities of "
            if len(city_names) == 1:
                parameter_mws += city_names[0]
            elif len(city_names) == 2:
                parameter_mws += " and ".join(city_names)
            else:
                parameter_mws += ", ".join(city_names[:-1]) + ", and " + city_names[-1]

        if final_data["hills_mws"] or final_data["ridges_mws"]:
            temp = [hill["name"] for hill in final_data["hills_mws"]]
            temp += [hill["name"] for hill in final_data["ridges_mws"]]
            parameter_mws += f". Key natural features such as {temp} shape the micro-watershed landscape and impact water flow"

        if final_data["forests_mws"]:
            large_forests_mws = [f for f in final_data["forests_mws"] if f["area_sq_m"] >= MIN_AREA_THRESHOLD]
            if large_forests_mws:
                parameter_mws += (
                    f". Part of {large_forests_mws[0]['name']}, covering roughly "
                    f"{(round(large_forests_mws[0]['area_sq_m'] / 10000))} hectares, lies within the micro-watershed supporting local wildlife and promoting biodiversity"
                )

        if final_data["lakes_mws"] or final_data["reservoirs_mws"]:
            large_lakes_mws = [lake for lake in final_data["lakes_mws"] if lake["area_sq_m"] >= MIN_AREA_THRESHOLD]
            large_reservoirs_mws = [res for res in final_data["reservoirs_mws"] if res["area_sq_m"] >= MIN_AREA_THRESHOLD]
            
            if large_lakes_mws or large_reservoirs_mws:
                rname = [temp["name"] for temp in large_lakes_mws]
                rname += [temp["name"] for temp in large_reservoirs_mws]
                rarea = [
                    str(round(temp["area_sq_m"] / 10000, 1))
                    for temp in large_lakes_mws
                ]
                rarea += [
                    str(round(temp["area_sq_m"] / 10000, 1))
                    for temp in large_reservoirs_mws
                ]

                parameter_mws += f". Additionally, large water bodies such as "
                if len(rname) == 1:
                    parameter_mws += rname[0]
                elif len(rname) == 2:
                    parameter_mws += " and ".join(rname)
                else:
                    parameter_mws += ", ".join(rname[:-1]) + ", and " + rname[-1]
                parameter_mws += f" span about "
                if len(rname) == 1:
                    parameter_mws += rarea[0]
                elif len(rname) == 2:
                    parameter_mws += " and ".join(rarea)
                else:
                    parameter_mws += ", ".join(rarea[:-1]) + ", and " + rarea[-1]
                parameter_mws += f"  hectares  respectively within the micro-watershed, providing essential resources for irrigation, fishing, and drinking water"

        if final_data["river_mws"]:
            rname = [temp["name"] for temp in final_data["river_mws"]]
            rarea = [
                str(round((temp["length"]) / 1000, 1))
                for temp in final_data["river_mws"]
            ]

            parameter_mws += f". The "
            if len(rname) == 1:
                parameter_mws += rname[0]
            elif len(rname) == 2:
                parameter_mws += " and ".join(rname)
            else:
                parameter_mws += ", ".join(rname[:-1]) + ", and " + rname[-1]
            parameter_mws += f" flowing "
            if len(rname) == 1:
                parameter_mws += rarea[0]
            elif len(rname) == 2:
                parameter_mws += " and ".join(rarea)
            else:
                parameter_mws += ", ".join(rarea[:-1]) + ", and " + rarea[-1]
            parameter_mws += f"  kilometers within the micro-watershed, serve"
            if len(rname) == 1:
                parameter_mws += "s"
            parameter_mws += (
                f" as a crucial water source for agriculture and daily needs"
            )
        
        if parameter_block == "":
            parameter_block = f"The Tehsil {block.capitalize()} lies in district {district.capitalize()} in {state.capitalize()}."
        else :
            parameter_block = f"The Tehsil {block} having total area {total_area} hectares" + parameter_block + "."

        if parameter_mws == "":
            parameter_mws = f"The micro-watershed {uid} is in Tehsil {block} which lies in district {district.capitalize()} in {state.capitalize()}."
        else :
            parameter_mws = f"The micro-watershed {uid} is in Tehsil {block}" + parameter_mws + "."

        return parameter_block, parameter_mws

    except Exception as e:
        logger.info("The geojson is empty !", e)
        return "", ""


def get_terrain_data(state, district, block, uid):
    try:
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
        df["hill_slope_area_percent"] = pd.to_numeric(df["hill_slope_area_percent"], errors="coerce")
        df["plain_area_percent"] = pd.to_numeric(df["plain_area_percent"], errors="coerce")
        df["ridge_area_percent"] = pd.to_numeric(df["ridge_area_percent"], errors="coerce")
        df["slopy_area_percent"] = pd.to_numeric(df["slopy_area_percent"], errors="coerce")
        df["valley_area_percent"] = pd.to_numeric(df["valley_area_percent"], errors="coerce")

        (area, hill_slope,plain_area,ridge_area,slopy_area,valley_area) = df.loc[df["UID"] == uid,
            [   "area_in_ha",
                "hill_slope_area_percent",
                "plain_area_percent",
                "ridge_area_percent",
                "slopy_area_percent",
                "valley_area_percent"
            ],
        ].values[0]

        selected_columns_cluster = [col for col in df.columns if col.startswith("terrain_description")]
        
        filtered_df = df.loc[df["UID"] == uid, selected_columns_cluster].values[0]
        mws_area = df.loc[df["UID"] == uid, "area_in_ha"].values[0]

        #? Parameters Desc
        parameter_main = f""
        parameter_comp = f""
        parameter_lulc = f"During  2017- 22, the micro-watershed's slopes and plains have exhibited distinct land-use patterns."
        mws_lulc_area_slope = [0, 0, 0, 0]
        block_lulc_area_slope = [0, 0, 0, 0]

        mws_lulc_area_plain = [0, 0, 0, 0]
        block_lulc_area_plain = [0, 0, 0, 0]

        percent_slope = df.loc[df["UID"] == uid, "slopy_area_percent"].values[0]
        percent_plain = df.loc[df["UID"] == uid, "plain_area_percent"].values[0]
        percent_hill = df.loc[df["UID"] == uid, "hill_slope_area_percent"].values[0]
        percent_valley = df.loc[df["UID"] == uid, "valley_area_percent"].values[0]

        if filtered_df[0] == "Broad Plains and Slopes":
            parameter_main += f"The micro-watershed is spread across {round(mws_area,2)} hectares. The micro-watershed includes flat plains and gentle slopes with {round(percent_plain, 2)} % area as plains and {round(percent_slope, 2)} % area under broad slopes."

        elif filtered_df[0] == "Mostly Plains":
            parameter_main += f"The micro-watershed is spread across {round(mws_area,2)} hectares. The micro-watershed mainly consists of flat plains covering {round(percent_plain, 2)} % micro-watershed area."

        elif filtered_df[0] == "Broad Sloppy and Hilly":
            parameter_main += f"The micro-watershed is spread across {round(mws_area,2)} hectares. The terrain of our micro-watershed consists of gently sloping land and rolling hills with {round(percent_slope,2)} % area under broad slopes and {round(percent_hill, 2)} % area under hills."

        else:
            parameter_main += f"The micro-watershed is spread across {round(mws_area, 2)} hectares. The micro-watershed terrain is mainly hills and valleys with {round(percent_hill, 2)} % under hills and {round(percent_valley, 2)} % under valleys."

        #? Divergence Test

        total_block_area = df["area_in_ha"].sum()

        #* Calculate weighted area for each topography type
        block_hill_slope = sum(df["hill_slope_area_percent"] * df["area_in_ha"] / 100)
        block_plain_area = sum(df["plain_area_percent"] * df["area_in_ha"] / 100)
        block_ridge_area = sum(df["ridge_area_percent"] * df["area_in_ha"] / 100)
        block_slopy_area = sum(df["slopy_area_percent"] * df["area_in_ha"] / 100)
        block_valley_area = sum(df["valley_area_percent"] * df["area_in_ha"] / 100)

        #? Create Dictionary for comparison
        terrain_types = [
            "Plain Area",
            "Ridge Area",
            "Slopy Area",
            "Valley Area",
            "Hill Slopes",
        ]

        mws_areas = [plain_area, ridge_area, slopy_area, valley_area, hill_slope]

        block_areas = [
            block_plain_area * 100 / total_block_area,
            block_ridge_area * 100 / total_block_area,
            block_slopy_area * 100 / total_block_area,
            block_valley_area * 100 / total_block_area,
            block_hill_slope * 100 / total_block_area,
        ]

        #? Test for terrain comparison
        test_mws_area = np.array(mws_areas) / np.sum(mws_areas)
        test_block_area = np.array(block_areas) / np.sum(block_areas)

        js_divergence = jensenshannon(test_mws_area, test_block_area)
        threshold = 0.1

        block_top2 = sorted(
            zip(terrain_types, block_areas), key=lambda x: x[1], reverse=True
        )[:2]
        mws_top2 = sorted(
            zip(terrain_types, mws_areas), key=lambda x: x[1], reverse=True
        )[:2]

        block_top1, block_top1_pct = block_top2[0]
        block_top2, block_top2_pct = block_top2[1]

        mws_top1, mws_top1_pct = mws_top2[0]
        mws_top2, mws_top2_pct = mws_top2[1]

        if js_divergence > threshold:
            parameter_comp += f"The microwatershed profile differs from the typical microwatershed profile observed at the Tehsil level. While the Tehsil-level terrain is predominantly characterized by {round(block_top1_pct, 1)} % {block_top1} and {round(block_top2_pct, 1)} % {block_top2}, the microwatershed primarily consists of {round(mws_top1_pct, 1)} % {mws_top1} and {round(mws_top2_pct, 1)} % {mws_top2}."
        else:
            parameter_comp += f"The microwatershed profile is similar to the typical microwatershed profile observed at the Tehsil level."


        #? Land use on Slopes and Plains
        if "terrain_lulc_slope" in excel_file.sheet_names:

            df_slopes = pd.read_excel(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx",sheet_name="terrain_lulc_slope")

            block_shrub_area = sum(df_slopes["shrub_scrubs_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_barren_area = sum(df_slopes["barren_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_tree_area = sum(df_slopes["forests_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_kh_area = sum(df_slopes["single_kharif_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_non_kh_area = sum(df_slopes["single_non_kharif_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_double_area = sum(df_slopes["double_cropping_area_percent"] * df_slopes["area_in_ha"] / 100)
            block_triple_area = sum(df_slopes["triple_cropping_area_percent"] * df_slopes["area_in_ha"] / 100)

            block_lulc_area_slope[0] += (block_shrub_area / total_block_area) * 100
            block_lulc_area_slope[1] += (block_barren_area / total_block_area) * 100
            block_lulc_area_slope[2] += (block_tree_area / total_block_area) * 100
            block_lulc_area_slope[3] += ((block_kh_area + block_non_kh_area + block_double_area + block_triple_area) / total_block_area) * 100

            if uid in df_slopes["UID"].values:
                (area, tree_percent, shrub_percent, barren_percent, single_crop_kh, single_crop_non_kh, double_crop, triple_crop) = df_slopes.loc[df_slopes["UID"] == uid, ["area_in_ha", "forests_area_percent", "shrub_scrubs_area_percent", "barren_area_percent", "single_kharif_area_percent", "single_non_kharif_area_percent", "double_cropping_area_percent", "triple_cropping_area_percent"]].values[0]

                mws_lulc_area_slope[0] += float(shrub_percent)

                mws_lulc_area_slope[1] += float(barren_percent)

                mws_lulc_area_slope[2] += float(tree_percent)

                single_area_kh = (area * single_crop_kh) / 100
                single_area_non_kh = (area * single_crop_non_kh) / 100
                double_area = (area * double_crop) / 100
                triple_area = (area * triple_crop) / 100

                farmland_area = single_area_kh + single_area_non_kh + double_area + triple_area
                mws_lulc_area_slope[3] += (farmland_area / area) * 100   

                parameter_lulc += f" On the slopes, land use is predominantly characterized by {round(tree_percent, 2)} % trees, {round(shrub_percent,2)} % shrubs, and {round(barren_percent,2)} % barren areas."

        if "terrain_lulc_plain" in excel_file.sheet_names:
            df_plain = pd.read_excel(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx",sheet_name="terrain_lulc_plain")

            block_shrub_area = sum(df_plain["shrub_scrubs_area_percent"] * df_plain["area_in_ha"] / 100)
            block_barren_area = sum(df_plain["barren_area_percent"] * df_plain["area_in_ha"] / 100)
            block_tree_area = sum(df_plain["forests_area_percent"] * df_plain["area_in_ha"] / 100)
            block_single_area = sum(df_plain["single_kharif_area_percent"] * df_plain["area_in_ha"] / 100)
            block_double_area = sum(df_plain["double_cropping_area_percent"] * df_plain["area_in_ha"] / 100)
            block_triple_area = sum(df_plain["triple_cropping_area_percent"] * df_plain["area_in_ha"] / 100)

            block_lulc_area_plain[0] += (block_shrub_area / total_block_area) * 100
            block_lulc_area_plain[1] += (block_barren_area / total_block_area) * 100
            block_lulc_area_plain[2] += (block_tree_area / total_block_area) * 100
            block_lulc_area_plain[3] += ((block_single_area + block_double_area + block_triple_area) / total_block_area) * 100

            if uid in df_plain["UID"].values:
                
                (area, barren_percent, shrub_percent, tree_percent, single_crop, double_crop, triple_crop) = df_plain.loc[df_plain["UID"] == uid, ["area_in_ha", "barren_area_percent", "shrub_scrubs_area_percent", "forests_area_percent", "single_kharif_area_percent", "double_cropping_area_percent", "triple_cropping_area_percent"]].values[0]

                mws_lulc_area_plain[0] += float(shrub_percent)

                mws_lulc_area_plain[1] += float(barren_percent)

                mws_lulc_area_plain[2] += float(tree_percent)

                single_area = (area * (single_crop)) / 100
                double_area = (area * double_crop) / 100
                triple_area = (area * triple_crop) / 100

                farmland_area = (single_area) + (double_area) + (triple_area)

                farmland_area_percent = (farmland_area / area) * 100

                mws_lulc_area_plain[3] += float(farmland_area_percent)

                parameter_lulc += f" On the plains, land use has predominance of {round(farmland_area_percent,2)} % farmlands, {round(barren_percent,2)} % barren areas, and {round(shrub_percent,2)} % shrubs."

        return parameter_main, mws_areas, block_areas, parameter_comp, parameter_lulc, mws_lulc_area_slope, block_lulc_area_slope, mws_lulc_area_plain, block_lulc_area_plain

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block", district, block, e
        )
        return "", [], [], "", "", [], [], [], []


def get_change_detection_data(state, district, block, uid):
    try:
        df_degrad = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="change_detection_degradation",
        )
        df_defo = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="change_detection_deforestation",
        )
        df_urban = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="change_detection_urbanization",
        )
        df_restore = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="restoration_vector",
        )

        parameter_land = f""
        parameter_tree = f""
        parameter_urban = f""
        parameter_restore = f""

        # ? Land Degradation
        df_degrad["total_degradation_area_in_ha"] = df_degrad["total_degradation_area_in_ha"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_degrad.loc[df_degrad["UID"] == uid, "total_degradation_area_in_ha"]
        degradation = filtered_df.iloc[0]
        avg = df_degrad["total_degradation_area_in_ha"].mean()

        if degradation >= 20:
            parameter_land += f"There has been a considerate level of degradation of farmlands in this micro watershed over the years 2017-2022. As compared to average degraded land area of {round(avg, 2)} hectares per microwater-shed for the entire tehsil, the degraded land area in this micro-watershed is close to {round(degradation, 2)} hectares."

        # ? Tree Reduction
        df_defo["total_deforestation_area_in_ha"] = df_defo["total_deforestation_area_in_ha"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_defo.loc[df_defo["UID"] == uid, "total_deforestation_area_in_ha"]
        reduction = filtered_df.iloc[0]
        avg = df_defo["total_deforestation_area_in_ha"].mean()

        if reduction >= 0:
            parameter_tree += f"There has been a considerate level of reduction in tree cover in this micro watershed over the years 2017-2022, about {round(reduction, 1)} hectares, as compared to {round(avg, 1)} hectares on average per micro watershed in the entire tehsil."

        # ? Urbanization
        df_urban["total_urbanization_area_in_ha"] = df_urban["total_urbanization_area_in_ha"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_urban.loc[df_urban["UID"] == uid, "total_urbanization_area_in_ha"]
        built_up_area = filtered_df.iloc[0]

        if built_up_area >= 40:
            parameter_urban += f"There has been a considerate level of urbanization in this micro watershed with about {round(built_up_area, 2)} hectares of land covered with settlements."

        # ? Wide Scale Restoration
        df_restore["wide_scale_restoration_area_in_ha"] = df_restore["wide_scale_restoration_area_in_ha"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_restore.loc[df_restore["UID"] == uid, "wide_scale_restoration_area_in_ha"]
        restoration_area = filtered_df.iloc[0]

        if restoration_area > 0:
            parameter_restore += f"{round(restoration_area, 2)} hectares of this microwatershed has less than 40% canopy density and requires wide scale restoration interventions."

        filtered_df = df_restore.loc[df_restore["UID"] == uid, "protection_area_in_ha"]
        protection_area = filtered_df.iloc[0]

        if protection_area > 0:
            parameter_restore += f" {round(protection_area, 2)} hectares, on the other hand, need to be protected so the canopy density doesn’t fall further."

        return parameter_land, parameter_tree, parameter_urban, parameter_restore

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for degradation",
            district,
            block,
            e,
        )
        return "", "", "", ""


def get_land_conflict_industrial_data(state, district, block, uid):
    try:
        df = pd.read_excel(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx",sheet_name="lcw_conflict")

        filtered_title = df.loc[df["UID"] == uid, "title_of_conflict"]
        filtered_link = df.loc[df["UID"] == uid, "link_to_conflict"]

        titles = filtered_title.tolist()
        links = filtered_link.tolist()

        conflicts = [
            {"title": title, "link": link} 
            for title, link in zip(titles, links)
        ]

        return conflicts

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Land Conflict",
            district,
            block,
            e,
        )
        return []


def get_factory_data(state, district, block, uid):
    try:
        df = pd.read_excel(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx",sheet_name="factory_csr")

        filtered_names = df.loc[df["UID"] == uid, "Company_Name"]
        filtered_address = df.loc[df["UID"] == uid, "ADDRESS"]
        filtered_type = df.loc[df["UID"] == uid, "LOCATION T"]

        names = filtered_names.tolist()
        addresses = filtered_address.tolist()
        types = filtered_type.tolist()

        factories = [
            {"name": name, "address": address, "type": type_val} 
            for name, address, type_val in zip(names, addresses, types)
        ]

        return factories

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Factory Data",
            district,
            block,
            e,
        )
        return []


def get_mining_data(state, district, block, uid):
    try:
        df = pd.read_excel(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx",sheet_name="mining")

        filtered_names = df.loc[df["UID"] == uid, "division"]
        filtered_address = df.loc[df["UID"] == uid, "sector_moefcc"]
        filtered_type = df.loc[df["UID"] == uid, "village"]

        names = filtered_names.tolist()
        sectors = filtered_address.tolist()
        villages = filtered_type.tolist()

        mining_sites = [
            {"division": division, "sector": sector, "village": village} 
            for division, sector, village in zip(names, sectors, villages)
        ]

        return mining_sites

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Mining Data",
            district,
            block,
            e,
        )
        return []


def get_cropping_intensity(state, district, block, uid):
    try:
        df = pd.read_excel(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", sheet_name="croppingIntensity_annual")
        df_drought = pd.read_excel( DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", sheet_name="croppingDrought_kharif")

        selected_columns_inten = [col for col in df.columns if col.startswith("cropping_intensity_")]

        current_years = extract_years(selected_columns_inten)

        df[selected_columns_inten] = df[selected_columns_inten].apply(pd.to_numeric, errors="coerce")

        df["cropping_intensity_row_avg"] = df[selected_columns_inten].mean(axis=1, skipna=True)

        block_avg = df["cropping_intensity_row_avg"].mean(skipna=True)

        filtered_df_inten = df.loc[df["UID"] == uid, selected_columns_inten]

        if current_years and len(current_years) > 0:
            year_range_text = f"{current_years[0]} to {current_years[-1]}"
        else:
            year_range_text = ""

        if not filtered_df_inten.empty:

            inten_parameter_1 = f""
            inten_parameter_2 = f""

            # ? Mann Kendal Slope Calculation
            result = mk.original_test(filtered_df_inten.values[0])

            avg_inten = sum(filtered_df_inten.values[0]) / len(filtered_df_inten.values[0])
            
            if result.trend == "increasing":
                inten_parameter_1 += (
                    f"The cropping intensity of the micro-watershed has increased over the years {year_range_text} "
                    f"from {min(filtered_df_inten.values[0])} to {max(filtered_df_inten.values[0])} "
                    f"compared to the average cropping intensity of {round(block_avg, 2)} across the micro watersheds "
                    f"over the years in the Tehsil. "
                )
            else:
                if result.trend == "decreasing":
                    inten_parameter_1 += (
                        f"The cropping intensity of this area has reduced over the years {year_range_text} "
                        f"from {max(filtered_df_inten.values[0])} to {min(filtered_df_inten.values[0])} "
                        f"compared to the average cropping intensity of {round(block_avg, 2)} across the micro watersheds "
                        f"over the years in the Tehsil. "
                    )
                else :
                    if avg_inten > block_avg:
                        inten_parameter_1 += (
                            f"The cropping intensity of this area shows no definite trend. The average cropping intensity over the years is {round(avg_inten, 2)}, "
                            f"more than the average cropping intensity of {round(block_avg, 2)} across the micro watersheds "
                            f"in the Tehsil. "
                        )
                    elif avg_inten < block_avg:
                        inten_parameter_1 += (
                            f"The cropping intensity of this area shows no definite trend. The average cropping intensity over the years is {round(avg_inten, 2)}, "
                            f"less than the average cropping intensity of {round(block_avg, 2)} across the micro watersheds "
                            f"in the Tehsil. "
                        )
                    else:
                        inten_parameter_1 += (
                            f"The cropping intensity of this area shows no definite trend. The average cropping intensity over the years is {round(avg_inten, 2)}, "
                            f"similar to the average cropping intensity of {round(block_avg, 2)} across the micro watersheds "
                            f"in the Tehsil. "
                        )
                if avg_inten < 1.5:
                    inten_parameter_1 += f"It might be possible to improve cropping intensity through more strategic placement, while keeping equity in mind, of rainwater harvesting or groundwater recharge structures. "
            
            #? Drought Parameters
            selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
            selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]
            
            df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric, errors="coerce")
            df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

            mws_drought_moderate = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            mws_drought_severe = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            drought_years = []
            non_drought_years = []

            for index, item in enumerate(mws_drought_moderate):
                drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if drought_check > 5:
                    if match_exp:
                        drought_years.append(match_exp.group(0))
                else:
                    if match_exp:
                        non_drought_years.append(match_exp.group(0))
            
            drought_inten = 0
            non_drought_inten = 0

            for year in drought_years:
                selected_columns_d = [col for col in df.columns if col.startswith("cropping_intensity_unit_less_" + year)]

                filtered_d_df = df.loc[df["UID"] == uid, selected_columns_d]

                if not filtered_d_df.empty:
                    drought_inten += filtered_d_df.values[0][0]

            for year in non_drought_years:
                selected_columns_nd = [col for col in df.columns if col.startswith("cropping_intensity_unit_less_" + year)]

                filtered_nd_df = df.loc[df["UID"] == uid, selected_columns_nd]

                if not filtered_nd_df.empty:
                    non_drought_inten += filtered_nd_df.values[0][0]
            
            if len(drought_years):
                drought_inten = drought_inten / len(drought_years)

            if len(non_drought_years):
                non_drought_inten = non_drought_inten / len(non_drought_years)
            
            formatted_years = format_years(drought_years)

            if (non_drought_inten - drought_inten) > 0.2 and len(drought_years):
                inten_parameter_2 += f"Cropping intensity is reduced by {round(abs(drought_inten - non_drought_inten), 2)} during the drought years (AAA and BBB), as compared to non-drought years, and reveals a marked sensitivity of agricultural productivity to water scarcity. This decline underscores the critical need for farmers to adopt drought-resilient practices, such as constructing water harvesting structures. By capturing and storing rainwater, these structures can provide a crucial buffer against drought periods, helping to stabilize cropping intensity and sustain productivity even in water-stressed conditions."

            inten_parameter_2 = inten_parameter_2.replace("AAA and BBB",formatted_years)

            #? Cropping Areas Graphs
            selected_columns_single = [col for col in df.columns if col.startswith("single_cropped_area_")]
            selected_columns_double = [col for col in df.columns if col.startswith("doubly_cropped_area_")]
            selected_columns_triple = [col for col in df.columns if col.startswith("triply_cropped_area_")]
            selected_columns_sum = [col for col in df.columns if col.startswith("sum")]

            df[selected_columns_single] = df[selected_columns_single].apply(pd.to_numeric, errors="coerce")
            df[selected_columns_double] = df[selected_columns_double].apply(pd.to_numeric, errors="coerce")
            df[selected_columns_triple] = df[selected_columns_triple].apply(pd.to_numeric, errors="coerce")
            df[selected_columns_sum] = df[selected_columns_sum].apply(pd.to_numeric, errors="coerce")

            filtered_d_single = df.loc[df["UID"] == uid, selected_columns_single]
            filtered_d_double = df.loc[df["UID"] == uid, selected_columns_double]
            filtered_d_triple = df.loc[df["UID"] == uid, selected_columns_triple]
            filtered_d_sum = df.loc[df["UID"] == uid, selected_columns_sum]

            final_single_percent = []
            final_double_percent = []
            final_triple_percent = []
            final_non_cropped = []

            if not filtered_d_single.empty and not filtered_d_double.empty and not filtered_d_triple.empty:

                for single, double, triple in zip(filtered_d_single.values[0], filtered_d_double.values[0], filtered_d_triple.values[0]):
                    if filtered_d_sum.values[0][0] != 0:
                        p1 = (float(single) / float(filtered_d_sum.values[0][0])) * 100
                        p2 = (float(double) / float(filtered_d_sum.values[0][0])) * 100
                        p3 = (float(triple) / float(filtered_d_sum.values[0][0])) * 100
                    else:
                        p1 = 0
                        p2 = 0
                        p3 = 0
                    final_single_percent.append(round(p1,2))
                    final_double_percent.append(round(p2,2))
                    final_triple_percent.append(round(p3,2))
                    final_non_cropped.append(100 - round(p1+p2+p3, 2))

            return inten_parameter_1, inten_parameter_2, final_single_percent, final_double_percent, final_triple_percent, final_non_cropped, current_years

        else:
            return "", "", [],[],[],[],[]

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity",
            district,
            block
        )
        return "", "", [],[],[],[],[]


def get_double_cropping_area(state, district, block, uid):
    try:
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
            sheet_name="croppingIntensity_annual",
        )

        selected_columns_single = [
            col for col in df.columns if col.startswith("single_cropped_area_")
        ]
        df[selected_columns_single] = df[selected_columns_single].apply(
            pd.to_numeric, errors="coerce"
        )

        selected_columns_double = [
            col for col in df.columns if col.startswith("doubly_cropped_area")
        ]
        df[selected_columns_double] = df[selected_columns_double].apply(
            pd.to_numeric, errors="coerce"
        )

        selected_columns_triple = [
            col for col in df.columns if col.startswith("triply_cropped_area")
        ]
        df[selected_columns_triple] = df[selected_columns_triple].apply(
            pd.to_numeric, errors="coerce"
        )

        filtered_df_single = df.loc[df["UID"] == uid, selected_columns_single].values[0]
        filtered_df_double = df.loc[df["UID"] == uid, selected_columns_double].values[0]
        filtered_df_triple = df.loc[df["UID"] == uid, selected_columns_triple].values[0]

        current_years = extract_years(selected_columns_single)

        if current_years and len(current_years) > 0:
            year_range_text = f"{current_years[0]} to {current_years[-1]}"
        else:
            year_range_text = ""

        double_cropping_percent = []

        for index, area in enumerate(filtered_df_single):
            total_cropped_area = (
                filtered_df_single[index]
                + filtered_df_double[index]
                + filtered_df_triple[index]
            )
            
            double_cropping_percent.append(
                (filtered_df_double[index] / total_cropped_area) * 100
            )

        double_cropping_percent_avg = sum(double_cropping_percent) / len(double_cropping_percent)

        double_cropping_avg = sum(filtered_df_double) / len(filtered_df_double)

        parameter_double_crop = f""

        if double_cropping_percent_avg < 30:
            parameter_double_crop += f"This microwatershed area has a low percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is less than 30% of the total agricultural land being cultivated twice a year."
        elif double_cropping_percent_avg >= 30 and double_cropping_percent_avg < 60:
            parameter_double_crop += f"This microwatershed area has a moderate percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is about {round(double_cropping_percent_avg, 2)}% of the total agricultural land being cultivated twice a year."
        else:
            parameter_double_crop += f"This microwatershed area has a high percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is more than 60% of the total agricultural land being cultivated twice a year."

        return parameter_double_crop, year_range_text

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for double cropping section",
            district,
            block
        )
        return "", ""


def get_surface_Water_bodies_data(state, district, block, uid):
    try:
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
            sheet_name="surfaceWaterBodies_annual",
        )
        df_drought = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="croppingDrought_kharif",
        )

        selected_columns = [col for col in df.columns if col.startswith("total_area_")]
        df[selected_columns] = df[selected_columns].apply(
            pd.to_numeric, errors="coerce"
        )

        current_years = extract_years(selected_columns)

        if current_years and len(current_years) > 0:
            year_range_text = f"{current_years[0]} to {current_years[-1]}"
        else:
            year_range_text = ""

        parameter_swb_1 = f""
        parameter_swb_2 = f""
        parameter_swb_3 = f""
        filtered_df_kharif = []
        filtered_df_rabi = []
        filtered_df_zaid = []

        filtered_df = df.loc[df["UID"] == uid, selected_columns]

        if not filtered_df.empty:

            selected_columns_kh = [col for col in df.columns if col.startswith("kharif_area_in_ha_")]

            selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
            selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]

            df[selected_columns_kh] = df[selected_columns_kh].apply(pd.to_numeric, errors="coerce")
            df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric, errors="coerce")
            df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")


            #? Trend Calculation
            filtered_df_kh = df.loc[df["UID"] == uid, selected_columns_kh].values[0]

            result = mk.original_test(filtered_df_kh)

            if result.trend == "increasing":
                parameter_swb_1 = f"Surface water presence has increased by {round(result.slope, 2)} hectares per year during 2017-22."
            elif result.trend == "decreasing":
                parameter_swb_1 = f"Surface water presence has decreased by {round(result.slope, 2)} hectares per year during 2017-22.Siltation could be a cause for decrease in surface water presence and therefore may require repair and maintenance of surface water bodies. Waterbody analysis can help identify waterbodies that may need such treatment."
            else:
                parameter_swb_1 = f"The surface water availability shows no definite trend over the years {year_range_text}."

            #? Drought Years SWB
            mws_drought_moderate = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            mws_drought_severe = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            drought_years = []
            non_drought_year = []

            for index, item in enumerate(mws_drought_moderate):
                drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    if drought_check > 5:
                        drought_years.append(match_exp.group(0))
                    else:
                        non_drought_year.append(match_exp.group(0))
            

            if len(drought_years):
                
                total_area_d = 0
                total_area_nd = 0

                for year in drought_years:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    yearly_area = df.loc[df["UID"] == uid, selected_column_temp].values[0]
                    total_area_d += yearly_area[0]


                for year in non_drought_year:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    yearly_area = df.loc[df["UID"] == uid, selected_column_temp].values[0]
                    total_area_nd += yearly_area[0]
                
                percent_nd_t_d = ((total_area_nd - total_area_d) / total_area_nd ) * 100

                if result.trend == "increasing":
                    parameter_swb_2 = f"During the monsoon, on average we observe that the area under surface water during drought years ({' and '.join(map(str, drought_years))}) is {round(percent_nd_t_d, 2)}% less than during non-drought years. This decline highlights a significant impact of drought on surface water availability during the primary crop-growing season, and indicates sensitivity of the cropping to droughts."
                    
                else:
                    parameter_swb_2 = f"During the monsoon, we observed a {round(percent_nd_t_d, 2)}% decrease in surface water area during drought years ({' and '.join(map(str, drought_years))}), as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."

            #? Non-Drought Years SWB
            if len(non_drought_year):
                area_under_rb_nd = 0
                area_under_kh_nd = 0
                percent_rb_kh = 0

                for year in non_drought_year:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    yearly_area_kh = df.loc[df["UID"] == uid, selected_column_temp].values[0]

                    selected_column_temp_rb = [col for col in df.columns if col.startswith("rabi_area_in_ha_" + year)]
                    yearly_area_rb = df.loc[df["UID"] == uid, selected_column_temp_rb].values[0]

                    area_under_rb_nd += yearly_area_rb[0]
                    area_under_kh_nd += yearly_area_kh[0]

                if area_under_kh_nd:
                    percent_rb_kh = ((area_under_kh_nd - area_under_rb_nd) / area_under_kh_nd ) * 100

                    if result.trend == "increasing":
                        parameter_swb_3 += f"In non-drought years, surface water typically decreases by {round(percent_rb_kh,2)}% from the Kharif to the Rabi season."
                    elif result.trend == "decreasing":
                        parameter_swb_3 += f"In non-drought years, surface water in kharif typically decreases by {round(percent_rb_kh,2)}% in rabi."
                    else:
                        parameter_swb_3 += f"In non-drought years, surface water in kharif typically decreases by {round(percent_rb_kh,2)}% in rabi."

            if len(drought_years):
                area_under_rb = 0
                area_under_kh = 0
                percent_rb_kh = 0

                for year in drought_years:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    yearly_area_kh = df.loc[df["UID"] == uid, selected_column_temp].values[0]

                    selected_column_temp_rb = [col for col in df.columns if col.startswith("rabi_area_in_ha_" + year)]
                    yearly_area_rb = df.loc[df["UID"] == uid, selected_column_temp_rb].values[0]

                    area_under_rb += yearly_area_rb[0]
                    area_under_kh += yearly_area_kh[0]
                
                if area_under_kh_nd:
                    percent_rb_kh = ((area_under_kh - area_under_rb) / area_under_kh ) * 100

                if result.trend == "increasing":
                    parameter_swb_3 += f" However, during drought years, this reduction reaches {round(percent_rb_kh,2)}% from Kharif to Rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."
                elif result.trend == "decreasing":
                    parameter_swb_3 += f" However, during drought years, this seasonal reduction is {round(percent_rb_kh,2)} % from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."
                else:
                    parameter_swb_3 += f" However, during drought years, this seasonal reduction is {round(percent_rb_kh,2)} % from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

            # ? Data yearwise for waterbody
            selected_columns_kharif = [col for col in df.columns if col.startswith("kharif_area_in_ha_")]
            selected_columns_rabi = [col for col in df.columns if col.startswith("rabi_area_in_ha_")]
            selected_columns_zaid = [col for col in df.columns if col.startswith("zaid_area_in_ha_")]

            df[selected_columns_kharif] = df[selected_columns_kharif].apply(pd.to_numeric, errors="coerce")
            df[selected_columns_rabi] = df[selected_columns_rabi].apply(pd.to_numeric, errors="coerce")
            df[selected_columns_zaid] = df[selected_columns_zaid].apply(pd.to_numeric, errors="coerce")

            filtered_df_kharif = (df.loc[df["UID"] == uid, selected_columns_kharif].values[0].tolist())
            filtered_df_rabi = (df.loc[df["UID"] == uid, selected_columns_rabi].values[0].tolist())
            filtered_df_zaid = (df.loc[df["UID"] == uid, selected_columns_zaid].values[0].tolist())

            #filtered_df_kharif = [abs(kharif - rabi) for kharif, rabi in zip(filtered_df_kharif, filtered_df_rabi)]
            #filtered_df_rabi = [abs(rabi - zaid) for rabi, zaid in zip(filtered_df_rabi, filtered_df_zaid)]

        else:
            parameter_swb_1 += (
                f"No surface water bodies were detected through remote sensing in this micro-watershed."
            )

        return (
            parameter_swb_1,
            parameter_swb_2,
            parameter_swb_3,
            filtered_df_kharif,
            filtered_df_rabi,
            filtered_df_zaid,
            current_years
        )

    except Exception as e:
        logger.info("Not able to access excel for %s district, %s block for Waterbodies",district,block)
        return "", "", "", [], [], [], []


def get_water_balance_data(state, district, block, uid):
    try:
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
            sheet_name="hydrological_annual",
        )
        df_drought = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="croppingDrought_kharif",
        )

        df_seasonal = pd.read_excel(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="hydrological_seasonal",
        )

        #? Parameters and Lists for Graphs
        trend_desc = f""
        good_rainfall = f""
        bad_rainfall = f""

        #? Columns
        selected_column_dg = [col for col in df.columns if col.startswith("DeltaG_")]
        selected_column_g = [col for col in df.columns if col.startswith("G_")]

        selected_columns_moderate = [col for col in df_drought.columns if col.startswith("Moderate_")]
        selected_columns_severe = [col for col in df_drought.columns if col.startswith("Severe_")]

        df[selected_column_dg] = df[selected_column_dg].apply(pd.to_numeric, errors="coerce")
        df[selected_column_g] = df[selected_column_g].apply(pd.to_numeric, errors="coerce")

        df_drought[selected_columns_moderate] = df_drought[selected_columns_moderate].apply(pd.to_numeric, errors="coerce")
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(pd.to_numeric, errors="coerce")

        current_years = extract_years(selected_column_dg)
        
        #? Trend Calculation
        filtered_df_dg = df.loc[df["UID"] == uid, selected_column_dg].values[0]
        avg_del_g = sum(filtered_df_dg) / len(filtered_df_dg)
        
        filtered_df_g = df.loc[df["UID"] == uid, selected_column_g].values[0]

        result = mk.original_test(filtered_df_g)

        if avg_del_g >= 0:
            if result.trend == "increasing":
                trend_desc += f"The water balance is positive and indicates that the groundwater situation in this microwatershed may be stable. Year on year, the groundwater situation seems to be improving."
            else:
                trend_desc += f"The water balance is positive and indicates that the groundwater situation in this microwatershed may be stable. This however should not be a cause for complacency - over-extraction should be reduced, because over the years it seems that the rate of extraction of groundwater has increased. "
        else:
            if result.trend == "increasing":
                trend_desc += f"The water balance is negative and indicates that the groundwater situation in this microwatershed is bad but is improving. There may be efforts of recharge which seems to improve groundwater despite extraction of groundwater."
            else:
                trend_desc += f"The water balance is negative and indicates that the groundwater situation in this microwatershed is bad and is worsening. This is a matter of worry. Year on year, the groundwater seems to be depleting due to persistent over-extraction over the years."
        
        # ? Drought Years
        mws_drought_moderate = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
        mws_drought_severe = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

        drought_years = []
        non_drought_years = []

        for index, item in enumerate(mws_drought_moderate):
            drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
            match_exp = re.search(r"\d{4}", selected_columns_severe[index])
            if drought_check > 5:
                if match_exp:
                    drought_years.append(match_exp.group(0))
            else:
                if match_exp:
                    non_drought_years.append(match_exp.group(0))


        #? Good Rainfall Years
        if len(non_drought_years):

            avg_rainfall = 0
            avg_fortnight_delg = 0
            monsoon_onset = []
            runoff_percent = 0

            for year in non_drought_years:

                #? Rainfall
                selected_column_precp = [col for col in df.columns if col.startswith("Precipitation_in_mm_" + year)]
                rainfall = df.loc[df["UID"] == uid, selected_column_precp].values[0]
                avg_rainfall += rainfall[0]

                #? Monsoon Onset
                selected_column_onset = [col for col in df_drought.columns if col.startswith("monsoon_onset_" + year)]
                onset = df_drought.loc[df_drought["UID"] == uid, selected_column_onset].values[0]
                monsoon_onset.append(onset[0])

                #? Fortnight Delg Calc
                selected_column_kh = [col for col in df_seasonal.columns if col.startswith("delta g_kharif_in_mm_" + year)]
                selected_column_rb = [col for col in df_seasonal.columns if col.startswith("delta g_rabi_in_mm_" + year)]
                selected_column_zd = [col for col in df_seasonal.columns if col.startswith("delta g_zaid_in_mm_" + year)]

                delg_kh = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_kh].values[0]
                delg_rb = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_rb].values[0]
                delg_zd = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_zd].values[0]

                avg_fortnight_delg += (delg_kh[0] + delg_rb[0] + delg_zd[0])

                #? Runoff
                selected_column_runoff = [col for col in df.columns if col.startswith("RunOff_in_mm_" + year)]
                runoff = df.loc[df["UID"] == uid, selected_column_runoff].values[0]

                runoff_percent += ((runoff[0] / rainfall[0]) * 100)
            
            avg_rainfall = avg_rainfall / len(non_drought_years)
            avg_fortnight_delg = avg_fortnight_delg / len(non_drought_years)
            runoff_percent = runoff_percent / len(non_drought_years)

            min_date, max_date = format_date_monsoon_onset(monsoon_onset)

            original_string = (
                "In the micro-watershed, XXX, YYY and ZZZ were good rainfall years,"
            )
            formatted_years = format_years(non_drought_years)
            good_rainfall += original_string.replace("XXX, YYY and ZZZ", formatted_years)

            good_rainfall += f"bringing an average annual rainfall of approximately {round(avg_rainfall,2)} mm"

            if(min_date != None and max_date != None):
                good_rainfall += f" with monsoon onset between [{min_date}, {max_date}]."
            else:
                good_rainfall += f"."

            if avg_fortnight_delg > 0:
                good_rainfall += f"This rainfall pattern resulted in positive groundwater recharge, with average groundwater change of {round(avg_fortnight_delg,2)} mm, indicating replenishment of groundwater resources. During these years, around {round(runoff_percent,2)} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds. "
            else:
                good_rainfall += f"This rainfall pattern resulted in negative groundwater recharge, with average groundwater change of {round(avg_fortnight_delg,2)} mm, indicating depletion of groundwater resources. During these years, around {round(runoff_percent,2)} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds. "

        #? Bad Rainfall Years
        if len(drought_years):
            avg_rainfall = 0
            avg_fortnight_delg = 0
            runoff_percent = 0

            for year in drought_years:

                #? Rainfall
                selected_column_precp = [col for col in df.columns if col.startswith("Precipitation_in_mm_" + year)]
                rainfall = df.loc[df["UID"] == uid, selected_column_precp].values[0]
                avg_rainfall += rainfall[0]

                #? Fortnight Delg Calc
                selected_column_kh = [col for col in df_seasonal.columns if col.startswith("delta g_kharif_in_mm_" + year)]
                selected_column_rb = [col for col in df_seasonal.columns if col.startswith("delta g_rabi_in_mm_" + year)]
                selected_column_zd = [col for col in df_seasonal.columns if col.startswith("delta g_zaid_in_mm_" + year)]

                delg_kh = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_kh].values[0]
                delg_rb = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_rb].values[0]
                delg_zd = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_zd].values[0]

                avg_fortnight_delg += (delg_kh[0] + delg_rb[0] + delg_zd[0])

                #? Runoff
                selected_column_runoff = [col for col in df.columns if col.startswith("RunOff_in_mm_" + year)]
                runoff = df.loc[df["UID"] == uid, selected_column_runoff].values[0]

                runoff_percent += ((runoff[0] / rainfall[0]) * 100)

            avg_rainfall = avg_rainfall / len(drought_years)
            avg_fortnight_delg = avg_fortnight_delg / len(drought_years)
            runoff_percent = runoff_percent / len(drought_years)

            original_string = (
                "In contrast, XXX and YYY were bad rainfall years,"
            )
            formatted_years = format_years(drought_years)
            bad_rainfall += original_string.replace("XXX and YYY", formatted_years)

            bad_rainfall += f" leading to annual rainfall averaging around {round(avg_rainfall,2)} mm."

            if avg_fortnight_delg >= 0:
                bad_rainfall += f"Limited water availability in these years resulted in positive groundwater changes, with an average replenishment of {round(avg_fortnight_delg,2)} mm. Runoff in these years is {round(runoff_percent,2)} % of total rainfall, diminishing the harvestable water. "
            else:
                bad_rainfall += f"Limited water availability in these years resulted in negative groundwater changes, with an average depletion of {round(avg_fortnight_delg,2)} mm. Runoff in these years is {round(runoff_percent,2)} % of total rainfall, diminishing the harvestable water."

        selected_columns_precip = [col for col in df.columns if col.startswith("Precipitation_")]
        df[selected_columns_precip] = df[selected_columns_precip].apply(pd.to_numeric, errors="coerce")
        filtered_df_precip = (df.loc[df["UID"] == uid, selected_columns_precip].values[0].tolist())

        selected_columns_runoff = [col for col in df.columns if col.startswith("RunOff_")]
        df[selected_columns_runoff] = df[selected_columns_runoff].apply(pd.to_numeric, errors="coerce")
        filtered_df_runoff = (df.loc[df["UID"] == uid, selected_columns_runoff].values[0].tolist())

        selected_columns_et = [col for col in df.columns if col.startswith("ET_")]
        df[selected_columns_et] = df[selected_columns_et].apply(pd.to_numeric, errors="coerce")
        filtered_df_et = (df.loc[df["UID"] == uid, selected_columns_et].values[0].tolist())

        selected_columns_dg = [col for col in df.columns if col.startswith("DeltaG_")]
        df[selected_columns_dg] = df[selected_columns_dg].apply(pd.to_numeric, errors="coerce")
        filtered_df_dg = (df.loc[df["UID"] == uid, selected_columns_dg].values[0].tolist())

        return (
            trend_desc,
            good_rainfall,
            bad_rainfall,
            filtered_df_precip,
            filtered_df_runoff,
            filtered_df_et,
            filtered_df_dg,
            current_years
        )

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Water Balance",
            district,
            block
        )
        return "", "", "", [], [], [], [], []


def get_soge_data(state, district, block, uid):
    try :
        df = pd.read_excel(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", sheet_name="aquifer_vector")
        df_soge = pd.read_excel(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", sheet_name="soge_vector")
        df_hydro = pd.read_excel(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", sheet_name="hydrological_annual")

        parameter_soge = f""

        aquifer_class = df.loc[df["UID"] == uid, "aquifer_class"].values[0]

        if(aquifer_class == "Alluvium"):
            soge_class = df_soge.loc[df_soge["UID"] == uid, "class_name"].values[0]

            selected_column_g = [col for col in df_hydro.columns if col.startswith("G_")]
            df_hydro[selected_column_g] = df_hydro[selected_column_g].apply(pd.to_numeric, errors="coerce")
            filtered_df_g = df_hydro.loc[df_hydro["UID"] == uid, selected_column_g].values[0]

            result = mk.original_test(filtered_df_g)

            if(soge_class == "Safe"):
                
                parameter_soge += f"Extraction is within recharge limits."
                
                if result.trend == "increasing" :
                    parameter_soge += f" However, the groundwater situation appears stable and annual usage is also within limits. Care should be taken that things remain the way they are."
                else:
                    parameter_soge += f" However, it requires close monitoring to check that the situation is not worsened."
            
            elif(soge_class == "Semi-Critical"):

                parameter_soge += f"Extraction is 70–90% of the recharge. The signs of stress have started to appear."

                if result.trend == "increasing" :
                    parameter_soge += f" The groundwater situation appears stable and annual usage is also within limits. Care should be taken that things remain the way they are."
                else:
                    parameter_soge += f" It requires close monitoring to check that the situation does not worsen."

            elif(soge_class == "Critical"):
                
                parameter_soge += f"Extraction is 90-100% of the recharge. There is a high risk of depletion of groundwater."

                if result.trend == "increasing" :
                    parameter_soge += f" Pressure to increase cropping intensity can worsen the situation. Innovative solutions of drip irrigation and strong water collectives along with canal irrigation must be considered to improve the situation."
                else:
                    parameter_soge += f" Policies for an immediate shift in cropping patterns might be required."

            else:

                parameter_soge += f"Extraction exceeds recharge; groundwater levels falling sharply."

                if result.trend == "increasing" :
                    parameter_soge += f" Pressure to increase cropping intensity can worsen the situation. Innovative solutions of drip irrigation and strong water collectives along with canal irrigation must be considered to improve the situation."
                else:
                    parameter_soge += f" Policies for an immediate shift in cropping patterns are required."

        return parameter_soge

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Soge Data",
            district,
            block
        )
        return ""


def get_drought_data(state, district, block, uid):
    try:
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
            sheet_name="croppingDrought_kharif",
        )

        # ? Drought Years
        selected_columns_mild = [col for col in df.columns if col.startswith("Mild_")]
        df[selected_columns_mild] = df[selected_columns_mild].apply(
            pd.to_numeric, errors="coerce"
        )

        selected_columns_moderate = [
            col for col in df.columns if col.startswith("Moderate_")
        ]
        df[selected_columns_moderate] = df[selected_columns_moderate].apply(
            pd.to_numeric, errors="coerce"
        )

        selected_columns_severe = [
            col for col in df.columns if col.startswith("Severe_")
        ]
        df[selected_columns_severe] = df[selected_columns_severe].apply(
            pd.to_numeric, errors="coerce"
        )

        mws_drought_mild = df.loc[df["UID"] == uid, selected_columns_mild].values[0]
        mws_drought_moderate = df.loc[
            df["UID"] == uid, selected_columns_moderate
        ].values[0]
        mws_drought_severe = df.loc[df["UID"] == uid, selected_columns_severe].values[0]

        drought_years = []
        non_drought_years = []

        drought_weeks = []

        for index, item in enumerate(mws_drought_moderate):
            drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
            drought_week = (
                mws_drought_mild[index]
                + 2 * mws_drought_moderate[index]
                + 3 * mws_drought_severe[index]
            ) / 6
            drought_weeks.append(drought_week)

            if drought_check > 5:
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    drought_years.append(match_exp.group(0))
            else:
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    non_drought_years.append(match_exp.group(0))

        parameter_drought = f""

        current_years = extract_years(selected_columns_mild)

        if current_years and len(current_years) > 0:
            year_range_text = f"{current_years[0]} to {current_years[-1]}"
        else:
            year_range_text = ""

        if len(drought_years):
            original_string = "An analysis of identified drought years — XXX, YYY and ZZZ reveals significant insights into the underlying rainfall patterns such as dry spells and deviations from normal precipitation. "
            formatted_years = format_years(drought_years)
            parameter_drought += original_string.replace("XXX, YYY and ZZZ", formatted_years)
        
        else :
            parameter_drought = f"Refer to the following graph and see how the intensity of drought has changed in this microwatershed over the years {year_range_text}"
        
        #? Get all the Dryspell for data for Graph
        selected_columns_drysp_all = [col for col in df.columns if col.startswith("drysp_unit_4_weeks")]
        df[selected_columns_drysp_all] = df[selected_columns_drysp_all].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df_drysp_all = df.loc[df["UID"] == uid, selected_columns_drysp_all].values[0].tolist()

        current_years = extract_years_single(selected_columns_drysp_all)

        if len(drought_years):
            # ? Dryspell Calc
            years = []
            drysp_tuple = []

            selected_columns_drysp = [col for col in df.columns if any(col.startswith(f"drysp_unit_4_weeks_{year}") for year in drought_years)]
            df[selected_columns_drysp] = df[selected_columns_drysp].apply(
                pd.to_numeric, errors="coerce"
            )
            filtered_df_drysp = (
                df.loc[df["UID"] == uid, selected_columns_drysp].values[0].tolist()
            )

            for index, item in enumerate(selected_columns_drysp):
                match_exp = re.search(r"\d{4}", item)
                if match_exp:
                    years.append(match_exp.group(0))

            for index, item in enumerate(years):
                if filtered_df_drysp[index] > 0:
                    temp_tuple = (filtered_df_drysp[index], item)
                    drysp_tuple.append(temp_tuple)

            sorted(drysp_tuple, key=lambda x: x[0], reverse=False)

            if len(drysp_tuple) > 0:
                parameter_drought += f"During the identified drought years, the longest dry spell recorded in"
                formatted_sentence = " "
                for index, item in enumerate(drysp_tuple):
                    if index < len(drysp_tuple) - 1:
                        formatted_sentence += f"{item[1]} lasted {item[0]} weeks, "
                    else:
                        formatted_sentence += f"and in {item[1]} lasted {item[0]} weeks."
                parameter_drought += formatted_sentence

        return parameter_drought, drought_weeks, mws_drought_moderate, mws_drought_severe, filtered_df_drysp_all, current_years

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Drought Data",
            district,
            block
        )
        return "", [], [], [], [], []


def get_village_data(state, district, block, uid):
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
        
        # Check available sheets
        excel_file = pd.ExcelFile(file_path)
        available_sheets = excel_file.sheet_names
        
        # Check if mws_intersect_villages sheet is present (mandatory)
        if "mws_intersect_villages" not in available_sheets:
            logger.info(
                "mws_intersect_villages sheet not found for %s district, %s block",
                district,
                block
            )
            return [], [], [], [], [], [], [], [], [], [], []
        
        # Load the main sheet
        df = pd.read_excel(file_path, sheet_name="mws_intersect_villages")
        
        # Check for optional sheets
        has_nrega = "nrega_assets_village" in available_sheets
        has_socio = "social_economic_indicator" in available_sheets
        
        # Load optional sheets if available
        df_village = None
        df_socio = None
        
        if has_nrega:
            df_village = pd.read_excel(file_path, sheet_name="nrega_assets_village")
        
        if has_socio:
            df_socio = pd.read_excel(file_path, sheet_name="social_economic_indicator")

        selected_columns_ids = [
            col for col in df.columns if col.startswith("Village IDs")
        ]
        matching = df.loc[df["MWS UID"] == uid, selected_columns_ids]

        if matching.empty:
            villages = []
        else:
            villages = matching.iloc[0].tolist()

        villages_name = []
        villages_sc = []
        villages_st = []
        villages_pop = []

        swc_works = []
        lr_works = []
        plantation_work = []
        iof_works = []
        ofl_works = []
        ca_works = []
        ofw_works = []

        if len(villages) > 0:
            villages = eval(villages[0])
            for id in villages:
                village_name = None
                
                # Try to get village name from NREGA sheet first
                if has_nrega and df_village is not None:
                    village_name_col = [
                        col for col in df_village.columns if col.startswith("vill_name")
                    ]
                    if len(village_name_col) > 0:
                        village_match = df_village.loc[df_village["vill_id"] == id, village_name_col]
                        if not village_match.empty:
                            name = village_match.values[0].tolist()
                            village_name = name[0] if name else None
                
                # Fallback to socio-economic sheet if name not found
                if village_name is None and has_socio and df_socio is not None:
                    village_name_col = [
                        col for col in df_socio.columns if col.startswith("village_name")
                    ]
                    if len(village_name_col) > 0:
                        village_match = df_socio.loc[df_socio["village_id"] == id, village_name_col]
                        if not village_match.empty:
                            name = village_match.values[0].tolist()
                            village_name = name[0] if name else None
                
                villages_name.append(village_name)

                # Process NREGA data if available
                if has_nrega and df_village is not None:
                    # Process all NREGA work categories
                    swc_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Soil and water conservation")
                    ]
                    if len(swc_cols) > 0:
                        df_village[swc_cols] = df_village[swc_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, swc_cols]
                        if not village_match.empty:
                            swc_works.append(sum(village_match.values[0].tolist()))
                        else:
                            swc_works.append(0)
                    else:
                        swc_works.append(0)

                    lr_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Land restoration")
                    ]
                    if len(lr_cols) > 0:
                        df_village[lr_cols] = df_village[lr_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, lr_cols]
                        if not village_match.empty:
                            lr_works.append(sum(village_match.values[0].tolist()))
                        else:
                            lr_works.append(0)
                    else:
                        lr_works.append(0)

                    plant_cols = [
                        col for col in df_village.columns if col.startswith("Plantations")
                    ]
                    if len(plant_cols) > 0:
                        df_village[plant_cols] = df_village[plant_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, plant_cols]
                        if not village_match.empty:
                            plantation_work.append(sum(village_match.values[0].tolist()))
                        else:
                            plantation_work.append(0)
                    else:
                        plantation_work.append(0)

                    iof_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Irrigation on farms")
                    ]
                    if len(iof_cols) > 0:
                        df_village[iof_cols] = df_village[iof_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, iof_cols]
                        if not village_match.empty:
                            iof_works.append(sum(village_match.values[0].tolist()))
                        else:
                            iof_works.append(0)
                    else:
                        iof_works.append(0)

                    ofl_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Off-farm livelihood assets")
                    ]
                    if len(ofl_cols) > 0:
                        df_village[ofl_cols] = df_village[ofl_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, ofl_cols]
                        if not village_match.empty:
                            ofl_works.append(sum(village_match.values[0].tolist()))
                        else:
                            ofl_works.append(0)
                    else:
                        ofl_works.append(0)

                    ca_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Community assets_count")
                    ]
                    if len(ca_cols) > 0:
                        df_village[ca_cols] = df_village[ca_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, ca_cols]
                        if not village_match.empty:
                            ca_works.append(sum(village_match.values[0].tolist()))
                        else:
                            ca_works.append(0)
                    else:
                        ca_works.append(0)

                    ofw_cols = [
                        col
                        for col in df_village.columns
                        if col.startswith("Other farm works")
                    ]
                    if len(ofw_cols) > 0:
                        df_village[ofw_cols] = df_village[ofw_cols].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_village.loc[df_village["vill_id"] == id, ofw_cols]
                        if not village_match.empty:
                            ofw_works.append(sum(village_match.values[0].tolist()))
                        else:
                            ofw_works.append(0)
                    else:
                        ofw_works.append(0)
                else:
                    # If NREGA sheet not available, append default values
                    swc_works.append(0)
                    lr_works.append(0)
                    plantation_work.append(0)
                    iof_works.append(0)
                    ofl_works.append(0)
                    ca_works.append(0)
                    ofw_works.append(0)

                # Process socio-economic data if available
                if has_socio and df_socio is not None:
                    sc_percent_col = [
                        col for col in df_socio.columns if col.startswith("SC_percent")
                    ]
                    if len(sc_percent_col) > 0:
                        df_socio[sc_percent_col] = df_socio[sc_percent_col].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_socio.loc[df_socio["village_id"] == id, sc_percent_col]
                        if not village_match.empty:
                            sc_percent = village_match.values[0].tolist()
                            villages_sc.append(round(sc_percent[0], 2))
                        else:
                            villages_sc.append(None)
                    else:
                        villages_sc.append(None)

                    st_percent_col = [
                        col for col in df_socio.columns if col.startswith("ST_percent")
                    ]
                    if len(st_percent_col) > 0:
                        df_socio[st_percent_col] = df_socio[st_percent_col].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_socio.loc[df_socio["village_id"] == id, st_percent_col]
                        if not village_match.empty:
                            st_percent = village_match.values[0].tolist()
                            villages_st.append(round(st_percent[0], 2))
                        else:
                            villages_st.append(None)
                    else:
                        villages_st.append(None)

                    pop_col = [
                        col
                        for col in df_socio.columns
                        if col.startswith("total_population")
                    ]
                    if len(pop_col) > 0:
                        df_socio[pop_col] = df_socio[pop_col].apply(
                            pd.to_numeric, errors="coerce"
                        )
                        village_match = df_socio.loc[df_socio["village_id"] == id, pop_col]
                        if not village_match.empty:
                            total_pop = village_match.values[0].tolist()
                            villages_pop.append(total_pop[0])
                        else:
                            villages_pop.append(None)
                    else:
                        villages_pop.append(None)
                else:
                    # If socio-economic sheet not available, append default values
                    villages_sc.append(None)
                    villages_st.append(None)
                    villages_pop.append(None)

        return (
            villages_name,
            villages_sc,
            villages_st,
            villages_pop,
            swc_works,
            lr_works,
            plantation_work,
            iof_works,
            ofl_works,
            ca_works,
            ofw_works,
        )

    except Exception as e:
        logger.info(
            "Error accessing excel for %s district, %s block: %s",
            district,
            block,
            str(e)
        )
        return [], [], [], [], [], [], [], [], [], [], []