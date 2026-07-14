import re
import functools
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

from nrm_app.settings import EXCEL_DIR, GEOSERVER_URL, OVERPASS_URL
from utilities.logger import setup_logger

logger = setup_logger(__name__)

DATA_DIR_TEMP = EXCEL_DIR


@functools.lru_cache(maxsize=8)
def _get_excel_file(path):
    """Cached ExcelFile handle. Every get_* function in this module used to call
    pd.read_excel()/pd.ExcelFile() independently for the same report's workbook,
    re-opening and re-parsing the whole .xlsx from disk on every call (dozens of
    times per report). This opens/parses each workbook once per path and reuses
    the in-memory handle for every sheet read after that."""
    return pd.ExcelFile(path)


def read_excel_sheet(path, sheet_name):
    """Read a sheet via the cached ExcelFile handle for `path`. Returns a fresh
    copy so callers can mutate the DataFrame freely without affecting other
    callers sharing the same cached workbook."""
    return _get_excel_file(path).parse(sheet_name).copy()


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
        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "terrain",
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
        out body geom;
        """

        response = {}
        block_detail = Overpass_Block_Details.objects.filter(location=f"{district}_{block}").first()

        if block_detail:
            logger.info(f"Using cached response for location: {district}_{block}")
            response = block_detail.overpass_response
        else:
            logger.info(f"No cached data found. Fetching from Overpass API for location: {district}_{block}")
            
            try:
                headers = {
                    'Accept': 'application/json',
                    'User-Agent': 'CoreStack-GIS/1.0'
                }
                
                response = requests.post(
                    OVERPASS_URL,
                    data={"data": overpass_query},
                    headers=headers,
                    timeout=60  # Overpass can be slow for large queries
                )
                response.raise_for_status()
                response = response.json()
                
                # if DEBUG: 
                #     with open('overpass_response.json', 'w', encoding='utf-8') as f:
                #         json.dump(response, f, indent=2, ensure_ascii=False)

                block_detail = Overpass_Block_Details.objects.create(
                    location=f"{district}_{block}",
                    overpass_response=response
                )
                logger.info(f"Response saved to DB for location: {district}_{block}")
            
            except requests.exceptions.Timeout:
                logger.error(f"Overpass API timeout for {district}_{block}")
                raise
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP {e.response.status_code} error fetching Overpass data: {e.response.text[:200]}")
                raise
            except Exception as e:
                # FIX: Don't pass exception as format argument
                logger.error(f"Failed to fetch Overpass API data: {str(e)}")
                raise

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
                # Combine, sort by area descending, cap at 5
                combined_water_bodies = large_lakes + large_reservoirs
                combined_water_bodies = sorted(combined_water_bodies, key=lambda x: x["area_sq_m"], reverse=True)[:5]

                rname = [temp["name"] for temp in combined_water_bodies]
                rarea = [str(round(temp["area_sq_m"] / 10000, 1)) for temp in combined_water_bodies]


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
                # Combine, sort by area descending, cap at 5
                combined_water_bodies_mws = large_lakes_mws + large_reservoirs_mws
                combined_water_bodies_mws = sorted(combined_water_bodies_mws, key=lambda x: x["area_sq_m"], reverse=True)[:5]

                rname = [temp["name"] for temp in combined_water_bodies_mws]
                rarea = [str(round(temp["area_sq_m"] / 10000, 1)) for temp in combined_water_bodies_mws]

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
            parameter_block = f"The Tehsil {block} having total area {total_area:,} hectares" + parameter_block + "."

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
        excel_file = _get_excel_file(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx")

        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "terrain",
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
        slope_tree_percent = 0
        slope_shrub_percent = 0
        plain_farmland_percent = 0

        if "terrain_lulc_slope" in excel_file.sheet_names:

            df_slopes = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "terrain_lulc_slope")

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

                slope_tree_percent = float(tree_percent)
                slope_shrub_percent = float(shrub_percent)

        if "terrain_lulc_plain" in excel_file.sheet_names:
            df_plain = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "terrain_lulc_plain")

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

                plain_farmland_percent = float(farmland_area_percent)

        #? Terrain cluster description
        checkdam_advice = " Cropping areas on plains may benefit from checkdams on gentle slopes and farm ponds and bunds on plain cropped areas to improve drought protection during Kharif and soil moisture for Rabi cropping."

        if filtered_df[0] == "Broad Sloppy and Hilly":
            parameter_main += f"The micro-watershed is spread across <strong>{round(mws_area,2)}</strong> hectares. The terrain of our micro-watershed consists of gently sloping land and rolling hills with <strong>{round(percent_slope,2)}</strong> % area under broad slopes and <strong>{round(percent_hill, 2)}</strong> % area under hills."

        elif filtered_df[0] == "Mostly Plains":
            parameter_main += f"The micro-watershed is spread across <strong>{round(mws_area,2)}</strong> hectares. The micro-watershed mainly consists of flat plains covering <strong>{round(percent_plain, 2)}</strong> % micro-watershed area."
            if plain_farmland_percent > 15:
                parameter_main += checkdam_advice

        elif filtered_df[0] == "Mostly Hills and Valleys":
            parameter_main += f"The micro-watershed is spread across <strong>{round(mws_area,2)}</strong> hectares. The micro-watershed terrain is mainly hills and valleys with <strong>{round(percent_hill, 2)}</strong> % under hills and <strong>{round(percent_valley, 2)}</strong> % under valleys."

        else:
            parameter_main += f"The micro-watershed is spread across <strong>{round(mws_area, 2)}</strong> hectares. The micro-watershed includes flat plains and gentle slopes with <strong>{round(percent_plain, 2)}</strong> % area as plains and <strong>{round(percent_slope, 2)}</strong> % area under broad slopes."
            if plain_farmland_percent > 15:
                parameter_main += checkdam_advice

        gully_plug_subjects = []
        if slope_tree_percent > 15:
            gully_plug_subjects.append("Forested areas in hills")
        if slope_shrub_percent > 10:
            gully_plug_subjects.append("sparsely vegetated areas on barren hills")

        if gully_plug_subjects:
            parameter_main += f" {' and '.join(gully_plug_subjects)} may benefit from gully plugs and staggered trenches to improve soil moisture for vegetation and reduce soil erosion."

        return parameter_main, mws_areas, block_areas, parameter_comp, parameter_lulc, mws_lulc_area_slope, block_lulc_area_slope, mws_lulc_area_plain, block_lulc_area_plain

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block", district, block
        )
        return "", [], [], "", "", [], [], [], []


def get_mws_barren_percent(state, district, block, uid):
    """Overall barren land percentage of the MWS, weighted from the slope and plain terrain portions."""
    try:
        base_path = DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx"

        df_terrain = read_excel_sheet(base_path, "terrain")
        row_terrain = df_terrain.loc[df_terrain["UID"] == uid]
        if row_terrain.empty:
            return 0

        percent_slope = float(row_terrain["slopy_area_percent"].values[0])
        percent_plain = float(row_terrain["plain_area_percent"].values[0])

        slope_barren_percent = 0
        try:
            df_slope = read_excel_sheet(base_path, "terrain_lulc_slope")
            row_slope = df_slope.loc[df_slope["UID"] == uid]
            if not row_slope.empty:
                slope_barren_percent = float(row_slope["barren_area_percent"].values[0])
        except Exception:
            pass

        plain_barren_percent = 0
        try:
            df_plain = read_excel_sheet(base_path, "terrain_lulc_plain")
            row_plain = df_plain.loc[df_plain["UID"] == uid]
            if not row_plain.empty:
                plain_barren_percent = float(row_plain["barren_area_percent"].values[0])
        except Exception:
            pass

        return round((percent_slope / 100 * slope_barren_percent) + (percent_plain / 100 * plain_barren_percent), 2)
    except Exception as e:
        logger.info(f"Failed to compute barren percent for {uid}: {e}")
        return 0


def get_crop_intensity_sankey_data(state, district, block, uid):
    """Cropping-intensity class transitions (single/double/triple) for the MWS, as sankey source-target-value links."""
    sankey_data = []
    try:
        df_cc = read_excel_sheet(
            DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            "change_detection_cropintensity",
        )
        row_cc = df_cc.loc[df_cc["UID"] == uid]
        if not row_cc.empty:
            # Only degradation flows: Triple/Double (left) -> Double/Single (right)
            transitions = [
                ("double_to_single_area_in_ha", "Double", "Single"),
                ("double_to_double_area_in_ha", "Double", "Double"),
                ("triple_to_single_area_in_ha", "Triple", "Single"),
                ("triple_to_double_area_in_ha", "Triple", "Double"),
            ]
            for col, source, target in transitions:
                if col not in row_cc.columns:
                    continue
                value = row_cc[col].values[0]
                value = float(value) if not pd.isna(value) else 0
                if value > 0:
                    sankey_data.append({
                        "source": f"{source} Cropping (Before)",
                        "target": f"{target} Cropping (After)",
                        "value": round(value, 2),
                    })
    except Exception as e:
        logger.info(f"Failed to read change_detection_cropintensity sheet for {uid}: {e}")

    return sankey_data


def get_tree_reduction_sankey_data(state, district, block, uid):
    """Forest cover transitions (to barren/built-up/farm/forest/scrub) for the MWS, as sankey source-target-value links."""
    sankey_data = []
    try:
        df_defo = read_excel_sheet(
            DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            "change_detection_deforestation",
        )
        row_defo = df_defo.loc[df_defo["UID"] == uid]
        if not row_defo.empty:
            transitions = [
                ("forest_to_forest_area_in_ha", "Forest"),
                ("forest_to_barren_area_in_ha", "Barren"),
                ("forest_to_built_up_area_in_ha", "Built Up"),
                ("forest_to_farm_area_in_ha", "Farm"),
                ("forest_to_scrub_land_area_in_ha", "Scrub Land"),
            ]
            for col, target in transitions:
                if col not in row_defo.columns:
                    continue
                value = row_defo[col].values[0]
                value = float(value) if not pd.isna(value) else 0
                if value > 0:
                    sankey_data.append({
                        "source": "Forest (Before)",
                        "target": f"{target} (After)",
                        "value": round(value, 2),
                    })
    except Exception as e:
        logger.info(f"Failed to read change_detection_deforestation sheet for {uid}: {e}")

    return sankey_data


def get_urbanization_sankey_data(state, district, block, uid):
    """Land cover transitions into built-up area for the MWS, as sankey source-target-value links."""
    sankey_data = []
    try:
        df_urban = read_excel_sheet(
            DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            "change_detection_urbanization",
        )
        row_urban = df_urban.loc[df_urban["UID"] == uid]
        if not row_urban.empty:
            transitions = [
                ("built_up_to_built_up_area_in_ha", "Built Up"),
                ("tree_farm_to_built_up_area_in_ha", "Tree/Farm"),
                ("barren_shrub_to_built_up_area_in_ha", "Barren/Shrub"),
                ("water_to_built_up_area_in_ha", "Water"),
            ]
            for col, source in transitions:
                if col not in row_urban.columns:
                    continue
                value = row_urban[col].values[0]
                value = float(value) if not pd.isna(value) else 0
                if value > 0:
                    sankey_data.append({
                        "source": f"{source} (Before)",
                        "target": "Built Up (After)",
                        "value": round(value, 2),
                    })
    except Exception as e:
        logger.info(f"Failed to read change_detection_urbanization sheet for {uid}: {e}")

    return sankey_data


def get_change_detection_data(state, district, block, uid):
    try:
        df_degrad = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "change_detection_degradation",
        )
        df_defo = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "change_detection_deforestation",
        )
        df_urban = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "change_detection_urbanization",
        )
        df_restore = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "restoration_vector",
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
            barren_percent = get_mws_barren_percent(state, district, block, uid)
            parameter_land += f"{barren_percent}% of the area is barren in this microwatershed. There has been a considerate level of degradation of farmlands in this micro watershed over the years 2017-2022. As compared to average degraded land area of {round(avg, 2)} hectares for the entire block, the degraded land area in this micro-watershed is close to {round(degradation, 2)} hectares."

        # ? Tree Reduction
        df_defo["total_deforestation_area_in_ha"] = df_defo["total_deforestation_area_in_ha"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_defo.loc[df_defo["UID"] == uid, "total_deforestation_area_in_ha"]
        reduction = filtered_df.iloc[0]
        avg = df_defo["total_deforestation_area_in_ha"].mean()

        if reduction >= 50:
            parameter_tree += f"There has been a considerate level of reduction in tree cover in this micro watershed over the years 2017-2022, about {round(reduction, 1)} hectares, as compared to {round(avg, 1)} hectares per micro watershed in the entire block."

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

        crop_intensity_sankey = get_crop_intensity_sankey_data(state, district, block, uid)
        tree_reduction_sankey = get_tree_reduction_sankey_data(state, district, block, uid)
        urbanization_sankey = get_urbanization_sankey_data(state, district, block, uid)

        return parameter_land, parameter_tree, parameter_urban, parameter_restore, crop_intensity_sankey, tree_reduction_sankey, urbanization_sankey

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for degradation",
            district,
            block,
        )
        return "", "", "", "", [], [], []


def get_land_conflict_industrial_data(state, district, block, uid):
    try:
        df = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "lcw_conflict")

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
        )
        return []


def get_factory_data(state, district, block, uid):
    try:
        df = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "factory_csr")

        # Filter by UID
        filtered_df = df[df["UID"] == uid]
        
        names = filtered_df["Company_Name"].tolist()
        addresses = filtered_df["ADDRESS"].tolist()
        types = filtered_df["LOCATION T"].tolist()

        def clean_address(address):
            if pd.isna(address):
                return ""
            
            address = str(address)
            
            # Remove everything after "Fax :", "Email :", or "Internet :"
            address = re.sub(r'\s*(?:Fax|Email|Internet)\s*:.*$', '', address, flags=re.IGNORECASE)
            
            return address.strip()

        factories = [
            {"name": name, "address": clean_address(address), "type": type_val} 
            for name, address, type_val in zip(names, addresses, types)
        ]

        return factories

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Factory Data",
            district,
            block,
        )
        return []


def get_mining_data(state, district, block, uid):
    try:
        df = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "mining")

        # Filter by UID first
        filtered_df = df[df["UID"] == uid]
        
        # Remove rows where division is "unknown"
        filtered_df = filtered_df[filtered_df["division"].str.lower() != "unknown"]
        
        # Remove duplicate entries based on "division" column
        filtered_df = filtered_df.drop_duplicates(subset=["division"])
        
        # Extract the data
        names = filtered_df["division"].tolist()
        sectors = filtered_df["sector_moefcc"].tolist()
        villages = filtered_df["village"].tolist()

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
        )
        return []


def get_green_credit_data(state, district, block, uid):
    try:
        df = read_excel_sheet(DATA_DIR_TEMP+ state.upper()+ "/"+ district.upper()+ "/"+ district.lower()+ "_"+ block.lower()+ ".xlsx", "green_credit")

        # Filter by UID
        filtered_df = df[df["UID"] == uid]

        division = filtered_df["division"].tolist()
        land_info = filtered_df["land_info"].tolist()

        green_credits = []

        for div, info in zip(division, land_info):
            if pd.isna(info) or pd.isna(div):
                continue
            
            # Split the land_info by "|"
            parts = [part.strip() for part in str(info).split("|")]
            
            if len(parts) >= 4:
                green_credits.append({
                    "division": div,
                    "registration_no": parts[0],
                    "total_area": parts[1],
                    "selected_area": parts[2],
                    "available_area": parts[3]
                })
        
        return green_credits

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Green Credit Data",
            district,
            block,
        )
        return []


def get_cropping_intensity(state, district, block, uid):
    try:
        df = read_excel_sheet(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", "croppingIntensity_annual")
        df_drought = read_excel_sheet( DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", "croppingDrought_kharif")

        selected_columns_inten = [col for col in df.columns if col.startswith("cropping_intensity_")]

        current_years = extract_years(selected_columns_inten)

        df[selected_columns_inten] = df[selected_columns_inten].apply(pd.to_numeric, errors="coerce")

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
            num_years = len(current_years) if current_years else len(filtered_df_inten.values[0])
            is_increasing = result.trend == "increasing"

            if is_increasing:
                inten_parameter_1 += (
                    f"The cropping intensity of the micro-watershed has increased over the last {num_years} years "
                    f"from {round(min(filtered_df_inten.values[0]), 2)} to {round(max(filtered_df_inten.values[0]), 2)}."
                )
            else:
                if result.trend == "decreasing":
                    inten_parameter_1 += (
                        f"The cropping intensity of this area has reduced over time "
                        f"from {round(max(filtered_df_inten.values[0]), 2)} to {round(min(filtered_df_inten.values[0]), 2)}."
                    )
                else:
                    inten_parameter_1 += (
                        f"The cropping intensity of this area has stayed steady at {round(avg_inten, 2)}."
                    )
                if avg_inten < 1.5:
                    inten_parameter_1 += " It might be possible to improve cropping intensity through more strategic placement, while keeping equity in mind, of rainwater harvesting or groundwater recharge structures."
            
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
                if drought_check >= 5:
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
                drought_diff = round(abs(drought_inten - non_drought_inten), 2)
                if is_increasing:
                    inten_parameter_2 += f"Cropping intensity is reduced by {drought_diff} during the drought years ({formatted_years}), as compared to non-drought years, and reveals a marked sensitivity of agricultural productivity to water scarcity. This decline underscores the critical need for farmers to adopt drought-resilient practices, such as constructing water harvesting structures. By capturing and storing rainwater, these structures can provide a crucial buffer against drought periods, helping to stabilize cropping intensity and sustain productivity even in water-stressed conditions."
                else:
                    inten_parameter_2 += f"The observed {drought_diff} reduction in the cropping intensity during drought years ({formatted_years}), compared to non-drought years, reveals a marked sensitivity of agricultural productivity to water scarcity. This decline underscores the critical need for farmers to adopt drought-resilient practices, such as constructing water harvesting structures. By capturing and storing rainwater, these structures can provide a crucial buffer against drought periods, helping to stabilize cropping intensity and sustain productivity even in water-stressed conditions."

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


def get_cropping_year_range(state, district, block, uid):
    try:
        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "croppingIntensity_annual",
        )

        selected_columns_single = [
            col for col in df.columns if col.startswith("single_cropped_area_")
        ]

        current_years = extract_years(selected_columns_single)

        if current_years and len(current_years) > 0:
            return f"{current_years[0]} to {current_years[-1]}"
        return ""

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for double cropping section",
            district,
            block
        )
        return ""


def get_mws_area_from_cropping_sheet(state, district, block, uid):
    """MWS area in hectares, read from the croppingIntensity_annual sheet."""
    try:
        df = read_excel_sheet(
            DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx",
            "croppingIntensity_annual",
        )
        row = df.loc[df["UID"] == uid]
        if not row.empty and "area_in_ha" in row.columns:
            return float(row["area_in_ha"].values[0])
    except Exception as e:
        logger.info(f"Failed to read croppingIntensity_annual sheet for {uid}: {e}")
    return 0


def get_waterbody_stats(state, district, block, uid):
    """Per-waterbody stats (count, count > 5 ha, % with declining trend) fetched from the geoserver swb layer."""
    total_count = 0
    large_count = 0
    declining_percent = 0
    try:
        url = (
            f"https://geoserver.core-stack.org:8443/geoserver/swb/ows?service=WFS&version=1.0.0"
            f"&request=GetFeature&typeName=swb:surface_waterbodies_{district.lower()}_{block.lower()}"
            f"&outputFormat=application/json&CQL_FILTER=MWS_UID='{uid}'"
        )
        res = requests.get(url, verify=False, timeout=30)
        if res.status_code == 200:
            features = res.json().get("features", [])
            total_count = len(features)

            if features:
                year_cols = sorted(
                    [k for k in features[0]["properties"].keys() if re.match(r"^area_\d{2}-\d{2}$", k)]
                )
                declining_count = 0
                for feature in features:
                    props = feature["properties"]
                    area_ored = props.get("area_ored") or 0
                    if area_ored > 5:
                        large_count += 1

                    series = [props.get(col) or 0 for col in year_cols]
                    if len(series) >= 4:
                        try:
                            wb_trend = mk.original_test(series)
                            if wb_trend.trend == "decreasing":
                                declining_count += 1
                        except Exception:
                            pass

                if total_count:
                    declining_percent = round((declining_count / total_count) * 100, 2)
    except Exception as e:
        logger.info(f"Failed to fetch waterbody stats for {uid}: {e}")

    return total_count, large_count, declining_percent


def get_surface_Water_bodies_data(state, district, block, uid):
    try:
        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "surfaceWaterBodies_annual",
        )
        df_drought = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "croppingDrought_kharif",
        )

        base_path = DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx"

        # ? Waterbody presence check
        df_intersect = read_excel_sheet(base_path, "mws_intersect_swb")
        has_waterbody = not df_intersect.loc[df_intersect["UID"] == uid].empty

        selected_columns = [col for col in df.columns if col.startswith("total_area_")]
        df[selected_columns] = df[selected_columns].apply(
            pd.to_numeric, errors="coerce"
        )

        current_years = extract_years(selected_columns)

        if current_years and len(current_years) > 0:
            year_range_text = f"{current_years[0]} to {current_years[-1]}"
        else:
            year_range_text = ""

        parameter_intro = f""
        parameter_swb_1 = f""
        parameter_swb_2 = f""
        parameter_swb_3 = f""
        parameter_season_avg = f""
        filtered_df_kharif = []
        filtered_df_rabi = []
        filtered_df_zaid = []

        filtered_df = df.loc[df["UID"] == uid, selected_columns]

        if not has_waterbody or filtered_df.empty:
            parameter_swb_1 = (
                f"No surface water bodies were detected through remote sensing in this micro-watershed."
            )
            return (
                "",
                parameter_swb_1,
                "",
                "",
                "",
                filtered_df_kharif,
                filtered_df_rabi,
                filtered_df_zaid,
                current_years,
            )

        if not filtered_df.empty:

            # ? Waterbody area / % of MWS / river & canal counts (intro paragraph)
            mws_area = get_mws_area_from_cropping_sheet(state, district, block, uid)
            latest_total_col = selected_columns[-1] if selected_columns else None
            total_wb_area = float(df.loc[df["UID"] == uid, latest_total_col].values[0]) if latest_total_col else 0
            wb_area_percent = round((total_wb_area / mws_area) * 100, 2) if mws_area else 0

            wb_total_count, wb_large_count, wb_declining_percent = get_waterbody_stats(state, district, block, uid)

            try:
                df_river = read_excel_sheet(base_path, "river")
                river_count = len(df_river.loc[df_river["UID"] == uid])
            except Exception:
                river_count = 0

            try:
                df_canal = read_excel_sheet(base_path, "canal")
                canal_count = len(df_canal.loc[df_canal["UID"] == uid])
            except Exception:
                canal_count = 0

            try:
                df_dd = read_excel_sheet(base_path, "drainage_density")
                row_dd = df_dd.loc[df_dd["UID"] == uid]
                drainage_density = round(float(row_dd["drainage_density_std_in_km_per_km2"].values[0]), 2) if not row_dd.empty else "-"
            except Exception:
                drainage_density = "-"

            parameter_intro = (
                f"The waterbodies in this microwatershed span a total area of {round(total_wb_area, 2)} hectares, "
                f"covering about {wb_area_percent}% of the microwatershed. "
            )
            if wb_large_count > 0:
                parameter_intro += f"Of the {wb_total_count} number of total waterbodies, {wb_large_count} are more than 5 hectares. "
            parameter_intro += (
                f"{wb_declining_percent}% of water bodies have a declining trend of surface water availability "
                f"over the years {year_range_text}. The drainage density in this micro-watershed is {drainage_density}. "
            )
            if river_count > 0:
                parameter_intro += f"{river_count} river{'s' if river_count != 1 else ''} are present"
                parameter_intro += f" and {canal_count} canal{'s' if canal_count != 1 else ''} are present." if canal_count > 0 else "."
            elif canal_count > 0:
                parameter_intro += f"{canal_count} canal{'s' if canal_count != 1 else ''} are present."

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
                parameter_swb_1 = f"In general, surface water presence has increased by {round(result.slope, 2)} hectares per year during {year_range_text}."
            elif result.trend == "decreasing":
                parameter_swb_1 = f"In general, surface water presence has decreased by {round(result.slope, 2)} hectares per year during {year_range_text}. Siltation could be a cause for decrease in surface water presence and therefore may require repair and maintenance of surface water bodies. Waterbody analysis can help identify waterbodies that may need such treatment."
            else:
                parameter_swb_1 = f"The surface water presence has remained steady during {year_range_text}."

            #? Drought Years SWB
            mws_drought_moderate = df_drought.loc[df_drought["UID"] == uid, selected_columns_moderate].values[0]
            mws_drought_severe = df_drought.loc[df_drought["UID"] == uid, selected_columns_severe].values[0]

            drought_years = []
            non_drought_year = []

            for index, item in enumerate(mws_drought_moderate):
                drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    if drought_check >= 5:
                        drought_years.append(match_exp.group(0))
                    else:
                        non_drought_year.append(match_exp.group(0))
            
            if len(drought_years):

                total_area_d = 0
                total_area_nd = 0

                for year in drought_years:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    if selected_column_temp:
                        yearly_area = df.loc[df["UID"] == uid, selected_column_temp].values
                        if len(yearly_area) > 0 and len(yearly_area[0]) > 0:
                            total_area_d += yearly_area[0][0]


                for year in non_drought_year:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    if selected_column_temp:
                        yearly_area = df.loc[df["UID"] == uid, selected_column_temp].values
                        if len(yearly_area) > 0 and len(yearly_area[0]) > 0:
                            total_area_nd += yearly_area[0][0]

                # Compare per-year averages, not raw sums, so unequal drought/non-drought
                # year counts don't distort the percentage.
                avg_area_d = total_area_d / len(drought_years) if len(drought_years) else 0
                avg_area_nd = total_area_nd / len(non_drought_year) if len(non_drought_year) else 0

                if avg_area_nd > 0:
                    percent_nd_t_d = ((avg_area_nd - avg_area_d) / avg_area_nd) * 100

                    if result.trend == "increasing":
                        parameter_swb_2 = f"During the monsoon, on average we observe that the area under surface water during drought years ({' and '.join(map(str, drought_years))}) is {round(percent_nd_t_d, 2)}% less than during non-drought years. This decline highlights a significant impact of drought on surface water availability during the primary crop-growing season, and indicates sensitivity of the cropping to droughts."

                    else:
                        parameter_swb_2 = f"During the monsoon, we observed a {round(percent_nd_t_d, 2)}% decrease in surface water area during drought years ({' and '.join(map(str, drought_years))}), as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."


            #? Non-Drought Years SWB
            # Defaults in case one of the two groups (drought/non-drought years) is empty,
            # so the comparison below never references an undefined value.
            percent_rb_kh_non_drought = None
            hectare_drop_non_drought = None

            if len(non_drought_year):
                area_under_rb_nd = 0
                area_under_kh_nd = 0
                percent_rb_kh = 0

                for year in non_drought_year:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    selected_column_temp_rb = [col for col in df.columns if col.startswith("rabi_area_in_ha_" + year)]

                    if selected_column_temp:
                        yearly_area_kh = df.loc[df["UID"] == uid, selected_column_temp].values
                        if len(yearly_area_kh) > 0 and len(yearly_area_kh[0]) > 0:
                            area_under_kh_nd += yearly_area_kh[0][0]

                    if selected_column_temp_rb:
                        yearly_area_rb = df.loc[df["UID"] == uid, selected_column_temp_rb].values
                        if len(yearly_area_rb) > 0 and len(yearly_area_rb[0]) > 0:
                            area_under_rb_nd += yearly_area_rb[0][0]

                # Handle division by zero for non-drought years
                if area_under_kh_nd > 0:
                    percent_rb_kh = ((area_under_kh_nd - area_under_rb_nd) / area_under_kh_nd) * 100
                    percent_rb_kh_non_drought = percent_rb_kh
                    hectare_drop_non_drought = area_under_kh_nd - area_under_rb_nd

                    if result.trend == "increasing":
                        parameter_swb_3 += f"In non-drought years, surface water typically decreases by {round(percent_rb_kh, 2)}% from the Kharif to the Rabi season."
                    elif result.trend == "decreasing":
                        parameter_swb_3 += f"In non-drought years, surface water in kharif typically decreases by {round(percent_rb_kh, 2)}% in rabi."
                    else:
                        parameter_swb_3 += f"In non-drought years, surface water in kharif typically decreases by {round(percent_rb_kh, 2)}% in rabi."

            if len(drought_years):
                area_under_rb = 0
                area_under_kh = 0
                percent_rb_kh = 0

                for year in drought_years:
                    selected_column_temp = [col for col in df.columns if col.startswith("kharif_area_in_ha_" + year)]
                    selected_column_temp_rb = [col for col in df.columns if col.startswith("rabi_area_in_ha_" + year)]

                    if selected_column_temp:
                        yearly_area_kh = df.loc[df["UID"] == uid, selected_column_temp].values
                        if len(yearly_area_kh) > 0 and len(yearly_area_kh[0]) > 0:
                            area_under_kh += yearly_area_kh[0][0]

                    if selected_column_temp_rb:
                        yearly_area_rb = df.loc[df["UID"] == uid, selected_column_temp_rb].values
                        if len(yearly_area_rb) > 0 and len(yearly_area_rb[0]) > 0:
                            area_under_rb += yearly_area_rb[0][0]

                # Handle division by zero for drought years
                if area_under_kh > 0:
                    percent_rb_kh = ((area_under_kh - area_under_rb) / area_under_kh) * 100
                    hectare_drop_drought = area_under_kh - area_under_rb

                    if result.trend == "increasing":
                        # Increasing-trend wording compares raw hectares dropped (both quantities
                        # are already in hectares), rather than percentages.
                        if hectare_drop_non_drought is None or hectare_drop_drought > hectare_drop_non_drought:
                            comparison = "significantly higher, and reaches"
                        elif hectare_drop_drought < hectare_drop_non_drought:
                            comparison = "actually lower, at just"
                        else:
                            comparison = "about the same, at"
                        parameter_swb_3 += f" However, during drought years, this reduction is {comparison} {round(hectare_drop_drought, 2)} hectares from Kharif to Rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."
                    else:
                        if percent_rb_kh_non_drought is None or percent_rb_kh > percent_rb_kh_non_drought:
                            comparison = "significantly higher, reaching"
                        elif percent_rb_kh < percent_rb_kh_non_drought:
                            comparison = "actually lower, at just"
                        else:
                            comparison = "about the same, at"
                        parameter_swb_3 += f" However, during drought years, this seasonal reduction is {comparison} {round(percent_rb_kh, 2)}% from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

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

            #? Average Rabi / Zaid surface water availability as % of MWS area
            if mws_area and filtered_df_rabi and filtered_df_zaid:
                avg_rabi_percent = round((sum(filtered_df_rabi) / len(filtered_df_rabi) / mws_area) * 100, 2)
                avg_zaid_percent = round((sum(filtered_df_zaid) / len(filtered_df_zaid) / mws_area) * 100, 2)
                parameter_season_avg = (
                    f"The average surface water availability in this micro watershed during the Rabi season is "
                    f"{avg_rabi_percent}%, while during the Zaid season, it is {avg_zaid_percent}%."
                )

        return (
            parameter_intro,
            parameter_swb_1,
            parameter_swb_2,
            parameter_swb_3,
            parameter_season_avg,
            filtered_df_kharif,
            filtered_df_rabi,
            filtered_df_zaid,
            current_years
        )

    except Exception as e:
        print(e)
        logger.info("Not able to access excel for %s state, %s district, %s block for Waterbodies",state.upper(),district.upper(),block.upper())
        return "", "-", "", "", "", [], [], [], []


def get_fortnightly_water_balance_data(state, district, block, uid):
    """Fortnightly Precipitation/ET/RunOff series for the MWS, fetched live from the geoserver deltaG_fortnight layer."""
    labels = []
    precip_data = []
    et_data = []
    runoff_data = []
    try:
        url = (
            f"https://geoserver.core-stack.org:8443/geoserver/mws_layers/ows?service=WFS&version=1.0.0"
            f"&request=GetFeature&typeName=mws_layers:deltaG_fortnight_{district.lower()}_{block.lower()}"
            f"&outputFormat=application/json&CQL_FILTER=uid='{uid}'"
        )
        res = requests.get(url, verify=False, timeout=60)
        if res.status_code == 200:
            features = res.json().get("features", [])
            if features:
                props = features[0]["properties"]
                date_keys = sorted([k for k in props.keys() if re.match(r"^\d{4}-\d{2}-\d{2}$", k)])

                for date_key in date_keys:
                    try:
                        raw_val = props[date_key]
                        val = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                        labels.append(date_key)
                        precip_data.append(round(float(val.get("Precipitation") or 0), 2))
                        et_data.append(round(float(val.get("ET") or 0), 2))
                        runoff_data.append(round(float(val.get("RunOff") or 0), 2))
                    except (ValueError, TypeError, json.JSONDecodeError):
                        continue
    except Exception as e:
        logger.info(f"Failed to fetch fortnightly water balance data for {uid}: {e}")

    return labels, precip_data, et_data, runoff_data


def get_water_balance_data(state, district, block, uid):
    try:
        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "hydrological_annual",
        )
        df_drought = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "croppingDrought_kharif",
        )

        df_seasonal = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "hydrological_seasonal",
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
        
        short_trend = result.trend.capitalize()

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
            if drought_check >= 5:
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
                if selected_column_precp:
                    rainfall_data = df.loc[df["UID"] == uid, selected_column_precp].values
                    if len(rainfall_data) > 0 and len(rainfall_data[0]) > 0:
                        rainfall = rainfall_data[0][0]
                        avg_rainfall += rainfall
                    else:
                        continue  # Skip this year if no rainfall data
                else:
                    continue

                #? Monsoon Onset
                selected_column_onset = [col for col in df_drought.columns if col.startswith("monsoon_onset_" + year)]
                if selected_column_onset:
                    onset_data = df_drought.loc[df_drought["UID"] == uid, selected_column_onset].values
                    if len(onset_data) > 0 and len(onset_data[0]) > 0:
                        onset = onset_data[0][0]
                        monsoon_onset.append(onset)

                #? Fortnight Delg Calc
                selected_column_kh = [col for col in df_seasonal.columns if col.startswith("delta g_kharif_in_mm_" + year)]
                selected_column_rb = [col for col in df_seasonal.columns if col.startswith("delta g_rabi_in_mm_" + year)]
                selected_column_zd = [col for col in df_seasonal.columns if col.startswith("delta g_zaid_in_mm_" + year)]

                delg_kh_val = 0
                delg_rb_val = 0
                delg_zd_val = 0

                if selected_column_kh:
                    delg_kh_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_kh].values
                    if len(delg_kh_data) > 0 and len(delg_kh_data[0]) > 0:
                        delg_kh_val = delg_kh_data[0][0]

                if selected_column_rb:
                    delg_rb_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_rb].values
                    if len(delg_rb_data) > 0 and len(delg_rb_data[0]) > 0:
                        delg_rb_val = delg_rb_data[0][0]

                if selected_column_zd:
                    delg_zd_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_zd].values
                    if len(delg_zd_data) > 0 and len(delg_zd_data[0]) > 0:
                        delg_zd_val = delg_zd_data[0][0]

                avg_fortnight_delg += (delg_kh_val + delg_rb_val + delg_zd_val)

                #? Runoff
                selected_column_runoff = [col for col in df.columns if col.startswith("RunOff_in_mm_" + year)]
                if selected_column_runoff:
                    runoff_data = df.loc[df["UID"] == uid, selected_column_runoff].values
                    if len(runoff_data) > 0 and len(runoff_data[0]) > 0:
                        runoff = runoff_data[0][0]
                        if rainfall > 0:  # Avoid division by zero
                            runoff_percent += ((runoff / rainfall) * 100)
            
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
                rainfall = None
                if selected_column_precp:
                    rainfall_data = df.loc[df["UID"] == uid, selected_column_precp].values
                    if len(rainfall_data) > 0 and len(rainfall_data[0]) > 0:
                        rainfall = rainfall_data[0][0]
                        avg_rainfall += rainfall
                    else:
                        continue  # Skip this year if no rainfall data
                else:
                    continue

                #? Fortnight Delg Calc
                selected_column_kh = [col for col in df_seasonal.columns if col.startswith("delta g_kharif_in_mm_" + year)]
                selected_column_rb = [col for col in df_seasonal.columns if col.startswith("delta g_rabi_in_mm_" + year)]
                selected_column_zd = [col for col in df_seasonal.columns if col.startswith("delta g_zaid_in_mm_" + year)]

                delg_kh_val = 0
                delg_rb_val = 0
                delg_zd_val = 0

                if selected_column_kh:
                    delg_kh_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_kh].values
                    if len(delg_kh_data) > 0 and len(delg_kh_data[0]) > 0:
                        delg_kh_val = delg_kh_data[0][0]

                if selected_column_rb:
                    delg_rb_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_rb].values
                    if len(delg_rb_data) > 0 and len(delg_rb_data[0]) > 0:
                        delg_rb_val = delg_rb_data[0][0]

                if selected_column_zd:
                    delg_zd_data = df_seasonal.loc[df_seasonal["UID"] == uid, selected_column_zd].values
                    if len(delg_zd_data) > 0 and len(delg_zd_data[0]) > 0:
                        delg_zd_val = delg_zd_data[0][0]

                avg_fortnight_delg += (delg_kh_val + delg_rb_val + delg_zd_val)

                #? Runoff
                selected_column_runoff = [col for col in df.columns if col.startswith("RunOff_in_mm_" + year)]
                if selected_column_runoff and rainfall is not None:
                    runoff_data = df.loc[df["UID"] == uid, selected_column_runoff].values
                    if len(runoff_data) > 0 and len(runoff_data[0]) > 0:
                        runoff = runoff_data[0][0]
                        if rainfall > 0:  # Avoid division by zero
                            runoff_percent += ((runoff / rainfall) * 100)

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
            short_trend,
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
        return "-", "", "", "", [], [], [], [], []


def get_hydro_tabular_data(state, district, block, uid):
    try:
        base_path = f"{DATA_DIR_TEMP}{state.upper()}/{district.upper()}/{district.lower()}_{block.lower()}.xlsx"
        
        # Read Area from mws sheet
        try:
            df_mws = read_excel_sheet(base_path, "mws")
            row_mws = df_mws.loc[df_mws["UID"] == uid]
            if not row_mws.empty and "area_in_ha" in row_mws.columns:
                import math
                import requests
                from shapely.geometry import shape
                import pyproj
                from shapely.ops import transform
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                
                area_val = row_mws["area_in_ha"].values[0]
                area = round(float(area_val), 2) if not pd.isna(area_val) else "-"
                if area != "-":
                    perimeter_circle = 0.2 * math.sqrt(math.pi * area)
                    
                    # Fetch true MWS perimeter from Geoserver using DEM layer
                    try:
                        url = f"https://geoserver.core-stack.org:8443/geoserver/dem/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=dem:{district.lower()}_{block.lower()}_dem_vector&cql_filter=uid='{uid}'&outputFormat=application/json"
                        res = requests.get(url, verify=False)
                        if res.status_code == 200:
                            data = res.json()
                            if len(data.get("features", [])) > 0:
                                geom = shape(data["features"][0]["geometry"])
                                project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform
                                geom_proj = transform(project, geom)
                                perimeter_mws = geom_proj.length / 1000
                                perimeter = round(perimeter_mws, 2)
                                compactness = round(perimeter_mws / perimeter_circle, 2)
                            else:
                                perimeter = "-"
                                compactness = "-"
                        else:
                            perimeter = "-"
                            compactness = "-"
                    except Exception as e:
                        logger.info(f"Failed to calculate MWS perimeter for {uid}: {e}")
                        perimeter = "-"
                        compactness = "-"
                else:
                    perimeter = "-"
                    compactness = "-"
            else:
                area, perimeter, compactness = "-", "-", "-"
        except Exception as e:
            logger.info(f"Failed to read mws sheet for {uid}: {e}")
            area, perimeter, compactness = "-", "-", "-"

        # Read DEM
        try:
            df_dem = read_excel_sheet(base_path, "dem")
            row_dem = df_dem.loc[df_dem["UID"] == uid]
            if not row_dem.empty:
                min_elev = round(float(row_dem["min_elevation_in_m"].values[0]), 2)
                max_elev = round(float(row_dem["max_elevation_in_m"].values[0]), 2)
                mean_elev = round(float(row_dem["mean_elevation_in_m"].values[0]), 2) if "mean_elevation_in_m" in row_dem.columns else "-"
                relief = round(max_elev - min_elev, 2) if min_elev != "-" and max_elev != "-" else "-"
            else:
                min_elev, max_elev, mean_elev, relief = "-", "-", "-", "-"
        except Exception as e:
            logger.info(f"Failed to read dem sheet for {uid}: {e}")
            min_elev, max_elev, mean_elev, relief = "-", "-", "-", "-"

        # Read Aquifer
        try:
            df_aq = read_excel_sheet(base_path, "aquifer_vector")
            row_aq = df_aq.loc[df_aq["UID"] == uid]
            aquifer_class = row_aq["aquifer_class"].values[0] if not row_aq.empty else "-"
        except Exception as e:
            logger.info(f"Failed to read aquifer_vector sheet for {uid}: {e}")
            aquifer_class = "-"

        # Read SOGE
        try:
            df_soge = read_excel_sheet(base_path, "soge_vector")
            row_soge = df_soge.loc[df_soge["UID"] == uid]
            soge_class = row_soge["class_name"].values[0] if not row_soge.empty else "-"
        except Exception as e:
            logger.info(f"Failed to read soge_vector sheet for {uid}: {e}")
            soge_class = "-"

        # Read Drainage Density
        try:
            df_dd = read_excel_sheet(base_path, "drainage_density")
            row_dd = df_dd.loc[df_dd["UID"] == uid]
            if not row_dd.empty:
                if "drainage_density_std_in_km_per_km2" in df_dd.columns:
                    drainage_density = row_dd["drainage_density_std_in_km_per_km2"].values[0]
                else:
                    drainage_density = "-"
                
                # Check for stream_order_length_in_km
                if "stream_order_length_in_km" in df_dd.columns:
                    total_length = row_dd["stream_order_length_in_km"].values[0]
                else:
                    total_length = "-"
            else:
                drainage_density, total_length = "-", "-"
        except Exception as e:
            logger.info(f"Failed to read drainage_density sheet for {uid}: {e}")
            drainage_density, total_length = "-", "-"
        print(f"drainage density is {drainage_density} and total length {total_length}")

        return min_elev, max_elev, mean_elev, relief, aquifer_class, soge_class, drainage_density, total_length, area, perimeter, compactness

    except Exception as e:
        logger.info(f"Error in get_hydro_tabular_data for {uid}: {e}")
        return "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"


def get_terrain_and_lulc_data(state, district, block, uid):
    try:
        base_path = f"{DATA_DIR_TEMP}{state.upper()}/{district.upper()}/{district.lower()}_{block.lower()}.xlsx"
        
        # Terrain Description
        terrain_desc = "-"
        try:
            df_terrain = read_excel_sheet(base_path, "terrain")
            row_t = df_terrain.loc[df_terrain["UID"] == uid]
            if not row_t.empty:
                terrain_desc = row_t["terrain_description"].values[0]
        except Exception as e:
            logger.info(f"Failed to read terrain sheet for {uid}: {e}")

        # LULC sums
        trees = 0.0
        crops = 0.0
        shrubs = 0.0
        barren = 0.0
        built_up = 0.0
        
        def get_vals(df, u):
            if df.empty: return 0.0, 0.0, 0.0, 0.0, 0.0
            row = df.loc[df["UID"] == u]
            if row.empty: return 0.0, 0.0, 0.0, 0.0, 0.0
            
            def val(col):
                if col in row.columns:
                    v = row[col].values[0]
                    return float(v) if not pd.isna(v) else 0.0
                return 0.0
            
            t = val("forests_area_percent")
            c = val("single_kharif_area_percent") + val("single_non_kharif_area_percent") + val("double_cropping_area_percent") + val("triple_cropping_area_percent")
            s = val("shrub_scrubs_area_percent")
            b = val("barren_area_percent")
            bu = val("built_up_area_percent")
            return t, c, s, b, bu

        ts_val, cs_val, ss_val, bs_val, bus_val = 0.0, 0.0, 0.0, 0.0, 0.0
        tp_val, cp_val, sp_val, bp_val, bup_val = 0.0, 0.0, 0.0, 0.0, 0.0

        try:
            df_slope = read_excel_sheet(base_path, "terrain_lulc_slope")
            ts_val, cs_val, ss_val, bs_val, bus_val = get_vals(df_slope, uid)
            trees += ts_val
            crops += cs_val
            shrubs += ss_val
            barren += bs_val
            built_up += bus_val
        except Exception as e:
            pass
            
        try:
            df_plain = read_excel_sheet(base_path, "terrain_lulc_plain")
            tp_val, cp_val, sp_val, bp_val, bup_val = get_vals(df_plain, uid)
            trees += tp_val
            crops += cp_val
            shrubs += sp_val
            barren += bp_val
            built_up += bup_val
        except Exception as e:
            pass

        return {
            "terrain_desc": terrain_desc,
            "tree_map_trees": round(trees, 2),
            "tree_map_crops": round(crops, 2),
            "tree_map_shrubs": round(shrubs, 2),
            "tree_map_barren": round(barren, 2),
            "tree_map_built_up": round(built_up, 2),
            "slope_trees": round(ts_val, 2),
            "slope_crops": round(cs_val, 2),
            "slope_shrubs": round(ss_val, 2),
            "slope_barren": round(bs_val, 2),
            "slope_built_up": round(bus_val, 2),
            "plain_trees": round(tp_val, 2),
            "plain_crops": round(cp_val, 2),
            "plain_shrubs": round(sp_val, 2),
            "plain_barren": round(bp_val, 2),
            "plain_built_up": round(bup_val, 2)
        }
    except Exception as e:
        logger.info(f"Error in get_terrain_and_lulc_data for {uid}: {e}")
        return {
            "terrain_desc": "-",
            "tree_map_trees": 0, "tree_map_crops": 0, "tree_map_shrubs": 0, "tree_map_barren": 0, "tree_map_built_up": 0,
            "slope_trees": 0, "slope_crops": 0, "slope_shrubs": 0, "slope_barren": 0, "slope_built_up": 0,
            "plain_trees": 0, "plain_crops": 0, "plain_shrubs": 0, "plain_barren": 0, "plain_built_up": 0
        }


def get_latest_col_val(df, prefix, uid):
    cols = [c for c in df.columns if c.startswith(prefix)]
    if cols:
        cols.sort(reverse=True)
        col = cols[0]
        row = df.loc[df["UID"] == uid]
        if not row.empty:
            v = row[col].values[0]
            return float(v) if not pd.isna(v) else 0.0
    return 0.0


def get_cropping_water_hydro_data(state, district, block, uid):
    try:
        base_path = f"{DATA_DIR_TEMP}{state.upper()}/{district.upper()}/{district.lower()}_{block.lower()}.xlsx"
        
        # Cropping Area
        try:
            df_crop = read_excel_sheet(base_path, "croppingIntensity_annual")
            single_kharif = get_latest_col_val(df_crop, "single_kharif_cropped_area_in_ha_", uid)
            doubly = get_latest_col_val(df_crop, "doubly_cropped_area_in_ha_", uid)
            triply = get_latest_col_val(df_crop, "triply_cropped_area_in_ha_", uid)
            single_non = get_latest_col_val(df_crop, "single_non_kharif_cropped_area_in_ha_", uid)

            crop_kharif_area = round(single_kharif + doubly + triply, 2)
            crop_rabi_area = round(single_non + doubly + triply, 2)
            crop_zaid_area = round(triply, 2)
        except Exception as e:
            logger.info(f"Failed to read croppingIntensity_annual sheet for {uid}: {e}")
            crop_kharif_area, crop_rabi_area, crop_zaid_area = "-", "-", "-"

        # Surface Water
        try:
            df_swb = read_excel_sheet(base_path, "surfaceWaterBodies_annual")
            water_kharif = round(get_latest_col_val(df_swb, "kharif_area_in_ha_", uid), 2)
            water_rabi = round(get_latest_col_val(df_swb, "rabi_area_in_ha_", uid), 2)
            water_zaid = round(get_latest_col_val(df_swb, "zaid_area_in_ha_", uid), 2)
            
            water_kharif = water_kharif if water_kharif != 0.0 else "-"
            water_rabi = water_rabi if water_rabi != 0.0 else "-"
            water_zaid = water_zaid if water_zaid != 0.0 else "-"
        except Exception as e:
            logger.info(f"Failed to read surfaceWaterBodies_annual sheet for {uid}: {e}")
            water_kharif, water_rabi, water_zaid = "-", "-", "-"

        # Hydrology Seasonal
        try:
            df_hydro = read_excel_sheet(base_path, "hydrological_seasonal")
            rf_kharif = round(get_latest_col_val(df_hydro, "precipitation_kharif_in_mm_", uid), 2)
            rf_rabi = round(get_latest_col_val(df_hydro, "precipitation_rabi_in_mm_", uid), 2)
            rf_zaid = round(get_latest_col_val(df_hydro, "precipitation_zaid_in_mm_", uid), 2)

            et_kharif = round(get_latest_col_val(df_hydro, "et_kharif_in_mm_", uid), 2)
            et_rabi = round(get_latest_col_val(df_hydro, "et_rabi_in_mm_", uid), 2)
            et_zaid = round(get_latest_col_val(df_hydro, "et_zaid_in_mm_", uid), 2)

            ro_kharif = round(get_latest_col_val(df_hydro, "runoff_kharif_in_mm_", uid), 2)
            ro_rabi = round(get_latest_col_val(df_hydro, "runoff_rabi_in_mm_", uid), 2)
            ro_zaid = round(get_latest_col_val(df_hydro, "runoff_zaid_in_mm_", uid), 2)
        except Exception as e:
            logger.info(f"Failed to read hydrological_seasonal sheet for {uid}: {e}")
            rf_kharif, rf_rabi, rf_zaid = "-", "-", "-"
            et_kharif, et_rabi, et_zaid = "-", "-", "-"
            ro_kharif, ro_rabi, ro_zaid = "-", "-", "-"

        return {
            "crop_kharif": crop_kharif_area,
            "crop_rabi": crop_rabi_area,
            "crop_zaid": crop_zaid_area,
            "water_kharif": water_kharif,
            "water_rabi": water_rabi,
            "water_zaid": water_zaid,
            "rf_kharif": rf_kharif,
            "rf_rabi": rf_rabi,
            "rf_zaid": rf_zaid,
            "et_kharif": et_kharif,
            "et_rabi": et_rabi,
            "et_zaid": et_zaid,
            "ro_kharif": ro_kharif,
            "ro_rabi": ro_rabi,
            "ro_zaid": ro_zaid
        }

    except Exception as e:
        logger.info(f"Error in get_cropping_water_hydro_data for {uid}: {e}")
        return {
            k: "-" for k in [
                "crop_kharif", "crop_rabi", "crop_zaid",
                "water_kharif", "water_rabi", "water_zaid",
                "rf_kharif", "rf_rabi", "rf_zaid",
                "et_kharif", "et_rabi", "et_zaid",
                "ro_kharif", "ro_rabi", "ro_zaid"
            ]
        }


def get_soge_data(state, district, block, uid):
    try :
        df = read_excel_sheet(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", "aquifer_vector")
        df_soge = read_excel_sheet(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", "soge_vector")
        df_hydro = read_excel_sheet(DATA_DIR_TEMP + state.upper() + "/" + district.upper() + "/" + district.lower() + "_" + block.lower() + ".xlsx", "hydrological_annual")

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
        df = read_excel_sheet(
            DATA_DIR_TEMP
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            "croppingDrought_kharif",
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

            if drought_check >= 5:
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
        excel_file = _get_excel_file(file_path)
        available_sheets = excel_file.sheet_names
        
        # Check if mws_intersect_villages sheet is present (mandatory)
        if "mws_intersect_villages" not in available_sheets:
            logger.info(
                "mws_intersect_villages sheet not found for %s district, %s block",
                district,
                block
            )
            return [], [], [], [], [], [], [], [], [], [], [], []

        # Load the main sheet
        df = read_excel_sheet(file_path, "mws_intersect_villages")
        
        # Check for optional sheets
        has_nrega = "nrega_assets_village" in available_sheets
        has_socio = "social_economic_indicator" in available_sheets
        
        # Load optional sheets if available
        df_village = None
        df_socio = None
        
        if has_nrega:
            df_village = read_excel_sheet(file_path, "nrega_assets_village")
        
        if has_socio:
            df_socio = read_excel_sheet(file_path, "social_economic_indicator")

        selected_columns_ids = [
            col for col in df.columns if col.startswith("Village IDs")
        ]
        matching = df.loc[df["MWS UID"] == uid, selected_columns_ids]

        if matching.empty:
            villages = []
        else:
            villages = matching.iloc[0].tolist()

        selected_columns_details = [
            col for col in df.columns if col.startswith("Village Details")
        ]
        matching_details = df.loc[df["MWS UID"] == uid, selected_columns_details]

        village_details = {}
        if not matching_details.empty:
            details_raw = matching_details.iloc[0].tolist()
            if details_raw:
                try:
                    village_details = eval(details_raw[0])
                except Exception:
                    village_details = {}

        villages_name = []
        villages_sc = []
        villages_st = []
        villages_pop = []
        villages_intersect_pct = []

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

                detail = village_details.get(id) or village_details.get(str(id)) or {}
                villages_intersect_pct.append(detail.get("percentage_of_area", 0))

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
            villages_intersect_pct,
        )

    except Exception as e:
        logger.info(
            "Error accessing excel for %s district, %s block: %s",
            district,
            block,
        )
        return [], [], [], [], [], [], [], [], [], [], [], []