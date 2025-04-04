import re
import requests
import geopandas as gpd
import pandas as pd
import numpy as np
import pymannkendall as mk
from datetime import datetime
from shapely.geometry import Polygon, MultiPolygon, Point, LineString
from shapely.ops import unary_union
from scipy.spatial.distance import jensenshannon

from nrm_app.settings import GEOSERVER_URL
from nrm_app.settings import OVERPASS_URL
from utilities.logger import setup_logger

logger = setup_logger(__name__)


# MARK: HELPER FUNCTIONS
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
    standardized_dates = []
    for date_str in date_list:
        parts = date_str.split("-")
        if len(parts) == 3:
            year, month, day = parts
            # Add leading zeros if needed
            standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            standardized_dates.append(standardized_date)

    # Parse string dates into datetime objects
    dates = []
    for date_str in standardized_dates:
        # Parse each date string into a datetime object
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        dates.append(date_obj)

    # Find min and max dates
    min_date = min(dates)
    max_date = max(dates)

    # Format to only show month and day (MM-DD)
    min_date_formatted = min_date.strftime("%m-%d")
    max_date_formatted = max_date.strftime("%m-%d")

    return min_date_formatted, max_date_formatted


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


# ? MAIN SECTION
def get_osm_data(district, block, uid):
    try:
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

        response = requests.get(OVERPASS_URL, params={"data": overpass_query})
        response = response.json()

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

        # ? Block Parameters
        parameter_block = f"The block {block}"

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
            parameter_block += f". Key natural features such as {temp} shape the block landscape and impact water flow"

        if final_data["forests"]:
            parameter_block += (
                f". Part of {final_data['forests'][0]['name']}, covering roughly "
                f"{round(final_data['forests'][0]['area_sq_m'] / 10000, 1)} hectares, lies within the block supporting local wildlife and promoting biodiversity"
            )

        if final_data["lakes"] or final_data["reservoirs"]:
            rname = [temp["name"] for temp in final_data["lakes"]]
            rname += [temp["name"] for temp in final_data["reservoirs"]]
            rarea = [
                str(round(temp["area_sq_m"] / 10000, 1)) for temp in final_data["lakes"]
            ]
            rarea += [
                str(round(temp["area_sq_m"] / 10000, 1))
                for temp in final_data["reservoirs"]
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
            parameter_block += f"  hectares  respectively within the block"

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
            parameter_block += f"  kilometers within the block, serve"
            if len(rname) == 1:
                parameter_block += "s"
            parameter_block += (
                f" as a crucial water source for agriculture and daily needs"
            )

        # ? MWS Parameters
        parameter_mws = f"The micro-watershed is in block {block}"

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
            parameter_mws += (
                f". Part of {final_data['forests_mws'][0]['name']}, covering roughly "
                f"{(round(final_data['forests_mws'][0]['area_sq_m'] / 10000))} hectares, lies within the micro-watershed supporting local wildlife and promoting biodiversity"
            )

        if final_data["lakes_mws"] or final_data["reservoirs_mws"]:
            rname = [temp["name"] for temp in final_data["lakes_mws"]]
            rname += [temp["name"] for temp in final_data["reservoirs_mws"]]
            rarea = [
                str(round(temp["area_sq_m"] / 10000, 1))
                for temp in final_data["lakes_mws"]
            ]
            rarea += [
                str(round(temp["area_sq_m"] / 10000, 1))
                for temp in final_data["reservoirs_mws"]
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

        return parameter_block, parameter_mws

    except Exception as e:
        logger.info("The geojson is empty !", e)
        return ""


def get_terrain_data(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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

        df["% of area hill_slope"] = pd.to_numeric(
            df["% of area hill_slope"], errors="coerce"
        )
        df["% of area plain_area"] = pd.to_numeric(
            df["% of area plain_area"], errors="coerce"
        )
        df["% of area ridge_area"] = pd.to_numeric(
            df["% of area ridge_area"], errors="coerce"
        )
        df["% of area slopy_area"] = pd.to_numeric(
            df["% of area slopy_area"], errors="coerce"
        )
        df["% of area valley_area"] = pd.to_numeric(
            df["% of area valley_area"], errors="coerce"
        )

        (
            terrain_desc,
            area,
            hill_slope,
            plain_area,
            ridge_area,
            slopy_area,
            valley_area,
        ) = df.loc[
            df["UID"] == uid,
            [
                "Terrain_Description",
                "area_in_hac",
                "% of area hill_slope",
                "% of area plain_area",
                "% of area ridge_area",
                "% of area slopy_area",
                "% of area valley_area",
            ],
        ].values[0]

        # ? Terrain Parameters
        parameter_mws = f""

        if terrain_desc == "Broad Sloppy and Hilly":
            parameter_mws += f"The micro-watershed is spread across {round(area, 1)} hectares.The terrain of our micro-watershed consists of gently sloping land and rolling hills with {round(slopy_area, 1)} % area under broad slopes and {round(hill_slope, 1)} % area under hills."
        elif terrain_desc == "Mostly Plains":
            parameter_mws += f"The micro-watershed is spread across {round(area, 1)} hectares.The micro-watershed mainly consists of flat plains covering {round(plain_area, 1)} % micro-watershed area."
        elif terrain_desc == "Mostly Hills and Valleys":
            parameter_mws += f"The micro-watershed is spread across {round(area, 1)} hectares.The micro-watershed terrain is mainly hills and valleys with {round(hill_slope, 1)} % under hills and {round(valley_area, 1)} % under valleys."
        else:
            parameter_mws += f"The micro-watershed is spread across {round(area, 1)} hectares.The micro-watershed includes flat plains and gentle slopes with {round(plain_area, 1)} % area as plains and {round(slopy_area, 1)} % area under broad slopes."

        # ? Calculate block-level total area and weighted topography areas
        total_block_area = df["area_in_hac"].sum()

        # ? Calculate weighted area for each topography type
        block_hill_slope = sum(df["% of area hill_slope"] * df["area_in_hac"] / 100)
        block_plain_area = sum(df["% of area plain_area"] * df["area_in_hac"] / 100)
        block_ridge_area = sum(df["% of area ridge_area"] * df["area_in_hac"] / 100)
        block_slopy_area = sum(df["% of area slopy_area"] * df["area_in_hac"] / 100)
        block_valley_area = sum(df["% of area valley_area"] * df["area_in_hac"] / 100)

        # ? Create Dictionary for comparison
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

        # ? Test for terrain comparison
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

        parameters_terrain_comp = f""

        if js_divergence > threshold:
            parameters_terrain_comp += f"The microwatershed profile differs from the typical microwatershed profile observed at the block level. While the block-level terrain is predominantly characterized by {round(block_top1_pct, 1)} % {block_top1} and {round(block_top2_pct, 1)} % {block_top2}, the microwatershed primarily consists of {round(mws_top1_pct, 1)} % {mws_top1} and {round(mws_top2_pct, 1)} % {mws_top2}."
        else:
            parameters_terrain_comp += f"The microwatershed profile is similar to the typical microwatershed profile observed at the block level."

        # ? For the LULCxSlope and LULCxPlain comparisons
        slope_df = pd.read_excel(
            "data/stats_excel_files/"
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="terrain_lulc_slope",
        )
        plain_df = pd.read_excel(
            "data/stats_excel_files/"
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="terrain_lulc_plain",
        )
        parameters_land_use = f"During  2017-22, the micro-watershed's slopes and plains have exhibited distinct land-use patterns. "

        mws_areas_slope = []
        block_areas_slope = []

        mws_areas_plain = []
        block_areas_plain = []

        if uid in slope_df["UID"].values:
            tree_slope, shrub_slope, barren_slope = slope_df.loc[
                slope_df["UID"] == uid,
                ["% of area forests", "% of area shrub_scrubs", "% of area barren"],
            ].values[0]
            parameters_land_use += f"On the slopes, land use is predominantly characterized by {round(tree_slope, 1)} % trees, {round(shrub_slope, 1)} % shrubs, and {round(barren_slope, 1)} % barren areas."

            # * Calculate block-level land use for slopes
            block_trees_slope = sum(
                slope_df["% of area forests"] * slope_df["area_in_hac"] / 100
            )
            block_shrub_slope = sum(
                slope_df["% of area shrub_scrubs"] * slope_df["area_in_hac"] / 100
            )
            block_barren_slope = sum(
                slope_df["% of area barren"] * slope_df["area_in_hac"] / 100
            )

            #! ASK aadi sir ki should we calculate the total area of block or total area under slope of block for calculation
            land_use_types = ["Shrubs", "Barren Areas", "Trees"]
            mws_areas_slope.append(shrub_slope)
            mws_areas_slope.append(barren_slope)
            mws_areas_slope.append(tree_slope)

            block_areas_slope.append(block_shrub_slope * 100 / total_block_area)
            block_areas_slope.append(block_barren_slope * 100 / total_block_area)
            block_areas_slope.append(block_trees_slope * 100 / total_block_area)

            # block_top_slope = sorted(zip(land_use_types, block_areas), key=lambda x: x[1], reverse=True)[:2]
            # mws_top_slope = sorted(zip(land_use_types, mws_areas), key=lambda x: x[1], reverse=True)[:2]

            # block_top_slope1, block_top_slope1_pct = block_top_slope[0]
            # block_top_slope2, block_top_slope2_pct = block_top_slope[1]

            # mws_top_slope1, mws_top_slope1_pct = mws_top_slope[0]
            # mws_top_slope2, mws_top_slope2_pct = mws_top_slope[1]

            # mws_areas = np.array(mws_areas) / np.sum(mws_areas)
            # block_areas = np.array(block_areas) / np.sum(block_areas)

            # js_divergence = jensenshannon(mws_areas, block_areas)
            # threshold = 0.1
            # parameters_lulc_comp = f""

            # if js_divergence > threshold:
            #     parameters_lulc_comp += f"The microwatershed profile differs from the typical microwatershed profile observed at the block level.At the block-level land-use is predominantly characterized by {round(block_top_slope1_pct,1)} % {block_top_slope1} and {round(block_top_slope2_pct,1)} % {block_top_slope2}, land-use at the microwatershed level primarily consists of {round(mws_top_slope1_pct,1)} % {mws_top_slope1} and {round(mws_top_slope2_pct,1)} % {mws_top_slope2}."
            # else:
            #     parameters_lulc_comp += f"The microwatershed profile is similar to the typical microwatershed profile observed at the block level."

        if uid in plain_df["UID"].values:
            single_plain, double_plain, triple_plain, barren_plain, shrub_plain = (
                plain_df.loc[
                    plain_df["UID"] == uid,
                    [
                        "% of area single_cropping",
                        "% of area double cropping",
                        "% of area triple cropping",
                        "% of area barren",
                        "% of area shrub_scrubs",
                    ],
                ].values[0]
            )
            parameters_land_use += f"On the plains, land use has predominance of  {round(single_plain + double_plain + triple_plain, 1)} % farmlands, {round(barren_plain, 1)} % barren areas, and {round(shrub_plain, 1)} % shrubs. "

            block_single_plain = sum(
                plain_df["% of area single_cropping"] * plain_df["area_in_hac"] / 100
            )
            block_double_plain = sum(
                plain_df["% of area double cropping"] * plain_df["area_in_hac"] / 100
            )
            block_triple_plain = sum(
                plain_df["% of area triple cropping"] * plain_df["area_in_hac"] / 100
            )
            block_farmland_plain = (
                block_double_plain + block_single_plain + block_triple_plain
            )
            block_shrub_plain = sum(
                plain_df["% of area shrub_scrubs"] * plain_df["area_in_hac"] / 100
            )
            block_barren_plain = sum(
                plain_df["% of area barren"] * plain_df["area_in_hac"] / 100
            )

            land_use_types = ["Shrubs", "Barren Areas", "Farmlands"]
            mws_areas_plain.append(shrub_plain)
            mws_areas_plain.append(barren_plain)
            mws_areas_plain.append(single_plain + double_plain + triple_plain)

            block_areas_plain.append(block_shrub_plain * 100 / total_block_area)
            block_areas_plain.append(block_barren_plain * 100 / total_block_area)
            block_areas_plain.append(block_farmland_plain * 100 / total_block_area)

        return (
            parameter_mws,
            mws_areas,
            block_areas,
            parameters_terrain_comp,
            parameters_land_use,
            mws_areas_slope,
            block_areas_slope,
            mws_areas_plain,
            block_areas_plain,
        )

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block", district, block, e
        )
        return "random"


def get_change_detection_data(state, district, block, uid):
    try:
        df_degrad = pd.read_excel(
            "data/stats_excel_files/"
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
            "data/stats_excel_files/"
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
            "data/stats_excel_files/"
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

        parameter_land = f""
        parameter_tree = f""
        parameter_urban = f""

        # ? Land Degradation
        df_degrad["DEGR_Total"] = df_degrad["DEGR_Total"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_degrad.loc[df_degrad["UID"] == uid, "DEGR_Total"]
        degradation = filtered_df.iloc[0]
        avg = df_degrad["DEGR_Total"].mean()

        if degradation >= 20:
            parameter_land += f"There has been a considerate level of degradation of farmlands in this micro watershed over the years 2017-2022. As compared to average degraded land area of {round(avg, 2)} hectares for the entire block, the degraded land area in this micro-watershed is close to {round(degradation, 2)} hectares."

        # ? Tree Reduction
        df_defo["DEFO_total"] = df_defo["DEFO_total"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_defo.loc[df_defo["UID"] == uid, "DEFO_total"]
        reduction = filtered_df.iloc[0]
        avg = df_defo["DEFO_total"].mean()

        if reduction >= 50:
            parameter_tree += f"There has been a considerate level of reduction in tree cover in this micro watershed over the years 2017-2022, about {round(reduction, 1)} hectares, as compared to {round(avg, 1)} hectares on average in the entire block."

        # ? Urbanization
        df_urban["URBA_Total"] = df_urban["URBA_Total"].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df = df_urban.loc[df_urban["UID"] == uid, "URBA_Total"]
        built_up_area = filtered_df.iloc[0]

        if built_up_area >= 40:
            parameter_urban += f"There has been a considerate level of urbanization in this micro watershed with about {round(built_up_area, 2)} hectares of land covered with settlements."

        return parameter_land, parameter_tree, parameter_urban

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for degradation",
            district,
            block,
            e,
        )


def get_cropping_intensity(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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
        df_drought = pd.read_excel(
            "data/stats_excel_files/"
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

        selected_columns_inten = [
            col for col in df.columns if col.startswith("cropping_intensity_")
        ]
        df[selected_columns_inten] = df[selected_columns_inten].apply(
            pd.to_numeric, errors="coerce"
        )

        filtered_df_inten = df.loc[df["UID"] == uid, selected_columns_inten].values[0]

        # ? Mann Kendal Slope Calculation
        result = mk.original_test(filtered_df_inten)

        parameter_inten = []

        temp_parameter = f""

        # ? Drought Desription
        selected_columns_moderate = [
            col for col in df_drought.columns if col.startswith("Moderate_")
        ]
        df_drought[selected_columns_moderate] = df_drought[
            selected_columns_moderate
        ].apply(pd.to_numeric, errors="coerce")

        selected_columns_severe = [
            col for col in df_drought.columns if col.startswith("Severe_")
        ]
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(
            pd.to_numeric, errors="coerce"
        )

        mws_drought_moderate = df_drought.loc[
            df_drought["UID"] == uid, selected_columns_moderate
        ].values[0]
        mws_drought_severe = df_drought.loc[
            df_drought["UID"] == uid, selected_columns_severe
        ].values[0]

        drought_years = []

        for index, item in enumerate(mws_drought_moderate):
            drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
            if drought_check > 5:
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    drought_years.append(match_exp.group(0))

        # ? AVG Diff b/w drought and non-drought years
        total_inten_d = 0  # Drought intensity

        for year in drought_years:
            temp_drought_cols = [
                col
                for col in df.columns
                if col.startswith("cropping_intensity_" + str(year))
            ]
            df[temp_drought_cols] = df[temp_drought_cols].apply(
                pd.to_numeric, errors="coerce"
            )
            drought_data = df.loc[df["UID"] == uid, temp_drought_cols].values[0]
            total_inten_d += drought_data[0]

        total_inten_nd = sum(filtered_df_inten) - total_inten_d  # non-Drought Intensity

        avg_crp_inten = sum(filtered_df_inten) / len(
            filtered_df_inten
        )  # avg cropping intensity

        # inten_diff = abs((total_inten_d / len(drought_years)) - (total_inten_nd / (len(filtered_df_inten) - len(drought_years))))

        if result.trend == "increasing":
            temp_parameter += f"The cropping intensity of the micro-watershed has increased over the last six years from ({round(filtered_df_inten[0], 2)} to({round(filtered_df_inten[len(filtered_df_inten) - 1], 2)}."
        elif result.trend == "decreasing":
            temp_parameter += f"The cropping intensity of this area has reduced over time from {round(filtered_df_inten[0], 2)} to ({round(filtered_df_inten[len(filtered_df_inten) - 1], 2)})."
        else:
            temp_parameter += f"The cropping intensity of this area has (stayed steady) at {round(avg_crp_inten, 2)}."

        if avg_crp_inten < 1.5 and result.trend != "increasing":
            temp_parameter += f"It might be possible to improve cropping intensity through more strategic placement, while keeping equity in mind, of rainwater harvesting or groundwater recharge structures."

        parameter_inten.append(temp_parameter)

        return parameter_inten

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Cropping Intensity",
            district,
            block,
            e,
        )


def get_double_cropping_area(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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
            col for col in df.columns if col.startswith("single_kharif_cropped_area")
        ]
        df[selected_columns_single] = df[selected_columns_single].apply(
            pd.to_numeric, errors="coerce"
        )

        selected_columns_singleK = [
            col
            for col in df.columns
            if col.startswith("single_non_kharif_cropped_area")
        ]
        df[selected_columns_singleK] = df[selected_columns_singleK].apply(
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
        filtered_df_singleK = df.loc[df["UID"] == uid, selected_columns_singleK].values[
            0
        ]
        filtered_df_double = df.loc[df["UID"] == uid, selected_columns_double].values[0]
        filtered_df_triple = df.loc[df["UID"] == uid, selected_columns_triple].values[0]

        double_cropping_percent = []

        for index, area in enumerate(filtered_df_single):
            total_cropped_area = (
                filtered_df_single[index]
                + filtered_df_singleK[index]
                + filtered_df_double[index]
                + filtered_df_triple[index]
            )
            double_cropping_percent.append(
                (filtered_df_double[index] / total_cropped_area) * 100
            )

        double_cropping_percent_avg = sum(double_cropping_percent) / len(
            double_cropping_percent
        )
        double_cropping_avg = sum(filtered_df_double) / len(filtered_df_double)

        parameter_double_crop = f""

        if double_cropping_percent_avg < 30:
            parameter_double_crop += f"This microwatershed area has a low percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is less than 30% of the total agricultural land being cultivated twice a year."
        elif double_cropping_percent_avg >= 30 and double_cropping_percent_avg < 60:
            parameter_double_crop += f"This microwatershed area has a moderate percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is about {round(double_cropping_percent_avg, 2)} of the total agricultural land being cultivated twice a year."
        else:
            parameter_double_crop += f"This microwatershed area has a high percentage of double-cropped land ({round(double_cropping_avg, 2)} hectares), which is more than 60% of the total agricultural land being cultivated twice a year."

        return parameter_double_crop

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for cropping",
            district,
            block,
            e,
        )


def get_surface_Water_bodies_data(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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
        df_area = pd.read_excel(
            "data/stats_excel_files/"
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
        df_drought = pd.read_excel(
            "data/stats_excel_files/"
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

        parameter_swb = f""
        parameter_trend_swb = []
        filtered_df_kharif = []
        filtered_df_rabi = []
        filtered_df_zaid = []
        avg_swb_avail = f""

        filtered_df = df.loc[df["UID"] == uid, selected_columns]

        if not filtered_df.empty:
            filtered_df = filtered_df.values[0]

            # ? Avg Suface waterbody area
            avg_surface_area = sum(filtered_df) / len(filtered_df)

            mws_area = df_area.loc[df_area["UID"] == uid, ["area_in_hac"]].values[0]

            percent_water_coverage = (avg_surface_area / mws_area[0]) * 100

            # ? Waterbody Description
            parameter_swb += f"{round(percent_water_coverage, 2)}% of the micro-watershed area, equivalent to {round(avg_surface_area, 2)} hectares, is covered by surface water bodies."

            # ? Data yearwise for waterbody
            selected_columns_kharif = [
                col for col in df.columns if col.startswith("kharif_area_")
            ]
            selected_columns_rabi = [
                col for col in df.columns if col.startswith("rabi_area")
            ]
            selected_columns_zaid = [
                col for col in df.columns if col.startswith("zaid_area_")
            ]

            df[selected_columns_kharif] = df[selected_columns_kharif].apply(
                pd.to_numeric, errors="coerce"
            )
            df[selected_columns_rabi] = df[selected_columns_rabi].apply(
                pd.to_numeric, errors="coerce"
            )
            df[selected_columns_zaid] = df[selected_columns_zaid].apply(
                pd.to_numeric, errors="coerce"
            )

            filtered_df_kharif = (
                df.loc[df["UID"] == uid, selected_columns_kharif].values[0].tolist()
            )
            filtered_df_rabi = (
                df.loc[df["UID"] == uid, selected_columns_rabi].values[0].tolist()
            )
            filtered_df_zaid = (
                df.loc[df["UID"] == uid, selected_columns_zaid].values[0].tolist()
            )

            # ? Mann Kendal Slope Calculation
            result = mk.original_test(filtered_df_kharif)

            if result.trend == "increasing":
                parameter_trend_swb.append(
                    f"Surface water presence has increased by {round(result.slope, 2)} hectares per year during 2017-22."
                )
            elif result.trend == "decreasing":
                parameter_trend_swb.append(
                    f"Surface water presence has decreased by {round(result.slope, 2)} hectares per year during 2017-22."
                )
                parameter_trend_swb.append(
                    f"Siltation could be a cause for decrease in surface water presence and therefore may require repair and maintenance of surface water bodies. Waterbody analysis can help identify waterbodies that may need such treatment."
                )
            else:
                parameter_trend_swb.append(
                    f"The surface water presence has remained steady during 2017-22."
                )

            # ? Drought Desription
            selected_columns_moderate = [
                col for col in df_drought.columns if col.startswith("Moderate_")
            ]
            df_drought[selected_columns_moderate] = df_drought[
                selected_columns_moderate
            ].apply(pd.to_numeric, errors="coerce")

            selected_columns_severe = [
                col for col in df_drought.columns if col.startswith("Severe_")
            ]
            df_drought[selected_columns_severe] = df_drought[
                selected_columns_severe
            ].apply(pd.to_numeric, errors="coerce")

            mws_drought_moderate = df_drought.loc[
                df_drought["UID"] == uid, selected_columns_moderate
            ].values[0]
            mws_drought_severe = df_drought.loc[
                df_drought["UID"] == uid, selected_columns_severe
            ].values[0]

            drought_years = []

            for index, item in enumerate(mws_drought_moderate):
                drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
                if drought_check > 5:
                    match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                    if match_exp:
                        drought_years.append(match_exp.group(0))

            total_swb_drought = 0
            total_swb_kharif = 0
            total_swb_rabi = 0
            total_swb_rabi_drought = 0

            percent_comp = 0
            percent_change_r_z = 0
            percent_change_r_z_drought = 0

            temp_Str = f""
            temp_Str_nd = f""

            avg_swb_drought = 0
            avg_swb_drought_rabi = 0
            avg_swb_drought_kharif = 0

            for year in drought_years:
                # ? Drought swb calculation
                temp_columns_drought = [
                    col
                    for col in df.columns
                    if col.startswith("kharif_area_" + str(year))
                ]
                df[temp_columns_drought] = df[temp_columns_drought].apply(
                    pd.to_numeric, errors="coerce"
                )
                drought_data = df.loc[df["UID"] == uid, temp_columns_drought].values[0]
                total_swb_drought += drought_data[0]

                # ? Total rabi drought
                temp_columns_drought = [
                    col
                    for col in df.columns
                    if col.startswith("rabi_area_" + str(year))
                ]
                df[temp_columns_drought] = df[temp_columns_drought].apply(
                    pd.to_numeric, errors="coerce"
                )
                drought_data = df.loc[df["UID"] == uid, temp_columns_drought].values[0]
                total_swb_rabi_drought += drought_data[0]

            temp_column_kharif = [
                col for col in df.columns if col.startswith("kharif_area_")
            ]
            df[temp_column_kharif] = df[temp_column_kharif].apply(
                pd.to_numeric, errors="coerce"
            )
            total_swb_kharif = sum(
                df.loc[df["UID"] == uid, temp_column_kharif].values[0]
            )

            temp_columns_rabi = [
                col for col in df.columns if col.startswith("rabi_area_")
            ]
            df[temp_columns_rabi] = df[temp_columns_rabi].apply(
                pd.to_numeric, errors="coerce"
            )
            total_swb_rabi = sum(df.loc[df["UID"] == uid, temp_columns_rabi].values[0])

            temp_column_zaid = [
                col for col in df.columns if col.startswith("zaid_area_")
            ]
            df[temp_column_zaid] = df[temp_column_zaid].apply(
                pd.to_numeric, errors="coerce"
            )
            total_swb_zaid = sum(df.loc[df["UID"] == uid, temp_column_zaid].values[0])

            avg_swb_avail += f"The average surface water availability in this micro watershed during the Rabi season is {round(((total_swb_rabi / len(temp_columns_rabi)) / mws_area[0]) * 100, 2)} %, while during the Zaid season, it is {round(((total_swb_zaid / len(temp_column_zaid)) / mws_area[0]) * 100, 2)} %"

            if len(drought_years):
                avg_swb_drought = total_swb_drought / len(drought_years)

            avg_swb_non_drought = (total_swb_kharif - total_swb_drought) / (
                len(temp_column_kharif) - len(drought_years)
            )
            percent_comp = (
                avg_swb_non_drought - avg_swb_drought / avg_swb_non_drought
            ) * 100

            # ? percentage change from rabi to zaid
            avg_swb_non_drought_rabi = (total_swb_rabi - total_swb_rabi_drought) / (
                len(temp_columns_rabi) - len(drought_years)
            )
            percent_change_r_z = (
                (avg_swb_non_drought - avg_swb_non_drought_rabi) / avg_swb_non_drought
            ) * 100

            if len(drought_years):
                avg_swb_drought_rabi = (total_swb_rabi_drought) / len(drought_years)
                avg_swb_drought_kharif = (total_swb_drought) / len(drought_years)
                percent_change_r_z_drought = (
                    (avg_swb_drought_kharif - avg_swb_drought_rabi)
                    / avg_swb_drought_kharif
                ) * 100

            if result.trend == "increasing":
                if len(drought_years):
                    temp_Str += f"During the monsoon, on average we observe that the area under surface water during drought years ("
                    for year in drought_years:
                        temp_Str += str(year) + ", "
                    temp_Str += f") is {round(percent_comp, 2)}% less than during non-drought years. This decline highlights a significant impact of drought on surface water availability during the primary crop-growing season, and indicates sensitivity of the cropping to droughts."
                temp_Str_nd += f"In non-drought years, surface water typically decreases by {round(percent_change_r_z, 2)}% from the Kharif to the Rabi season. However, during drought years, this reduction is significantly higher, and reaches {round(percent_change_r_z_drought, 2)} % from Kharif to Rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

            elif result.trend == "decreasing":
                if len(drought_years):
                    temp_Str += f"During the monsoon, we observed a {round(percent_comp, 2)}% decrease in surface water area during drought years ("
                    for year in drought_years:
                        temp_Str += str(year) + ", "
                    temp_Str += f"), as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."
                temp_Str_nd += f"In non-drought years, surface water in kharif typically decreases by {round(percent_change_r_z, 2)}% in rabi. However, during drought years, this seasonal reduction is significantly higher, reaching {round(percent_change_r_z_drought, 2)} % from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions. "

            else:
                if len(drought_years):
                    temp_Str += f"During the monsoon, we observed a {round(percent_comp, 2)}% decrease in surface water area during drought years ("
                    for year in drought_years:
                        temp_Str += str(year) + ", "
                    temp_Str += f"), as compared to non-drought years. This decline serves as a sensitivity measure, highlighting the significant impact of drought on surface water availability during the primary crop-growing season."
                temp_Str_nd += f"In non-drought years, surface water in kharif typically decreases by {round(percent_change_r_z, 2)} % in rabi. However, during drought years, this seasonal reduction is significantly higher, reaching {round(percent_change_r_z_drought, 2)} % from kharif to rabi. This underscores the need for enhanced water conservation measures during kharif to stabilize surface water availability and support rabi agriculture under drought conditions."

            parameter_trend_swb.append(temp_Str)
            parameter_trend_swb.append(temp_Str_nd)

            filtered_df_kharif = [
                abs(kharif - rabi)
                for kharif, rabi in zip(filtered_df_kharif, filtered_df_rabi)
            ]
            filtered_df_rabi = [
                abs(rabi - zaid)
                for rabi, zaid in zip(filtered_df_rabi, filtered_df_zaid)
            ]

        else:
            parameter_swb += (
                f"The MicrowaterShed doesn't have any surface water of its own."
            )

        return (
            parameter_swb,
            parameter_trend_swb,
            avg_swb_avail,
            filtered_df_kharif,
            filtered_df_rabi,
            filtered_df_zaid,
        )

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Waterbodies",
            district,
            block,
            e,
        )


def get_water_balance_data(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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
            "data/stats_excel_files/"
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

        selected_columns = [col for col in df.columns if col.startswith("DeltaG_")]
        df[selected_columns] = df[selected_columns].apply(
            pd.to_numeric, errors="coerce"
        )

        filtered_df_deltag = (
            df.loc[df["UID"] == uid, selected_columns].values[0].tolist()
        )

        selected_columns_g = [col for col in df.columns if col.startswith("G_")]
        df[selected_columns_g] = df[selected_columns_g].apply(
            pd.to_numeric, errors="coerce"
        )

        filtered_df_g = df.loc[df["UID"] == uid, selected_columns_g].values[0].tolist()

        avg_deltag = sum(filtered_df_deltag) / len(filtered_df_deltag)

        parameter_wb_desc = []

        # ? Mann Kendal Slope Calculation
        result = mk.original_test(filtered_df_g)

        parameter_wb = f""
        parameter_wb_rainfall = f""
        parameter_wb_rainfall_bad = f""

        if avg_deltag > 0:
            parameter_wb += f"The water balance is positive and indicates that the groundwater situation in this microwatershed may be stable."
            if result.trend == "increasing":
                parameter_wb += (
                    f"Year on year, the groundwater situation seems to be improving."
                )
            else:
                parameter_wb += f"This however should not be a cause for complacency - over-extraction should be reduced, because over the years it seems that the rate of extraction of groundwater has increased."
        else:
            parameter_wb += f"The water balance is negative and indicates that the groundwater situation in this microwatershed is bad but is improving, or it’s bad and is worsening."
            if result.trend == "increasing":
                parameter_wb += f"There may be efforts of recharge which seems to improve groundwater despite extraction of groundwater."
            else:
                parameter_wb += f"This is a matter of worry. Year on year, the groundwater seems to be depleting due to persistent over-extraction over the years."

        # ? Drought Years
        selected_columns_moderate = [
            col for col in df_drought.columns if col.startswith("Moderate_")
        ]
        df_drought[selected_columns_moderate] = df_drought[
            selected_columns_moderate
        ].apply(pd.to_numeric, errors="coerce")

        selected_columns_severe = [
            col for col in df_drought.columns if col.startswith("Severe_")
        ]
        df_drought[selected_columns_severe] = df_drought[selected_columns_severe].apply(
            pd.to_numeric, errors="coerce"
        )

        mws_drought_moderate = df_drought.loc[
            df_drought["UID"] == uid, selected_columns_moderate
        ].values[0]
        mws_drought_severe = df_drought.loc[
            df_drought["UID"] == uid, selected_columns_severe
        ].values[0]

        drought_years = []
        non_drought_years = []

        for index, item in enumerate(mws_drought_moderate):
            drought_check = mws_drought_moderate[index] + mws_drought_severe[index]
            if drought_check > 5:
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    drought_years.append(match_exp.group(0))
            else:
                match_exp = re.search(r"\d{4}", selected_columns_severe[index])
                if match_exp:
                    non_drought_years.append(match_exp.group(0))

        # ? Good Rainfall Desc
        if len(non_drought_years) > 0:
            original_string = (
                "In the micro-watershed, XXX, YYY and ZZZ were good rainfall years,"
            )
            formatted_years = format_years(non_drought_years)
            parameter_wb_rainfall += original_string.replace(
                "XXX, YYY and ZZZ", formatted_years
            )

            total_preci = 0
            total_good_deltag = 0
            total_percent_runoff = 0
            monsoon_onset = []

            for item in non_drought_years:
                # ? Precipation
                selected_columns_preci = [
                    col
                    for col in df.columns
                    if col.startswith("Precipitation_" + str(item))
                ]
                df[selected_columns_preci] = df[selected_columns_preci].apply(
                    pd.to_numeric, errors="coerce"
                )
                precip_data = df.loc[df["UID"] == uid, selected_columns_preci].values[0]
                total_preci += precip_data[0]

                # ? deltaG
                selected_columns_deltag = [
                    col for col in df.columns if col.startswith("DeltaG_" + str(item))
                ]
                df[selected_columns_deltag] = df[selected_columns_deltag].apply(
                    pd.to_numeric, errors="coerce"
                )
                deltag_data = df.loc[df["UID"] == uid, selected_columns_deltag].values[
                    0
                ]
                total_good_deltag += deltag_data[0]

                # ? Runoff
                selected_columns_runoff = [
                    col for col in df.columns if col.startswith("RunOff_" + str(item))
                ]
                df[selected_columns_runoff] = df[selected_columns_runoff].apply(
                    pd.to_numeric, errors="coerce"
                )
                runoff_data = df.loc[df["UID"] == uid, selected_columns_runoff].values[
                    0
                ]
                total_percent_runoff += (runoff_data[0] / precip_data[0]) * 100

                # ? Monsoon Onset
                selected_columns_monsoon = [
                    col
                    for col in df_drought.columns
                    if col.startswith("monsoon_onset_" + str(item))
                ]
                monsoon_date = df_drought.loc[
                    df_drought["UID"] == uid, selected_columns_monsoon
                ].values[0]
                monsoon_onset.append(monsoon_date[0])

            avg_total_precip = total_preci / len(non_drought_years)

            min_date, max_date = format_date_monsoon_onset(monsoon_onset)

            parameter_wb_rainfall += f" bringing an {get_rainfall_type(avg_total_precip)} average annual rainfall of approximately {round(avg_total_precip, 2)} mm  with monsoon onset between dates {min_date} & {max_date} (mm-dd). "

            if total_good_deltag > 0:
                parameter_wb_rainfall += f"This favorable rainfall pattern resulted in positive groundwater recharge, with average groundwater change of {round((total_good_deltag / len(non_drought_years)), 2)} mm, indicating replenishment of groundwater resources.During these years, around {round((total_percent_runoff / len(non_drought_years)), 2)} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds."
            else:
                parameter_wb_rainfall += f"This favorable rainfall pattern resulted in negative groundwater recharge, with average groundwater change of {round((total_good_deltag / len(non_drought_years)), 2)} mm, indicating depletion of groundwater resources.During these years, around {round((total_percent_runoff / len(non_drought_years)), 2)} % of the rainfall became surface runoff, offering potential for water harvesting, although this should be evaluated carefully so as to not impact downstream micro-watersheds."

        # ? Bad Rainfall Desc
        if len(drought_years) > 0:
            original_string = "In contrast, XXX and YYY were bad rainfall years,"
            formatted_years = format_years(drought_years)
            parameter_wb_rainfall_bad += original_string.replace(
                "XXX and YYY", formatted_years
            )

            total_preci = 0
            total_deltag = 0
            total_percent_runoff = 0

            for item in drought_years:
                # ? Precipitation
                selected_columns_preci = [
                    col
                    for col in df.columns
                    if col.startswith("Precipitation_" + str(item))
                ]
                df[selected_columns_preci] = df[selected_columns_preci].apply(
                    pd.to_numeric, errors="coerce"
                )
                precip_data = df.loc[df["UID"] == uid, selected_columns_preci].values[0]
                total_preci += precip_data[0]

                # ? deltaG
                selected_columns_deltag = [
                    col for col in df.columns if col.startswith("DeltaG_" + str(item))
                ]
                df[selected_columns_deltag] = df[selected_columns_deltag].apply(
                    pd.to_numeric, errors="coerce"
                )
                deltag_data = df.loc[df["UID"] == uid, selected_columns_deltag].values[
                    0
                ]
                total_deltag += deltag_data[0]

                # ? Runoff
                selected_columns_runoff = [
                    col for col in df.columns if col.startswith("RunOff_" + str(item))
                ]
                df[selected_columns_runoff] = df[selected_columns_runoff].apply(
                    pd.to_numeric, errors="coerce"
                )
                runoff_data = df.loc[df["UID"] == uid, selected_columns_runoff].values[
                    0
                ]
                total_percent_runoff += (runoff_data[0] / precip_data[0]) * 100

            avg_total_precip = total_preci / len(drought_years)

            parameter_wb_rainfall_bad += f"leading to reduced annual rainfall averaging around {round(avg_total_precip, 2)} mm."

            if total_deltag > 0:
                parameter_wb_rainfall_bad += f" Limited water availability in these years resulted in positive groundwater changes, with an average replenishment of {round(total_deltag, 2)} mm. Runoff in these years is reduced to {round((total_percent_runoff / len(drought_years)), 2)} % of total rainfall, diminishing the harvestable water. "
            else:
                parameter_wb_rainfall_bad += f" Limited water availability in these years resulted in negative groundwater changes, with an average depletion of {round(total_deltag, 2)} mm. Runoff in these years is reduced to {round((total_percent_runoff / len(drought_years)), 2)} % of total rainfall, diminishing the harvestable water. "

        parameter_wb_desc.append(parameter_wb)
        parameter_wb_desc.append(parameter_wb_rainfall)
        parameter_wb_desc.append(parameter_wb_rainfall_bad)

        selected_columns_precip = [
            col for col in df.columns if col.startswith("Precipitation_")
        ]
        df[selected_columns_precip] = df[selected_columns_precip].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df_precip = (
            df.loc[df["UID"] == uid, selected_columns_g].values[0].tolist()
        )

        selected_columns_runoff = [
            col for col in df.columns if col.startswith("RunOff_")
        ]
        df[selected_columns_runoff] = df[selected_columns_runoff].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df_runoff = (
            df.loc[df["UID"] == uid, selected_columns_runoff].values[0].tolist()
        )

        selected_columns_et = [col for col in df.columns if col.startswith("ET_")]
        df[selected_columns_et] = df[selected_columns_et].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df_et = (
            df.loc[df["UID"] == uid, selected_columns_et].values[0].tolist()
        )

        selected_columns_dg = [col for col in df.columns if col.startswith("DeltaG_")]
        df[selected_columns_dg] = df[selected_columns_dg].apply(
            pd.to_numeric, errors="coerce"
        )
        filtered_df_dg = (
            df.loc[df["UID"] == uid, selected_columns_dg].values[0].tolist()
        )

        return (
            parameter_wb_desc,
            filtered_df_precip,
            filtered_df_runoff,
            filtered_df_et,
            filtered_df_dg,
        )

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Water Balance",
            district,
            block,
            e,
        )


def get_drought_data(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
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
        original_string = "An analysis of identified drought years — XXX, YYY and ZZZ reveals significant insights into the underlying rainfall patterns such as dry spells and deviations from normal precipitation. "
        formatted_years = format_years(drought_years)
        parameter_drought += original_string.replace(
            "XXX, YYY and ZZZ", formatted_years
        )

        # ? Dryspell Calc
        years = []
        drysp_tuple = []

        selected_columns_drysp = [col for col in df.columns if col.startswith("drysp_")]
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

        return parameter_drought, drought_weeks

    except Exception as e:
        logger.info(
            "Not able to access excel for %s district, %s block for Water Balance",
            district,
            block,
            e,
        )


def get_village_data(state, district, block, uid):
    try:
        df = pd.read_excel(
            "data/stats_excel_files/"
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="mws_intersect_villages",
        )
        df_village = pd.read_excel(
            "data/stats_excel_files/"
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="nrega_assets_village",
        )
        df_socio = pd.read_excel(
            "data/stats_excel_files/"
            + state.upper()
            + "/"
            + district.upper()
            + "/"
            + district.lower()
            + "_"
            + block.lower()
            + ".xlsx",
            sheet_name="social_economic_indicator",
        )

        selected_columns_ids = [
            col for col in df.columns if col.startswith("Village IDs")
        ]
        villages = df.loc[df["MWS UID"] == uid, selected_columns_ids].values[0].tolist()

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

            print(villages)

            for id in villages:
                print(id)

                village_name_col = [
                    col for col in df_village.columns if col.startswith("vill_name")
                ]
                name = (
                    df_village.loc[df_village["vill_id"] == id, village_name_col]
                    .values[0]
                    .tolist()
                )
                villages_name.append(name[0])

                swc_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Soil and water conservation")
                ]
                df_village[swc_cols] = df_village[swc_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                swc_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, swc_cols]
                        .values[0]
                        .tolist()
                    )
                )

                lr_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Land restoration")
                ]
                df_village[lr_cols] = df_village[lr_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                lr_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, lr_cols]
                        .values[0]
                        .tolist()
                    )
                )

                plant_cols = [
                    col for col in df_village.columns if col.startswith("Plantations")
                ]
                df_village[plant_cols] = df_village[plant_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                plantation_work.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, plant_cols]
                        .values[0]
                        .tolist()
                    )
                )

                iof_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Irrigation on farms")
                ]
                df_village[iof_cols] = df_village[iof_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                iof_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, iof_cols]
                        .values[0]
                        .tolist()
                    )
                )

                ofl_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Off-farm livelihood assets")
                ]
                df_village[ofl_cols] = df_village[ofl_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                ofl_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, ofl_cols]
                        .values[0]
                        .tolist()
                    )
                )

                ca_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Community assets_2018")
                ]
                df_village[ca_cols] = df_village[ca_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                ca_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, ca_cols]
                        .values[0]
                        .tolist()
                    )
                )

                ofw_cols = [
                    col
                    for col in df_village.columns
                    if col.startswith("Other farm works")
                ]
                df_village[ofw_cols] = df_village[ofw_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                ofw_works.append(
                    sum(
                        df_village.loc[df_village["vill_id"] == id, ofw_cols]
                        .values[0]
                        .tolist()
                    )
                )

                sc_percent_col = [
                    col for col in df_socio.columns if col.startswith("SC_percentage")
                ]
                df_socio[sc_percent_col] = df_socio[sc_percent_col].apply(
                    pd.to_numeric, errors="coerce"
                )
                sc_percent = (
                    df_socio.loc[df_socio["village_id"] == id, sc_percent_col]
                    .values[0]
                    .tolist()
                )
                villages_sc.append(round(sc_percent[0], 2))

                st_percent_col = [
                    col for col in df_socio.columns if col.startswith("ST_percentage")
                ]
                df_socio[st_percent_col] = df_socio[st_percent_col].apply(
                    pd.to_numeric, errors="coerce"
                )
                st_percent = (
                    df_socio.loc[df_socio["village_id"] == id, st_percent_col]
                    .values[0]
                    .tolist()
                )
                villages_st.append(round(st_percent[0], 2))

                pop_col = [
                    col
                    for col in df_socio.columns
                    if col.startswith("total_population")
                ]
                df_socio[pop_col] = df_socio[pop_col].apply(
                    pd.to_numeric, errors="coerce"
                )
                total_pop = (
                    df_socio.loc[df_socio["village_id"] == id, pop_col]
                    .values[0]
                    .tolist()
                )
                villages_pop.append(total_pop[0])

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
            "Not able to access excel for %s district, %s block for village data",
            district,
            block,
            e,
        )
