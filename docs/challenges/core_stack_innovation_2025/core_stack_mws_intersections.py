import ee
import folium
from folium.plugins import Fullscreen
#import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, shape
#import numpy as np
from io import BytesIO
from ast import literal_eval
import json
from core_stack_orm import *
from core_stack_layer_load import *
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from base64 import b64encode

#HELPER METHOD TO READ GEOJSON OF AN AREA OF INTEREST AROUND WHICH MICRO-WATERSHEDS CAN BE STUDIED
def parse_geojson(geojson_path):
    """Parse GeoJSON file and extract geometries"""
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    geometries = []
    
    # Handle different GeoJSON structures
    if geojson_data['type'] == 'FeatureCollection':
        for feature in geojson_data['features']:
            if 'geometry' in feature and feature['geometry']:
                geometries.append(shape(feature['geometry']))
    elif geojson_data['type'] == 'Feature':
        if 'geometry' in geojson_data and geojson_data['geometry']:
            geometries.append(shape(geojson_data['geometry']))
    elif geojson_data['type'] in ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon']:
        # Direct geometry object
        geometries.append(shape(geojson_data))
    
    return geometries

#HELPER METHOD TO CONVERT GEOJSON GEOMETRY FOR OPERATIONS ON GEE
def shapely_to_ee_geometry(geom):
    """Convert Shapely geometry to Earth Engine geometry"""
    if geom.geom_type == 'Point':
        return ee.Geometry.Point([geom.x, geom.y])
    elif geom.geom_type == 'LineString':
#        coords = list(geom.coords)
        coords = [[c[0], c[1]] for c in geom.coords]
        return ee.Geometry.LineString(coords)
    elif geom.geom_type == 'Polygon':
#        coords = list(geom.exterior.coords)
        coords = [[c[0], c[1]] for c in geom.exterior.coords]
        return ee.Geometry.Polygon([coords])
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")

#METHOD TO INTERSECT AREA OF INTEREST WITH MICRO-WATERSHEDS v2 BOUNDARIES ON GEE
def get_intersecting_mws(kml_geometry, mws_polygons):
    """Find MWSes that intersect with the KML geometry"""
    ee_geom = shapely_to_ee_geometry(kml_geometry)
    intersecting = mws_polygons.filterBounds(ee_geom)
    return intersecting

#METHOD TO FIND UPSTREAM MICRO-WATERSHEDS TO A GIVEN LIST OF MICRO-WATERSHEDS
#USE MICRO-WATERSHED CONNECTIVITY LAYER ON GEE FOR THIS PURPOSE
def get_upstream_mws(mws_ids, connectivity_fc, level=1):
    """Get upstream MWSes for given MWS IDs up to specified level"""
    if level == 0 or not mws_ids:
        return set()
    
    upstream_ids = set()
    
    # Find all features where 'uid' is in our mws_ids
    for mws_id in mws_ids:
        mws_features = connectivity_fc.filter(
            ee.Filter.eq('uid', mws_id)
        ).getInfo()['features']
        
        for feature in mws_features:
            upstream_list = literal_eval(feature['properties'].get('upstream', []))
            if isinstance(upstream_list, list):
                upstream_ids.update(upstream_list)
            elif upstream_list:  # Single value
                upstream_ids.add(upstream_list)
    
    # Recursively get upstream MWSes for the next level
    if level > 1:
        next_level = get_upstream_mws(upstream_ids, connectivity_fc, level - 1)
        upstream_ids.update(next_level)
    
    return upstream_ids

#METHOD TO FIND DOWNSTREAM MICRO-WATERSHEDS TO A GIVEN LIST OF MICRO-WATERSHEDS
#USE MICRO-WATERSHED CONNECTIVITY LAYER ON GEE FOR THIS PURPOSE
def get_downstream_mws(mws_ids, connectivity_fc):
    """Get immediate downstream MWSes for given MWS IDs"""
    downstream_ids = set()
    
    # Find all features where 'uid' is in our mws_ids
    for mws_id in mws_ids:
        mws_features = connectivity_fc.filter(
            ee.Filter.eq('uid', mws_id)
        ).getInfo()['features']
        
        for feature in mws_features:
            downstream = feature['properties'].get('downstream')
            if downstream:
                downstream_ids.add(downstream)
    
    return downstream_ids

#HELPER METHOD TO GET CENTROID OF A POLYGON
def get_mws_centroid(mws_geometry):
    """Get centroid of an MWS geometry"""
    geom = mws_geometry
    if geom['type'] == 'Polygon':
        coords = geom['coordinates'][0]
    elif geom['type'] == 'MultiPolygon':
        coords = geom['coordinates'][0][0]
    else:
        return None
    
    # Calculate centroid
    x_coords = [c[0] for c in coords]
    y_coords = [c[1] for c in coords]
    centroid = [np.mean(x_coords), np.mean(y_coords)]
    return centroid

#METHOD TO FIND TEHSILS THAT A LIST OF MICRO-WATERSHEDS INTERSECT WITH
#USE TEHSIL LAYER ON GEE FOR THIS PURPOSE
def get_intersecting_tehsils(mws_ids, mws_polygons, tehsil_fc):
    """Get tehsils that intersect with the given MWS IDs"""
    # Filter MWS polygons by IDs
    mws_features = mws_polygons.filter(
        ee.Filter.inList('uid', list(mws_ids))
    )
    
    # Get the union of all MWS geometries
    mws_union = mws_features.geometry().dissolve()
    
    # Find intersecting tehsils
    intersecting_tehsils = tehsil_fc.filterBounds(mws_union)
    
    # Get tehsil information
    tehsil_info = intersecting_tehsils.getInfo()
    
    tehsil_list = []
    for feature in tehsil_info['features']:
        props = feature['properties']
        tehsil_list.append({
            'STATE': props.get('STATE', 'N/A').lower(),
            'District': props.get('District', 'N/A').lower(),
            'TEHSIL': props.get('TEHSIL', 'N/A').lower()
        })
    
    return tehsil_list

#PRIMARY METHOD TO BUILD THE MICRO-WATERSHED CONNECTIVITY GRAPH BY CALLING VARIOUS HELPER METHODS
#ALSO COMPUTE CENTROIDS OF THE MICRO-WATERSHEDS
def build_mws_network(geojson_path):
    """Main function to process GeoJSON and plot MWS network"""
    
    # Parse KML
    print("Parsing GeoJSON file...")
    geometries = parse_geojson(geojson_path)
    
    if not geometries:
        raise ValueError("No geometries found in GeoJSON file")

    # Load datasets
    print("Loading GEE datasets...")
    mws_connectivity = ee.FeatureCollection(
        "projects/corestack-datasets/assets/datasets/India_mws_connectivity"
    )
    mws_polygons = ee.FeatureCollection(
        "projects/corestack-datasets/assets/datasets/India_mws_uid_area_gt_500"
    )
    tehsil_fc = ee.FeatureCollection(
        "projects/ext-datasets/assets/datasets/SOI_tehsil"
    )
    
    # Get intersecting MWSes
    print("Finding intersecting MWSes...")
    all_intersecting_ids = set()
    
    for geom in geometries:
        intersecting = get_intersecting_mws(geom, mws_polygons)
        intersecting_info = intersecting.getInfo()
        
        for feature in intersecting_info['features']:
            uid = feature['properties'].get('uid')
            if uid:
                all_intersecting_ids.add(uid)
    
    print(f"Found {len(all_intersecting_ids)} intersecting MWSes: {all_intersecting_ids}")
    
    # Get upstream and downstream MWSes
    print("Finding upstream MWSes (3 levels)...")
    upstream_level1 = get_upstream_mws(all_intersecting_ids, mws_connectivity, level=1)
    upstream_level2 = get_upstream_mws(upstream_level1, mws_connectivity, level=1)
    upstream_level3 = get_upstream_mws(upstream_level2, mws_connectivity, level=1)
    
    print("Finding downstream MWSes (1 level)...")
    downstream_level1 = get_downstream_mws(all_intersecting_ids, mws_connectivity)
    
    # Combine all MWS IDs
    all_mws_ids = all_intersecting_ids.union(upstream_level1).union(upstream_level2).union(upstream_level3).union(downstream_level1)
    
    print(f"Total MWSes: {len(all_mws_ids)}")
    print(f"  - Intersecting: {len(all_intersecting_ids)} - {all_intersecting_ids}")
    print(f"  - Upstream Level 1: {len(upstream_level1)} - {upstream_level1}")
    print(f"  - Upstream Level 2: {len(upstream_level2)} - {upstream_level2}")
    print(f"  - Upstream Level 3: {len(upstream_level3)} - {upstream_level3}")
    print(f"  - Downstream: {len(downstream_level1)} - {downstream_level1}")
    
    # Get intersecting tehsils
    print("Finding intersecting tehsils...")
    tehsil_list = get_intersecting_tehsils(all_mws_ids, mws_polygons, tehsil_fc)
    
    print(f"\nIntersecting Tehsils ({len(tehsil_list)}):")
    for tehsil in tehsil_list:
        print(f"  - {tehsil['TEHSIL']}, {tehsil['District']}, {tehsil['STATE']}")
    
    # Get MWS features for plotting
    print("\nFetching MWS geometries...")
    mws_features_list = mws_polygons.filter(
        ee.Filter.inList('uid', list(all_mws_ids))
    ).getInfo()['features']
    
    # Create a map of uid to centroid and level
    mws_data = {}
    for feature in mws_features_list:
        uid = feature['properties']['uid']
        centroid = get_mws_centroid(feature['geometry'])
        
        # Determine level
        if uid in all_intersecting_ids:
            level = 'intersecting'
        elif uid in upstream_level1:
            level = 'upstream_l1'
        elif uid in upstream_level2:
            level = 'upstream_l2'
        elif uid in upstream_level3:
            level = 'upstream_l3'
        elif uid in downstream_level1:
            level = 'downstream_l1'
        else:
            level = 'other'
        
        mws_data[uid] = {
            'centroid': centroid,
            'level': level,
            'geometry': feature['geometry']
        }

    return all_mws_ids, tehsil_list, mws_data, geometries, mws_connectivity

#PRIMARY FUNCTION TO PLOT THE MICRO-WATERSHEDS AND TIME SERIES DATA ON A MAP
def plot_mws_network(tehsils_df, all_mws_ids, tehsil_list, mws_data, geometries, mws_connectivity, output_html='data/mws_network.html'):
    # Create map centered on the KML area
    first_geom = geometries[0]
    center_coords = list(first_geom.centroid.coords)[0]
    m = folium.Map(location=[center_coords[1], center_coords[0]], zoom_start=12)
    
    folium.TileLayer(
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google',
        name='Google Satellite',
        overlay=True,
        control=True
    ).add_to(m)
    folium.LayerControl().add_to(m)

    Fullscreen(
        position="topright",
        title="Expand me",
        title_cancel="Exit me",
        force_separate_button=True,
    ).add_to(m)

    # Color scheme for different levels
    colors = {
        'intersecting': '#FF0000',    # Red
        'upstream_l1': '#FFA500',     # Orange
        'upstream_l2': '#FFFF00',     # Yellow
        'upstream_l3': '#FFF000',     # XXX
        'downstream_l1': '#0000FF',   # Blue
        'other': '#808080'            # Gray
    }
    
    # Plot MWS polygons
    print("Plotting MWS polygons...")
    for uid, data in mws_data.items():
        geom = data['geometry']
        level = data['level']
        color = colors.get(level, '#808080')
        
        # Convert geometry to GeoJSON format for Folium
        if geom['type'] == 'Polygon':
            coords = [[[c[1], c[0]] for c in geom['coordinates'][0]]]
        elif geom['type'] == 'MultiPolygon':
            coords = [[[[c[1], c[0]] for c in poly[0]] for poly in geom['coordinates']]]
        else:
            continue
        
        row_df = tehsils_df.query(f"uid == '{uid}'")
        if row_df.empty:
            continue  # Skip if no matching row found
        row = row_df.iloc[0]  # Get first row as a Series

        # Set font sizes globally for all plots
        plt.rcParams.update({'font.size': 8, 'axes.titlesize': 8, 'axes.labelsize': 8, 
                            'xtick.labelsize': 8, 'ytick.labelsize': 8, 'legend.fontsize': 8})        

        # Prepare terrain values and labels
        area_in_ha = (float)(row.get("area_in_ha"))
        terrain_vals = []
        terrain_labels = []
        for key in ['plain_area', 'slopy_area', 'hill_slope', 'valley_are', 'ridge_area']:
            val = row.get(key)
            # Check if value is not null and > 0
            if pd.notna(val) and float(val) > 0:
                terrain_vals.append(float(val) * area_in_ha / 100)
                terrain_labels.append(key.replace('_', ' ').capitalize())

        if terrain_vals and sum(terrain_vals) > 0:
            plt.figure(figsize=(2.5, 2))
            plt.bar(terrain_labels, terrain_vals)
            plt.ylabel('Area in ha', fontsize=8)
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.yticks(fontsize=8)
            plt.tight_layout()
            pie_path = f"data/pie_{uid}.png"
            plt.savefig(pie_path, dpi=200, bbox_inches='tight')
            plt.close()    

            # Prepare HTML for terrain chart image
            with open(pie_path, 'rb') as imgf:
                pie_img_b64 = b64encode(imgf.read()).decode('utf-8')
                pie_html = f'<img src="data:image/png;base64,{pie_img_b64}" style="width:175px; height:auto;"/>'
        else:
            pie_html = ''

        # Prepare tree cover loss values and labels
        treeloss_vals = []
        treeloss_labels = []
        for key in ['forest_to_barren', 'forest_to_builtu', 'forest_to_farm', 'forest_to_scrub']:
            val = row.get(key)
            # Check if value is not null and > 0
            if pd.notna(val) and float(val) > 0:
                treeloss_vals.append(float(val))
                treeloss_labels.append(key.replace('_', ' ').capitalize())

        if treeloss_vals and sum(treeloss_vals) > 0:
            plt.figure(figsize=(2.5, 2))
            plt.bar(treeloss_labels, treeloss_vals)
            plt.ylabel('Area in ha', fontsize=8)
            plt.xticks(rotation=45, ha='right', fontsize=8)
            plt.yticks(fontsize=8)
            plt.title('Tree cover loss', fontsize=8)
            plt.tight_layout()
            tree_path = f"data/tree_{uid}.png"
            plt.savefig(tree_path, dpi=100, bbox_inches='tight')
            plt.close()    

            # Prepare HTML for terrain chart image
            with open(tree_path, 'rb') as imgf:
                tree_img_b64 = b64encode(imgf.read()).decode('utf-8')
                tree_html = f'<img src="data:image/png;base64,{tree_img_b64}" style="width:175px; height:auto;"/>'
        else:
            tree_html = ''

        # Generate time-series plot for water availability
        sw_ts_html = ''
        sw_k_list = row.get('sw_k', [])
        sw_r_list = row.get('sw_r', [])
        sw_z_list = row.get('sw_z', [])
        
        if sw_k_list or sw_r_list or sw_z_list:
            # Ensure all lists have the same length (pad with NaN if needed)
            max_len = max(len(sw_k_list) if sw_k_list else 0,
                         len(sw_r_list) if sw_r_list else 0,
                         len(sw_z_list) if sw_z_list else 0)
            if max_len > 0:
                # Years for x-axis (assuming data starts from 2017 and is consecutive)
                years = list(range(2017, 2017 + max_len))
                
                if len(sw_k_list) < max_len:
                    sw_k_list = sw_k_list + [np.nan] * (max_len - len(sw_k_list))
                if len(sw_r_list) < max_len:
                    sw_r_list = sw_r_list + [np.nan] * (max_len - len(sw_r_list))
                if len(sw_z_list) < max_len:
                    sw_z_list = sw_z_list + [np.nan] * (max_len - len(sw_z_list))
                
                plt.figure(figsize=(2.5, 2))
                plt.plot(years, sw_k_list, marker='o', label='Kharif (sw_k)', linewidth=1.5, markersize=3)
                plt.plot(years, sw_r_list, marker='s', label='Kharif+Rabi (sw_r)', linewidth=1.5, markersize=3)
                plt.plot(years, sw_z_list, marker='^', label='Kharif+Rabi+Zaid (sw_z)', linewidth=1.5, markersize=3)
                plt.xlabel('Year', fontsize=8)
                plt.ylabel('Water Availability (% of area)', fontsize=8)
                plt.title('Surface Water Availability Time-Series', fontsize=8)
                plt.legend(prop={'size': 8}, loc='best')
                plt.xticks(fontsize=8)
                plt.yticks(fontsize=8)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                
                sw_ts_path = f"data/sw_ts_{uid}.png"
                plt.savefig(sw_ts_path, dpi=100, bbox_inches='tight')
                plt.close()
                
                # Prepare HTML for time-series chart image
                with open(sw_ts_path, 'rb') as imgf:
                    sw_ts_img_b64 = b64encode(imgf.read()).decode('utf-8')
                    sw_ts_html = f'<img src="data:image/png;base64,{sw_ts_img_b64}" style="width:175px; height:auto;"/>'

        # Generate time-series plot for hydrological data
        # Create figure with two y-axes
        fig, ax1 = plt.subplots(figsize=(7, 3.5))
        
        # Create second y-axis that shares the same x-axis
        ax2 = ax1.twinx()
        
        hydro_dates = list(row.get('hydro_dates')) if row.get('hydro_dates') is not None else []
        hydro_rainfall = row.get('hydro_rainfall') if row.get('hydro_rainfall') is not None else np.array([])
        hydro_et = row.get('hydro_et') if row.get('hydro_et') is not None else np.array([])
        hydro_runoff = row.get('hydro_runoff') if row.get('hydro_runoff') is not None else np.array([])
        hydro_waterbalance = row.get('hydro_waterbalance') if row.get('hydro_waterbalance') is not None else np.array([])

        # Plot precipitation as bars coming from top (on ax2, inverted)
        ax2.bar(hydro_dates, hydro_rainfall, width=5, color='steelblue', alpha=0.6, label='Rainfall')
        ax2.set_ylabel('Rainfall (mm)', fontsize=8, color='steelblue')
        ax2.tick_params(axis='y', labelcolor='steelblue')
        ax2.invert_yaxis()  # Invert so rain comes from top
        ax2.set_ylim(max(hydro_rainfall) * 1.25, 0)  # Set limits with padding
        
        # Plot stacked ET and runoff (on ax1)
#        ax1.bar(hydro_dates, hydro_et, width=5, color='orange', alpha=0.7, label='ET')
#        ax1.bar(hydro_dates, hydro_runoff, width=5, bottom=hydro_et, color='brown', alpha=0.7, label='Runoff')
        ax1.bar(hydro_dates, hydro_runoff, width=5, color='brown', alpha=0.7, label='Runoff')
        
        # Plot water balance as a line (on ax1)
#        ax1.plot(hydro_dates, hydro_waterbalance, color='green', linewidth=1, marker='o', 
#                markersize=1, label='Water Balance', zorder=5)

        # Add horizontal line at y=0 for reference
        ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        
        # Format x-axis
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Labels and title
        ax1.set_xlabel('Date', fontsize=8)
        ax1.set_ylabel('ET, Runoff, Water Balance (mm)', fontsize=8)
        ax1.set_title('Water Balance Analysis', fontsize=8)
        
        # Legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=8)
        
        # Grid
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Tight layout
        plt.tight_layout()

        hydro_ts_path = f"data/hydro_ts_{uid}.png"
        plt.savefig(hydro_ts_path, dpi=100, bbox_inches='tight')
        plt.close()
                
        # Prepare HTML for time-series chart image
        with open(hydro_ts_path, 'rb') as imgf:
            hydro_ts_img_b64 = b64encode(imgf.read()).decode('utf-8')
            hydro_ts_html = f'<img src="data:image/png;base64,{hydro_ts_img_b64}" style="width:400px; height:auto;"/>'

        # Prepare the popup HTML with side-by-side layout
        charts_html = ''
        if pie_html or tree_html or sw_ts_html:
            charts_html = '<div style="display:flex; flex-direction:row; width:100%; min-width:500px;"><table border=0><tr>'
#            if pie_html:
#                charts_html += f'<div style="flex:1; text-align:center;"><b style="font-size:11px;">Terrain Distribution</b>{pie_html}</div>'
            if sw_ts_html:
                charts_html += f'<td><div style="flex:1; text-align:left;">{sw_ts_html}</div></td>'
            if tree_html:
                charts_html += f'<td><div style="flex:1; text-align:left;">{tree_html}</div></td>'
            if hydro_ts_html:
                charts_html += f'</tr><tr><td colspan=2><div style="flex:1; text-align:left;">{hydro_ts_html}</div></td></tr><tr>'

            charts_html += '</tr></table></div>'
        
        popup_html = (
            f"MWS UID: {uid}<br>"
            f"Level: {level}<br>"
            f"Area (ha): {row.get('area_in_ha', '?')}"
            f"{charts_html}"
            f"<a href=\"{row.get('url')}\">MWS report</a>"
        )

        folium.Polygon(
            locations=coords[0] if geom['type'] == 'Polygon' else coords[0][0],
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.3,
            weight=2,
            popup = folium.Popup(popup_html, max_width=400)
        ).add_to(m)
    
    # Plot connections with arrows
    print("Plotting connections...")
    for uid in all_mws_ids:
        if uid not in mws_data:
            continue
        
        # Get connectivity info
        conn_features = mws_connectivity.filter(
            ee.Filter.eq('uid', uid)
        ).getInfo()['features']
        
        for feature in conn_features:
            props = feature['properties']
            downstream = props.get('downstream')
            
            if downstream and downstream in mws_data:
                from_centroid = mws_data[uid]['centroid']
                to_centroid = mws_data[downstream]['centroid']
                
                if from_centroid and to_centroid:
                    # Draw arrow
                    start = [from_centroid[1], from_centroid[0]]
                    end = [to_centroid[1], to_centroid[0]]
                    
                    folium.PolyLine(
                        locations=[start, end],
                        color='#000000',
                        weight=2,
                        opacity=0.6,
                        popup=f"{uid} → {downstream}"
                    ).add_to(m)
                    
                    # Add arrowhead
                    folium.RegularPolygonMarker(
                        location=end,
                        fill_color='#000000',
                        color='#000000',
                        number_of_sides=3,
                        radius=6,
                        rotation=0
                    ).add_to(m)
    
    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 100px; height: auto; 
                background-color: white; z-index:9999; font-size:10px;
                border:2px solid grey; padding: 2px">
    <p style="margin:0"><b>MWS Levels</b></p>
    <p style="margin:2px 0"><span style="color:#FF0000">█</span> Intersecting</p>
    <p style="margin:2px 0"><span style="color:#FFA500">█</span> Upstream L1</p>
    <p style="margin:2px 0"><span style="color:#FFFF00">█</span> Upstream L2</p>
    <p style="margin:2px 0"><span style="color:#FFFF00">█</span> Upstream L3</p>
    <p style="margin:2px 0"><span style="color:#0000FF">█</span> Downstream</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save map
    m.save(output_html)
    print(f"\nMap saved to {output_html}")

#PRIMARY INVOVATION FUNCTION TO PLOT THE MAP FOR A GIVEN AREA OF INTEREST
if __name__ == "__main__":
    geojson_file = "C:\\Users\\Prof. A Set\\Downloads\\Sunderpahari.geojson"  # Replace with your GeoJSON file path

    # Initialize Earth Engine, replace with your project
    ee.Authenticate()
    ee.Initialize(project="ee-aaditeshwar")

    mwses, tehsils, mws_data, geometries, mws_connectivity = build_mws_network(geojson_file)
    print(mwses)
    print(tehsils)

    tehsils_df = None
    mws_params = [
        "change_detection_deforestation",
        "change_detection_afforestation", 
        "hydrological_annual",
        "surfaceWaterBodies_annual",
        "terrain",
        "terrain_lulc_plain",
        "terrain_lulc_slope"
        ]

    active_tehsils = get_active_tehsils()
    for tehsil in tehsils:
        if tehsil in active_tehsils:
            print(f"found tehsil {tehsil}")
#            tehsil_obj = get_tehsil_data_from_layers(tehsil["STATE"], tehsil["District"], tehsil["TEHSIL"])
            tehsil_obj = get_tehsil_data_from_api(tehsil["STATE"], tehsil["District"], tehsil["TEHSIL"], mws_params)
            get_hydrological_data_from_api(tehsil_obj, mwses)
            df = build_df(tehsil_obj, mwses)
            if tehsils_df is None:
                tehsils_df = df
            else:
                tehsils_df = pd.concat([tehsils_df, df], ignore_index=True).drop_duplicates(subset=['uid'])
        else:
            print(f"not found tehsil {tehsil}")

    plot_mws_network(tehsils_df, mwses, tehsils, mws_data, geometries, mws_connectivity)


