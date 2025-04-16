import os
import json
import subprocess
from django.conf import settings


def convert_kml_to_geojson(kml_file_path):
    """
    Convert KML file to GeoJSON using ogr2ogr.
    
    Args:
        kml_file_path: Path to the KML file
        
    Returns:
        dict: Parsed GeoJSON data or None if conversion failed
    """
    try:
        # Create a temporary GeoJSON file path
        filename = os.path.basename(kml_file_path)
        base_name = os.path.splitext(filename)[0]
        temp_geojson_path = os.path.join(
            settings.MEDIA_ROOT, 
            'temp', 
            f"{base_name}.geojson"
        )
        
        # Create temp directory if it doesn't exist
        os.makedirs(os.path.dirname(temp_geojson_path), exist_ok=True)
        
        # Run ogr2ogr to convert KML to GeoJSON
        cmd = [
            'ogr2ogr',
            '-f', 'GeoJSON',
            temp_geojson_path,
            kml_file_path
        ]
        
        # Execute the command
        subprocess.run(cmd, check=True)
        
        # Read the generated GeoJSON file
        with open(temp_geojson_path, 'r') as f:
            geojson_data = json.load(f)
        
        # Clean up temporary file
        os.remove(temp_geojson_path)
        
        return geojson_data
    
    except Exception as e:
        print(f"Error converting KML to GeoJSON: {str(e)}")
        return None


def merge_geojson_files(geojson_list, output_path):
    """
    Merge multiple GeoJSON objects into a single GeoJSON file.
    
    Args:
        geojson_list: List of GeoJSON objects
        output_path: Path where to save the merged GeoJSON
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create a base FeatureCollection
        merged_geojson = {
            "type": "FeatureCollection",
            "features": []
        }
        
        # Add features from each GeoJSON object
        for geojson in geojson_list:
            if geojson and geojson.get("type") == "FeatureCollection":
                merged_geojson["features"].extend(geojson.get("features", []))
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write merged GeoJSON to file
        with open(output_path, 'w') as f:
            json.dump(merged_geojson, f)
        
        return True
    
    except Exception as e:
        print(f"Error merging GeoJSON files: {str(e)}")
        return False