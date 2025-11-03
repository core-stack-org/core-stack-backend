"""
Google Earth Engine Publisher for Mandi Vector Data

This module publishes mandi vector maps to Google Earth Engine as assets
and provides visualization capabilities.
"""

import ee
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import mapping

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GEEPublisher:
    """
    Publish mandi vector data to Google Earth Engine
    """

    def __init__(self, project_id: str = None, service_account_key: str = None):
        """
        Initialize GEE publisher

        Args:
            project_id: GEE project ID
            service_account_key: Path to service account key file (optional)
        """
        self.project_id = project_id

        # Initialize Earth Engine
        try:
            if service_account_key:
                # Use service account for authentication
                credentials = ee.ServiceAccountCredentials(
                    email=None,
                    key_file=service_account_key
                )
                ee.Initialize(credentials)
            else:
                # Use default authentication
                ee.Authenticate()
                ee.Initialize(project=project_id)

            logger.info("Earth Engine initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {e}")
            raise

        # Set default asset paths
        if project_id:
            self.asset_base_path = f"projects/{project_id}/assets"
        else:
            self.asset_base_path = "users/default/assets"

    def geodataframe_to_features(self, gdf: gpd.GeoDataFrame) -> List[ee.Feature]:
        """
        Convert GeoDataFrame to Earth Engine Features

        Args:
            gdf: Input GeoDataFrame

        Returns:
            List of ee.Feature objects
        """
        features = []

        for idx, row in gdf.iterrows():
            # Convert geometry to GeoJSON
            geom = mapping(row['geometry'])

            # Prepare properties (excluding geometry)
            properties = {}
            for col in gdf.columns:
                if col != 'geometry':
                    value = row[col]
                    # Convert to appropriate type for EE
                    if pd.isna(value):
                        continue
                    elif isinstance(value, (int, float)):
                        properties[col] = float(value)
                    else:
                        properties[col] = str(value)

            # Create EE Feature
            feature = ee.Feature(ee.Geometry(geom), properties)
            features.append(feature)

        logger.info(f"Converted {len(features)} features to Earth Engine format")
        return features

    def create_feature_collection(self, features: List[ee.Feature],
                                 metadata: Dict = None) -> ee.FeatureCollection:
        """
        Create an Earth Engine FeatureCollection

        Args:
            features: List of ee.Feature objects
            metadata: Optional metadata dictionary

        Returns:
            ee.FeatureCollection
        """
        # Create FeatureCollection
        fc = ee.FeatureCollection(features)

        # Add metadata if provided
        if metadata:
            for key, value in metadata.items():
                fc = fc.set(key, value)

        # Add default metadata
        fc = fc.set('created_date', datetime.now().isoformat())
        fc = fc.set('source', 'Agmarknet')
        fc = fc.set('geocoding', 'Google Places API')
        fc = fc.set('feature_count', len(features))

        logger.info(f"Created FeatureCollection with {len(features)} features")
        return fc

    def upload_asset(self, feature_collection: ee.FeatureCollection,
                    asset_id: str, description: str = None) -> str:
        """
        Upload FeatureCollection as an Earth Engine asset

        Args:
            feature_collection: FeatureCollection to upload
            asset_id: Asset ID (name)
            description: Asset description

        Returns:
            Full asset path
        """
        # Construct full asset path
        if not asset_id.startswith('projects/'):
            asset_path = f"{self.asset_base_path}/mandi_locations/{asset_id}"
        else:
            asset_path = asset_id

        try:
            # Check if asset already exists
            try:
                ee.data.getAsset(asset_path)
                logger.warning(f"Asset {asset_path} already exists. Deleting...")
                ee.data.deleteAsset(asset_path)
                time.sleep(2)  # Wait for deletion
            except ee.EEException:
                pass  # Asset doesn't exist

            # Create export task
            task = ee.batch.Export.table.toAsset(
                collection=feature_collection,
                description=description or f"Mandi locations upload {datetime.now().strftime('%Y%m%d')}",
                assetId=asset_path
            )

            # Start the task
            task.start()
            logger.info(f"Upload task started for {asset_path}")

            # Monitor task progress
            while task.active():
                logger.info(f"Task status: {task.status()}")
                time.sleep(10)

            # Check final status
            if task.status()['state'] == 'COMPLETED':
                logger.info(f"Asset successfully uploaded to {asset_path}")
                return asset_path
            else:
                raise Exception(f"Upload failed: {task.status()}")

        except Exception as e:
            logger.error(f"Failed to upload asset: {e}")
            raise

    def create_visualization_params(self) -> Dict:
        """
        Create visualization parameters for mandi points

        Returns:
            Dictionary of visualization parameters
        """
        vis_params = {
            'color': 'FF0000',  # Red color for points
            'pointSize': 3,
            'width': 1,
            'fillColor': 'FF000080',  # Semi-transparent red fill
        }

        return vis_params

    def generate_gee_script(self, asset_path: str) -> str:
        """
        Generate Earth Engine JavaScript code for visualization

        Args:
            asset_path: Path to the uploaded asset

        Returns:
            JavaScript code string
        """
        script = f'''
// Mandi Locations Visualization Script
// Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

// Load the mandi locations asset
var mandiLocations = ee.FeatureCollection('{asset_path}');

// Print feature collection info
print('Total mandis:', mandiLocations.size());
print('First mandi:', mandiLocations.first());

// Get unique states
var states = mandiLocations.aggregate_array('state_name').distinct();
print('States covered:', states);

// Visualization parameters
var visParams = {{
  color: 'red',
  pointSize: 3,
  width: 1
}};

// Add to map
Map.centerObject(mandiLocations, 5);
Map.addLayer(mandiLocations, visParams, 'Mandi Locations');

// Create a styled layer with different colors per state
var stateColors = ee.Dictionary({{
  'Maharashtra': 'FF0000',
  'Gujarat': '00FF00',
  'Rajasthan': '0000FF',
  'Madhya Pradesh': 'FFFF00',
  'Uttar Pradesh': 'FF00FF',
  'Karnataka': '00FFFF',
  'Tamil Nadu': 'FFA500',
  'Andhra Pradesh': '800080',
  'Telangana': '008080',
  'Bihar': '808000',
  'West Bengal': '000080',
  'Punjab': 'FF1493',
  'Haryana': '00CED1',
  'Kerala': 'FFD700',
  'Odisha': 'FF6347'
}});

// Function to style features by state
var styleByState = function(feature) {{
  var state = feature.get('state_name');
  var color = ee.Algorithms.If(
    stateColors.contains(state),
    stateColors.get(state),
    '808080'  // Default gray for other states
  );

  return feature.set('style', {{
    pointSize: 3,
    color: color,
    width: 1
  }});
}};

// Apply styling
var styledMandis = mandiLocations.map(styleByState);

// Add styled layer
Map.addLayer(styledMandis.style({{styleProperty: 'style'}}), {{}}, 'Mandis by State');

// Create charts
// 1. Mandis per state
var mandisByState = ui.Chart.feature.histogram(
  mandiLocations, 'state_name', 10
).setOptions({{
  title: 'Mandis per State',
  hAxis: {{title: 'State'}},
  vAxis: {{title: 'Count'}},
  bar: {{groupWidth: '80%'}}
}});

print(mandisByState);

// 2. Create heatmap
var heatmap = mandiLocations.style({{
  color: 'FF0000',
  width: 0,
  fillColor: 'FF0000',
  pointSize: 10,
  pointShape: 'circle'
}});

Map.addLayer(heatmap, {{}}, 'Mandi Heatmap', false);

// Add legend
var legend = ui.Panel({{
  style: {{
    position: 'bottom-right',
    padding: '8px 15px'
  }}
}});

legend.add(ui.Label({{
  value: 'Mandi Locations',
  style: {{
    fontWeight: 'bold',
    fontSize: '16px',
    margin: '0 0 4px 0'
  }}
}});

legend.add(ui.Label('Red dots indicate mandi locations'));

Map.add(legend);

// Export options
Export.table.toDrive({{
  collection: mandiLocations,
  description: 'mandi_locations_export',
  fileFormat: 'CSV'
}});
'''
        return script

    def validate_asset(self, asset_path: str) -> bool:
        """
        Validate uploaded asset

        Args:
            asset_path: Path to the asset

        Returns:
            True if asset is valid
        """
        try:
            # Load the asset
            fc = ee.FeatureCollection(asset_path)

            # Get basic info
            size = fc.size()
            first = fc.first()

            # Check if we can get info
            info = {
                'feature_count': size.getInfo(),
                'first_feature': first.getInfo()
            }

            logger.info(f"Asset validation successful. Features: {info['feature_count']}")
            return True

        except Exception as e:
            logger.error(f"Asset validation failed: {e}")
            return False

    def create_visualization_app(self, asset_path: str, output_file: str = None):
        """
        Create a Python script for visualizing the asset

        Args:
            asset_path: Path to the EE asset
            output_file: Output file for the script
        """
        script = f'''#!/usr/bin/env python3
"""
Mandi Locations Visualization with Google Earth Engine
Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

import ee
import folium
import geemap

# Initialize Earth Engine
ee.Initialize()

# Load the mandi locations
mandis = ee.FeatureCollection('{asset_path}')

# Get collection info
total_mandis = mandis.size()
print(f"Total mandis: {{total_mandis.getInfo()}}")

# Create interactive map using geemap
Map = geemap.Map()

# Center the map on India
Map.setCenter(78.9629, 20.5937, 5)

# Add mandi locations layer
vis_params = {{
    'color': 'red',
    'pointSize': 3
}}

Map.addLayer(mandis, vis_params, 'Mandi Locations')

# Add state boundaries if available
states = ee.FeatureCollection("FAO/GAUL/2015/level1").filter(
    ee.Filter.eq('ADM0_NAME', 'India')
)
Map.addLayer(states, {{'color': 'black', 'width': 1}}, 'State Boundaries')

# Create cluster map for better visualization at different zoom levels
def create_cluster_map(mandis):
    """Create a clustered visualization of mandis"""

    # Convert to list of features
    features = mandis.getInfo()['features']

    # Create folium map
    m = folium.Map(location=[20.5937, 78.9629], zoom_start=5)

    # Add marker cluster
    from folium.plugins import MarkerCluster
    marker_cluster = MarkerCluster().add_to(m)

    # Add markers
    for feature in features:
        coords = feature['geometry']['coordinates']
        props = feature['properties']

        popup_text = f"""
        <b>{{props.get('mandi_name', 'Unknown')}}</b><br>
        State: {{props.get('state_name', 'N/A')}}<br>
        District: {{props.get('district_name', 'N/A')}}<br>
        Commodities: {{props.get('commodities', 'N/A')}}
        """

        folium.Marker(
            location=[coords[1], coords[0]],
            popup=popup_text,
            tooltip=props.get('mandi_name', 'Mandi')
        ).add_to(marker_cluster)

    return m

# Display the map
Map.show()

# Optional: Create and save clustered map
# cluster_map = create_cluster_map(mandis)
# cluster_map.save('mandi_clusters.html')
'''

        if output_file:
            output_path = Path(output_file)
            output_path.write_text(script)
            logger.info(f"Visualization script saved to {output_path}")

        return script

    def run(self, input_file: str, asset_name: str = None) -> Dict:
        """
        Run the complete GEE publishing process

        Args:
            input_file: Path to vector data file
            asset_name: Name for the EE asset

        Returns:
            Dictionary with asset path and scripts
        """
        # Load vector data
        input_path = Path(input_file)

        if input_path.suffix == '.geojson':
            gdf = gpd.read_file(input_path)
        elif input_path.suffix == '.shp':
            gdf = gpd.read_file(input_path)
        else:
            raise ValueError("Input must be GeoJSON or Shapefile")

        logger.info(f"Loaded {len(gdf)} features from {input_file}")

        # Convert to EE features
        features = self.geodataframe_to_features(gdf)

        # Create FeatureCollection
        metadata = {
            'upload_date': datetime.now().isoformat(),
            'source_file': input_file,
            'total_mandis': len(features)
        }
        fc = self.create_feature_collection(features, metadata)

        # Generate asset name if not provided
        if not asset_name:
            asset_name = f"mandi_locations_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Upload to Earth Engine
        asset_path = self.upload_asset(fc, asset_name)

        # Validate upload
        if not self.validate_asset(asset_path):
            raise Exception("Asset validation failed")

        # Generate scripts
        js_script = self.generate_gee_script(asset_path)
        py_script = self.create_visualization_app(asset_path)

        # Save scripts
        output_dir = Path("data/gee_scripts")
        output_dir.mkdir(parents=True, exist_ok=True)

        js_path = output_dir / f"{asset_name}.js"
        js_path.write_text(js_script)

        py_path = output_dir / f"{asset_name}_viz.py"
        py_path.write_text(py_script)

        results = {
            'asset_path': asset_path,
            'js_script': js_path,
            'py_script': py_path,
            'feature_count': len(features)
        }

        logger.info(f"GEE publishing complete. Asset: {asset_path}")
        return results


if __name__ == "__main__":
    # Example usage
    publisher = GEEPublisher(project_id="your-project-id")

    results = publisher.run(
        "data/vector/mandi_locations.geojson",
        asset_name="mandi_locations_india"
    )

    print(f"Published to: {results['asset_path']}")
    print(f"Scripts saved: {results['js_script']}, {results['py_script']}")