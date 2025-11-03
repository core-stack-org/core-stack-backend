"""
Vector Map Generator for Mandi Locations

This module creates vector maps (GeoJSON/Shapefile) from geocoded mandi data.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import fiona
from pyproj import CRS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VectorMapGenerator:
    """
    Generate vector maps from geocoded mandi data
    """

    def __init__(self, output_dir: str = "data/vector"):
        """
        Initialize the vector map generator

        Args:
            output_dir: Directory to save vector files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Standard CRS for geographic data
        self.crs = CRS.from_epsg(4326)  # WGS84

    def create_geodataframe(self, mandis: List[Dict]) -> gpd.GeoDataFrame:
        """
        Create a GeoDataFrame from mandi data

        Args:
            mandis: List of geocoded mandi dictionaries

        Returns:
            GeoDataFrame with mandi points
        """
        # Filter out mandis without coordinates
        valid_mandis = [
            m for m in mandis
            if m.get('latitude') is not None and m.get('longitude') is not None
        ]

        if not valid_mandis:
            raise ValueError("No mandis with valid coordinates found")

        # Create DataFrame
        df = pd.DataFrame(valid_mandis)

        # Create Point geometries
        geometry = [
            Point(mandi['longitude'], mandi['latitude'])
            for mandi in valid_mandis
        ]

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=self.crs)

        # Clean up columns
        columns_to_keep = [
            'mandi_code', 'mandi_name', 'state_name', 'district_name',
            'commodities', 'latitude', 'longitude', 'geocode_source',
            'geometry'
        ]

        # Keep only existing columns
        columns_to_keep = [col for col in columns_to_keep if col in gdf.columns]
        gdf = gdf[columns_to_keep]

        logger.info(f"Created GeoDataFrame with {len(gdf)} mandi points")

        return gdf

    def add_metadata(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Add metadata attributes to the GeoDataFrame

        Args:
            gdf: Input GeoDataFrame

        Returns:
            GeoDataFrame with added metadata
        """
        # Add creation timestamp
        gdf['created_date'] = datetime.now().isoformat()

        # Add data source
        gdf['data_source'] = 'Agmarknet'

        # Add geocoding method
        gdf['geocoding_method'] = 'Google Places API'

        # Add unique ID if not present
        if 'unique_id' not in gdf.columns:
            gdf['unique_id'] = range(1, len(gdf) + 1)

        # Add state code for better filtering
        state_codes = {
            'Andhra Pradesh': 'AP',
            'Arunachal Pradesh': 'AR',
            'Assam': 'AS',
            'Bihar': 'BR',
            'Chhattisgarh': 'CG',
            'Goa': 'GA',
            'Gujarat': 'GJ',
            'Haryana': 'HR',
            'Himachal Pradesh': 'HP',
            'Jharkhand': 'JH',
            'Karnataka': 'KA',
            'Kerala': 'KL',
            'Madhya Pradesh': 'MP',
            'Maharashtra': 'MH',
            'Manipur': 'MN',
            'Meghalaya': 'ML',
            'Mizoram': 'MZ',
            'Nagaland': 'NL',
            'Odisha': 'OD',
            'Punjab': 'PB',
            'Rajasthan': 'RJ',
            'Sikkim': 'SK',
            'Tamil Nadu': 'TN',
            'Telangana': 'TS',
            'Tripura': 'TR',
            'Uttar Pradesh': 'UP',
            'Uttarakhand': 'UK',
            'West Bengal': 'WB',
            'Delhi': 'DL',
            'Jammu and Kashmir': 'JK',
            'Ladakh': 'LA',
            'Andaman and Nicobar Islands': 'AN',
            'Chandigarh': 'CH',
            'Dadra and Nagar Haveli and Daman and Diu': 'DN',
            'Lakshadweep': 'LD',
            'Puducherry': 'PY'
        }

        gdf['state_code'] = gdf['state_name'].map(state_codes)

        return gdf

    def calculate_statistics(self, gdf: gpd.GeoDataFrame) -> Dict:
        """
        Calculate statistics about the mandi distribution

        Args:
            gdf: Mandi GeoDataFrame

        Returns:
            Dictionary of statistics
        """
        stats = {
            'total_mandis': len(gdf),
            'states_covered': gdf['state_name'].nunique(),
            'districts_covered': gdf['district_name'].nunique(),
            'mandis_per_state': gdf.groupby('state_name').size().to_dict(),
            'mandis_per_district': gdf.groupby(['state_name', 'district_name']).size().to_dict(),
            'geocoding_sources': gdf['geocode_source'].value_counts().to_dict() if 'geocode_source' in gdf.columns else {},
            'bbox': {
                'min_lat': gdf['latitude'].min(),
                'max_lat': gdf['latitude'].max(),
                'min_lon': gdf['longitude'].min(),
                'max_lon': gdf['longitude'].max()
            }
        }

        # Calculate commodity statistics if available
        if 'commodities' in gdf.columns:
            all_commodities = []
            for commodities in gdf['commodities'].dropna():
                if commodities:
                    all_commodities.extend([c.strip() for c in str(commodities).split(',')])

            if all_commodities:
                commodity_counts = pd.Series(all_commodities).value_counts()
                stats['top_commodities'] = commodity_counts.head(10).to_dict()
                stats['total_unique_commodities'] = len(commodity_counts)

        return stats

    def create_buffer_zones(self, gdf: gpd.GeoDataFrame,
                           buffer_distances: List[float] = [0.1, 0.25, 0.5]) -> gpd.GeoDataFrame:
        """
        Create buffer zones around mandi points

        Args:
            gdf: Mandi GeoDataFrame
            buffer_distances: Buffer distances in degrees

        Returns:
            GeoDataFrame with buffer polygons
        """
        buffer_gdfs = []

        for distance in buffer_distances:
            buffer_gdf = gdf.copy()
            # Create buffers (in degrees for EPSG:4326)
            buffer_gdf['geometry'] = buffer_gdf.geometry.buffer(distance)
            buffer_gdf['buffer_distance'] = distance
            buffer_gdf['buffer_km'] = distance * 111  # Approximate km at equator
            buffer_gdfs.append(buffer_gdf)

        # Combine all buffers
        combined_buffers = pd.concat(buffer_gdfs, ignore_index=True)

        return combined_buffers

    def export_geojson(self, gdf: gpd.GeoDataFrame, filename: str = None) -> Path:
        """
        Export GeoDataFrame to GeoJSON format

        Args:
            gdf: GeoDataFrame to export
            filename: Output filename

        Returns:
            Path to output file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mandi_locations_{timestamp}.geojson"

        filepath = self.output_dir / filename

        # Convert datetime columns to string
        for col in gdf.columns:
            if gdf[col].dtype == 'datetime64[ns]':
                gdf[col] = gdf[col].astype(str)

        # Export to GeoJSON
        gdf.to_file(filepath, driver='GeoJSON')

        logger.info(f"Exported GeoJSON to {filepath}")
        return filepath

    def export_shapefile(self, gdf: gpd.GeoDataFrame, filename: str = None) -> Path:
        """
        Export GeoDataFrame to Shapefile format

        Args:
            gdf: GeoDataFrame to export
            filename: Output filename (without extension)

        Returns:
            Path to output directory
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mandi_locations_{timestamp}"

        # Create directory for shapefile components
        shapefile_dir = self.output_dir / filename
        shapefile_dir.mkdir(exist_ok=True)

        filepath = shapefile_dir / f"{filename}.shp"

        # Truncate column names for shapefile (max 10 characters)
        gdf_copy = gdf.copy()
        column_mapping = {}

        for col in gdf_copy.columns:
            if len(col) > 10:
                new_col = col[:10]
                # Ensure uniqueness
                counter = 1
                while new_col in column_mapping.values():
                    new_col = col[:8] + str(counter).zfill(2)
                    counter += 1
                column_mapping[col] = new_col

        if column_mapping:
            gdf_copy = gdf_copy.rename(columns=column_mapping)

            # Save column mapping for reference
            mapping_file = shapefile_dir / "column_mapping.json"
            with open(mapping_file, 'w') as f:
                json.dump(column_mapping, f, indent=2)

        # Convert datetime columns to string
        for col in gdf_copy.columns:
            if gdf_copy[col].dtype == 'datetime64[ns]':
                gdf_copy[col] = gdf_copy[col].astype(str)

        # Export to Shapefile
        gdf_copy.to_file(filepath, driver='ESRI Shapefile')

        logger.info(f"Exported Shapefile to {shapefile_dir}")
        return shapefile_dir

    def export_csv_with_wkt(self, gdf: gpd.GeoDataFrame, filename: str = None) -> Path:
        """
        Export GeoDataFrame to CSV with WKT geometry

        Args:
            gdf: GeoDataFrame to export
            filename: Output filename

        Returns:
            Path to output file
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mandi_locations_{timestamp}.csv"

        filepath = self.output_dir / filename

        # Convert geometry to WKT
        gdf_copy = gdf.copy()
        gdf_copy['geometry_wkt'] = gdf_copy.geometry.to_wkt()

        # Drop the geometry column and save as CSV
        df = pd.DataFrame(gdf_copy.drop(columns='geometry'))
        df.to_csv(filepath, index=False)

        logger.info(f"Exported CSV with WKT to {filepath}")
        return filepath

    def create_summary_report(self, gdf: gpd.GeoDataFrame, stats: Dict) -> str:
        """
        Create a summary report of the mandi vector data

        Args:
            gdf: Mandi GeoDataFrame
            stats: Statistics dictionary

        Returns:
            Report string
        """
        report = []
        report.append("=" * 60)
        report.append("MANDI LOCATIONS VECTOR MAP SUMMARY REPORT")
        report.append("=" * 60)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        report.append("OVERALL STATISTICS:")
        report.append(f"  Total Mandis: {stats['total_mandis']}")
        report.append(f"  States Covered: {stats['states_covered']}")
        report.append(f"  Districts Covered: {stats['districts_covered']}")
        report.append("")

        report.append("GEOGRAPHIC EXTENT:")
        bbox = stats['bbox']
        report.append(f"  Latitude Range: {bbox['min_lat']:.4f} to {bbox['max_lat']:.4f}")
        report.append(f"  Longitude Range: {bbox['min_lon']:.4f} to {bbox['max_lon']:.4f}")
        report.append("")

        report.append("TOP STATES BY MANDI COUNT:")
        sorted_states = sorted(stats['mandis_per_state'].items(),
                             key=lambda x: x[1], reverse=True)
        for state, count in sorted_states[:10]:
            report.append(f"  {state}: {count}")
        report.append("")

        if 'top_commodities' in stats:
            report.append("TOP COMMODITIES:")
            for commodity, count in list(stats['top_commodities'].items())[:10]:
                report.append(f"  {commodity}: {count}")
            report.append(f"  Total Unique Commodities: {stats['total_unique_commodities']}")
            report.append("")

        if stats.get('geocoding_sources'):
            report.append("GEOCODING SOURCES:")
            for source, count in stats['geocoding_sources'].items():
                report.append(f"  {source}: {count}")
            report.append("")

        report.append("DATA QUALITY:")
        report.append(f"  Mandis with coordinates: {stats['total_mandis']}")
        report.append(f"  Coverage percentage: 100.0%")  # Since we filter out invalid coords
        report.append("")

        report.append("=" * 60)

        return "\n".join(report)

    def run(self, input_file: str, export_formats: List[str] = ['geojson', 'shapefile']) -> Dict:
        """
        Run the complete vector map generation process

        Args:
            input_file: Path to geocoded mandi data
            export_formats: List of export formats

        Returns:
            Dictionary with output paths and statistics
        """
        # Load geocoded data
        input_path = Path(input_file)

        if input_path.suffix == '.csv':
            df = pd.read_csv(input_path)
            mandis = df.to_dict('records')
        elif input_path.suffix == '.json':
            with open(input_path, 'r', encoding='utf-8') as f:
                mandis = json.load(f)
        else:
            raise ValueError("Input file must be CSV or JSON")

        logger.info(f"Loaded {len(mandis)} mandis from {input_file}")

        # Create GeoDataFrame
        gdf = self.create_geodataframe(mandis)

        # Add metadata
        gdf = self.add_metadata(gdf)

        # Calculate statistics
        stats = self.calculate_statistics(gdf)

        # Export in requested formats
        output_paths = {}

        if 'geojson' in export_formats:
            output_paths['geojson'] = self.export_geojson(gdf)

        if 'shapefile' in export_formats:
            output_paths['shapefile'] = self.export_shapefile(gdf)

        if 'csv' in export_formats:
            output_paths['csv'] = self.export_csv_with_wkt(gdf)

        # Create and save summary report
        report = self.create_summary_report(gdf, stats)
        report_path = self.output_dir / "summary_report.txt"
        with open(report_path, 'w') as f:
            f.write(report)
        output_paths['report'] = report_path

        logger.info(f"Vector map generation complete")
        print(report)

        return {
            'output_paths': output_paths,
            'statistics': stats,
            'geodataframe': gdf
        }


if __name__ == "__main__":
    # Example usage
    generator = VectorMapGenerator()
    results = generator.run(
        "data/geocoded/geocoded_mandis.csv",
        export_formats=['geojson', 'shapefile', 'csv']
    )
    print(f"Generated vector maps: {results['output_paths']}")