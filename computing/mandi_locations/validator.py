"""
Mandi Data Validator

This module provides validation and quality checks for mandi location data
at various stages of the pipeline.
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, Polygon
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MandiValidator:
    """
    Validate mandi location data for quality and accuracy
    """

    def __init__(self, output_dir: str = "data/validation"):
        """
        Initialize the validator

        Args:
            output_dir: Directory to save validation reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # India bounding box
        self.india_bounds = {
            'min_lat': 8.0,
            'max_lat': 37.0,
            'min_lon': 68.0,
            'max_lon': 97.0
        }

        # State-wise approximate bounds (simplified)
        self.state_bounds = {
            'Maharashtra': {'min_lat': 15.6, 'max_lat': 22.0, 'min_lon': 72.6, 'max_lon': 80.9},
            'Gujarat': {'min_lat': 20.1, 'max_lat': 24.7, 'min_lon': 68.2, 'max_lon': 74.5},
            'Rajasthan': {'min_lat': 23.0, 'max_lat': 30.2, 'min_lon': 69.5, 'max_lon': 78.2},
            'Madhya Pradesh': {'min_lat': 21.0, 'max_lat': 26.9, 'min_lon': 74.0, 'max_lon': 82.8},
            'Uttar Pradesh': {'min_lat': 23.9, 'max_lat': 30.4, 'min_lon': 77.1, 'max_lon': 84.6},
            'Bihar': {'min_lat': 24.3, 'max_lat': 27.5, 'min_lon': 83.3, 'max_lon': 88.3},
            'West Bengal': {'min_lat': 21.5, 'max_lat': 27.2, 'min_lon': 85.8, 'max_lon': 89.9},
            'Karnataka': {'min_lat': 11.6, 'max_lat': 18.5, 'min_lon': 74.0, 'max_lon': 78.6},
            'Tamil Nadu': {'min_lat': 8.0, 'max_lat': 13.6, 'min_lon': 76.2, 'max_lon': 80.3},
            'Andhra Pradesh': {'min_lat': 12.6, 'max_lat': 19.1, 'min_lon': 76.7, 'max_lon': 84.8},
            'Telangana': {'min_lat': 15.8, 'max_lat': 19.9, 'min_lon': 77.2, 'max_lon': 81.3},
            'Punjab': {'min_lat': 29.5, 'max_lat': 32.5, 'min_lon': 73.9, 'max_lon': 77.0},
            'Haryana': {'min_lat': 27.7, 'max_lat': 30.9, 'min_lon': 74.3, 'max_lon': 77.6},
            'Kerala': {'min_lat': 8.2, 'max_lat': 12.8, 'min_lon': 74.9, 'max_lon': 77.4},
            'Odisha': {'min_lat': 17.8, 'max_lat': 22.7, 'min_lon': 81.3, 'max_lon': 87.5},
        }

    def validate_data_completeness(self, data: Union[pd.DataFrame, List[Dict]]) -> Dict:
        """
        Check data completeness and missing fields

        Args:
            data: Input data (DataFrame or list of dictionaries)

        Returns:
            Validation results dictionary
        """
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        results = {
            'total_records': len(df),
            'missing_values': {},
            'completeness_score': 0,
            'warnings': [],
            'errors': []
        }

        # Check required fields
        required_fields = ['mandi_name', 'state_name', 'district_name']
        for field in required_fields:
            if field not in df.columns:
                results['errors'].append(f"Required field '{field}' is missing")
            else:
                missing = df[field].isna().sum()
                if missing > 0:
                    results['missing_values'][field] = missing
                    results['warnings'].append(f"{missing} records missing '{field}'")

        # Check optional but important fields
        optional_fields = ['latitude', 'longitude', 'commodities', 'mandi_code']
        for field in optional_fields:
            if field in df.columns:
                missing = df[field].isna().sum()
                if missing > 0:
                    results['missing_values'][field] = missing

        # Calculate completeness score
        if df.shape[0] > 0:
            total_fields = len(df.columns)
            total_values = df.shape[0] * total_fields
            missing_values = df.isna().sum().sum()
            results['completeness_score'] = (1 - missing_values / total_values) * 100

        return results

    def validate_coordinates(self, data: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Dict:
        """
        Validate geographic coordinates

        Args:
            data: Input data with latitude/longitude

        Returns:
            Validation results dictionary
        """
        if isinstance(data, gpd.GeoDataFrame):
            df = data.copy()
        else:
            df = pd.DataFrame(data) if isinstance(data, list) else data.copy()

        results = {
            'total_points': len(df),
            'valid_coordinates': 0,
            'invalid_coordinates': 0,
            'outside_india': [],
            'outside_state': [],
            'coordinate_issues': []
        }

        # Check if coordinates exist
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            results['coordinate_issues'].append("Latitude/longitude columns missing")
            return results

        for idx, row in df.iterrows():
            lat = row.get('latitude')
            lon = row.get('longitude')

            # Check for missing coordinates
            if pd.isna(lat) or pd.isna(lon):
                results['invalid_coordinates'] += 1
                continue

            # Check India bounds
            if not (self.india_bounds['min_lat'] <= lat <= self.india_bounds['max_lat'] and
                   self.india_bounds['min_lon'] <= lon <= self.india_bounds['max_lon']):
                results['outside_india'].append({
                    'mandi': row.get('mandi_name', 'Unknown'),
                    'coordinates': (lat, lon)
                })
                results['invalid_coordinates'] += 1
                continue

            # Check state bounds if available
            state = row.get('state_name')
            if state and state in self.state_bounds:
                bounds = self.state_bounds[state]
                if not (bounds['min_lat'] <= lat <= bounds['max_lat'] and
                       bounds['min_lon'] <= lon <= bounds['max_lon']):
                    results['outside_state'].append({
                        'mandi': row.get('mandi_name', 'Unknown'),
                        'state': state,
                        'coordinates': (lat, lon)
                    })

            results['valid_coordinates'] += 1

        # Calculate percentage
        if results['total_points'] > 0:
            results['validity_percentage'] = (results['valid_coordinates'] / results['total_points']) * 100
        else:
            results['validity_percentage'] = 0

        return results

    def check_duplicates(self, data: Union[pd.DataFrame, List[Dict]],
                        distance_threshold: float = 0.001) -> Dict:
        """
        Check for duplicate mandis based on name and location

        Args:
            data: Input data
            distance_threshold: Distance threshold in degrees for location duplicates

        Returns:
            Validation results dictionary
        """
        df = pd.DataFrame(data) if isinstance(data, list) else data.copy()

        results = {
            'total_records': len(df),
            'name_duplicates': [],
            'location_duplicates': [],
            'exact_duplicates': []
        }

        # Check for exact duplicates (same name and district)
        if 'mandi_name' in df.columns and 'district_name' in df.columns:
            duplicates = df[df.duplicated(subset=['mandi_name', 'district_name'], keep=False)]
            if not duplicates.empty:
                for name, group in duplicates.groupby(['mandi_name', 'district_name']):
                    results['exact_duplicates'].append({
                        'mandi_name': name[0],
                        'district_name': name[1],
                        'count': len(group)
                    })

        # Check for name duplicates within same state
        if 'mandi_name' in df.columns and 'state_name' in df.columns:
            for state, state_df in df.groupby('state_name'):
                name_counts = state_df['mandi_name'].value_counts()
                duplicated_names = name_counts[name_counts > 1]
                for name, count in duplicated_names.items():
                    results['name_duplicates'].append({
                        'state': state,
                        'mandi_name': name,
                        'count': count
                    })

        # Check for location duplicates (very close coordinates)
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df.dropna(subset=['latitude', 'longitude'])

            if len(valid_coords) > 1:
                coords = valid_coords[['latitude', 'longitude']].values

                # Calculate pairwise distances
                distances = cdist(coords, coords)

                # Find close pairs (excluding diagonal)
                close_pairs = np.where((distances < distance_threshold) & (distances > 0))

                for i, j in zip(close_pairs[0], close_pairs[1]):
                    if i < j:  # Avoid duplicate pairs
                        results['location_duplicates'].append({
                            'mandi1': valid_coords.iloc[i]['mandi_name'] if 'mandi_name' in valid_coords.columns else 'Unknown',
                            'mandi2': valid_coords.iloc[j]['mandi_name'] if 'mandi_name' in valid_coords.columns else 'Unknown',
                            'distance': distances[i, j]
                        })

        return results

    def validate_distribution(self, data: Union[pd.DataFrame, gpd.GeoDataFrame]) -> Dict:
        """
        Analyze spatial distribution of mandis

        Args:
            data: Input data with coordinates

        Returns:
            Distribution analysis results
        """
        df = pd.DataFrame(data) if isinstance(data, list) else data.copy()

        results = {
            'state_distribution': {},
            'district_distribution': {},
            'spatial_statistics': {},
            'clustering_analysis': {}
        }

        # State-wise distribution
        if 'state_name' in df.columns:
            state_counts = df['state_name'].value_counts()
            results['state_distribution'] = state_counts.to_dict()

            # Check for states with very few mandis
            low_count_states = state_counts[state_counts < 10]
            if not low_count_states.empty:
                results['warnings'] = [f"Low mandi count in: {', '.join(low_count_states.index.tolist())}"]

        # District-wise distribution
        if 'district_name' in df.columns and 'state_name' in df.columns:
            district_counts = df.groupby(['state_name', 'district_name']).size()
            results['district_distribution'] = {
                'mean_per_district': district_counts.mean(),
                'median_per_district': district_counts.median(),
                'std_per_district': district_counts.std(),
                'districts_with_mandis': len(district_counts)
            }

        # Spatial statistics
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df.dropna(subset=['latitude', 'longitude'])

            if not valid_coords.empty:
                lats = valid_coords['latitude'].values
                lons = valid_coords['longitude'].values

                results['spatial_statistics'] = {
                    'centroid': (lats.mean(), lons.mean()),
                    'lat_range': (lats.min(), lats.max()),
                    'lon_range': (lons.min(), lons.max()),
                    'spatial_extent': {
                        'lat_span': lats.max() - lats.min(),
                        'lon_span': lons.max() - lons.min()
                    }
                }

                # Simple clustering check (grid-based)
                lat_bins = pd.cut(lats, bins=10)
                lon_bins = pd.cut(lons, bins=10)
                grid_counts = pd.crosstab(lat_bins, lon_bins)

                results['clustering_analysis'] = {
                    'empty_grid_cells': (grid_counts == 0).sum().sum(),
                    'max_mandis_per_cell': grid_counts.max().max(),
                    'mean_mandis_per_cell': grid_counts.mean().mean()
                }

        return results

    def generate_validation_report(self, all_results: Dict) -> str:
        """
        Generate comprehensive validation report

        Args:
            all_results: Dictionary containing all validation results

        Returns:
            Report string
        """
        report = []
        report.append("=" * 70)
        report.append("MANDI DATA VALIDATION REPORT")
        report.append("=" * 70)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Data Completeness
        if 'completeness' in all_results:
            comp = all_results['completeness']
            report.append("DATA COMPLETENESS:")
            report.append(f"  Total Records: {comp['total_records']}")
            report.append(f"  Completeness Score: {comp['completeness_score']:.2f}%")

            if comp['missing_values']:
                report.append("  Missing Values:")
                for field, count in comp['missing_values'].items():
                    report.append(f"    - {field}: {count}")

            if comp['errors']:
                report.append("  Errors:")
                for error in comp['errors']:
                    report.append(f"    ! {error}")

            if comp['warnings']:
                report.append("  Warnings:")
                for warning in comp['warnings']:
                    report.append(f"    ? {warning}")
            report.append("")

        # Coordinate Validation
        if 'coordinates' in all_results:
            coords = all_results['coordinates']
            report.append("COORDINATE VALIDATION:")
            report.append(f"  Total Points: {coords['total_points']}")
            report.append(f"  Valid Coordinates: {coords['valid_coordinates']}")
            report.append(f"  Invalid Coordinates: {coords['invalid_coordinates']}")
            report.append(f"  Validity Percentage: {coords.get('validity_percentage', 0):.2f}%")

            if coords['outside_india']:
                report.append(f"  Points Outside India: {len(coords['outside_india'])}")

            if coords['outside_state']:
                report.append(f"  Points Outside State Bounds: {len(coords['outside_state'])}")
            report.append("")

        # Duplicate Check
        if 'duplicates' in all_results:
            dups = all_results['duplicates']
            report.append("DUPLICATE ANALYSIS:")
            report.append(f"  Total Records: {dups['total_records']}")

            if dups['exact_duplicates']:
                report.append(f"  Exact Duplicates Found: {len(dups['exact_duplicates'])}")
                for dup in dups['exact_duplicates'][:5]:  # Show first 5
                    report.append(f"    - {dup['mandi_name']} in {dup['district_name']} ({dup['count']} times)")

            if dups['location_duplicates']:
                report.append(f"  Location Duplicates (nearby points): {len(dups['location_duplicates'])}")
            report.append("")

        # Distribution Analysis
        if 'distribution' in all_results:
            dist = all_results['distribution']
            report.append("SPATIAL DISTRIBUTION:")

            if dist['state_distribution']:
                report.append(f"  States Covered: {len(dist['state_distribution'])}")
                top_states = sorted(dist['state_distribution'].items(),
                                  key=lambda x: x[1], reverse=True)[:5]
                report.append("  Top 5 States by Mandi Count:")
                for state, count in top_states:
                    report.append(f"    - {state}: {count}")

            if dist.get('district_distribution'):
                dd = dist['district_distribution']
                report.append(f"  Districts with Mandis: {dd['districts_with_mandis']}")
                report.append(f"  Mean Mandis per District: {dd['mean_per_district']:.2f}")

            if dist.get('spatial_statistics'):
                stats = dist['spatial_statistics']
                report.append(f"  Geographic Centroid: {stats['centroid'][0]:.4f}, {stats['centroid'][1]:.4f}")
                report.append(f"  Spatial Extent: {stats['spatial_extent']['lat_span']:.2f}° x {stats['spatial_extent']['lon_span']:.2f}°")
            report.append("")

        # Overall Assessment
        report.append("OVERALL ASSESSMENT:")
        issues = 0

        if 'completeness' in all_results:
            if all_results['completeness']['completeness_score'] < 95:
                issues += 1
                report.append("  ⚠ Data completeness below 95%")

        if 'coordinates' in all_results:
            if all_results['coordinates'].get('validity_percentage', 0) < 95:
                issues += 1
                report.append("  ⚠ Coordinate validity below 95%")

        if 'duplicates' in all_results:
            if all_results['duplicates']['exact_duplicates']:
                issues += 1
                report.append("  ⚠ Exact duplicates found")

        if issues == 0:
            report.append("  ✓ All validation checks passed")
        else:
            report.append(f"  ⚠ {issues} issue(s) require attention")

        report.append("")
        report.append("=" * 70)

        return "\n".join(report)

    def create_validation_plots(self, data: Union[pd.DataFrame, gpd.GeoDataFrame],
                              save_path: str = None):
        """
        Create validation visualization plots

        Args:
            data: Input data
            save_path: Path to save plots
        """
        df = pd.DataFrame(data) if isinstance(data, list) else data.copy()

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))

        # Plot 1: State-wise distribution
        if 'state_name' in df.columns:
            state_counts = df['state_name'].value_counts().head(15)
            axes[0, 0].barh(state_counts.index, state_counts.values)
            axes[0, 0].set_xlabel('Number of Mandis')
            axes[0, 0].set_title('Top 15 States by Mandi Count')

        # Plot 2: Spatial scatter plot
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df.dropna(subset=['latitude', 'longitude'])
            axes[0, 1].scatter(valid_coords['longitude'], valid_coords['latitude'],
                             alpha=0.5, s=10)
            axes[0, 1].set_xlabel('Longitude')
            axes[0, 1].set_ylabel('Latitude')
            axes[0, 1].set_title('Spatial Distribution of Mandis')
            axes[0, 1].grid(True, alpha=0.3)

        # Plot 3: Missing data heatmap
        missing_data = df.isna().sum()
        missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
        if not missing_data.empty:
            axes[1, 0].barh(missing_data.index, missing_data.values)
            axes[1, 0].set_xlabel('Number of Missing Values')
            axes[1, 0].set_title('Missing Data by Field')

        # Plot 4: Geocoding source distribution
        if 'geocode_source' in df.columns:
            source_counts = df['geocode_source'].value_counts()
            axes[1, 1].pie(source_counts.values, labels=source_counts.index,
                          autopct='%1.1f%%')
            axes[1, 1].set_title('Geocoding Source Distribution')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"Validation plots saved to {save_path}")
        else:
            plt.show()

        plt.close()

    def run_full_validation(self, data_path: str) -> Dict:
        """
        Run complete validation pipeline

        Args:
            data_path: Path to data file

        Returns:
            Dictionary with all validation results
        """
        # Load data
        input_path = Path(data_path)

        if input_path.suffix == '.csv':
            df = pd.read_csv(input_path)
        elif input_path.suffix == '.json':
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data)
        elif input_path.suffix in ['.geojson', '.shp']:
            df = gpd.read_file(input_path)
        else:
            raise ValueError("Unsupported file format")

        logger.info(f"Loaded {len(df)} records from {data_path}")

        # Run all validation checks
        all_results = {
            'completeness': self.validate_data_completeness(df),
            'coordinates': self.validate_coordinates(df),
            'duplicates': self.check_duplicates(df),
            'distribution': self.validate_distribution(df)
        }

        # Generate report
        report = self.generate_validation_report(all_results)

        # Save report
        report_path = self.output_dir / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w') as f:
            f.write(report)

        logger.info(f"Validation report saved to {report_path}")
        print(report)

        # Create plots
        plot_path = self.output_dir / f"validation_plots_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        self.create_validation_plots(df, plot_path)

        return all_results


if __name__ == "__main__":
    # Example usage
    validator = MandiValidator()
    results = validator.run_full_validation("data/geocoded/geocoded_mandis.csv")
    print(f"Validation complete. Check results in: data/validation/")