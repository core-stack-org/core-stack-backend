"""
Mandi Locations Visualization for Google Earth Engine
=====================================================
This module provides visualization capabilities for mandi locations
in the GEE pipeline.

Author: C4GT Team
Issue: #223
"""

import ee
import folium
from typing import Dict, List, Optional, Tuple
import json


class MandiVisualization:
    """
    Visualization class for mandi locations in Google Earth Engine
    """

    def __init__(self, asset_id: str):
        """
        Initialize visualization with mandi asset

        Args:
            asset_id: Earth Engine asset ID for mandi locations
        """
        ee.Initialize()
        self.asset_id = asset_id
        self.mandi_fc = ee.FeatureCollection(asset_id)

    def add_to_map(self, Map: ee.Map, vis_params: Optional[Dict] = None) -> ee.Map:
        """
        Add mandi locations to Earth Engine Map

        Args:
            Map: Earth Engine Map object
            vis_params: Visualization parameters

        Returns:
            Updated Map object
        """
        if vis_params is None:
            vis_params = {
                'color': 'red',
                'pointSize': 5,
                'width': 2
            }

        # Style the mandi points
        styled = self.mandi_fc.style(**vis_params)

        # Add layer to map
        Map.addLayer(styled, {}, 'Mandi Locations')

        # Center map on India
        Map.setCenter(78.9629, 20.5937, 5)

        return Map

    def filter_by_state(self, state: str) -> ee.FeatureCollection:
        """
        Filter mandis by state

        Args:
            state: State name

        Returns:
            Filtered FeatureCollection
        """
        return self.mandi_fc.filter(ee.Filter.eq('state', state))

    def filter_by_district(self, district: str) -> ee.FeatureCollection:
        """
        Filter mandis by district

        Args:
            district: District name

        Returns:
            Filtered FeatureCollection
        """
        return self.mandi_fc.filter(ee.Filter.eq('district', district))

    def filter_by_commodity(self, commodity: str) -> ee.FeatureCollection:
        """
        Filter mandis by commodity

        Args:
            commodity: Commodity name

        Returns:
            Filtered FeatureCollection
        """
        return self.mandi_fc.filter(ee.Filter.stringContains('commodities', commodity))

    def get_statistics(self) -> Dict:
        """
        Get statistics about mandi locations

        Returns:
            Dictionary with statistics
        """
        # Total count
        total = self.mandi_fc.size()

        # Unique states
        states = self.mandi_fc.aggregate_array('state').distinct()

        # Unique districts
        districts = self.mandi_fc.aggregate_array('district').distinct()

        stats = {
            'total_mandis': total.getInfo(),
            'total_states': states.size().getInfo(),
            'total_districts': districts.size().getInfo(),
            'states_list': states.getInfo(),
            'districts_list': districts.getInfo()
        }

        return stats

    def create_density_map(self, scale: int = 10000) -> ee.Image:
        """
        Create mandi density heatmap

        Args:
            scale: Resolution in meters

        Returns:
            Density image
        """
        # Convert points to image
        density = self.mandi_fc.reduceToImage(
            properties=['mandi_name'],
            reducer=ee.Reducer.count()
        ).reproject(crs='EPSG:4326', scale=scale)

        return density

    def buffer_analysis(self, radius_meters: int = 10000) -> ee.FeatureCollection:
        """
        Create buffer zones around mandis

        Args:
            radius_meters: Buffer radius in meters

        Returns:
            FeatureCollection with buffer zones
        """
        def add_buffer(feature):
            return feature.buffer(radius_meters)

        buffered = self.mandi_fc.map(add_buffer)
        return buffered

    def nearest_mandi_analysis(self, point: ee.Geometry.Point, n: int = 5) -> ee.FeatureCollection:
        """
        Find nearest mandis to a given point

        Args:
            point: Reference point
            n: Number of nearest mandis to find

        Returns:
            FeatureCollection of nearest mandis
        """
        # Add distance property to each mandi
        def add_distance(feature):
            distance = feature.geometry().distance(point)
            return feature.set('distance_meters', distance)

        with_distance = self.mandi_fc.map(add_distance)

        # Sort by distance and limit
        nearest = with_distance.sort('distance_meters').limit(n)

        return nearest

    def create_folium_map(self, center: Tuple[float, float] = (20.5937, 78.9629),
                         zoom: int = 5) -> folium.Map:
        """
        Create interactive Folium map with mandi locations

        Args:
            center: Map center (lat, lon)
            zoom: Initial zoom level

        Returns:
            Folium Map object
        """
        # Create base map
        m = folium.Map(location=center, zoom_start=zoom)

        # Get mandi features
        mandis = self.mandi_fc.getInfo()

        # Add each mandi as a marker
        for feature in mandis['features']:
            props = feature['properties']
            coords = feature['geometry']['coordinates']

            # Create popup text
            popup_text = f"""
            <b>{props.get('mandi_name', 'Unknown')}</b><br>
            State: {props.get('state', 'Unknown')}<br>
            District: {props.get('district', 'Unknown')}<br>
            Commodities: {props.get('commodities', 'Unknown')}<br>
            Confidence: {props.get('confidence', 0):.2f}
            """

            # Add marker
            folium.Marker(
                location=[coords[1], coords[0]],  # lat, lon
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=props.get('mandi_name', 'Unknown'),
                icon=folium.Icon(color='green', icon='shopping-basket', prefix='fa')
            ).add_to(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        return m

    def integrate_with_crop_data(self, crop_asset_id: str) -> ee.FeatureCollection:
        """
        Integrate mandi locations with crop production data

        Args:
            crop_asset_id: Earth Engine asset ID for crop data

        Returns:
            Integrated FeatureCollection
        """
        crop_data = ee.FeatureCollection(crop_asset_id)

        # Spatial join - find mandis within crop regions
        def add_crop_info(mandi):
            # Find intersecting crop regions
            intersecting = crop_data.filterBounds(mandi.geometry())

            # Get crop types
            crop_types = intersecting.aggregate_array('crop_type').distinct()

            return mandi.set({
                'nearby_crops': crop_types,
                'crop_regions_count': intersecting.size()
            })

        integrated = self.mandi_fc.map(add_crop_info)
        return integrated


class MandiGEEIntegration:
    """
    Integration class for mandi locations with existing GEE compute pipeline
    """

    def __init__(self, mandi_asset_id: str):
        """
        Initialize integration

        Args:
            mandi_asset_id: Earth Engine asset ID for mandi locations
        """
        ee.Initialize()
        self.mandi_asset_id = mandi_asset_id
        self.mandi_fc = ee.FeatureCollection(mandi_asset_id)

    def add_to_compute_pipeline(self, compute_instance):
        """
        Add mandi analysis methods to existing ComputeOnGEE instance

        Args:
            compute_instance: Instance of ComputeOnGEE class
        """
        # Add mandi_analysis method to the compute instance
        def mandi_analysis(self, mws_path: str, output_path: str):
            """
            Analyze mandi locations within MWS boundaries
            """
            # Load MWS boundaries
            mws = ee.FeatureCollection(mws_path)

            # Find mandis within MWS boundaries
            mandis_in_mws = self.mandi_fc.filterBounds(mws.geometry())

            # Calculate statistics
            stats = mandis_in_mws.reduceColumns(
                reducer=ee.Reducer.count(),
                selectors=['mandi_name']
            )

            # Group by district
            district_stats = mandis_in_mws.aggregate_histogram('district')

            # Export results
            task = ee.batch.Export.table.toAsset(
                collection=mandis_in_mws,
                description='mandi_mws_analysis',
                assetId=output_path
            )
            task.start()

            return {
                'total_mandis': stats.getInfo(),
                'district_distribution': district_stats.getInfo(),
                'task_id': task.id
            }

        # Bind the method to compute instance
        compute_instance.mandi_analysis = mandi_analysis.__get__(compute_instance)

        # Add mandi_accessibility method
        def mandi_accessibility(self, mws_path: str, max_distance_km: float = 50):
            """
            Calculate accessibility of mandis from MWS locations
            """
            mws = ee.FeatureCollection(mws_path)

            # Convert km to meters
            max_distance_m = max_distance_km * 1000

            # For each MWS, find accessible mandis
            def find_accessible_mandis(mws_feature):
                # Buffer MWS by max distance
                buffer = mws_feature.geometry().buffer(max_distance_m)

                # Find mandis within buffer
                accessible = self.mandi_fc.filterBounds(buffer)

                # Calculate distances
                def add_distance(mandi):
                    distance = mandi.geometry().distance(mws_feature.geometry())
                    return mandi.set('distance_from_mws', distance)

                accessible_with_distance = accessible.map(add_distance)

                return mws_feature.set({
                    'accessible_mandis': accessible_with_distance.size(),
                    'nearest_mandi_distance': accessible_with_distance.aggregate_min('distance_from_mws')
                })

            mws_with_accessibility = mws.map(find_accessible_mandis)

            return mws_with_accessibility

        compute_instance.mandi_accessibility = mandi_accessibility.__get__(compute_instance)

    def create_composite_analysis(self, precipitation_data: ee.Image,
                                 runoff_data: ee.Image) -> ee.Image:
        """
        Create composite analysis combining mandi locations with water data

        Args:
            precipitation_data: Precipitation image
            runoff_data: Runoff image

        Returns:
            Composite analysis image
        """
        # Sample precipitation and runoff at mandi locations
        sampled = precipitation_data.addBands(runoff_data).sampleRegions(
            collection=self.mandi_fc,
            properties=['mandi_name', 'district', 'state'],
            scale=1000
        )

        # Calculate statistics
        stats = sampled.reduceColumns(
            reducer=ee.Reducer.mean().combine(
                reducer2=ee.Reducer.stdDev(),
                sharedInputs=True
            ),
            selectors=['precipitation', 'runoff']
        )

        return stats


# Utility functions for integration with existing system
def update_compute_class():
    """
    Update the existing ComputeOnGEE class with mandi methods
    """
    code = '''
    def mandi_locations(self, mandi_asset_id: str, mws_path: str, out_path: str):
        """
        Analyze mandi locations within MWS boundaries

        Args:
            mandi_asset_id: Earth Engine asset ID for mandi locations
            mws_path: Path to MWS boundaries
            out_path: Output path for results
        """
        from .mandi_visualization import MandiGEEIntegration

        # Initialize mandi integration
        mandi_integration = MandiGEEIntegration(mandi_asset_id)

        # Load MWS boundaries
        mws = ee.FeatureCollection(mws_path)

        # Find mandis within MWS
        mandis = ee.FeatureCollection(mandi_asset_id)
        mandis_in_mws = mandis.filterBounds(mws.geometry())

        # Calculate statistics
        stats = mandis_in_mws.reduceRegions(
            collection=mws,
            reducer=ee.Reducer.count(),
            scale=1000
        )

        # Export to asset
        task = ee.batch.Export.table.toAsset(
            collection=stats,
            description='mandi_analysis',
            assetId=out_path
        )
        task.start()

        return task.id
    '''
    return code


if __name__ == "__main__":
    # Example usage
    mandi_asset = "users/your_username/mandi_locations"

    # Initialize visualization
    viz = MandiVisualization(mandi_asset)

    # Get statistics
    stats = viz.get_statistics()
    print(f"Total mandis: {stats['total_mandis']}")
    print(f"States covered: {stats['total_states']}")

    # Create density map
    density = viz.create_density_map()

    # Create folium map
    interactive_map = viz.create_folium_map()
    interactive_map.save("mandi_map.html")