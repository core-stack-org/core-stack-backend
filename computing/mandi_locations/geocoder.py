"""
Mandi Geocoder using Google Places API

This module geocodes mandi locations using the Google Places API,
converting mandi names and addresses to latitude/longitude coordinates.
"""

import time
import json
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

import pandas as pd
import googlemaps
from geopy.geocoders import GoogleV3
from shapely.geometry import Point
import geopandas as gpd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MandiGeocoder:
    """
    Geocode mandi locations using Google Places API
    """

    def __init__(self, api_key: str, output_dir: str = "data/geocoded"):
        """
        Initialize the geocoder

        Args:
            api_key: Google Places API key
            output_dir: Directory to save geocoded data
        """
        if not api_key:
            raise ValueError("Google API key is required")

        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Google Maps client
        self.gmaps = googlemaps.Client(key=api_key)
        self.geocoder = GoogleV3(api_key=api_key)

        # Cache for geocoded results
        self.geocode_cache = {}
        self.load_cache()

    def load_cache(self):
        """Load geocoding cache from file if exists"""
        cache_file = self.output_dir / "geocode_cache.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.geocode_cache = json.load(f)
                logger.info(f"Loaded {len(self.geocode_cache)} cached geocode results")
            except Exception as e:
                logger.warning(f"Could not load cache: {e}")

    def save_cache(self):
        """Save geocoding cache to file"""
        cache_file = self.output_dir / "geocode_cache.json"
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.geocode_cache, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(self.geocode_cache)} geocode results to cache")
        except Exception as e:
            logger.error(f"Could not save cache: {e}")

    def build_query(self, mandi: Dict) -> str:
        """
        Build geocoding query string from mandi data

        Args:
            mandi: Mandi dictionary

        Returns:
            Query string for geocoding
        """
        # Build hierarchical query
        components = []

        # Add mandi name
        mandi_name = mandi.get('mandi_name', '')
        if mandi_name:
            # Clean up mandi name
            mandi_name = mandi_name.replace('(', '').replace(')', '')
            components.append(f"{mandi_name} Mandi")

        # Add district
        district = mandi.get('district_name', '')
        if district:
            components.append(district)

        # Add state
        state = mandi.get('state_name', '')
        if state:
            components.append(state)

        # Add country
        components.append('India')

        return ', '.join(components)

    def geocode_with_places(self, query: str) -> Optional[Tuple[float, float]]:
        """
        Geocode using Google Places API

        Args:
            query: Search query

        Returns:
            Tuple of (latitude, longitude) or None
        """
        try:
            # Search for place
            places_result = self.gmaps.places(
                query=query,
                language='en',
                region='in'  # India region bias
            )

            if places_result['results']:
                place = places_result['results'][0]
                location = place['geometry']['location']
                return (location['lat'], location['lng'])

        except Exception as e:
            logger.warning(f"Places API error for '{query}': {e}")

        return None

    def geocode_with_geocoding(self, query: str) -> Optional[Tuple[float, float]]:
        """
        Geocode using Google Geocoding API

        Args:
            query: Address query

        Returns:
            Tuple of (latitude, longitude) or None
        """
        try:
            # Geocode address
            geocode_result = self.gmaps.geocode(
                address=query,
                region='in',  # India region bias
                language='en'
            )

            if geocode_result:
                location = geocode_result[0]['geometry']['location']
                return (location['lat'], location['lng'])

        except Exception as e:
            logger.warning(f"Geocoding API error for '{query}': {e}")

        return None

    def geocode_mandi(self, mandi: Dict) -> Dict:
        """
        Geocode a single mandi location

        Args:
            mandi: Mandi dictionary

        Returns:
            Mandi dictionary with added latitude and longitude
        """
        mandi_copy = mandi.copy()

        # Build query
        query = self.build_query(mandi)

        # Check cache
        if query in self.geocode_cache:
            cached = self.geocode_cache[query]
            mandi_copy['latitude'] = cached['lat']
            mandi_copy['longitude'] = cached['lng']
            mandi_copy['geocode_source'] = cached.get('source', 'cache')
            mandi_copy['geocode_query'] = query
            return mandi_copy

        coordinates = None

        # Try Places API first (more accurate for POIs)
        coordinates = self.geocode_with_places(query)
        source = 'places_api'

        # Fallback to Geocoding API
        if not coordinates:
            coordinates = self.geocode_with_geocoding(query)
            source = 'geocoding_api'

        # Try broader query if still no results
        if not coordinates:
            broader_query = f"{mandi['district_name']}, {mandi['state_name']}, India"
            coordinates = self.geocode_with_geocoding(broader_query)
            source = 'geocoding_api_broad'
            logger.warning(f"Using district center for {mandi['mandi_name']}")

        if coordinates:
            lat, lng = coordinates
            mandi_copy['latitude'] = lat
            mandi_copy['longitude'] = lng
            mandi_copy['geocode_source'] = source
            mandi_copy['geocode_query'] = query

            # Add to cache
            self.geocode_cache[query] = {
                'lat': lat,
                'lng': lng,
                'source': source
            }
        else:
            mandi_copy['latitude'] = None
            mandi_copy['longitude'] = None
            mandi_copy['geocode_source'] = 'failed'
            mandi_copy['geocode_query'] = query
            logger.error(f"Failed to geocode: {query}")

        return mandi_copy

    def geocode_batch(self, mandis: List[Dict],
                      batch_size: int = 50,
                      delay: float = 0.1) -> List[Dict]:
        """
        Geocode multiple mandis with rate limiting

        Args:
            mandis: List of mandi dictionaries
            batch_size: Number of mandis to geocode before saving
            delay: Delay between API calls (seconds)

        Returns:
            List of geocoded mandi dictionaries
        """
        geocoded = []
        total = len(mandis)

        logger.info(f"Starting geocoding for {total} mandis...")

        for i, mandi in enumerate(mandis, 1):
            # Geocode mandi
            geocoded_mandi = self.geocode_mandi(mandi)
            geocoded.append(geocoded_mandi)

            # Log progress
            if i % 10 == 0:
                logger.info(f"Geocoded {i}/{total} mandis...")

            # Save cache periodically
            if i % batch_size == 0:
                self.save_cache()

            # Rate limiting
            time.sleep(delay)

        # Save final cache
        self.save_cache()

        logger.info(f"Geocoding complete. Successfully geocoded "
                   f"{sum(1 for m in geocoded if m.get('latitude'))}/{total} mandis")

        return geocoded

    def validate_coordinates(self, mandi: Dict,
                            state_bounds: Optional[Dict] = None) -> bool:
        """
        Validate geocoded coordinates

        Args:
            mandi: Geocoded mandi dictionary
            state_bounds: Optional state boundary GeoDataFrame

        Returns:
            True if coordinates are valid
        """
        lat = mandi.get('latitude')
        lng = mandi.get('longitude')

        if lat is None or lng is None:
            return False

        # Basic India bounds check
        # India roughly extends from 8°N to 37°N and 68°E to 97°E
        if not (8 <= lat <= 37 and 68 <= lng <= 97):
            logger.warning(f"Coordinates outside India bounds: {mandi['mandi_name']} ({lat}, {lng})")
            return False

        # Check against state boundaries if provided
        if state_bounds:
            point = Point(lng, lat)
            state_name = mandi.get('state_name', '').lower()

            if state_name in state_bounds:
                if not state_bounds[state_name].contains(point).any():
                    logger.warning(f"Mandi {mandi['mandi_name']} outside state boundary")
                    return False

        return True

    def deduplicate_mandis(self, mandis: List[Dict],
                          distance_threshold: float = 0.001) -> List[Dict]:
        """
        Remove duplicate mandis based on coordinates

        Args:
            mandis: List of geocoded mandis
            distance_threshold: Distance threshold in degrees

        Returns:
            Deduplicated list of mandis
        """
        unique_mandis = []
        seen_coords = {}

        for mandi in mandis:
            lat = mandi.get('latitude')
            lng = mandi.get('longitude')

            if lat is None or lng is None:
                unique_mandis.append(mandi)
                continue

            # Check for nearby duplicates
            is_duplicate = False
            for (seen_lat, seen_lng), seen_mandi in seen_coords.items():
                distance = ((lat - seen_lat) ** 2 + (lng - seen_lng) ** 2) ** 0.5

                if distance < distance_threshold:
                    logger.info(f"Duplicate found: {mandi['mandi_name']} near {seen_mandi['mandi_name']}")
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_mandis.append(mandi)
                seen_coords[(lat, lng)] = mandi

        logger.info(f"Removed {len(mandis) - len(unique_mandis)} duplicate mandis")
        return unique_mandis

    def save_geocoded_data(self, mandis: List[Dict], filename: str = None):
        """
        Save geocoded mandi data

        Args:
            mandis: List of geocoded mandis
            filename: Output filename
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"geocoded_mandis_{timestamp}"

        # Save as CSV
        csv_path = self.output_dir / f"{filename}.csv"
        df = pd.DataFrame(mandis)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"Saved CSV to {csv_path}")

        # Save as JSON
        json_path = self.output_dir / f"{filename}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(mandis, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON to {json_path}")

        return csv_path, json_path

    def run(self, input_file: str) -> List[Dict]:
        """
        Run the complete geocoding process

        Args:
            input_file: Path to input file (CSV or JSON)

        Returns:
            List of geocoded mandis
        """
        # Load input data
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

        # Geocode mandis
        geocoded = self.geocode_batch(mandis)

        # Validate coordinates
        valid_mandis = []
        for mandi in geocoded:
            if self.validate_coordinates(mandi):
                valid_mandis.append(mandi)

        # Deduplicate
        unique_mandis = self.deduplicate_mandis(valid_mandis)

        # Save results
        self.save_geocoded_data(unique_mandis)

        return unique_mandis


if __name__ == "__main__":
    # Example usage
    api_key = "YOUR_GOOGLE_API_KEY"  # Replace with actual API key
    geocoder = MandiGeocoder(api_key=api_key)

    # Run geocoding on scraped data
    geocoded_mandis = geocoder.run("data/raw/mandi_data.csv")
    print(f"Geocoded {len(geocoded_mandis)} mandis")