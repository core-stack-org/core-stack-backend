
"""
Mandi Locations Vector Map Module
==================================
This module handles scraping, geocoding, and publishing mandi location data
for integration into the Google Earth Engine pipeline.

Author: C4GT Team
Issue: #223
"""

import os
import time
import json
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import ee
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import googlemaps
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class MandiInfo:
    """Data class for storing mandi information"""
    mandi_name: str
    state: str
    district: str
    commodities: List[str]
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geocoding_confidence: Optional[float] = None
    address: Optional[str] = None


class AgmarknetScraper:
    """
    Scraper for extracting mandi information from Agmarknet website
    """

    BASE_URL = "http://agmarknet.gov.in"
    MANDI_URL = f"{BASE_URL}/SearchCmmMkt.aspx"

    def __init__(self, headless: bool = True):
        """
        Initialize the scraper with Selenium WebDriver

        Args:
            headless: Run browser in headless mode
        """
        self.options = Options()
        if headless:
            self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.driver = None

    def __enter__(self):
        """Context manager entry"""
        self.driver = webdriver.Chrome(options=self.options)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.driver:
            self.driver.quit()

    def get_states(self) -> List[Dict[str, str]]:
        """
        Get list of all states from Agmarknet

        Returns:
            List of dictionaries containing state code and name
        """
        logger.info("Fetching states from Agmarknet")
        self.driver.get(self.MANDI_URL)

        # Wait for page to load
        wait = WebDriverWait(self.driver, 10)
        state_dropdown = wait.until(
            EC.presence_of_element_located((By.ID, "ddlState"))
        )

        state_select = Select(state_dropdown)
        states = []

        for option in state_select.options[1:]:  # Skip the first "Select" option
            states.append({
                'code': option.get_attribute('value'),
                'name': option.text
            })

        logger.info(f"Found {len(states)} states")
        return states

    def get_districts(self, state_code: str) -> List[Dict[str, str]]:
        """
        Get list of districts for a given state

        Args:
            state_code: State code from Agmarknet

        Returns:
            List of dictionaries containing district code and name
        """
        # Select state
        state_select = Select(self.driver.find_element(By.ID, "ddlState"))
        state_select.select_by_value(state_code)

        # Wait for districts to load
        time.sleep(2)

        district_dropdown = self.driver.find_element(By.ID, "ddlDistrict")
        district_select = Select(district_dropdown)
        districts = []

        for option in district_select.options[1:]:  # Skip "Select" option
            districts.append({
                'code': option.get_attribute('value'),
                'name': option.text
            })

        return districts

    def get_mandis(self, state_code: str, district_code: str) -> List[Dict[str, str]]:
        """
        Get list of mandis for a given district

        Args:
            state_code: State code from Agmarknet
            district_code: District code from Agmarknet

        Returns:
            List of dictionaries containing mandi information
        """
        # Select district
        district_select = Select(self.driver.find_element(By.ID, "ddlDistrict"))
        district_select.select_by_value(district_code)

        # Wait for mandis to load
        time.sleep(2)

        mandi_dropdown = self.driver.find_element(By.ID, "ddlMarket")
        mandi_select = Select(mandi_dropdown)
        mandis = []

        for option in mandi_select.options[1:]:  # Skip "Select" option
            mandis.append({
                'code': option.get_attribute('value'),
                'name': option.text
            })

        return mandis

    def get_commodities(self, mandi_code: str) -> List[str]:
        """
        Get list of commodities traded in a mandi

        Args:
            mandi_code: Mandi code from Agmarknet

        Returns:
            List of commodity names
        """
        # This would require navigating to the commodity page
        # For now, returning a placeholder
        # In production, this would scrape actual commodity data
        return ["Wheat", "Rice", "Maize", "Pulses"]  # Placeholder

    def scrape_all_mandis(self) -> List[MandiInfo]:
        """
        Scrape all mandi information from Agmarknet

        Returns:
            List of MandiInfo objects
        """
        all_mandis = []

        states = self.get_states()

        for state in tqdm(states, desc="Scraping states"):
            logger.info(f"Processing state: {state['name']}")

            try:
                districts = self.get_districts(state['code'])

                for district in tqdm(districts, desc=f"Districts in {state['name']}", leave=False):
                    try:
                        mandis = self.get_mandis(state['code'], district['code'])

                        for mandi in mandis:
                            commodities = self.get_commodities(mandi['code'])

                            mandi_info = MandiInfo(
                                mandi_name=mandi['name'],
                                state=state['name'],
                                district=district['name'],
                                commodities=commodities
                            )
                            all_mandis.append(mandi_info)

                    except Exception as e:
                        logger.error(f"Error processing district {district['name']}: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error processing state {state['name']}: {e}")
                continue

        logger.info(f"Total mandis scraped: {len(all_mandis)}")
        return all_mandis


class MandiGeocoder:
    """
    Geocoder for converting mandi names and locations to coordinates
    """

    def __init__(self, api_key: str):
        """
        Initialize geocoder with Google Maps API key

        Args:
            api_key: Google Maps API key
        """
        self.gmaps = googlemaps.Client(key=api_key)
        self.cache = {}  # Simple cache to avoid duplicate API calls

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def geocode_mandi(self, mandi: MandiInfo) -> MandiInfo:
        """
        Geocode a single mandi location

        Args:
            mandi: MandiInfo object to geocode

        Returns:
            MandiInfo object with latitude and longitude filled
        """
        # Build search query
        query = f"{mandi.mandi_name} mandi, {mandi.district}, {mandi.state}, India"

        # Check cache first
        if query in self.cache:
            cached_result = self.cache[query]
            mandi.latitude = cached_result['lat']
            mandi.longitude = cached_result['lng']
            mandi.geocoding_confidence = cached_result['confidence']
            mandi.address = cached_result['address']
            return mandi

        try:
            # Try exact match first
            result = self.gmaps.geocode(query)

            if not result:
                # Try broader search
                query = f"mandi {mandi.district}, {mandi.state}, India"
                result = self.gmaps.geocode(query)

            if result:
                location = result[0]['geometry']['location']
                mandi.latitude = location['lat']
                mandi.longitude = location['lng']
                mandi.address = result[0]['formatted_address']

                # Calculate confidence based on address components match
                confidence = self._calculate_confidence(result[0], mandi)
                mandi.geocoding_confidence = confidence

                # Cache the result
                self.cache[query] = {
                    'lat': location['lat'],
                    'lng': location['lng'],
                    'confidence': confidence,
                    'address': mandi.address
                }

                logger.info(f"Geocoded: {mandi.mandi_name} - ({mandi.latitude}, {mandi.longitude})")
            else:
                logger.warning(f"Could not geocode: {query}")

        except Exception as e:
            logger.error(f"Error geocoding {mandi.mandi_name}: {e}")

        # Rate limiting
        time.sleep(0.2)  # Respect API rate limits

        return mandi

    def _calculate_confidence(self, geocode_result: Dict, mandi: MandiInfo) -> float:
        """
        Calculate geocoding confidence score

        Args:
            geocode_result: Google Maps geocoding result
            mandi: Original mandi information

        Returns:
            Confidence score between 0 and 1
        """
        confidence = 0.5  # Base confidence

        # Check if district name is in the result
        if mandi.district.lower() in geocode_result['formatted_address'].lower():
            confidence += 0.2

        # Check if state name is in the result
        if mandi.state.lower() in geocode_result['formatted_address'].lower():
            confidence += 0.2

        # Check location type
        if geocode_result['geometry'].get('location_type') == 'ROOFTOP':
            confidence += 0.1

        return min(confidence, 1.0)

    def batch_geocode(self, mandis: List[MandiInfo], batch_size: int = 50) -> List[MandiInfo]:
        """
        Geocode multiple mandis with batching

        Args:
            mandis: List of MandiInfo objects to geocode
            batch_size: Number of mandis to geocode in each batch

        Returns:
            List of geocoded MandiInfo objects
        """
        geocoded_mandis = []

        for i in tqdm(range(0, len(mandis), batch_size), desc="Geocoding batches"):
            batch = mandis[i:i + batch_size]

            for mandi in batch:
                geocoded_mandi = self.geocode_mandi(mandi)
                geocoded_mandis.append(geocoded_mandi)

        return geocoded_mandis


class MandiDataValidator:
    """
    Validator for mandi data quality and deduplication
    """

    @staticmethod
    def validate_coordinates(lat: float, lon: float) -> bool:
        """
        Validate if coordinates are within India's bounds

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            True if coordinates are valid
        """
        # India's approximate bounds
        return (8.0 <= lat <= 37.0) and (68.0 <= lon <= 97.5)

    @staticmethod
    def deduplicate_mandis(mandis: List[MandiInfo], threshold: float = 0.001) -> List[MandiInfo]:
        """
        Remove duplicate mandis based on location proximity

        Args:
            mandis: List of MandiInfo objects
            threshold: Distance threshold in degrees for considering duplicates

        Returns:
            Deduplicated list of mandis
        """
        unique_mandis = []

        for mandi in mandis:
            if mandi.latitude is None or mandi.longitude is None:
                continue

            # Check if this mandi is too close to any existing unique mandi
            is_duplicate = False

            for unique_mandi in unique_mandis:
                if unique_mandi.latitude is None or unique_mandi.longitude is None:
                    continue

                distance = ((mandi.latitude - unique_mandi.latitude) ** 2 +
                           (mandi.longitude - unique_mandi.longitude) ** 2) ** 0.5

                if distance < threshold:
                    # Keep the one with higher confidence
                    if mandi.geocoding_confidence > unique_mandi.geocoding_confidence:
                        unique_mandis.remove(unique_mandi)
                        unique_mandis.append(mandi)
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_mandis.append(mandi)

        logger.info(f"Deduplication: {len(mandis)} -> {len(unique_mandis)} mandis")
        return unique_mandis

    @staticmethod
    def validate_district_boundary(mandi: MandiInfo, district_shapefile: Optional[gpd.GeoDataFrame] = None) -> bool:
        """
        Validate if mandi falls within expected district boundary

        Args:
            mandi: MandiInfo object to validate
            district_shapefile: GeoDataFrame with district boundaries

        Returns:
            True if mandi is within district boundary
        """
        if district_shapefile is None or mandi.latitude is None:
            return True  # Skip validation if no shapefile provided

        point = Point(mandi.longitude, mandi.latitude)

        # Find the district in the shapefile
        district_match = district_shapefile[
            district_shapefile['district_name'].str.lower() == mandi.district.lower()
        ]

        if not district_match.empty:
            return district_match.geometry.contains(point).any()

        return True  # Default to valid if district not found


class VectorMapGenerator:
    """
    Generator for creating vector maps from mandi data
    """

    @staticmethod
    def create_geojson(mandis: List[MandiInfo], output_path: str) -> gpd.GeoDataFrame:
        """
        Create GeoJSON file from mandi data

        Args:
            mandis: List of MandiInfo objects
            output_path: Path to save GeoJSON file

        Returns:
            GeoDataFrame with mandi points
        """
        # Filter out mandis without coordinates
        valid_mandis = [m for m in mandis if m.latitude is not None and m.longitude is not None]

        # Create DataFrame
        data = []
        for mandi in valid_mandis:
            data.append({
                'mandi_name': mandi.mandi_name,
                'state': mandi.state,
                'district': mandi.district,
                'commodities': ', '.join(mandi.commodities),
                'latitude': mandi.latitude,
                'longitude': mandi.longitude,
                'confidence': mandi.geocoding_confidence,
                'address': mandi.address
            })

        df = pd.DataFrame(data)

        # Create geometry column
        geometry = [Point(row['longitude'], row['latitude']) for _, row in df.iterrows()]

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

        # Save to GeoJSON
        gdf.to_file(output_path, driver='GeoJSON')
        logger.info(f"GeoJSON saved to: {output_path}")

        return gdf

    @staticmethod
    def create_shapefile(mandis: List[MandiInfo], output_path: str) -> gpd.GeoDataFrame:
        """
        Create Shapefile from mandi data

        Args:
            mandis: List of MandiInfo objects
            output_path: Path to save Shapefile

        Returns:
            GeoDataFrame with mandi points
        """
        # Filter out mandis without coordinates
        valid_mandis = [m for m in mandis if m.latitude is not None and m.longitude is not None]

        # Create DataFrame
        data = []
        for mandi in valid_mandis:
            # Shapefile has field name limitations (10 chars)
            data.append({
                'mandi_name': mandi.mandi_name[:50],  # Truncate for shapefile
                'state': mandi.state[:30],
                'district': mandi.district[:30],
                'commodities': ', '.join(mandi.commodities)[:100],
                'lat': mandi.latitude,
                'lon': mandi.longitude,
                'confidence': mandi.geocoding_confidence,
                'address': mandi.address[:100] if mandi.address else ''
            })

        df = pd.DataFrame(data)

        # Create geometry column
        geometry = [Point(row['lon'], row['lat']) for _, row in df.iterrows()]

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

        # Save to Shapefile
        gdf.to_file(output_path, driver='ESRI Shapefile')
        logger.info(f"Shapefile saved to: {output_path}")

        return gdf


class EarthEnginePublisher:
    """
    Publisher for uploading mandi vector data to Google Earth Engine
    """

    def __init__(self, service_account_key: Optional[str] = None):
        """
        Initialize Earth Engine

        Args:
            service_account_key: Path to service account key file
        """
        if service_account_key:
            credentials = ee.ServiceAccountCredentials(
                email=None,
                key_file=service_account_key
            )
            ee.Initialize(credentials)
        else:
            ee.Initialize()

    def upload_vector_asset(self, geojson_path: str, asset_id: str, metadata: Dict = None) -> str:
        """
        Upload GeoJSON as Earth Engine vector asset

        Args:
            geojson_path: Path to GeoJSON file
            asset_id: Earth Engine asset ID (e.g., 'users/username/mandi_locations')
            metadata: Additional metadata for the asset

        Returns:
            Task ID for monitoring upload progress
        """
        # Read GeoJSON
        with open(geojson_path, 'r') as f:
            geojson = json.load(f)

        # Convert to Earth Engine FeatureCollection
        features = []
        for feature in geojson['features']:
            ee_feature = ee.Feature(
                ee.Geometry(feature['geometry']),
                feature['properties']
            )
            features.append(ee_feature)

        fc = ee.FeatureCollection(features)

        # Prepare metadata
        if metadata is None:
            metadata = {}

        metadata.update({
            'source': 'Agmarknet',
            'geocoding': 'Google Places API',
            'created': datetime.now().isoformat(),
            'total_mandis': len(features)
        })

        # Export to asset
        task = ee.batch.Export.table.toAsset(
            collection=fc,
            description='mandi_locations_upload',
            assetId=asset_id,
            properties=metadata
        )

        task.start()
        logger.info(f"Upload task started. Task ID: {task.id}")

        return task.id

    def check_upload_status(self, task_id: str) -> Dict:
        """
        Check status of upload task

        Args:
            task_id: Earth Engine task ID

        Returns:
            Task status information
        """
        tasks = ee.batch.Task.list()

        for task in tasks:
            if task.id == task_id:
                return {
                    'state': task.state,
                    'error_message': task.status().get('error_message', None)
                }

        return {'state': 'NOT_FOUND'}


class MandiLocationsPipeline:
    """
    Main pipeline for processing mandi locations
    """

    def __init__(self, google_maps_api_key: str, ee_service_account_key: Optional[str] = None):
        """
        Initialize pipeline

        Args:
            google_maps_api_key: Google Maps API key for geocoding
            ee_service_account_key: Earth Engine service account key file path
        """
        self.geocoder = MandiGeocoder(google_maps_api_key)
        self.validator = MandiDataValidator()
        self.map_generator = VectorMapGenerator()
        self.ee_publisher = EarthEnginePublisher(ee_service_account_key)

    def run(self,
            output_dir: str = './output',
            asset_id: str = 'users/your_username/mandi_locations',
            skip_scraping: bool = False,
            input_csv: Optional[str] = None) -> Dict:
        """
        Run the complete pipeline

        Args:
            output_dir: Directory to save output files
            asset_id: Earth Engine asset ID for upload
            skip_scraping: Skip scraping and use existing data
            input_csv: Path to existing mandi data CSV

        Returns:
            Dictionary with pipeline results
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Step 1: Scraping
        if not skip_scraping:
            logger.info("Starting Agmarknet scraping...")
            with AgmarknetScraper() as scraper:
                mandis = scraper.scrape_all_mandis()

            # Save raw data
            raw_data_path = os.path.join(output_dir, 'mandis_raw.json')
            with open(raw_data_path, 'w') as f:
                json.dump([asdict(m) for m in mandis], f, indent=2)
            logger.info(f"Raw data saved to: {raw_data_path}")
        else:
            # Load existing data
            if input_csv:
                df = pd.read_csv(input_csv)
                mandis = [MandiInfo(**row) for _, row in df.iterrows()]
            else:
                raise ValueError("Either enable scraping or provide input_csv")

        # Step 2: Geocoding
        logger.info("Starting geocoding...")
        geocoded_mandis = self.geocoder.batch_geocode(mandis)

        # Save geocoded data
        geocoded_data_path = os.path.join(output_dir, 'mandis_geocoded.csv')
        df = pd.DataFrame([asdict(m) for m in geocoded_mandis])
        df.to_csv(geocoded_data_path, index=False)
        logger.info(f"Geocoded data saved to: {geocoded_data_path}")

        # Step 3: Validation and Deduplication
        logger.info("Validating and deduplicating...")

        # Filter valid coordinates
        valid_mandis = []
        for mandi in geocoded_mandis:
            if (mandi.latitude is not None and
                mandi.longitude is not None and
                self.validator.validate_coordinates(mandi.latitude, mandi.longitude)):
                valid_mandis.append(mandi)

        # Deduplicate
        unique_mandis = self.validator.deduplicate_mandis(valid_mandis)

        # Step 4: Vector Map Generation
        logger.info("Generating vector maps...")
        geojson_path = os.path.join(output_dir, 'mandi_locations.geojson')
        shapefile_path = os.path.join(output_dir, 'mandi_locations.shp')

        gdf = self.map_generator.create_geojson(unique_mandis, geojson_path)
        self.map_generator.create_shapefile(unique_mandis, shapefile_path)

        # Step 5: Earth Engine Publishing
        logger.info("Publishing to Earth Engine...")
        task_id = self.ee_publisher.upload_vector_asset(
            geojson_path,
            asset_id,
            metadata={
                'total_mandis': len(unique_mandis),
                'states_covered': len(set(m.state for m in unique_mandis)),
                'districts_covered': len(set(m.district for m in unique_mandis))
            }
        )

        # Generate validation report
        report = {
            'total_scraped': len(mandis) if not skip_scraping else 0,
            'total_geocoded': len(geocoded_mandis),
            'total_valid': len(valid_mandis),
            'total_unique': len(unique_mandis),
            'coverage': {
                'states': len(set(m.state for m in unique_mandis)),
                'districts': len(set(m.district for m in unique_mandis))
            },
            'geocoding_stats': {
                'high_confidence': len([m for m in unique_mandis if m.geocoding_confidence > 0.8]),
                'medium_confidence': len([m for m in unique_mandis if 0.5 <= m.geocoding_confidence <= 0.8]),
                'low_confidence': len([m for m in unique_mandis if m.geocoding_confidence < 0.5])
            },
            'earth_engine_task_id': task_id,
            'output_files': {
                'geojson': geojson_path,
                'shapefile': shapefile_path,
                'csv': geocoded_data_path
            }
        }

        # Save report
        report_path = os.path.join(output_dir, 'validation_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Validation report saved to: {report_path}")

        return report


def main():
    """
    Main entry point for the mandi locations pipeline
    """
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    # Configuration
    GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
    EE_SERVICE_ACCOUNT_KEY = os.getenv('EE_SERVICE_ACCOUNT_KEY')
    EE_ASSET_ID = os.getenv('EE_ASSET_ID', 'users/your_username/mandi_locations')

    if not GOOGLE_MAPS_API_KEY:
        raise ValueError("GOOGLE_MAPS_API_KEY not found in environment variables")

    # Initialize and run pipeline
    pipeline = MandiLocationsPipeline(
        google_maps_api_key=GOOGLE_MAPS_API_KEY,
        ee_service_account_key=EE_SERVICE_ACCOUNT_KEY
    )

    results = pipeline.run(
        output_dir='./mandi_output',
        asset_id=EE_ASSET_ID,
        skip_scraping=False  # Set to True if you have existing data
    )

    print("\nPipeline completed successfully!")
    print(f"Total unique mandis: {results['total_unique']}")
    print(f"States covered: {results['coverage']['states']}")
    print(f"Districts covered: {results['coverage']['districts']}")
    print(f"Earth Engine Task ID: {results['earth_engine_task_id']}")


if __name__ == "__main__":
    main()