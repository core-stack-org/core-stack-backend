"""
Main Pipeline for Mandi Locations Vector Map Generation

This script orchestrates the entire pipeline from scraping to GEE publishing.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, Optional

from .scraper import MandiScraper
from .geocoder import MandiGeocoder
from .vector_generator import VectorMapGenerator
from .gee_publisher import GEEPublisher
from .validator import MandiValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MandiLocationsPipeline:
    """
    Main pipeline orchestrator for mandi locations processing
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the pipeline

        Args:
            config_path: Path to configuration file
        """
        self.config = self.load_config(config_path)
        self.output_base = Path(self.config.get('output_dir', 'data'))
        self.output_base.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.scraper = None
        self.geocoder = None
        self.vector_generator = None
        self.gee_publisher = None
        self.validator = None

    def load_config(self, config_path: str = None) -> Dict:
        """
        Load configuration from file or environment

        Args:
            config_path: Path to config file

        Returns:
            Configuration dictionary
        """
        config = {
            'output_dir': 'data',
            'google_api_key': os.environ.get('GOOGLE_API_KEY', ''),
            'gee_project_id': os.environ.get('GEE_PROJECT_ID', ''),
            'gee_service_account': os.environ.get('GEE_SERVICE_ACCOUNT', ''),
            'headless_browser': True,
            'geocoding_batch_size': 50,
            'geocoding_delay': 0.1,
            'validation_enabled': True
        }

        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)

        return config

    def step1_scrape_mandis(self, force: bool = False) -> str:
        """
        Step 1: Scrape mandi data from Agmarknet

        Args:
            force: Force re-scraping even if data exists

        Returns:
            Path to scraped data file
        """
        logger.info("=" * 60)
        logger.info("STEP 1: SCRAPING MANDI DATA")
        logger.info("=" * 60)

        output_file = self.output_base / 'raw' / 'mandi_data.json'

        if output_file.exists() and not force:
            logger.info(f"Scraped data already exists at {output_file}")
            logger.info("Use --force-scrape to re-scrape")
            return str(output_file)

        self.scraper = MandiScraper(
            output_dir=str(self.output_base / 'raw'),
            headless=self.config['headless_browser']
        )

        mandis = self.scraper.run(save_format='both')

        logger.info(f"✓ Scraped {len(mandis)} mandis successfully")
        return str(output_file)

    def step2_geocode_mandis(self, input_file: str, force: bool = False) -> str:
        """
        Step 2: Geocode mandi locations

        Args:
            input_file: Path to scraped data
            force: Force re-geocoding

        Returns:
            Path to geocoded data file
        """
        logger.info("=" * 60)
        logger.info("STEP 2: GEOCODING MANDI LOCATIONS")
        logger.info("=" * 60)

        output_file = self.output_base / 'geocoded' / 'geocoded_mandis.json'

        if output_file.exists() and not force:
            logger.info(f"Geocoded data already exists at {output_file}")
            logger.info("Use --force-geocode to re-geocode")
            return str(output_file)

        if not self.config['google_api_key']:
            raise ValueError("Google API key not configured. Set GOOGLE_API_KEY environment variable.")

        self.geocoder = MandiGeocoder(
            api_key=self.config['google_api_key'],
            output_dir=str(self.output_base / 'geocoded')
        )

        geocoded = self.geocoder.run(input_file)

        logger.info(f"✓ Geocoded {len(geocoded)} mandis successfully")
        return str(output_file)

    def step3_generate_vector_map(self, input_file: str) -> Dict:
        """
        Step 3: Generate vector map from geocoded data

        Args:
            input_file: Path to geocoded data

        Returns:
            Dictionary with output paths
        """
        logger.info("=" * 60)
        logger.info("STEP 3: GENERATING VECTOR MAPS")
        logger.info("=" * 60)

        self.vector_generator = VectorMapGenerator(
            output_dir=str(self.output_base / 'vector')
        )

        results = self.vector_generator.run(
            input_file,
            export_formats=['geojson', 'shapefile', 'csv']
        )

        logger.info(f"✓ Generated vector maps successfully")
        for format_type, path in results['output_paths'].items():
            logger.info(f"  - {format_type}: {path}")

        return results

    def step4_validate_data(self, data_file: str) -> Dict:
        """
        Step 4: Validate the generated data

        Args:
            data_file: Path to data file to validate

        Returns:
            Validation results
        """
        logger.info("=" * 60)
        logger.info("STEP 4: VALIDATING DATA")
        logger.info("=" * 60)

        self.validator = MandiValidator(
            output_dir=str(self.output_base / 'validation')
        )

        results = self.validator.run_full_validation(data_file)

        # Check validation results
        issues = []
        if results['completeness']['completeness_score'] < 95:
            issues.append("Data completeness below 95%")
        if results['coordinates'].get('validity_percentage', 0) < 95:
            issues.append("Coordinate validity below 95%")
        if results['duplicates']['exact_duplicates']:
            issues.append(f"{len(results['duplicates']['exact_duplicates'])} exact duplicates found")

        if issues:
            logger.warning(f"⚠ Validation issues found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info(f"✓ All validation checks passed")

        return results

    def step5_publish_to_gee(self, vector_file: str) -> Dict:
        """
        Step 5: Publish vector map to Google Earth Engine

        Args:
            vector_file: Path to vector file (GeoJSON or Shapefile)

        Returns:
            Publishing results
        """
        logger.info("=" * 60)
        logger.info("STEP 5: PUBLISHING TO GOOGLE EARTH ENGINE")
        logger.info("=" * 60)

        if not self.config['gee_project_id']:
            logger.warning("GEE project ID not configured. Skipping GEE publishing.")
            logger.warning("Set GEE_PROJECT_ID environment variable to enable.")
            return {}

        self.gee_publisher = GEEPublisher(
            project_id=self.config['gee_project_id'],
            service_account_key=self.config.get('gee_service_account')
        )

        asset_name = f"mandi_locations_{datetime.now().strftime('%Y%m%d')}"
        results = self.gee_publisher.run(vector_file, asset_name)

        logger.info(f"✓ Published to Earth Engine: {results['asset_path']}")
        logger.info(f"  - JavaScript code: {results['js_script']}")
        logger.info(f"  - Python visualization: {results['py_script']}")

        return results

    def run_pipeline(self,
                    skip_scraping: bool = False,
                    skip_geocoding: bool = False,
                    skip_validation: bool = False,
                    skip_gee: bool = False,
                    force_scrape: bool = False,
                    force_geocode: bool = False) -> Dict:
        """
        Run the complete pipeline

        Args:
            skip_scraping: Skip scraping step
            skip_geocoding: Skip geocoding step
            skip_validation: Skip validation step
            skip_gee: Skip GEE publishing
            force_scrape: Force re-scraping
            force_geocode: Force re-geocoding

        Returns:
            Pipeline results
        """
        logger.info("🚀 STARTING MANDI LOCATIONS PIPELINE")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")

        results = {}

        try:
            # Step 1: Scraping
            if not skip_scraping:
                scraped_file = self.step1_scrape_mandis(force=force_scrape)
                results['scraped_data'] = scraped_file
            else:
                # Look for existing scraped data
                scraped_file = self.output_base / 'raw' / 'mandi_data.json'
                if not scraped_file.exists():
                    raise FileNotFoundError(f"No scraped data found. Run scraping first.")
                results['scraped_data'] = str(scraped_file)

            # Step 2: Geocoding
            if not skip_geocoding:
                geocoded_file = self.step2_geocode_mandis(
                    results['scraped_data'],
                    force=force_geocode
                )
                results['geocoded_data'] = geocoded_file
            else:
                # Look for existing geocoded data
                geocoded_file = self.output_base / 'geocoded' / 'geocoded_mandis.json'
                if not geocoded_file.exists():
                    raise FileNotFoundError(f"No geocoded data found. Run geocoding first.")
                results['geocoded_data'] = str(geocoded_file)

            # Step 3: Vector Generation
            vector_results = self.step3_generate_vector_map(results['geocoded_data'])
            results['vector_maps'] = vector_results['output_paths']
            results['statistics'] = vector_results['statistics']

            # Step 4: Validation
            if not skip_validation:
                validation_results = self.step4_validate_data(
                    results['vector_maps']['geojson']
                )
                results['validation'] = validation_results

            # Step 5: GEE Publishing
            if not skip_gee:
                gee_results = self.step5_publish_to_gee(
                    results['vector_maps']['geojson']
                )
                results['gee'] = gee_results

            logger.info("")
            logger.info("=" * 60)
            logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            # Summary
            logger.info("SUMMARY:")
            logger.info(f"  Total Mandis: {results['statistics']['total_mandis']}")
            logger.info(f"  States Covered: {results['statistics']['states_covered']}")
            logger.info(f"  Districts Covered: {results['statistics']['districts_covered']}")

            logger.info("")
            logger.info("OUTPUT FILES:")
            for key, path in results.get('vector_maps', {}).items():
                logger.info(f"  - {key}: {path}")

            if 'gee' in results and results['gee']:
                logger.info("")
                logger.info("EARTH ENGINE:")
                logger.info(f"  Asset Path: {results['gee']['asset_path']}")

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise

        return results


def main():
    """Main entry point for the pipeline"""
    parser = argparse.ArgumentParser(
        description='Mandi Locations Vector Map Generation Pipeline'
    )

    parser.add_argument(
        '--config',
        help='Path to configuration file',
        default='config.json'
    )

    parser.add_argument(
        '--skip-scraping',
        action='store_true',
        help='Skip the scraping step'
    )

    parser.add_argument(
        '--skip-geocoding',
        action='store_true',
        help='Skip the geocoding step'
    )

    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip the validation step'
    )

    parser.add_argument(
        '--skip-gee',
        action='store_true',
        help='Skip GEE publishing'
    )

    parser.add_argument(
        '--force-scrape',
        action='store_true',
        help='Force re-scraping even if data exists'
    )

    parser.add_argument(
        '--force-geocode',
        action='store_true',
        help='Force re-geocoding even if data exists'
    )

    args = parser.parse_args()

    # Run pipeline
    pipeline = MandiLocationsPipeline(config_path=args.config)
    results = pipeline.run_pipeline(
        skip_scraping=args.skip_scraping,
        skip_geocoding=args.skip_geocoding,
        skip_validation=args.skip_validation,
        skip_gee=args.skip_gee,
        force_scrape=args.force_scrape,
        force_geocode=args.force_geocode
    )

    # Save results
    results_file = pipeline.output_base / f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        # Convert paths to strings for JSON serialization
        json_results = {}
        for key, value in results.items():
            if isinstance(value, Path):
                json_results[key] = str(value)
            elif isinstance(value, dict):
                json_results[key] = {k: str(v) if isinstance(v, Path) else v
                                   for k, v in value.items()}
            else:
                json_results[key] = value
        json.dump(json_results, f, indent=2)

    logger.info(f"Results saved to: {results_file}")


if __name__ == "__main__":
    main()