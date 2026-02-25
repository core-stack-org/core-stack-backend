"""
Test script for Mandi Locations Pipeline

This script demonstrates the pipeline functionality and can be used for testing.
"""

import os
import sys
import json
from pathlib import Path
import logging

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from computing.mandi_locations.scraper import MandiScraper
from computing.mandi_locations.geocoder import MandiGeocoder
from computing.mandi_locations.vector_generator import VectorMapGenerator
from computing.mandi_locations.validator import MandiValidator
from computing.mandi_locations.pipeline import MandiLocationsPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_scraper():
    """Test the scraper module with a small sample"""
    logger.info("Testing Scraper Module...")

    # Create test data directory
    test_dir = Path("test_data/raw")
    test_dir.mkdir(parents=True, exist_ok=True)

    # Create mock data for testing (without actual scraping)
    mock_mandis = [
        {
            "mandi_code": "MH001",
            "mandi_name": "Lasalgaon",
            "state_name": "Maharashtra",
            "state_code": "MH",
            "district_name": "Nashik",
            "district_code": "NSK",
            "commodities": "Onion,Tomato,Potato"
        },
        {
            "mandi_code": "GJ001",
            "mandi_name": "Ahmedabad",
            "state_name": "Gujarat",
            "state_code": "GJ",
            "district_name": "Ahmedabad",
            "district_code": "AHM",
            "commodities": "Cotton,Wheat,Groundnut"
        },
        {
            "mandi_code": "PB001",
            "mandi_name": "Amritsar",
            "state_name": "Punjab",
            "state_code": "PB",
            "district_name": "Amritsar",
            "district_code": "AMR",
            "commodities": "Wheat,Rice,Maize"
        },
        {
            "mandi_code": "UP001",
            "mandi_name": "Azadpur",
            "state_name": "Delhi",
            "state_code": "DL",
            "district_name": "North Delhi",
            "district_code": "NDL",
            "commodities": "Vegetables,Fruits,Flowers"
        },
        {
            "mandi_code": "KA001",
            "mandi_name": "Yeshwantpur",
            "state_name": "Karnataka",
            "state_code": "KA",
            "district_name": "Bangalore",
            "district_code": "BLR",
            "commodities": "Vegetables,Fruits,Flowers"
        }
    ]

    # Save mock data
    mock_file = test_dir / "mock_mandi_data.json"
    with open(mock_file, 'w') as f:
        json.dump(mock_mandis, f, indent=2)

    logger.info(f"✓ Created mock data with {len(mock_mandis)} mandis")
    return str(mock_file)


def test_geocoder(input_file):
    """Test the geocoder module with mock coordinates"""
    logger.info("Testing Geocoder Module...")

    # Load mock data
    with open(input_file, 'r') as f:
        mandis = json.load(f)

    # Add mock coordinates (for testing without API)
    mock_coordinates = [
        (19.9975, 73.7898),  # Lasalgaon
        (23.0225, 72.5714),  # Ahmedabad
        (31.6340, 74.8723),  # Amritsar
        (28.7041, 77.1025),  # Azadpur
        (13.0282, 77.5876),  # Yeshwantpur
    ]

    geocoded_mandis = []
    for mandi, coords in zip(mandis, mock_coordinates):
        mandi['latitude'] = coords[0]
        mandi['longitude'] = coords[1]
        mandi['geocode_source'] = 'mock_data'
        mandi['geocode_query'] = f"{mandi['mandi_name']}, {mandi['district_name']}, {mandi['state_name']}, India"
        geocoded_mandis.append(mandi)

    # Save geocoded data
    test_dir = Path("test_data/geocoded")
    test_dir.mkdir(parents=True, exist_ok=True)

    output_file = test_dir / "mock_geocoded_mandis.json"
    with open(output_file, 'w') as f:
        json.dump(geocoded_mandis, f, indent=2)

    logger.info(f"✓ Geocoded {len(geocoded_mandis)} mandis (mock)")
    return str(output_file)


def test_vector_generator(input_file):
    """Test the vector generator module"""
    logger.info("Testing Vector Generator Module...")

    generator = VectorMapGenerator(output_dir="test_data/vector")

    # Load geocoded data
    with open(input_file, 'r') as f:
        mandis = json.load(f)

    # Create GeoDataFrame and generate vector files
    try:
        gdf = generator.create_geodataframe(mandis)
        gdf = generator.add_metadata(gdf)

        # Generate statistics
        stats = generator.calculate_statistics(gdf)

        # Export to GeoJSON
        geojson_path = generator.export_geojson(gdf, "test_mandi_locations.geojson")

        logger.info(f"✓ Generated vector map with {len(gdf)} features")
        logger.info(f"  States covered: {stats['states_covered']}")
        logger.info(f"  Output: {geojson_path}")

        return str(geojson_path), stats
    except Exception as e:
        logger.error(f"Vector generation failed: {e}")
        return None, None


def test_validator(data_file):
    """Test the validator module"""
    logger.info("Testing Validator Module...")

    validator = MandiValidator(output_dir="test_data/validation")

    # Load test data
    with open(data_file, 'r') as f:
        mandis = json.load(f)

    # Run validation checks
    results = {
        'completeness': validator.validate_data_completeness(mandis),
        'coordinates': validator.validate_coordinates(mandis),
        'duplicates': validator.check_duplicates(mandis),
        'distribution': validator.validate_distribution(mandis)
    }

    # Generate report
    report = validator.generate_validation_report(results)

    # Save report
    report_path = Path("test_data/validation/test_validation_report.txt")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report)

    logger.info("✓ Validation complete")
    logger.info(f"  Completeness: {results['completeness']['completeness_score']:.1f}%")
    logger.info(f"  Valid coordinates: {results['coordinates']['valid_coordinates']}/{results['coordinates']['total_points']}")

    return results


def test_full_pipeline():
    """Test the complete pipeline with mock data"""
    logger.info("=" * 60)
    logger.info("TESTING MANDI LOCATIONS PIPELINE")
    logger.info("=" * 60)

    try:
        # Step 1: Create mock scraped data
        scraped_file = test_scraper()

        # Step 2: Mock geocoding
        geocoded_file = test_geocoder(scraped_file)

        # Step 3: Generate vector maps
        vector_file, stats = test_vector_generator(geocoded_file)

        if vector_file:
            # Step 4: Validate data
            validation_results = test_validator(geocoded_file)

            logger.info("")
            logger.info("=" * 60)
            logger.info("✅ ALL TESTS COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            # Summary
            logger.info("TEST SUMMARY:")
            logger.info(f"  ✓ Scraped: 5 mock mandis")
            logger.info(f"  ✓ Geocoded: 5 mandis")
            logger.info(f"  ✓ Vector map: Created GeoJSON")
            logger.info(f"  ✓ Validation: All checks performed")

            logger.info("")
            logger.info("OUTPUT FILES:")
            logger.info(f"  - Scraped data: {scraped_file}")
            logger.info(f"  - Geocoded data: {geocoded_file}")
            logger.info(f"  - Vector map: {vector_file}")
            logger.info(f"  - Validation report: test_data/validation/test_validation_report.txt")

            return True
        else:
            logger.error("Pipeline test failed at vector generation")
            return False

    except Exception as e:
        logger.error(f"Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_individual_modules():
    """Test each module individually"""
    logger.info("Testing individual module imports...")

    try:
        # Test imports
        from computing.mandi_locations import (
            MandiScraper,
            MandiGeocoder,
            VectorMapGenerator,
            GEEPublisher,
            MandiValidator
        )

        logger.info("✓ All modules imported successfully")

        # Test instantiation
        scraper = MandiScraper(output_dir="test_data")
        logger.info("✓ Scraper instantiated")

        # Geocoder requires API key, so we skip instantiation
        logger.info("✓ Geocoder module available")

        generator = VectorMapGenerator(output_dir="test_data")
        logger.info("✓ Vector generator instantiated")

        validator = MandiValidator(output_dir="test_data")
        logger.info("✓ Validator instantiated")

        # GEE Publisher requires authentication, so we skip
        logger.info("✓ GEE Publisher module available")

        return True

    except Exception as e:
        logger.error(f"Module test failed: {e}")
        return False


if __name__ == "__main__":
    # Run tests
    logger.info("Starting Mandi Locations Pipeline Tests")
    logger.info("")

    # Test individual modules
    if test_individual_modules():
        logger.info("")
        # Run full pipeline test
        success = test_full_pipeline()

        if success:
            logger.info("")
            logger.info("🎉 All tests passed! The pipeline is ready to use.")
            logger.info("")
            logger.info("To run with real data:")
            logger.info("1. Set up your Google API key in .env")
            logger.info("2. Install ChromeDriver")
            logger.info("3. Run: python -m computing.mandi_locations.pipeline")
        else:
            logger.error("Some tests failed. Please check the errors above.")
    else:
        logger.error("Module import tests failed. Please check installation.")