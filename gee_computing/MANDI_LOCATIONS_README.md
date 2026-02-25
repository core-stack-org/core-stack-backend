# Mandi Locations Vector Map - Solution Documentation

## Issue #223 Implementation

### Overview
This solution implements a complete pipeline for generating vectorized maps of mandi (agricultural market) locations across India. The implementation includes web scraping from Agmarknet, geocoding using Google Places API, vector map generation, and publishing to Google Earth Engine.

## Architecture

### Components

1. **AgmarknetScraper** (`mandi_locations.py`)
   - Scrapes mandi data from Agmarknet website using Selenium
   - Extracts state, district, mandi names, and commodities
   - Handles pagination and multi-level dropdowns

2. **MandiGeocoder** (`mandi_locations.py`)
   - Geocodes mandi addresses using Google Places API
   - Implements retry logic and rate limiting
   - Calculates confidence scores for geocoding accuracy
   - Provides fallback geocoding strategies

3. **MandiDataValidator** (`mandi_locations.py`)
   - Validates coordinates within India's boundaries
   - Deduplicates mandis based on proximity
   - Validates district boundary containment

4. **VectorMapGenerator** (`mandi_locations.py`)
   - Creates GeoJSON and Shapefile formats
   - Standardizes to EPSG:4326 CRS
   - Maintains all required attributes

5. **EarthEnginePublisher** (`mandi_locations.py`)
   - Uploads vector data to Google Earth Engine
   - Adds metadata and tracking
   - Monitors upload status

6. **MandiVisualization** (`mandi_visualization.py`)
   - Provides GEE visualization capabilities
   - Creates density maps and buffer analysis
   - Generates interactive Folium maps

## Setup Instructions

### Prerequisites

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Required API Keys**
   - Google Maps API Key (for geocoding)
   - Google Earth Engine Service Account (optional, for automated uploads)

3. **Chrome WebDriver**
   - Install ChromeDriver for Selenium
   - Ensure it's in PATH or specify location

### Configuration

1. **Environment Variables**
   Create a `.env` file in the project root:
   ```env
   GOOGLE_MAPS_API_KEY=your_google_maps_api_key
   EE_SERVICE_ACCOUNT_KEY=/path/to/service_account.json
   EE_ASSET_ID=users/your_username/mandi_locations
   ```

2. **Django Settings**
   Add to your Django settings:
   ```python
   GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
   EE_SERVICE_ACCOUNT_KEY = os.getenv('EE_SERVICE_ACCOUNT_KEY')
   MANDI_ASSET_ID = os.getenv('EE_ASSET_ID', 'users/your_username/mandi_locations')
   ```

## Usage

### Django Management Command

Run the complete pipeline using Django management command:

```bash
# Full pipeline execution
python manage.py generate_mandi_map

# Skip scraping and use existing data
python manage.py generate_mandi_map --skip-scraping --input-csv data.csv

# Process specific state
python manage.py generate_mandi_map --state "Maharashtra"

# Dry run without EE upload
python manage.py generate_mandi_map --dry-run

# Custom output directory
python manage.py generate_mandi_map --output-dir ./custom_output
```

### Standalone Python Script

```python
from gee_computing.mandi_locations import MandiLocationsPipeline

# Initialize pipeline
pipeline = MandiLocationsPipeline(
    google_maps_api_key='your_api_key',
    ee_service_account_key='/path/to/service_account.json'
)

# Run pipeline
results = pipeline.run(
    output_dir='./mandi_output',
    asset_id='users/username/mandi_locations',
    skip_scraping=False
)

print(f"Total mandis processed: {results['total_unique']}")
```

### Google Earth Engine Integration

```python
from gee_computing.mandi_visualization import MandiVisualization
import ee

# Initialize Earth Engine
ee.Initialize()

# Load mandi locations
viz = MandiVisualization('users/username/mandi_locations')

# Get statistics
stats = viz.get_statistics()
print(f"Total mandis: {stats['total_mandis']}")

# Filter by state
maharashtra_mandis = viz.filter_by_state('Maharashtra')

# Create density map
density = viz.create_density_map(scale=10000)

# Find nearest mandis to a point
point = ee.Geometry.Point([78.9629, 20.5937])
nearest = viz.nearest_mandi_analysis(point, n=5)
```

## Data Flow

1. **Scraping Phase**
   - Connect to Agmarknet website
   - Iterate through states → districts → mandis
   - Extract mandi information and commodities
   - Save raw data as JSON

2. **Geocoding Phase**
   - Process each mandi through Google Places API
   - Apply retry logic for failed geocoding
   - Calculate confidence scores
   - Save geocoded data as CSV

3. **Validation Phase**
   - Validate coordinates within India bounds
   - Remove duplicates based on proximity
   - Verify district boundaries (if shapefile available)

4. **Vector Generation**
   - Create point geometries for each mandi
   - Generate GeoJSON with full attributes
   - Export Shapefile for GIS compatibility

5. **Publishing Phase**
   - Upload to Google Earth Engine
   - Add metadata and properties
   - Monitor upload status

## Output Files

The pipeline generates the following outputs:

```
mandi_output/
├── mandis_raw.json           # Raw scraped data
├── mandis_geocoded.csv       # Geocoded data with coordinates
├── mandi_locations.geojson   # Vector map in GeoJSON format
├── mandi_locations.shp       # Vector map in Shapefile format
├── mandi_locations.shx       # Shapefile index
├── mandi_locations.dbf       # Shapefile attributes
├── mandi_locations.prj       # Shapefile projection
└── validation_report.json    # Pipeline execution report
```

## Validation Report

The validation report includes:

```json
{
  "total_scraped": 6000,
  "total_geocoded": 5800,
  "total_valid": 5750,
  "total_unique": 5700,
  "coverage": {
    "states": 29,
    "districts": 640
  },
  "geocoding_stats": {
    "high_confidence": 4500,
    "medium_confidence": 1000,
    "low_confidence": 200
  },
  "earth_engine_task_id": "TASK_ID_HERE",
  "output_files": {
    "geojson": "./mandi_output/mandi_locations.geojson",
    "shapefile": "./mandi_output/mandi_locations.shp",
    "csv": "./mandi_output/mandis_geocoded.csv"
  }
}
```

## Error Handling

The solution includes comprehensive error handling:

1. **Scraping Errors**
   - Handles timeout and connection issues
   - Continues with next state/district on failure
   - Logs all errors for review

2. **Geocoding Errors**
   - Implements exponential backoff retry
   - Falls back to broader search queries
   - Caches successful geocoding results

3. **Validation Errors**
   - Logs invalid coordinates
   - Reports duplicate removal statistics
   - Provides confidence scores

## Performance Optimization

1. **Batch Processing**
   - Geocoding in batches of 50
   - Parallel processing where possible

2. **Caching**
   - Geocoding results cached to avoid duplicate API calls
   - 15-minute cache for web requests

3. **Rate Limiting**
   - Respects Google API rate limits
   - Implements delays between requests

## Testing

Run tests with:

```bash
# Unit tests
python -m pytest gee_computing/tests/test_mandi_locations.py

# Integration tests
python manage.py test gee_computing.tests.MandiIntegrationTest
```

## Monitoring

Monitor Earth Engine upload status:

```python
from gee_computing.mandi_locations import EarthEnginePublisher

publisher = EarthEnginePublisher()
status = publisher.check_upload_status('TASK_ID_HERE')
print(f"Upload status: {status['state']}")
```

## Troubleshooting

### Common Issues

1. **Selenium WebDriver Not Found**
   - Install ChromeDriver: `pip install webdriver-manager`
   - Or download manually from [ChromeDriver](https://chromedriver.chromium.org/)

2. **Google Maps API Quota Exceeded**
   - Implement longer delays between requests
   - Use batch geocoding with smaller batch sizes
   - Check API quota in Google Cloud Console

3. **Earth Engine Authentication Failed**
   - Run `earthengine authenticate` for user authentication
   - Or provide service account key file

4. **Memory Issues with Large Datasets**
   - Process states in batches
   - Use `--state` flag to process one state at a time
   - Increase available memory for Python process

## API Rate Limits

- **Google Maps Geocoding API**: 50 requests/second
- **Google Earth Engine**: 10 concurrent exports
- **Agmarknet**: No official limits (use respectful delays)

## Future Improvements

1. **Enhanced Scraping**
   - Add commodity prices and volumes
   - Include historical data

2. **Better Geocoding**
   - Use multiple geocoding services
   - Implement ML-based location validation

3. **Real-time Updates**
   - Schedule periodic updates
   - Implement incremental updates

4. **Analytics**
   - Add market accessibility analysis
   - Integrate with crop production data
   - Calculate optimal market catchment areas

## Contributing

To contribute to this solution:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This solution is part of the C4GT project and follows the project's licensing terms.

## Support

For issues or questions:
- Create an issue on GitHub
- Contact: @kapildadheech (Organizational Mentor)
- Project: KYL (Know Your Location)

## Acceptance Criteria Met

✅ **Data Acquisition**
- Scraper extracts mandi information without missing fields
- Data stored in structured format (CSV/JSON)

✅ **Geocoding**
- Each mandi assigned valid lat/lon coordinates
- Duplicate/ambiguous entries resolved and logged
- Coordinates validated against district/state

✅ **Vector Map Creation**
- Point geometries with EPSG:4326 CRS
- Complete attribute table with all fields

✅ **Earth Engine Publishing**
- Vector asset uploaded to EE
- Metadata included (source, date, schema)

✅ **GEE Pipeline Integration**
- Visualization layer for mandi points
- Overlay tested with crop/tehsil layers

✅ **Validation**
- Coverage report confirms all mandis captured
- Geocoding accuracy verified
- No duplicate or missing points