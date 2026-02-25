# Mandi Locations Vector Map Generation

## Overview

This module provides a comprehensive pipeline for generating vectorized maps of mandi (agricultural market) locations across India. It scrapes data from Agmarknet, geocodes the locations using Google Places API, generates vector maps in multiple formats, and publishes them to Google Earth Engine for integration with GEE pipelines.

## Features

- **Web Scraping**: Automated scraping of mandi data from Agmarknet website
- **Geocoding**: Accurate geocoding using Google Places API with fallback mechanisms
- **Vector Map Generation**: Creates GeoJSON, Shapefile, and CSV outputs
- **Data Validation**: Comprehensive validation and quality checks
- **Earth Engine Integration**: Direct publishing to Google Earth Engine
- **Visualization**: Interactive maps and analysis tools

## Architecture

```
┌─────────────┐     ┌──────────┐     ┌────────────────┐     ┌──────────────┐
│  Agmarknet  │────▶│ Scraper  │────▶│    Geocoder    │────▶│   Vector     │
│   Website   │     │  Module  │     │  (Google API)  │     │  Generator   │
└─────────────┘     └──────────┘     └────────────────┘     └──────────────┘
                                                                     │
                                                                     ▼
┌─────────────┐     ┌──────────┐     ┌────────────────┐     ┌──────────────┐
│     GEE     │◀────│   GEE    │◀────│   Validator    │◀────│   Vector     │
│   Assets    │     │Publisher │     │    Module      │     │    Maps      │
└─────────────┘     └──────────┘     └────────────────┘     └──────────────┘
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Google Chrome browser (for web scraping)
- ChromeDriver (matching your Chrome version)
- Google Cloud account with Places API enabled
- Google Earth Engine account (optional, for GEE publishing)

### Step 1: Install Dependencies

```bash
# Navigate to the module directory
cd core-stack-backend/computing/mandi_locations

# Install Python dependencies
pip install -r requirements.txt
```

### Step 2: Download ChromeDriver

1. Check your Chrome version: chrome://settings/help
2. Download matching ChromeDriver from: https://chromedriver.chromium.org/
3. Add ChromeDriver to your system PATH or set CHROMEDRIVER_PATH in .env

### Step 3: Configure API Keys

1. Copy the environment template:
```bash
cp .env.template .env
```

2. Edit `.env` and add your credentials:
```
GOOGLE_API_KEY=your_google_api_key_here
GEE_PROJECT_ID=your_gee_project_id
```

### Step 4: Initialize Earth Engine (Optional)

If you plan to publish to Earth Engine:

```bash
earthengine authenticate
```

## Usage

### Quick Start

Run the complete pipeline with default settings:

```bash
python -m mandi_locations.pipeline
```

### Command Line Options

```bash
python -m mandi_locations.pipeline [OPTIONS]

Options:
  --config PATH           Path to configuration file (default: config.json)
  --skip-scraping        Skip the scraping step (use existing data)
  --skip-geocoding       Skip the geocoding step
  --skip-validation      Skip data validation
  --skip-gee            Skip Google Earth Engine publishing
  --force-scrape        Force re-scraping even if data exists
  --force-geocode       Force re-geocoding even if data exists
```

### Examples

1. **Full Pipeline Execution**:
```bash
python -m mandi_locations.pipeline
```

2. **Skip Scraping (Use Existing Data)**:
```bash
python -m mandi_locations.pipeline --skip-scraping
```

3. **Force Re-geocoding**:
```bash
python -m mandi_locations.pipeline --skip-scraping --force-geocode
```

4. **Custom Configuration**:
```bash
python -m mandi_locations.pipeline --config my_config.json
```

### Using Individual Modules

You can also use modules independently:

```python
# Scraping only
from mandi_locations.scraper import MandiScraper

scraper = MandiScraper(output_dir="data/raw")
mandis = scraper.run(save_format='both')

# Geocoding only
from mandi_locations.geocoder import MandiGeocoder

geocoder = MandiGeocoder(api_key="YOUR_API_KEY")
geocoded = geocoder.run("data/raw/mandi_data.csv")

# Vector generation only
from mandi_locations.vector_generator import VectorMapGenerator

generator = VectorMapGenerator()
results = generator.run("data/geocoded/geocoded_mandis.csv")
```

## Configuration

Create a `config.json` file (see `config.json.template`):

```json
{
  "output_dir": "data",
  "google_api_key": "YOUR_API_KEY",
  "gee_project_id": "YOUR_PROJECT",
  "headless_browser": true,
  "geocoding_batch_size": 50,
  "geocoding_delay": 0.1,
  "export_formats": ["geojson", "shapefile", "csv"]
}
```

## Output Structure

```
data/
├── raw/
│   ├── mandi_data.json
│   └── mandi_data.csv
├── geocoded/
│   ├── geocoded_mandis.json
│   ├── geocoded_mandis.csv
│   └── geocode_cache.json
├── vector/
│   ├── mandi_locations_YYYYMMDD.geojson
│   ├── mandi_locations_YYYYMMDD/
│   │   ├── mandi_locations.shp
│   │   ├── mandi_locations.shx
│   │   ├── mandi_locations.dbf
│   │   └── mandi_locations.prj
│   └── summary_report.txt
├── validation/
│   ├── validation_report_YYYYMMDD.txt
│   └── validation_plots_YYYYMMDD.png
└── gee_scripts/
    ├── mandi_locations_YYYYMMDD.js
    └── mandi_locations_YYYYMMDD_viz.py
```

## Data Schema

### Output Fields

| Field | Type | Description |
|-------|------|-------------|
| mandi_code | String | Unique identifier from Agmarknet |
| mandi_name | String | Name of the mandi |
| state_name | String | State where mandi is located |
| state_code | String | Two-letter state code |
| district_name | String | District where mandi is located |
| commodities | String | Comma-separated list of commodities |
| latitude | Float | Geocoded latitude |
| longitude | Float | Geocoded longitude |
| geocode_source | String | Source of geocoding (places_api, geocoding_api) |
| created_date | String | Timestamp of data creation |

## Validation Checks

The validator performs the following checks:

1. **Data Completeness**: Missing values in required fields
2. **Coordinate Validation**:
   - Points within India bounds
   - Points within state bounds
   - Valid coordinate ranges
3. **Duplicate Detection**:
   - Name duplicates
   - Location duplicates (proximity check)
   - Exact duplicates
4. **Distribution Analysis**:
   - State-wise distribution
   - District coverage
   - Spatial clustering

## Google Earth Engine Integration

### Publishing Assets

The pipeline automatically publishes vector data to Earth Engine:

```javascript
// Access the published asset in Earth Engine
var mandis = ee.FeatureCollection('projects/YOUR_PROJECT/assets/mandi_locations/mandi_locations_20241031');

// Visualize on map
Map.addLayer(mandis, {color: 'red'}, 'Mandi Locations');
```

### Using in GEE Pipeline

```python
import ee

# Initialize Earth Engine
ee.Initialize()

# Load mandi locations
mandis = ee.FeatureCollection('projects/YOUR_PROJECT/assets/mandi_locations/latest')

# Buffer analysis example
buffered = mandis.map(lambda f: f.buffer(5000))  # 5km buffer

# Spatial join with crop data
crops = ee.ImageCollection('MODIS/006/MOD13Q1')
mandi_crop_analysis = crops.map(lambda img:
    img.reduceRegions(
        collection=buffered,
        reducer=ee.Reducer.mean(),
        scale=250
    )
)
```

## Troubleshooting

### Common Issues

1. **ChromeDriver not found**:
   - Ensure ChromeDriver is in PATH or set CHROMEDRIVER_PATH
   - Match ChromeDriver version with Chrome browser version

2. **Google API quota exceeded**:
   - Reduce batch_size in config
   - Increase delay between requests
   - Check Google Cloud Console for quota limits

3. **Geocoding failures**:
   - Check API key permissions
   - Verify internet connectivity
   - Review geocode_cache.json for patterns

4. **Earth Engine authentication**:
   - Run `earthengine authenticate`
   - Check service account permissions
   - Verify project ID

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## How This Helps Your Project

This mandi locations module significantly enhances the KYL (Know Your Land) platform by:

1. **Market Access Analysis**:
   - Farmers can identify nearest mandis for selling produce
   - Optimize transportation routes and costs
   - Understand market coverage in different regions

2. **Agricultural Planning**:
   - Correlate crop patterns with mandi proximity
   - Identify underserved areas needing new mandis
   - Support policy decisions on market infrastructure

3. **GEE Integration Benefits**:
   - Overlay mandi locations with crop health indices
   - Analyze catchment areas using buffer zones
   - Cross-reference with weather and soil data
   - Generate accessibility maps for farmers

4. **Data-Driven Insights**:
   - Commodity distribution patterns
   - Market density analysis
   - Regional agricultural economics
   - Supply chain optimization

5. **Visualization Capabilities**:
   - Interactive web maps for stakeholders
   - District/state-wise mandi distribution
   - Commodity-specific market networks
   - Temporal analysis of market development

## Testing

Run the test suite:

```bash
# Unit tests
python -m pytest tests/test_scraper.py
python -m pytest tests/test_geocoder.py
python -m pytest tests/test_validator.py

# Integration tests
python -m pytest tests/test_pipeline.py

# Coverage report
python -m pytest --cov=mandi_locations tests/
```

## API Rate Limits

- **Google Places API**:
  - 6,000 requests per minute
  - Configure delay in config.json

- **Google Geocoding API**:
  - 3,000 requests per minute
  - Automatic fallback mechanism

- **Agmarknet**:
  - No official limits
  - Respectful scraping with delays

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run validation checks
6. Submit a pull request

## License

This project is part of the C4GT Community initiative.

## Support

For issues or questions:
- GitHub Issues: https://github.com/C4GT/core-stack-backend/issues
- Documentation: https://docs.c4gt.org

## Acknowledgments

- Agmarknet for providing market data
- Google Maps Platform for geocoding services
- Google Earth Engine for geospatial infrastructure
- C4GT Community for project support