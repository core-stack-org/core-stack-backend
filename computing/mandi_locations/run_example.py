"""
Example script to demonstrate mandi locations pipeline functionality
This script uses mock data to demonstrate the pipeline without external dependencies
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# Create mock data directory
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

def create_mock_mandi_data():
    """Create mock mandi data for demonstration"""

    # Sample mandi data covering major agricultural states
    mock_mandis = [
        # Maharashtra
        {"mandi_code": "MH001", "mandi_name": "Lasalgaon", "state_name": "Maharashtra",
         "district_name": "Nashik", "commodities": "Onion,Tomato,Potato",
         "latitude": 19.9975, "longitude": 73.7898},
        {"mandi_code": "MH002", "mandi_name": "Vashi", "state_name": "Maharashtra",
         "district_name": "Mumbai", "commodities": "Vegetables,Fruits",
         "latitude": 19.0771, "longitude": 73.0087},

        # Punjab
        {"mandi_code": "PB001", "mandi_name": "Amritsar", "state_name": "Punjab",
         "district_name": "Amritsar", "commodities": "Wheat,Rice,Maize",
         "latitude": 31.6340, "longitude": 74.8723},
        {"mandi_code": "PB002", "mandi_name": "Ludhiana", "state_name": "Punjab",
         "district_name": "Ludhiana", "commodities": "Wheat,Cotton,Vegetables",
         "latitude": 30.9010, "longitude": 75.8573},

        # Gujarat
        {"mandi_code": "GJ001", "mandi_name": "Ahmedabad", "state_name": "Gujarat",
         "district_name": "Ahmedabad", "commodities": "Cotton,Groundnut,Wheat",
         "latitude": 23.0225, "longitude": 72.5714},
        {"mandi_code": "GJ002", "mandi_name": "Rajkot", "state_name": "Gujarat",
         "district_name": "Rajkot", "commodities": "Groundnut,Cotton,Vegetables",
         "latitude": 22.3039, "longitude": 70.8022},

        # Uttar Pradesh
        {"mandi_code": "UP001", "mandi_name": "Agra", "state_name": "Uttar Pradesh",
         "district_name": "Agra", "commodities": "Potato,Vegetables,Wheat",
         "latitude": 27.1767, "longitude": 78.0081},
        {"mandi_code": "UP002", "mandi_name": "Kanpur", "state_name": "Uttar Pradesh",
         "district_name": "Kanpur", "commodities": "Wheat,Rice,Pulses",
         "latitude": 26.4499, "longitude": 80.3319},

        # Karnataka
        {"mandi_code": "KA001", "mandi_name": "Yeshwantpur", "state_name": "Karnataka",
         "district_name": "Bangalore", "commodities": "Vegetables,Fruits,Flowers",
         "latitude": 13.0282, "longitude": 77.5876},
        {"mandi_code": "KA002", "mandi_name": "Hubli", "state_name": "Karnataka",
         "district_name": "Dharwad", "commodities": "Cotton,Chilli,Onion",
         "latitude": 15.3647, "longitude": 75.1240},

        # Madhya Pradesh
        {"mandi_code": "MP001", "mandi_name": "Indore", "state_name": "Madhya Pradesh",
         "district_name": "Indore", "commodities": "Soybean,Wheat,Cotton",
         "latitude": 22.7196, "longitude": 75.8577},
        {"mandi_code": "MP002", "mandi_name": "Bhopal", "state_name": "Madhya Pradesh",
         "district_name": "Bhopal", "commodities": "Wheat,Soybean,Vegetables",
         "latitude": 23.2599, "longitude": 77.4126},

        # Rajasthan
        {"mandi_code": "RJ001", "mandi_name": "Jaipur", "state_name": "Rajasthan",
         "district_name": "Jaipur", "commodities": "Wheat,Mustard,Vegetables",
         "latitude": 26.9124, "longitude": 75.7873},
        {"mandi_code": "RJ002", "mandi_name": "Kota", "state_name": "Rajasthan",
         "district_name": "Kota", "commodities": "Soybean,Wheat,Coriander",
         "latitude": 25.2138, "longitude": 75.8648},

        # West Bengal
        {"mandi_code": "WB001", "mandi_name": "Koley Market", "state_name": "West Bengal",
         "district_name": "Kolkata", "commodities": "Vegetables,Rice,Fish",
         "latitude": 22.5726, "longitude": 88.3639},

        # Tamil Nadu
        {"mandi_code": "TN001", "mandi_name": "Koyambedu", "state_name": "Tamil Nadu",
         "district_name": "Chennai", "commodities": "Vegetables,Fruits,Flowers",
         "latitude": 13.0695, "longitude": 80.2013},

        # Andhra Pradesh
        {"mandi_code": "AP001", "mandi_name": "Guntur", "state_name": "Andhra Pradesh",
         "district_name": "Guntur", "commodities": "Chilli,Cotton,Tobacco",
         "latitude": 16.3067, "longitude": 80.4365},

        # Telangana
        {"mandi_code": "TS001", "mandi_name": "Bowenpally", "state_name": "Telangana",
         "district_name": "Hyderabad", "commodities": "Vegetables,Fruits,Flowers",
         "latitude": 17.4817, "longitude": 78.4864},

        # Bihar
        {"mandi_code": "BR001", "mandi_name": "Patna", "state_name": "Bihar",
         "district_name": "Patna", "commodities": "Rice,Wheat,Vegetables",
         "latitude": 25.5941, "longitude": 85.1376},

        # Haryana
        {"mandi_code": "HR001", "mandi_name": "Karnal", "state_name": "Haryana",
         "district_name": "Karnal", "commodities": "Wheat,Rice,Sugarcane",
         "latitude": 29.6857, "longitude": 76.9905}
    ]

    # Add additional metadata
    for mandi in mock_mandis:
        mandi['geocode_source'] = 'mock_data'
        mandi['created_date'] = datetime.now().isoformat()
        mandi['data_source'] = 'Agmarknet_mock'

    return mock_mandis

def generate_statistics(mandis):
    """Generate statistics from mandi data"""
    df = pd.DataFrame(mandis)

    stats = {
        "total_mandis": len(mandis),
        "states_covered": df['state_name'].nunique(),
        "districts_covered": df['district_name'].nunique(),
        "mandis_per_state": df['state_name'].value_counts().to_dict(),
        "geographic_extent": {
            "min_lat": df['latitude'].min(),
            "max_lat": df['latitude'].max(),
            "min_lon": df['longitude'].min(),
            "max_lon": df['longitude'].max()
        },
        "commodities": {}
    }

    # Analyze commodities
    all_commodities = []
    for commodities in df['commodities']:
        all_commodities.extend([c.strip() for c in commodities.split(',')])

    commodity_counts = pd.Series(all_commodities).value_counts()
    stats['commodities'] = {
        "total_unique": len(commodity_counts),
        "top_10": commodity_counts.head(10).to_dict()
    }

    return stats

def create_geojson(mandis):
    """Create GeoJSON from mandi data"""
    features = []

    for mandi in mandis:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [mandi['longitude'], mandi['latitude']]
            },
            "properties": {
                "mandi_code": mandi['mandi_code'],
                "mandi_name": mandi['mandi_name'],
                "state_name": mandi['state_name'],
                "district_name": mandi['district_name'],
                "commodities": mandi['commodities']
            }
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        }
    }

    return geojson

def generate_report(mandis, stats):
    """Generate a summary report"""
    report = []
    report.append("=" * 70)
    report.append("MANDI LOCATIONS VECTOR MAP - DEMONSTRATION REPORT")
    report.append("=" * 70)
    report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    report.append("SUMMARY STATISTICS:")
    report.append(f"  Total Mandis: {stats['total_mandis']}")
    report.append(f"  States Covered: {stats['states_covered']}")
    report.append(f"  Districts Covered: {stats['districts_covered']}")
    report.append("")

    report.append("GEOGRAPHIC EXTENT:")
    extent = stats['geographic_extent']
    report.append(f"  Latitude Range: {extent['min_lat']:.4f}° to {extent['max_lat']:.4f}°")
    report.append(f"  Longitude Range: {extent['min_lon']:.4f}° to {extent['max_lon']:.4f}°")
    report.append("")

    report.append("STATE-WISE DISTRIBUTION:")
    for state, count in sorted(stats['mandis_per_state'].items(),
                              key=lambda x: x[1], reverse=True):
        report.append(f"  {state}: {count} mandi(s)")
    report.append("")

    report.append("TOP COMMODITIES:")
    for commodity, count in list(stats['commodities']['top_10'].items())[:10]:
        report.append(f"  {commodity}: {count} mandis")
    report.append(f"  Total Unique Commodities: {stats['commodities']['total_unique']}")
    report.append("")

    report.append("SAMPLE MANDI ENTRIES:")
    for mandi in mandis[:3]:
        report.append(f"  - {mandi['mandi_name']} ({mandi['state_name']})")
        report.append(f"    Location: {mandi['latitude']:.4f}°N, {mandi['longitude']:.4f}°E")
        report.append(f"    Commodities: {mandi['commodities']}")
        report.append("")

    report.append("=" * 70)

    return "\n".join(report)

def main():
    """Main demonstration function"""
    print("MANDI LOCATIONS PIPELINE DEMONSTRATION")
    print("=" * 50)
    print("")

    # Create output directories
    for subdir in ['raw', 'geocoded', 'vector', 'reports']:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)

    # Step 1: Create mock mandi data
    print("Step 1: Creating mock mandi data...")
    mandis = create_mock_mandi_data()

    # Save raw data
    raw_file = data_dir / 'raw' / 'mock_mandi_data.json'
    with open(raw_file, 'w') as f:
        json.dump(mandis, f, indent=2)
    print(f"  [OK] Created {len(mandis)} mock mandi records")
    print(f"  [OK] Saved to: {raw_file}")
    print("")

    # Step 2: Generate statistics
    print("Step 2: Analyzing mandi data...")
    stats = generate_statistics(mandis)
    print(f"  [OK] States covered: {stats['states_covered']}")
    print(f"  [OK] Districts covered: {stats['districts_covered']}")
    print(f"  [OK] Unique commodities: {stats['commodities']['total_unique']}")
    print("")

    # Step 3: Create GeoJSON
    print("Step 3: Creating vector map (GeoJSON)...")
    geojson = create_geojson(mandis)

    # Save GeoJSON
    vector_file = data_dir / 'vector' / 'mandi_locations.geojson'
    with open(vector_file, 'w') as f:
        json.dump(geojson, f, indent=2)
    print(f"  [OK] Generated GeoJSON with {len(geojson['features'])} features")
    print(f"  [OK] Saved to: {vector_file}")
    print("")

    # Step 4: Generate report
    print("Step 4: Generating summary report...")
    report = generate_report(mandis, stats)

    # Save report
    report_file = data_dir / 'reports' / 'mandi_summary_report.txt'
    with open(report_file, 'w') as f:
        f.write(report)
    print(f"  [OK] Report generated")
    print(f"  [OK] Saved to: {report_file}")
    print("")

    # Print summary
    print("=" * 50)
    print("DEMONSTRATION COMPLETED SUCCESSFULLY")
    print("=" * 50)
    print("")
    print("FILES CREATED:")
    print(f"  1. Raw Data:     {raw_file}")
    print(f"  2. Vector Map:   {vector_file}")
    print(f"  3. Report:       {report_file}")
    print("")
    print("NEXT STEPS:")
    print("  1. Install dependencies: pip install -r requirements.txt")
    print("  2. Set up Google API key in .env file")
    print("  3. Run full pipeline: python -m mandi_locations.pipeline")
    print("")
    print("HOW THIS HELPS YOUR PROJECT:")
    print("  - Provides market access data for farmers")
    print("  - Enables proximity analysis for agricultural planning")
    print("  - Integrates with GEE for spatial analytics")
    print("  - Supports policy decisions on market infrastructure")
    print("  - Enhances supply chain optimization")

    # Display sample from report
    print("")
    print("SAMPLE REPORT OUTPUT:")
    print("-" * 50)
    print("\n".join(report.split("\n")[:30]))
    print("...")
    print("-" * 50)

if __name__ == "__main__":
    main()