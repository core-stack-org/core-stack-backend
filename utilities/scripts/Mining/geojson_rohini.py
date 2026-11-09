import pandas as pd
import json
import numpy as np

# Read the CSV file
print("Reading mine data...")
df = pd.read_csv('mine_selected_data.csv')

print(f"Total mines: {len(df)}")
print(f"Columns: {list(df.columns)}")

# Create GeoJSON structure
geojson = {
    "type": "FeatureCollection",
    "features": []
}

# Convert each row to a GeoJSON feature
for idx, row in df.iterrows():
    # Create properties dictionary, converting NaN to None
    properties = {}
    for col in df.columns:
        if col not in ['latitude', 'longitude']:
            value = row[col]
            # Convert NaN, empty strings to None for proper JSON
            if pd.isna(value) or value == '':
                properties[col] = None
            else:
                properties[col] = value
    
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(row['longitude']), float(row['latitude'])]  # [lon, lat]
        },
        "properties": properties
    }
    
    geojson['features'].append(feature)

# Save to GeoJSON file
output_file = 'mines_india.geojson'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(geojson, f, indent=2, ensure_ascii=False)

print(f"\n✅ GeoJSON file created: {output_file}")
print(f"✅ Total features: {len(geojson['features'])}")

# Print sample feature
print(f"\n📍 Sample feature:")
print(json.dumps(geojson['features'][0], indent=2, ensure_ascii=False))

# Print statistics
print(f"\n📊 Statistics:")

# Check if 'state' column exists (renamed from state_cl)
state_col = 'state' if 'state' in df.columns else 'state_cl'
if state_col in df.columns:
    states = df[state_col].value_counts()
    print(f"\nMines by state (Top 10):")
    for state, count in states.head(10).items():
        print(f"  {state}: {count}")

# Check for sector column
if 'sector_moefcc' in df.columns:
    sectors = df['sector_moefcc'].value_counts()
    print(f"\nMines by sector:")
    for sector, count in sectors.head(5).items():
        print(f"  {sector}: {count}")