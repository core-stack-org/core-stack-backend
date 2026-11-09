import csv
import json
import os
import sys

def csv_to_geojson(csv_path, lat_col='LAT', lon_col='LNG'):
    # Automatically create GeoJSON filename in same folder
    base, _ = os.path.splitext(csv_path)
    geojson_path = base + '.geojson'

    features = []

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lat = float(row[lat_col])
                lon = float(row[lon_col])
            except (KeyError, ValueError):
                continue  # skip invalid rows

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {k: v for k, v in row.items() if k not in [lat_col, lon_col]}
            }
            features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(geojson_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)

    print(f"✅ Converted {csv_path} → {geojson_path} ({len(features)} features)")
