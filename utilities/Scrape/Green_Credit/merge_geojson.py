import json
import glob
from pathlib import Path

# Resolve paths dynamically relative to the script's directory
SCRIPT_DIR = Path(__file__).parent.resolve()
INPUT_FOLDER = SCRIPT_DIR / "ALL_state_geojson_2"
OUTPUT_FILE = SCRIPT_DIR / "PAN_INDIA_merged_new_oct.geojson"

merged_features = []

# Read all GeoJSON files
for file_path in INPUT_FOLDER.glob("*.geojson"):
    print(f"Reading: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

        # Handle FeatureCollection
        if data.get("type") == "FeatureCollection":
            merged_features.extend(data.get("features", []))

        # Handle single Feature
        elif data.get("type") == "Feature":
            merged_features.append(data)

# Create final merged GeoJSON
merged_geojson = {
    "type": "FeatureCollection",
    "features": merged_features
}

# Save merged file
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(merged_geojson, f, indent=2)

print(f"\nMerged {len(merged_features)} features into: {OUTPUT_FILE}")
