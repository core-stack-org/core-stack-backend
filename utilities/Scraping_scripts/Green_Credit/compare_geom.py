import geopandas as gpd
import json

# Files
pan_india_file = "PAN_INDIA_merged_new_oct.geojson" #newly scrapped
green_credit_file = "PAN_INDIA_updated_removed_dup_geometry.geojson" #last scrapped(removed duplicates)

# Output
output_file = "PAN_INDIA_updated_removed_dup_geometry_oct.geojson"

# Read files
pan_gdf = gpd.read_file(pan_india_file)
green_gdf = gpd.read_file(green_credit_file)

# Filter pan_gdf to only valid, non-empty geometries for safe comparison
pan_gdf_valid = pan_gdf[pan_gdf.geometry.notna() & ~pan_gdf.geometry.is_empty]

# remove duplicates from pan_gdf_valid
initial_count = len(pan_gdf_valid)
pan_gdf_valid = pan_gdf_valid[~pan_gdf_valid.geometry.to_wkt().duplicated(keep='first')]
removed_count = initial_count - len(pan_gdf_valid)
print(f"Removed {removed_count} duplicate geometries from PAN_INDIA dataset.")
print(f"Existing unique, valid PAN_INDIA geometries: {len(pan_gdf_valid)}")


new_features = []
added_geometries = []

for _, row in green_gdf.iterrows():
    # 1. Skip rows with missing or empty geometry
    if row.geometry is None or isinstance(row.geometry, float) or row.geometry.is_empty:
        continue

    # 2. Check if this geometry already exists in the master PAN_INDIA dataset (pan_gdf_valid)
    if pan_gdf_valid.geometry.geom_equals(row.geometry).any():
        continue

    # 3. Check if we have already added this geometry in the current loop (removes duplicates within green_gdf)
    if any(geom.equals(row.geometry) for geom in added_geometries):
        continue

    # Extract metadata properties
    parcel_id = str(row.get("parcel_id", row.get("EPID", ""))).strip()
    state = row.get("state", "")
    district = row.get("district", "")
    division = row.get("division", "")
    land_info = str(row.get("land_info", ""))
    kml_url = row.get("kml_url", "")

    # Parse land_info
    parts = [x.strip() for x in land_info.split("|")]
    registration_no = parts[0] if len(parts) > 0 else ""
    land_area = parts[1] if len(parts) > 1 else ""

    print(
        f"Adding -> State: {state}, "
        f"District: {district}, "
        f"Parcel ID: {parcel_id if parcel_id else 'N/A'}"
    )

    # Create PAN_INDIA style feature
    new_feature = {
        "type": "Feature",
        "properties": {
            "registration_no": registration_no,
            "EPID": 0,
            "state": state,
            "district": district,
            "division": division,
            "range": parcel_id if parcel_id else "UNKNOWN",
            "entity_name": "",
            "land_area": land_area,
            "kml_url": kml_url,
        },
        "geometry": row.geometry.__geo_interface__,
    }

    new_features.append(new_feature)
    added_geometries.append(row.geometry)

print(f"\nNew features to add: {len(new_features)}")

# Load original PAN_INDIA geojson
with open(pan_india_file, "r") as f:
    pan_geojson = json.load(f)

# Append new features
pan_geojson["features"].extend(new_features)

# Save updated file
with open(output_file, "w") as f:
    json.dump(pan_geojson, f)

print(f"\nSaved updated GeoJSON: {output_file}")
print(f"Total final features: {len(pan_geojson['features'])}")
