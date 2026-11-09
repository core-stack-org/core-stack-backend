import os
import re
import sys
import json
import argparse
import requests
from pathlib import Path

# Setup simple console and file logging
def log_message(msg):
    print(msg)

def log_missing_s3(state, district, url):
    with open("s3_missing.log", "a", encoding="utf-8") as f:
        f.write(f"[MISSING S3] State: {state}, District: {district} - URL: {url}\n")

def log_similarity_duplicate(coord, feat1, feat2, score, matched_fields, state, district):
    p1 = feat1.get("properties", {})
    p2 = feat2.get("properties", {})
    
    id1 = p1.get("Asset ID", p1.get("assetid", "N/A"))
    code1 = p1.get("Work Code", p1.get("workcode", "N/A"))
    name1 = p1.get("Work Name", p1.get("workname", "N/A"))
    src1 = feat1.get("__source", "unknown")
    
    id2 = p2.get("Asset ID", p2.get("assetid", "N/A"))
    code2 = p2.get("Work Code", p2.get("workcode", "N/A"))
    name2 = p2.get("Work Name", p2.get("workname", "N/A"))
    src2 = feat2.get("__source", "unknown")
    
    with open("similarity_duplicates.log", "a", encoding="utf-8") as f:
        f.write(f"[DUPLICATE MATCH] State: {state} | District: {district} | Coordinate: {coord} | Score: {score}\n")
        f.write(f"  Retained (Source: {src1}): Asset ID={id1}, Work Code={code1}, Work Name={name1}\n")
        f.write(f"  Discarded (Source: {src2}): Asset ID={id2}, Work Code={code2}, Work Name={name2}\n")
        f.write(f"  Matched fields: {', '.join(matched_fields)}\n")
        f.write("-" * 80 + "\n")

# Formatting function requested by user
def valid_gee_text(description):
    description = re.sub(r"[^a-zA-Z0-9 ,:;_-]", "", description)
    return description.replace(" ", "_")

def normalize_key(key):
    return key.lower().replace(" ", "").replace("_", "").replace("-", "")

def get_normalized_property(properties, field_name):
    target = normalize_key(field_name)
    # Special normalization mapping: e.g. gpname/grampanchayat are treated as the same
    for k, v in properties.items():
        norm_k = normalize_key(k)
        if norm_k == target:
            return v
        if target == 'grampanchayat' and norm_k == 'gpname':
            return v
    return None

def get_clean_asset_id(properties):
    val = get_normalized_property(properties, 'assetid')
    if val is None:
        return None
    val_str = str(val).strip()
    # Check if it is a float representation of integer (e.g. 12345.0) and convert to string representation of int
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    if val_str in ("", "-1", "0", "none", "null", "nan", "undefined"):
        return None
    return val_str

def has_valid_lat_lon(feature):
    geom = feature.get("geometry")
    if not geom or not isinstance(geom, dict):
        return False
    coords = geom.get("coordinates")
    if not coords or not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return False
    lon, lat = coords[0], coords[1]
    if lon is None or lat is None:
        return False
    try:
        lon_f = float(lon)
        lat_f = float(lat)
        if lon_f == 0.0 or lat_f == 0.0 or lon_f == -1.0 or lat_f == -1.0:
            return False
        return True
    except (ValueError, TypeError):
        return False

def calculate_similarity_score(properties1, properties2):
    fields_to_check = [
        'workname', 'worktype', 'grampanchayat', 'block',
        'assetname', 'observername', 'workcode'
    ]
    score = 0
    matched_fields = []
    for field in fields_to_check:
        v1 = get_normalized_property(properties1, field)
        v2 = get_normalized_property(properties2, field)
        if v1 is not None and v2 is not None:
            s1 = str(v1).strip().lower()
            s2 = str(v2).strip().lower()
            if s1 and s2 and s1 not in ("", "-1", "none", "null", "nan", "undefined") and s1 == s2:
                score += 1
                matched_fields.append(field)
    return score, matched_fields

def deduplicate_features(features, state_name, district_name):
    # Separate features into those with valid Asset ID and those without
    with_asset_id = []
    without_asset_id = []
    
    for f in features:
        asset_id = get_clean_asset_id(f.get("properties", {}))
        if asset_id:
            with_asset_id.append((asset_id, f))
        else:
            without_asset_id.append(f)
            
    log_message(f"Initial: {len(features)} features (with Asset ID: {len(with_asset_id)}, without Asset ID: {len(without_asset_id)})")
    
    # Step 1: Dedup on Asset ID
    asset_id_groups = {}
    for asset_id, f in with_asset_id:
        asset_id_groups.setdefault(asset_id, []).append(f)
        
    retained_from_step1 = []
    for asset_id, group in asset_id_groups.items():
        if len(group) == 1:
            retained_from_step1.append(group[0])
        else:
            # Select the one with valid lat/lon
            valid_coords_group = [f for f in group if has_valid_lat_lon(f)]
            if valid_coords_group:
                # Prioritize local source, then s3
                valid_coords_group.sort(key=lambda x: 0 if x.get("__source") == "local" else 1)
                retained_from_step1.append(valid_coords_group[0])
            else:
                # None has valid coordinates, keep local first
                group.sort(key=lambda x: 0 if x.get("__source") == "local" else 1)
                retained_from_step1.append(group[0])
                
    log_message(f"After Step 1 (Asset ID dedup): Retained {len(retained_from_step1)} features from groups")
    
    # Step 2: Dedup on Lat/Long and Similarity Score
    # Combine step 1 retained features and features without Asset ID
    all_features_to_coords_dedup = retained_from_step1 + without_asset_id
    
    # Separate into features with valid lat/lon and those without
    with_coords = []
    no_coords = []
    for f in all_features_to_coords_dedup:
        if has_valid_lat_lon(f):
            with_coords.append(f)
        else:
            no_coords.append(f)
            
    # Group by coordinate tuple rounded to 7 decimal places
    coord_groups = {}
    for f in with_coords:
        coords = f["geometry"]["coordinates"]
        lon, lat = float(coords[0]), float(coords[1])
        coord_key = (round(lon, 7), round(lat, 7))
        coord_groups.setdefault(coord_key, []).append(f)
        
    final_retained_coords = []
    
    for coord, group in coord_groups.items():
        if len(group) == 1:
            final_retained_coords.append(group[0])
        else:
            # Sort local first, then s3
            group.sort(key=lambda x: 0 if x.get("__source") == "local" else 1)
            unique_kept = []
            
            for candidate in group:
                is_duplicate = False
                for kept in unique_kept:
                    score, matched_fields = calculate_similarity_score(
                        candidate.get("properties", {}), kept.get("properties", {})
                    )
                    if score > 4:
                        is_duplicate = True
                        log_similarity_duplicate(coord, kept, candidate, score, matched_fields, state_name, district_name)
                        break
                if not is_duplicate:
                    unique_kept.append(candidate)
                    
            final_retained_coords.extend(unique_kept)
            
    final_features = final_retained_coords + no_coords
    log_message(f"After Step 2 (Coordinate similarity dedup): Final features count = {len(final_features)}")
    
    return final_features

def process_district(state_name, district_name, state_local_dir, new_bhuvan_folder):
    log_message(f"\n==================================================")
    log_message(f"Processing State: {state_name} | District: {district_name}")
    log_message(f"==================================================")
    
    # 1. Fetch S3 file
    s3_state = valid_gee_text(state_name).upper()
    s3_district = valid_gee_text(district_name).upper()
    s3_url = f"https://naregageojson.s3.ap-south-1.amazonaws.com/{s3_state}/{s3_district}.geojson"
    
    log_message(f"Fetching from S3: {s3_url}")
    try:
        response = requests.get(s3_url, timeout=30)
        if response.status_code == 404:
            log_message(f"❌ S3 returned 404 for URL: {s3_url}")
            log_missing_s3(state_name, district_name, s3_url)
            log_message(f"Skipping processing for {district_name} as requested.")
            return False
        response.raise_for_status()
        s3_data = response.json()
        s3_features = s3_data.get("features", [])
        for f in s3_features:
            f["__source"] = "s3"
        log_message(f"Successfully loaded {len(s3_features)} features from S3.")
    except Exception as e:
        log_message(f"❌ Failed to fetch/parse S3 file: {e}")
        # Log failure and skip
        log_missing_s3(state_name, district_name, s3_url)
        log_message(f"Skipping processing for {district_name} due to fetch error.")
        return False
        
    # 2. Load Local file
    # Look for district geojson in new_bhuvan_files
    local_file_name = f"{district_name}.geojson"
    local_path = new_bhuvan_folder / local_file_name
    
    # If not found directly, try case-insensitive or space/underscore conversions
    if not local_path.exists():
        normalized_target = normalize_key(district_name)
        for child in new_bhuvan_folder.glob("*.geojson"):
            if normalize_key(child.stem) == normalized_target:
                local_path = child
                break
                
    if not local_path.exists():
        log_message(f"❌ Local newly scraped GeoJSON not found at: {new_bhuvan_folder / local_file_name}")
        log_message(f"Skipping processing for {district_name}.")
        return False
        
    log_message(f"Loading local file: {local_path}")
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            local_data = json.load(f)
        local_features = local_data.get("features", [])
        for f in local_features:
            f["__source"] = "local"
        log_message(f"Successfully loaded {len(local_features)} features from local file.")
    except Exception as e:
        log_message(f"❌ Failed to load local file: {e}")
        return False
        
    # 3. Merge and De-duplicate
    merged_features = s3_features + local_features
    log_message(f"Merging S3 and local features (total {len(merged_features)} features)")
    deduplicated_features = deduplicate_features(merged_features, state_name, district_name)
    
    # Remove internal '__source' helper keys from final features
    for f in deduplicated_features:
        f.pop("__source", None)
        
    # 4. Save output
    output_geojson = {
        "type": "FeatureCollection",
        "features": deduplicated_features
    }
    
    # Save in state's root directory, matching the input district name casing/format
    output_path = state_local_dir / f"{district_name}.geojson"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_geojson, f, indent=2)
        log_message(f"✅ Successfully wrote final de-duplicated GeoJSON to {output_path}")
        return True
    except Exception as e:
        log_message(f"❌ Failed to write output file: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="De-duplicate and merge district GeoJSONs from S3 and local files.")
    parser.add_argument("--state", required=True, help="State name (e.g. GOA or ANDAMAN AND NICOBAR)")
    parser.add_argument("--district", help="Specific district name (optional). If omitted, processes all districts in local folder.")
    args = parser.parse_args()
    
    workspace_dir = Path(__file__).parent.resolve()
    
    # Resolve the local state directory name flexibly (check with spaces, check with underscores)
    state_input = args.state
    state_variants = [
        state_input,
        state_input.replace("_", " "),
        state_input.replace(" ", "_"),
        state_input.upper(),
        state_input.upper().replace("_", " "),
        state_input.upper().replace(" ", "_")
    ]
    
    state_local_dir = None
    for variant in state_variants:
        path_candidate = workspace_dir / variant
        if path_candidate.is_dir():
            state_local_dir = path_candidate
            break
            
    if not state_local_dir:
        print(f"❌ Error: State directory not found locally. Tried variants: {state_variants}")
        sys.exit(1)
        
    new_bhuvan_folder = state_local_dir / "new_bhuvan_files"
    if not new_bhuvan_folder.is_dir():
        print(f"❌ Error: 'new_bhuvan_files' folder not found under {state_local_dir}")
        sys.exit(1)
        
    if args.district:
        process_district(state_input, args.district, state_local_dir, new_bhuvan_folder)
    else:
        # Automatically process all geojson files in new_bhuvan_files
        local_geojson_files = list(new_bhuvan_folder.glob("*.geojson"))
        if not local_geojson_files:
            print(f"No GeoJSON files found in {new_bhuvan_folder}")
            sys.exit(0)
            
        print(f"Found {len(local_geojson_files)} districts to process in {new_bhuvan_folder.name}.")
        success_count = 0
        skipped_count = 0
        
        for file_path in local_geojson_files:
            # Use the stem of the file as the district name
            dist_name = file_path.stem
            success = process_district(state_input, dist_name, state_local_dir, new_bhuvan_folder)
            if success:
                success_count += 1
            else:
                skipped_count += 1
                
        print(f"\nProcessing complete: {success_count} succeeded, {skipped_count} skipped/logged.")

if __name__ == "__main__":
    main()
