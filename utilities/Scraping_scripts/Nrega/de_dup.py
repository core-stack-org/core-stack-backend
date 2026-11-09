import os
import re
import json
import pandas as pd
import geopandas as gpd
import networkx as nx
from rapidfuzz import fuzz
from shapely.geometry import mapping

### config
BASE_DIR = "MERGED_OLD_NEW"
OUTPUT_DIR = "CLEAN_DATA"


### helpers (unchanged logic from the notebook)

def safe_repair(val):
    if not isinstance(val, str):
        return val
    try:
        # Try to restore raw bytes using latin1 and decode to utf-8
        return val.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        try:
            # Fallback for Windows-1252 encoding differences
            return val.encode('cp1252').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            # If it cannot be encoded/decoded, it's already correct or clean
            return val


def clean_val(val):
    if pd.isna(val) or val is None:
        return ""
    val = str(val).lower().strip()
    val = re.sub(r'[^\w\s]', '', val)
    val = re.sub(r'[\s_-]+', '', val)
    return val


def gdf_to_geojson(gdf, geojson_path):
    """
    Write a GeoDataFrame out to GeoJSON, following the same Feature structure
    as the provided csv_to_geojson function — but since we're already reading
    a geojson (so a real geometry already exists), we take the geometry
    straight from the geometry column instead of rebuilding it from lat/lon.
    """
    features = []

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue  # skip rows with no geometry

        feature = {
            "type": "Feature",
            "geometry": mapping(geom),
            "properties": {k: v for k, v in row.items() if k != 'geometry'}
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(geojson_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, default=str)

    print(f"Converted -> {geojson_path} ({len(features)} features)")


### core pipeline (same logic as the notebook, wrapped per-file)

def process_geojson(geojson_path, state_name, district_name):
    print(f"\n=== Processing {state_name}/{district_name} ===")

    ### read data & shape
    data = gpd.read_file(geojson_path)
    data = data.dropna(subset=['geometry'])
    data = data[~data.geometry.is_empty & data.geometry.is_valid]

    for col in data.select_dtypes(include=['object']).columns:
        if col != 'geometry':  # Skip geometry
            data[col] = data[col].apply(safe_repair)
    print(data.columns)
    print(data.shape)

    ### De Dup
    assert data.index.is_unique, "data index is not unique — fix before proceeding"
    print("Index unique:", data.index.is_unique)

    candidate_pairs = set()

    # --- from Work Code groups ---
    for work_code, group in data.groupby("Work Code"):
        idxs = group.index.tolist()
        for i in range(len(idxs)):
            for j in range(i + 1, len(idxs)):
                candidate_pairs.add(tuple(sorted((idxs[i], idxs[j]))))

    work_code_pair_count = len(candidate_pairs)

    # --- from Geometry groups ---
    data["geom_wkt"] = data.geometry.apply(lambda g: g.wkt if g is not None else None)

    for geom_key, group in data.groupby("geom_wkt"):
        if geom_key is None:
            continue
        idxs = group.index.tolist()
        for i in range(len(idxs)):
            for j in range(i + 1, len(idxs)):
                candidate_pairs.add(tuple(sorted((idxs[i], idxs[j]))))  # set auto-dedupes

    data.drop(columns="geom_wkt", inplace=True)

    print(f"Candidate pairs from Work Code: {work_code_pair_count}")
    print(f"Total unique candidate pairs (Work Code + Geometry): {len(candidate_pairs)}")

    results = []

    for idx1, idx2 in candidate_pairs:
        row1 = data.loc[idx1]
        row2 = data.loc[idx2]

        asset_name_score = fuzz.token_set_ratio(
            clean_val(row1["Asset Name"]), clean_val(row2["Asset Name"])
        )
        work_name_score = fuzz.token_set_ratio(
            clean_val(row1["Work Name"]), clean_val(row2["Work Name"])
        )
        work_code_score = fuzz.token_set_ratio(
            clean_val(row1["Work Code"]), clean_val(row2["Work Code"])
        )
        work_type_score = fuzz.token_set_ratio(
            clean_val(row1["Work Type"]), clean_val(row2["Work Type"])
        )
        state_score = fuzz.token_set_ratio(
        clean_val(row1["State"]), clean_val(row2["State"])
        )
        district_score = fuzz.token_set_ratio(
            clean_val(row1["District"]), clean_val(row2["District"])
        )
        block_score = fuzz.token_set_ratio(
            clean_val(row1["Block"]), clean_val(row2["Block"])
        )
        gram_panchayat_score = fuzz.token_set_ratio(
            clean_val(row1["Gram_panchayat"]), clean_val(row2["Gram_panchayat"])
        )

        if (
            asset_name_score >= 97 and
            work_name_score >= 95 and
            work_code_score >= 100 and
            work_type_score >= 100 and
            state_score == 100 and
            district_score ==100 and
            block_score==100 and
            gram_panchayat_score == 100
        ):
            results.append({
                "Index 1": idx1,
                "Index 2": idx2,
                "Work Code 1": row1["Work Code"],
                "Work Code 2": row2["Work Code"],
                "Asset ID 1": row1["Asset ID"],
                "Asset ID 2": row2["Asset ID"],
                "Asset Name 1": row1["Asset Name"],
                "Asset Name 2": row2["Asset Name"],
                "Asset Name Score": asset_name_score,
                "work name 1": row1["Work Name"],
                "work name 2": row2["Work Name"],
                "Work Name Score": work_name_score,
                "work type 1": row1["Work Type"],
                "work type 2": row2["Work Type"],
                "Work Type Score": work_type_score,
                "state 1":row1['State'],
                "state 2":row2['State'],
                "state_score":state_score,
                "district 1":row1['District'],
                "district 2":row2['District'],
                "district_score":district_score,
                "block 1":row1['Block'],
                "block 2":row1['Block'],
                "block_score":block_score,
                "gram panchayat 1":row1['Gram_panchayat'],
                "gram panchayat 2":row2['Gram_panchayat'],
                "gram panchayta score":gram_panchayat_score
            })

    duplicate_df = pd.DataFrame(results)
    print(f"Found {len(duplicate_df)} duplicate pairs")
    print(duplicate_df)

    indices_to_remove = set()
    groups_info = []

    if not duplicate_df.empty:
        G = nx.Graph()
        G.add_edges_from(zip(duplicate_df["Index 1"], duplicate_df["Index 2"]))

        for component in nx.connected_components(G):
            component = sorted(component)
            keep_idx = component[0]
            drop_idxs = component[1:]
            indices_to_remove.update(drop_idxs)
            groups_info.append({"keep": keep_idx, "dropped": drop_idxs, "group_size": len(component)})

    print(f"Number of duplicate groups: {len(groups_info)}")
    print(f"Total rows to remove: {len(indices_to_remove)}")

    chain_groups = [g for g in groups_info if g["group_size"] > 2]
    print(f"Groups with 3+ mutually-linked rows: {len(chain_groups)}")

    for g in chain_groups:
        idxs = [g["keep"]] + g["dropped"]
        print(f"\nGroup (size {g['group_size']}):")
        print(data.loc[idxs, ["Asset ID", "Asset Name", "Work Code", "Work Name", "Work Type"]])

    clean_data = data.drop(index=indices_to_remove).copy()

    print(f"Original: {len(data)}")
    print(f"Cleaned:  {len(clean_data)}")
    print(f"Removed:  {len(data) - len(clean_data)}")

    duplicates_sorted = clean_data.sort_values(by="Work Code")

    print(f"Pairs unique to geometry (not caught by Work Code): "
          f"{len(candidate_pairs) - work_code_pair_count}")

    ### save output using the custom geojson writer (instead of to_file(driver='GeoJSON'))
    state_out_dir = os.path.join(OUTPUT_DIR, state_name)
    os.makedirs(state_out_dir, exist_ok=True)
    out_path = os.path.join(state_out_dir, f"{district_name.upper()}.geojson")

    gdf_to_geojson(clean_data, out_path)


def main():
    for state_name in sorted(os.listdir(BASE_DIR)):
        state_dir = os.path.join(BASE_DIR, state_name)
        if not os.path.isdir(state_dir):
            continue

        for filename in sorted(os.listdir(state_dir)):
            if not filename.lower().endswith(".geojson"):
                continue

            district_name = os.path.splitext(filename)[0]
            geojson_path = os.path.join(state_dir, filename)

            try:
                process_geojson(geojson_path, state_name, district_name)
            except Exception as e:
                print(f"Failed processing {geojson_path}: {e}")


if __name__ == "__main__":
    main()