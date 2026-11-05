import json
from pathlib import Path

import boto3

# ================= AWS CONFIG =================

ACCESS_KEY = ""
SECRET_KEY = ""
REGION = ""
BUCKET = ""

# ==============================================

LOCAL_ROOT = Path("CLEAN_DATA")
OUTPUT_ROOT = Path("MERGED_OLD_NEW")

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

s3 = boto3.client(
    "s3",
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

IGNORE_S3_FOLDERS = {"TELANGANA", "TRIPURA", "WEST_BENGAL", "nrega_04_2026"}


def list_s3_files():
    """
    Returns:
        {
            "ODISHA/KALAHANDI.geojson",
            "JHARKHAND/DUMKA.geojson",
            ...
        }
    """
    files = set()

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".geojson"):
                continue

            state = key.split("/")[0]
            if state in IGNORE_S3_FOLDERS:
                continue
            files.add(key)

    return files


def list_local_files():
    """
    Returns relative paths of local geojsons.
    """
    files = set()

    for file in LOCAL_ROOT.rglob("*.geojson"):
        files.add(file.relative_to(LOCAL_ROOT).as_posix())

    return files


print("Listing S3 files...")
s3_files = list_s3_files()

print(f"Found {len(s3_files)} GeoJSONs in S3")

print("Listing local files...")
local_files = list_local_files()

print(f"Found {len(local_files)} GeoJSONs locally")

all_files = sorted(s3_files | local_files)

print(f"Total unique files: {len(all_files)}\n")

merged_count = 0
local_only_count = 0
s3_only_count = 0

for rel_path in all_files:

    local_exists = rel_path in local_files
    s3_exists = rel_path in s3_files

    output_file = OUTPUT_ROOT / rel_path
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if local_exists:
        with open(LOCAL_ROOT / rel_path, "r", encoding="utf-8") as f:
            local_geojson = json.load(f)

    if s3_exists:
        response = s3.get_object(Bucket=BUCKET, Key=rel_path)
        s3_geojson = json.loads(response["Body"].read())

    if local_exists and s3_exists:

        merged = {
            "type": "FeatureCollection",
            "features": (local_geojson["features"] + s3_geojson["features"]),
        }

        merged_count += 1
        status = "MERGED"

    elif local_exists:

        merged = local_geojson

        local_only_count += 1
        status = "LOCAL ONLY"

    else:

        merged = s3_geojson

        s3_only_count += 1
        status = "S3 ONLY"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            merged,
            f,
            ensure_ascii=False,
        )

    print(f"{status:<12} {rel_path}")

print("\n================ SUMMARY ================")
print(f"Merged files      : {merged_count}")
print(f"Local only files  : {local_only_count}")
print(f"S3 only files     : {s3_only_count}")
print(f"Total output      : {len(all_files)}")
print("=========================================")
