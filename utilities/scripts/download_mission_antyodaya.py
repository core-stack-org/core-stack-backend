import requests
import zipfile
import csv
import os
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional

# Determine base directory (works when run from project root or script directory)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BASE_DIR = PROJECT_ROOT

# Path to the facilities GeoJSON data
ANTYODAYA_DATA_PATH = BASE_DIR / "data" / "antyodaya" / "raw_files"

# Load state mapping from LGD data
LGD_STATE_JSON = BASE_DIR / "data" / "lgd" / "lgd_state.json"

# Mission Antyodaya download links (fresh as of 2026-04-08)
MISSION_ANTYODAYA_LINKS = {
    2018: "https://missionantyodaya.dord.gov.in/dataFileDownlaod?transactionId=50e6321e-ab17-4382-a8ef-3f67d5ce0c35",
    2019: "https://missionantyodaya.dord.gov.in/dataFileDownlaod?transactionId=5a38c0a9-3998-42d4-869a-019ad8efedc3",
    2020: "https://missionantyodaya.dord.gov.in/dataFileDownlaod?transactionId=1011137f-b350-4630-9d9e-4d045d0eaefd",
    2023: "https://missionantyodaya.dord.gov.in/dataFileDownlaod?transactionId=51cfd285-fb3d-4ce7-b6d9-df10cf41a3bf",
}



def load_state_mapping() -> Dict[int, str]:
    """Load state mapping from LGD JSON and convert to snake_case."""
    try:
        with open(LGD_STATE_JSON, 'r', encoding='utf-8') as f:
            states = json.load(f)

        state_mapping = {}
        for state in states:
            state_code = state['state_code']
            state_name = state['state_name_english'].lower().replace(' ', '_').replace('(', '').replace(')', '').replace('&', 'and')
            state_mapping[state_code] = state_name

        return state_mapping
    except Exception as e:
        print(f"Warning: Could not load state mapping: {e}")
        return {}


def download_mission_antyodaya(url: str, year: int, base_output_dir: str = ANTYODAYA_DATA_PATH) -> bool:
    """
    Download Mission Antyodaya data for a specific year and convert to proper CSV files.

    IMPORTANT: The downloaded .csv file is actually a ZIP archive containing multiple
    pipe-delimited CSV files (one per state/UT). This function:
    1. Downloads the file
    2. Extracts the ZIP contents to year-specific folder
    3. Converts pipe-delimited files to proper comma-delimited CSVs
    4. Renames files using snake_case state names

    Args:
        url: Download URL with transactionId parameter
        year: Year of the data (e.g., 2019)
        base_output_dir: Base directory to save files

    Returns:
        True if successful, False otherwise
    """
    # Create year-specific output directory
    output_dir = Path(base_output_dir) / str(year)
    output_dir.mkdir(parents=True, exist_ok=True)

    transaction_id = url.split("transactionId=")[-1]
    temp_zip_path = output_dir / f"temp_{transaction_id}.zip"

    print(f"\n{'='*50}")
    print(f"Downloading Mission Antyodaya data for year {year}")
    print(f"URL: {url}")
    print(f"Transaction ID: {transaction_id}")
    print(f"Output directory: {output_dir}")
    print(f"{'='*50}")

    # Download with proper headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/octet-stream, text/csv, application/zip, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    try:
        response = requests.get(url, headers=headers, timeout=300, allow_redirects=True)
        response.raise_for_status()

        # Check if we got HTML instead of binary data
        if response.content[:10].startswith(b'<!DOCTYPE') or response.content[:5].startswith(b'<html'):
            print("❌ Error: Received HTML page instead of file. The URL may be invalid or expired.")
            print("Please get a fresh download link from https://missionantyodaya.dord.gov.in/")
            return False

        # Save the file
        with open(temp_zip_path, 'wb') as f:
            f.write(response.content)
        print(f"✅ Downloaded: {temp_zip_path} ({len(response.content) / (1024*1024):.1f} MB)")

    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

    try:
        # Extract ZIP contents
        print("📦 Extracting files...")
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)

        # Load state mapping for renaming
        state_mapping = load_state_mapping()

        # Convert pipe-delimited files to comma-delimited and rename
        extracted_files = sorted(output_dir.glob("*.csv"), key=lambda x: int(x.stem) if x.stem.isdigit() else 999)
        print(f"📊 Found {len(extracted_files)} state files")

        converted_files = []
        for csv_file in extracted_files:
            state_code = int(csv_file.stem) if csv_file.stem.isdigit() else None
            state_name = state_mapping.get(state_code, f"state_{csv_file.stem}") if state_code else csv_file.stem

            # Convert the file
            print(f"🔄 Converting: {csv_file.name} -> {state_name}.csv")
            if convert_pipe_to_comma(csv_file):
                # Rename the file
                new_name = csv_file.parent / f"{state_name}.csv"
                csv_file.rename(new_name)
                converted_files.append(new_name)

        # Clean up temp file
        temp_zip_path.unlink()

        # Show summary
        total_size = sum(f.stat().st_size for f in converted_files)
        print(f"\n✅ Successfully processed Mission Antyodaya data:")
        print(f"   Year: {year}")
        print(f"   Files: {len(converted_files)}")
        print(f"   Total size: {total_size / (1024*1024):.1f} MB")
        print(f"   Location: {output_dir}/")
        return True

    except zipfile.BadZipFile:
        print(f"❌ Error: {temp_zip_path} is not a valid ZIP file.")
        print("The download link may have expired.")
        temp_zip_path.unlink()
        return False
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return False
def convert_pipe_to_comma(file_path: Path) -> bool:
    """Convert a pipe-delimited CSV file to comma-delimited.

    Returns:
        True if conversion was successful, False otherwise
    """
    try:
        # Read the file with pipe delimiter
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, delimiter='|')
            rows = list(reader)

        if not rows:
            print(f"  ⚠️  Skipping {file_path.name} - empty file")
            return False

        # Write back as comma-delimited
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        print(f"  ✅ Converted {file_path.name}: {len(rows)-1} rows, {len(rows[0])} columns")
        return True

    except Exception as e:
        print(f"  ❌ Error converting {file_path.name}: {e}")
        return False


def main():
    """Main CLI function for downloading Mission Antyodaya data."""
    parser = argparse.ArgumentParser(
        description="Download and process Mission Antyodaya data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all available years
  python download_mission_antyodaya.py

  # Download specific year
  python download_mission_antyodaya.py --year 2019

  # Download multiple years
  python download_mission_antyodaya.py --year 2019 2020 2023

  # Download from custom URL
  python download_mission_antyodaya.py --url "https://..." --year 2024

  # List available years
  python download_mission_antyodaya.py --list

Available years: """ + ", ".join(str(y) for y in sorted(MISSION_ANTYODAYA_LINKS.keys()))
    )

    parser.add_argument(
        '--year', '-y',
        type=int,
        nargs='*',
        help='Year(s) to download (default: all available)'
    )

    parser.add_argument(
        '--url', '-u',
        type=str,
        help='Custom download URL with transactionId (overrides year-based URLs)'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List available years and exit'
    )

    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default=ANTYODAYA_DATA_PATH,
        help=f'Output directory (default: {ANTYODAYA_DATA_PATH})'
    )

    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Show what would be downloaded without actually downloading'
    )

    args = parser.parse_args()

    # List available years and exit
    if args.list:
        print("Available Mission Antyodaya data years:")
        for year in sorted(MISSION_ANTYODAYA_LINKS.keys()):
            print(f"  {year}: {MISSION_ANTYODAYA_LINKS[year]}")
        return

    # Determine which years to download
    if args.url and args.year:
        # Custom URL with specified year
        years_to_download = args.year
        print(f"Using custom URL for year {args.year[0]}")
    elif args.year:
        # Specific years requested
        years_to_download = args.year
        missing_years = [y for y in years_to_download if y not in MISSION_ANTYODAYA_LINKS]
        if missing_years:
            print(f"❌ Error: No download links available for years: {missing_years}")
            print(f"Available years: {sorted(MISSION_ANTYODAYA_LINKS.keys())}")
            return
    else:
        # Download all available years
        years_to_download = sorted(MISSION_ANTYODAYA_LINKS.keys())
        print(f"Downloading all available years: {years_to_download}")

    # Process each year
    success_count = 0
    total_count = len(years_to_download)

    for year in years_to_download:
        if args.url and year == args.year[0]:
            # Use custom URL
            url = args.url
        else:
            # Use predefined URL
            url = MISSION_ANTYODAYA_LINKS[year]

        if args.dry_run:
            print(f"Would download year {year} from: {url}")
            success_count += 1
            continue

        if download_mission_antyodaya(url, year, args.output_dir):
            success_count += 1

    # Summary
    if not args.dry_run:
        print(f"\n{'='*50}")
        if success_count == total_count:
            print("🎉 All downloads completed successfully!")
        else:
            print(f"⚠️  Completed {success_count}/{total_count} downloads")
        print(f"Data location: {args.output_dir}")
        print(f"{'='*50}")


if __name__ == "__main__":
    main()