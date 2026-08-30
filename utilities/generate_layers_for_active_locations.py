import json
import requests
import time
import os
import argparse

"""
## How to run the script
python batch_generate_mws_layers.py --refresh --skip 80 --limit 20 gee_account_id 19 --start_year 2020 --end_year 2023

## Parameters
--refresh: Refresh the active locations list from the API without this parameter, the script will use the existing list.
--skip: Skip the first N locations
--limit: Limit the number of locations to process
--gee_account_id: GEE account ID to use
--start_year: Start year for analysis (optional)
--end_year: End year for analysis (optional)

## To Add in script
-- pass your bearer token for authentication.
-- API for which layer to be generated

"""

bearer_token = ""
layer_api = "http://localhost:8000/api/v1/tree_health_raster/"


def get_active_locations():
    url = "https://geoserver.core-stack.org/api/v1/proposed_blocks/"
    print(f"Fetching active locations from {url}...")

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Flatten the JSON
    flattened = []
    for state in data:
        for district in state.get("district", []):
            for block in district.get("blocks", []):
                flattened.append(
                    {
                        "state": state["label"],
                        "district": district["label"],
                        "block": block["label"],
                    }
                )

    # Save to JSON file
    with open("flattened_locations.json", "w", encoding="utf-8") as f:
        json.dump(flattened, f, ensure_ascii=False, indent=2)

    print(
        f"Flattened JSON saved to flattened_locations.json. Total active blocks: {len(flattened)}"
    )
    return flattened


def trigger_layer_generation(
    location, token, gee_account_id, start_year=None, end_year=None
):
    """Hits the generate_mws_layer API for a given location."""

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Build payload with required fields
    payload = {
        "state": location["state"],
        "district": location["district"],
        "block": location["block"],
        "gee_account_id": gee_account_id,
        "start_year": start_year,
        "end_year": end_year,
    }

    try:
        response = requests.post(layer_api, json=payload, headers=headers)

        # Check if the request was successful
        if response.ok:
            print(
                f"Successfully triggered for {location['district']} - {location['block']}. Response: {response.text}"
            )
        else:
            print(
                f"Failed for {location['district']} - {location['block']}. Status: {response.status_code}"
            )
            print(f"Error details: {response.text}")

    except Exception as e:
        print(
            f"Request failed for {location['district']} - {location['block']} with exception: {str(e)}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Batch generate MWS layers for active blocks"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of locations to process at a time",
    )
    parser.add_argument(
        "--skip", type=int, default=0, help="Number of starting locations to skip"
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force fetch from the API and refresh locations list",
    )
    parser.add_argument(
        "--gee_account_id",
        type=int,
        default=19,
        help="GEE account ID to use (default: 19)",
    )
    parser.add_argument(
        "--start_year",
        type=int,
        default=None,
        help="Start year for analysis (optional)",
    )
    parser.add_argument(
        "--end_year", type=int, default=None, help="End year for analysis (optional)"
    )

    args = parser.parse_args()

    active_locations_file = "flattened_locations.json"

    # Fetch from API if refreshing or if the file doesn't exist yet
    if args.refresh or not os.path.exists(active_locations_file):
        locations = get_active_locations()
    else:
        print(f"Reading active locations from existing '{active_locations_file}'...")
        with open(active_locations_file, "r", encoding="utf-8") as f:
            locations = json.load(f)
        print(f"Loaded {len(locations)} active locations from file.")

    # Apply skip if specified
    if args.skip and args.skip > 0:
        locations = locations[args.skip :]
        print(
            f"\nSkipping the first {args.skip} locations. Remaining: {len(locations)}"
        )

    # Apply limit if specified
    if args.limit and args.limit > 0:
        locations = locations[: args.limit]
        print(f"\nProcessing limited to {args.limit} locations.\n")

    # Show which parameters are being used
    params_used = [f"GEE Account ID: {args.gee_account_id}"]
    if args.start_year is not None:
        params_used.append(f"Start Year: {args.start_year}")
    if args.end_year is not None:
        params_used.append(f"End Year: {args.end_year}")

    # Loop through each location and trigger the generation
    for i, loc in enumerate(locations, 1):
        print(f"\n--- Processing location {i} of {len(locations)} ---")
        trigger_layer_generation(
            loc, bearer_token, args.gee_account_id, args.start_year, args.end_year
        )

        # Small delay between requests to be gentle to the API server
        if i < len(locations):  # Don't sleep after the last request
            time.sleep(2)


if __name__ == "__main__":
    main()
