"""
Agriculture Census Pipeline

End-to-end pipeline to:
1. Scrape crop data from the Agriculture Census website
2. Clean and structure the data
3. Match tehsil names to CoRE Stack SOI boundaries
4. Export matched data as CSV for GEE integration

Usage:
    python -m computing.misc.agriculture_census.pipeline \
        --boundary-file /path/to/soi_tehsil.geojson \
        --output-dir /path/to/output \
        --states "Madhya Pradesh" "Rajasthan"
"""

import os
import argparse
import json
import pandas as pd
import geopandas as gpd

from .scraper import scrape_agcensus
from .tehsil_matcher import match_tehsils


def run_pipeline(
    boundary_file,
    output_dir,
    states=None,
    max_districts=None,
    headless=True,
    skip_scraping=False,
    scraped_csv=None,
):
    """Run the full agriculture census pipeline.

    Args:
        boundary_file: Path to SOI tehsil boundary GeoJSON
        output_dir: Path to write output files
        states: List of state names to process
        max_districts: Limit districts per state (for testing)
        headless: Run browser headless
        skip_scraping: If True, load from scraped_csv instead
        scraped_csv: Path to previously scraped data CSV

    Returns:
        dict with matched_df and stats
    """
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Scrape or load data
    print("=" * 60)
    print("Step 1: Getting agriculture census data...")
    print("=" * 60)

    if skip_scraping and scraped_csv:
        print(f"Loading previously scraped data from {scraped_csv}")
        census_df = pd.read_csv(scraped_csv)
    else:
        census_df = scrape_agcensus(
            output_dir=output_dir,
            states=states,
            max_districts=max_districts,
            headless=headless,
        )

    if census_df.empty:
        print("No data to process. Exiting.")
        return {"matched_df": census_df, "stats": {}}

    print(f"  Records: {len(census_df)}")

    # Step 2: Load SOI boundaries
    print("\n" + "=" * 60)
    print("Step 2: Loading SOI tehsil boundaries...")
    print("=" * 60)

    boundary_gdf = gpd.read_file(boundary_file)
    boundary_df = pd.DataFrame(boundary_gdf.drop(columns="geometry"))
    print(f"  Boundary records: {len(boundary_df)}")
    print(f"  Columns: {list(boundary_df.columns)}")

    # Step 3: Match tehsils
    print("\n" + "=" * 60)
    print("Step 3: Matching tehsil names...")
    print("=" * 60)

    matched_df, stats = match_tehsils(census_df, boundary_df)

    # Save outputs
    matched_path = os.path.join(output_dir, "agriculture_census_matched.csv")
    matched_df.to_csv(matched_path, index=False)

    stats_path = os.path.join(output_dir, "agri_census_match_stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\n  Match statistics:")
    print(f"    Total:     {stats['total']}")
    print(f"    Exact:     {stats['exact']}")
    print(f"    Fuzzy:     {stats['fuzzy']}")
    print(f"    Unmatched: {stats['unmatched']}")
    print(f"    Match %:   {stats['match_pct']}%")
    print(f"\n  Saved to {matched_path}")

    return {"matched_df": matched_df, "stats": stats}


def main():
    parser = argparse.ArgumentParser(
        description="Scrape and process Agriculture Census data"
    )
    parser.add_argument(
        "--boundary-file", required=True,
        help="Path to SOI tehsil boundary GeoJSON file"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to write output files"
    )
    parser.add_argument(
        "--states", nargs="*", default=None,
        help="States to process (space-separated)"
    )
    parser.add_argument(
        "--max-districts", type=int, default=None,
        help="Max districts per state (for testing)"
    )
    parser.add_argument(
        "--no-headless", action="store_true",
        help="Run browser with visible window"
    )
    parser.add_argument(
        "--skip-scraping", action="store_true",
        help="Skip scraping, load from --scraped-csv instead"
    )
    parser.add_argument(
        "--scraped-csv", default=None,
        help="Path to previously scraped CSV"
    )

    args = parser.parse_args()
    run_pipeline(
        boundary_file=args.boundary_file,
        output_dir=args.output_dir,
        states=args.states,
        max_districts=args.max_districts,
        headless=not args.no_headless,
        skip_scraping=args.skip_scraping,
        scraped_csv=args.scraped_csv,
    )


if __name__ == "__main__":
    main()
