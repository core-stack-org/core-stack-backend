"""
Livestock Census Pipeline

End-to-end pipeline to:
1. Download and parse the 20th Livestock Census (DAHD) data
2. Aggregate male + female counts per village
3. Fuzzy match village names to CoRE Stack village boundaries
4. Export matched data as CSV for further GEE integration
5. Report match statistics per state

Usage:
    python -m computing.misc.livestock_census.pipeline \
        --boundaries-dir /path/to/village_boundaries \
        --output-dir /path/to/output \
        --cache-dir /path/to/cache

Or import and use programmatically:
    from computing.misc.livestock_census.pipeline import run_pipeline
    results = run_pipeline(boundaries_dir="...", output_dir="...")
"""

import os
import argparse
import json
import pandas as pd

from .data_loader import load_livestock_data, aggregate_livestock_data
from .village_matcher import (
    load_village_boundaries,
    match_villages,
    match_stats_by_state,
)


def run_pipeline(
    boundaries_dir,
    output_dir,
    cache_dir=None,
    data_url=None,
    states=None,
    similarity_threshold=0.80,
):
    """Run the full livestock census data processing pipeline.

    Args:
        boundaries_dir: Path to directory with state-wise village boundary files
        output_dir: Path to write output CSV and stats
        cache_dir: Optional path to cache downloaded xlsx
        data_url: Override the default DAHD data URL
        states: List of state names to process (None = all)
        similarity_threshold: Minimum score for fuzzy matching

    Returns:
        dict with keys: matched_df, stats_by_state, overall_stats
    """
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Load and parse livestock census data
    print("=" * 60)
    print("Step 1: Loading livestock census data...")
    print("=" * 60)
    kwargs = {}
    if data_url:
        kwargs["url"] = data_url
    if cache_dir:
        kwargs["cache_dir"] = cache_dir

    raw_df = load_livestock_data(**kwargs)
    print(f"  Raw records: {len(raw_df)}")
    print(f"  States: {raw_df['state_name'].nunique()}")
    print(f"  Columns: {list(raw_df.columns)}")

    # Step 2: Aggregate male + female counts
    print("\n" + "=" * 60)
    print("Step 2: Aggregating livestock counts (male + female)...")
    print("=" * 60)
    agg_df = aggregate_livestock_data(raw_df)

    if states:
        states_lower = [s.lower().strip() for s in states]
        agg_df = agg_df[agg_df["state_name"].isin(states_lower)]
        print(f"  Filtered to {len(states)} states: {states}")

    # Save aggregated data
    agg_path = os.path.join(output_dir, "livestock_census_aggregated.csv")
    agg_df.to_csv(agg_path, index=False)
    print(f"  Saved aggregated data to {agg_path}")

    # Step 3: Load village boundaries
    print("\n" + "=" * 60)
    print("Step 3: Loading village boundaries...")
    print("=" * 60)
    boundaries_df = load_village_boundaries(boundaries_dir)
    print(f"  Boundary records: {len(boundaries_df)}")

    # Step 4: Match villages
    print("\n" + "=" * 60)
    print("Step 4: Matching villages (this may take a while)...")
    print("=" * 60)
    matched_df, overall_stats = match_villages(
        agg_df, boundaries_df, similarity_threshold=similarity_threshold
    )

    # Save matched data
    matched_path = os.path.join(output_dir, "livestock_census_matched.csv")
    matched_df.to_csv(matched_path, index=False)
    print(f"\n  Saved matched data to {matched_path}")

    # Step 5: Compute and save statistics
    print("\n" + "=" * 60)
    print("Step 5: Computing match statistics...")
    print("=" * 60)
    state_stats = match_stats_by_state(matched_df)

    stats_path = os.path.join(output_dir, "match_stats_by_state.csv")
    state_stats.to_csv(stats_path, index=False)

    overall_path = os.path.join(output_dir, "match_stats_overall.json")
    with open(overall_path, "w") as f:
        json.dump(overall_stats, f, indent=2)

    print(f"\n  Overall match statistics:")
    print(f"    Total records:   {overall_stats['total_records']}")
    print(f"    Exact matches:   {overall_stats['exact_matches']} ({overall_stats['exact_match_pct']}%)")
    print(f"    Fuzzy matches:   {overall_stats['fuzzy_matches']} ({overall_stats['fuzzy_match_pct']}%)")
    print(f"    Unmatched:       {overall_stats['unmatched']}")
    print(f"    Total match %:   {overall_stats['total_match_pct']}%")

    print(f"\n  Per-state stats saved to {stats_path}")
    print(f"  Overall stats saved to {overall_path}")

    return {
        "matched_df": matched_df,
        "stats_by_state": state_stats,
        "overall_stats": overall_stats,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Process 20th Livestock Census data and match to village boundaries"
    )
    parser.add_argument(
        "--boundaries-dir", required=True,
        help="Directory containing state-wise village boundary shapefiles"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory to write output files"
    )
    parser.add_argument(
        "--cache-dir", default=None,
        help="Directory to cache downloaded data"
    )
    parser.add_argument(
        "--data-url", default=None,
        help="Override default DAHD data URL"
    )
    parser.add_argument(
        "--states", nargs="*", default=None,
        help="Process only these states (space-separated)"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.80,
        help="Fuzzy match similarity threshold (default: 0.80)"
    )

    args = parser.parse_args()
    run_pipeline(
        boundaries_dir=args.boundaries_dir,
        output_dir=args.output_dir,
        cache_dir=args.cache_dir,
        data_url=args.data_url,
        states=args.states,
        similarity_threshold=args.threshold,
    )


if __name__ == "__main__":
    main()
