#!/usr/bin/env python3
"""Clean raw facility sources into pipeline intermediates."""

from __future__ import annotations

import argparse
from pathlib import Path

from facility_utils import find_repo_root
from facility_pipeline import run_clean


def main() -> None:
    repo_root = find_repo_root(Path(__file__).resolve())
    parser = argparse.ArgumentParser(description="Process raw facility sources into per-source intermediates.")
    parser.add_argument(
        "--config",
        type=Path,
        default=repo_root / "utilities" / "scripts" / "facilities_utils" / "config" / "facilities_master.yaml",
    )
    parser.add_argument("--source", type=str, default=None)
    parser.add_argument("--sample-rows", type=int, default=None)
    parser.add_argument("--chunksize", type=int, default=500_000)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run_clean(args)


if __name__ == "__main__":
    main()
