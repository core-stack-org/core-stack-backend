#!/usr/bin/env python3
"""CLI orchestrator for the pan-India facilities pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from facility_cleaners import process_source, source_paths
from facility_utils import (
    CLASS_K_COLUMNS,
    add_attribute_table_to_gpkg,
    ensure_sqlite_indexes,
    file_fingerprint,
    find_repo_root,
    read_yaml,
    resolve_path,
    setup_logging,
    taxonomy_rows,
    write_yaml,
)


log = logging.getLogger("facilities")


def load_config(config_path: Path) -> tuple[Path, Dict[str, Any]]:
    repo_root = find_repo_root(config_path.resolve())
    return repo_root, read_yaml(config_path)


def state_path(repo_root: Path, config: Dict[str, Any]) -> Path:
    return resolve_path(config["pipeline"]["state_path"], repo_root)


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"sources": {}, "runs": []}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def source_config_fingerprint(repo_root: Path, config: Dict[str, Any], source_key: str, config_path: Path) -> str:
    paths = source_paths(repo_root, config, config["sources"][source_key])
    paths = [*paths, config_path]
    return file_fingerprint(paths)


def active_sources(config: Dict[str, Any], requested: Optional[str]) -> List[str]:
    sources = []
    for key, source in config.get("sources", {}).items():
        if requested and key != requested:
            continue
        if source.get("active", True):
            sources.append(key)
    if requested and not sources:
        raise KeyError(f"Unknown or inactive source: {requested}")
    return sources


def run_clean(args: argparse.Namespace) -> None:
    repo_root, config = load_config(args.config)
    setup_logging(args.debug, resolve_path(config["pipeline"]["log_path"], repo_root))
    state_file = state_path(repo_root, config)
    state = load_state(state_file)
    summaries = []

    for source_key in active_sources(config, args.source):
        fingerprint = source_config_fingerprint(repo_root, config, source_key, args.config)
        previous = state["sources"].get(source_key, {})
        if not args.force and not args.sample_rows and previous.get("fingerprint") == fingerprint:
            log.info("Skipping unchanged source: %s", source_key)
            summaries.append(previous.get("summary", {"source_key": source_key, "skipped": True}))
            continue
        log.info("Processing source: %s", source_key)
        summary = process_source(
            repo_root=repo_root,
            config=config,
            source_key=source_key,
            sample_rows=args.sample_rows,
            chunksize=args.chunksize,
        )
        summary["fingerprint"] = fingerprint
        summary["processed_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        summary["sample_rows"] = args.sample_rows
        state["sources"][source_key] = {"fingerprint": fingerprint, "summary": summary}
        summaries.append(summary)
        save_state(state_file, state)

    source_summary_path = resolve_path(config["outputs"]["source_summary_csv"], repo_root)
    source_summary_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([item.get("summary", item) for item in state["sources"].values()]).to_csv(source_summary_path, index=False)
    log.info("Saved source summary: %s", source_summary_path)
    if not getattr(args, "skip_monitor", False):
        from facility_metadata_monitor import write_monitor

        monitor_path = write_monitor(args.config)
        log.info("Updated metadata monitor: %s", monitor_path)


def concat_csvs(paths: List[Path], out_path: Path, columns: Optional[List[str]] = None) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    header = True
    for path in paths:
        if not path.exists():
            continue
        for chunk in pd.read_csv(path, chunksize=250_000, low_memory=False):
            if columns:
                for column in columns:
                    if column not in chunk.columns:
                        chunk[column] = pd.NA
                chunk = chunk[columns]
            chunk.to_csv(out_path, mode="a", index=False, header=header)
            header = False


def membership_counts(membership_csv: Path) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for chunk in pd.read_csv(membership_csv, usecols=["facility_uid", "class_l3_facility_class"], chunksize=500_000, low_memory=False):
        chunk = chunk.drop_duplicates()
        part = chunk.groupby("facility_uid", sort=False).size()
        for uid, count in part.items():
            counts[uid] = counts.get(uid, 0) + int(count)
    return counts


def build_facility_csv(repo_root: Path, config: Dict[str, Any]) -> None:
    intermediate = resolve_path(config["pipeline"]["intermediate_dir"], repo_root)
    output_facilities = resolve_path(config["outputs"]["facilities_csv"], repo_root)
    output_memberships = resolve_path(config["outputs"]["memberships_csv"], repo_root)
    invalid_output = resolve_path(config["outputs"]["invalid_coordinates_csv"], repo_root)
    taxonomy_output = resolve_path(config["outputs"]["taxonomy_csv"], repo_root)

    source_keys = [key for key, source in config["sources"].items() if source.get("active", True)]
    facility_paths = [intermediate / "source_facilities" / f"{key}.csv" for key in source_keys]
    membership_paths = [intermediate / "source_memberships" / f"{key}.csv" for key in source_keys]
    invalid_paths = [intermediate / "invalid" / f"{key}.csv" for key in source_keys]

    concat_csvs(membership_paths, output_memberships, config["schema"]["membership_columns"])
    counts = membership_counts(output_memberships)

    output_facilities.parent.mkdir(parents=True, exist_ok=True)
    if output_facilities.exists():
        output_facilities.unlink()
    header = True
    columns = config["schema"]["facility_columns"]
    seen: set[str] = set()
    for path in facility_paths:
        if not path.exists():
            log.warning("Missing source facility intermediate: %s", path)
            continue
        for chunk in pd.read_csv(path, chunksize=250_000, low_memory=False):
            if "facility_uid" not in chunk.columns:
                continue
            chunk = chunk[~chunk["facility_uid"].isin(seen)].copy()
            seen.update(chunk["facility_uid"].dropna().astype(str).tolist())
            chunk["membership_count"] = chunk["facility_uid"].map(counts).fillna(1).astype("Int64")
            for column in columns:
                if column not in chunk.columns:
                    chunk[column] = pd.NA
            chunk[columns].to_csv(output_facilities, mode="a", index=False, header=header)
            header = False
    concat_csvs(invalid_paths, invalid_output)
    pd.DataFrame(taxonomy_rows(config)).to_csv(taxonomy_output, index=False)
    log.info("Saved facilities CSV: %s", output_facilities)
    log.info("Saved memberships CSV: %s", output_memberships)
    log.info("Saved taxonomy CSV: %s", taxonomy_output)


def write_large_attribute_table(gpkg_path: Path, table_name: str, csv_path: Path) -> None:
    if not csv_path.exists():
        return
    with sqlite3.connect(gpkg_path) as con:
        first = True
        for chunk in pd.read_csv(csv_path, chunksize=250_000, low_memory=False):
            chunk.to_sql(table_name, con, if_exists="replace" if first else "append", index=False)
            first = False
        con.execute(
            """
            insert or replace into gpkg_contents
            (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id)
            values (?, 'attributes', ?, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), null, null, null, null, null)
            """,
            (table_name, table_name),
        )
        con.commit()


def write_gpkg(repo_root: Path, config: Dict[str, Any], chunksize: int) -> None:
    try:
        import geopandas as gpd
        import pyogrio
    except ImportError as exc:
        raise RuntimeError("GeoPackage output requires geopandas, shapely, and pyogrio.") from exc

    facility_csv = resolve_path(config["outputs"]["facilities_csv"], repo_root)
    membership_csv = resolve_path(config["outputs"]["memberships_csv"], repo_root)
    taxonomy_csv = resolve_path(config["outputs"]["taxonomy_csv"], repo_root)
    source_summary_csv = resolve_path(config["outputs"]["source_summary_csv"], repo_root)
    invalid_csv = resolve_path(config["outputs"]["invalid_coordinates_csv"], repo_root)
    gpkg_path = resolve_path(config["outputs"]["facilities_gpkg"], repo_root)
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    if gpkg_path.exists():
        gpkg_path.unlink()

    append = False
    total = 0
    text_columns = [
        "facility_uid",
        "facility_name",
        "facility_code",
        "class_l1_domain",
        "class_l2_filter_group",
        "class_l3_facility_class",
        "class_l4_facility_subtype",
        *CLASS_K_COLUMNS,
        "urban_rural",
        "village_census11",
        "village_name",
    ]
    integer_columns = ["pincode", "establishment_year", "district_lgd", "membership_count"]
    for chunk in pd.read_csv(facility_csv, chunksize=chunksize, low_memory=False):
        chunk["latitude"] = pd.to_numeric(chunk["latitude"], errors="coerce")
        chunk["longitude"] = pd.to_numeric(chunk["longitude"], errors="coerce")
        chunk = chunk.dropna(subset=["latitude", "longitude"])
        for column in text_columns:
            if column in chunk.columns:
                chunk[column] = chunk[column].astype("string")
        for column in integer_columns:
            if column in chunk.columns:
                chunk[column] = pd.to_numeric(chunk[column], errors="coerce").astype("Int64")
        geometry = gpd.points_from_xy(chunk["longitude"], chunk["latitude"], crs=config["schema"]["crs"])
        gdf = gpd.GeoDataFrame(chunk, geometry=geometry, crs=config["schema"]["crs"])
        pyogrio.write_dataframe(gdf, gpkg_path, layer=config["schema"]["facility_layer"], driver="GPKG", append=append)
        append = True
        total += len(gdf)
        log.info("Wrote %d facility points", total)

    write_large_attribute_table(gpkg_path, config["schema"]["membership_table"], membership_csv)
    add_attribute_table_to_gpkg(gpkg_path, config["schema"]["taxonomy_table"], pd.read_csv(taxonomy_csv))
    if source_summary_csv.exists():
        add_attribute_table_to_gpkg(gpkg_path, config["schema"]["source_summary_table"], pd.read_csv(source_summary_csv))
    if invalid_csv.exists():
        add_attribute_table_to_gpkg(gpkg_path, config["schema"]["invalid_table"], pd.read_csv(invalid_csv, low_memory=False))
    ensure_sqlite_indexes(gpkg_path)
    log.info("Saved facilities GeoPackage: %s (%d points)", gpkg_path, total)


def run_build(args: argparse.Namespace) -> None:
    repo_root, config = load_config(args.config)
    setup_logging(args.debug, resolve_path(config["pipeline"]["log_path"], repo_root))
    if not getattr(args, "gpkg_only", False):
        build_facility_csv(repo_root, config)
    if not args.skip_gpkg:
        write_gpkg(repo_root, config, args.gpkg_chunksize)
    if not getattr(args, "skip_monitor", False):
        from facility_metadata_monitor import write_monitor

        monitor_path = write_monitor(args.config)
        log.info("Updated metadata monitor: %s", monitor_path)


def run_proximity(args: argparse.Namespace) -> None:
    from facility_proximity_finder import run_from_args

    run_from_args(args)


def run_all(args: argparse.Namespace) -> None:
    original_skip_monitor = getattr(args, "skip_monitor", False)
    args.skip_monitor = True
    run_clean(args)
    run_build(args)
    if args.with_proximity:
        run_proximity(args)
    args.skip_monitor = original_skip_monitor
    if not args.skip_monitor:
        from facility_metadata_monitor import write_monitor

        monitor_path = write_monitor(args.config)
        log.info("Updated metadata monitor: %s", monitor_path)


def run_monitor(args: argparse.Namespace) -> None:
    from facility_metadata_monitor import write_monitor

    monitor_path = write_monitor(args.config)
    print(monitor_path)


def build_parser() -> argparse.ArgumentParser:
    repo_root = find_repo_root(Path(__file__).resolve())
    default_config = repo_root / "utilities" / "scripts" / "facilities_utils" / "config" / "facilities_master.yaml"
    parser = argparse.ArgumentParser(description="Build and maintain the pan-India facilities pipeline.")
    parser.add_argument("--config", type=Path, default=default_config)
    parser.add_argument("--debug", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    clean = sub.add_parser("clean", help="Process raw sources into per-source intermediates.")
    clean.add_argument("--source", type=str, default=None)
    clean.add_argument("--sample-rows", type=int, default=None)
    clean.add_argument("--chunksize", type=int, default=500_000)
    clean.add_argument("--force", action="store_true")
    clean.add_argument("--skip-monitor", action="store_true")
    clean.set_defaults(func=run_clean)

    build = sub.add_parser("build", help="Build CSV outputs and the facilities GPKG from intermediates.")
    build.add_argument("--skip-gpkg", action="store_true")
    build.add_argument("--gpkg-only", action="store_true")
    build.add_argument("--gpkg-chunksize", type=int, default=250_000)
    build.add_argument("--skip-monitor", action="store_true")
    build.set_defaults(func=run_build)

    prox = sub.add_parser("proximity", help="Build village-to-facility proximity GPKG.")
    add_proximity_args(prox)
    prox.set_defaults(func=run_proximity)

    all_cmd = sub.add_parser("all", help="Run clean, build, and optionally proximity.")
    all_cmd.add_argument("--source", type=str, default=None)
    all_cmd.add_argument("--sample-rows", type=int, default=None)
    all_cmd.add_argument("--chunksize", type=int, default=500_000)
    all_cmd.add_argument("--force", action="store_true")
    all_cmd.add_argument("--skip-gpkg", action="store_true")
    all_cmd.add_argument("--gpkg-chunksize", type=int, default=250_000)
    all_cmd.add_argument("--with-proximity", action="store_true")
    add_proximity_args(all_cmd, include_force=False)
    all_cmd.set_defaults(func=run_all)

    monitor = sub.add_parser("monitor", help="Refresh utilities/scripts/facilities_utils/config/facilities_metadata_monitor.yaml.")
    monitor.set_defaults(func=run_monitor)
    return parser


def add_proximity_args(parser: argparse.ArgumentParser, include_force: bool = True) -> None:
    parser.add_argument("--classes", type=str, default=None, help="Comma-separated L3 classes to compute.")
    parser.add_argument("--sample-villages", type=int, default=None)
    parser.add_argument("--sample-classes", type=int, default=None)
    parser.add_argument("--village-chunksize", type=int, default=100_000)
    parser.add_argument("--output-gpkg", type=Path, default=None)
    parser.add_argument("--materialize-derived", action="store_true")
    parser.add_argument("--no-derived-views", action="store_true")
    parser.add_argument("--refresh-derived-only", action="store_true", help="Rebuild class-map and L1/L2 derived outputs from existing L3 rows.")
    parser.add_argument("--skip-monitor", action="store_true")
    if include_force:
        parser.add_argument("--force", action="store_true")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
