#!/usr/bin/env python3
"""Raw-source processors for the facilities pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from facility_utils import (
    CLASS_K_COLUMNS,
    clean_identifier,
    clean_string_series,
    clean_text_series,
    coordinate_key,
    coordinate_status,
    enforce_numeric,
    parse_coordinate_pair,
    read_csv_selected,
    resolve_path,
    slugify,
    taxonomy_lookup,
    validate_pincode,
    validate_year,
)


log = logging.getLogger("facilities")

def source_paths(repo_root: Path, config: Dict[str, Any], source_cfg: Dict[str, Any]) -> List[Path]:
    raw_dir = resolve_path(config["pipeline"]["raw_dir"], repo_root)
    return [raw_dir / name for name in source_cfg.get("raw_files") or []]


def load_pincode_centroids(repo_root: Path, config: Dict[str, Any]) -> Dict[int, Tuple[float, float]]:
    path = resolve_path(config["references"]["pincode_centroids"], repo_root)
    if not path.exists():
        return {}
    df = read_csv_selected(path, usecols=["pin_code", "pin_lat", "pin_long"])
    df = df.dropna(subset=["pin_code", "pin_lat", "pin_long"])
    return {
        int(row.pin_code): (float(row.pin_lat), float(row.pin_long))
        for row in df.itertuples(index=False)
    }


def fill_coords_from_pincode(df: pd.DataFrame, lat_col: str, lon_col: str, pin_col: str, centroids: Dict[int, Tuple[float, float]]) -> pd.DataFrame:
    if not centroids or pin_col not in df.columns:
        return df
    bad = df[lat_col].isna() | df[lon_col].isna() | (df[lat_col] == 0) | (df[lon_col] == 0)
    if not bad.any():
        return df
    pins = pd.to_numeric(df.loc[bad, pin_col], errors="coerce")
    filled = 0
    for idx, pin in pins.dropna().items():
        pin_int = int(pin)
        if pin_int in centroids:
            df.at[idx, lat_col] = centroids[pin_int][0]
            df.at[idx, lon_col] = centroids[pin_int][1]
            filled += 1
    if filled:
        log.info("Filled %d coordinates from pincode centroids", filled)
    return df


def required_columns_for_table(source_cfg: Dict[str, Any]) -> List[str]:
    columns = [
        source_cfg.get("id_col"),
        source_cfg.get("name_col"),
        source_cfg.get("lat_col"),
        source_cfg.get("lon_col"),
        source_cfg.get("subtype_col"),
        source_cfg.get("facility_code_col"),
        source_cfg.get("urban_rural_col"),
        source_cfg.get("pincode_col"),
        source_cfg.get("establishment_year_col"),
        source_cfg.get("district_lgd_col"),
        source_cfg.get("village_census11_col"),
        source_cfg.get("village_name_col"),
        source_cfg.get("coord_fallback_col"),
    ]
    for item in (source_cfg.get("class_k") or {}).values():
        columns.append((item or {}).get("source"))
    columns.extend(source_cfg.get("clean_text_cols") or [])
    return [column for column in dict.fromkeys(columns) if column]


def add_class_k_fields(out: pd.DataFrame, raw: pd.DataFrame, source_cfg: Dict[str, Any]) -> None:
    for class_k in CLASS_K_COLUMNS:
        spec = (source_cfg.get("class_k") or {}).get(class_k) or {}
        source_col = spec.get("source")
        out[class_k] = clean_string_series(raw[source_col]) if source_col and source_col in raw.columns else pd.NA


def standard_facility_frame(
    raw: pd.DataFrame,
    source_cfg: Dict[str, Any],
    facility_class: str,
    tax: Dict[str, Any],
    subtype: Optional[pd.Series] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    id_col = source_cfg["id_col"]
    lat_col = source_cfg["lat_col"]
    lon_col = source_cfg["lon_col"]
    source_id = clean_identifier(raw[id_col]) if id_col in raw.columns else pd.Series(pd.NA, index=raw.index, dtype="string")
    missing_id = source_id.isna()
    if missing_id.any():
        source_id = source_id.fillna("__row_" + raw.index.astype(str).astype("string"))

    lat = pd.to_numeric(raw[lat_col], errors="coerce") if lat_col in raw.columns else pd.Series(pd.NA, index=raw.index)
    lon = pd.to_numeric(raw[lon_col], errors="coerce") if lon_col in raw.columns else pd.Series(pd.NA, index=raw.index)
    coord_col = source_cfg.get("coord_fallback_col")
    if coord_col and coord_col in raw.columns:
        missing = lat.isna() | lon.isna() | (lat == 0) | (lon == 0)
        if missing.any():
            parsed = raw.loc[missing, coord_col].map(parse_coordinate_pair)
            lon.loc[missing] = parsed.map(lambda item: item[0])
            lat.loc[missing] = parsed.map(lambda item: item[1])

    uid = source_cfg["facility_uid_prefix"] + ":" + source_id.astype(str)
    if source_cfg.get("uid_strategy") == "id_coord":
        uid = uid + ":" + coordinate_key(lat) + ":" + coordinate_key(lon)

    out = pd.DataFrame(index=raw.index)
    out["facility_uid"] = uid
    out["source_id"] = source_id
    out["latitude"] = lat
    out["longitude"] = lon
    out["facility_name"] = clean_string_series(raw[source_cfg["name_col"]]) if source_cfg.get("name_col") in raw.columns else pd.NA
    for col in source_cfg.get("clean_text_cols") or []:
        if col == source_cfg.get("name_col") and "facility_name" in out:
            out["facility_name"] = clean_text_series(raw[col])
    out["facility_code"] = clean_string_series(raw[source_cfg["facility_code_col"]]) if source_cfg.get("facility_code_col") in raw.columns else pd.NA
    out["class_l1_domain"] = tax["class_l1_domain"]
    out["class_l2_filter_group"] = tax["class_l2_filter_group"]
    out["class_l3_facility_class"] = facility_class
    if subtype is not None:
        out["class_l4_facility_subtype"] = clean_string_series(subtype)
    elif source_cfg.get("subtype_col") in raw.columns:
        out["class_l4_facility_subtype"] = clean_string_series(raw[source_cfg["subtype_col"]])
    else:
        out["class_l4_facility_subtype"] = pd.NA
    add_class_k_fields(out, raw, source_cfg)
    out["urban_rural"] = clean_string_series(raw[source_cfg["urban_rural_col"]]) if source_cfg.get("urban_rural_col") in raw.columns else pd.NA
    out["pincode"] = validate_pincode(raw[source_cfg["pincode_col"]]) if source_cfg.get("pincode_col") in raw.columns else pd.NA
    out["establishment_year"] = validate_year(raw[source_cfg["establishment_year_col"]]) if source_cfg.get("establishment_year_col") in raw.columns else pd.NA
    out["district_lgd"] = enforce_numeric(raw[source_cfg["district_lgd_col"]]).astype("Int64") if source_cfg.get("district_lgd_col") in raw.columns else pd.NA
    out["village_census11"] = clean_identifier(raw[source_cfg["village_census11_col"]]) if source_cfg.get("village_census11_col") in raw.columns else pd.NA
    out["village_name"] = clean_string_series(raw[source_cfg["village_name_col"]]) if source_cfg.get("village_name_col") in raw.columns else pd.NA
    out["coordinate_status"] = coordinate_status(out["latitude"], out["longitude"])
    return out, membership_from_facilities(out)


def membership_from_facilities(facilities: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "facility_uid",
        "class_l1_domain",
        "class_l2_filter_group",
        "class_l3_facility_class",
        "class_l4_facility_subtype",
        *CLASS_K_COLUMNS,
    ]
    return facilities[columns].copy()


def append_csv(df: pd.DataFrame, path: Path, header: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, mode="a", index=False, header=header)


def split_valid_invalid(facilities: pd.DataFrame, memberships: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    valid_mask = facilities["coordinate_status"] == "valid"
    invalid = facilities.loc[~valid_mask].copy()
    valid = facilities.loc[valid_mask].copy()
    memberships = memberships[memberships["facility_uid"].isin(set(valid["facility_uid"]))].copy()
    valid = valid.drop(columns=["coordinate_status", "source_id"], errors="ignore")
    return valid, memberships, invalid


def process_table_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    source_cfg: Dict[str, Any],
    sample_rows: Optional[int],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = source_paths(repo_root, config, source_cfg)[0]
    raw = read_csv_selected(path, usecols=required_columns_for_table(source_cfg), nrows=sample_rows)
    if source_cfg.get("pincode_col") and source_cfg.get("lat_col") in raw.columns and source_cfg.get("lon_col") in raw.columns:
        centroids = load_pincode_centroids(repo_root, config)
        raw[source_cfg["lat_col"]] = pd.to_numeric(raw[source_cfg["lat_col"]], errors="coerce")
        raw[source_cfg["lon_col"]] = pd.to_numeric(raw[source_cfg["lon_col"]], errors="coerce")
        raw = fill_coords_from_pincode(raw, source_cfg["lat_col"], source_cfg["lon_col"], source_cfg["pincode_col"], centroids)
    lookup = taxonomy_lookup(config)
    facility_class = source_cfg["facility_class"]
    facilities, memberships = standard_facility_frame(raw, source_cfg, facility_class, lookup[facility_class])
    return split_valid_invalid(facilities, memberships)


def process_apmc_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    source_cfg: Dict[str, Any],
    sample_rows: Optional[int],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = source_paths(repo_root, config, source_cfg)[0]
    raw = read_csv_selected(
        path,
        usecols=["gid", "mandi_code", "fatehabad", "market_cat", "district_n", "lat", "long"],
        nrows=sample_rows,
    )
    apmc_mapping = (config.get("mappings") or {}).get("apmc") or {}
    category_normalise = apmc_mapping.get("category_normalise") or {}
    category_labels = apmc_mapping.get("category_labels") or {}
    raw["apmc_category"] = raw["market_cat"].fillna("Other").replace(category_normalise).map(category_labels).fillna("Other")
    raw["apmc_name"] = clean_text_series(raw["fatehabad"]).fillna("") + " APMC"
    raw = raw.rename(columns={"gid": "id", "lat": "latitude", "long": "longitude", "mandi_code": "facility_code", "district_n": "village_name"})
    local_cfg = {
        **source_cfg,
        "id_col": "id",
        "name_col": "apmc_name",
        "lat_col": "latitude",
        "lon_col": "longitude",
        "facility_code_col": "facility_code",
        "village_name_col": "village_name",
    }
    lookup = taxonomy_lookup(config)
    facilities, memberships = standard_facility_frame(raw, local_cfg, "apmc", lookup["apmc"], subtype=raw["apmc_category"])
    return split_valid_invalid(facilities, memberships)


def process_health_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    source_cfg: Dict[str, Any],
    sample_rows: Optional[int],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = source_paths(repo_root, config, source_cfg)[0]
    raw = read_csv_selected(path, usecols=["FID", "facility_t", "facility_n", "coordinates"], nrows=sample_rows)
    parsed = raw["coordinates"].map(parse_coordinate_pair)
    raw["longitude"] = parsed.map(lambda item: item[0])
    raw["latitude"] = parsed.map(lambda item: item[1])
    raw["facility_t"] = clean_string_series(raw["facility_t"])
    frames: List[pd.DataFrame] = []
    memberships: List[pd.DataFrame] = []
    invalid: List[pd.DataFrame] = []
    lookup = taxonomy_lookup(config)
    for source_type, facility_class in source_cfg["type_to_class"].items():
        sub = raw[raw["facility_t"] == source_type].copy()
        if sub.empty:
            continue
        local_cfg = {
            **source_cfg,
            "id_col": "FID",
            "name_col": "facility_n",
            "lat_col": "latitude",
            "lon_col": "longitude",
            "facility_uid_prefix": f"health_{source_type}",
        }
        f, m = standard_facility_frame(sub, local_cfg, facility_class, lookup[facility_class])
        vf, vm, inv = split_valid_invalid(f, m)
        frames.append(vf)
        memberships.append(vm)
        invalid.append(inv)
    return (
        pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(),
        pd.concat(memberships, ignore_index=True, sort=False) if memberships else pd.DataFrame(),
        pd.concat(invalid, ignore_index=True, sort=False) if invalid else pd.DataFrame(),
    )


def process_agri_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    source_cfg: Dict[str, Any],
    sample_rows: Optional[int],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    frames = []
    for path in source_paths(repo_root, config, source_cfg):
        part = read_csv_selected(path, usecols=["facility_i", "fac_desc", "dt_lgd", "lattitude", "longitude", "subtype"], nrows=sample_rows)
        part["raw_file"] = path.name
        frames.append(part)
    raw = pd.concat(frames, ignore_index=True, sort=False)
    agri_mapping = (config.get("mappings") or {}).get("agri_industry") or {}
    subtype_to_category = {
        subtype: category
        for category, subtypes in (agri_mapping.get("subtype_categories") or {}).items()
        for subtype in (subtypes or [])
    }
    category_to_class = agri_mapping.get("category_to_class") or {}
    raw["reclassified_category"] = raw["subtype"].map(subtype_to_category).fillna("Industrial Manufacturing")
    raw["facility_class"] = raw["reclassified_category"].map(category_to_class).fillna("agri_industry_industrial_manufacturing")
    raw = raw.rename(
        columns={
            "facility_i": "id",
            "fac_desc": "facility_name",
            "dt_lgd": "district_lgd",
            "lattitude": "latitude",
            "longitude": "longitude",
        }
    )
    raw["facility_name"] = clean_text_series(raw["facility_name"])
    lookup = taxonomy_lookup(config)
    frames_out: List[pd.DataFrame] = []
    memberships_out: List[pd.DataFrame] = []
    invalid_out: List[pd.DataFrame] = []
    for facility_class, sub in raw.groupby("facility_class", sort=True):
        local_cfg = {
            **source_cfg,
            "id_col": "id",
            "name_col": "facility_name",
            "lat_col": "latitude",
            "lon_col": "longitude",
            "district_lgd_col": "district_lgd",
            "uid_strategy": "id_coord",
        }
        f, m = standard_facility_frame(sub, local_cfg, facility_class, lookup[facility_class], subtype=sub["subtype"])
        vf, vm, inv = split_valid_invalid(f, m)
        frames_out.append(vf)
        memberships_out.append(vm)
        invalid_out.append(inv)
    return (
        pd.concat(frames_out, ignore_index=True, sort=False),
        pd.concat(memberships_out, ignore_index=True, sort=False),
        pd.concat(invalid_out, ignore_index=True, sort=False),
    )


def process_school_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    source_cfg: Dict[str, Any],
    sample_rows: Optional[int],
    chunksize: int,
    facility_path: Path,
    membership_path: Path,
    invalid_path: Path,
) -> Dict[str, Any]:
    path = source_paths(repo_root, config, source_cfg)[0]
    usecols = ["lgd_distri", "vilcode11", "vilname", "schcd", "schname", "school_cat", "schcat", "management", "schmgt", "latitude", "longitude"]
    lookup = taxonomy_lookup(config)
    school_mapping = (config.get("mappings") or {}).get("school") or {}
    category_labels = {int(key): value for key, value in (school_mapping.get("category_labels") or {}).items()}
    category_to_classes = {
        int(key): list(value or [])
        for key, value in (school_mapping.get("category_to_classes") or {}).items()
    }
    header_f = header_m = header_i = True
    input_rows = valid_rows = membership_rows = invalid_rows = 0
    reader = pd.read_csv(path, usecols=usecols, chunksize=chunksize, low_memory=False, nrows=sample_rows)
    for chunk in reader:
        input_rows += len(chunk)
        chunk["schcat_num"] = pd.to_numeric(chunk["schcat"], errors="coerce").astype("Int64")
        missing_category = clean_string_series(chunk["school_cat"]).isna()
        if missing_category.any():
            chunk.loc[missing_category, "school_cat"] = chunk.loc[missing_category, "schcat_num"].map(category_labels)
        levels = chunk["schcat_num"].map(lambda code: category_to_classes.get(int(code), []) if pd.notna(code) else [])
        primary_class = levels.map(lambda items: items[0] if items else pd.NA)
        keep = primary_class.notna()
        chunk = chunk.loc[keep].copy()
        levels = levels.loc[keep]
        primary_class = primary_class.loc[keep]
        if chunk.empty:
            continue

        local_cfg = {
            **source_cfg,
            "id_col": "schcd",
            "name_col": "schname",
            "lat_col": "latitude",
            "lon_col": "longitude",
            "facility_code_col": "schcd",
            "district_lgd_col": "lgd_distri",
            "village_census11_col": "vilcode11",
            "village_name_col": "vilname",
            "clean_text_cols": ["schname"],
        }
        facilities, _ = standard_facility_frame(chunk, local_cfg, primary_class.iloc[0], lookup[primary_class.iloc[0]], subtype=chunk["school_cat"])
        facilities["class_l1_domain"] = primary_class.map(lambda cls: lookup[cls]["class_l1_domain"])
        facilities["class_l2_filter_group"] = primary_class.map(lambda cls: lookup[cls]["class_l2_filter_group"])
        facilities["class_l3_facility_class"] = primary_class.values

        memberships = []
        for facility_class in sorted(set(cls for items in levels for cls in items)):
            mask = levels.map(lambda items: facility_class in items)
            part = facilities.loc[mask].copy()
            part["class_l1_domain"] = lookup[facility_class]["class_l1_domain"]
            part["class_l2_filter_group"] = lookup[facility_class]["class_l2_filter_group"]
            part["class_l3_facility_class"] = facility_class
            memberships.append(membership_from_facilities(part))
        membership_df = pd.concat(memberships, ignore_index=True, sort=False)
        valid, membership_df, invalid = split_valid_invalid(facilities, membership_df)
        append_csv(valid, facility_path, header_f)
        append_csv(membership_df, membership_path, header_m)
        if not invalid.empty:
            append_csv(invalid, invalid_path, header_i)
            header_i = False
        header_f = header_m = False
        valid_rows += len(valid)
        membership_rows += len(membership_df)
        invalid_rows += len(invalid)
        log.info("Processed school rows=%d valid=%d memberships=%d", input_rows, valid_rows, membership_rows)
    return {
        "source_key": source_key,
        "input_rows": input_rows,
        "valid_rows": valid_rows,
        "membership_rows": membership_rows,
        "invalid_rows": invalid_rows,
    }


PROCESSORS = {
    "table": process_table_source,
    "apmc": process_apmc_source,
    "health_center_split": process_health_source,
    "agri_industry_reclassified": process_agri_source,
}


def process_source(
    repo_root: Path,
    config: Dict[str, Any],
    source_key: str,
    sample_rows: Optional[int],
    chunksize: int,
) -> Dict[str, Any]:
    source_cfg = config["sources"][source_key]
    intermediate = resolve_path(config["pipeline"]["intermediate_dir"], repo_root)
    facility_path = intermediate / "source_facilities" / f"{source_key}.csv"
    membership_path = intermediate / "source_memberships" / f"{source_key}.csv"
    invalid_path = intermediate / "invalid" / f"{source_key}.csv"
    for path in [facility_path, membership_path, invalid_path]:
        if path.exists():
            path.unlink()

    if source_cfg["processor"] == "school_mhrd":
        return process_school_source(repo_root, config, source_key, source_cfg, sample_rows, chunksize, facility_path, membership_path, invalid_path)

    processor = PROCESSORS[source_cfg["processor"]]
    facilities, memberships, invalid = processor(repo_root, config, source_key, source_cfg, sample_rows)
    append_csv(facilities, facility_path, True)
    append_csv(memberships, membership_path, True)
    if not invalid.empty:
        append_csv(invalid, invalid_path, True)
    return {
        "source_key": source_key,
        "input_rows": int(len(facilities) + len(invalid)),
        "valid_rows": int(len(facilities)),
        "membership_rows": int(len(memberships)),
        "invalid_rows": int(len(invalid)),
    }
