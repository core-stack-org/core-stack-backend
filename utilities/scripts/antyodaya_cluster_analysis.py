#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans


os.environ.setdefault("OMP_NUM_THREADS", "1")

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "antyodaya"
DEFAULT_MAPPING_CONFIG = DATA_DIR / "mappings" / "antyodaya_variable_mapping.json"
DEFAULT_OUTPUT_DIR = DATA_DIR / "output"

RANDOM_STATE = 42
FEATURE_SAMPLE_SIZE = 50_000
FEATURE_PREDICT_CHUNK = 50_000
WRITE_CHUNK = 50_000

INT_TO_LABEL = {0: "Low", 1: "Medium", 2: "High"}
LABEL_TO_SCORE = {"Low": 0.0, "Medium": 0.5, "High": 1.0}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cluster Mission Antyodaya 2020 raw files using the clean variable mapping config."
    )
    parser.add_argument("--mapping-config", type=Path, default=DEFAULT_MAPPING_CONFIG)
    parser.add_argument("--raw-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def as_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def label_counts(labels: np.ndarray) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for value in np.unique(labels):
        counts[INT_TO_LABEL[int(value)]] = int((labels == value).sum())
    return dict(sorted(counts.items()))


def shannon_entropy(distribution: dict[str, float | int], *, probabilities: bool = False) -> float:
    values = np.array(list(distribution.values()), dtype=np.float64)
    if values.size == 0:
        return 0.0
    probs = values if probabilities else values / max(float(values.sum()), 1e-9)
    probs = probs[probs > 0]
    if probs.size == 0:
        return 0.0
    return float(-(probs * np.log2(probs)).sum())


def normalize_values(values: np.ndarray, inverse: bool = False) -> np.ndarray:
    arr = values.astype(np.float32, copy=True)
    if inverse:
        np.subtract(1.0, arr, out=arr)
    min_val = float(np.nanmin(arr)) if arr.size else 0.0
    max_val = float(np.nanmax(arr)) if arr.size else 0.0
    arr[~np.isfinite(arr)] = 0.0
    if max_val > min_val:
        arr -= min_val
        arr /= max_val - min_val
    return arr


def ratio_values(df: pd.DataFrame, numerator: str, denominator: str, inverse: bool = False) -> np.ndarray:
    num = numeric(df[numerator]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    den = numeric(df[denominator]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    np.clip(den, 1e-9, None, out=den)
    np.divide(num, den, out=num)
    return normalize_values(num, inverse=inverse)


def count_values(df: pd.DataFrame, column: str, inverse: bool = False) -> np.ndarray:
    values = numeric(df[column]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    return normalize_values(values, inverse=inverse)


def clipped_ratio_score(df: pd.DataFrame, numerator: str, denominator: str) -> np.ndarray:
    num = numeric(df[numerator]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    den = numeric(df[denominator]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    np.clip(den, 1e-9, None, out=den)
    np.divide(num, den, out=num)
    np.clip(num, 0.0, 1.0, out=num)
    return num


def ratio_score(df: pd.DataFrame, definition: dict[str, Any]) -> np.ndarray:
    if definition.get("undefined") == "null":
        num = numeric(df[definition["numerator"]]).fillna(0).to_numpy(dtype=np.float32, copy=True)
        den = numeric(df[definition["denominator"]]).fillna(0).to_numpy(dtype=np.float32, copy=True)
        valid = den > 0
        values = np.full(len(df), np.nan, dtype=np.float32)
        safe_den = den[valid].copy()
        np.clip(safe_den, 1e-9, None, out=safe_den)
        values[valid] = num[valid] / safe_den
        np.clip(values, 0.0, 1.0, out=values)
    else:
        values = clipped_ratio_score(df, definition["numerator"], definition["denominator"])
    if definition.get("invert", False):
        np.subtract(1.0, values, out=values)
    return values


def difference_score(df: pd.DataFrame, definition: dict[str, Any]) -> np.ndarray:
    values = numeric(df[definition["minuend"]]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    values -= numeric(df[definition["subtrahend"]]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    if "floor" in definition:
        np.maximum(values, float(definition["floor"]), out=values)
    if "ceiling" in definition:
        np.minimum(values, float(definition["ceiling"]), out=values)
    return values.astype(np.float32)


def summed_columns(
    df: pd.DataFrame,
    columns: list[str],
    terms: list[dict[str, Any]] | None = None,
) -> np.ndarray:
    values = np.zeros(len(df), dtype=np.float32)
    for column in columns:
        values += numeric(df[column]).fillna(0).to_numpy(dtype=np.float32, copy=True)
    for term in terms or []:
        column = term["column"]
        multiplier = float(term.get("multiplier", 1.0))
        values += numeric(df[column]).fillna(0).to_numpy(dtype=np.float32, copy=True) * multiplier
    return values


def weighted_ratio_score(df: pd.DataFrame, definition: dict[str, Any]) -> np.ndarray:
    weighted_sum = np.zeros(len(df), dtype=np.float32)
    total_weight = 0.0
    for component in definition["components"]:
        numerators = list(component.get("numerators", []))
        denominators = list(component.get("denominators", []))
        if "numerator" in component:
            numerators.append(component["numerator"])
        if "denominator" in component:
            denominators.append(component["denominator"])
        num = summed_columns(df, numerators, component.get("numerator_terms", []))
        den = summed_columns(df, denominators, component.get("denominator_terms", []))
        np.clip(den, 1e-9, None, out=den)
        np.divide(num, den, out=num)
        cap = max(float(component.get("cap", 1.0)), 1e-9)
        num /= cap
        np.clip(num, 0.0, 1.0, out=num)
        if component.get("invert", False):
            np.subtract(1.0, num, out=num)
        weight = float(component.get("weight", 1.0))
        weighted_sum += num * weight
        total_weight += weight
    if total_weight <= 0.0:
        return weighted_sum
    weighted_sum /= total_weight
    return weighted_sum.astype(np.float32)


def access_score(df: pd.DataFrame, availability_column: str, distance_column: str) -> np.ndarray:
    available = (numeric(df[availability_column]).fillna(0).to_numpy(dtype=np.float32) == 1.0).astype(np.float32)
    distance = numeric(df[distance_column]).to_numpy(dtype=np.float32, copy=True)
    observed_codes = distance[np.isfinite(distance) & (distance > 0.0)]
    max_code = float(observed_codes.max()) if len(observed_codes) else 1.0
    distance[~np.isfinite(distance)] = max_code
    np.clip(distance, 0.0, max_code, out=distance)
    distance_component = 1.0 - (distance / max_code)
    return np.where(available == 1.0, 1.0, distance_component).astype(np.float32)


def distance_proximity_score(df: pd.DataFrame, distance_column: str) -> np.ndarray:
    distance = numeric(df[distance_column]).to_numpy(dtype=np.float32, copy=True)
    observed_codes = distance[np.isfinite(distance) & (distance > 0.0)]
    max_code = float(observed_codes.max()) if len(observed_codes) else 1.0
    distance[~np.isfinite(distance)] = max_code
    np.clip(distance, 0.0, max_code, out=distance)
    return (1.0 - (distance / max_code)).astype(np.float32)


def score_from_definition(df: pd.DataFrame, definition: dict[str, Any]) -> np.ndarray:
    source = definition["source_column"]
    source_values = numeric(df[source])
    score_map = {float(code): float(score) for code, score in definition["code_scores"].items()}
    values = source_values.map(score_map).fillna(0.0).to_numpy(dtype=np.float32)

    if definition["kind"] == "code_score_with_distance_fallback":
        fallback_codes = {float(code) for code in definition.get("fallback_codes", [])}
        fallback_mask = source_values.isin(fallback_codes).to_numpy()
        if "distance_code_scores" in definition:
            distance_values = numeric(df[definition["distance_column"]])
            distance_score_map = {
                float(code): float(score)
                for code, score in definition["distance_code_scores"].items()
            }
            fallback_scores = distance_values.map(distance_score_map).fillna(0.0).to_numpy(dtype=np.float32)
        else:
            fallback_scores = distance_proximity_score(df, definition["distance_column"])
        values = np.where(fallback_mask, fallback_scores, values).astype(np.float32)

    return values


def mixed_component_score(df: pd.DataFrame, component: dict[str, Any]) -> np.ndarray:
    if "score_column" in component:
        values = numeric(df[component["score_column"]]).to_numpy(dtype=np.float32, copy=True)
        np.clip(values, 0.0, 1.0, out=values)
        return values
    if "binary_column" in component:
        return (numeric(df[component["binary_column"]]).fillna(0).to_numpy(dtype=np.float32) > 0).astype(np.float32)
    if "numerator" in component and "denominator" in component:
        return clipped_ratio_score(df, component["numerator"], component["denominator"])
    raise ValueError(f"Unsupported mean_mixed_score component: {component}")


def mean_mixed_score(df: pd.DataFrame, definition: dict[str, Any]) -> np.ndarray:
    scores = [mixed_component_score(df, component) for component in definition["components"]]
    with np.errstate(invalid="ignore"):
        values = np.nanmean(np.vstack(scores), axis=0)
    values = np.nan_to_num(values, nan=0.0)
    return values.astype(np.float32)


def fit_continuous_labels(
    values: np.ndarray,
    requested_n_clusters: int | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    unique_values = len(np.unique(values))
    variance = float(np.var(values))
    natural_n_clusters = 2 if unique_values <= 2 else 3
    n_clusters = requested_n_clusters or natural_n_clusters
    n_clusters = max(1, min(int(n_clusters), unique_values))
    if n_clusters == 1:
        mean_value = float(np.mean(values)) if len(values) else 0.0
        mapped_label = 0 if mean_value < 0.34 else 1 if mean_value < 0.67 else 2
        labels = np.full(len(values), mapped_label, dtype=np.int8)
        distribution = label_counts(labels)
        return labels, {
            "n_clusters": 1,
            "variance": variance,
            "label_distribution": distribution,
            "shannon_entropy_bits": shannon_entropy(distribution),
            "requested_n_clusters": requested_n_clusters,
        }
    rng = np.random.default_rng(RANDOM_STATE)
    sample = values.reshape(-1, 1)
    if len(sample) > FEATURE_SAMPLE_SIZE:
        sample_idx = rng.choice(len(sample), FEATURE_SAMPLE_SIZE, replace=False)
        fit_sample = sample[sample_idx]
    else:
        fit_sample = sample

    model = MiniBatchKMeans(
        n_clusters=n_clusters,
        random_state=RANDOM_STATE,
        batch_size=5_000,
        n_init=3,
        max_iter=100,
    )
    model.fit(fit_sample)

    raw_labels = np.empty(len(values), dtype=np.int8)
    for start in range(0, len(values), FEATURE_PREDICT_CHUNK):
        stop = min(start + FEATURE_PREDICT_CHUNK, len(values))
        raw_labels[start:stop] = model.predict(sample[start:stop]).astype(np.int8)

    means = []
    for cluster_id in range(n_clusters):
        cluster_values = values[raw_labels == cluster_id]
        means.append(float(cluster_values.mean()) if len(cluster_values) else 0.0)
    order = np.argsort(means)

    labels = np.empty(len(raw_labels), dtype=np.int8)
    if n_clusters == 2:
        mapping = {order[0]: 0, order[1]: 2}
    else:
        mapping = {order[0]: 0, order[1]: 1, order[2]: 2}
    for raw_id, mapped in mapping.items():
        labels[raw_labels == raw_id] = mapped

    distribution = label_counts(labels)
    return labels, {
        "n_clusters": int(n_clusters),
        "variance": variance,
        "label_distribution": distribution,
        "shannon_entropy_bits": shannon_entropy(distribution),
        "requested_n_clusters": requested_n_clusters,
    }


def processing_sets(config: dict[str, Any]) -> dict[str, Any]:
    processing = config["processing"]
    aggregation = processing["aggregation_groups"]
    presence_sources = {
        definition["source_column"]
        for definition in processing["derived_variables"]["presence_flags"].values()
    }
    categorical_sources = {
        definition["source_column"]
        for definition in processing["derived_variables"]["categorical_flags"].values()
    }
    return {
        "group_by": set(processing["group_by_columns"]),
        "display": set(processing["display_identifier_columns"]),
        "admin": set(processing["admin_audit_columns"]),
        "sum": set(aggregation["sum"]),
        "max": set(aggregation["max"]),
        "distance_min": set(aggregation["min_distance_code"]),
        "row_score": set(aggregation.get("derived_score_then_max", [])),
        "direct_binary": set(processing["direct_binary_presence"]),
        "presence_sources": presence_sources,
        "categorical_sources": categorical_sources,
    }


def build_processed_frame(raw: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    sets = processing_sets(config)
    required = set(config["processing"]["required_raw_columns"])
    direct_binary = config["processing"]["direct_binary_presence"]

    columns: dict[str, pd.Series | int] = {}
    for column in sorted(sets["group_by"] | sets["display"]):
        columns[column] = raw[column]
    for column in sorted(sets["admin"]):
        if column in raw.columns:
            columns[column] = raw[column]
    columns["source_row_count"] = 1

    for column in sorted(sets["sum"] & required):
        columns[column] = numeric(raw[column]).fillna(0).astype(np.float32)
    for column in sorted(sets["max"] & required):
        columns[column] = numeric(raw[column]).fillna(0).astype(np.float32)
    for column in sorted(sets["distance_min"] & required):
        columns[column] = numeric(raw[column]).fillna(5).astype(np.float32)
    for column in sorted(sets["direct_binary"] & required):
        codes = set(float(code) for code in direct_binary[column])
        columns[column] = numeric(raw[column]).isin(codes).astype(np.int8)
    for variable in sorted(sets["row_score"]):
        definition = config["processing"]["derived_variables"]["scores"][variable]
        columns[variable] = score_from_definition(raw, definition)

    min_source_columns = (sets["presence_sources"] | sets["categorical_sources"]) & required
    min_source_columns -= set(columns)
    for column in sorted(min_source_columns):
        columns[column] = numeric(raw[column]).astype(np.float32)

    return pd.DataFrame(columns)


def load_and_aggregate(config: dict[str, Any], raw_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    state_files = sorted(raw_dir.glob("*.csv"))
    if not state_files:
        raise FileNotFoundError(f"No CSV files found under {raw_dir}")

    required_columns = set(config["processing"]["required_raw_columns"])
    frames: list[pd.DataFrame] = []
    raw_row_count = 0
    missing_by_file: dict[str, list[str]] = {}

    for path in state_files:
        header = set(pd.read_csv(path, nrows=0, encoding="utf-8-sig").columns)
        missing = sorted(required_columns - header)
        if missing:
            missing_by_file[path.name] = missing
            continue
        raw = pd.read_csv(path, usecols=lambda c: c in required_columns, low_memory=False)
        raw_row_count += len(raw)
        frames.append(build_processed_frame(raw, config))
        del raw
        gc.collect()

    if missing_by_file:
        raise ValueError(f"Required raw columns are missing: {missing_by_file}")

    combined = pd.concat(frames, ignore_index=True)
    del frames
    gc.collect()

    sets = processing_sets(config)
    group_keys = list(config["processing"]["group_by_columns"])
    agg_map: dict[str, str] = {column: "first" for column in config["processing"]["display_identifier_columns"]}
    agg_map["source_row_count"] = "sum"

    for column in config["processing"]["admin_audit_columns"]:
        if column in combined.columns:
            agg_map[column] = "nunique" if column.endswith("_code") else "first"
    for column in sorted(sets["sum"] & set(combined.columns)):
        agg_map[column] = "sum"
    for column in sorted(sets["max"] & set(combined.columns)):
        agg_map[column] = "max"
    for column in sorted(sets["distance_min"] & set(combined.columns)):
        agg_map[column] = "min"
    for column in sorted(sets["row_score"] & set(combined.columns)):
        agg_map[column] = "max"
    for column in sorted(sets["direct_binary"] & set(combined.columns)):
        agg_map[column] = "max"
    for column in sorted((sets["presence_sources"] | sets["categorical_sources"]) & set(combined.columns)):
        agg_map[column] = "min"

    grouped = combined.groupby(group_keys, as_index=False, sort=False).agg(agg_map)
    rename_map = {
        column: f"{column}_nunique"
        for column in config["processing"]["admin_audit_columns"]
        if column.endswith("_code") and column in grouped.columns
    }
    grouped = grouped.rename(columns=rename_map)
    grouped["village_key"] = (
        grouped["state_code"].astype(str)
        + ":"
        + grouped["district_code"].astype(str)
        + ":"
        + grouped["sub_district_code"].astype(str)
        + ":"
        + grouped["village_code"].astype(str)
    )

    derive_variables(grouped, config)

    duplicate_groups = int((grouped["source_row_count"] > 1).sum())
    duplicate_rows = int(grouped.loc[grouped["source_row_count"] > 1, "source_row_count"].sum() - duplicate_groups)
    return grouped, {
        "raw_files": [str(path) for path in state_files],
        "raw_row_count": int(raw_row_count),
        "aggregated_row_count": int(len(grouped)),
        "duplicate_group_count": duplicate_groups,
        "duplicate_extra_row_count": duplicate_rows,
        "source_row_count_distribution": {
            str(int(key)): int(value)
            for key, value in grouped["source_row_count"].value_counts().sort_index().items()
        },
    }


def derive_variables(df: pd.DataFrame, config: dict[str, Any]) -> None:
    derived = config["processing"]["derived_variables"]
    for variable, definition in derived["presence_flags"].items():
        source = definition["source_column"]
        codes = set(float(code) for code in definition["presence_codes"])
        df[variable] = numeric(df[source]).isin(codes).astype(np.int8)

    for variable, definition in derived["categorical_flags"].items():
        source = definition["source_column"]
        match_code = float(definition["match_code"])
        df[variable] = numeric(df[source]).eq(match_code).astype(np.int8)

    for variable, definition in derived["scores"].items():
        if variable in df.columns:
            continue
        if definition["kind"] == "mean_access_score":
            scores = []
            for component in definition["components"]:
                scores.append(access_score(df, component["availability_column"], component["distance_column"]))
            df[variable] = np.mean(np.vstack(scores), axis=0).astype(np.float32)
        elif definition["kind"] == "mean_ratio_score":
            scores = []
            for component in definition["components"]:
                scores.append(clipped_ratio_score(df, component["numerator"], component["denominator"]))
            df[variable] = np.mean(np.vstack(scores), axis=0).astype(np.float32)
        elif definition["kind"] == "ordinal_code_score":
            df[variable] = score_from_definition(df, definition)
        elif definition["kind"] == "code_score_with_distance_fallback":
            df[variable] = score_from_definition(df, definition)
        elif definition["kind"] == "weighted_ratio_score":
            df[variable] = weighted_ratio_score(df, definition)
        elif definition["kind"] == "mean_mixed_score":
            df[variable] = mean_mixed_score(df, definition)
        elif definition["kind"] == "ratio_score":
            df[variable] = ratio_score(df, definition)
        elif definition["kind"] == "sum_columns":
            df[variable] = summed_columns(df, definition["columns"])
        elif definition["kind"] == "difference_score":
            df[variable] = difference_score(df, definition)
        else:
            raise ValueError(f"Unsupported score derivation kind: {definition['kind']}")


def compute_feature_clusters(
    df: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], list[dict[str, Any]]]:
    feature_values: dict[str, np.ndarray] = {}
    feature_labels: dict[str, np.ndarray] = {}
    metadata_rows: list[dict[str, Any]] = []

    for feature in config["features"]:
        method = feature["method"]
        inputs = list(feature["input_variables"])
        inverse = bool(feature.get("inverse", False))

        if method == "ratio":
            values = ratio_values(df, inputs[0], inputs[1], inverse=inverse)
            labels, cluster_meta = fit_continuous_labels(values)
        elif method in {"count", "score"}:
            value_column = feature.get("score_variable", inputs[0])
            values = count_values(df, value_column, inverse=inverse)
            labels, cluster_meta = fit_continuous_labels(values)
        elif method == "binary":
            values = (numeric(df[inputs[0]]).fillna(0).to_numpy(dtype=np.float32) > 0).astype(np.float32)
            labels = (values.astype(np.int8) * 2).astype(np.int8)
            distribution = label_counts(labels)
            cluster_meta = {
                "n_clusters": int(len(np.unique(labels))),
                "variance": float(np.var(values)),
                "label_distribution": distribution,
                "shannon_entropy_bits": shannon_entropy(distribution),
            }
        elif method in {"additive", "composite_binary_share"}:
            values = np.zeros(len(df), dtype=np.float32)
            for column in inputs:
                values += (numeric(df[column]).fillna(0).to_numpy(dtype=np.float32) > 0).astype(np.float32)
            values /= max(float(len(inputs)), 1.0)
            labels, cluster_meta = fit_continuous_labels(values)
        else:
            raise ValueError(f"Unsupported feature method: {method}")

        feature_values[feature["feature_id"]] = values
        feature_labels[feature["feature_id"]] = labels
        metadata_row = {
            "feature_id": feature["feature_id"],
            "feature_column": feature["feature_column"],
            "category_id": feature["category_id"],
            "display_name": feature["display_name"],
            "method": method,
            "inverse": inverse,
            "input_variables": inputs,
            "raw_dependencies": feature["raw_dependencies"],
            "derived_dependencies": feature["derived_dependencies"],
            "label_distribution": cluster_meta["label_distribution"],
            "n_clusters": cluster_meta["n_clusters"],
            "variance": cluster_meta["variance"],
            "shannon_entropy_bits": cluster_meta["shannon_entropy_bits"],
        }
        if "score_variable" in feature:
            metadata_row["score_variable"] = feature["score_variable"]
        metadata_rows.append(metadata_row)

    return feature_values, feature_labels, metadata_rows


def compute_category_clusters(
    feature_labels: dict[str, np.ndarray],
    config: dict[str, Any],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], list[dict[str, Any]]]:
    category_values: dict[str, np.ndarray] = {}
    category_labels: dict[str, np.ndarray] = {}
    metadata_rows: list[dict[str, Any]] = []

    for category in config["categories"]:
        score_stack = []
        for feature_id in category["feature_ids"]:
            labels = feature_labels[feature_id]
            scores = np.where(labels == 2, 1.0, np.where(labels == 1, 0.5, 0.0)).astype(np.float32)
            score_stack.append(scores)
        values = np.mean(np.vstack(score_stack), axis=0).astype(np.float32)
        category_rule = category.get("category_cluster_rule", {})
        labels, cluster_meta = fit_continuous_labels(
            values,
            requested_n_clusters=category_rule.get("n_clusters"),
        )
        category_values[category["category_column"]] = values
        category_labels[category["category_column"]] = labels
        metadata_row = {
            "category_id": category["category_id"],
            "category_column": category["category_column"],
            "index_column": category["index_column"],
            "display_name": category["display_name"],
            "feature_ids": category["feature_ids"],
            "label_distribution": cluster_meta["label_distribution"],
            "n_clusters": cluster_meta["n_clusters"],
            "variance": cluster_meta["variance"],
            "shannon_entropy_bits": cluster_meta["shannon_entropy_bits"],
        }
        if category_rule:
            metadata_row["category_cluster_rule"] = category_rule
        metadata_rows.append(metadata_row)

    return category_values, category_labels, metadata_rows


def base_frame(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    columns = [
        "village_key",
        *config["processing"]["group_by_columns"],
        *config["processing"]["display_identifier_columns"],
        "source_row_count",
    ]
    for admin_column in config["processing"]["admin_audit_columns"]:
        output_column = f"{admin_column}_nunique" if admin_column.endswith("_code") else admin_column
        if output_column in df.columns:
            columns.append(output_column)
    return df[columns].copy()


def write_label_csv(
    base: pd.DataFrame,
    labels: dict[str, np.ndarray],
    ordered_columns: list[str],
    output_path: Path,
) -> None:
    header_written = False
    if output_path.exists():
        output_path.unlink()
    for start in range(0, len(base), WRITE_CHUNK):
        stop = min(start + WRITE_CHUNK, len(base))
        chunk = base.iloc[start:stop].copy()
        for column in ordered_columns:
            chunk[column] = pd.Series(labels[column][start:stop], index=chunk.index).map(INT_TO_LABEL)
        chunk.to_csv(output_path, mode="a" if header_written else "w", index=False, header=not header_written)
        header_written = True


def write_value_csv(
    base: pd.DataFrame,
    values: dict[str, np.ndarray],
    ordered_columns: list[str],
    output_path: Path,
) -> None:
    header_written = False
    if output_path.exists():
        output_path.unlink()
    for start in range(0, len(base), WRITE_CHUNK):
        stop = min(start + WRITE_CHUNK, len(base))
        chunk = base.iloc[start:stop].copy()
        for column in ordered_columns:
            chunk[column] = values[column][start:stop]
        chunk.to_csv(output_path, mode="a" if header_written else "w", index=False, header=not header_written)
        header_written = True


def write_category_gis_csv(
    base: pd.DataFrame,
    category_values: dict[str, np.ndarray],
    category_labels: dict[str, np.ndarray],
    ordered_columns: list[str],
    output_path: Path,
) -> None:
    header_written = False
    if output_path.exists():
        output_path.unlink()
    for start in range(0, len(base), WRITE_CHUNK):
        stop = min(start + WRITE_CHUNK, len(base))
        chunk = base.iloc[start:stop].copy()
        for column in ordered_columns:
            chunk[f"{column}_index"] = category_values[column][start:stop]
            chunk[f"{column}_cluster"] = pd.Series(category_labels[column][start:stop], index=chunk.index).map(INT_TO_LABEL)
        chunk.to_csv(output_path, mode="a" if header_written else "w", index=False, header=not header_written)
        header_written = True


def quality_tables(
    feature_meta: list[dict[str, Any]],
    category_meta: list[dict[str, Any]],
    output_dir: Path,
) -> dict[str, Path]:
    feature_path = output_dir / "antyodaya_feature_quality_metrics.csv"
    category_path = output_dir / "antyodaya_category_quality_metrics.csv"

    def flatten(row: dict[str, Any]) -> dict[str, Any]:
        flattened = {
            key: value
            for key, value in row.items()
            if key not in {
                "label_distribution",
                "input_variables",
                "raw_dependencies",
                "derived_dependencies",
                "feature_ids",
                "category_cluster_rule",
            }
        }
        for label in ("Low", "Medium", "High"):
            flattened[f"{label.lower()}_count"] = row.get("label_distribution", {}).get(label, 0)
        for key in ("input_variables", "raw_dependencies", "derived_dependencies", "feature_ids"):
            if key in row:
                flattened[key] = json.dumps(row[key], ensure_ascii=False)
        if "category_cluster_rule" in row:
            flattened["category_cluster_rule"] = json.dumps(
                row["category_cluster_rule"],
                ensure_ascii=False,
            )
        return flattened

    def write_quality_csv(frame: pd.DataFrame, path: Path) -> Path:
        try:
            frame.to_csv(path, index=False)
            return path
        except PermissionError:
            fallback = path.with_name(f"{path.stem}.latest{path.suffix}")
            frame.to_csv(fallback, index=False)
            return fallback

    feature_path = write_quality_csv(pd.DataFrame([flatten(row) for row in feature_meta]), feature_path)
    category_path = write_quality_csv(pd.DataFrame([flatten(row) for row in category_meta]), category_path)
    return {"feature_quality_csv": feature_path, "category_quality_csv": category_path}


def write_report(output_dir: Path, metadata: dict[str, Any]) -> Path:
    report_path = output_dir / "antyodaya_cluster_analysis_report.md"
    input_stats = metadata["input"]
    lines = [
        "# Antyodaya Cluster Analysis Report",
        "",
        "This is the clean raw-file Antyodaya 2020 clustering run. It uses the mapping JSON as the workflow config and does not use SHRUG/baseline comparison inputs.",
        "",
        "## Output Shape",
        "",
        f"- raw rows: `{input_stats['raw_row_count']:,}`",
        f"- grouped village rows: `{input_stats['aggregated_row_count']:,}`",
        f"- duplicate village groups: `{input_stats['duplicate_group_count']:,}`",
        f"- duplicate extra rows: `{input_stats['duplicate_extra_row_count']:,}`",
        f"- features: `{len(metadata['feature_cluster_columns'])}`",
        f"- categories: `{len(metadata['category_cluster_columns'])}`",
        "",
        "## Output Files",
        "",
    ]
    for label, path in metadata["output_files"].items():
        lines.append(f"- `{label}`: `{Path(path).relative_to(REPO_ROOT)}`")
    lines.extend(
        [
            "",
            "The `village_category_indices_clusters_csv` file is the most convenient table for GIS use: each category has a normalized category index and final Low/Medium/High or Low/High class label in the same village-level file.",
            "",
            "## Method Notes",
            "",
            "- Raw parameters are converted into features; categories are scored from those feature classes and then clustered by the finalized category rules.",
            "- Continuous feature scores use 3 clusters whenever the score has at least 3 unique values; binary-only scores remain 2-class.",
            "- Category outputs use 3 classes by default, except where the finalized rule intentionally keeps 2 classes because Medium does not add a clear qualitative distinction.",
            "- Two-class outputs are reported as Low/High rather than creating an empty Medium bucket.",
            "- Binary availability features score present villages as High for GIS readability.",
            "- `electrification_rate_feature` uses the richer domestic electricity-hours code: no electricity = 0.00, 1-4 hrs = 0.25, 4-8 hrs = 0.50, 8-12 hrs = 0.75, and >12 hrs = 1.00.",
            "- `agriculture_land_cultivation_category` uses one composite `land_utilization_feature`; its component signals are land cultivation rate, irrigation coverage, and seasonal cropping intensity. Seasonal cropping intensity uses `(kharif + 2*rabi + 3*other) / net_sown_area_in_hac` before capping and normalization.",
            "",
            "## Metric Usage upgrades",
            "",
            "The following raw categorical fields are now used as richer score metrics before feature classification. Row-level scores are computed first and duplicate village records keep the best observed score via max aggregation.",
            "",
            "- `piped_water_coverage_feature` replaces the former two piped-water features with a composite score: mean of household piped-water connection ratio and `availability_of_piped_tap_water` coverage.",
            "- `availability_of_internal_pucca_road`: fully covered = 1.00, partially covered = 0.50, not covered = 0.00.",
            "- `availability_of_drainage_system`: closed drainage = 1.00, covered open = 0.75, uncovered open = 0.50, kuchha = 0.25, no drainage = 0.00. This replaces the former four separate drainage one-hot features with one `drainage_quality_feature`.",
            "- `availability_of_fpos_pacs`: none = 0, FPO or PACS = 1, both FPO and PACS = 2; normalized during feature scoring.",
            "- `availability_of_market`: mandi = 1.00, regular market = 0.75, weekly haat = 0.50, no local market = 0.00.",
            "- Distance fields are excluded from the current workflow because the `distance_of_*` series was found unreliable; facility availability now uses local availability flags or ordinal type codes only.",
            "- `livelihoods_employment_category` uses the farm household employment ratio only and is displayed as Farm Employment.",
            "- `agriculture_irrigation_watershed_index` now includes the modern irrigation feature; the separate Agriculture Modern Irrigation category is no longer emitted in the current 21-category output.",
            "",
            "Distance-based enrichment can be revisited later only if a cleaner source for facility proximity is supplied.",
            "",
            "## Category Distributions",
            "",
        ]
    )
    for row in metadata["category_cluster_columns"]:
        dist = row["label_distribution"]
        parts = [
            f"{label} `{dist[label]:,}`"
            for label in ("Low", "Medium", "High")
            if dist.get(label, 0) > 0
        ]
        lines.append(f"- `{row['category_column']}`: " + ", ".join(parts))
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    config = load_config(args.mapping_config)
    raw_dir = args.raw_dir or as_path(config["source"]["raw_files_dir"])

    aggregated, input_stats = load_and_aggregate(config, raw_dir)
    feature_values, feature_labels, feature_meta = compute_feature_clusters(aggregated, config)
    category_values, category_labels, category_meta = compute_category_clusters(feature_labels, config)

    base = base_frame(aggregated, config)
    output_files = {
        "feature_values_csv": args.output_dir / "antyodaya_village_feature_values_normalized.csv",
        "feature_clusters_csv": args.output_dir / "antyodaya_village_feature_clusters.csv",
        "category_values_csv": args.output_dir / "antyodaya_village_category_values_normalized.csv",
        "category_clusters_csv": args.output_dir / "antyodaya_village_category_clusters.csv",
        "village_category_indices_clusters_csv": args.output_dir / "antyodaya_village_category_indices_clusters.csv",
        "metadata_json": args.output_dir / "antyodaya_cluster_metadata.json",
    }
    output_files.update(quality_tables(feature_meta, category_meta, args.output_dir))

    feature_order = [feature["feature_id"] for feature in config["features"]]
    category_order = [category["category_column"] for category in config["categories"]]
    write_value_csv(base, feature_values, feature_order, output_files["feature_values_csv"])
    write_label_csv(base, feature_labels, feature_order, output_files["feature_clusters_csv"])
    write_value_csv(base, category_values, category_order, output_files["category_values_csv"])
    write_label_csv(base, category_labels, category_order, output_files["category_clusters_csv"])
    write_category_gis_csv(base, category_values, category_labels, category_order, output_files["village_category_indices_clusters_csv"])

    metadata = {
        "pipeline": "antyodaya_cluster_analysis",
        "mapping_config": args.mapping_config,
        "raw_dir": raw_dir,
        "input": input_stats,
        "output_files": output_files,
        "features": config["features"],
        "categories": config["categories"],
        "feature_cluster_columns": feature_meta,
        "category_cluster_columns": category_meta,
    }
    report_path = write_report(args.output_dir, metadata)
    metadata["output_files"]["report_md"] = report_path
    output_files["metadata_json"].write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False, default=json_default) + "\n",
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "raw_rows": input_stats["raw_row_count"],
                "grouped_rows": input_stats["aggregated_row_count"],
                "feature_count": len(feature_meta),
                "category_count": len(category_meta),
                "output_dir": str(args.output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
