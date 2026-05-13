#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import textwrap
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "antyodaya"
DEFAULT_MAPPING_CONFIG = DATA_DIR / "mappings" / "antyodaya_variable_mapping.json"
DEFAULT_CLUSTER_METADATA = DATA_DIR / "output" / "antyodaya_cluster_metadata.json"
DEFAULT_OUTPUT_DIR = DATA_DIR / "output" / "visualisations"

CLUSTER_ORDER = ("Low", "Medium", "High")
CLUSTER_COLORS = {
    "Low": "#C83E3A",
    "Medium": "#F2A541",
    "High": "#2A9D8F",
}
TEXT_COLOR = "#25313B"
GRID_COLOR = "#D9E2EC"
PANEL_BG = "#FBFCFE"
REFERENCE_COLOR = "#7B8794"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Antyodaya final cluster QA visualisations from clean clustering outputs."
    )
    parser.add_argument("--mapping-config", type=Path, default=DEFAULT_MAPPING_CONFIG)
    parser.add_argument("--cluster-metadata", type=Path, default=DEFAULT_CLUSTER_METADATA)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def slugify(text: str) -> str:
    text = text.replace("&", " and ")
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    text = text.replace("\n", " ")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower())
    return text.strip("_")


def wrapped_label(text: str, width: int = 15) -> str:
    return "\n".join(textwrap.wrap(text, width=width, break_long_words=False)) or text


def read_outputs(metadata: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    outputs = metadata["output_files"]
    feature_values = pd.read_csv(outputs["feature_values_csv"], low_memory=False)
    feature_clusters = pd.read_csv(outputs["feature_clusters_csv"], low_memory=False)
    category_values = pd.read_csv(outputs["category_values_csv"], low_memory=False)
    category_clusters = pd.read_csv(outputs["category_clusters_csv"], low_memory=False)
    return feature_values, feature_clusters, category_values, category_clusters


def format_count(value: int) -> str:
    return f"{value:,}"


def feature_lookup(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {feature["feature_id"]: feature for feature in config["features"]}


def plot_category_distributions(metadata: dict[str, Any], output_dir: Path) -> Path:
    rows = []
    for category in metadata["category_cluster_columns"]:
        total = sum(category["label_distribution"].values())
        row = {
            "category": category["display_name"],
            **{
                label: category["label_distribution"].get(label, 0) / max(total, 1) * 100.0
                for label in CLUSTER_ORDER
            },
        }
        rows.append(row)
    df = pd.DataFrame(rows)

    fig, ax = plt.subplots(figsize=(14, 12))
    fig.patch.set_facecolor("white")
    ax.set_facecolor(PANEL_BG)
    left = np.zeros(len(df))
    y = np.arange(len(df))
    for label in CLUSTER_ORDER:
        values = df[label].to_numpy()
        bars = ax.barh(
            y,
            values,
            left=left,
            label=label,
            color=CLUSTER_COLORS[label],
            edgecolor="white",
            linewidth=0.8,
            height=0.72,
        )
        for bar, value, start in zip(bars, values, left):
            if value >= 4.0:
                ax.text(
                    start + value / 2.0,
                    bar.get_y() + bar.get_height() / 2.0,
                    f"{value:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="white" if label != "Medium" else TEXT_COLOR,
                    fontweight="bold",
                )
        left += df[label].to_numpy()
    ax.set_yticks(y)
    ax.set_yticklabels(df["category"], fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("Share of villages (%)", color=TEXT_COLOR)
    ax.set_title("Final Category Cluster Distribution", fontsize=18, fontweight="bold", color=TEXT_COLOR)
    ax.legend(loc="lower right", frameon=True)
    ax.grid(axis="x", color=GRID_COLOR, linewidth=0.9)
    fig.tight_layout()

    path = output_dir / "category_cluster_distribution.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_category_entropy(metadata: dict[str, Any], output_dir: Path) -> Path:
    rows = [
        {
            "category": row["display_name"],
            "entropy": row["shannon_entropy_bits"],
        }
        for row in metadata["category_cluster_columns"]
    ]
    df = pd.DataFrame(rows).sort_values("entropy")
    fig, ax = plt.subplots(figsize=(12, 9))
    fig.patch.set_facecolor("white")
    ax.set_facecolor(PANEL_BG)
    cmap = plt.get_cmap("viridis")
    max_entropy = max(float(df["entropy"].max()), 1e-9)
    colors = [cmap(float(value) / max_entropy) for value in df["entropy"]]
    bars = ax.barh(df["category"], df["entropy"], color=colors, edgecolor="white", linewidth=0.8)
    for bar, value in zip(bars, df["entropy"]):
        ax.text(
            value + 0.02,
            bar.get_y() + bar.get_height() / 2.0,
            f"{value:.2f}",
            va="center",
            fontsize=8,
            color=TEXT_COLOR,
        )
    ax.set_xlabel("Shannon entropy (bits)", color=TEXT_COLOR)
    ax.set_title("Category Cluster Balance", fontsize=18, fontweight="bold", color=TEXT_COLOR)
    ax.grid(axis="x", color=GRID_COLOR, linewidth=0.9)
    fig.tight_layout()

    path = output_dir / "category_cluster_entropy.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_category_correlation(category_values: pd.DataFrame, config: dict[str, Any], output_dir: Path) -> Path:
    category_columns = [category["category_column"] for category in config["categories"]]
    display_names = [category["display_name"] for category in config["categories"]]
    corr = category_values[category_columns].corr()

    fig, ax = plt.subplots(figsize=(13, 11))
    fig.patch.set_facecolor("white")
    ax.set_facecolor(PANEL_BG)
    image = ax.imshow(corr.to_numpy(), cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(np.arange(len(category_columns)))
    ax.set_yticks(np.arange(len(category_columns)))
    ax.set_xticklabels(display_names, rotation=65, ha="right", fontsize=8)
    ax.set_yticklabels(display_names, fontsize=8)
    ax.set_title("Category Score Correlation", fontsize=18, fontweight="bold", color=TEXT_COLOR)
    fig.colorbar(image, ax=ax, fraction=0.045, pad=0.04)
    fig.tight_layout()

    path = output_dir / "category_index_correlation.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_source_row_quality(feature_values: pd.DataFrame, output_dir: Path) -> Path:
    counts = feature_values["source_row_count"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor(PANEL_BG)
    ax.bar(counts.index.astype(str), counts.values, color="#4C78A8", edgecolor="white", linewidth=0.8)
    ax.set_yscale("log")
    ax.set_xlabel("Raw rows collapsed into a village record", color=TEXT_COLOR)
    ax.set_ylabel("Village count (log scale)", color=TEXT_COLOR)
    ax.set_title("Duplicate Raw-Row Collapse Distribution", fontsize=16, fontweight="bold", color=TEXT_COLOR)
    ax.grid(axis="y", color=GRID_COLOR, linewidth=0.9)
    fig.tight_layout()

    path = output_dir / "source_row_count_distribution.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def category_feature_plot(
    category: dict[str, Any],
    feature_values: pd.DataFrame,
    feature_clusters: pd.DataFrame,
    category_clusters: pd.DataFrame,
    features_by_id: dict[str, dict[str, Any]],
    output_dir: Path,
    category_index: int,
    *,
    vertical: bool = False,
) -> Path:
    feature_ids = category["feature_ids"]
    value_df = feature_values[["village_key", *feature_ids]].set_index("village_key")
    feature_cluster_df = feature_clusters[["village_key", *feature_ids]].set_index("village_key")
    category_cluster_df = category_clusters[["village_key", category["category_column"]]].set_index("village_key")
    common_village_keys = value_df.index.intersection(category_cluster_df.index)
    value_df = value_df.loc[common_village_keys]
    feature_cluster_df = feature_cluster_df.loc[feature_cluster_df.index.intersection(common_village_keys)]
    category_cluster_series = category_cluster_df.loc[common_village_keys, category["category_column"]].dropna()
    value_df = value_df.loc[category_cluster_series.index]
    feature_cluster_df = feature_cluster_df.loc[feature_cluster_df.index.intersection(category_cluster_series.index)]
    cluster_labels = category_cluster_series
    total_villages = int(cluster_labels.shape[0])
    full_cluster_counts = cluster_labels.value_counts().to_dict()

    present_clusters = [
        cluster
        for cluster in CLUSTER_ORDER
        if int((category_cluster_series == cluster).sum()) > 0
    ]
    if not present_clusters:
        present_clusters = ["Low"]

    if vertical:
        fig_width = max(8.0, 1.15 * len(feature_ids) + 3.0)
        fig, axes = plt.subplots(len(present_clusters), 1, figsize=(fig_width, 5.2 * len(present_clusters)), sharey=True)
    else:
        fig, axes = plt.subplots(1, len(present_clusters), figsize=(6.4 * len(present_clusters), 6.4), sharey=True)
    fig.patch.set_facecolor("white")
    if len(present_clusters) == 1:
        axes = np.array([axes])
    labels = [features_by_id[feature_id]["display_name"] for feature_id in feature_ids]
    x = np.arange(1, len(feature_ids) + 1)

    for ax, cluster_label in zip(axes, present_clusters):
        ax.set_facecolor(PANEL_BG)
        values = [
            value_df.loc[category_cluster_series == cluster_label, feature_id]
            .dropna()
            .to_numpy(dtype=np.float32)
            for feature_id in feature_ids
        ]
        non_empty = [value if len(value) else np.array([0.0], dtype=np.float32) for value in values]
        ax.boxplot(
            non_empty,
            positions=x,
            widths=0.58,
            patch_artist=True,
            showmeans=True,
            meanline=True,
            boxprops={"facecolor": CLUSTER_COLORS[cluster_label], "edgecolor": "#333333", "linewidth": 1.2, "alpha": 0.45},
            medianprops={"color": "#0B3D91", "linewidth": 1.8},
            meanprops={"color": "#D73027", "linewidth": 1.6, "linestyle": "--"},
            whiskerprops={"color": "#333333"},
            capprops={"color": "#333333"},
            flierprops={"markersize": 1.1, "alpha": 0.14},
        )
        stat_fontsize = 7 if len(feature_ids) > 7 else 8
        label_counts = []
        for pos, feature_values_for_cluster in enumerate(values, start=1):
            n_villages = len(feature_values_for_cluster)
            feature_id = feature_ids[pos - 1]
            label_counts.append(int((feature_cluster_df[feature_id] == cluster_label).sum()))
            if n_villages == 0:
                continue
            median = float(np.median(feature_values_for_cluster))
            mean = float(np.mean(feature_values_for_cluster))
            q1 = float(np.percentile(feature_values_for_cluster, 25))
            q3 = float(np.percentile(feature_values_for_cluster, 75))
            iqr = q3 - q1
            std = float(np.std(feature_values_for_cluster))
            ax.text(
                pos,
                1.20,
                f"Med {median:.2f}\nMean {mean:.2f}\nIQR {iqr:.2f}\nSD {std:.2f}",
                ha="center",
                va="top",
                fontsize=stat_fontsize,
                color=TEXT_COLOR,
                bbox={
                    "boxstyle": "round,pad=0.28",
                    "facecolor": "white",
                    "edgecolor": CLUSTER_COLORS[cluster_label],
                    "linewidth": 1.0,
                    "alpha": 0.94,
                },
            )
        ax.axhline(y=0.33, color=REFERENCE_COLOR, linestyle=":", alpha=0.65, linewidth=1.0)
        ax.axhline(y=0.67, color=REFERENCE_COLOR, linestyle=":", alpha=0.65, linewidth=1.0)
        full_count = int(full_cluster_counts.get(cluster_label, 0))
        share = full_count / max(total_villages, 1) * 100.0
        ax.set_title(
            (
                f"{cluster_label.upper()} CATEGORY CLASS - {category['display_name']}\n"
                f"Villages: {format_count(full_count)} / {format_count(total_villages)} total "
                f"({share:.1f}%)"
            ),
            color=TEXT_COLOR,
            fontweight="bold",
            fontsize=10,
            bbox={
                "boxstyle": "round,pad=0.35",
                "facecolor": CLUSTER_COLORS[cluster_label],
                "edgecolor": "#333333",
                "linewidth": 0.9,
                "alpha": 0.18,
            },
        )
        ax.set_xticks(x)
        display_labels = [
            wrapped_label(f"{label} (n={format_count(label_count)})", width=16)
            for label, label_count in zip(labels, label_counts)
        ]
        ax.set_xticklabels(display_labels, rotation=30, ha="right", fontsize=9)
        ax.set_ylim(-0.05, 1.23)
        ax.grid(axis="y", color=GRID_COLOR, linewidth=0.8)

    axes[0].set_ylabel("Normalized feature value", color=TEXT_COLOR)
    fig.text(
        0.5,
        0.01,
        "Blue line = median | Red dashed line = mean | Box = Q1-Q3 | Whiskers = 1.5x IQR | n = villages in that final category class",
        ha="center",
        fontsize=10,
        color=TEXT_COLOR,
    )
    fig.suptitle(category["display_name"], fontsize=18, fontweight="bold", color=TEXT_COLOR)
    fig.tight_layout(rect=[0, 0.04, 1, 0.94])

    suffix = "_vertical" if vertical else ""
    path = output_dir / f"{category_index:02d}_{slugify(category['display_name'])}{suffix}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def write_report(output_dir: Path, generated: dict[str, Any], metadata: dict[str, Any]) -> Path:
    report_path = output_dir / "antyodaya_visualisation_report.md"
    lines = [
        "# Antyodaya Visualisation Report",
        "",
        "This report lists final QA visualisations generated from the clean Antyodaya raw-file clustering outputs.",
        "",
        "## Global QA Plots",
        "",
    ]
    for label, path in generated["global"].items():
        lines.append(f"- `{label}`: `{Path(path).relative_to(REPO_ROOT)}`")
    lines.extend(["", "## Category Feature Box Plots", ""])
    for item in generated["category_feature_plots"]:
        lines.append(f"- `{item['category']}`: `{Path(item['path']).relative_to(REPO_ROOT)}`")
    lines.extend(["", "## Category Feature Box Plots - Vertical", ""])
    for item in generated.get("category_feature_plots_vertical", []):
        lines.append(f"- `{item['category']}`: `{Path(item['path']).relative_to(REPO_ROOT)}`")
    lines.extend(
        [
            "",
            "## Source Shape",
            "",
            f"- raw rows: `{metadata['input']['raw_row_count']:,}`",
            f"- grouped village rows: `{metadata['input']['aggregated_row_count']:,}`",
            f"- duplicate village groups: `{metadata['input']['duplicate_group_count']:,}`",
            f"- duplicate extra rows: `{metadata['input']['duplicate_extra_row_count']:,}`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    category_plot_dir = args.output_dir / "category_feature_box_plots"
    vertical_category_plot_dir = args.output_dir / "category_feature_box_plots_vertical"
    category_plot_dir.mkdir(parents=True, exist_ok=True)
    vertical_category_plot_dir.mkdir(parents=True, exist_ok=True)

    config = load_json(args.mapping_config)
    metadata = load_json(args.cluster_metadata)
    feature_values, feature_clusters, category_values, category_clusters = read_outputs(metadata)

    generated: dict[str, Any] = {
        "global": {},
        "category_feature_plots": [],
        "category_feature_plots_vertical": [],
    }
    generated["global"]["category_cluster_distribution"] = plot_category_distributions(metadata, args.output_dir)
    generated["global"]["category_cluster_entropy"] = plot_category_entropy(metadata, args.output_dir)
    generated["global"]["category_index_correlation"] = plot_category_correlation(category_values, config, args.output_dir)
    generated["global"]["source_row_count_distribution"] = plot_source_row_quality(feature_values, args.output_dir)

    features_by_id = feature_lookup(config)
    for index, category in enumerate(config["categories"], start=1):
        path = category_feature_plot(
            category,
            feature_values,
            feature_clusters,
            category_clusters,
            features_by_id,
            category_plot_dir,
            index,
        )
        generated["category_feature_plots"].append({"category": category["display_name"], "path": path})
        vertical_path = category_feature_plot(
            category,
            feature_values,
            feature_clusters,
            category_clusters,
            features_by_id,
            vertical_category_plot_dir,
            index,
            vertical=True,
        )
        generated["category_feature_plots_vertical"].append({"category": category["display_name"], "path": vertical_path})

    report_path = write_report(args.output_dir, generated, metadata)
    print(
        json.dumps(
            {
                "global_plot_count": len(generated["global"]),
                "category_plot_count": len(generated["category_feature_plots"]),
                "vertical_category_plot_count": len(generated["category_feature_plots_vertical"]),
                "report": str(report_path),
                "output_dir": str(args.output_dir),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
