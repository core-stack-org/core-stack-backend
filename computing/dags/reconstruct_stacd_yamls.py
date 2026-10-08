#!/usr/bin/env python3
"""
Reconstruct STACD dag.yaml / algo_repo.yaml / dataset_repo.yaml
from the STAC catalog files under stacd_catalog_data/dags/<dag>/.

Usage:
    python3 reconstruct_stacd_yamls.py \
        --catalog-dir ./stacd_catalog_data/dags \
        --output-dir  ./reconstructed_yaml
"""
import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import yaml


def load_json(p: Path):
    return json.loads(p.read_text())


def latest_version_file(algo_dir: Path):
    versions = sorted(
        (algo_dir / "versions").glob("v*.json"),
        key=lambda p: int(re.search(r"v(\d+)", p.stem).group(1)),
    )
    return versions[-1] if versions else None


def parse_date(iso: str) -> datetime:
    # Return a real datetime object (not a string) so yaml.safe_dump emits an
    # unquoted timestamp, which PyYAML's FullLoader parses back into a
    # datetime object — required by the SQLite DateTime column.
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(tzinfo=None)


def dump_doc(tag: str, data: dict) -> str:
    body = yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
    return f"--- {tag}\n{body}\n"


def build_dag_yaml(dag_json: dict, algo_types: list, dataset_types: list) -> str:
    dag = dag_json
    doc = dump_doc("!DAG", {
        "id": dag["stacd:dag_id"],
        "name": dag["title"],
        "version": dag["stacd:version"],
        "description": dag["description"],
        "params": dag.get("stacd:parameters", []),
        "alg_type_nodes": [n["id"] for n in dag["stacd:graph"]["nodes"] if n["type"] == "algorithm"],
        "dataset_type_nodes": [n["id"] for n in dag["stacd:graph"]["nodes"] if n["type"] == "dataset"],
    })
    out = [doc]
    for dt in dataset_types:
        out.append(dump_doc("!Dataset_Type", dt))
    for at in algo_types:
        out.append(dump_doc("!Algorithm_Type", at))
    return "\n".join(out)


def build_algo_repo_yaml(instances: list) -> str:
    return "\n".join(dump_doc("!Algorithm_Instance", inst) for inst in instances)


def build_dataset_repo_yaml(root_datasets: list) -> str:
    if not root_datasets:
        return ""
    return "\n".join(dump_doc("!Dataset_Instance", d) for d in root_datasets)


# Known asset_ids we can recover (extend this as you identify more)
KNOWN_ASSET_IDS = {
    "Pan_India_MWS": {
        "asset_id": "projects/corestack-datasets/assets/datasets/hydrological_boundaries/microwatershed",
        "region": "pan_india",
        "metadata": {
            "source": "CoRE Stack public GEE datasets account (corestack-datasets)",
            "description": "Pan-India micro-watershed V2 boundaries — public read-only GEE asset",
        },
    },
}


def process_dag(dag_dir: Path, out_dir: Path):
    dag_json = load_json(dag_dir / "dag.json")
    nodes = dag_json["stacd:graph"]["nodes"]
    edges = dag_json["stacd:graph"]["edges"]

    dataset_ids = [n["id"] for n in nodes if n["type"] == "dataset"]
    algo_ids = [n["id"] for n in nodes if n["type"] == "algorithm"]

    # root dataset = never the target of any edge (nothing produces it)
    targets = {e["target"] for e in edges}
    root_dataset_ids = [d for d in dataset_ids if d not in targets]

    algo_types, algo_instances = [], []
    for algo_id in algo_ids:
        algo_dir = dag_dir / "algorithms" / algo_id
        algo_json = load_json(algo_dir / "algorithm.json")
        algo_types.append({
            "id": algo_json["id"],
            "name": algo_json["title"],
            "description": algo_json["description"],
            "params": algo_json.get("stacd:parameters", []),
            "input_datasets": algo_json.get("stacd:input_datasets", []),
            "outputs": algo_json.get("stacd:output_datasets", []),
        })

        vfile = latest_version_file(algo_dir)
        if vfile:
            v = load_json(vfile)
            props = v["properties"]
            algo_instances.append({
                "type": props["stacd:algorithm_id"],
                "version": props["stacd:version"],
                "assets": props.get("stacd:assets", {}),
                "date": parse_date(props.get("datetime") or props["stacd:registered_at"]),
                "execution_modes": props.get("stacd:execution_modes", {}),
            })

    dataset_types = []
    for ds_id in dataset_ids:
        dataset_types.append({
            "id": ds_id,
            "name": ds_id.replace("_", " "),
            "description": ds_id.replace("_", " "),
            "format": "GEEAsset",  # TODO: verify — GEEAsset vs GeoServerLayer not in catalog
        })

    root_datasets = []
    for ds_id in root_dataset_ids:
        known = KNOWN_ASSET_IDS.get(ds_id, {})
        root_datasets.append({
            "type_id": ds_id,
            "version": "1",
            "region": known.get("region", "TODO_FILL_REGION"),
            "asset_id": known.get("asset_id", "TODO_FILL_ASSET_ID"),
            "metadata": known.get("metadata", {}),
        })

    target = out_dir / dag_dir.name
    target.mkdir(parents=True, exist_ok=True)
    (target / "dag.yaml").write_text(build_dag_yaml(dag_json, algo_types, dataset_types))
    (target / "algo_repo.yaml").write_text(build_algo_repo_yaml(algo_instances))
    ds_repo = build_dataset_repo_yaml(root_datasets)
    if ds_repo:
        (target / "dataset_repo.yaml").write_text(ds_repo)
    print(f"✓ {dag_dir.name}: {len(algo_types)} algos, {len(dataset_types)} datasets "
          f"({len(root_datasets)} root) -> {target}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog-dir", required=True)
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    catalog_dir = Path(args.catalog_dir)
    out_dir = Path(args.output_dir)
    for dag_dir in sorted(p for p in catalog_dir.iterdir() if (p / "dag.json").exists()):
        process_dag(dag_dir, out_dir)


if __name__ == "__main__":
    main()
