import json
import os

from computing.STAC_specs.stac_collection import STACConfig, sanitize_text


def _read_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def build_stac_item_id(state, district, block, layer_name, year=""):
    parts = [state, district, block, layer_name]
    if year not in (None, ""):
        parts.append(str(year))
    return "_".join(parts)


def collect_generated_stac_specs(
    *,
    state,
    district,
    block,
    layer_name,
    layer_type,
    start_year="",
    end_year="",
):
    """Return STAC catalog metadata and items scoped to a single layer."""
    state = sanitize_text(str(state).lower())
    district = sanitize_text(str(district).lower())
    block = sanitize_text(str(block).lower())
    layer_name = sanitize_text(str(layer_name).lower())

    stac_config = STACConfig()
    tehsil_dirname = stac_config.tehsil_dirname
    base_dir = stac_config.stac_files_dir
    tehsil_dir = os.path.join(base_dir, tehsil_dirname)
    state_dir = os.path.join(tehsil_dir, state)
    district_dir = os.path.join(state_dir, district)
    block_dir = os.path.join(district_dir, block)

    root_catalog = _read_json(os.path.join(base_dir, "catalog.json"))
    tehsil_catalog = _read_json(os.path.join(tehsil_dir, "catalog.json"))
    state_collection = _read_json(os.path.join(state_dir, "collection.json"))
    district_collection = _read_json(os.path.join(district_dir, "collection.json"))
    block_collection = _read_json(os.path.join(block_dir, "collection.json"))

    requested_item_ids = []
    if layer_type == "raster" and str(start_year).strip() and str(end_year).strip():
        requested_item_ids = [
            build_stac_item_id(state, district, block, layer_name, str(y))
            for y in range(int(start_year), int(end_year) + 1)
        ]
    elif str(start_year).strip():
        requested_item_ids = [
            build_stac_item_id(
                state, district, block, layer_name, str(start_year).strip()
            )
        ]
    else:
        requested_item_ids = [
            build_stac_item_id(state, district, block, layer_name, "")
        ]

    item_prefix = build_stac_item_id(state, district, block, layer_name, "")
    discovered_item_ids = []
    if os.path.isdir(block_dir):
        for entry in os.listdir(block_dir):
            if entry.startswith(item_prefix) and os.path.isdir(
                os.path.join(block_dir, entry)
            ):
                discovered_item_ids.append(entry)

    item_ids = sorted(set(requested_item_ids + discovered_item_ids))

    items = []
    for item_id in item_ids:
        item_path = os.path.join(block_dir, item_id, f"{item_id}.json")
        item_spec = _read_json(item_path)
        if item_spec is not None:
            items.append(item_spec)

    return {
        "root_catalog": root_catalog,
        "tehsil_catalog": tehsil_catalog,
        "state_collection": state_collection,
        "district_collection": district_collection,
        "block_collection": block_collection,
        "items": items,
    }
