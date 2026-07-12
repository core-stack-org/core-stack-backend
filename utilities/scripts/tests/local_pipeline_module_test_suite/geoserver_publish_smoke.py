#!/usr/bin/env python
"""Smoke test GeoPackage publication through the local pipeline publisher."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gpkg", help="GeoPackage path to publish and verify.")
    parser.add_argument("--workspace", default="testworkspace")
    parser.add_argument("--layer-name", help="Published name for a single-layer package.")
    parser.add_argument("--source-layer", default=None)
    parser.add_argument(
        "--layer",
        action="append",
        default=[],
        metavar="PUBLISHED=SOURCE",
        help="Repeat to publish and verify multiple layers from one GeoPackage.",
    )
    parser.add_argument("--store-name", help="GeoServer datastore name for --layer mode.")
    parser.add_argument("--keep-layer", action="store_true")
    args = parser.parse_args()

    from dataclasses import asdict

    from computing.misc.local_pipeline.publish import (
        delete_datastore,
        publish_gpkg_layer,
        publish_gpkg_layers,
    )

    if args.layer:
        layers = dict(item.split("=", 1) for item in args.layer)
        store_name = args.store_name or "local_pipeline_multi_layer_smoke"
        results = publish_gpkg_layers(
            args.gpkg,
            workspace=args.workspace,
            store_name=store_name,
            layers=layers,
            overwrite=True,
        )
        print(json.dumps({name: asdict(value) for name, value in results.items()}, indent=2, default=str))
    else:
        if not args.layer_name:
            parser.error("--layer-name is required unless --layer is provided")
        store_name = args.layer_name
        result = publish_gpkg_layer(
            args.gpkg,
            workspace=args.workspace,
            layer_name=args.layer_name,
            source_layer=args.source_layer,
            overwrite=True,
        )
        print(json.dumps(asdict(result), indent=2, default=str))
    if not args.keep_layer:
        delete_datastore(args.workspace, store_name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
