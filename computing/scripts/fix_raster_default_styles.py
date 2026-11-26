"""Apply the intended default style to rasters on PROD GeoServer that lack one.

Run from the repository root (dry run by default, nothing is changed)::

    python -m computing.scripts.fix_raster_default_styles
    python -m computing.scripts.fix_raster_default_styles --workspace tree_ch_raster
    python -m computing.scripts.fix_raster_default_styles --apply

Targets PROD_GEOSERVER_* from nrm_app/.env only. For each layer in the selected
workspaces it reads the layer's current default style and fixes it only when:

* it is GeoServer's built-in ``raster`` (i.e. no style was ever applied), or
* the layer read fails with HTTP 500 (dangling/null style reference).

Layers that already carry any other style are never touched. Layers with a
coverage store but no layer record (HTTP 404) are only reported; this script
cannot fix those, they need a re-publish.
"""

import argparse
import logging
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import django
import requests

# workspace -> style that should be the layer default
WORKSPACE_STYLES = {
    "tree_ch_raster": "tree_ch_style",
    "tree_ccd_raster": "tree_ccd_style",
    "tree_overall_raster": "tree_overall_style",
    "plantation": "site_suitability",
    "LULC_level_1": "lulc_level_1_style",
    "LULC_level_2": "lulc_level_2_style",
    "LULC_level_3": "lulc_level_3_style",
    "terrain": "terrain_raster",
    "restoration": "restoration_style",
    "clart": "testClart",
    "stream_order": "stream_order",
    "catchment_area_singleflow": "catchment_area_singleflow",
    "distance_nearest_upstream_DL": "distance_nearest_upstream_DL",
    "natural_depression": "natural_depression",
    "slope_percentage": "slope_percentage",
}
UNSTYLED = "raster"  # GeoServer's built-in default raster style

logger = logging.getLogger("fix_raster_default_styles")


def _setup_django():
    sys.path.insert(0, os.getcwd())
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nrm_app.settings")
    django.setup()


def _prod_conf():
    from django.conf import settings

    url = (getattr(settings, "PROD_GEOSERVER_URL", "") or "").rstrip("/")
    if not url:
        raise SystemExit("PROD_GEOSERVER_URL is not configured; refusing to run.")
    return url, (settings.PROD_GEOSERVER_USERNAME, settings.PROD_GEOSERVER_PASSWORD)


def _get(url, auth, path):
    return requests.get(
        f"{url}/rest/{path}",
        auth=auth,
        headers={"Accept": "application/json"},
        timeout=60,
    )


def _list_stores(url, auth, workspace):
    resp = _get(url, auth, f"workspaces/{workspace}/coveragestores.json")
    resp.raise_for_status()
    stores = resp.json().get("coverageStores")
    return [s["name"] for s in stores["coverageStore"]] if stores else []


def _current_style(url, auth, workspace, layer_name):
    """Return (status, style_name_or_None) for a layer."""
    resp = _get(url, auth, f"layers/{workspace}:{layer_name}.json")
    if resp.status_code != 200:
        return resp.status_code, None
    style = resp.json()["layer"].get("defaultStyle") or {}
    return 200, style.get("name")


def _style_exists(url, auth, workspace, style_name):
    return _get(url, auth, f"workspaces/{workspace}/styles/{style_name}.json").ok


def _classify(status, current, workspace, style_name):
    if status == 404:
        return "no_layer"
    if status != 200:
        return "broken"  # e.g. 500 dangling style reference
    if current == f"{workspace}:{style_name}" or current == style_name:
        return "ok"
    if current in (None, UNSTYLED):
        return "unstyled"
    return "other_style"  # deliberately styled; leave alone


def run(workspaces, apply, workers):
    from utilities.geoserver_utils import Geoserver

    url, auth = _prod_conf()
    geo = Geoserver(service_url=url, username=auth[0], password=auth[1])
    print(f"GeoServer: {url}  mode: {'APPLY' if apply else 'DRY RUN'}")

    exit_code = 0
    for workspace in workspaces:
        style_name = WORKSPACE_STYLES[workspace]
        if not _style_exists(url, auth, workspace, style_name):
            print(f"[{workspace}] style '{style_name}' missing on prod; skipping.")
            exit_code = 1
            continue

        stores = _list_stores(url, auth, workspace)

        def check(name):
            status, current = _current_style(url, auth, workspace, name)
            return name, _classify(status, current, workspace, style_name), current

        with ThreadPoolExecutor(workers) as pool:
            results = list(pool.map(check, stores))

        counts = Counter(kind for _, kind, _ in results)
        print(f"[{workspace}] {len(stores)} layers, target style '{style_name}': "
              f"{dict(counts)}")
        to_fix = [n for n, kind, _ in results if kind in ("unstyled", "broken")]
        for name, kind, _ in results:
            if kind == "no_layer":
                print(f"  NO LAYER (needs re-publish): {workspace}:{name}")

        if not apply:
            for name in to_fix:
                print(f"  would fix: {workspace}:{name}")
            continue

        failed = []
        for name in to_fix:
            try:
                resp = geo.publish_style(
                    layer_name=name, style_name=style_name, workspace=workspace
                )
                status, current = _current_style(url, auth, workspace, name)
                if _classify(status, current, workspace, style_name) != "ok":
                    failed.append((name, resp))
            except Exception as exc:  # keep going; report at the end
                failed.append((name, exc))
        print(f"  fixed {len(to_fix) - len(failed)}/{len(to_fix)}")
        for name, why in failed:
            print(f"  FAILED: {workspace}:{name}: {why}")
            exit_code = 1
    return exit_code


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--workspace",
        action="append",
        choices=sorted(WORKSPACE_STYLES),
        help="Limit to a workspace (repeatable). Default: all of them.",
    )
    parser.add_argument(
        "--apply", action="store_true", help="Make changes (default is a dry run)."
    )
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    _setup_django()
    sys.exit(run(args.workspace or sorted(WORKSPACE_STYLES), args.apply, args.workers))


if __name__ == "__main__":
    main()
