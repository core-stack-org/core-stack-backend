"""Fetch one tehsil's prod data for sanity-config testing.

Downloads, into <cache>/<state>__<district>__<tehsil>/:
  metadata/                  layer catalog + tehsil_data (via public_api_client)
  vector/<config>.geojson    the prod vector layer each config validates
  stats.xlsx                 one sheet per config sheet_name found in tehsil_data

Run inside the conda env (geo libraries need it), from the repo root:
  conda run -n corestackenv python .claude/skills/sanity-config/scripts/fetch_tehsil.py \
      --state jharkhand --district dumka --tehsil masalia [--configs cropping_intensity,terrain]

The API key is read from --key-file (default _rundata/core-stack-api-key.txt) and
passed to the client through the environment; it is never printed.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

import pandas as pd
import yaml

SKILL_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = SKILL_DIR.parents[2]
CONFIG_DIR = REPO_DIR / "computing" / "sanity" / "configs"
DEFAULT_CACHE = Path.home() / ".cache" / "core-stack-sanity"
GEOSERVER_WFS = (
    "https://geoserver.core-stack.org:8443/geoserver/{workspace}/ows?service=WFS"
    "&version=1.0.0&request=GetFeature&typeName={workspace}:{name}"
)


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def load_config(stem: str) -> dict:
    return yaml.safe_load((CONFIG_DIR / f"{stem}.yaml").read_text(encoding="utf-8-sig"))


def run_client(args, out_dir: Path, cache: Path) -> None:
    key_file = Path(args.key_file)
    if not key_file.is_file():
        sys.exit(f"API key file not found: {key_file}")
    env = dict(os.environ, PUBLIC_API_X_API_KEY=key_file.read_text().strip())
    active = cache / "active_locations.json"
    cmd = [
        sys.executable,
        str(REPO_DIR / "installation" / "public_api_client.py"),
        "--active-locations-file", str(active),
    ]
    if not active.is_file():
        cmd.append("--refresh-active-locations")
    cmd += [
        "download",
        "--state", args.state, "--district", args.district, "--tehsil", args.tehsil,
        "--datasets", "layer_catalog,tehsil_data", "--metadata-only",
        "--output-dir", str(out_dir),
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        # The client explains unmatched locations; surface that, nothing else.
        sys.exit(result.stdout[-2000:] + result.stderr[-2000:])


def fetch_vector(url: str, dest: Path) -> int:
    if "outputFormat" not in url:
        url += "&outputFormat=application/json"
    with urllib.request.urlopen(url, timeout=300) as response:
        data = response.read()
    dest.write_bytes(data)
    return len(json.loads(data).get("features", []))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state", required=True)
    parser.add_argument("--district", required=True)
    parser.add_argument("--tehsil", required=True)
    parser.add_argument("--configs", help="Comma-separated config stems (default: all)")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--key-file", default=str(REPO_DIR / "_rundata" / "core-stack-api-key.txt"))
    args = parser.parse_args()

    args.cache.mkdir(parents=True, exist_ok=True)
    out_dir = args.cache / "__".join(slug(v) for v in (args.state, args.district, args.tehsil))
    (out_dir / "vector").mkdir(parents=True, exist_ok=True)

    if not (out_dir / "metadata" / "tehsil_data.json").is_file():
        run_client(args, out_dir, args.cache)
    meta = out_dir / "metadata"
    resolved = json.loads((meta / "resolved_location.json").read_text())
    district, tehsil = slug(resolved["district"]), slug(resolved["tehsil"])
    print(f"{resolved['state']} / {resolved['district']} / {resolved['tehsil']} -> {out_dir}")

    layers = json.loads((meta / "generated_layer_urls.json").read_text())
    layers = layers if isinstance(layers, list) else layers.get("layers", layers)
    tehsil_data = json.loads((meta / "tehsil_data.json").read_text())
    layer_map = yaml.safe_load((SKILL_DIR / "references" / "layer_map.yaml").read_text())

    all_stems = sorted(p.stem for p in CONFIG_DIR.glob("*.yaml"))
    # Sheets are free (already in tehsil_data), so always write every config's
    # sheet; --configs only limits which vector layers get downloaded.
    sheets = {}
    for stem in all_stems:
        for sheet in load_config(stem).get("sheet_names") or []:
            rows = tehsil_data.get(sheet)
            if rows:
                sheets[sheet] = pd.DataFrame(rows)

    for stem in args.configs.split(",") if args.configs else all_stems:
        target = layer_map.get(stem)
        if target is None:
            print(f"  {stem:34} vector: none on prod (layer_map: null)")
            continue
        if "wfs" in target:
            # On prod GeoServer but not in the generated-layer catalog.
            workspace, name = target["wfs"].format(district=district, tehsil=tehsil).split(":", 1)
            match = {"layer_name": name, "layer_url": GEOSERVER_WFS.format(workspace=workspace, name=name),
                     "layer_version": "wfs"}
        else:
            pattern = re.compile(target["layer"].format(district=district, tehsil=tehsil))
            candidates = [
                l for l in layers if l.get("layer_type") == "vector"
                and l.get("dataset_name") == target["dataset"] and pattern.search(l["layer_name"])
            ]
            if target.get("pick") == "latest_year":
                # Year-suffixed layers (e.g. soge_vector_<d>_<t>_2025): prefer the newest.
                def year(layer):
                    found = re.search(r"_(\d{4})$", layer["layer_name"])
                    return int(found.group(1)) if found else 0
                candidates.sort(key=year, reverse=True)
            match = candidates[0] if candidates else None
            if match is None:
                print(f"  {stem:34} vector: NOT FOUND ({target['dataset']} / {pattern.pattern})")
                continue
        dest = out_dir / "vector" / f"{stem}.geojson"
        if dest.is_file():
            print(f"  {stem:34} vector: cached {match['layer_name']}")
            continue
        try:
            n = fetch_vector(match["layer_url"], dest)
            print(f"  {stem:34} vector: {match['layer_name']} v{match.get('layer_version')} ({n} features)")
        except Exception as exc:  # one broken layer shouldn't stop the rest
            dest.unlink(missing_ok=True)
            print(f"  {stem:34} vector: FAILED {match['layer_name']}: {exc}")

    if sheets:
        with pd.ExcelWriter(out_dir / "stats.xlsx") as writer:
            for sheet, frame in sheets.items():
                frame.to_excel(writer, sheet_name=sheet[:31], index=False)
    print(f"  excel sheets written: {len(sheets)}")


if __name__ == "__main__":
    main()
