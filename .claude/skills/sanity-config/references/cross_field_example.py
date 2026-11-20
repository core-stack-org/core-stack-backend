"""Example cross-field check (cropping intensity). Copy and adapt per pipeline.

The sanity framework checks one field at a time, so relationships between
fields need an ad hoc script. Derive the relationships from the pipeline code
(here computing/cropping_intensity/cropping_intensity_local.py), then test them
on fetched prod data. Findings either reveal a pipeline bug or justify a future
cross-field rule type in computing/sanity/rules.py.

  python -I cross_field_example.py ~/.cache/core-stack-sanity
"""

import json
import sys
from pathlib import Path

import pandas as pd

TOL = 0.01  # areas are rounded; ignore float noise

for vec in sorted(Path(sys.argv[1]).glob("*/vector/cropping_intensity.geojson")):
    df = pd.DataFrame([f["properties"] for f in json.loads(vec.read_text())["features"]])
    denom = next(c for c in df if c.startswith("total_cropable_area_ever_hydroyear_"))
    years = sorted(int(c.rsplit("_", 1)[1]) for c in df if c.startswith("cropping_intensity_"))
    issues = {}

    def count(name, mask):
        if int(mask.sum()):
            issues[name] = issues.get(name, 0) + int(mask.sum())

    count("croppable > mws area", df[denom] > df["area_in_ha"] + TOL)
    for y in years:
        single, kharif, non_kharif = (df[f"{p}_cropped_area_{y}"] for p in ("single", "single_kharif", "single_non_kharif"))
        double, triple = df[f"doubly_cropped_area_{y}"], df[f"triply_cropped_area_{y}"]
        count("single != kharif + non_kharif", (single - (kharif + non_kharif)).abs() > TOL)
        count("cropped > croppable", (single + double + triple) > df[denom] + TOL)
        recomputed = ((single + 2 * double + 3 * triple) / df[denom]).where(df[denom] > 0, 0.0)
        count("intensity != recomputed", (df[f"cropping_intensity_{y}"] - recomputed).abs() > TOL)

    print(f"{vec.parents[1].name}: {len(df)} rows x {len(years)} years; issues={issues or 'none'}")
