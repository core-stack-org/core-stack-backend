"""Re-run Phase 3 for Sanganer and Dudu with all three fixes applied:
1. -9999 nodata → NaN at read time (using src.nodata from raster metadata)
2. MAI capped at [0,1] — values > 1 are raster artifacts, logged and capped
3. Improved logging: nan_farms, valid_farms, severe count all reported
"""
import os
import dotenv
dotenv.load_dotenv('.env')

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)

from computing.farm_boundaries.et_intersection import intersect_et_with_farms

print("=" * 60)
print("Re-running Phase 3 — Sanganer, Jaipur 2018")
print("=" * 60)
intersect_et_with_farms('rajasthan', 'jaipur', 'sanganer', year=2018, overwrite=True)

print("\n" + "=" * 60)
print("Re-running Phase 3 — Dudu, Jaipur 2018")
print("=" * 60)
intersect_et_with_farms('rajasthan', 'jaipur', 'dudu', year=2018, overwrite=True)

print("\nDone. All parquets regenerated with bug fixes applied.")
