import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterio.mask import mask

# ------------------------------------------------------------------
# INPUTS
# ------------------------------------------------------------------

HWSD_RASTER = "data/HWSD_RASTER/hwsd_wgs84.tif"
HWSD_CSV = "data/HWSD_DATA.csv"
INDIA_BOUNDARY = "data/india_state_outer_no_islands.geojson"

OUTPUT_RASTER = "data/india_subsoil_texture.tif"

# ------------------------------------------------------------------
# LOAD INDIA BOUNDARY
# ------------------------------------------------------------------

india = gpd.read_file(INDIA_BOUNDARY)

if india.crs is None:
    india = india.set_crs("EPSG:4326")
# ------------------------------------------------------------------
# LOAD LOOKUP TABLE
# ------------------------------------------------------------------

df = pd.read_csv(HWSD_CSV, usecols=["MU_GLOBAL", "S_USDA_TEX_CLASS"], low_memory=False)

lookup_df = df[["MU_GLOBAL", "S_USDA_TEX_CLASS"]].dropna().drop_duplicates("MU_GLOBAL")

max_mu = int(lookup_df["MU_GLOBAL"].max())

# Lookup table:
# lut[MU_GLOBAL] = S_USDA_TEX_CLASS
lut = np.zeros(max_mu + 1, dtype=np.int16)

lut[lookup_df["MU_GLOBAL"].astype(int)] = lookup_df["S_USDA_TEX_CLASS"].astype(int)

# ------------------------------------------------------------------
# CLIP HWSD TO INDIA
# ------------------------------------------------------------------

with rasterio.open(HWSD_RASTER) as src:
    print(src.crs)  # None

    if src.crs is None:
        raster_crs = "EPSG:4326"
    else:
        raster_crs = src.crs

    if india.crs != raster_crs:
        india = india.to_crs(raster_crs)

    clipped, transform = mask(src, india.geometry, crop=True, nodata=0)

    profile = src.profile.copy()

mu_global = clipped[0]

# ------------------------------------------------------------------
# CONVERT MU_GLOBAL -> SUBSOIL TEXTURE CLASS
# ------------------------------------------------------------------

texture = np.zeros(mu_global.shape, dtype=np.int16)

valid = (mu_global > 0) & (mu_global <= max_mu)

texture[valid] = lut[mu_global[valid]]

# ------------------------------------------------------------------
# SAVE OUTPUT
# ------------------------------------------------------------------

profile.update(
    driver="GTiff",
    height=texture.shape[0],
    width=texture.shape[1],
    transform=transform,
    dtype=rasterio.int16,
    nodata=0,
    compress="lzw",
)

with rasterio.open(OUTPUT_RASTER, "w", **profile) as dst:
    dst.write(texture, 1)

print(f"Saved: {OUTPUT_RASTER}")

# ------------------------------------------------------------------
# OPTIONAL: PRINT CLASS DISTRIBUTION
# ------------------------------------------------------------------

unique, counts = np.unique(texture[texture > 0], return_counts=True)

print("\nTexture classes found in India:")
for cls, cnt in zip(unique, counts):
    print(f"Class {cls}: {cnt:,} pixels")
