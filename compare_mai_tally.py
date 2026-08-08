"""
Fast MAI tally — compares our farm-level annual & kharif MAI against Shuvam's GEE raster.

Shuvam's raster bands (crop year Jul 2018 – Jun 2019):
  b1=Jul, b2=Aug, b3=Sep, b4=Oct, b5=Nov, b6=Dec,
  b7=Jan, b8=Feb, b9=Mar, b10=Apr, b11=May, b12=Jun,
  b13=Annual (crop-year mean)

Kharif months Jul-Oct (bands 1-4) are the SAME calendar period in both
pipelines, so kharif comparison is the fairest apples-to-apples tally.
"""
import dotenv
dotenv.load_dotenv('.env')

import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
import rasterio.features
import rasterio.windows
import time

TEHSILS = [
    {
        "name": "sanganer", "state": "rajasthan",
        "district": "jaipur", "block": "sanganer",
        "shuvam_raster": "data/et_rasters/shuvam_mai_sanganer_2018.tif",
    },
    {
        "name": "dudu", "state": "rajasthan",
        "district": "jaipur", "block": "dudu",
        "shuvam_raster": "data/et_rasters/shuvam_mai_dudu_2018.tif",
    },
]

# Shuvam crop-year band mapping (1-indexed for rasterio)
# Jul=1, Aug=2, Sep=3, Oct=4 -> kharif bands
SHUVAM_KHARIF_BANDS = [1, 2, 3, 4]   # Jul, Aug, Sep, Oct
SHUVAM_ANNUAL_BAND  = 13


def _zonal_mean_band(gdf_reprojected, bbox, raster_path, band_number):
    """Read one band and compute per-farm zonal mean."""
    minx, miny, maxx, maxy = bbox
    with rasterio.open(raster_path) as src:
        window    = rasterio.windows.from_bounds(minx, miny, maxx, maxy, src.transform)
        transform = rasterio.windows.transform(window, src.transform)
        data      = src.read(band_number, window=window).astype("float32")
        nodata_val = float(src.nodata) if src.nodata is not None else -9999.0

    data[data == nodata_val] = np.nan
    data[data <= -9999]      = np.nan

    out_shape = data.shape
    n_farms   = len(gdf_reprojected)
    shapes    = ((geom, idx) for idx, geom in enumerate(gdf_reprojected.geometry, start=1))
    labels    = rasterio.features.rasterize(
        shapes, out_shape=out_shape, transform=transform,
        fill=0, dtype="int32", all_touched=True,
    )

    valid    = np.isfinite(data)
    v_labels = labels[valid]
    v_vals   = data[valid]

    sums   = np.bincount(v_labels, weights=v_vals, minlength=n_farms + 1)
    counts = np.bincount(v_labels,                 minlength=n_farms + 1)
    with np.errstate(invalid="ignore", divide="ignore"):
        means = sums[1:] / counts[1:]
    means[counts[1:] == 0] = np.nan
    return means


def fast_zonal_means(gdf, raster_path, bands):
    """
    Compute per-farm mean for each band in `bands`.
    Returns a dict: band_number -> np.ndarray of length n_farms.
    """
    with rasterio.open(raster_path) as src:
        raster_crs = src.crs

    farms = gdf.to_crs(raster_crs)
    bbox  = farms.total_bounds

    results = {}
    for band in bands:
        results[band] = _zonal_mean_band(farms, bbox, raster_path, band)
    return results


def print_diff(label, ours, theirs):
    both = ~(np.isnan(ours) | np.isnan(theirs))
    d    = np.abs(ours[both] - theirs[both])
    n    = both.sum()
    if n == 0:
        print(f"  {label}: no overlapping data")
        return
    print(f"  {label}: n={n:,} | mean_diff={d.mean():.4f} | median={np.median(d):.4f} "
          f"| max={d.max():.4f} | <0.01: {(d<0.01).sum():,} ({(d<0.01).mean()*100:.1f}%) "
          f"| >0.05: {(d>0.05).sum():,} ({(d>0.05).mean()*100:.1f}%)")


for t in TEHSILS:
    t0 = time.time()
    print(f"\n{'='*65}")
    print(f"TALLY — {t['name'].upper()}  2018")
    print(f"{'='*65}")

    base    = f"data/farm_boundaries/{t['state']}/{t['district']}/{t['block']}"
    static  = gpd.read_parquet(f"{base}/farm_static.parquet")
    annual  = pd.read_parquet(f"{base}/farm_annual.parquet")
    annual  = annual[annual['year'] == 2018]
    monthly = pd.read_parquet(f"{base}/farm_monthly.parquet")
    monthly = monthly[monthly['year'] == 2018]

    # Our kharif MAI = mean of Jul, Aug, Sep, Oct monthly MAI
    monthly['month_str'] = monthly['date'].astype(str).str[5:7]
    kharif_monthly = monthly[monthly['month_str'].isin(['07','08','09','10'])]
    kharif_mai_ours = kharif_monthly.groupby('farm_id')['mai'].mean().reset_index()
    kharif_mai_ours.columns = ['farm_id', 'kharif_mai_ours']

    gdf = static[['farm_id','geometry']].merge(
        annual[['farm_id','mai_annual']], on='farm_id', how='inner'
    ).merge(kharif_mai_ours, on='farm_id', how='left')
    print(f"Loaded {len(gdf):,} farm polygons")

    # Compute zonal means for annual band + kharif bands
    bands_to_read = [SHUVAM_ANNUAL_BAND] + SHUVAM_KHARIF_BANDS
    print(f"Computing zonal means (bands {bands_to_read})...")
    band_means = fast_zonal_means(gdf, t['shuvam_raster'], bands_to_read)

    gdf['shuvam_annual']  = band_means[SHUVAM_ANNUAL_BAND]
    # Shuvam kharif MAI = mean of Jul(b1)+Aug(b2)+Sep(b3)+Oct(b4) per farm
    kharif_stack = np.stack([band_means[b] for b in SHUVAM_KHARIF_BANDS], axis=1)
    kharif_mean  = np.nanmean(kharif_stack, axis=1)
    kharif_mean[np.all(np.isnan(kharif_stack), axis=1)] = np.nan
    gdf['shuvam_kharif'] = kharif_mean

    print(f"Done in {time.time()-t0:.0f}s\n")

    print("--- ANNUAL MAI (our Jan-Dec vs Shuvam crop-year Jul18-Jun19) ---")
    print_diff("annual", gdf['mai_annual'].values, gdf['shuvam_annual'].values)

    print("\n--- KHARIF MAI Jul-Oct 2018 (same period in both pipelines) ---")
    print_diff("kharif", gdf['kharif_mai_ours'].values, gdf['shuvam_kharif'].values)

    print("\n--- Coverage ---")
    print(f"  NaN in our annual:     {gdf['mai_annual'].isna().sum():,}")
    print(f"  NaN in Shuvam annual:  {gdf['shuvam_annual'].isna().sum():,}")
    print(f"  NaN in our kharif:     {gdf['kharif_mai_ours'].isna().sum():,}")
    print(f"  NaN in Shuvam kharif:  {gdf['shuvam_kharif'].isna().sum():,}")

    # Save full tally
    out = f"{base}/mai_tally_2018.parquet"
    gdf[['farm_id','mai_annual','shuvam_annual','kharif_mai_ours','shuvam_kharif']].to_parquet(out, index=False)
    print(f"\nFull tally saved -> {out}")
    print(f"Total time: {time.time()-t0:.0f}s")

print("\nAll done.")
