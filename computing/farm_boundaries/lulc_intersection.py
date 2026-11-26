import os
import logging

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.windows import Window
import numpy as np
from shapely.geometry import box


from utilities.constants import (
    FARM_BOUNDARIES_PATH,
    LulcClass,
)
from computing.local_compute_helper import (
    ALL_CROP_CLASSES, LULC_BASE_DIR,
    SINGLE_CROP_CLASSES, compute_cropping_intensity, resolve_lulc_raster_paths,
)

logger = logging.getLogger(__name__)

INITIAL_YEAR = 2017

CROP_NAME_COLUMNS = ["crop1", "crop2", "crop3"]
CROP_CONF_COLUMNS = ["conf1", "conf2", "conf3"]
CROP_DATE_COLUMNS = ["crop_start_date", "crop_end_date"]

#* Helpers

def _farm_parquet_path(state, district, block, parquet_type):
    return os.path.join(os.path.join(FARM_BOUNDARIES_PATH, state, district, block), parquet_type)

def _enforce_crop_schema(df):
    """Crop-name columns -> string, confidence columns -> float64, date columns -> dd/mm/yyyy string."""
    df = df.copy()
    for col in CROP_NAME_COLUMNS + CROP_DATE_COLUMNS:
        if col not in df.columns:
            df[col] = None
        elif pd.api.types.is_numeric_dtype(df[col]):
            if df[col].notna().any():
                logger.warning(
                    "Column %s held numeric codes; nulling it (reprocess to restore names).",
                    col,
                )
            df[col] = None
        df[col] = df[col].astype("string")
    for col in CROP_CONF_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    if "crop_intensity" not in df.columns:
        df["crop_intensity"] = np.nan
    df["crop_intensity"] = pd.to_numeric(df["crop_intensity"], errors="coerce").astype("float64")
    return df

def _write_annual_parquet(df, path):
    """Write annual parquet with an explicit crop schema, even if a column is all-null."""
    df = _enforce_crop_schema(df)
    table = pa.Table.from_pandas(df, preserve_index=False)
    targets = {c: pa.string() for c in CROP_NAME_COLUMNS + CROP_DATE_COLUMNS}
    targets.update({c: pa.float64() for c in CROP_CONF_COLUMNS})
    targets["crop_intensity"] = pa.float64()
    for col, typ in targets.items():
        i = table.schema.get_field_index(col)
        if table.schema.field(i).type != typ:
            table = table.set_column(i, pa.field(col, typ), table.column(i).cast(typ))
    pq.write_table(table, path)


def _farm_pixel_window(geom, transform, arr_height, arr_width):
    minx, miny, maxx, maxy = geom.bounds
    inv = ~transform  # map (x, y) -> fractional (col, row) pixel coords

    col_a, row_a = inv * (minx, maxy)
    col_b, row_b = inv * (maxx, miny)

    row_start = max(int(np.floor(min(row_a, row_b))) - 1, 0)
    row_stop  = min(int(np.ceil(max(row_a, row_b))) + 1, arr_height)
    col_start = max(int(np.floor(min(col_a, col_b))) - 1, 0)
    col_stop  = min(int(np.ceil(max(col_a, col_b))) + 1, arr_width)

    return row_start, row_stop, col_start, col_stop    


def _farm_pixel_weights(geom, transform, row_start, row_stop, col_start, col_stop):

    weighted_pixels = []
    for row in range(row_start, row_stop):
        for col in range(col_start, col_stop):
            x0, y0 = transform * (col, row)
            x1, y1 = transform * (col + 1, row + 1)
            pixel_box = box(min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))

            if not geom.intersects(pixel_box):
                continue

            overlap = geom.intersection(pixel_box)
            weight = overlap.area
            if weight > 0:
                weighted_pixels.append((row, col, weight))

    return weighted_pixels    


def compute_farm_cropping_intensity(farm_gdf, curr_year):
    
    lulc_dir = LULC_BASE_DIR
    initial_year = INITIAL_YEAR
    ever_cropped = None
    ref = None
    areas = {}
    farms = farm_gdf[["farm_id", "geometry"]].reset_index(drop=True)
    n_farms = len(farms)
    
    
    for year in range(initial_year, curr_year + 1):
        try:
            raster_path = resolve_lulc_raster_paths(year, year, lulc_dir)[0]
            
        except FileNotFoundError:
            print(f"LULC raster for {year}-{year + 1} not found - stopping")
            break

        with rasterio.open(raster_path) as src:
            if ref is None:
                win = src.window(*farms.total_bounds)
                col0 = max(int(np.floor(win.col_off)), 0)
                row0 = max(int(np.floor(win.row_off)), 0)
                col1 = min(int(np.ceil(win.col_off + win.width)), src.width)
                row1 = min(int(np.ceil(win.row_off + win.height)), src.height)

                if col1 <= col0 or row1 <= row0:
                    print("Farms lie outside the LULC raster - cropping intensity is 0.0 for all farms")
                    return pd.DataFrame(
                        {"farm_id": farms["farm_id"].values, "year": curr_year, "crop_intensity": 0.0}
                    )

                window = Window(col0, row0, col1 - col0, row1 - row0)
                shape = (row1 - row0, col1 - col0)
                ref = (src.transform, (src.height, src.width), window)
                transform = src.window_transform(window)

                f_idx, f_rows, f_cols, f_w = [], [], [], []
                for i, geom in enumerate(farms.geometry):
                    if geom is None or geom.is_empty:
                        continue
                    
                    r0, r1, c0, c1 = _farm_pixel_window(geom, transform, *shape)
                    for r, c, weight in _farm_pixel_weights(geom, transform, r0, r1, c0, c1):
                        f_idx.append(i)
                        f_rows.append(r)
                        f_cols.append(c)
                        f_w.append(weight)
                    if (i + 1) % 10000 == 0 or (i + 1) == n_farms:
                        print(f"Computed LULC pixel weights for {i + 1}/{n_farms} farms")

                farm_idx = np.asarray(f_idx, dtype=np.int64)
                rows = np.asarray(f_rows, dtype=np.int64)
                cols = np.asarray(f_cols, dtype=np.int64)
                w = np.asarray(f_w, dtype=np.float64)

                ever_cropped = np.zeros(shape, dtype=bool)

            lulc = src.read(1, window=ref[2])

        print(f"Computing farm cropping intensity inputs for {year}-{year + 1}")
        vals = lulc[rows, cols]
        areas[year] = (
            np.bincount(farm_idx, weights=w * np.isin(vals, SINGLE_CROP_CLASSES), minlength=n_farms),
            np.bincount(farm_idx, weights=w * (vals == LulcClass.DOUBLE_CROPPING), minlength=n_farms),
            np.bincount(farm_idx, weights=w * (vals == LulcClass.TRIPLE_ANNUAL_PERENNIAL), minlength=n_farms),
        )
        ever_cropped |= np.isin(lulc, ALL_CROP_CLASSES)

    if curr_year not in areas:
        raise FileNotFoundError(f"LULC raster for {curr_year}-{curr_year + 1} not found in {lulc_dir}")

    if ref is not None:
        denominator = np.bincount(farm_idx, weights=w * ever_cropped[rows, cols], minlength=n_farms)
    else:
        denominator = np.zeros(n_farms)

    
    intensity = compute_cropping_intensity(*areas[curr_year], denominator)
    return pd.DataFrame(
        {"farm_id": farms["farm_id"].values, "year": curr_year, "crop_intensity": intensity}
    )



# Main Function

def compute_year_LULC_values(state, district, block, year):
    year = int(year)
    logger.info(
        "Computing Cropping Intensity values for: %s/%s/%s of year %d",
        state, district, block, year
    )

    static_path = _farm_parquet_path(state, district, block, "farms/static.parquet")
    annual_path = _farm_parquet_path(state, district, block, "farms/annual.parquet")

    if not os.path.exists(static_path):
        raise FileNotFoundError(f"Static parquet not found on path: {static_path}")

    if not os.path.exists(annual_path):
        raise FileNotFoundError(f"Annual parquet not found on path: {annual_path}")

    static_gdf = gpd.read_parquet(static_path)
    annual = pd.read_parquet(annual_path)

    ci = compute_farm_cropping_intensity(static_gdf, year)
    ci["crop_intensity"] = ci["crop_intensity"].round(4)

    in_year = annual["year"] == year

    if in_year.any():
        # rows for this year already exist, only fill the crop_intensity column
        mapping = ci.set_index("farm_id")["crop_intensity"]
        annual.loc[in_year, "crop_intensity"] = annual.loc[in_year, "farm_id"].map(mapping)
        action = "updated"
    else:
        # new year: add one row per farm
        new = ci[["farm_id", "year", "crop_intensity"]].copy()
        new["state"], new["district"], new["tehsil"] = state, district, block
        new["area_in_ha"] = new["farm_id"].map(static_gdf.set_index("farm_id")["area_m2"] / 10_000).round(4)
        annual = pd.concat([annual, new], ignore_index=True)
        action = "appended"

    tmp = annual_path + ".tmp"
    _write_annual_parquet(annual, tmp)
    os.replace(tmp, annual_path)

    return {"year": year, "action": action, "farms": int(len(ci)),
            "null_ci": int(ci["crop_intensity"].isna().sum())}
