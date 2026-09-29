# convert_lulc_to_uint8.py

Converts the pan-India LULC rasters (`lulc_v3_{year}_{year+1}.tif`, float64 with NaN) to
uint8 with no-data `0`. The grid stays the same and the CRS is written as EPSG:4326.

## What changes

| Pixel | Result |
|---|---|
| NaN (outside India) | `0` |
| Float noise, within 0.001 of a whole number (e.g. `5.999999999999999`) | rounded |
| Fractional (e.g. `3.26`, blended pixels on river-basin boundaries) | nearest class among its 8 clean neighbours; rounded if it has none |

Every other pixel keeps its value.

## Usage

```bash
python utilities/scripts/convert_lulc_to_uint8.py \
    data/base_layers/lulc/lulc_v3_20??_20??.tif \
    --output-dir data/base_layers/lulc_uint8
```

The `20??_20??` pattern skips `lulc_v3_2018_2019_old.tif`.

### Inside Docker

The repo is mounted at `/app` and the data at `$DATA_DIR` (the host's `./data`), so no rebuild is needed:

```bash
docker compose exec backend sh -c 'python utilities/scripts/convert_lulc_to_uint8.py \
    $DATA_DIR/base_layers/lulc/lulc_v3_20??_20??.tif \
    --output-dir $DATA_DIR/base_layers/lulc_uint8'
```

Keep the single quotes so the file pattern and `$DATA_DIR` are expanded inside the container.
For a long run, add `-d` after `exec` and append `> $DATA_DIR/lulc_uint8.log 2>&1` inside the quotes,
then follow it on the host with `tail -f data/lulc_uint8.log`.

If a file stops with "was not written as EPSG:4326", the container's PROJ setup is broken; run it on the host instead.

| Option | Default | |
|---|---|---|
| `--workers` | all cores | worker processes |
| `--tile` | `2048` | tile size in pixels (multiple of 256) |
| `--tol` | `0.001` | distance from a whole number treated as float noise |

Memory is about 0.5 GB per worker (GDAL's block cache is capped at 256 MB per process via
`GDAL_CACHEMAX`); with 24 workers a file takes roughly 5 minutes.

Each file is written as `.tmp` and moved into place only after its CRS is confirmed as
EPSG:4326. Files already in the output folder are skipped, so an interrupted run can be restarted.
The script prints, per file, how many pixels were NaN, rounded, and snapped.

## After converting

1. Move the originals to a backup folder **outside** `data/base_layers/lulc/`.
   Soil health uses the last three `lulc_v3_*.tif` files in that folder, so any extra file there gets picked up.
2. Move the converted files into `data/base_layers/lulc/` under the same names.
3. Upload them to `s3://corestack-datasets/base_layers/periodic_layers/lulc/lulc_v3/`.

Files written from the container are owned by root on the host, so do steps 1–2 inside the
container too (`docker compose exec backend sh -c 'mv ...'`) or with `sudo`.
