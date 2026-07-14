"""Local-disk download helper for the Farm Stress Detection System.

Uses Earth Engine's synchronous getDownloadURL endpoint rather than the
async GCS/Drive export+poll path used elsewhere in this repo: the historical
archive rasters here are small (single-band, ~11km resolution, well under
Earth Engine's per-request download size limit), so a direct HTTP download
is simpler and faster than submitting a batch export task and separately
pulling it down from GCS afterwards. That async path is still the right
tool for the much larger weekly 500m operational rasters (a later phase).

Adapted from a similar CHIRPS/MODIS-PET download script already used
elsewhere in this repo (data/test_code.py) for a closely related
SPEI-inputs pipeline.
"""

import time
from pathlib import Path

import requests


def download_image(image, region, output_path, scale, crs="EPSG:4326", retries=3):
    """Stream a single ee.Image to a local GeoTIFF via the direct download endpoint.

    region: an ee.Geometry (e.g. from get_india_bbox()).
    output_path: str or Path for the local .tif file; parent dirs are created.
    """
    output_path = Path(output_path)
    params = {"scale": scale, "region": region, "format": "GEO_TIFF"}
    if crs:
        params["crs"] = crs

    for attempt in range(1, retries + 1):
        try:
            url = image.getDownloadURL(params)
            with requests.get(url, stream=True, timeout=300) as response:
                response.raise_for_status()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
            return output_path
        except Exception:
            # Remove partial files so a retry (or a later rerun) doesn't
            # mistake a corrupt/truncated download for a completed one.
            if output_path.exists():
                output_path.unlink()
            if attempt == retries:
                raise
            time.sleep(2 * attempt)
