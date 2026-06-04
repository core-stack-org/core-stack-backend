import ee
import argparse
import calendar
import sys
import time


MONTH_ABBR = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

MODIS_COL = "MODIS/061/MOD16A2GF"
MCD12Q1_COL = "MODIS/061/MCD12Q1"

# NoData sentinel used in GEE images and written to masked pixels in assets.
NODATA = -9999.0
EXPORT_BAND_NAMES = [f"b{i}" for i in range(1, 14)]


def _asset_token(value: str) -> str:
    cleaned = []
    for char in str(value).strip().lower():
        cleaned.append(char if char.isalnum() else "_")
    token = "".join(cleaned).strip("_")
    while "__" in token:
        token = token.replace("__", "_")
    return token or "unknown"


def _build_asset_id(cfg: dict, label: str) -> str:
    root = str(cfg.get("asset_root", "")).rstrip("/")
    if not root:
        raise ValueError("asset_root is required")
    asset_suffix = _asset_token(cfg.get("asset_suffix"))
    year = int(cfg["year"])
    return f"{root}/{label}_{asset_suffix}_{year}"


def _asset_exists(asset_id: str) -> bool:
    try:
        ee.data.getAsset(asset_id)
        return True
    except Exception:
        return False


def build_common_pixel_mask(
    region: ee.Geometry, default_proj: ee.Projection
) -> ee.Image:
    """Rasterize the tehsil once on the chosen 30 m grid for all outputs."""
    return (
        ee.Image.constant(1)
        .rename("common_mask")
        .setDefaultProjection(default_proj)
        .clip(region)
        .unmask(0)
        .gt(0)
        .selfMask()
    )


def ee_annual_total_band(
    monthly_stack: ee.Image, prefix: str, year: int, band_name: str = "annual"
) -> ee.Image:
    annual = ee.Image.constant(0).float()
    valid_count = ee.Image.constant(0).float()
    for month in range(1, 13):
        month_band = monthly_stack.select(f"{prefix}_{month:02d}")
        days = calendar.monthrange(year, month)[1]
        annual = annual.add(month_band.unmask(0).multiply(days))
        valid_count = valid_count.add(month_band.mask().gt(0).unmask(0))
    return annual.updateMask(valid_count.gt(0)).rename(band_name).float()


def ee_annual_mean_band(
    monthly_stack: ee.Image, prefix: str, band_name: str = "annual"
) -> ee.Image:
    images = [
        monthly_stack.select(f"{prefix}_{month:02d}").rename("annual_src").float()
        for month in range(1, 13)
    ]
    return ee.ImageCollection.fromImages(images).mean().rename(band_name).float()


def _apply_image_properties(img: ee.Image, props: dict) -> ee.Image:
    out = img
    for key, value in props.items():
        out = out.set(key, value)
    return out


def finalize_export_image(
    monthly_stack: ee.Image,
    annual_band: ee.Image,
    region: ee.Geometry,
    metadata: dict,
    band_descriptions: list,
    default_proj: ee.Projection = None,
    common_mask: ee.Image = None,
) -> ee.Image:
    image = monthly_stack.addBands(annual_band).rename(EXPORT_BAND_NAMES)
    if default_proj is not None:
        image = image.setDefaultProjection(default_proj)
    if common_mask is not None:
        image = image.updateMask(common_mask)
    image = image.clip(region)
    image = image.unmask(NODATA).float()
    props = {"nodata": NODATA}
    props.update(metadata)
    for idx, desc in enumerate(band_descriptions, start=1):
        props[f"band_{idx}_description"] = desc
    return _apply_image_properties(image, props)


def _start_asset_export(image: ee.Image, asset_id: str, description: str):
    export_kwargs = {
        "image": image,
        "description": description,
        "assetId": asset_id,
        "scale": 30,
        "maxPixels": 1e13,
    }
    task = ee.batch.Export.image.toAsset(**export_kwargs)
    task.start()
    print(f"  Export task started -> {asset_id}")
    return task


def wait_for_tasks(
    task_specs: list, poll_seconds: int = 30, fail_on_error: bool = False
) -> dict:
    if not task_specs:
        return {}
    poll_seconds = max(5, int(poll_seconds))
    pending = {spec["asset_id"]: spec for spec in task_specs}
    final_statuses = {}
    print(f"\n[exports] Waiting for {len(task_specs)} Earth Engine task(s) ...")
    while pending:
        finished_now = []
        for asset_id, spec in pending.items():
            if not spec["task"]:
                finished_now.append(asset_id)
                final_statuses[asset_id] = "No Task"
                continue
            status = spec["task"].status()
            state = status.get("state", "UNKNOWN")
            if state in {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}:
                finished_now.append(asset_id)
                final_statuses[asset_id] = status
                print(f"  [{state}] {spec['label']} -> {asset_id}")
                if status.get("error_message"):
                    print(f"    Error: {status['error_message']}")
        for asset_id in finished_now:
            pending.pop(asset_id, None)
        if pending:
            print(
                f"  Still running: {len(pending)} task(s). Checking again in {poll_seconds}s ..."
            )
            time.sleep(poll_seconds)
    if fail_on_error:
        failed = []
        for spec in task_specs:
            if not spec["task"]:
                continue
            asset_id = spec["asset_id"]
            status = final_statuses.get(asset_id, {})
            state = status.get("state", "UNKNOWN")
            if state != "COMPLETED":
                message = status.get(
                    "error_message", "No error message from Earth Engine."
                )
                failed.append(f"{spec['label']} ({asset_id}) -> {state}: {message}")
        if failed:
            raise RuntimeError(
                "One or more Earth Engine export tasks did not complete successfully:\n"
                + "\n".join(failed)
            )
    return final_statuses


def _prepare_asset_target(asset_id: str, overwrite: bool) -> bool:
    if not _asset_exists(asset_id):
        return False
    if not overwrite:
        print(f"GEE asset already exists: {asset_id}\n")
        return True
        # raise RuntimeError(
        #     f"GEE asset already exists: {asset_id}\n"
        #     "Pass overwrite_assets=True to replace it."
        # )
    print(f"  Overwriting existing asset -> {asset_id}")
    ee.data.deleteAsset(asset_id)
    return True


def export_product_asset(
    label: str, display_name: str, image: ee.Image, cfg: dict
) -> dict:
    asset_id = _build_asset_id(cfg, label)
    asset_exists = _prepare_asset_target(
        asset_id, bool(cfg.get("overwrite_assets", False))
    )
    print(f"  {display_name} asset -> {asset_id}")
    task = None
    if not asset_exists:
        task = _start_asset_export(
            image,
            asset_id,
            description=f"export_{label}_{_asset_token(cfg['asset_suffix'])}_{cfg['year']}",
        )
    return {"asset_id": asset_id, "task": task, "label": label}


def build_classifier(model_path: str) -> ee.Classifier:
    trees = (
        ee.FeatureCollection(model_path)
        .aggregate_array("tree")
        .map(lambda s: ee.String(s).replace("#.*", "", "g").trim())
    )
    return ee.Classifier.decisionTreeEnsemble(trees)


def cast_monthly_band(img: ee.Image, value_band: str) -> ee.Image:
    """Force a one-band monthly image to a generic float schema."""
    return (
        ee.Image(img)
        .select([value_band])
        .cast({value_band: ee.PixelType.float()}, [value_band])
    )


def empty_monthly_band(value_band: str, proj: ee.Projection = None) -> ee.Image:
    """Return a fully masked one-band image that preserves monthly schema."""
    img = ee.Image.constant(0).rename(value_band).updateMask(ee.Image.constant(0))
    img = cast_monthly_band(img, value_band)
    if proj is not None:
        img = img.setDefaultProjection(proj)
    return img


def ensure_monthly_band(
    img: ee.Image, value_band: str, proj: ee.Projection = None
) -> ee.Image:
    """Keep value_band if present; otherwise replace with a masked placeholder."""
    img = ee.Image(img)
    empty = empty_monthly_band(value_band, proj)
    out = ee.Image(
        ee.Algorithms.If(
            img.bandNames().contains(value_band),
            cast_monthly_band(img.select(value_band).rename(value_band), value_band),
            empty,
        )
    )
    if proj is not None:
        out = out.setDefaultProjection(proj)
    return out


def fill_monthly_collection(
    raw_monthly: ee.ImageCollection,
    value_band: str,
    fallback_value=None,
    proj: ee.Projection = None,
) -> ee.ImageCollection:
    """Fill monthly gaps from neighbouring months within a +/-60 day window."""

    """Fill monthly gaps from neighbouring months within a +/-30 day window."""

    def normalize(img):
        img = ee.Image(img)
        out = ensure_monthly_band(img, value_band, proj)
        return (
            out.set("month", img.get("month"))
            .set("system:time_start", img.get("system:time_start"))
            .set("source_count", img.get("source_count"))
            .set("is_placeholder", img.get("is_placeholder"))
        )

    safe_monthly = raw_monthly.map(normalize)

    def interpolate(img):
        time_start = img.get("system:time_start")
        neighbours = safe_monthly.select(value_band).filterDate(
            ee.Date(time_start).advance(-30, "day"),
            ee.Date(time_start).advance(30, "day"),
        )
        filled = neighbours.mean()
        out = img.select(value_band).unmask(filled)
        if fallback_value is not None:
            out = out.unmask(fallback_value)
        return (
            cast_monthly_band(out.rename(value_band), value_band)
            .set("month", img.get("month"))
            .set("system:time_start", time_start)
        )

    return safe_monthly.map(interpolate)


def monthly_collection_to_stack(
    monthly_col: ee.ImageCollection,
    value_band: str,
    output_prefix: str,
    region: ee.Geometry,
) -> ee.Image:
    """Convert a monthly image collection into a named 12-band stack."""

    def rename_month(img):
        month_str = ee.String(ee.Number(img.get("month")).format("%02d"))
        return (
            img.select(value_band)
            .rename(ee.String(output_prefix).cat(month_str))
            .float()
        )

    named = monthly_col.map(rename_month)
    stack = named.toBands().clip(region)
    current_names = stack.bandNames()
    new_names = current_names.map(lambda n: ee.String(n).split("_").slice(1).join("_"))
    return stack.rename(new_names)


def get_proj_30m(region: ee.Geometry, year: int) -> ee.Projection:
    """Return the 30 m Landsat projection for this region/year."""
    ls_ref = (
        ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
        .filterBounds(region)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year + 1, 1, 1))
        .first()
    )
    return ls_ref.select("SR_B5").projection()


def monthly_valid_mask(monthly_stack: ee.Image, prefix: str) -> ee.Image:
    """Mask pixels that have at least one valid monthly value for a prefix."""
    valid = ee.Image.constant(0).float()
    for month in range(1, 13):
        valid = valid.max(
            monthly_stack.select(f"{prefix}_{month:02d}").mask().gt(0).unmask(0)
        )
    return valid.gt(0).selfMask()
