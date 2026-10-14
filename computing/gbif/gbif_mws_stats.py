"""
Phase 4 — per-MWS biodiversity indicators, computed server-side in GEE.

Pattern: ee.Join.saveAll() joins each MWS polygon to the GBIF points inside it, preserving species
identity (taxonKey) — which reduceRegions() on a rasterized image would destroy. Then each MWS is
mapped to its indicator set with aggregate_count_distinct / aggregate_histogram.

Indicators computed here (server-side): species_richness, occurrence_count, shannon/simpson/pielou,
rare_species_count, threatened_species_count, per-class taxonomy counts, data_poor.
Derived indicators (dominant_class, biodiversity_category, observation_density) are cheap local
post-processing in Phase 5.
"""

import logging

import ee
from utilities.gee_utils import (
    get_gee_asset_path,
    valid_gee_text,
    export_vector_asset_to_gee,
    gdf_to_ee_fc,
    check_task_status,
    is_gee_asset_exists,
)

from . import config

logger = logging.getLogger(__name__)

# class/kingdom filters -> exported property name
_TAXON_FILTERS = [
    ("bird_species_count", "class", "Aves"),
    ("mammal_species_count", "class", "Mammalia"),
    ("reptile_species_count", "class", "Reptilia"),
    ("amphibian_species_count", "class", "Amphibia"),
    ("insect_species_count", "class", "Insecta"),
    ("plant_species_count", "kingdom", "Plantae"),
]

# Explicit, static output schema for the exported table (drops the join's List<Feature> and any
# other MWS properties). A static list is required so Export.table can resolve the schema — a
# dynamically-computed selector makes Earth Engine type the column as Feature and the export fails.
_OUTPUT_PROPERTIES = [
    "uid",
    "area_in_ha",
    "species_richness",
    "occurrence_count",
    "shannon_diversity_index",
    "simpson_diversity_index",
    "pielou_evenness",
    "rare_species_count",
    "threatened_species_count",
    "data_poor",
    "dominant_class",
    "biodiversity_category",
    "observation_density_per_km2",
] + [name for name, _, _ in _TAXON_FILTERS]

# Argmax order for dominant_class (matches the Python CLASS_MAP tie-break order).
_DOMINANT_ORDER = [
    ("Aves", "bird_species_count"),
    ("Mammalia", "mammal_species_count"),
    ("Plantae", "plant_species_count"),
    ("Reptilia", "reptile_species_count"),
    ("Amphibia", "amphibian_species_count"),
    ("Insecta", "insect_species_count"),
]


def _round3(x):
    """Round an ee.Number to 3 dp (float) — matches the previous Python round(...,3)."""
    return ee.Number(x).multiply(1000).round().divide(1000)


def load_mws_featurecollection(state, district, block):
    """MWS polygons for the block, straight from the GEE asset (same asset used everywhere)."""
    return ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )


def points_to_ee_fc(gdf, state, district, block):
    """
    Cleaned GBIF points (GeoDataFrame) -> ee.FeatureCollection. Returns (fc, part_asset_ids).

    Small blocks are sent inline via the shared gdf_to_ee_fc helper. Earth Engine caps a single
    request at 10 MB (~50k points), which an all-taxa urban block exceeds, so larger sets are exported
    to GEE as intermediate assets in chunks of config.POINTS_CHUNK_SIZE and read back as one merged
    FeatureCollection. Chunk exports are idempotent (an existing part asset is reused). The caller
    deletes the part assets once the per-MWS stats asset has been written (delete_points_assets).
    """
    n = len(gdf)
    if n <= config.POINTS_INLINE_LIMIT:
        return gdf_to_ee_fc(gdf), []

    part_ids, task_ids = [], []
    for i, start in enumerate(range(0, n, config.POINTS_CHUNK_SIZE)):
        part_id = config.get_gee_points_asset_id(state, district, block, i)
        part_ids.append(part_id)
        if is_gee_asset_exists(part_id):
            continue
        chunk = gdf.iloc[start : start + config.POINTS_CHUNK_SIZE]
        task_ids.append(
            export_vector_asset_to_gee(gdf_to_ee_fc(chunk), part_id.split("/")[-1], part_id)
        )
    logger.info(f"[gbif] {n} points -> {len(part_ids)} chunk assets ({len(task_ids)} new exports)")
    check_task_status(task_ids)

    missing = [p for p in part_ids if not is_gee_asset_exists(p)]
    if missing:
        raise RuntimeError(f"GBIF points chunk export failed for: {missing}")

    fc = ee.FeatureCollection(part_ids[0])
    for part_id in part_ids[1:]:
        fc = fc.merge(ee.FeatureCollection(part_id))
    return fc, part_ids


def delete_points_assets(part_ids):
    """Remove the intermediate points chunk assets (best effort; they are only a staging step)."""
    for part_id in part_ids:
        try:
            ee.data.deleteAsset(part_id)
        except Exception as exc:
            logger.warning(f"[gbif] could not delete intermediate asset {part_id}: {exc}")


def _diversity_from_histogram(occurrences, n):
    """Return (shannon, simpson, pielou, richness, rare_count) as ee.Numbers from one histogram."""
    hist = occurrences.aggregate_histogram("taxonKey")
    counts = ee.Dictionary(hist).values()
    total = counts.reduce(ee.Reducer.sum())
    richness = occurrences.aggregate_count_distinct("taxonKey")

    proportions = counts.map(
        lambda c: ee.Number(c).divide(total)
    )
    shannon = ee.Algorithms.If(
        n.gt(1),
        ee.Number(
            proportions.map(lambda p: ee.Number(p).multiply(ee.Number(p).log())).reduce(
                ee.Reducer.sum()
            )
        ).multiply(-1),
        ee.Number(0),
    )
    simpson = ee.Algorithms.If(
        n.gt(1),
        ee.Number(1).subtract(
            ee.Number(
                proportions.map(lambda p: ee.Number(p).pow(2)).reduce(ee.Reducer.sum())
            )
        ),
        ee.Number(0),
    )
    pielou = ee.Algorithms.If(
        ee.Number(richness).gt(1),
        ee.Number(shannon).divide(ee.Number(richness).log()),
        ee.Number(0),
    )
    rare = counts.map(lambda c: ee.Number(c).eq(1)).reduce(ee.Reducer.sum())
    return shannon, simpson, pielou, richness, rare


def compute_mws_biodiversity(gbif_fc, mws_fc):
    """Join GBIF points to MWS polygons and compute per-MWS indicators. Returns an FC."""
    spatial_filter = ee.Filter.intersects(
        leftField=".geo", rightValue=None, rightField=".geo", maxError=10
    )
    join = ee.Join.saveAll(matchesKey="gbif_occurrences")
    joined = join.apply(primary=mws_fc, secondary=gbif_fc, condition=spatial_filter)

    threatened = config.THREATENED_IUCN_CATEGORIES

    def compute_stats(feature):
        occ = ee.FeatureCollection(ee.List(feature.get("gbif_occurrences")))
        n = occ.size()
        shannon, simpson, pielou, richness, rare = _diversity_from_histogram(occ, n)

        threatened_count = occ.filter(
            ee.Filter.inList("iucnRedListCategory", threatened)
        ).aggregate_count_distinct("taxonKey")

        # per-class taxonomy counts
        tax = {
            name: ee.Number(
                occ.filter(ee.Filter.eq(field, value)).aggregate_count_distinct("taxonKey")
            ).toInt()
            for name, field, value in _TAXON_FILTERS
        }
        # dominant_class = argmax over the six class counts (Unknown if all zero) — server-side
        dom_names = ee.List([n0 for n0, _ in _DOMINANT_ORDER])
        dom_vals = ee.List([tax[c] for _, c in _DOMINANT_ORDER])
        dom_max = ee.Number(dom_vals.reduce(ee.Reducer.max()))
        dominant_class = ee.Algorithms.If(
            dom_max.gt(0), dom_names.get(dom_vals.indexOf(dom_max)), "Unknown"
        )
        # biodiversity_category = threshold band on richness
        r = ee.Number(richness)
        category = ee.Algorithms.If(
            r.lt(10), "Very Low",
            ee.Algorithms.If(r.lt(25), "Low",
            ee.Algorithms.If(r.lt(50), "Moderate",
            ee.Algorithms.If(r.lt(100), "High", "Very High"))),
        )
        # observation density per km2 (area is in ha); 0 if area unavailable
        area = ee.Number(feature.get("area_in_ha"))
        density = ee.Algorithms.If(
            area.gt(0),
            ee.Number(n).divide(area.divide(100)).multiply(100).round().divide(100),
            ee.Number(0),
        )
        # Explicit types so matched and zero-record features share ONE schema per property.
        props = {
            "uid": ee.String(feature.get("uid")),
            "area_in_ha": area.toFloat(),
            "species_richness": ee.Number(richness).toInt(),
            "occurrence_count": ee.Number(n).toInt(),
            "shannon_diversity_index": _round3(shannon),
            "simpson_diversity_index": _round3(simpson),
            "pielou_evenness": _round3(pielou),
            "rare_species_count": ee.Number(rare).toInt(),
            "threatened_species_count": ee.Number(threatened_count).toInt(),
            "data_poor": ee.Number(n.lt(config.MIN_RECORDS)).toInt(),
            "dominant_class": dominant_class,
            "biodiversity_category": category,
            "observation_density_per_km2": density,
        }
        props.update(tax)
        # Build a FRESH feature (geometry + scalar props only) to detach from the join schema.
        return ee.Feature(feature.geometry(), props)

    stats = joined.map(compute_stats)

    # MWS with zero contained points are dropped by the join — add them back as data_poor zeros.
    matched = stats.aggregate_array("uid")
    zeros = mws_fc.filter(ee.Filter.inList("uid", matched).Not()).map(
        lambda f: ee.Feature(
            f.geometry(),
            {
                "uid": ee.String(f.get("uid")),
                "area_in_ha": ee.Number(f.get("area_in_ha")).toFloat(),
                "species_richness": ee.Number(0).toInt(),
                "occurrence_count": ee.Number(0).toInt(),
                "shannon_diversity_index": _round3(0),
                "simpson_diversity_index": _round3(0),
                "pielou_evenness": _round3(0),
                "rare_species_count": ee.Number(0).toInt(),
                "threatened_species_count": ee.Number(0).toInt(),
                "data_poor": ee.Number(1).toInt(),
                "dominant_class": "Unknown",
                "biodiversity_category": "Very Low",
                "observation_density_per_km2": ee.Number(0),
                **{name: ee.Number(0).toInt() for name, _, _ in _TAXON_FILTERS},
            },
        )
    )
    # Project to the explicit output schema so column order/consistency is guaranteed.
    return stats.merge(zeros).select(_OUTPUT_PROPERTIES)


def export_stats_to_asset(state, district, block, stats_fc):
    """
    Persist the per-MWS stats FeatureCollection to a GEE asset via the shared helper — the same
    pattern as change_detection_vector. toAsset keeps ALL properties (no selectors), so nothing is
    truncated. Returns (task_id, asset_id).
    """
    asset_id = config.get_gee_block_asset_id(state, district, block)
    description = asset_id.split("/")[-1]
    task_id = export_vector_asset_to_gee(stats_fc, description, asset_id)
    logger.info(f"[gbif] stats export task {task_id} -> {asset_id}")
    return task_id, asset_id
