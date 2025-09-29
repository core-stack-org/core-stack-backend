import ee
from computing.utils import (
    sync_layer_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
)
from nrm_app.celery import app


@app.task(bind=True)
def generate_terrain_clusters(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)

    asset_name = (
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_terrain_clusters"
    )

    asset_id = get_gee_asset_path(state, district, block) + asset_name
    layer_id = None
    if not is_gee_asset_exists(asset_id):
        layer_id = compute_on_gee(state, district, block, asset_id, asset_name)

    layer_at_geoserver = sync_to_geoserver(state, district, block, asset_id, layer_id)
    return layer_at_geoserver


def compute_on_gee(state, district, block, asset_id, asset_name):
    dem = ee.Image("USGS/SRTMGL1_003")
    mt1k = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )

    def process_geometry(feature):
        # Get the geometry of the feature
        micro_watershed_id = feature.id()
        micro_watershed = mt1k.filter(ee.Filter.eq("system:index", micro_watershed_id))
        study_area = micro_watershed.geometry()
        dem_clipped = dem.clip(study_area)

        small_inner = ee.Number(5)
        small_outer = ee.Number(10)
        small_inner_circle = ee.Kernel.circle(small_inner, "pixels", False, -1)
        small_outer_circle = ee.Kernel.circle(small_outer, "pixels", False, 1)
        small_kernel = small_outer_circle.add(
            small_inner_circle, True
        )  # created annulus

        large_inner = ee.Number(62)
        large_outer = ee.Number(67)
        large_inner_circle = ee.Kernel.circle(large_inner, "pixels", False, -1)
        large_outer_circle = ee.Kernel.circle(large_outer, "pixels", False, 1)
        large_kernel = large_outer_circle.add(
            large_inner_circle, True
        )  # created annulus

        focal_mean_small = dem_clipped.reduceNeighborhood(
            ee.Reducer.mean(), small_kernel
        )
        focal_mean_large = dem_clipped.reduceNeighborhood(
            ee.Reducer.mean(), large_kernel
        )

        tpi_small = dem_clipped.subtract(focal_mean_small)
        tpi_large = dem_clipped.subtract(focal_mean_large)

        mean = tpi_small.reduceRegion(reducer=ee.Reducer.mean()).get("elevation")
        tpi_small = tpi_small.subtract(ee.Number(mean))
        std_dev = tpi_small.reduceRegion(reducer=ee.Reducer.stdDev()).get("elevation")
        tpi_small = (
            tpi_small.divide(ee.Number(std_dev))
            .multiply(ee.Number(100))
            .add(ee.Number(0.5))
        )

        mean = tpi_large.reduceRegion(reducer=ee.Reducer.mean()).get("elevation")
        tpi_large = tpi_large.subtract(ee.Number(mean))

        std_dev = tpi_large.reduceRegion(reducer=ee.Reducer.stdDev()).get("elevation")

        tpi_large = (
            tpi_large.divide(ee.Number(std_dev))
            .multiply(ee.Number(100))
            .add(ee.Number(0.5))
        )

        combined_image = tpi_small.addBands(tpi_large)

        std_dev = tpi_large.reduceRegion(reducer=ee.Reducer.stdDev()).get("elevation")

        slope = ee.Terrain.slope(dem)
        clipped_slope = slope.clip(study_area)

        # ------------------------------------------------------------------------
        # Classification

        lf300x2k = ee.Image.constant(0).clip(study_area)
        dem_std = dem_clipped.reduceRegion(reducer=ee.Reducer.stdDev()).get("elevation")
        dem_std = ee.Number(dem_std).add(1)
        dem_mean = dem_clipped.reduceRegion(reducer=ee.Reducer.mean()).get("elevation")

        factor = ee.Number(3).subtract(ee.Number(dem_std).log10())

        right_limit = ee.Number(100).multiply(factor)
        left_limit = ee.Number(-100).multiply(factor)

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(clipped_slope.lt(5)),
            5,
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(clipped_slope.gte(5))
            .And(clipped_slope.lt(20)),
            6,
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gte(right_limit))
            .And(clipped_slope.lt(6)),
            7,  # Flat Ridge Tops
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit))
            .And(clipped_slope.gte(20)),
            8,  # Upper Slopes
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.gte(right_limit))
            .And(clipped_slope.gte(6)),
            8,  # Upper Slopes
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gt(left_limit)
            .And(tpi_small.lt(right_limit))
            .And(tpi_large.lte(left_limit)),
            4,
        )

        lf300x2k = lf300x2k.where(
            tpi_small.lte(left_limit)
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit)),
            2,
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gte(right_limit)
            .And(tpi_large.gt(left_limit))
            .And(tpi_large.lt(right_limit)),
            10,
        )

        lf300x2k = lf300x2k.where(
            tpi_small.lte(left_limit).And(tpi_large.gte(right_limit)), 3
        )

        lf300x2k = lf300x2k.where(
            tpi_small.lte(left_limit).And(tpi_large.lte(left_limit)), 1
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gte(right_limit).And(tpi_large.gte(right_limit)), 11
        )

        lf300x2k = lf300x2k.where(
            tpi_small.gte(right_limit).And(tpi_large.lte(left_limit)), 9
        )

        study_area = lf300x2k.select("constant")

        # lulc = area_lulc.select('class')
        dem_clipped = dem.clip(study_area.geometry())

        # 10 landforms to 5 general landforms

        # slopy = lf300x2k.eq(6)
        # plains = lf300x2k.eq(5)
        # steep_slopes = lf300x2k.eq(8)
        # ridge = lf300x2k.gte(9).Or(lf300x2k.eq(7))
        # valleys = lf300x2k.gte(1).And(lf300x2k.lte(4))

        slopy = lf300x2k.eq(6)
        plains = lf300x2k.eq(5)
        steep_slopes = lf300x2k.eq(8)
        ridge = (
            lf300x2k.eq(3).Or(lf300x2k.eq(7)).Or(lf300x2k.eq(10)).Or(lf300x2k.eq(11))
        )
        valleys = (
            lf300x2k.eq(1).Or(lf300x2k.eq(2)).Or(lf300x2k.eq(4)).Or(lf300x2k.eq(9))
        )

        mwshed_area = ee.Number(
            study_area.neq(0)
            .multiply(ee.Image.pixelArea())
            .reduceRegion(
                reducer=ee.Reducer.sum(),
                geometry=study_area.geometry(),
                scale=30,
                maxPixels=1e10,
            )
            .get("constant")
        ).divide(1e6)

        plain_area = (
            ee.Number(
                (plains.eq(1))
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=study_area.geometry(),
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )

        feature = feature.set("plain_area", plain_area.multiply(100))

        valley_area = (
            ee.Number(
                (valleys.eq(1))
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=study_area.geometry(),
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )
        feature = feature.set("valley_area", valley_area.multiply(100))

        hill_slopes_area = (
            ee.Number(
                (steep_slopes.eq(1))
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=study_area.geometry(),
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )
        feature = feature.set("hill_slopes_area", hill_slopes_area.multiply(100))

        ridge_area = (
            ee.Number(
                (ridge.eq(1))
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=study_area.geometry(),
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )
        feature = feature.set("ridge_area", ridge_area.multiply(100))

        slopy_area = (
            ee.Number(
                (slopy.eq(1))
                .multiply(ee.Image.pixelArea())
                .reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=study_area.geometry(),
                    scale=30,
                    maxPixels=1e10,
                )
                .get("constant")
            )
            .divide(1e6)
            .divide(mwshed_area)
        )
        feature = feature.set("slopy_area", slopy_area.multiply(100))

        centroids = [
            [0.36255426, 0.21039965, 0.12161905, 0.17393119, 0.13149585],
            [0.09171062, 0.84299211, 0.035222, 0.02172654, 0.00834873],
            [0.08497599, 0.01051893, 0.23763531, 0.37992855, 0.28694122],
            [0.22301813, 0.5611825, 0.08511123, 0.07314189, 0.05754624],
        ]

        # Example: Classify a new feature vector
        new_feature_vector = ee.List(
            [slopy_area, plain_area, ridge_area, valley_area, hill_slopes_area]
        )

        def dif_func(c_list):
            return (
                ee.Number(ee.List(c_list).get(0))
                .subtract(ee.Number(ee.List(c_list).get(1)))
                .pow(2)
            )

        def sum_square_func(feat):
            clist = ee.List(feat).zip(new_feature_vector)
            sum_square = clist.map(dif_func).reduce(ee.Reducer.sum())
            return sum_square

        distances = ee.List(centroids).map(sum_square_func)

        # print(distances)
        # **5. Assign to Cluster**
        min_distance = ee.List(distances).reduce(ee.Reducer.min())
        closest_cluster_index = ee.List(distances).indexOf(ee.Number(min_distance))

        feature = feature.set("terrainClusters", closest_cluster_index)
        return feature

    fc = mt1k.map(process_geometry)
    # Export an ee.FeatureCollection as an Earth Engine asset.
    task = export_vector_asset_to_gee(fc, asset_name, asset_id)
    check_task_status([task])

    layer_id = None
    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_cluster",
            asset_id=asset_id,
            dataset_name="Terrain Vector",
        )
        make_asset_public(asset_id)
    return layer_id


def sync_to_geoserver(state, district, block, asset_id, layer_id):
    fc = ee.FeatureCollection(asset_id).getInfo()
    fc = {"features": fc["features"], "type": fc["type"]}
    res = sync_layer_to_geoserver(
        state,
        fc,
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_cluster",
        "terrain",
    )
    print(res)
    layer_at_geoserver = False
    if res["status_code"] == 201 and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
        print("sync to geoserver flag is updated")
        layer_at_geoserver = True
    return layer_at_geoserver
