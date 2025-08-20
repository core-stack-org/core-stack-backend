import ee
from nrm_app.celery import app
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
from .utils import aez_lulcXterrain_cluster_centroids, process_mws, calculate_area


@app.task(bind=True)
def lulc_on_slope_cluster(self, state, district, block, start_year, end_year):
    ee_initialize()

    asset_description = (
        valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_lulcXslopes_clusters"
    )
    asset_id = get_gee_asset_path(state, district, block) + asset_description

    if not is_gee_asset_exists(asset_id):
        aez_india = ee.FeatureCollection("users/mtpictd/agro_eco_regions")

        landforms = ee.Image(
            get_gee_asset_path(state, district, block)
            + "terrain_raster_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )  # The eleven landforms raster

        mwsheds = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid"
        )

        filtered_aez = aez_india.filterBounds(mwsheds.geometry())

        aez_no = filtered_aez.first().get("ae_regcode").getInfo()
        print("aez_no=", aez_no)

        lulc_imgs = []
        for y in range(start_year, end_year + 1):
            lulc_img = ee.Image(
                get_gee_asset_path(state, district, block)
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
                + "_"
                + str(y)
                + "-07-01_"
                + str(y + 1)
                + "-06-30_LULCmap_10m"
            )
            lulc_imgs.append(lulc_img)

        lulc_img_collection = ee.ImageCollection.fromImages(lulc_imgs)
        study_area_lulc = lulc_img_collection.mode().clip(mwsheds)
        study_area_landforms = landforms.clip(mwsheds)

        mwsheds_with_clusters = process_mws(mwsheds)
        slope_mwsheds = mwsheds_with_clusters.filter(
            ee.Filter.Or(
                ee.Filter.eq("terrain_cluster", 0), ee.Filter.eq("terrain_cluster", 3)
            )
        )
        slope_centroids = aez_lulcXterrain_cluster_centroids[f"aez{aez_no}"]["slopes"]

        result = process_feature_collection(
            slope_mwsheds, study_area_landforms, study_area_lulc, slope_centroids
        )
        print("Processing completed successfully")
        task = export_vector_asset_to_gee(result, asset_description, asset_id)
        task_id_list = check_task_status([task])
        print("lulc_on_slope_cluster task completed - task_id_list:", task_id_list)

    if is_gee_asset_exists(asset_id):
        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_lulc_slope",
            asset_id=asset_id,
            dataset_name="Terrain LULC",
            misc={
                "start_year": start_year,
                "end_year": end_year,
            },
        )
        make_asset_public(asset_id)

        fc = ee.FeatureCollection(asset_id).getInfo()
        fc = {"features": fc["features"], "type": fc["type"]}
        res = sync_layer_to_geoserver(
            state,
            fc,
            valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_lulc_slope",
            "terrain_lulc",
        )
        print(res)
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("sync to geoserver flag updated")


def process_feature_collection(fc, landforms, area_lulc, slope_centroids):
    """
    Process an entire FeatureCollection by applying the L2 cluster assignment.
    """
    return fc.map(lambda f: assign_l2_cluster(f, landforms, area_lulc, slope_centroids))


def assign_l2_cluster(feature, landforms, area_lulc, slope_centroids):
    """
    Assigns L2 clusters to features based on landform and land use characteristics.
    """
    study_area = feature.geometry()
    lf300x2k = landforms.clip(study_area)

    # Get LULC data
    lulc = area_lulc.select("predicted_label")

    # # Convert 10 landforms to 4 general landforms
    # slopy = lf300x2k.eq(6)
    # plains = lf300x2k.eq(5).Or(lf300x2k.gte(12))
    # steep_slopes = lf300x2k.eq(8)
    # ridge = lf300x2k.eq(7).Or(lf300x2k.gte(9).And(lf300x2k.lte(11)))
    # valleys = lf300x2k.gte(1).And(lf300x2k.lte(4))

    # Convert 10 landforms to 4 general landforms
    slopy = lf300x2k.eq(6)
    plains = lf300x2k.eq(5)
    steep_slopes = lf300x2k.eq(8)
    # ridge = lf300x2k.gte(9).Or(lf300x2k.eq(7))
    # valleys = lf300x2k.gte(1).And(lf300x2k.lte(4))
    ridge = lf300x2k.eq(3).Or(lf300x2k.eq(7)).Or(lf300x2k.eq(10)).Or(lf300x2k.eq(11))
    valleys = lf300x2k.eq(1).Or(lf300x2k.eq(2)).Or(lf300x2k.eq(4)).Or(lf300x2k.eq(9))

    # Calculate areas
    plain_area = calculate_area(plains, study_area)
    valley_area = calculate_area(valleys, study_area)
    hill_slopes_area = calculate_area(steep_slopes, study_area)
    slopy_area = calculate_area(slopy, study_area)

    plain_plus_slope_area = plain_area.add(slopy_area)

    # Calculate LULC proportions
    def calculate_lulc_proportion(lulc_class):
        area_image = (
            slopy.eq(1)
            .And(lulc.eq(lulc_class))
            .multiply(ee.Image.pixelArea())
            .rename("area")
        )

        area = area_image.reduceRegion(
            reducer=ee.Reducer.sum(), geometry=study_area, scale=30, maxPixels=1e10
        )

        return ee.Number(area.get("area")).divide(1e6).divide(plain_plus_slope_area)

    # Calculate all proportions
    slopy_barren = calculate_lulc_proportion(7)  # Barren
    slopy_double = calculate_lulc_proportion(10)  # Double crop
    slopy_shrub_scrub = calculate_lulc_proportion(12)  # Shrubs/scrubs
    slopy_single_kharif = calculate_lulc_proportion(8)  # Single crop
    slopy_single_non_kharif = calculate_lulc_proportion(9)  # Single non-kharif
    slopy_forests = calculate_lulc_proportion(6)  # Forest
    slopy_triple = calculate_lulc_proportion(11)  # Triple crop

    # Create feature vector
    slope_new_feature_vector = ee.List(
        [
            slopy_barren,
            slopy_shrub_scrub,
            slopy_forests,
        ]
    )

    # Convert centroids to ee.List format
    centroid_vectors = [
        slope_centroids[str(i)]["cluster_vector"] for i in range(len(slope_centroids))
    ]
    ee_centroid_vectors = ee.List(centroid_vectors)

    # Calculate distances
    def diff_func(value_pair):
        return (
            ee.Number(ee.List(value_pair).get(0))
            .subtract(ee.Number(ee.List(value_pair).get(1)))
            .pow(2)
        )

    def calculate_distances(centroid):
        centroid_list = ee.List(centroid)
        paired_values = centroid_list.zip(slope_new_feature_vector)
        return paired_values.map(diff_func).reduce(ee.Reducer.sum())

    distances_slope = ee_centroid_vectors.map(calculate_distances)

    # Find closest cluster
    min_distance_slope = distances_slope.reduce(ee.Reducer.min())
    closest_cluster_index_slope = distances_slope.indexOf(min_distance_slope)

    # Create cluster names dictionary
    cluster_names = ee.Dictionary(
        {
            str(i): slope_centroids[str(i)]["cluster_name"]
            for i in range(len(slope_centroids))
        }
    )

    # Set cluster index and name
    return (
        feature.set("LxS_cluster", closest_cluster_index_slope)
        .set(
            "clust_name",
            cluster_names.get(closest_cluster_index_slope.format()),
        )
        .set("barren", slopy_barren.multiply(100))
        .set("double", slopy_double.multiply(100))
        .set("shrub_scrub", slopy_shrub_scrub.multiply(100))
        .set("sing_kharif", slopy_single_kharif.multiply(100))
        .set("sing_non_kharif", slopy_single_non_kharif.multiply(100))
        .set("forests", slopy_forests.multiply(100))
        .set("triple", slopy_triple.multiply(100))
    )
