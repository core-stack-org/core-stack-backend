import ee
from nrm_app.celery import app

from utilities.constants import GEE_DATASET_PATH
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    make_asset_public,
    export_vector_asset_to_gee,
    # earthdata_auth,
    # gdf_to_ee_fc,
    # upload_shp_to_gee,
)

# import zipfile
# import pandas as pd
# from osgeo import gdal
# from shapely.geometry import box
# from pcraster import *
# import os
from computing.utils import save_layer_info_to_db


@app.task(bind=True)
def mws_layer(self, state, district, block, gee_account_id):
    ee_initialize(gee_account_id)
    description = (
        "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    if not is_gee_asset_exists(asset_id):
        mwses_uid_fc = ee.FeatureCollection(
            GEE_DATASET_PATH + "/hydrological_boundaries/microwatershed"
        )

        admin_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "admin_boundary_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        filtered_mws_block_uid = mwses_uid_fc.filterBounds(admin_boundary.geometry())

        task_id = export_vector_asset_to_gee(
            filtered_mws_block_uid, description, asset_id
        )
        mws_task_id_list = check_task_status([task_id])
        print("mws_task_id_list", mws_task_id_list)
    layer_generated = False
    if is_gee_asset_exists(asset_id):
        make_asset_public(asset_id)
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"mws_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            asset_id=asset_id,
            dataset_name="MWS",
            algorithm_version="1.2",
        )
        layer_generated = True
    return layer_generated


# @app.task(bind=True)
# def mws_layer(self, state, district, block):
#     ee_initialize()
#     description = (
#         "filtered_mws_"
#         + valid_gee_text(district.lower())
#         + "_"
#         + valid_gee_text(block.lower())
#         + "_uid"
#     )
#     asset_id = get_gee_asset_path(state, district, block) + description
#     if not is_gee_asset_exists(asset_id):
#         mwses_uid_fc = ee.FeatureCollection(GEE_DATASET_PATH + "/India_mws_UID_Merged")
#
#         admin_boundary = ee.FeatureCollection(
#             get_gee_asset_path(state, district, block)
#             + "admin_boundary_"
#             + valid_gee_text(district.lower())
#             + "_"
#             + valid_gee_text(block.lower())
#         )
#         filtered_mws_block_uid = mwses_uid_fc.filterBounds(admin_boundary.geometry())
#
#         def area_func(feature):
#             area_in_sqm = feature.area()
#             area_in_hec = ee.Number(area_in_sqm).divide(ee.Number(10000))
#             return feature.set("area_in_ha", area_in_hec)
#
#         filtered_mws_block_uid = filtered_mws_block_uid.map(area_func)
#         layer_path, is_heavy_data = merge_small_mwses(
#             filtered_mws_block_uid.getInfo(), state, district, block
#         )
#         if is_heavy_data:
#             export_shp_to_gee(district, block, layer_path, asset_id)
#             if is_gee_asset_exists(asset_id):
#                 save_layer_info_to_db(
#                     state,
#                     district,
#                     block,
#                     layer_name="",
#                     asset_id=asset_id,
#                     dataset_name="mws_only",
#                 )
#                 print("save mws layer info at the gee level...")
#         else:
#             gdf = gpd.read_file(layer_path + ".geojson")
#             gdf = gdf.to_crs("EPSG:4326")
#             fc = gdf_to_ee_fc(gdf)
#             # Export feature collection to GEE
#             task_id = export_vector_asset_to_gee(fc, description, asset_id)
#             mws_task_id_list = check_task_status([task_id])
#             print("mws_task_id_list", mws_task_id_list)
#             if is_gee_asset_exists(asset_id):
#                 save_layer_info_to_db(
#                     state,
#                     district,
#                     block,
#                     layer_name="",
#                     asset_id=asset_id,
#                     dataset_name="mws_only",
#                 )
#                 print("save mws layer info at the gee level...")
#                 make_asset_public(asset_id)
#
#
# def export_shp_to_gee(district, block, layer_path, asset_id):
#     layer_name = (
#         "mws_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
#     )
#     layer_path = os.path.splitext(layer_path)[0] + "/" + layer_path.split("/")[-1]
#     upload_shp_to_gee(layer_path, layer_name, asset_id)
#
#
# def merge_small_mwses(gdf_mwses, state, district, block):
#     state = state.split()
#     if len(state) == 1:
#         state = state[0].lower()
#     else:
#         first_word = state[0].lower()
#         remaining_words = "".join(word.capitalize() for word in state[1:])
#         state = first_word + remaining_words
#     block = block.split(" ")[0]
#     district = district.split(" ")[0]
#
#     mws_path = os.path.join(MERGE_MWS_PATH, state)
#     if not os.path.exists(mws_path):
#         os.mkdir(mws_path)
#
#     mws_path = os.path.join(str(mws_path), district.lower() + "_" + block.lower())
#     if not os.path.exists(mws_path):
#         os.mkdir(mws_path)
#
#     gdf_mwses = gpd.GeoDataFrame.from_features(gdf_mwses).reset_index()
#     gdf_mwses.rename(columns={"index": "id"}, inplace=True)
#
#     gdf_lt_500 = gdf_mwses[gdf_mwses["area_in_ha"] < 500]
#     gdf_lt_500.plot()
#
#     srtm30_bbs = gpd.read_file(MERGE_MWS_PATH + "/srtm30m_bounding_boxes.geojson")
#
#     # convert to polygon
#     geom = box(*gdf_lt_500.total_bounds)
#     srtm30_bbs["intersects"] = srtm30_bbs.intersects(geom)
#     rslt_df = srtm30_bbs[srtm30_bbs["intersects"] == True]
#     hgts = list(rslt_df["dataFile"])
#
#     d = {"col1": ["total_bounds"], "geometry": [geom]}
#     gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")
#     gdf.to_file(mws_path + "/geom.json", driver="GeoJSON")
#     input_files_path = []
#     for hgt in hgts:
#         filename = earthdata_auth(hgt, mws_path)
#         with zipfile.ZipFile(filename, "r") as zip_ref:
#             zip_ref.extractall(mws_path)
#
#         input_files_path.append(mws_path + "/" + zip_ref.namelist()[0])
#
#     output_file_path = (
#         mws_path + "/dem-merged-" + district.lower() + "_" + block.lower() + ".tif"
#     )
#
#     cmd = f"gdal_merge.py -of GTiff -o {output_file_path} {input_files_path[0]}"  # TODO Change for multiple input files
#     os.system(command=cmd)
#
#     # !gdalwarp -t_srs EPSG:4326 dem-merged-angul.tif dem-merged-angul_4326.tif
#     merged_4326 = output_file_path.split(".")[0] + "_4326.tif"
#
#     cmd = f"gdalwarp -t_srs EPSG:4326 {output_file_path} {merged_4326}"
#     os.system(command=cmd)
#
#     # !gdalwarp -cutline geom.json -crop_to_cutline dem-merged-angul_4326.tif cropped.tif
#     geom = mws_path + "/geom.json"
#     cropped_tif = mws_path + "/cropped.tif"
#     cmd = f"gdalwarp -cutline {geom} -crop_to_cutline {merged_4326} {cropped_tif}"
#     os.system(command=cmd)
#
#     # Open the file:
#     raster = gdal.Open(cropped_tif)
#
#     # !gdal_translate -of PCRaster -ot Float32 -mo "VS_SCALAR" cropped.tif cropped.map
#     cmd = f"gdal_translate -of PCRaster -ot Float32 -mo VS_SCALAR {cropped_tif} {mws_path}/cropped.map"
#     os.system(command=cmd)
#     setclone(mws_path + "/cropped.map")
#     ldd = lddcreate(mws_path + "/cropped.map", 9999999, 9999999, 9999999, 9999999)
#     report(ldd, mws_path + "/ldd-cropped.map")
#
#     sptl = spatial(scalar(1.0))
#     report(sptl, mws_path + "/spatial_cropped.map")
#
#     Resultflux = accuflux(
#         mws_path + "/ldd-cropped.map", mws_path + "/spatial_cropped.map"
#     )
#     report(Resultflux, mws_path + "/Resultflux_cropped.map")
#
#     #  !gdal_translate -of xyz -co ADD_HEADER_LINE=YES -co COLUMN_SEPARATOR="," Resultflux_cropped.map Resultflux_cropped.csv
#     cmd = f"gdal_translate -of xyz -co ADD_HEADER_LINE=YES -co COLUMN_SEPARATOR=',' {mws_path}/Resultflux_cropped.map {mws_path}/Resultflux_cropped.csv"
#     os.system(command=cmd)
#
#     df = pd.read_csv(mws_path + "/Resultflux_cropped.csv")
#
#     gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.X, df.Y), crs="epsg:4326")
#
#     gdf.to_file(mws_path + "/Resultflux_cropped.geojson", driver="GeoJSON")
#
#     overlaid = gpd.overlay(gdf, gdf_lt_500, how="intersection")
#     overlaid.plot()
#     overlaid.to_file(mws_path + "/overlaid_frompcraster.geojson", driver="GeoJSON")
#
#     gdf_flux = gpd.read_file(mws_path + "/overlaid_frompcraster.geojson")
#
#     gdf_intersect = gdf_flux
#
#     def get_max_score(group):
#         return group.loc[group["Z"].idxmax()]
#
#     gdf_maxFlux = gdf_intersect.groupby("id").apply(get_max_score)
#
#     gdf_maxFlux = gdf_maxFlux.reset_index(drop=True)
#     gdf_maxFlux.plot()
#     gdf_maxFlux.to_file(mws_path + "/maxFlux_for_each_mws.geojson", driver="GeoJSON")
#
#     gdf_gt_500 = gdf_mwses[gdf_mwses["area_in_ha"] > 500]
#     gdf_maxFlux.set_crs(epsg=4326, inplace=True)
#
#     nearest_mws_id = []
#     for index, row in gdf_maxFlux.iterrows():
#         max_flux_point = row["geometry"]
#         nearest_mws_index = gdf_gt_500.distance(max_flux_point).sort_values().index[0]
#         nearest_mws_id.append(gdf_gt_500.loc[nearest_mws_index]["id"])
#
#     gdf_maxFlux["nearest_mws_id"] = nearest_mws_id
#     gdf_maxFlux.to_file(
#         mws_path + "/maxFlux_for_each_mws_with_nearest_mwsId.geojson", driver="GeoJSON"
#     )
#
#     gdf_max_flux_geo_dropped = gdf_maxFlux.drop("geometry", axis=1)
#
#     gdf_join = pd.merge(gdf_lt_500, gdf_max_flux_geo_dropped, on="id")
#
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id = gdf_join.dissolve(by="nearest_mws_id")
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id = (
#         gdf_mwses_lt_500_dissolve_by_nearest_mws_id.reset_index()
#     )
#
#     # gdf_mwseslt500_dissolveBy_nearestMwsId.drop(['id', 'DN_x', 'area_in_ha_x', 'ID', 'X',
#     #        'Y', 'Resultfluxl', 'DN_y', 'area_in_ha_y'], axis=1, inplace=True)
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id.drop(
#         ["id", "DN_x", "area_in_ha_x", "X", "Y", "Z", "DN_y", "area_in_ha_y"],
#         axis=1,
#         inplace=True,
#     )
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id.rename(
#         columns={"nearest_mws_id": "id"}, inplace=True
#     )
#
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id.plot()
#
#     gdf_mwses_lt_500_dissolve_by_nearest_mws_id.to_file(
#         mws_path + "/mwseslt500_dissolveBy_nearestMwsId.geojson", driver="GeoJSON"
#     )
#
#     gdf_dissolve_inner = gdf_gt_500.merge(
#         gdf_mwses_lt_500_dissolve_by_nearest_mws_id, on="id", how="inner"
#     )
#
#     x = gpd.GeoSeries(gdf_dissolve_inner["geometry_x"])
#     y = gpd.GeoSeries(gdf_dissolve_inner["geometry_y"])
#
#     gdf_dissolve_inner["geometry"] = x.union(y, align=True)
#
#     gdf_dissolve_inner = gdf_dissolve_inner.set_geometry("geometry")
#     gdf_dissolve_inner.plot()
#
#     gdf_dissolve_left = gdf_gt_500.merge(
#         gdf_mwses_lt_500_dissolve_by_nearest_mws_id, on="id", how="left"
#     )
#
#     gdf_dissolve_left = gdf_dissolve_left.drop(
#         gdf_dissolve_left[gdf_dissolve_left["geometry_y"] != None].index
#     )
#
#     gdf_dissolve_left.drop(["DN", "area_in_ha", "geometry_y"], axis=1, inplace=True)
#     gdf_dissolve_left.rename(columns={"geometry_x": "geometry"}, inplace=True)
#
#     rdf = gpd.GeoDataFrame(
#         pd.concat([gdf_dissolve_inner, gdf_dissolve_left], ignore_index=True)
#     )
#     rdf.crs = "epsg:4326"
#     rdf = rdf.to_crs({"init": "epsg:3857"})
#
#     rdf["area_in_ha"] = rdf["geometry"].area / 10**4
#
#     rdf.plot()
#
#     # filtered = rdf.loc[rdf["area_in_ha"] < 500]
#     # print("filtered.shape", filtered.shape)
#
#     rdf.drop(["DN", "geometry_x", "geometry_y", "uid_x", "uid_y"], axis=1, inplace=True)
#     rdf = rdf.to_crs(3857)
#     layer_path = mws_path + "/rdf_revised_pcraster"
#     rdf.to_file(layer_path + ".geojson", driver="GeoJSON")
#
#     file_size_bytes = os.path.getsize(layer_path + ".geojson")
#     file_size_mb = file_size_bytes / (1024 * 1024)
#
#     print(f"File size: {file_size_bytes} bytes ({file_size_mb:.2f} MB)")
#     is_heavy_data = False
#     if file_size_mb > 10:
#         is_heavy_data = True
#         rdf.to_file(layer_path, driver="ESRI Shapefile", encoding="UTF-8")
#     return layer_path, is_heavy_data
