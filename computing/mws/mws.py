import ee
import geopandas as gpd
from nrm_app.celery import app
from utilities.constants import MERGE_MWS_PATH
from .precipitation import precipitation
from .run_off_chunks import run_off
from .evapotranspiration_chunk import evapotranspiration
from .delta_g import delta_g
from .net_value import net_value
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    earthdata_auth,
    gdf_to_ee_fc,
    get_gee_asset_path,
    is_gee_asset_exists,
    make_asset_public,
)
import zipfile
from .well_depth import well_depth
from .calculateG import calculate_g
import pandas as pd
from osgeo import gdal
from shapely.geometry import box
from pcraster import *
import os
import sys
from computing.views import create_dataset_for_generated_layer


@app.task(bind=True)
def mws_layer(self, state, district, block, start_year, end_year, is_annual, user):
    ee_initialize()

    sys.setrecursionlimit(6000)

    generate_mws_layer(state, district, block)

    task_list = []
    start_date = f"{start_year}-07-01"
    end_date = f"{end_year}-06-30"
    ppt_task_id = precipitation(state, district, block, start_date, end_date, is_annual)
    if ppt_task_id:
        task_list.append(ppt_task_id)

    et_task_id = evapotranspiration(
        state, district, block, start_year, end_year, is_annual
    )
    if et_task_id:
        task_list.append(et_task_id)

    ro_task_id = run_off(state, district, block, start_date, end_date, is_annual)
    if ro_task_id:
        task_list.append(ro_task_id)

    task_id_list = check_task_status(task_list) if len(task_list) > 0 else []
    print("task_id_list", task_id_list)

    dg_task_id, asset_id = delta_g(
        state, district, block, start_date, end_date, is_annual
    )
    task_id_list = check_task_status([dg_task_id]) if dg_task_id else []
    print("dg task_id_list", task_id_list)

    layer_name = (
        "deltaG_fortnight_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    if is_annual:
        wd_task_id = well_depth(state, district, block, start_date, end_date)
        task_id_list = check_task_status([wd_task_id]) if wd_task_id else []
        print("wd task_id_list", task_id_list)

        wd_task_id, asset_id = net_value(state, district, block, start_date, end_date)
        task_id_list = check_task_status([wd_task_id]) if wd_task_id else []
        print("wdn task_id_list", task_id_list)

        layer_name = (
            "deltaG_well_depth_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )

        # Generated Dataset data to db 
        prec_annual = get_gee_asset_path(state, district, block) + "Prec_annual_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        et_annual = get_gee_asset_path(state, district, block) + "ET_annual_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        runoff_annual = get_gee_asset_path(state, district, block) + "Runoff_annual_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        well_depth_annual = get_gee_asset_path(state, district, block) + "well_depth_annual_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        filtered_delta_g_annual = get_gee_asset_path(state, district, block) + "filtered_delta_g_annual_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()) + "_uid"
        gee_path = {"prec_annual":prec_annual, "et_annual":et_annual, "runoff_annual":runoff_annual, "well_depth_annual":well_depth_annual, "filtered_delta_g_annual":filtered_delta_g_annual}
        try:
            create_dataset_for_generated_layer(state, district, block, layer_name, user, gee_path=gee_path, layer_type='vector', workspace='mws_layers', algorithm=None, version=None, style_name=None, misc=None)
            print("Dataset entry created for Mws well depth Annual")
        except Exception as e:
            print(f"Exception while creating entry for Mws well depth Annual in dataset table: {str(e)}")
        

    calculate_g(state, start_date, end_date, asset_id, layer_name, is_annual)
    # Generated Dataset data to db 
    filter_mws = get_gee_asset_path(state, district, block) + "filtered_mws_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()) + "_uid"
    prec_fortnight = get_gee_asset_path(state, district, block) + "Prec_fortnight_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    et_fortnight = get_gee_asset_path(state, district, block) + "ET_fortnight_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    runoff_fortnight = get_gee_asset_path(state, district, block) + "Runoff_fortnight_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    filtered_delta_g_fortnight = get_gee_asset_path(state, district, block) + "filtered_delta_g_fortnight_" + valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower()) + "_uid"
    gee_path = {"filter_mws":filter_mws, "prec_fortnight":prec_fortnight, "et_fortnight":et_fortnight, "runoff_fortnight":runoff_fortnight, "filtered_delta_g_fortnight":filtered_delta_g_fortnight}
    try:
        create_dataset_for_generated_layer(state, district, block, layer_name, user, gee_path=gee_path, layer_type='vector', workspace='mws_layers', algorithm=None, version=None, style_name=None, misc=None)
        print("Dataset entry created for Mws Fortnight")
    except Exception as e:
        print(f"Exception while creating entry for Mws Fortnight in dataset table: {str(e)}")
        

def generate_mws_layer(state, district, block):
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
            "projects/ee-dharmisha-siddharth/assets/India_mws_UID_Merged"
        )

        admin_boundary = ee.FeatureCollection(
            get_gee_asset_path(state, district, block)
            + "admin_boundary_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        filtered_mws_block_uid = mwses_uid_fc.filterBounds(admin_boundary.geometry())

        def area_func(feature):
            area_in_sqm = feature.area()
            area_in_hec = ee.Number(area_in_sqm).divide(ee.Number(10000))
            return feature.set("area_in_ha", area_in_hec)

        filtered_mws_block_uid = filtered_mws_block_uid.map(area_func)
        layer_path = merge_small_mwses(
            filtered_mws_block_uid.getInfo(), state, district, block
        )

        gdf = gpd.read_file(layer_path)
        gdf = gdf.to_crs("EPSG:4326")
        fc = gdf_to_ee_fc(gdf)

        try:
            task = ee.batch.Export.table.toAsset(
                **{
                    "collection": fc,
                    "description": description,
                    "assetId": asset_id,
                }
            )
            task.start()
            print("Successfully started the mws_task", task.status())
            # return task.status()["id"]
            mws_task_id_list = check_task_status([task.status()["id"]])
            print("mws_task_id_list", mws_task_id_list)
        except Exception as e:
            print(f"Error occurred in running mws_task: {e}")

    make_asset_public(asset_id)


def merge_small_mwses(gdf_mwses, state, district, block):
    state = state.split()
    if len(state) == 1:
        state = state[0].lower()
    else:
        first_word = state[0].lower()
        remaining_words = "".join(word.capitalize() for word in state[1:])
        state = first_word + remaining_words
    block = block.split(" ")[0]
    district = district.split(" ")[0]

    mws_path = os.path.join(MERGE_MWS_PATH, state)
    if not os.path.exists(mws_path):
        os.mkdir(mws_path)

    mws_path = os.path.join(str(mws_path), district.lower() + "_" + block.lower())
    if not os.path.exists(mws_path):
        os.mkdir(mws_path)

    gdf_mwses = gpd.GeoDataFrame.from_features(gdf_mwses).reset_index()
    gdf_mwses.rename(columns={"index": "id"}, inplace=True)

    gdf_lt_500 = gdf_mwses[gdf_mwses["area_in_ha"] < 500]
    gdf_lt_500.plot()

    srtm30_bbs = gpd.read_file(MERGE_MWS_PATH + "/srtm30m_bounding_boxes.geojson")

    # total_bounds = gdf_lt_500.total_bounds
    # minx, miny, maxx, maxy = (
    #     total_bounds[0],
    #     total_bounds[1],
    #     total_bounds[2],
    #     total_bounds[3],
    # )

    # convert to polygon
    geom = box(*gdf_lt_500.total_bounds)
    srtm30_bbs["intersects"] = srtm30_bbs.intersects(geom)
    rslt_df = srtm30_bbs[srtm30_bbs["intersects"] == True]
    hgts = list(rslt_df["dataFile"])

    d = {"col1": ["total_bounds"], "geometry": [geom]}
    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")
    gdf.to_file(mws_path + "/geom.json", driver="GeoJSON")
    input_files_path = []
    for hgt in hgts:
        filename = earthdata_auth(hgt, mws_path)
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall(mws_path)

        input_files_path.append(mws_path + "/" + zip_ref.namelist()[0])

    output_file_path = (
        mws_path + "/dem-merged-" + district.lower() + "_" + block.lower() + ".tif"
    )

    cmd = f"gdal_merge.py -of GTiff -o {output_file_path} {input_files_path[0]}"  # TODO Change for multiple input files
    os.system(command=cmd)

    # !gdalwarp -t_srs EPSG:4326 dem-merged-angul.tif dem-merged-angul_4326.tif
    merged_4326 = output_file_path.split(".")[0] + "_4326.tif"

    cmd = f"gdalwarp -t_srs EPSG:4326 {output_file_path} {merged_4326}"
    os.system(command=cmd)

    # !gdalwarp -cutline geom.json -crop_to_cutline dem-merged-angul_4326.tif cropped.tif
    geom = mws_path + "/geom.json"
    cropped_tif = mws_path + "/cropped.tif"
    cmd = f"gdalwarp -cutline {geom} -crop_to_cutline {merged_4326} {cropped_tif}"
    os.system(command=cmd)

    # Open the file:
    raster = gdal.Open(cropped_tif)

    # !gdal_translate -of PCRaster -ot Float32 -mo "VS_SCALAR" cropped.tif cropped.map
    cmd = f"gdal_translate -of PCRaster -ot Float32 -mo VS_SCALAR {cropped_tif} {mws_path}/cropped.map"
    os.system(command=cmd)
    setclone(mws_path + "/cropped.map")
    ldd = lddcreate(mws_path + "/cropped.map", 9999999, 9999999, 9999999, 9999999)
    report(ldd, mws_path + "/ldd-cropped.map")

    sptl = spatial(scalar(1.0))
    report(sptl, mws_path + "/spatial_cropped.map")

    Resultflux = accuflux(
        mws_path + "/ldd-cropped.map", mws_path + "/spatial_cropped.map"
    )
    report(Resultflux, mws_path + "/Resultflux_cropped.map")

    #  !gdal_translate -of xyz -co ADD_HEADER_LINE=YES -co COLUMN_SEPARATOR="," Resultflux_cropped.map Resultflux_cropped.csv
    cmd = f"gdal_translate -of xyz -co ADD_HEADER_LINE=YES -co COLUMN_SEPARATOR=',' {mws_path}/Resultflux_cropped.map {mws_path}/Resultflux_cropped.csv"
    os.system(command=cmd)

    df = pd.read_csv(mws_path + "/Resultflux_cropped.csv")

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.X, df.Y), crs="epsg:4326")

    gdf.to_file(mws_path + "/Resultflux_cropped.geojson", driver="GeoJSON")

    overlaid = gpd.overlay(gdf, gdf_lt_500, how="intersection")
    overlaid.plot()
    overlaid.to_file(mws_path + "/overlaid_frompcraster.geojson", driver="GeoJSON")

    gdf_flux = gpd.read_file(mws_path + "/overlaid_frompcraster.geojson")

    gdf_intersect = gdf_flux

    def get_max_score(group):
        return group.loc[group["Z"].idxmax()]

    gdf_maxFlux = gdf_intersect.groupby("id").apply(get_max_score)

    gdf_maxFlux = gdf_maxFlux.reset_index(drop=True)
    gdf_maxFlux.plot()
    gdf_maxFlux.to_file(mws_path + "/maxFlux_for_each_mws.geojson", driver="GeoJSON")

    gdf_gt_500 = gdf_mwses[gdf_mwses["area_in_ha"] > 500]
    gdf_maxFlux.set_crs(epsg=4326, inplace=True)

    nearest_mws_id = []
    for index, row in gdf_maxFlux.iterrows():
        max_flux_point = row["geometry"]
        nearest_mws_index = gdf_gt_500.distance(max_flux_point).sort_values().index[0]
        nearest_mws_id.append(gdf_gt_500.loc[nearest_mws_index]["id"])

    gdf_maxFlux["nearest_mws_id"] = nearest_mws_id
    gdf_maxFlux.to_file(
        mws_path + "/maxFlux_for_each_mws_with_nearest_mwsId.geojson", driver="GeoJSON"
    )

    gdf_max_flux_geo_dropped = gdf_maxFlux.drop("geometry", axis=1)

    gdf_join = pd.merge(gdf_lt_500, gdf_max_flux_geo_dropped, on="id")

    gdf_mwses_lt_500_dissolve_by_nearest_mws_id = gdf_join.dissolve(by="nearest_mws_id")
    gdf_mwses_lt_500_dissolve_by_nearest_mws_id = (
        gdf_mwses_lt_500_dissolve_by_nearest_mws_id.reset_index()
    )

    # gdf_mwseslt500_dissolveBy_nearestMwsId.drop(['id', 'DN_x', 'area_in_ha_x', 'ID', 'X',
    #        'Y', 'Resultfluxl', 'DN_y', 'area_in_ha_y'], axis=1, inplace=True)
    gdf_mwses_lt_500_dissolve_by_nearest_mws_id.drop(
        ["id", "DN_x", "area_in_ha_x", "X", "Y", "Z", "DN_y", "area_in_ha_y"],
        axis=1,
        inplace=True,
    )
    gdf_mwses_lt_500_dissolve_by_nearest_mws_id.rename(
        columns={"nearest_mws_id": "id"}, inplace=True
    )

    gdf_mwses_lt_500_dissolve_by_nearest_mws_id.plot()

    gdf_mwses_lt_500_dissolve_by_nearest_mws_id.to_file(
        mws_path + "/mwseslt500_dissolveBy_nearestMwsId.geojson", driver="GeoJSON"
    )

    gdf_dissolve_inner = gdf_gt_500.merge(
        gdf_mwses_lt_500_dissolve_by_nearest_mws_id, on="id", how="inner"
    )

    x = gpd.GeoSeries(gdf_dissolve_inner["geometry_x"])
    y = gpd.GeoSeries(gdf_dissolve_inner["geometry_y"])

    gdf_dissolve_inner["geometry"] = x.union(y, align=True)

    gdf_dissolve_inner = gdf_dissolve_inner.set_geometry("geometry")
    gdf_dissolve_inner.plot()

    gdf_dissolve_left = gdf_gt_500.merge(
        gdf_mwses_lt_500_dissolve_by_nearest_mws_id, on="id", how="left"
    )

    gdf_dissolve_left = gdf_dissolve_left.drop(
        gdf_dissolve_left[gdf_dissolve_left["geometry_y"] != None].index
    )

    gdf_dissolve_left.drop(["DN", "area_in_ha", "geometry_y"], axis=1, inplace=True)
    gdf_dissolve_left.rename(columns={"geometry_x": "geometry"}, inplace=True)

    rdf = gpd.GeoDataFrame(
        pd.concat([gdf_dissolve_inner, gdf_dissolve_left], ignore_index=True)
    )
    rdf.crs = "epsg:4326"
    rdf = rdf.to_crs({"init": "epsg:3857"})

    rdf["area_in_ha"] = rdf["geometry"].area / 10**4

    rdf.plot()

    # filtered = rdf.loc[rdf["area_in_ha"] < 500]
    # print("filtered.shape", filtered.shape)

    rdf.drop(["DN", "geometry_x", "geometry_y", "uid_x", "uid_y"], axis=1, inplace=True)
    rdf = rdf.to_crs(3857)
    layer_path = mws_path + "/rdf_revised_pcraster.geojson"
    rdf.to_file(layer_path, driver="GeoJSON")
    return layer_path
