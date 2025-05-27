# Notebook corresponding to this logic is here:
# https://github.com/Nirzaree/ponds_and_wells_detection/blob/master/final_merging_logic_ee_pipeline.ipynb

import geopandas as gpd
import pandas as pd
import numpy as np
import os
import shapely
import ee
import geemap
import re

from computing.utils import sync_vector_to_gcs, get_geojson_from_gcs
from utilities.constants import GEE_HELPER_PATH

from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    create_gee_directory,
    make_asset_public,
    check_task_status,
)


def split_multipolygon_into_individual_polygons(data_gdf):
    data_gdf = data_gdf.explode()
    return data_gdf


def clip_to_boundary(data_gdf, boundary_gdf):
    data_gdf = data_gdf.sjoin(boundary_gdf[["geometry"]], how="inner")
    data_gdf.drop(["index_right"], axis=1, inplace=True)
    return data_gdf


def change_crs(data_gdf, crs):
    data_gdf.to_crs(crs)
    return data_gdf


def generate_pond_id(data_gdf):
    data_gdf.drop(
        ["FID"], axis=1, inplace=True, errors="ignore"
    )  # drop if the column exists
    data_gdf["pond_id"] = range(data_gdf.shape[0])
    return data_gdf


def dissolve_boundary(data_gdf):
    data_gdf = data_gdf.dissolve()
    return data_gdf


def uid_for_ponds(mws_id_list, pond_id):
    # print(row)
    row_uid = [str(x) for x in mws_id_list]
    row_uid = "_".join(row_uid)
    row_uid = "_".join([row_uid, str(pond_id)])
    return row_uid


# from onprem repo utils.py
def sync_fc_to_gee(fc, description, asset_id):
    try:
        task = ee.batch.Export.table.toAsset(
            **{"collection": fc, "description": description, "assetId": asset_id}
        )
        task.start()
        print("Successfully started task", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error in task: {e}")
        return None


# inspired from https://github.com/core-stack-org/core-stack-backend-onprem/blob/latest_common_download/compute/layers/utils.py
def export_gdf_to_gee_in_chunks(
    gdf, output_suffix, ee_assets_prefix, description, state, district, block
):
    df_size = gdf.shape[0]
    chunk_size = 1000
    if df_size > chunk_size:
        asset_ids = []
        assets = []
        task_ids = []
        ee_initialize(project="helper")
        create_gee_directory(state, district, block, GEE_HELPER_PATH)
        for i in range(0, df_size, chunk_size):
            chunk = gdf.iloc[i : i + chunk_size]
            fc = geemap.geopandas_to_ee(chunk)
            chunk_output_suffix = f"{output_suffix}_{i}_{i + chunk_size}"
            asset_id = (
                get_gee_asset_path(state, district, block, GEE_HELPER_PATH)
                + chunk_output_suffix
            )
            asset_ids.append(asset_id)
            assets.append(ee.FeatureCollection(asset_id))
            print("asset_id" + asset_id)
            task_id = sync_fc_to_gee(fc, chunk_output_suffix, asset_id)
            if task_id:
                task_ids.append(task_id)

        check_task_status(task_ids)
        for asset_id in asset_ids:
            make_asset_public(asset_id)

        final_asset = ee.FeatureCollection(assets).flatten()

        ee_initialize()
        asset_id = (
            get_gee_asset_path(state, district, block, ee_assets_prefix) + output_suffix
        )
        sync_fc_to_gee(final_asset, description, asset_id)
    else:
        fc = geemap.geopandas_to_ee(gdf)
        asset_id = (
            get_gee_asset_path(state, district, block, ee_assets_prefix) + output_suffix
        )
        sync_fc_to_gee(fc, description, asset_id)


@app.task(bind=True)
def merge_swb_ponds(
    self,
    state,
    district,
    block,
    ee_assets_prefix="projects/ee-corestackdev/assets/apps/mws/",
    output_suffix="_merged_swb_ponds",
    target_crs="epsg:4326",
):
    """
    module to merge swb and ponds layer
    """
    # ee.Initialize()
    ee_initialize()

    state = state.lower()
    district = district.lower()
    block = block.lower()

    block_path_ee = get_gee_asset_path(
        asset_path=ee_assets_prefix, state=state, district=district, block=block
    )

    # swb asset
    assets = ee.data.listAssets({"parent": block_path_ee})["assets"]
    swb_layer_path = [
        asset["id"] for asset in assets if "swb3" in os.path.basename(asset["id"])
    ]
    if len(swb_layer_path) == 0:
        swb_layer_path = [
            asset["id"] for asset in assets if "swb2" in os.path.basename(asset["id"])
        ]

    # ponds asset
    ponds_layer_path = [
        asset["id"] for asset in assets if "ponds_" in os.path.basename(asset["id"])
    ]

    # mws asset
    mws_layer_path = [
        asset["id"] for asset in assets if "mws_" in os.path.basename(asset["id"])
    ]

    # admin boundary asset
    # admin_boundary_layer_path = [asset['id'] for asset in assets if 'admin_boundary' in os.path.basename(asset['id'])]

    swb_fc = ee.FeatureCollection(swb_layer_path[0])
    ponds_fc = ee.FeatureCollection(ponds_layer_path[0])
    mws_fc = ee.FeatureCollection(mws_layer_path[0])
    # admin_boundary_fc = ee.FeatureCollection(admin_boundary_layer_path[0])

    # Adding handling for cases where there are more than 5000 rows in any of the files below,
    # in which case just getInfo() won't work

    ponds_gdf = gpd.GeoDataFrame.from_features(ponds_fc.getInfo())
    # swb_gdf = gpd.GeoDataFrame.from_features(swb_fc.getInfo())
    # mws_gdf = gpd.GeoDataFrame.from_features(mws_fc.getInfo())
    # admin_boundary_gdf = gpd.GeoDataFrame.from_features(admin_boundary_fc.getInfo())

    try:
        ponds_gdf = gpd.GeoDataFrame.from_features(ponds_fc.getInfo())
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(ponds_fc, "swb", "GeoJSON")
        check_task_status([task_id])
        ponds_dict = get_geojson_from_gcs("ponds")
        ponds_gdf = gpd.GeoDataFrame.from_features(ponds_dict)

    try:
        swb_gdf = gpd.GeoDataFrame.from_features(swb_fc.getInfo())
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(swb_fc, "swb", "GeoJSON")
        check_task_status([task_id])
        swb_dict = get_geojson_from_gcs("swb")
        swb_gdf = gpd.GeoDataFrame.from_features(swb_dict)

    try:
        mws_gdf = gpd.GeoDataFrame.from_features(mws_fc.getInfo())
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(mws_fc, "swb", "GeoJSON")
        check_task_status([task_id])
        mws_dict = get_geojson_from_gcs("mws")
        mws_gdf = gpd.GeoDataFrame.from_features(mws_dict)

    ponds_gdf = ponds_gdf.set_crs(target_crs)
    swb_gdf = swb_gdf.set_crs(target_crs)
    mws_gdf = mws_gdf.set_crs(target_crs)
    # admin_boundary_gdf = admin_boundary_gdf.set_crs(target_crs)

    if ponds_gdf.shape[0] == 1:
        ponds_gdf = split_multipolygon_into_individual_polygons(ponds_gdf)

    if "pond_id" not in ponds_gdf.columns:
        ponds_gdf = generate_pond_id(ponds_gdf)

    # if admin_boundary_gdf.shape[0] > 1:
    #     admin_boundary_gdf = dissolve_boundary(admin_boundary_gdf)

    # ponds_gdf = clip_to_boundary(
    #     data_gdf=ponds_gdf,
    #     boundary_gdf=admin_boundary_gdf)

    # swb_gdf = clip_to_boundary(
    #     data_gdf=swb_gdf,
    #     boundary_gdf=admin_boundary_gdf)

    mws_outer_boundary_gdf = dissolve_boundary(mws_gdf)

    ponds_gdf = clip_to_boundary(
        data_gdf=ponds_gdf, boundary_gdf=mws_outer_boundary_gdf
    )

    swb_gdf = clip_to_boundary(data_gdf=swb_gdf, boundary_gdf=mws_outer_boundary_gdf)

    # Create merged df
    # 1. add standalone swbs
    intersecting_UIDs = swb_gdf.sjoin(ponds_gdf)["UID"].tolist()
    standalone_swb_gdf = swb_gdf[~swb_gdf["UID"].isin(intersecting_UIDs)]
    merged_gdf = standalone_swb_gdf

    # 2. add standalone ponds
    intersecting_pond_ids = ponds_gdf.sjoin(swb_gdf)["pond_id"].tolist()
    standalone_ponds_gdf = ponds_gdf[~ponds_gdf["pond_id"].isin(intersecting_pond_ids)]

    # add UID column to standalone ponds
    mws_gdf.rename(columns={"uid": "MWS_UID"}, inplace=True)
    mws_uid_ponds_df = standalone_ponds_gdf.sjoin(
        mws_gdf[["MWS_UID", "geometry"]], how="left"
    )
    mws_uid_ponds_df.drop("index_right", inplace=True, axis=1)
    pond_mws_intersections_df = (
        mws_uid_ponds_df.groupby(["pond_id"])["MWS_UID"].unique().reset_index()
    )
    pond_mws_intersections_df["UID"] = pond_mws_intersections_df.apply(
        lambda row: uid_for_ponds(row.MWS_UID, row.pond_id), axis=1
    )
    standalone_ponds_gdf = standalone_ponds_gdf.merge(
        pond_mws_intersections_df[["pond_id", "UID"]]
    )

    merged_gdf = pd.concat([merged_gdf, standalone_ponds_gdf])

    ## 3.Intersection scenarios
    # case 1:
    intersections_gdf = swb_gdf.sjoin(ponds_gdf)
    swb_intersections_df = (
        intersections_gdf.groupby(["UID"])["pond_id"].unique().reset_index()
    )
    pond_intersections_df = (
        intersections_gdf.groupby(["pond_id"])["UID"].unique().reset_index()
    )

    single_intersection_uids = [
        row["UID"]
        for ind, row in swb_intersections_df.iterrows()
        if len(row["pond_id"]) == 1
    ]

    case_1_swb_ids = []
    for x in single_intersection_uids:
        for y in pond_intersections_df["UID"]:
            if x in y:
                if len(y) == 1:
                    case_1_swb_ids.append(x)

    case1_gdf = swb_gdf[swb_gdf["UID"].isin(case_1_swb_ids)].sjoin(
        ponds_gdf, how="left"
    )

    case1_gdf.drop(["index_right"], axis=1, inplace=True)

    for index, row in case1_gdf.iterrows():
        case1_gdf.loc[index, "geometry"] = shapely.ops.unary_union(
            [
                row["geometry"],
                ponds_gdf[ponds_gdf["pond_id"] == row["pond_id"]]["geometry"].iloc[0],
            ]
        )

    merged_gdf = pd.concat([merged_gdf, case1_gdf])

    # case 2:
    # single_intersection_pond_ids = [row['pond_id'] for ind,row in pond_intersections_df.iterrows() if len(row['UID']) == 1]
    # #ponds that intersect with only 1 swb

    # multi_intersection_pond_ids = [row['pond_id'] for ind,row in pond_intersections_df.iterrows() if len(row['UID']) > 1]
    # #ponds that intersect with only 1 swb

    case2_swb_ids = []
    for x in single_intersection_uids:
        for y in pond_intersections_df["UID"]:
            if x in y:
                if len(y) > 1:
                    case2_swb_ids.append(x)

    case2_gdf = swb_gdf[swb_gdf["UID"].isin(case2_swb_ids)].sjoin(ponds_gdf, how="left")

    case2_gdf.drop(["index_right"], axis=1, inplace=True)

    merged_gdf = pd.concat([merged_gdf, case2_gdf])

    merged_gdf["pond_id"] = merged_gdf["pond_id"].astype("Int64")

    # case 3 and 4
    multi_intersection_uids = [
        row["UID"]
        for ind, row in swb_intersections_df.iterrows()
        if len(row["pond_id"]) > 1
    ]

    case3_4_swb_ids = []
    case3_4_pond_ids = []
    for x in multi_intersection_uids:
        for ind, row in pond_intersections_df.iterrows():
            if x in row["UID"]:
                if len(row["UID"]) >= 1:
                    # print(row['pond_id'])
                    case3_4_swb_ids.append(x)
                    case3_4_pond_ids.append(row["pond_id"])

    case3_4_gdf = swb_gdf[swb_gdf["UID"].isin(case3_4_swb_ids)].sjoin(
        ponds_gdf[ponds_gdf["pond_id"].isin(case3_4_pond_ids)], how="left"
    )

    swb_ponds_case3_4 = case3_4_gdf.groupby(["UID"])["pond_id"].agg(set).reset_index()
    swb_ponds_case3_4["pond_id"] = swb_ponds_case3_4["pond_id"].apply(lambda x: list(x))

    case3_4_merged_geom = []
    for swb in swb_ponds_case3_4["UID"]:
        # get corresponding farmpond geometries and merge them
        # print(swb)
        merged_geom = swb_gdf[swb_gdf["UID"] == swb]["geometry"].iloc[0]
        # print(merged_geom)
        # case3_gdf.loc[case3_gdf['UID'] == swb,'pond_id'] = np.nan
        for pond in list(
            swb_ponds_case3_4[swb_ponds_case3_4["UID"] == swb]["pond_id"].iloc[0]
        ):
            merged_geom = shapely.ops.unary_union(
                [
                    merged_geom,
                    ponds_gdf[ponds_gdf["pond_id"] == pond]["geometry"].iloc[0],
                ]
            )
        case3_4_merged_geom.append(merged_geom)
        case3_4_gdf.loc[case3_4_gdf["UID"] == swb, "geometry"] = merged_geom

    case3_4_gdf.drop(["index_right"], axis=1, inplace=True)

    merged_gdf = pd.concat([merged_gdf, case3_4_gdf])

    merged_gdf.reset_index(drop=True, inplace=True)
    merged_gdf = merged_gdf.set_crs(target_crs)

    # merged_fc = geemap.geopandas_to_ee(merged_gdf)

    # try:
    #     task = ee.batch.Export.table.toAsset(
    #         **{
    #             "collection": merged_fc,
    #             "description": 'merging swb and pond layer',
    #             "assetId": block_path_ee + str(district) + '_' + str(block) + output_suffix + '_test',
    #         }
    #     )
    #     task.start()
    #     print("Successfully started the merge chunk", task.status())
    #     return task.status()["id"]
    # except Exception as e:
    #     print(f"Error occurred in running merge task: {e}")

    fc_output_suffix = str(district) + "_" + str(block) + output_suffix
    export_gdf_to_gee_in_chunks(
        gdf=merged_gdf,
        output_suffix=fc_output_suffix,
        ee_assets_prefix=ee_assets_prefix,
        description="merging swb and pond layer",
        state=state,
        district=district,
        block=block,
    )


# example run for a block (gobindpur)

# merge_swb_ponds(
#     state='jharkhand',
#     district='saraikela-kharsawan',
#     block='gobindpur'
# )
