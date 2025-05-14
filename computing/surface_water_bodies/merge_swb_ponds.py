import geopandas as gpd
import pandas as pd
import numpy as np
import os
import shapely
import ee
import geemap
import re

def split_multipolygon_into_individual_polygons(data_gdf):
    data_gdf = data_gdf.explode()
    return data_gdf

def clip_to_boundary(data_gdf,boundary_gdf):
    data_gdf = data_gdf.sjoin(boundary_gdf[['geometry']],how = 'inner')
    data_gdf.drop(['index_right'],axis=1,inplace=True)
    return data_gdf

def change_crs(data_gdf,crs):
    data_gdf.to_crs(crs)
    return data_gdf

def generate_pond_id(data_gdf):
    data_gdf.drop(['FID'],axis=1,inplace=True,errors='ignore') #drop if the column exists
    data_gdf['pond_id'] = range(data_gdf.shape[0])
    return data_gdf

def dissolve_boundary(data_gdf):
    data_gdf = data_gdf.dissolve()
    return data_gdf

def valid_gee_text(description):
    description = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", description)
    return description.replace(" ", "_")

def get_gee_asset_path(asset_path, state, district=None, block=None):
    gee_path = asset_path + valid_gee_text(state.lower()) + "/"
    if district:
        gee_path += valid_gee_text(district.lower()) + "/"
    if block:
        gee_path += valid_gee_text(block.lower()) + "/"
    return gee_path

def merge_swb_ponds(state,
                    district,
                    block,
                    ee_assets_prefix = 'projects/ee-corestackdev/assets/apps/mws/',
                    output_suffix = '_merged_swb_ponds'
                    ):
    '''
    module to merge swb and ponds layer
    '''
    ee.Initialize()

    state = state.lower()
    district = district.lower()
    block = block.lower()

    block_path_ee = get_gee_asset_path(
        asset_path=ee_assets_prefix,
        state=state,
        district=district,
        block=block
    )

    #swb asset
    assets = ee.data.listAssets({'parent': block_path_ee})['assets']
    swb_layer_path = [asset['id'] for asset in assets if 'swb3' in os.path.basename(asset['id'])]
    if (len(swb_layer_path) == 0):
        swb_layer_path = [asset['id'] for asset in assets if 'swb2' in os.path.basename(asset['id'])]

    #ponds asset
    ponds_layer_path = [asset['id'] for asset in assets if 'ponds_' in os.path.basename(asset['id'])]

    #mws asset
    mws_layer_path = [asset['id'] for asset in assets if 'mws_' in os.path.basename(asset['id'])]

    #admin boundary asset
    admin_boundary_layer_path = [asset['id'] for asset in assets if 'admin_boundary' in os.path.basename(asset['id'])]

    swb_fc = ee.FeatureCollection(swb_layer_path[0])
    ponds_fc = ee.FeatureCollection(ponds_layer_path[0])
    mws_fc = ee.FeatureCollection(mws_layer_path[0])
    admin_boundary_fc = ee.FeatureCollection(admin_boundary_layer_path[0])

    ponds_gdf = gpd.GeoDataFrame.from_features(ponds_fc.getInfo())
    swb_gdf = gpd.GeoDataFrame.from_features(swb_fc.getInfo())
    mws_gdf = gpd.GeoDataFrame.from_features(mws_fc.getInfo())
    admin_boundary_gdf = gpd.GeoDataFrame.from_features(admin_boundary_fc.getInfo())

    if ponds_gdf.shape[0] == 1:
        ponds_gdf = split_multipolygon_into_individual_polygons(ponds_gdf)

    if 'pond_id' not in ponds_gdf.columns:
        ponds_gdf = generate_pond_id(ponds_gdf)

    if admin_boundary_gdf.shape[0] > 1:
        admin_boundary_gdf = dissolve_boundary(admin_boundary_gdf)

    ponds_gdf = clip_to_boundary(
        data_gdf=ponds_gdf,
        boundary_gdf=admin_boundary_gdf)
    
    swb_gdf = clip_to_boundary(
        data_gdf=swb_gdf,
        boundary_gdf=admin_boundary_gdf)

    # mws_outer_boundary_gdf = dissolve_boundary(mws_gdf)

    # ponds_gdf = clip_to_boundary(
    #     data_gdf=ponds_gdf,
    #     boundary_gdf=mws_outer_boundary_gdf)
    
    # swb_gdf = clip_to_boundary(
    #     data_gdf=swb_gdf,
    #     boundary_gdf=mws_outer_boundary_gdf)    

    #create merged df
    #add standalone swbs
    intersecting_UIDs = swb_gdf.sjoin(ponds_gdf)['UID'].tolist()
    standalone_swb_gdf = swb_gdf[~swb_gdf['UID'].isin(intersecting_UIDs)]
    merged_gdf = standalone_swb_gdf

    #add standalone ponds
    intersecting_pond_ids = ponds_gdf.sjoin(swb_gdf)['pond_id'].tolist()
    standalone_ponds_gdf = ponds_gdf[~ponds_gdf['pond_id'].isin(intersecting_pond_ids)]
    merged_gdf = pd.concat([merged_gdf,
                        standalone_ponds_gdf])
    
    ## Intersection scenarios 
    #case 1:
    intersections_gdf = swb_gdf.sjoin(ponds_gdf)
    swb_intersections_df = intersections_gdf.groupby(['UID'])['pond_id'].unique().reset_index()
    pond_intersections_df = intersections_gdf.groupby(['pond_id'])['UID'].unique().reset_index()

    single_intersection_uids = [row['UID'] for ind,row in swb_intersections_df.iterrows() if len(row['pond_id']) == 1]

    case_1_swb_ids = []
    for x in single_intersection_uids:
        for y in pond_intersections_df['UID']:
            if (x in y):
                if (len(y) == 1):
                    case_1_swb_ids.append(x)

    case1_gdf = swb_gdf[swb_gdf['UID'].isin(case_1_swb_ids)].sjoin(
        ponds_gdf,
        how='left')
    
    case1_gdf.drop(['index_right'],axis=1,inplace=True)

    for index,row in case1_gdf.iterrows():
        case1_gdf.loc[index,'geometry'] = shapely.ops.unary_union(
            [
                row['geometry'],
                ponds_gdf[ponds_gdf['pond_id'] == row['pond_id']]['geometry'].iloc[0]
            ])

    merged_gdf = pd.concat(
        [
            merged_gdf,
            case1_gdf
        ])

    #case 2:
    # single_intersection_pond_ids = [row['pond_id'] for ind,row in pond_intersections_df.iterrows() if len(row['UID']) == 1]
    # #ponds that intersect with only 1 swb

    # multi_intersection_pond_ids = [row['pond_id'] for ind,row in pond_intersections_df.iterrows() if len(row['UID']) > 1]
    # #ponds that intersect with only 1 swb

    case2_swb_ids = []
    for x in single_intersection_uids:
        for y in pond_intersections_df['UID']:
            if (x in y):
                if (len(y) > 1):
                    case2_swb_ids.append(x)

    case2_gdf = swb_gdf[swb_gdf['UID'].isin(case2_swb_ids)].sjoin(
        ponds_gdf,
        how='left')
    
    case2_gdf.drop(['index_right'],axis=1,inplace=True)

    merged_gdf = pd.concat(
        [
            merged_gdf,
            case2_gdf
        ])
    
    merged_gdf['pond_id'] = merged_gdf['pond_id'].astype('Int64')

    # case 3 and 4
    multi_intersection_uids = [row['UID'] for ind,row in swb_intersections_df.iterrows() if len(row['pond_id']) > 1]
    
    case3_4_swb_ids = []
    case3_4_pond_ids = []
    for x in multi_intersection_uids:
        for ind,row in pond_intersections_df.iterrows():
            if (x in row['UID']):
                if (len(row['UID']) >= 1):
                    # print(row['pond_id'])
                    case3_4_swb_ids.append(x)
                    case3_4_pond_ids.append(row['pond_id'])    

    case3_4_gdf = swb_gdf[swb_gdf['UID'].isin(case3_4_swb_ids)].sjoin(
        ponds_gdf[ponds_gdf['pond_id'].isin(case3_4_pond_ids)],
        how='left'
    )

    swb_ponds_case3_4 = case3_4_gdf.groupby(['UID'])['pond_id'].agg(set).reset_index()
    swb_ponds_case3_4['pond_id'] = swb_ponds_case3_4['pond_id'].apply(lambda x: list(x))

    case3_4_merged_geom = []
    for swb in swb_ponds_case3_4['UID']:
        #get corresponding farmpond geometries and merge them 
        # print(swb)
        merged_geom = swb_gdf[swb_gdf['UID'] == swb]['geometry'].iloc[0]
        # print(merged_geom)
        # case3_gdf.loc[case3_gdf['UID'] == swb,'pond_id'] = np.nan
        for pond in list(swb_ponds_case3_4[swb_ponds_case3_4['UID'] == swb]['pond_id'].iloc[0]):
            merged_geom = shapely.ops.unary_union(
                [
                    merged_geom,
                    ponds_gdf[ponds_gdf['pond_id'] == pond]['geometry'].iloc[0]
                ])
        case3_4_merged_geom.append(merged_geom)
        case3_4_gdf.loc[case3_4_gdf['UID'] == swb,'geometry'] = merged_geom    

    case3_4_gdf.drop(['index_right'],axis=1,inplace=True)

    merged_gdf = pd.concat([
        merged_gdf,
        case3_4_gdf])
    
    merged_gdf.reset_index(drop=True,inplace=True)

    merged_gdf = merged_gdf.set_crs('epsg:4326')
    merged_fc = geemap.geopandas_to_ee(merged_gdf)

    task = ee.batch.Export.table.toAsset(
        **{
            "collection": merged_fc,
            "description": 'merging swb and pond layer',
            "assetId": block_path_ee + str(district) + '_' + str(block) + output_suffix + '_test',
        }
    )
    task.start() 

#example run for a block (gobindpur)

# merge_swb_ponds(
#     state='jharkhand',
#     district='saraikela-kharsawan',
#     block='gobindpur'
# )
