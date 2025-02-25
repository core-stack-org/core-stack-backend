{"state": "Bihar", "district": "Gaya", "block": "Mohanpur"}


# files = os.listdir(f'{NREGA_ASSETS_INPUT_DIR}/{state_name.upper()}')
# print(files)

# for file in files:
#     print("inside loop")
#     var = os.path.join(f'{NREGA_ASSETS_INPUT_DIR}/{state_name.upper()}', file)
#     print(var)
#     gdf = gpd.read_file(os.path.join(f'{NREGA_ASSETS_INPUT_DIR}/{state_name.upper()}', file))
#     gdf['Work Name'] = gdf['Work Name'].astype('unicode')

#     block_shape_file_df = gdf[gdf['Block'].str.lower() == block_name.lower()]

#     # district_shape_file_df = district_shape_file_df[
#         # (district_shape_file_df['Block'].str.lower() == block_name.lower())]

#     block_shape_file_df.to_file(
#         os.path.join(NREGA_ASSETS_OUTPUT_DIR, f'nrega_{district_name}_{block_name}'),
#         driver = 'ESRI Shapefile')

#     path = os.path.join(NREGA_ASSETS_OUTPUT_DIR, f'nrega_{district_name}_{block_name}')
#     return push_shape_to_geoserver(path)
