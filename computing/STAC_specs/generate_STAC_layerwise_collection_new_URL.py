# %% [markdown]
# Final flow
# - all code in functions
# - data pull from geoserver
# - column descriptions and layer descriptions from csv
# - style file from github
# - output stored locally
#
# Common functions between raster and vector wherever possible

# %%
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

import rasterio
import os

# import fsspec
# import s3fs

# import json
import xml.etree.ElementTree as ET
import datetime

# from datetime import datetime, timezone

import urllib
import requests
from io import BytesIO

# from rasterio.warp import transform_bounds

from matplotlib.colors import ListedColormap, Normalize
from shapely.geometry import mapping, box, Polygon

import pystac

import boto3
import json
import tqdm
import glob

import re

import sys

sys.path.append("..")
from computing.STAC_specs import constants

from nrm_app.settings import S3_ACCESS_KEY, S3_SECRET_KEY
from utilities.gee_utils import valid_gee_text

aws_creds = {}
aws_creds["aws_access_key_id"] = S3_ACCESS_KEY
aws_creds["aws_secret_access_key"] = S3_SECRET_KEY


# %%
GEOSERVER_BASE_URL = constants.GEOSERVER_BASE_URL
# GEOSERVER_BASE_URL

# %%
# THUMBNAIL_DATA_URL = constants.GITHUB_DATA_URL
THUMBNAIL_DATA_URL = constants.S3_STAC_BUCKET_URL

# %%
# STAC_S3_BUCKET_URL=constants.S3_STAC_BUCKET_URL
# STAC_S3_BUCKET_URL

# %%
# LOCAL_DATA_DIR = '../data/'
LOCAL_DATA_DIR = "data/STAC_specs/"

# %%
STYLE_FILE_DIR = os.path.join(LOCAL_DATA_DIR, "input/style_files/")

# %%
THUMBNAIL_DIR = os.path.join(LOCAL_DATA_DIR, "STAC_output_merged_collection")
# THUMBNAIL_DIR

# %%
TEHSIL_DIRNAME = "tehsil_wise"

# %%
STAC_FILES_DIR = os.path.join(
    LOCAL_DATA_DIR, "CorestackCatalogs_merged_collection"  # test folder
)
#'CorestackCatalogs_exception_handling'

# %%
LAYER_DESC_GITHUB_URL = constants.LAYER_DESC_GITHUB_URL
# LAYER_DESC_GITHUB_URL

# %%
VECTOR_COLUMN_DESC_GITHUB_URL = constants.VECTOR_COLUMN_DESC_GITHUB_URL
# VECTOR_COLUMN_DESC_GITHUB_URL

# %%
S3_STAC_BUCKET_NAME = constants.S3_STAC_BUCKET_NAME
# S3_STAC_BUCKET_NAME

# %%
layer_STAC_generated = False  # output flag


# %%
def valid_gee_text(description):
    description = re.sub(r"[^a-zA-Z0-9 .,:;_-]", "", description)
    return description.replace(
        " ", "_"
    )  # in the main module can be taken from utilities.gee_utils


# %% [markdown]
# ### Raster flow


def generate_raster_describe_url(workspace, layer_name, geoserver_base_url):
    
    describe_url = (
        f"{geoserver_base_url}/{workspace}/wcs?"
        f"service=WCS&version=2.0.1&request=DescribeCoverage&"
        f"coverageId={workspace}:{layer_name}"
    )
    return describe_url

#%%
def generate_raster_thumbnail_url(workspace, layer_name, bbox, geoserver_base_url):
  
    bbox_str = ",".join(map(str, bbox))
    
    thumbnail_url = (
        f"{geoserver_base_url}/{workspace}/wms?"
        f"service=WMS&version=1.1.0&request=GetMap&"
        f"layers={workspace}:{layer_name}&"
        f"bbox={bbox_str}&"
        f"width=180&height=180&srs=EPSG:4326&styles=&format=image/png"
    )
    return thumbnail_url

#%%

def get_metadata_from_wcs(describe_url):
    response = requests.get(describe_url, verify=False)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        # GeoServer uses these standard namespaces for WCS 2.0.1
        ns = {
            'gml': 'http://www.opengis.net/gml/3.2', 
            'wcs': 'http://www.opengis.net/wcs/2.0'
        }
        
        # 1. Fetch Bounding Box (Envelope)
        envelope = root.find('.//gml:Envelope', ns)
        if envelope is not None:
            lower = envelope.find('gml:lowerCorner', ns).text.split()
            upper = envelope.find('gml:upperCorner', ns).text.split()
            
            
            bbox = [float(lower[1]), float(lower[0]), float(upper[1]), float(upper[0])]
            
            footprint = Polygon([
                [bbox[0], bbox[1]],
                [bbox[0], bbox[3]],
                [bbox[2], bbox[3]],
                [bbox[2], bbox[1]]
            ])

            # 2. Fetch Actual Pixel Shape (GridEnvelope)
            grid_low = root.find('.//gml:low', ns)
            grid_high = root.find('.//gml:high', ns)
            
            if grid_low is not None and grid_high is not None:
                low_coords = grid_low.text.split()
                high_coords = grid_high.text.split()
                
               
                actual_width = int(high_coords[0]) - int(low_coords[0]) + 1
                actual_height = int(high_coords[1]) - int(low_coords[1]) + 1
                shape = [actual_height, actual_width] 
            else:
                shape = [0, 0] 

            return bbox, mapping(footprint), "EPSG:4326", shape
            
    return None

#%%

def download_wms_thumbnail(wms_url, output_path):
    response = requests.get(wms_url, verify=False)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(response.content)
        return True
    return False

# %%
def read_layer_description(filepath, layer_name, overwrite_existing):
    if (os.path.exists(filepath)) and (not overwrite_existing):
        layer_desc_df = pd.read_csv(filepath)
    else:
        # download and save
        print("STAC:downloading layer description csv from github")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        layer_desc_df = pd.read_csv(LAYER_DESC_GITHUB_URL)
        layer_desc_df.to_csv(filepath)
    if layer_name in layer_desc_df["layer_name"].tolist():
        layer_desc = layer_desc_df[layer_desc_df["layer_name"] == layer_name][
            "layer_description"
        ].iloc[0]
    else:
        print(f"layer description for {layer_name} layer does not exist currently")
        layer_desc = ""
    return layer_desc


# %%
def generate_raster_url(
    workspace, layer_name, geoserver_base_url, output_format="geotiff"
):
    wcs_url = (
        f"{geoserver_base_url}/{workspace}/wcs?"
        f"service=WCS&version=2.0.1&request=GetCoverage&"
        f"CoverageId={workspace}:{layer_name}&"
        f"format={output_format}&compression=LZW"
    )
    print("Raster URL:", wcs_url)
    return wcs_url


# %%
# def read_raster_data(raster_url):

#     # when reading from geoserver
#     response = requests.get(raster_url, verify=False)

#     # exception handling: scenario 1: when fetching data from geoserver
#     try:
#         response.raise_for_status()
#     except Exception as e:
#         print("STAC_Error: " + str(e) + "when fetching geoserver data")
#         print("exiting STAC pipeline")
#         return layer_STAC_generated

#     raster_data = BytesIO(response.content)

#     # read the data and fetch the metadata
#     with rasterio.open(raster_data) as r:
#         crs = r.crs
#         bounds = r.bounds
#         bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
#         footprint = Polygon(
#             [
#                 [bounds.left, bounds.bottom],
#                 [bounds.left, bounds.top],
#                 [bounds.right, bounds.top],
#                 [bounds.right, bounds.bottom],
#             ]
#         )
#         data = r.read(1)  # TODO: wouldn't work if there are multiple bands

#         # id = os.path.basename(raster_url) #works when data is local
#         # id = layer_name
#         # gsd = 10
#         shape = r.shape
#         data_type = str(r.dtypes[0])

#         return (
#             data,
#             bbox,
#             mapping(footprint),
#             crs,
#             # id,
#             # gsd,
#             shape,
#             data_type,
#         )


# %%
def create_raster_item(describe_url,
                       id,
                       layer_title,
                       layer_description,
                       display_name,
                       theme,
                       start_date='',
                       end_date='',
                       gsd=pd.NA
                       ):

    
    metadata = get_metadata_from_wcs(describe_url)
    if metadata is None:
        print(f"STAC_Error: Could not fetch metadata for {id}")
        return None
        
    bbox, footprint, crs, shape = metadata
    keyword = display_name.lower()

    if ((start_date != '') & (end_date != '') & (not pd.isna(gsd))):
        raster_item = pystac.Item(id=id,
                                  geometry=footprint,
                                  bbox=bbox,
                                  datetime=datetime.datetime.now(datetime.timezone.utc),
                                  properties={
                                        "title" : layer_title,
                                        "description" : layer_description,
                                        "start_datetime": start_date.isoformat() + 'Z',
                                        "end_datetime": end_date.isoformat() + 'Z',
                                        "gsd": gsd,
                                        "keywords": [theme]
                                  })
    elif (not pd.isna(gsd)): 
         raster_item = pystac.Item(id=id,
                                  geometry=footprint,
                                  bbox=bbox,
                                  datetime=datetime.datetime.now(datetime.timezone.utc),  
                                  properties={
                                        "title" : layer_title,
                                        "description" : layer_description,
                                      "gsd": gsd,
                                      "start_datetime": constants.DEFAULT_START_DATE.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                      "end_datetime": constants.DEFAULT_END_DATE.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                      "keywords": [theme]                                                 
                                  })       
    else:
         raster_item = pystac.Item(id=id,
                                  geometry=footprint,
                                  bbox=bbox,
                                  datetime=datetime.datetime.now(datetime.timezone.utc),
                                  properties={
                                        "title" : layer_title,
                                        "description" : layer_description,
                                        "gsd": gsd,
                                        "start_datetime": constants.DEFAULT_START_DATE.strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                        "end_datetime": constants.DEFAULT_END_DATE.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                        "keywords": [theme]
                                  })            
    
    
    proj_ext = pystac.extensions.projection.ProjectionExtension.ext(raster_item, add_if_missing=True)
    proj_ext.epsg = crs
    proj_ext.shape = [shape[0], shape[1]]

    return raster_item 


# %%
def add_raster_data_asset(raster_item, geoserver_url):
    raster_item.add_asset(
        "data",
        pystac.Asset(
            # href=os.path.join(data_url, os.path.relpath(raster_path, start=data_dir)), #TODO
            href=geoserver_url,
            media_type=pystac.MediaType.GEOTIFF,
            roles=["data"],
            title="Raster Layer",
        ),
    )

    return raster_item


# %%
# def add_raster_extension(raster_item): #TODO
#         #add certain metadata under raster extension
#     raster_ext = pystac.extensions.raster.RasterExtension.ext(raster_item.assets["data"], add_if_missing=True)
#     raster_band = pystac.extensions.raster.RasterBand.create(
#         data_type=data_type,
#         spatial_resolution=gsd,
#         # nodata=nodata
#     )
#     raster_ext.bands = [raster_band]


# %%
def parse_raster_style_file(style_file_url, STYLE_FILE_DIR):

    # download style file if not already downloaded, and save it locally
    style_file_name = os.path.basename(style_file_url)
    style_file_local_path = os.path.join(STYLE_FILE_DIR, style_file_name)

    if not os.path.exists(style_file_local_path):
        # TODO: try statement
        os.makedirs(os.path.dirname(style_file_local_path), exist_ok=True)
        try:
            urllib.request.urlretrieve(style_file_url, style_file_local_path)
        # exception handling: scenario 2: when fetching style file from github
        except Exception as e:
            print(
                "STAC_Error: Could not retrieve style file from github. Error: "
                + str(e)
            )
            print("exiting STAC pipeline")
            return layer_STAC_generated

    tree = ET.parse(style_file_local_path)
    root = tree.getroot()
    classes = []

    for entry in root.findall(".//paletteEntry"):
        class_info = {}
        for attr_key, attr_value in entry.attrib.items():
            if attr_key == "value":
                try:
                    class_info[attr_key] = int(attr_value)
                except ValueError:
                    class_info[attr_key] = attr_value
            else:
                class_info[attr_key] = attr_value
        classes.append(class_info)

    # If no paletteEntry tags are found, check for item tags
    if not classes:
        for entry in root.findall(".//item"):
            class_info = {}
            for attr_key, attr_value in entry.attrib.items():
                if attr_key == "value":
                    try:
                        class_info[attr_key] = int(attr_value)
                    except ValueError:
                        class_info[attr_key] = attr_value
                else:
                    class_info[attr_key] = attr_value
            classes.append(class_info)
    return classes


# %%
def add_classification_extension(raster_style_url, raster_item, STYLE_FILE_DIR):

    style_info = parse_raster_style_file(
        style_file_url=raster_style_url, STYLE_FILE_DIR=STYLE_FILE_DIR
    )
    classification_ext = pystac.extensions.classification.ClassificationExtension.ext(
        raster_item.assets["data"], add_if_missing=True
    )
    stac_classes = []
    for cls in style_info:
        stac_class_obj = pystac.extensions.classification.Classification.create(
            value=int(cls["value"]),
            name=cls.get("label") or f"Class {cls['value']}",
            description=cls.get("label"),
            color_hint=cls["color"].replace("#", ""),
        )
        stac_classes.append(stac_class_obj)
    classification_ext.classes = stac_classes

    return (raster_item, style_info)  # style info is required for thumbnail


# %%
def add_stylefile_asset(STAC_item, style_file_url):
    STAC_item.add_asset(
        "style",
        pystac.Asset(
            # href=os.path.join(data_url, os.path.relpath(raster_style_path, start=data_dir)),
            href=style_file_url,
            media_type=pystac.MediaType.XML,
            roles=["metadata"],
            title="QGIS Style file",
        ),
    )
    return STAC_item


# %%
def generate_raster_thumbnail(raster_data, style_info, output_path):

    unique_raster_values = np.unique(
        raster_data.compressed()
        if isinstance(raster_data, np.ma.MaskedArray)
        else raster_data
    )
    # Filter QML info to only include values present in the raster data
    filtered_style_info = [
        cls for cls in style_info if cls.get("value") in unique_raster_values
    ]

    values = [cls["value"] for cls in filtered_style_info if "value" in cls]
    colors = [cls["color"] for cls in filtered_style_info if "color" in cls]

    # print(f"Parsed QML values: {values}")
    # print(f"Parsed QML colors: {colors}")

    try:
        if not values or not colors or len(values) != len(colors):
            raise ValueError("Invalid or insufficient palette information in QML file.")

        sorted_indices = np.argsort(values)
        sorted_values = np.array(values)[sorted_indices]
        sorted_colors = np.array(colors)[sorted_indices]

        cmap = ListedColormap(sorted_colors)
        bounds = np.array(sorted_values) - 0.5
        bounds = np.append(bounds, sorted_values[-1] + 0.5)
        norm = Normalize(vmin=bounds.min(), vmax=bounds.max())

    except ValueError as e:
        print(
            f"Skipping palette generation due to error: {e}. Using a default colormap."
        )
        cmap = "gray"
        norm = None
    plt.figure(figsize=(3, 3), dpi=100)

    plt.imshow(raster_data, cmap=cmap, norm=norm, interpolation="none")
    plt.axis("off")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)
    plt.close()


# %%
def add_thumbnail_asset(
    STAC_item, THUMBNAIL_PATH, LOCAL_DATA_DIR, THUMBNAIL_DATA_URL  # TODO
):
    STAC_item.add_asset(
        "thumbnail",
        pystac.Asset(
            href=os.path.join(
                THUMBNAIL_DATA_URL,
                os.path.relpath(THUMBNAIL_PATH, start=LOCAL_DATA_DIR),
            ),
            # href=os.path.join(THUMBNAIL_DATA_URL, os.path.relpath(THUMBNAIL_PATH,
            #                                                    start=LOCAL_DATA_DIR)),
            media_type=pystac.MediaType.PNG,
            roles=["thumbnail"],
            title="Thumbnail",
        ),
    )

    return STAC_item


# %%
def read_layer_mapping(
    layer_map_csv_path,  # TODO: update this function for each type of layer
    layer_name,
    district,  #
    block,
    start_year="",
    #    end_year = ''
):
    layer_mapping_df = pd.read_csv(layer_map_csv_path)
    layer_display_name = layer_mapping_df[layer_mapping_df["layer_name"] == layer_name][
        "display_name"
    ].iloc[0]

    geoserver_workspace_name = layer_mapping_df[
        layer_mapping_df["layer_name"] == layer_name
    ]["geoserver_workspace_name"].iloc[0]
    geoserver_layer_name = layer_mapping_df[
        layer_mapping_df["layer_name"] == layer_name
    ]["geoserver_layer_name"].iloc[0]

    style_file_url = layer_mapping_df[layer_mapping_df["layer_name"] == layer_name][
        "style_file_url"
    ].iloc[0]
    ee_layer_name = layer_mapping_df[layer_mapping_df["layer_name"] == layer_name][
        "ee_layer_name"
    ].iloc[0]

    gsd = layer_mapping_df[layer_mapping_df["layer_name"] == layer_name][
        "spatial_resolution_in_meters"
    ].iloc[0]

    theme = layer_mapping_df[layer_mapping_df["layer_name"] == layer_name][
        "theme"
    ].iloc[0]

    if layer_name == "land_use_land_cover_raster":
        start_year_modified = str(
            int(start_year) % 100
        )  # keep only last 2 digits of the full year
        end_year_modified = str((int(start_year) + 1) % 100)
        print("start_year_modified=", start_year_modified)
        print("end_year_modified=", end_year_modified)
        geoserver_layer_name = geoserver_layer_name.format(
            start_year=start_year_modified,
            end_year=end_year_modified,
            district=district,
            block=block,
        )
    # prin
    # print(geoserver_workspace_name,geoserver_layer_name)
    elif (layer_name == "tree_canopy_cover_density_raster") | (
        layer_name == "tree_canopy_height_raster"
    ):
        geoserver_layer_name = geoserver_layer_name.format(
            start_year=start_year, district=district, block=block
        )
    else:  # only LULC has some specific things such as 2 digit year representation and no district information
        geoserver_layer_name = geoserver_layer_name.format(
            district=district, block=block
        )
    return (
        geoserver_workspace_name,
        geoserver_layer_name,
        style_file_url,
        layer_display_name,
        ee_layer_name,
        gsd,
        theme,
    )


# %%
def generate_raster_item(state, district, block, layer_name, layer_map_csv_path, 
                         layer_desc_csv_path, start_year, overwrite_existing): 
    
    # 1. read layer description
    layer_description = read_layer_description(filepath=layer_desc_csv_path,
                                             layer_name=layer_name,
                                             overwrite_existing=overwrite_existing)
    
    #2. get geoserver url parameters from the layer details
    geoserver_workspace_name, geoserver_layer_name, style_file_url, layer_display_name, ee_layer_name, gsd ,theme = \
        read_layer_mapping(layer_map_csv_path=layer_map_csv_path,
                          district=district, block=block, layer_name=layer_name, start_year=start_year)

    # 3.generate describe url
    geoserver_url = generate_raster_url(workspace=geoserver_workspace_name,
                                       layer_name=geoserver_layer_name,
                                       geoserver_base_url=GEOSERVER_BASE_URL)

    # 4. create raster item using the describe_url
    start_date = ''
    end_date = ''
    layer_title = layer_display_name 
    layer_id = f"{state}_{district}_{block}_{layer_name}" 

    if start_year != "":
        layer_title = f"{layer_display_name} : {start_year}"
        layer_id = f"{state}_{district}_{block}_{layer_name}_{start_year}"
        start_date = start_year + '-' + constants.AGRI_YEAR_START_DATE
        start_date = pd.to_datetime(start_date)
        end_date = str(int(start_year) + 1) + '-' + constants.AGRI_YEAR_END_DATE
        end_date = pd.to_datetime(end_date)
    
    DESCRIBE_URL = generate_raster_describe_url(
        workspace=geoserver_workspace_name, 
        layer_name=geoserver_layer_name, 
        geoserver_base_url=GEOSERVER_BASE_URL
    )
    
    raster_item = create_raster_item(describe_url=DESCRIBE_URL, 
                                     id=layer_id,
                                     layer_title=layer_title,
                                     layer_description=layer_description,
                                     display_name=layer_display_name,
                                     theme=theme,
                                     start_date=start_date,
                                     end_date=end_date,
                                     gsd=gsd)
    
    if raster_item is None:
        return None

    # 5. Add Raster Data Asset
    raster_item = add_raster_data_asset(raster_item, geoserver_url=geoserver_url)
    
    # 6. Add Classification Extension
    raster_item, style_info = add_classification_extension(raster_style_url=style_file_url,
                                                          raster_item=raster_item,
                                                          STYLE_FILE_DIR=STYLE_FILE_DIR)
    
    # 7. Add Style File Asset
    add_stylefile_asset(STAC_item=raster_item, style_file_url=style_file_url)

    # 8. GENERATE  THUMBNAIL URL 
    if (start_year != ''):
        thumbnail_filename = f'{state}_{district}_{block}_{layer_name}_{start_year}.png'
    else:
        thumbnail_filename = f'{state}_{district}_{block}_{layer_name}.png' #TODO:
    
    THUMBNAIL_PATH = os.path.join(THUMBNAIL_DIR, thumbnail_filename)
    
    # Generate the thumbnail URL using the bbox from the pystac item metadata
    THUMBNAIL_URL = generate_raster_thumbnail_url(
        workspace=geoserver_workspace_name,
        layer_name=geoserver_layer_name,
        bbox=raster_item.bbox,
        geoserver_base_url=GEOSERVER_BASE_URL
    )

    # 9. Download and Add Thumbnail Asset
    if download_wms_thumbnail(wms_url=THUMBNAIL_URL, output_path=THUMBNAIL_PATH):
        raster_item = add_thumbnail_asset(
            STAC_item=raster_item,
            THUMBNAIL_PATH=THUMBNAIL_PATH,
            LOCAL_DATA_DIR=LOCAL_DATA_DIR,
            THUMBNAIL_DATA_URL=THUMBNAIL_DATA_URL
        )

    return raster_item


#%%


def update_STAC_files(state, district, block, STAC_item):

    # Helper function to merge two bounding boxes: [minx, miny, maxx, maxy]
    def merge_bboxes(bbox1, bbox2):
        if not bbox1 or bbox1 == [0, 0, 0, 0]:
            return bbox2
        return [
            min(bbox1[0], bbox2[0]),
            min(bbox1[1], bbox2[1]),
            max(bbox1[2], bbox2[2]),
            max(bbox1[3], bbox2[3]),
        ]

    item_bbox = STAC_item.bbox

    # 1. create block collection, if not already existing
    block_dir = os.path.join(STAC_FILES_DIR, TEHSIL_DIRNAME, state, district, block)
    os.makedirs(block_dir, exist_ok=True)

    block_catalog_path = os.path.join(block_dir, "collection.json")

    if os.path.exists(block_catalog_path):
        block_catalog = pystac.read_file(block_catalog_path)
        print(f"Loaded existing block collection: {block}")

        current_bbox = block_catalog.extent.spatial.bboxes[0]
        block_catalog.extent.spatial.bboxes = [merge_bboxes(current_bbox, item_bbox)]

        block_items = list(block_catalog.get_all_items())
        if STAC_item.id in [x.id for x in block_items]:
            block_catalog.remove_item(STAC_item.id)
            print(f"Removed previously existing {STAC_item.id} from the collection")

        block_catalog.add_item(STAC_item)
        block_catalog.normalize_and_save(
            block_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
        )
        layer_STAC_generated = True
        return layer_STAC_generated
    else:

        block_catalog = pystac.Collection(
            id=block,
            title=f"{block}",
            description=f"STAC collection for {block} block data in {district}, {state}",
            license="CC-BY-4.0",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([item_bbox]),
                temporal=pystac.TemporalExtent(
                    [[constants.DEFAULT_START_DATE, constants.DEFAULT_END_DATE]]
                ),
            ),
            providers=[
                pystac.Provider(
                    name="CoRE Stack",
                    roles=[
                        pystac.ProviderRole.PRODUCER,
                        pystac.ProviderRole.PROCESSOR,
                        pystac.ProviderRole.HOST,
                        pystac.ProviderRole.LICENSOR,
                    ],
                    url="https://core-stack.org/",
                )
            ],
            keywords=[
                "social-ecological",
                "sustainability",
                "CoRE stack",
                block,
                district,
                state,
            ],
        )
        print("created block collection")

    # 2. add item to block collection
    block_catalog.add_item(STAC_item)

    # 3. create district collection if not existing
    district_dir = os.path.join(STAC_FILES_DIR, TEHSIL_DIRNAME, state, district)
    os.makedirs(district_dir, exist_ok=True)

    district_catalog_path = os.path.join(district_dir, "collection.json")

    if os.path.exists(district_catalog_path):
        district_catalog = pystac.read_file(district_catalog_path)
        print("loaded district collection")

        current_bbox = district_catalog.extent.spatial.bboxes[0]
        district_catalog.extent.spatial.bboxes = [merge_bboxes(current_bbox, item_bbox)]

        district_catalog.add_child(block_catalog)
        district_catalog.normalize_and_save(
            district_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
        )
        layer_STAC_generated = True
        return layer_STAC_generated
    else:

        district_catalog = pystac.Collection(
            id=district,
            title=f"{district}",
            description=f"STAC collection for data of {district} district",
            license="CC-BY-4.0",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([item_bbox]),
                temporal=pystac.TemporalExtent(
                    [[constants.DEFAULT_START_DATE, constants.DEFAULT_END_DATE]]
                ),
            ),
            providers=[
                pystac.Provider(
                    name="CoRE Stack",
                    roles=[
                        pystac.ProviderRole.PRODUCER,
                        pystac.ProviderRole.PROCESSOR,
                        pystac.ProviderRole.HOST,
                        pystac.ProviderRole.LICENSOR,
                    ],
                    url="https://core-stack.org/",
                )
            ],
            keywords=[
                "social-ecological",
                "sustainability",
                "CoRE stack",
                district,
                state,
            ],
        )
        print("created district collection")
        district_catalog.add_child(block_catalog)
        district_catalog.normalize_and_save(
            district_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
        )

    # 4. create state collection if not existing
    state_dir = os.path.join(STAC_FILES_DIR, TEHSIL_DIRNAME, state)
    state_collection_path = os.path.join(state_dir, "collection.json")

    if os.path.exists(state_collection_path):
        state_collection = pystac.read_file(state_collection_path)
        print("loaded state collection")

        current_bbox = state_collection.extent.spatial.bboxes[0]
        state_collection.extent.spatial.bboxes = [merge_bboxes(current_bbox, item_bbox)]

        state_collection.add_child(district_catalog)
        state_collection.normalize_and_save(
            state_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
        )
        layer_STAC_generated = True
        return True
    else:
        state_collection = pystac.Collection(
            id=state,
            title=state,
            description=f"STAC Collection for data of {state} state.",
            license="CC-BY-4.0",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([item_bbox]),
                temporal=pystac.TemporalExtent(
                    [[constants.DEFAULT_START_DATE, constants.DEFAULT_END_DATE]]
                ),
            ),
            providers=[
                pystac.Provider(
                    name="CoRE Stack",
                    roles=[
                        pystac.ProviderRole.PRODUCER,
                        pystac.ProviderRole.PROCESSOR,
                        pystac.ProviderRole.HOST,
                        pystac.ProviderRole.LICENSOR,
                    ],
                    url="https://core-stack.org/",
                )
            ],
            keywords=[
                "social-ecological",
                "sustainability",
                "CoRE stack",
                state,
            ],
        )

        state_collection.add_link(
            pystac.Link(
                rel=pystac.RelType.LICENSE,
                target="https://spdx.org/licenses/CC-BY-4.0.html",
                media_type="text/html",
            )
        )

        state_collection.add_link(
            pystac.Link(
                rel="documentation",
                target="https://core-stack.org/",
                title="CoRE stack",
                media_type="application/pdf",
            )
        )

        state_collection.add_link(
            pystac.Link(
                rel="documentation",
                target="https://drive.google.com/file/d/1ZxovdpPThkN09cB1TcUYSE2BImI7M3k_/view",
                title="Technical Manual",
                media_type="application/pdf",
            )
        )

        state_collection.add_link(
            pystac.Link(
                rel="documentation",
                target="https://github.com/orgs/core-stack-org/repositories",
                title="Github link",
                media_type="application/pdf",
            )
        )

        print("created state collection")
        state_collection.add_child(district_catalog)
        state_collection.normalize_and_save(
            state_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
        )

    # 5. check if tehsil level catalog exists
    tehsil_dir = os.path.join(STAC_FILES_DIR, TEHSIL_DIRNAME)
    tehsil_catalog_path = os.path.join(tehsil_dir, "catalog.json")
    if os.path.exists(tehsil_catalog_path):
        tehsil_catalog = pystac.read_file(tehsil_catalog_path)
        print("loaded tehsil catalog")
    else:
        tehsil_catalog = pystac.Catalog(
            id=TEHSIL_DIRNAME,
            title=constants.TEHSIL_CATALOG_TITLE,
            description=constants.TEHSIL_CATALOG_DESCRIPTION,
        )
        print("created tehsil catalog")

    if state_collection.id not in [child.id for child in tehsil_catalog.get_children()]:
        tehsil_catalog.add_child(state_collection)
    tehsil_catalog.normalize_and_save(
        tehsil_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    # 6. create root catalog if not existing
    root_catalog_path = os.path.join(STAC_FILES_DIR, "catalog.json")
    if os.path.exists(root_catalog_path):
        root_catalog = pystac.read_file(root_catalog_path)
        print("loaded root catalog")
    else:
        root_catalog = pystac.Catalog(
            id="corestack_STAC",
            title=constants.ROOT_CATALOG_TITLE,
            description=constants.ROOT_CATALOG_DESCRIPTION,
        )
        print("created root catalog")

    if tehsil_catalog.id not in [child.id for child in root_catalog.get_children()]:
        root_catalog.add_child(tehsil_catalog)

    root_catalog.normalize_and_save(
        STAC_FILES_DIR, catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
    layer_STAC_generated = True
    return layer_STAC_generated


# %% [markdown]
# ### Vector flow


# %%
def generate_vector_url(
    workspace, layer_name, geoserver_base_url, output_format="json"
):
    # wfs_url = (
    #     f"{geoserver_base_url}/{workspace}/ows?"
    #     f"service=WFS&version=1.0.0&request=GetFeature&"
    #     f"typeName={workspace}:{layer_name}&"
    #     f"outputFormat==application/json"
    # )

    wfs_url = (
        f"{geoserver_base_url}/{workspace}/ows?"
        f"service=WFS&version=1.0.0&request=GetFeature&"
        f"typeName={workspace}:{layer_name}&outputFormat=application/json"
    )
    # print("Vector URL:",wfs_url)

    return wfs_url


# %%
def read_vector_data(vector_url, target_crs="4326"):
    try:
        vector_gdf = gpd.read_file(vector_url)
    except requests.exceptions.RequestException as e:
        # Handle specific requests exceptions (e.g., network issues, invalid URL)
        print(
            f"STAC_Error: Network or URL error: {e} when fetching vector data from geoserver"
        )
        print("exiting STAC pipeline")
        return layer_STAC_generated
    except Exception as e:
        # Handle other general GeoPandas/GDAL-related exceptions (e.g., file format issues)
        print(
            f"STAC_Error: unexpected error : {e} when fetching vector data from geoserver"
        )
        print("exiting STAC pipeline")
        return layer_STAC_generated

    vector_gdf = vector_gdf.to_crs(epsg=target_crs)
    # TODO: remove such constants like here in crs. make it standard. available in constants.
    bounds = vector_gdf.total_bounds
    print(bounds)
    bbox = [float(b) for b in bounds]  # footprint also in vector
    print(bbox)
    # geometry=box(*bounds).wkt
    footprint = Polygon(
        [
            [bbox[0], bbox[1]],
            [bbox[0], bbox[3]],
            [bbox[2], bbox[3]],
            [bbox[2], bbox[1]],
            [bbox[0], bbox[1]],
        ]
    )

    # geom = mapping(vector_gdf.union_all())
    geom = mapping(footprint)
    return (vector_gdf, bounds, bbox, geom, footprint)


# %%
def create_vector_item(
    vector_url,
    id,
    layer_title,
    layer_description,
    column_desc_csv_path,
    display_name,
    theme,
):
    try:
        vector_gdf, bounds, bbox, footprint, geom = read_vector_data(
            vector_url=vector_url
        )
    except Exception as e:
        print(f"STAC_Error: {e} when fetching vector data from geoserver")
        print("exiting STAC pipeline")
        return layer_STAC_generated

    start_iso = constants.DEFAULT_START_DATE.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = constants.DEFAULT_END_DATE.strftime("%Y-%m-%dT%H:%M:%SZ")

    vector_item = pystac.Item(
        id=id,
        geometry=footprint,
        bbox=bbox,
        datetime=datetime.datetime.now(datetime.timezone.utc),
        properties={
            "title": layer_title,
            "description": layer_description,
            "start_datetime": start_iso,
            "end_datetime": end_iso,
            "keywords": [theme],
        },
    )

    return vector_item, vector_gdf


# %%
def add_vector_data_asset(vector_item, geoserver_url):

    vector_item.add_asset(
        "data",
        pystac.Asset(
            href=geoserver_url,
            media_type=pystac.MediaType.GEOJSON,
            roles=["data"],
            title="Vector Layer",
        ),
    )

    return vector_item


# %%
def add_tabular_extension(
    vector_item,
    vector_data_gdf,
    column_desc_csv_path,
    ee_layer_name,
    overwrite_existing=False,
):
    if (os.path.exists(column_desc_csv_path)) and (not overwrite_existing):
        vector_column_desc_gdf = pd.read_csv(column_desc_csv_path)
    else:
        print("STAC:downloading column descriptions csv from github")
        os.makedirs(os.path.dirname(column_desc_csv_path), exist_ok=True)
        vector_column_desc_gdf = pd.read_csv(VECTOR_COLUMN_DESC_GITHUB_URL)
        vector_column_desc_gdf.to_csv(column_desc_csv_path)

    vector_column_desc_filtered_gdf = vector_column_desc_gdf[
        vector_column_desc_gdf["ee_layer_name"] == ee_layer_name
    ]
    vector_column_desc_filtered_gdf.rename(
        {"column_name_description": "column_description"}, axis=1, inplace=True
    )
    table_ext = pystac.extensions.table.TableExtension.ext(
        vector_item, add_if_missing=True
    )
    vector_merged_df = vector_data_gdf.dtypes.reset_index()
    vector_merged_df.columns = ["column_name", "column_dtype"]
    vector_merged_df = vector_merged_df.merge(
        vector_column_desc_filtered_gdf[["column_name", "column_description"]],
        on="column_name",
        how="left",
    ).fillna("")

    table_ext.columns = [
        {
            "name": row["column_name"],
            "type": str(row["column_dtype"]),
            "description": row["column_description"],
        }
        for ind, row in vector_merged_df.iterrows()
    ]

    return vector_item


# %%
# helper functions to generate vector thumbnail
def rgba_to_hex(rgba_tuple):
    if rgba_tuple is None:
        return "#808080"  # Default gray
    r, g, b, a = rgba_tuple
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


def extract_styling_info(symbol_element):
    fill_color = None
    outline_color = None
    line_width = None

    if symbol_element is None:
        return fill_color, outline_color, line_width

    fill_layer = symbol_element.find('.//layer[@class="SimpleFill"]')
    if fill_layer is not None:
        color_option = fill_layer.find('Option[@name="color"]')
        if color_option is not None:
            try:
                rgb_parts = [int(p) for p in color_option.get("value").split(",")[:3]]
                fill_color = tuple([p / 255 for p in rgb_parts])
            except (ValueError, TypeError):
                fill_color = None

        outline_option = fill_layer.find('Option[@name="outline_color"]')
        if outline_option is not None:
            try:
                rgb_parts = [int(p) for p in outline_option.get("value").split(",")[:3]]
                outline_color = tuple([p / 255 for p in rgb_parts])
            except (ValueError, TypeError):
                outline_color = None

        width_option = fill_layer.find('Option[@name="outline_width"]')
        if width_option is not None:
            try:
                line_width = float(width_option.get("value"))
            except (ValueError, TypeError):
                line_width = None

    line_layer = symbol_element.find('.//layer[@class="SimpleLine"]')
    if line_layer is not None:
        color_option = line_layer.find('Option[@name="line_color"]')
        if color_option is not None:
            try:
                rgb_parts = [int(p) for p in color_option.get("value").split(",")[:3]]
                outline_color = tuple([p / 255 for p in rgb_parts])
            except (ValueError, TypeError):
                outline_color = None

        width_option = line_layer.find('Option[@name="line_width"]')
        if width_option is not None:
            try:
                line_width = float(width_option.get("value"))
            except (ValueError, TypeError):
                line_width = None

    return fill_color, outline_color, line_width


def parse_vector_style_file(style_file_url, STYLE_FILE_DIR):
    # download style file if not already downloaded, and save it locally
    style_file_name = os.path.basename(style_file_url)
    style_file_local_path = os.path.join(STYLE_FILE_DIR, style_file_name)

    if not os.path.exists(style_file_local_path):
        # TODO: try statement
        os.makedirs(os.path.dirname(style_file_local_path), exist_ok=True)
        try:
            urllib.request.urlretrieve(style_file_url, style_file_local_path)
        except Exception as e:
            print("Could not retrieve style file from url. Error: " + str(e))

    try:
        tree = ET.parse(style_file_local_path)
        root = tree.getroot()
        renderer_element = root.find(".//renderer-v2")

        if renderer_element is None:
            print("No renderer-v2 element found.")
            return None

        renderer_type = renderer_element.get("type")
        style = {"renderer_type": renderer_type}
        symbols = {s.get("name"): s for s in root.findall(".//symbols/symbol")}

        if renderer_type == "singleSymbol":
            symbol_element = renderer_element.find(".//symbol") or symbols.get(
                renderer_element.get("symbol")
            )
            if symbol_element is not None:

                color_option = symbol_element.find(
                    './/layer/Option[@name="line_color"]'
                ) or symbol_element.find('.//layer/Option[@name="color"]')

                if color_option is not None:
                    color_value = color_option.get("value").split(",")[0:3]
                    rgb_parts = [int(p) for p in color_value]
                    style["color"] = (
                        rgb_parts[0] / 255,
                        rgb_parts[1] / 255,
                        rgb_parts[2] / 255,
                    )
                else:

                    color_prop = symbol_element.find('.//prop[@k="color"]')
                    if color_prop is not None:
                        rgb_parts = [int(p) for p in color_prop.get("v").split(",")[:3]]
                        style["color"] = (
                            rgb_parts[0] / 255,
                            rgb_parts[1] / 255,
                            rgb_parts[2] / 255,
                        )
                    else:
                        print(
                            f"Warning: Single symbol color not found in {style_file_local_path}."
                        )
                        return None
            else:
                print(
                    f"Warning: Could not find symbol element for singleSymbol in {style_file_local_path}."
                )
                return None

        elif renderer_type == "categorizedSymbol":
            style["attribute"] = renderer_element.get("attr")
            style["categories"] = []
            for cat in renderer_element.findall("categories/category"):
                symbol_element = cat.find("symbol") or symbols.get(cat.get("symbol"))
                fill_color, outline_color, line_width = extract_styling_info(
                    symbol_element
                )
                style["categories"].append(
                    {
                        "value": cat.get("value"),
                        "label": cat.get("label"),
                        "fill_color": fill_color,
                        "outline_color": outline_color,
                        "line_width": line_width,
                    }
                )

        elif renderer_type == "graduatedSymbol":
            style["attribute"] = renderer_element.get("attr")
            style["classes"] = []
            for cls in renderer_element.findall("classes/class"):
                symbol_element = cls.find("symbol") or symbols.get(cls.get("symbol"))
                fill_color, outline_color, line_width = extract_styling_info(
                    symbol_element
                )
                style["classes"].append(
                    {
                        "lower_bound": float(cls.get("lower")),
                        "upper_bound": float(cls.get("upper")),
                        "label": cls.get("label"),
                        "fill_color": fill_color,
                        "outline_color": outline_color,
                        "line_width": line_width,
                    }
                )

        elif renderer_type == "RuleRenderer":
            style["rules"] = []
            for rule in renderer_element.findall(".//rule"):
                symbol_element = rule.find(".//symbol")
                fill_color, outline_color, line_width = extract_styling_info(
                    symbol_element
                )
                style["rules"].append(
                    {
                        "filter": rule.get("filter"),
                        "label": rule.get("label"),
                        "fill_color": fill_color,
                        "outline_color": outline_color,
                        "line_width": line_width,
                    }
                )
        else:
            print(
                f"Warning: Unsupported renderer type '{renderer_type}'. Using default style."
            )
            return None
        return style
    except Exception as e:
        print(f"Error parsing QML file {style_file_local_path}: {e}")
        return None


# %%
def generate_vector_thumbnail(vector_gdf, style_file_url, output_path, STYLE_FILE_DIR):

    try:
        # vector_gdf = gpd.read_file(vector_path)
        style_info = parse_vector_style_file(style_file_url, STYLE_FILE_DIR)

        print("style_info=", style_info)  # TODO: temporary debug print
        fig, ax = plt.subplots(figsize=(6, 6))

        default_fill_color = (0.8, 0.8, 0.8, 1.0)  # Light gray
        default_outline_color = (0, 0, 0, 1.0)  # Black
        default_line_width = 1.0

        if style_info is None:
            print("Applying default style due to parsing error.")
            vector_gdf.plot(
                ax=ax,
                color=rgba_to_hex(default_fill_color),
                edgecolor=rgba_to_hex(default_outline_color),
                linewidth=default_line_width,
            )

        elif style_info.get("renderer_type") == "singleSymbol":
            print("Applying single symbol style...")
            fill_color = style_info.get("fill_color", default_fill_color)
            outline_color = style_info.get("outline_color", default_outline_color)
            line_width = style_info.get("line_width", default_line_width)
            vector_gdf.plot(
                ax=ax,
                color=rgba_to_hex(fill_color),
                edgecolor=rgba_to_hex(outline_color),
                linewidth=line_width,
            )

        elif style_info.get("renderer_type") == "categorizedSymbol":
            print("Applying categorized style...")

            color_map = {
                cat.get("value"): rgba_to_hex(
                    cat.get("fill_color" or default_fill_color)
                )
                for cat in style_info.get("categories", [])
            }

            outline_color_map = {
                cat.get("value"): rgba_to_hex(
                    cat.get("outline_color" or default_outline_color)
                )
                for cat in style_info.get("categories", [])
            }

            attribute_name = style_info.get("attribute")
            print("attribute_name = ", attribute_name)  # TODO: temporary debug print

            if attribute_name not in vector_gdf.columns:
                print(
                    f"Error: Attribute column '{attribute_name}' not found. Applying default style."
                )
                vector_gdf.plot(
                    ax=ax,
                    color=rgba_to_hex(default_fill_color),
                    edgecolor=rgba_to_hex(default_outline_color),
                    linewidth=default_line_width,
                )
            else:
                vector_gdf["mapped_value"] = vector_gdf[attribute_name].apply(
                    lambda x: str(x).strip() if pd.notnull(x) else None
                )

                fill_colors = vector_gdf["mapped_value"].map(color_map)
                fill_colors = fill_colors.fillna(rgba_to_hex(default_fill_color))

                outline_colors = vector_gdf["mapped_value"].map(outline_color_map)
                outline_colors = outline_colors.fillna(
                    rgba_to_hex(default_outline_color)
                )

                print("fill_colors=", fill_colors)  # TODO : debug print

                vector_gdf.plot(
                    ax=ax,
                    color=fill_colors,
                    edgecolor=outline_colors,
                    linewidth=default_line_width,
                )

        elif style_info.get("renderer_type") == "graduatedSymbol":
            print("Applying graduated style...")
            attribute_name = style_info.get("attribute")
            if attribute_name not in vector_gdf.columns:
                print(
                    f"Error: Attribute column '{attribute_name}' not found. Applying default style."
                )
                vector_gdf.plot(
                    ax=ax,
                    color=rgba_to_hex(default_fill_color),
                    edgecolor=rgba_to_hex(default_outline_color),
                    linewidth=default_line_width,
                )
            else:
                fill_colors = []
                for _, row in vector_gdf.iterrows():
                    val = row[attribute_name]
                    found_color = default_fill_color
                    for cls in style_info.get("classes", []):
                        if (
                            cls.get("lower_bound") is not None
                            and cls.get("upper_bound") is not None
                        ):
                            if cls["lower_bound"] <= val < cls["upper_bound"]:
                                found_color = cls.get("fill_color", default_fill_color)
                                break
                    fill_colors.append(rgba_to_hex(found_color))

                vector_gdf.plot(
                    ax=ax,
                    color=fill_colors,
                    edgecolor=rgba_to_hex(default_outline_color),
                    linewidth=default_line_width,
                )

        elif style_info.get("renderer_type") == "RuleRenderer":
            print("Applying rule-based style...")
            fill_colors = []
            for _, row in vector_gdf.iterrows():
                assigned_color = default_fill_color
                for rule in style_info.get("rules", []):
                    try:
                        attribute_name = (
                            rule["filter"].split(" ")[0].strip().strip('"').strip("'")
                        )
                        if attribute_name in row and pd.eval(
                            rule["filter"],
                            local_dict={attribute_name: row[attribute_name]},
                        ):
                            assigned_color = rule.get("fill_color", default_fill_color)
                            break
                    except Exception:
                        continue
                fill_colors.append(rgba_to_hex(assigned_color))

            vector_gdf.plot(
                ax=ax,
                color=fill_colors,
                edgecolor=rgba_to_hex(default_outline_color),
                linewidth=default_line_width,
            )

        else:
            print("Applying default blue style.")
            vector_gdf.plot(
                ax=ax,
                color="lightblue",
                edgecolor=rgba_to_hex(default_outline_color),
                linewidth=default_line_width,
            )

        ax.set_axis_off()
        plt.tight_layout()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path)
        plt.close(fig)
        print(f"Thumbnail saved to: {output_path}")

    except Exception as e:
        print(f"Error generating vector thumbnail: {e}")


# %%
def generate_vector_item(
    state,
    district,
    block,
    layer_name,
    layer_map_csv_path,
    layer_desc_csv_path,
    column_desc_csv_path,
    overwrite_existing,
):
    # print(layer_map_csv_path)
    # print(layer_desc_csv_path)
    # print(column_desc_csv_path)
    # 1. read layer description
    layer_description = read_layer_description(
        filepath=layer_desc_csv_path,
        layer_name=layer_name,
        overwrite_existing=overwrite_existing,
    )

    # 2. get geoserver url parameters from the layer details
    (
        geoserver_workspace_name,
        geoserver_layer_name,
        style_file_url,
        layer_display_name,
        ee_layer_name,
        gsd,
        theme,
    ) = read_layer_mapping(
        layer_map_csv_path=layer_map_csv_path,
        district=district,
        block=block,
        layer_name=layer_name,
        #    start_year=start_year,
        #    end_year=end_year
    )

    # print(f"geoserver_workspace_name={geoserver_workspace_name}")
    # print(f"geoserver_layer_name={geoserver_layer_name}")
    # print(f"style file url = {style_file_url}")

    # 3. generate geoserver url
    geoserver_url = generate_vector_url(
        workspace=geoserver_workspace_name,
        layer_name=geoserver_layer_name,
        geoserver_base_url=GEOSERVER_BASE_URL,
    )
    # print(f"geoserver url={geoserver_url}")

    # 4. create vector item
    layer_title = layer_display_name
    layer_id = f"{state}_{district}_{block}_{layer_name}"

    vector_item, vector_data_gdf = create_vector_item(
        geoserver_url,
        id=layer_id,
        layer_title=layer_title,
        layer_description=layer_description,
        column_desc_csv_path=column_desc_csv_path,
        display_name=layer_display_name,
        theme=theme,
    )
    # 5. add vector data asset
    vector_item = add_vector_data_asset(vector_item, geoserver_url=geoserver_url)

    # 6. add table extension
    vector_item = add_tabular_extension(
        vector_item=vector_item,
        vector_data_gdf=vector_data_gdf,
        column_desc_csv_path=column_desc_csv_path,
        ee_layer_name=ee_layer_name,
        overwrite_existing=overwrite_existing,
    )

    # 7. add style file asset
    add_stylefile_asset(STAC_item=vector_item, style_file_url=style_file_url)

    # start from here : TODO
    # 8. generate thumbnail
    # if (start_year != ''):
    #     thumbnail_filename = f'{block}_{layer_name}_{start_year}.png'
    # else:
    thumbnail_filename = f"{state}_{district}_{block}_{layer_name}.png"  # TODO:
    THUMBNAIL_PATH = os.path.join(THUMBNAIL_DIR, thumbnail_filename)

    generate_vector_thumbnail(
        vector_gdf=vector_data_gdf,
        style_file_url=style_file_url,
        output_path=THUMBNAIL_PATH,
        STYLE_FILE_DIR=STYLE_FILE_DIR,
    )

    # 9. add thumbnail asset
    vector_item = add_thumbnail_asset(
        STAC_item=vector_item,
        THUMBNAIL_PATH=THUMBNAIL_PATH,
        LOCAL_DATA_DIR=LOCAL_DATA_DIR,
        THUMBNAIL_DATA_URL=THUMBNAIL_DATA_URL,
    )

    return vector_item


# %% [markdown]
# Upload folders to S3


# %%
def create_aws_client(
    service_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = "ap-south-1",
    aws_session_token=None,
):
    return boto3.client(
        service_name=service_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        aws_session_token=aws_session_token,
    )


# %%
# AWS_CREDS_FILEPATH = constants.AWS_CREDS_FILEPATH

# %%
# with open(AWS_CREDS_FILEPATH) as src:
#     aws_creds = json.load(src)


# %%
def upload_file_to_s3(
    aws_creds,
    filepath,
    s3_bucket,
    s3_prefix,
):
    if s3_bucket is None:
        raise Exception("'s3_folderpath'" + "should be non None.")

    s3_client = create_aws_client(
        service_name="s3",
        aws_access_key_id=aws_creds["aws_access_key_id"],
        aws_secret_access_key=aws_creds["aws_secret_access_key"],
        region_name="ap-south-1",
        # aws_session_token=aws_creds.aws_session_token,
    )

    # print('---------------------')
    # print('File set to upload:')
    # print('---------------------')
    # print('filepath :', filepath)
    # print('bucket   :', s3_bucket)
    # print('prefix   :', s3_prefix)
    # print('---------------------')

    s3_client.upload_file(
        filepath,
        s3_bucket,
        s3_prefix,
        ExtraArgs={"ACL": "bucket-owner-full-control"},
    )

    return (s3_bucket, s3_prefix)


# %%
def get_all_files_in_folder(folderpath: str):
    # https://stackoverflow.com/questions/18394147/how-to-do-a-recursive-sub-folder-search-and-return-files-in-a-list
    return [
        y
        for x in os.walk(folderpath)
        for y in glob.glob(os.path.join(x[0], "*"))
        if os.path.isfile(y)
    ]


# %%
def upload_folder_to_s3(aws_creds, folderpath, s3_bucket):
    filepaths = get_all_files_in_folder(folderpath=folderpath)
    for filepath in tqdm.tqdm(filepaths):
        s3_prefix = filepath.split("data/STAC_specs/")[1]
        upload_file_to_s3(
            aws_creds=aws_creds,
            filepath=filepath,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
        )


# %% [markdown]
# Making a combined function to generate raster/vector STAC :
# The function generates the item, and updates the files


# %%
def generate_vector_stac(
    state,
    district,
    block,
    layer_name,
    # layer_map_csv_path='data/input/metadata/layer_mapping.csv',
    # layer_desc_csv_path='data/input/metadata/layer_descriptions.csv',
    # column_desc_csv_path='data/input/metadata/vector_column_descriptions.csv',
    layer_map_csv_path="data/STAC_specs/input/metadata/layer_mapping.csv",
    layer_desc_csv_path="data/STAC_specs/input/metadata/layer_descriptions.csv",
    column_desc_csv_path="data/STAC_specs/input/metadata/vector_column_descriptions.csv",
    upload_to_s3=False,
    overwrite_existing=False,
):
    # print(layer_map_csv_path)
    state = valid_gee_text(state.lower())
    district = valid_gee_text(district.lower())
    block = valid_gee_text(block.lower())

    # print("state=",state)
    # print("district=",district)
    # print("block=",block)

    vector_item = generate_vector_item(
        state,
        district,
        block,
        layer_name,
        layer_map_csv_path,
        layer_desc_csv_path,
        column_desc_csv_path,
        overwrite_existing,
    )

    layer_STAC_generated = update_STAC_files(
        state, district, block, STAC_item=vector_item
    )

    if upload_to_s3:
        upload_folder_to_s3(
            aws_creds=aws_creds,
            folderpath=STAC_FILES_DIR,
            s3_bucket=S3_STAC_BUCKET_NAME,
        )

        upload_folder_to_s3(
            aws_creds=aws_creds, folderpath=THUMBNAIL_DIR, s3_bucket=S3_STAC_BUCKET_NAME
        )

    return layer_STAC_generated


# %%
def generate_raster_stac(
    state,
    district,
    block,
    layer_name,
    # layer_map_csv_path='data/input/metadata/layer_mapping.csv',
    # layer_desc_csv_path='data/input/metadata/layer_descriptions.csv',
    layer_map_csv_path="data/STAC_specs/input/metadata/layer_mapping.csv",
    layer_desc_csv_path="data/STAC_specs/input/metadata/layer_descriptions.csv",
    start_year="",
    #  end_year='',
    upload_to_s3=False,
    overwrite_existing=False,
):
    state = valid_gee_text(state.lower())
    district = valid_gee_text(district.lower())
    block = valid_gee_text(block.lower())

    # print("state=",state)
    # print("district=",district)
    # print("block=",block)

    raster_item = generate_raster_item(
        state,
        district,
        block,
        layer_name,
        layer_map_csv_path,
        layer_desc_csv_path,
        start_year,
        #    end_year,
        overwrite_existing,
    )

    layer_STAC_generated = update_STAC_files(
        state, district, block, STAC_item=raster_item
    )

    if upload_to_s3:
        upload_folder_to_s3(
            aws_creds=aws_creds,
            folderpath=STAC_FILES_DIR,
            s3_bucket=S3_STAC_BUCKET_NAME,
        )

        upload_folder_to_s3(
            aws_creds=aws_creds, folderpath=THUMBNAIL_DIR, s3_bucket=S3_STAC_BUCKET_NAME
        )

    return layer_STAC_generated


# %% [markdown]
# Test the raster and vector flow

# %%
# block_district_state_df = pd.DataFrame({
#     'block' : ['gobindpur','mirzapur','koraput','badlapur','hilsa','masalia'],
#     'district' : ['saraikela-kharsawan','mirzapur','koraput','jaunpur','nalanda','dumka'],
#     'state' : ['jharkhand','uttar_pradesh','odisha','uttar_pradesh','bihar','jharkhand']
# })

# block_district_state_df

# %%
# block = 'masalia'
# district = block_district_state_df[block_district_state_df['block'] == block]['district'].iloc[0]
# state = block_district_state_df[block_district_state_df['block'] == block]['state'].iloc[0]
# print(state,district,block)

# %%
# block = 'kharsawan'
# state = 'jharkhand'
# district = 'saraikela-kharsawan'

# %%
# block='Giridih'
# state='Jharkhand'
# district='Giridih'


# %% [markdown]
# <!-- Aquifer:
#
# 1. Style file has issue : fill color against the categorized column name is none.
#
# 2. Some data layers are different than others : 2 data layers instead of 1 (masalia vs others (e.g. koraput))  -->

# %%
# generate_vector_stac(state=state,
#                      district=district,
#                      block=block,
#                      layer_name='nrega_vector',
#                      upload_to_s3=False,
#                      overwrite_existing=False
#                     #  layer_map_csv_path='../data/input/metadata/layer_mapping.csv',
#                     #  layer_desc_csv_path='../data/input/metadata/layer_descriptions.csv',
#                     #  column_desc_csv_path='../data/input/metadata/vector_column_descriptions.csv'
#                     )



# %%
# block = 'badlapur'
# district = block_district_state_df[block_district_state_df['block'] == block]['district'].iloc[0]
# state = block_district_state_df[block_district_state_df['block'] == block]['state'].iloc[0]
# print(state,district,block)

# %%
# state='uttar_pradesh'
# district='jaunpur'
# block='badlapur'

# %%
# state='gujarat'
# district='mahisagar'
# block='virpur'

# %%
# state='Uttar Pradesh'
# district='Jaunpur'
# block='Badlapur'

# %%
# generate_vector_stac(state=state,
#                      district=district,
#                      block=block,
#                      layer_name='nrega_vector',
#                      upload_to_s3=False,
#                      overwrite_existing=True
#                     #  layer_map_csv_path='../data/input/metadata/layer_mapping.csv',
#                     #  layer_desc_csv_path='../data/input/metadata/layer_descriptions.csv',
#                     #  column_desc_csv_path='../data/input/metadata/vector_column_descriptions.csv'
#                     )

# %%
# generate_raster_stac(state=state,
#                      district=district,
#                      block=block,
#                      layer_name='change_tree_cover_gain_raster',
#                     #  layer_map_csv_path='../data/input/metadata/layer_mapping.csv',
#                     #  layer_desc_csv_path='../data/input/metadata/layer_descriptions.csv',
#                      start_year='2021'
#                      )

# %%
# generate_vector_stac(state=state,
#                      district=district,
#                      block=block,
#                      layer_name='aquifer_vector',
#                      # column_desc_csv_path='../data/input/metadata/vector_column_descriptions.csv',
#                      # layer_map_csv_path='../data/input/metadata/layer_mapping.csv',
#                      # layer_desc_csv_path='../data/input/metadata/layer_descriptions.csv',
#                  )

# %%
# generate_raster_stac(state=state,
#                      district=district,
#                      block=block,
#                      layer_name='tree_canopy_height_raster',
#                     #  layer_map_csv_path='../data/input/metadata/layer_mapping.csv',
#                     #  layer_desc_csv_path='../data/input/metadata/layer_descriptions.csv',
#                      start_year='2021'
#                      )