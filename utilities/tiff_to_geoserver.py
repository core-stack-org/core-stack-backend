"""
This script is responsible for pushing Tiff files from data/tiff/folder to GeoServer
"""
import os
import shutil
from .geoserver_utils import Geoserver

def upload_to_geoserver(file_path, workspace, style_name):
    geo = Geoserver()
    layer_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Uploading: {layer_name} to workspace: {workspace} with style: {style_name}")
    
    geo.create_coveragestore(path=file_path, workspace=workspace, layer_name=layer_name)
    geo.publish_style(layer_name=layer_name, style_name=style_name, workspace=workspace)
    
    print(f"Layer published: {layer_name}\n")

def lulc(folder_path):
    temp_folder = os.path.join(folder_path, "temp")
    os.makedirs(temp_folder, exist_ok=True)

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".tif"):
            original_path = os.path.join(folder_path, file_name)
            base_name = file_name.replace("_resolution.tif", "")

            # Level 1
            level_1_name = f"{base_name}                        .tif"
            level_1_path = os.path.join(temp_folder, level_1_name)
            shutil.copy(original_path, level_1_path)
            upload_to_geoserver(level_1_path, "LULC_level_1", "lulc_level_1_style")

            # Level 2
            level_2_name = f"{base_name}_level_2.tif"
            level_2_path = os.path.join(temp_folder, level_2_name)
            shutil.copy(original_path, level_2_path)
            upload_to_geoserver(level_2_path, "LULC_level_2", "lulc_level_2_style")
            
            # Level 3
            level_3_name = f"{base_name}_level_3.tif"
            level_3_path = os.path.join(temp_folder, level_3_name)
            shutil.copy(original_path, level_3_path)
            upload_to_geoserver(level_3_path, "LULC_level_3", "lulc_level_3_style")

    # Clean up temporary files
    shutil.rmtree(temp_folder)
    
    
folder_path = "/home/ankit/gramvaani/nrm/checkin/backend/nrm-app/data/lulc/LULC_devdurga_3oct2024"
lulc(folder_path)