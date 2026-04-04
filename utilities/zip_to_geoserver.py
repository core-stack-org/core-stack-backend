import os
from .geoserver_utils import Geoserver
from computing.utils import push_shape_to_geoserver
from utilities.constants import zip_to_geoserver_folder_path

geo = Geoserver()

workspace = "drainage"

for folder_name in os.listdir(zip_to_geoserver_folder_path):
    shape_path = os.path.join(zip_to_geoserver_folder_path, folder_name)
    print(shape_path)
    push_shape_to_geoserver(shape_path=shape_path, workspace=workspace)
