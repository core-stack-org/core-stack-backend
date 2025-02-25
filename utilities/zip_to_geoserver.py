import os
from .geoserver_utils import Geoserver
from computing.utils import push_shape_to_geoserver

geo = Geoserver()

folder_path = "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/zip-shapefiles/drainage"
workspace = "drainage"

for folder_name in os.listdir(folder_path):
    shape_path = os.path.join(folder_path, folder_name)
    print(shape_path)
    push_shape_to_geoserver(shape_path=shape_path, workspace=workspace)
