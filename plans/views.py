# from django.shortcuts import render
# from rest_framework.response import Response
# from rest_framework.decorators import api_view
# from .utils import generate_output_dir,fetch_odk_result, build_shape_file_hemlet, build_shape_file_well, build_shape_file_wb, build_shape_file_plan
# from utilities.geoserver_utils import Geoserver
# import shutil
# import geopandas, subprocess
# import time
# import os
# import random

# Create your views here.
# @api_view(['GET'])
# def updateHemletLayer(request):
#     geo = Geoserver('http://geoserver.gramvaani.org:8080/geoserver', username='', password='')
#     block_name = request.query_params.get('block_name')
#     layer_name = request.query_params.get('layer_name')
#     layer_type = request.query_params.get('layer_type', None)
#     print (layer_type)
#     print (layer_name)
#     csv_path='/tmp/'+str(layer_name)+str(block_name)+'.csv'

#     zip_dir, output_geojson, output_shapefile = generate_output_dir(block_name, layer_name)
#     print (output_geojson)

#     odk_result = fetch_odk_result(layer_name, csv_path, layer_type)

#     if layer_name=="hemlet_layer":
#         build_shape_file_hemlet(odk_result, block_name, output_geojson,csv_path, output_shapefile)
#     if layer_name=="well_layer":
#         build_shape_file_well(odk_result, block_name, output_geojson,csv_path, output_shapefile)
#     if layer_name=="wb_layer":
#         build_shape_file_wb(odk_result, block_name, output_geojson,csv_path, output_shapefile)
#     if layer_name in ["plan_layer_wb", "plan_layer_agri", "plan_layer_gw"]:
#         print ('inside plan loop')
#         build_shape_file_plan(odk_result, block_name, output_geojson,csv_path, output_shapefile, layer_type)

#     print (output_geojson)

#     gdf = geopandas.read_file(output_geojson)
#     args = ['ogr2ogr', '-f', 'ESRI Shapefile', output_shapefile, output_geojson]
#     command = 'ogr2ogr -f ESRI Shapefile ' + output_shapefile +' ' + output_geojson
#     p = subprocess.Popen(args)
#     p.wait()

#     print ("zip dir")
#     print (zip_dir)
#     zip_dir_shp = 'assets/'+str(block_name)+'_'+str(layer_name)
#     shutil.make_archive(zip_dir, 'zip', zip_dir+'/')

#     print (zip_dir)
#     geo.create_shp_datastore(path=zip_dir+'.zip')
#     #shutil.rmtree(zip_dir_shp)
#     #os.remove(zip_dir_shp+'.zip')


#     return Response()
