import time

import ee
import geopandas as gpd


def ee_config():
    service_account = "nrm-water@df-project-iit.iam.gserviceaccount.com"
    credentials = ee.ServiceAccountCredentials(
        service_account, "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/gcloud-private-key.json"
    )
    ee.Initialize(credentials)

def gdf_to_ee_fc(gdf):
    features = []
    for i, row in gdf.iterrows():
        properties = row.drop('geometry').to_dict()
        geometry = ee.Geometry(row.geometry.__geo_interface__)
        feature = ee.Feature(geometry, properties)
        features.append(feature)
    return ee.FeatureCollection(features)

if __name__ == "__main__":
    ee_config()
    
    geojson_file_path = "/home/ankit/gramvaani/nrm/checkin/backend/fromgitlab/nrm-app/data/layer/rdf_revised_pcraster_angul.geojson"
    gdf = gpd.read_file(geojson_file_path).to_crs('EPSG:4326')
    
    ee_fc = gdf_to_ee_fc(gdf)

    asset_id = 'projects/df-project-iit/assets/test_mws/pcraster_mws_angul_1'
    asset_properties = {
        'system:description': 'GeoJSON asset',
        'system:provider_url': '',
        'system:tags': ['geojson'],
        'system:title': 'GeoJSON Asset'
    }
    
    task = ee.batch.Export.table.toAsset(
        collection=ee_fc,
        description='gs_to_ee_fc',
        assetId=asset_id,
        properties=asset_properties
    )
    
    task.start()
    
    while task.active():
        print(f'Task status: {task.status()}')
        time.sleep(60)

    print(f'Task completed: {task.status()}')