from django.shortcuts import render
from .models import LayerInfo
from .utils import get_url
from nrm_app.settings import GEOSERVER_URL

# Create your views here.

def raster_tiff_download_url(workspace, layer_name):
    geotiff_url = f"{GEOSERVER_URL}/{workspace}/wcs?service=WCS&version=2.0.1&request=GetCoverage&CoverageId={workspace}:{layer_name}&format=geotiff&compression=LZW&tiling=true&tileheight=256&tilewidth=256"
    print("Geojson url",  geotiff_url)
    return geotiff_url


def fetch_generated_layer_urls(district, block):
    """
    Fetch all vector and raster layers and return their metadata as JSON.
    """
    all_layers = LayerInfo.objects.all()
    layer_data = []

    for layer in all_layers:
        workspace = layer.workspace
        layer_type = layer.layer_type
        layer_desc = layer.layer_desc
        style_name = layer.style_name
        layer_name = layer.layer_name.format(district=district, block=block)

        if layer_type == "vector":
            layer_url = get_url(workspace, layer_name)
        elif layer_type == "raster":
            layer_url = raster_tiff_download_url(workspace, layer_name)
        else:
            continue  # skip unknown layer types

        layer_data.append({
            "layer_desc": layer_desc,
            "layer_type": layer_type,
            "layer_url": layer_url,
            "style_name": style_name
        })

    return layer_data


    
