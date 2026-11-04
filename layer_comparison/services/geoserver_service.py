from django.conf import settings

from .gee_style_service import get_geoserver_sld


def get_geoserver_wms_config(layer):
    base = getattr(settings, "GEOSERVER_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("Configure GEOSERVER_URL.")

    workspace = layer.get("workspace")
    layer_name = layer.get("geoserver_layer")
    if not workspace or not layer_name:
        raise ValueError("GeoServer workspace/layer mapping is missing.")

    params = {
        "layers": f"{workspace}:{layer_name}",
        "styles": layer.get("style") or "",
        "format": "image/png",
        "transparent": True,
        "version": "1.1.1",
    }
    inline_sld = get_geoserver_sld(
        layer.get("gee_style_url", ""),
        f"{workspace}:{layer_name}",
        layer.get("layer_type", ""),
    )
    if inline_sld:
        params["styles"] = ""
        params["SLD_BODY"] = inline_sld

    return {
        "url": f"{base}/{workspace}/wms",
        "params": params,
    }
