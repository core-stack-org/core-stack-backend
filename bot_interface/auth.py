import bot_interface.models
import json
from nrm_app.settings import (
    AUTH_TOKEN_360,
    ES_AUTH,
    CALL_PATCH_API_KEY
)


BSP_Headers = {
    "HEADERS_360": {
        "D360-API-KEY": f"{AUTH_TOKEN_360}",
        "Content-Type": "application/json",
    }
}

BSP_URLS = {
    "URL_360": "https://waba-v2.360dialog.io/v1/"
}

ES_HEADERS = {
    "Content-type": "application/json; charset=utf-8",
    "Authorization": "Basic " + ES_AUTH,
}

CALL_PATCH_HEADER = {"Authorization": "ApiKey " + CALL_PATCH_API_KEY}


def get_bsp_url_headers(bot_instance_id):
    bot_instance_config = bot_interface.models.Bot.objects.get(
        pk=bot_instance_id
    )
    bot_config_json = json.loads(bot_instance_config.config_json)
    bsp_url = bot_config_json.get("bsp_url")
    headers = bot_config_json.get("headers")
    namespace = bot_config_json.get("namespace")
    BSP_URL = BSP_URLS[bsp_url]
    HEADERS = BSP_Headers[headers]
    return BSP_URL, HEADERS, namespace
