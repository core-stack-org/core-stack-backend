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
    "URL_360": "https://waba-v2.360dialog.io/"
}

ES_HEADERS = {
    "Content-type": "application/json; charset=utf-8",
    "Authorization": f"Basic {ES_AUTH}",
}

CALL_PATCH_HEADER = {"Authorization": f"ApiKey {CALL_PATCH_API_KEY}"}


def get_bsp_url_headers(bot_instance_id):
    bot_instance_config = bot_interface.models.Bot.objects.get(
        pk=bot_instance_id
    )

    # Handle both string and dict types for config_json
    if isinstance(bot_instance_config.config_json, str):
        bot_config_json = json.loads(bot_instance_config.config_json)
    elif isinstance(bot_instance_config.config_json, dict):
        bot_config_json = bot_instance_config.config_json
    else:
        raise ValueError(f"Unexpected config_json type: {type(bot_instance_config.config_json)}")

    bsp_url = bot_config_json.get("bsp_url")
    headers = bot_config_json.get("headers")
    namespace = bot_config_json.get("namespace")

    # Add error handling for missing keys
    if not bsp_url:
        raise ValueError(f"Missing 'bsp_url' in bot config. Available keys: {list(bot_config_json.keys())}")
    if not headers:
        raise ValueError(f"Missing 'headers' in bot config. Available keys: {list(bot_config_json.keys())}")

    if bsp_url not in BSP_URLS:
        raise ValueError(f"Invalid bsp_url '{bsp_url}'. Available options: {list(BSP_URLS.keys())}")
    if headers not in BSP_Headers:
        raise ValueError(f"Invalid headers '{headers}'. Available options: {list(BSP_Headers.keys())}")

    BSP_URL = BSP_URLS[bsp_url]
    HEADERS = BSP_Headers[headers]
    return BSP_URL, HEADERS, namespace
