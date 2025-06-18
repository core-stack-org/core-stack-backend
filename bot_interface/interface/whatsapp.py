from django.db import models

import json
import bot_interface.models
import bot_interface.interface.generic
import bot_interface.utils
import bot_interface.api
from django.utils import timezone

class WhatsAppInterface(bot_interface.interface.generic.GenericInterface):
    # functions through which we are posting something to user, it should be interface fuction
    @staticmethod
    def create_event_packet(json_obj, bot_id, event="start"):
        bot_instance = bot_interface.models.Bot.objects.get(
            id=bot_id
        )
        event_packet = {}
        data = ""
        media_id = ""
        wa_id = ""
        misc = ""
        timestamp = data_type = message_id = ""
        json_obj = json.loads(json_obj)
        print("Json Obj in create_event_packet", json_obj)
        if "contacts" in json_obj:
            print("Json Obj in create_event_packet in contacts", json_obj)
            contacts = json_obj["contacts"][0]
            wa_id = contacts["wa_id"] if "wa_id" in contacts else ""
            event_packet["msisdn"] = wa_id
            data_type = (
                json_obj["messages"][0]["type"] if "messages" in json_obj else ""
            )
            if data_type:
                if data_type == "text":
                    event_packet["type"] = data_type
                    data = json_obj["messages"][0]["text"]["body"]
                elif data_type == "order":
                    event_packet["type"] = data_type
                    data = json_obj["messages"][0]["order"]["product_items"]
                elif data_type == "interactive":
                    event_packet["type"] = "button"
                    data = (
                        json_obj["messages"][0]["interactive"]["list_reply"]["title"]
                        if json_obj["messages"][0]["interactive"].get("list_reply")
                        else json_obj["messages"][0]["interactive"]["button_reply"][
                            "title"
                        ]
                    )

                    misc = (json_obj["messages"][0]["interactive"]["list_reply"]["id"] if json_obj["messages"][0]["interactive"].get("list_reply") else "")
                    event_packet["context_id"] = (
                        json_obj["messages"][0]["context"]["id"]
                        if json_obj["messages"][0]["context"]
                        else ""
                    )
                    # print("create_event_packet:",event_packet['type'])
                elif data_type == "image":
                    event_packet["type"] = data_type
                    mime_type = json_obj["messages"][0]["image"]["mime_type"]
                    media_id = json_obj["messages"][0]["image"]["id"]
                    response, filepath = download_image(
                        bot_id, mime_type, media_id
                    )

                    file_identifier, cType = bot_interface.utils.get_filename_extension(filepath)
                    file_name = "docs/images/" + filepath.split("/")[-1]
                    status, s3_url, exc = bot_interface.utils.push_to_s3(
                        filepath, BUCKET_NAME, file_name, cType
                    )
                    data = filepath
                elif data_type == "audio" or data_type == "voice":
                    event_packet["type"] = "audio"
                    messages = json_obj["messages"][0]
                    msg_id = messages["id"]
                    timestamp = messages["timestamp"]
                    media_id = (
                        messages["voice"]["id"]
                        if messages.get("voice")
                        else messages["audio"]["id"]
                    )
                    # checksum = messages['voice']['sha256']
                    mime_type = (
                        messages["voice"]["mime_type"]
                        if messages.get("voice")
                        else messages["audio"]["mime_type"]
                    )
                    response, filepath = download_audio(
                        bot_id, mime_type, media_id
                    )

                    file_identifier, cType = bot_interface.utils.get_filename_extension(filepath)
                    file_name = "docs/audios/" + filepath.split("/")[-1]
                    status, s3_url, exc = bot_interface.utils.push_to_s3(
                        filepath, BUCKET_NAME, file_name, cType
                    )
                    # print(response)
                    data = filepath
                elif data_type == "video":
                    event_packet["type"] = data_type
                timestamp = json_obj["messages"][0]["timestamp"]
                message_id = json_obj["messages"][0]["id"]
        elif ("source_name" in json_obj and json_obj["source_name"] == "shopify_draft_order"):
            data = json_obj
            print("data in create event packet >>> ", data)
            print("data in create event packet >>> ", type(data))
            customer = data.get("customer")

            customer_phone = customer["phone"]
            event_packet["msisdn"] = str(customer_phone)[1:]
            event_packet["type"] = "notification"
            event_packet["state"] = bot_instance.init_state
        if (not message_id and "id" in json_obj and "type" in json_obj and json_obj["type"] == "interactive"):
            message_id = json_obj["id"]
            event_packet["message_to"] = json_obj["to"]
            event_packet["type"] = json_obj["type"]
        # event_packet['smjid'] = smjid
        # event_packet['state'] = state
        event_packet["event"] = event
        event_packet["data"] = data
        event_packet["timestamp"] = timestamp
        event_packet["message_id"] = message_id
        event_packet["media_id"] = media_id
        event_packet["wa_id"] = wa_id
        event_packet["misc"] = misc
        # event_packet['type'] = data_type
        event_packet["bot_id"] = bot_id

        return event_packet
