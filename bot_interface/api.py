import json
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
from rest_framework import status
import bot_interface.models
import bot_interface.utils
import bot_interface.tasks
import bot_interface.auth
import requests
import os
import emoji

# from deep_translator import GoogleTranslator
from utilities.auth_utils import auth_free

import time
from datetime import datetime, timedelta
import subprocess
from requests.exceptions import RequestException

from django.conf import settings
from django.http import HttpResponse, HttpResponseForbidden, JsonResponse

# from pydub import AudioSegment
# from pydub.utils import which

# Set to track processed message IDs
processed_message_ids = set()

# Define WhatsApp media path
WHATSAPP_MEDIA_PATH = settings.WHATSAPP_MEDIA_PATH

# Create the directory if it doesn't exist
os.makedirs(WHATSAPP_MEDIA_PATH, exist_ok=True)


def mark_message_as_read(bot_instance_id, message_id):
    """Mark WhatsApp message as read"""
    try:
        BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
            bot_instance_id
        )
        print("message_id : ", message_id)
        response = requests.post(
            f"{BSP_URL}messages",
            headers=HEADERS,
            timeout=10,
            json={
                "messaging_product": "whatsapp",
                "status": "read",
                "message_id": message_id,
            },
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"Error marking message as read: {str(e)}")


@api_view(["POST", "GET"])
@auth_free
@schema(None)
def whatsapp_webhook(request):

    print("Webhook start")
    print("START TIME = ", datetime.now())
    if request.method == "GET":
        # Example incoming:
        # GET /whatsapp_webhook?hub.mode=subscribe&hub.challenge=964057123&hub.verify_token=hello
        mode = request.GET.get("hub.mode")
        challenge = request.GET.get("hub.challenge")
        verify_token = request.GET.get("hub.verify_token")

        print(
            "Webhook verification attempt: mode=%s token=%s challenge=%s",
            mode,
            verify_token,
            challenge,
        )

        if mode == "subscribe" and verify_token == "Hello Coretsack":
            # Return challenge as plain text with 200 OK
            return HttpResponse(challenge, content_type="text/plain", status=200)
        else:
            print(
                "Webhook verification failed: mode=%s verify_token=%s expected=%s",
                mode,
                verify_token,
                "Hello Corestack",
            )
            return HttpResponseForbidden("Verification token mismatch")

    webhook_params = request.GET.dict()
    # Extract JSON data from the POST body
    json_data = request.data
    print("Webhook request JSON data ::: ", json.dumps(json_data, indent=4))

    # Extract msisdn from the nested structure
    entry = json_data.get("entry", [])
    if not entry or "changes" not in entry[0]:
        print(
            f"Invalid webhook structure: missing 'entry' or 'changes'. Received data: {json.dumps(json_data, indent=4)}"
        )
        return Response(
            {"error": "Invalid webhook structure"}, status=status.HTTP_400_BAD_REQUEST
        )

    changes = entry[0]["changes"]
    if not changes or "value" not in changes[0]:
        print(
            f"Invalid webhook structure: missing 'value' in 'changes'. Changes content: {changes}"
        )
        return Response(
            {"error": "Invalid webhook structure"}, status=status.HTTP_400_BAD_REQUEST
        )

    value = changes[0]["value"]
    metadata = value.get("metadata", {})
    msisdn = metadata.get("display_phone_number")

    if not msisdn:
        print(
            f"Missing 'display_phone_number' in metadata: {json.dumps(metadata, indent=4)}"
        )
        return Response(
            {"error": "Missing 'display_phone_number'"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    print("webhook intiated for phone number :: ", msisdn)
    json_data = json.dumps(request.data)
    print("Webhook request JSON data  ::: ", json_data)

    try:
        print("SERVICE MSISDN :: ", msisdn)
        bot = bot_interface.models.Bot.objects.get(bot_number=msisdn)
    except bot_interface.models.Bot.DoesNotExist:
        print(
            f"No App_instance_config found for msisdn: {msisdn}. Request data: {json.dumps(request.data, indent=4)}, Webhook params: {webhook_params}"
        )
        return Response(
            {"error": "App_instance_config not found"}, status=status.HTTP_404_NOT_FOUND
        )

    bot_id = bot.id
    print("bot_id :: ", bot_id)
    app_type = bot.app_type
    print("app_type :: ", app_type)
    # Check if the message status is "read"
    # if 'statuses' in entry[0]['changes'][0]['value']:
    #     message_status = entry[0]['changes'][0]['value']['statuses'][0]['status']
    #     if message_status in ["sent", "delivered"]:
    #         return Response({"status": message_status}, status=status.HTTP_200_OK)

    # If no statuses, use messages to get message ID and mark as read
    if "messages" in entry[0]["changes"][0]["value"]:
        message_id = entry[0]["changes"][0]["value"]["messages"][0]["id"]
        if message_id in processed_message_ids:
            return Response(
                {"error": "Duplicate message ID"}, status=status.HTTP_200_OK
            )
        processed_message_ids.add(message_id)
        mark_message_as_read(bot.id, message_id)
        Response({"success": True}, status=status.HTTP_200_OK)

    print("Flating user data")
    event = ""
    # create event packet
    factoryInterface = bot_interface.models.FactoryInterface()
    print("Factory Interface created :", bot.app_type)
    interface = factoryInterface.build_interface(app_type=bot.app_type)

    print("START TIME FOR CREATE EVENT PACKET= ", datetime.now())

    # Convert entry to JSON string as the create_event_packet expects a JSON string
    entry_json = json.dumps(entry)
    event_packet = interface.create_event_packet(
        json_obj=entry_json, bot_id=bot.id, event=event
    )
    print("EVENT PACKET= ", event_packet)

    # if not request.data.get('contacts') and event_packet['type'] == 'button':
    #     set_message_id = True

    bot_interface.tasks.StartUserSession.apply_async(
        kwargs={
            "event_packet": event_packet.to_dict(),  # ✅ converted
            "event": event,
            "bot_id": bot.id,
            "app_type": app_type,
        },
        queue="whatsapp",
    )

    print("END")
    # WhatsappUserSession.create_session(whatsapp_user, json_obj)
    return Response({"success": "success"}, status=status.HTTP_200_OK)


def send_text_url(app_instance_config_id, contact_number, text):
    print(text)
    BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
        bot_instance_id=app_instance_config_id
    )
    # text = emoji.emojize(text)
    response = requests.post(
        url=BSP_URL + "messages",
        headers=HEADERS,
        json={
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": contact_number,
            "type": "text",
            "text": {"preview_url": True, "body": text},
        },
    ).json()
    return response


def send_text(bot_instance_id, contact_number, text, bold=False):
    """This function sends a text message to a WhatsApp user.
    Args:
        bot_instance_id (int): The ID of the bot instance.
        contact_number (str): The phone number of the recipient.
        text (str): The text message to send.
        bold (bool): Whether to send the text in bold.
    """
    print("Sending text message to:", contact_number)
    print("Text message content:", text)

    # Validate phone number format
    if not contact_number:
        print("ERROR: Empty contact number")
        return {"error": "Empty contact number"}

    # Remove any + or - characters for WhatsApp API
    cleaned_number = contact_number.replace("+", "").replace("-", "").replace(" ", "")
    print(f"Cleaned contact number: {type(cleaned_number)}")

    BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
        bot_instance_id
    )
    print("BSP_URL:", BSP_URL)
    print("HEADERS:", HEADERS)

    # Prepare the request payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": cleaned_number,  # Use cleaned number
        "type": "text",
        "text": {"body": text},
    }

    # text = emoji.emojize(text)
    if bold:
        text = "*" + text + "*"
        payload["text"]["body"] = text

    print("Request URL:", BSP_URL + "messages")
    print("Request payload:", json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"Request headers: {json.dumps(HEADERS, indent=2)}")

    response = requests.post(
        url=BSP_URL + "messages",
        headers=HEADERS,
        json=payload,
    )

    print(f"TEXT SENT: {text}")
    print(f"RESPONSE STATUS CODE: {response.status_code}")
    print(f"RESPONSE HEADERS: {dict(response.headers)}")

    try:
        response_json = response.json()
        print(
            f"RESPONSE JSON: {json.dumps(response_json, indent=2, ensure_ascii=False)}"
        )
    except json.JSONDecodeError:
        print(f"RESPONSE TEXT: {response.text}")
        response_json = {"error": "Invalid JSON response"}

    # If error, print more details
    if response.status_code >= 400:
        print(f"ERROR DETAILS:")
        print(f"  Status: {response.status_code}")
        print(f"  Reason: {response.reason}")
        print(f"  URL: {response.url}")
        if hasattr(response, "request") and response.request:
            print(f"  Request Headers: {dict(response.request.headers)}")
            print(f"  Request Body: {response.request.body}")

    return response_json


def send_url(app_instance_config_id, contact_number, item_url):
    BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
        bot_instance_id=app_instance_config_id
    )
    return requests.post(
        url=BSP_URL + "messages",
        headers=HEADERS,
        timeout=10,
        json={
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": contact_number,
            "type": "text",
            "text": {"preview_url": "true", "body": "            " + item_url},
        },
    ).json()


def send_items(app_instance_config_id, contact_number, caption, items):
    """
    This function sends items sequentially.
    """
    print("Sending items to:", contact_number)

    if caption:
        send_text(app_instance_config_id, contact_number, caption)

    for item, title, s3_audio_url, is_youtube in items:
        if is_youtube:
            send_url(app_instance_config_id, contact_number, s3_audio_url)
        else:
            item_response = send_url(app_instance_config_id, contact_number, item)
            print("ITEM CARD SENT RESPONSE:", item_response)

            response = send_audio_with_retries(
                app_instance_config_id, contact_number, s3_audio_url, caption
            )
            print("ITEM AUDIO FILE SENT RESPONSE:", response, response.json())

        time.sleep(2.0)
    time.sleep(2.0)


def send_audio_with_retries(
    app_instance_config_id, contact_number, s3_audio_url, caption, max_retries=3
):
    """
    Helper function to send audio with retries.
    """
    count = 0
    while count <= max_retries:
        try:
            response = send_audio_as_reply(
                app_instance_config_id, contact_number, s3_audio_url, caption
            )
            if response.status_code == 201:
                return response
            print(f"RETRYING SENDING ITEM TIMES: {count}; ITEM_URL: {s3_audio_url}")
        except RequestException as e:
            print(f"Error sending audio: {e}")
        count += 1
        time.sleep(1)
    return response


# def send_items(app_instance_config_id, contact_number, caption, items):
#     """
#     This function sends items.

#     """
#     print("Sending items to : ", contact_number)
#     if caption != "":
#         send_text(app_instance_config_id, contact_number, caption)

#     for item, title, s3_audio_url, is_youtube in items:
#         if is_youtube:
#             send_url(app_instance_config_id, contact_number, s3_audio_url)
#         else:
#             item_response = send_url(app_instance_config_id, contact_number, item)
#             print("ITEM CARD SENT RESPONSE:: ", item_response)
#             # image_url = s3_image_url if s3_image_url else DEFAULT_IMAGE
#             # item_response = send_image_as_reply(contact_number, image_url, title)
#             response = send_audio_as_reply(
#                 app_instance_config_id, contact_number, s3_audio_url, caption
#             )
#             print("ITEM AUDIO FILE SENT RESPONSE:: ", response, response.json())
#             count = 0
#             while response.status_code != 201 and count <= 3:
#                 time.sleep(1)
#                 print(f"RETRYING SENDING ITEM TIMES: {count} ; ITEM_URL: {s3_audio_url}")
#                 response = send_audio_as_reply(
#                     app_instance_config_id, contact_number, s3_audio_url, caption
#                 )
#                 print("ITEM AUDIO FILE SENT RESPONSE:: ", response, response.json())
#                 count += 1
#             # send_text(contact_number, "*"*40)
#             time.sleep(0.5)
#     time.sleep(1.5)


def send_button_msg(bot_instance_id, contact_number, text, menu_list):
    """
    This function sends a button message to a WhatsApp user.
    Args:
        bot_instance_id (int): The ID of the bot instance.
        contact_number (str): The phone number of the recipient.
        text (str): The text message to send with buttons.
        menu_list (list): List of button options with 'label' and 'value' keys.
    """
    print("Sending button message to:", contact_number)
    print("Button message content:", text)
    print("Menu options:", menu_list)

    # Validate phone number format
    if not contact_number:
        print("ERROR: Empty contact number")
        return {"error": "Empty contact number"}

    # Remove any + or - characters for WhatsApp API
    cleaned_number = contact_number.replace("+", "").replace("-", "").replace(" ", "")
    print(f"Cleaned contact number: {cleaned_number}")

    try:
        BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
            bot_instance_id=bot_instance_id
        )
        print("BSP_URL:", BSP_URL)
        print("HEADERS:", HEADERS)
    except Exception as e:
        print(f"ERROR: Failed to get BSP URL/headers: {str(e)}")
        return {"error": f"Failed to get BSP URL/headers: {str(e)}"}

    def create_reply_json(menu_list):
        reply_json = []
        for i in range(len(menu_list)):
            label = emoji.emojize(menu_list[i]["label"])
            reply = {
                "type": "reply",
                "reply": {"title": label, "id": menu_list[i]["value"]},
            }
            reply_json.append(reply)
        return reply_json

    reply_btn_json = create_reply_json(menu_list)
    text = emoji.emojize(text)

    # Prepare the request payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": cleaned_number,  # Use cleaned number
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": text},
            "action": {"buttons": reply_btn_json},
        },
    }

    print("Request URL:", BSP_URL + "messages")
    print("Request payload:", json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"Request headers: {json.dumps(HEADERS, indent=2)}")

    response = requests.post(
        BSP_URL + "messages",
        headers=HEADERS,
        json=payload,
    )

    print(f"BUTTON MESSAGE SENT: {text}")
    print(f"RESPONSE STATUS CODE: {response.status_code}")
    print(f"RESPONSE HEADERS: {dict(response.headers)}")

    try:
        response_json = response.json()
        print(
            f"RESPONSE JSON: {json.dumps(response_json, indent=2, ensure_ascii=False)}"
        )

        # Save context ID if successful
        if (
            response.status_code == 200
            and "messages" in response_json
            and len(response_json["messages"]) > 0
        ):
            context_id = response_json["messages"][0]["id"]
            print("context id >> ", context_id)
            # TODO: Implement save_context_id_in_user_misc function
            # bot_interface.utils.save_context_id_in_user_misc(
            #     cleaned_number, context_id, app_instance_config_id, "WA"
            # )

    except json.JSONDecodeError:
        print(f"RESPONSE TEXT: {response.text}")
        response_json = {"error": "Invalid JSON response"}
    except Exception as e:
        print(f"ERROR: Failed to process response: {str(e)}")
        response_json = {"error": f"Failed to process response: {str(e)}"}

    # If error, print more details
    if response.status_code >= 400:
        print(f"ERROR DETAILS:")
        print(f"  Status: {response.status_code}")
        print(f"  Reason: {response.reason}")
        print(f"  URL: {response.url}")
        if hasattr(response, "request") and response.request:
            print(f"  Request Headers: {dict(response.request.headers)}")
            print(f"  Request Body: {response.request.body}")

    return response


def send_list_msg(
    bot_instance_id,
    contact_number,
    text,
    menu_list,
    button_label="Menu (मेनू)",
):
    """
    This function sends a list message to a WhatsApp user.
    Args:
        bot_instance_id (int): The ID of the bot instance.
        contact_number (str): The phone number of the recipient.
        text (str): The text message to send with list.
        menu_list (list): List of menu options.
        button_label (str): Label for the list button.
    """
    print("Sending list message to:", contact_number)
    print("List message content:", text)
    print("Menu list:", menu_list)
    print("Button label:", button_label)

    # Validate phone number format
    if not contact_number:
        print("ERROR: Empty contact number")
        return {"error": "Empty contact number"}

    # Remove any + or - characters for WhatsApp API
    cleaned_number = contact_number.replace("+", "").replace("-", "").replace(" ", "")
    print(f"Cleaned contact number: {cleaned_number}")

    try:
        BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
            bot_instance_id=bot_instance_id
        )
        print("BSP_URL:", BSP_URL)
        print("HEADERS:", HEADERS)
    except Exception as e:
        print(f"ERROR getting BSP URL/Headers: {str(e)}")
        return {"error": f"Failed to get BSP config: {str(e)}"}

    # Get language configuration
    try:
        bot_instance_config = bot_interface.models.Bot.objects.get(id=bot_instance_id)
        language = bot_instance_config.language
        if language == "hi":
            section_title = "कोई एक विकल्प चुनें:"
        else:
            section_title = "Choose one option :"
        print(f"Language: {language}, Section title: {section_title}")
    except Exception as e:
        print(f"ERROR getting app config: {str(e)}")
        section_title = "Choose one option :"  # Default fallback

    def create_reply_json(menu_list):
        reply_json = []
        try:
            for i in range(len(menu_list)):
                description = (
                    str(menu_list[i]["description"])
                    if "description" in menu_list[i]
                    else ""
                )
                reply = {
                    "id": str(menu_list[i]["value"]),
                    "title": str(menu_list[i]["label"]),
                    "description": description,
                }
                reply_json.append(reply)
        except Exception as e:
            print(f"ERROR creating reply JSON: {str(e)}")
            return []
        return reply_json

    reply_list_json = create_reply_json(menu_list)
    if not reply_list_json:
        print("ERROR: Failed to create reply list JSON")
        return {"error": "Failed to create list options"}

    # Prepare the request payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": cleaned_number,  # Use cleaned number
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": emoji.emojize(text)},
            "action": {
                "button": button_label,
                "sections": [
                    {
                        "title": section_title,
                        "rows": reply_list_json,
                    }
                ],
            },
        },
    }

    print("Request URL:", BSP_URL + "messages")
    print("Request payload:", json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"Request headers: {json.dumps(HEADERS, indent=2)}")

    try:
        response = requests.post(
            BSP_URL + "messages",
            headers=HEADERS,
            json=payload,
        )

        print(f"LIST MESSAGE SENT: {text}")
        print(f"RESPONSE STATUS CODE: {response.status_code}")
        print(f"RESPONSE HEADERS: {dict(response.headers)}")

        try:
            response_json = response.json()
            print(
                f"RESPONSE JSON: {json.dumps(response_json, indent=2, ensure_ascii=False)}"
            )
        except json.JSONDecodeError:
            print(f"RESPONSE TEXT: {response.text}")
            response_json = {"error": "Invalid JSON response"}
            return response_json

        # If error, print more details
        if response.status_code >= 400:
            print(f"ERROR DETAILS:")
            print(f"  Status: {response.status_code}")
            print(f"  Reason: {response.reason}")
            print(f"  URL: {response.url}")
            if hasattr(response, "request") and response.request:
                print(f"  Request Headers: {dict(response.request.headers)}")
                print(f"  Request Body: {response.request.body}")
            return response_json

        # Save context ID if successful
        try:
            if "messages" in response_json and len(response_json["messages"]) > 0:
                context_id = response_json["messages"][0]["id"]
                print("Context ID:", context_id)
                # TODO: Implement save_context_id_in_user_misc function
                # bot_interface.utils.save_context_id_in_user_misc(
                #     cleaned_number, context_id, app_instance_config_id, "WA"
                # )
            else:
                print("WARNING: No context ID found in response")
        except Exception as e:
            print(f"ERROR saving context ID: {str(e)}")

        return response_json

    except Exception as e:
        print(f"ERROR sending list message: {str(e)}")
        return {"error": f"Failed to send list message: {str(e)}"}


def send_location_request(bot_instance_id, contact_number, text):
    """Send a location request message
    Args:
        bot_instance_id (int): The ID of the bot instance.
        contact_number (str): The phone number of the recipient.
        text (str): The text message to send with the location request.
    """
    BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
        bot_instance_id=bot_instance_id
    )
    print("Sending location request to:", contact_number)
    print("Location request content:", text)
    # Validate phone number format
    if not contact_number:
        print("ERROR: Empty contact number")
        return {"error": "Empty contact number"}
    # Remove any + or - characters for WhatsApp API
    cleaned_number = contact_number.replace("+", "").replace("-", "").replace(" ", "")
    print(f"Cleaned contact number: {cleaned_number}")
    print("BSP_URL:", BSP_URL)
    print("HEADERS:", HEADERS)
    # Prepare the request payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": cleaned_number,
        "type": "interactive",
        "interactive": {
            "type": "location_request_message",
            "body": {"text": text},
            "action": {"name": "send_location"},
        },
    }

    try:
        response = requests.post(BSP_URL + "messages", headers=HEADERS, json=payload)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR sending location request: {str(e)}")
        return {"error": f"Failed to send location request: {str(e)}"}
    except json.JSONDecodeError:
        print(f"ERROR decoding JSON response: {response.text}")
        return {"error": "Invalid JSON response from WhatsApp API"}
    except requests.exceptions.Timeout:
        print("ERROR: Request timed out while sending location request")
        return {"error": "Request timed out while sending location request"}
    except Exception as e:
        print(f"ERROR sending location request: {str(e)}")
        return {"error": f"Failed to send location request: {str(e)}"}


def download_image(app_instance_config_id, mime_type, media_id):
    """This function downloads image message"""
    print(os.getcwd())
    BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
        bot_instance_id=app_instance_config_id
    )
    filepath = WHATSAPP_MEDIA_PATH + media_id + ".jpg"
    url = BSP_URL.split("v24.0")[0] + "v24.0" + "/" + media_id
    print("url :: ", url)
    r = requests.get(url, headers=HEADERS)
    print("r :: ", r, r.json())
    filepath, success = download_media_from_url(
        app_instance_config_id, media_response=r.json()
    )
    print("filepath :: ", filepath)
    if success:
        hdpi_path = bot_interface.utils.convert_image_hdpi(filepath)
        print("hdpi_path :: ", hdpi_path)
        return r, hdpi_path
    return r, filepath


def download_audio(app_instance_config_id, mime_type, media_id):
    """Downloads audio message and voice message using proper WhatsApp API flow"""
    try:
        # Step 1: Get media metadata from WhatsApp API
        BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
            bot_instance_id=app_instance_config_id
        )

        # Get media info using proper endpoint
        media_info_url = BSP_URL.split("v24.0")[0] + "v24.0/" + media_id
        print(f"Getting media info from: {media_info_url}")
        r = requests.get(media_info_url, headers=HEADERS, timeout=30)
        print(
            f"Media info response: {r.status_code}, {r.json() if r.status_code == 200 else r.text}"
        )

        if r.status_code != 200:
            print(f"Failed to get media info: {r.status_code}")
            return r, None

        # Step 2: Use existing download_media_from_url function to download actual file
        filepath, success = download_media_from_url(
            app_instance_config_id, media_response=r.json()
        )
        print(f"Audio download result - filepath: {filepath}, success: {success}")

        if success and filepath:
            print(f"Successfully downloaded audio to: {filepath}")
            return r, filepath
        else:
            print("Audio download failed in download_media_from_url")
            return r, None

    except Exception as e:
        print(f"Error in download_audio: {e}")
        return None, None


def is_audio_file(mime_type):
    """Check if mime type is audio"""
    AUDIO_MIME_TYPES = {
        "audio/aac": ".aac",
        "audio/amr": ".amr",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "audio/ogg": ".opus",
    }
    return mime_type in AUDIO_MIME_TYPES


def get_audio_extension(mime_type: str) -> str:
    """Get proper file extension based on audio mime type"""
    AUDIO_EXTENSIONS = {
        "audio/aac": ".aac",
        "audio/amr": ".amr",
        "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a",
        "audio/ogg": ".ogg",
        "audio/opus": ".opus",
    }
    return AUDIO_EXTENSIONS.get(mime_type, ".mp3")  # Default to .mp3


def convert_wav_to_mp3(input_path, bitrate="192k"):
    """
    Convert WAV to MP3 with quality checks and detailed logging.

    Args:
        input_path: Path to input WAV file
        bitrate: Target MP3 bitrate (default: 192k for good quality)
    """
    try:
        # Input validation and size check
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        input_size = os.path.getsize(input_path)
        print(f"Input WAV file size: {input_size / 1024:.2f} KB")

        output_path = input_path.replace(".wav", ".mp3")

        # Direct FFmpeg conversion for better control
        command = [
            "ffmpeg",
            "-i",
            input_path,  # Input file
            "-codec:a",
            "libmp3lame",  # Use LAME MP3 encoder
            "-q:a",
            "2",  # Quality setting (2 is high quality, range is 0-9)
            "-b:a",
            bitrate,  # Target bitrate
            "-ar",
            "48000",  # Maintain sampling rate close to original
            "-map_metadata",
            "0",  # Copy metadata
            "-y",  # Overwrite output if exists
            output_path,
        ]

        # Run conversion
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            print("Conversion failed. FFmpeg output:")
            print(result.stderr)
            raise Exception("FFmpeg conversion failed")

        # Verify output
        output_size = os.path.getsize(output_path)
        print(f"Output MP3 file size: {output_size / 1024:.2f} KB")

        if output_size < 1000:  # Basic sanity check
            raise Exception(f"Output file too small ({output_size} bytes)")

        print(f"Successfully converted to: {output_path}")
        return output_path

    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        return None


def convert_ogg_to_wav(input_path):
    """Convert .ogg audio file to .mp3 using pydub and ffmpeg"""
    try:
        # output_path = input_path.replace('.ogg', '.mp3')
        wav_path = input_path.replace(".ogg", ".wav")

        ogg_to_wav_cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-acodec",
            "pcm_s16le",
            "-ar",
            "48100",
            wav_path,
        ]

        # Execute the first command (OGG to WAV)
        ogg_to_wav_result = subprocess.run(
            ogg_to_wav_cmd, capture_output=True, text=True
        )
        ogg_to_wav_output = ogg_to_wav_result.stdout + "\n" + ogg_to_wav_result.stderr

        # Execute the second command (WAV to MP3) if the first step succeeds
        wav_to_mp3_result = input_path  # Default fallback
        if ogg_to_wav_result.returncode == 0:
            wav_to_mp3_result = convert_wav_to_mp3(wav_path)
            # wav_to_mp3_output = wav_to_mp3_result.stdout + "\n" + wav_to_mp3_result.stderr
        else:
            wav_to_mp3_output = "OGG to WAV conversion failed, skipping MP3 conversion."
        print(
            f"Converted audio from ogg format :{input_path} to mp3 format: {wav_to_mp3_result}"
        )
        return wav_to_mp3_result
    except subprocess.CalledProcessError as e:
        print(f"Error converting audio: {str(e)}")
        return input_path


def download_media_from_url(app_instance_config_id, media_response):
    """
    Downloads media from WhatsApp Business API
    Args:
        app_instance_config_id: App instance config ID
        media_response: JSON response containing media details
    Returns:
        tuple: (filepath, success)
    """
    try:
        # Extract media URL path
        fb_url = media_response["url"]
        print("fb url" + str(fb_url))
        media_path = fb_url

        # Get BSP URL and headers
        BSP_URL, HEADERS, namespace = bot_interface.auth.get_bsp_url_headers(
            bot_instance_id=app_instance_config_id
        )

        # Construct download URL
        download_url = f"{media_path}"

        # Determine file extension from mime_type
        mime_type = media_response["mime_type"]
        extension = mime_type.split("/")[-1]
        print("media_response :: ", media_response, mime_type)

        # Create filepath
        media_id = media_response["id"]
        print("media_id :: ", media_id)
        print("extension :: ", extension)
        print("WHATSAPP_MEDIA_PATH :: ", WHATSAPP_MEDIA_PATH)
        # assert False
        filepath = f"{WHATSAPP_MEDIA_PATH}{media_id}.{extension}"
        print("filepath in download_media_from_url function :: ", filepath)

        # Download file
        response = requests.get(download_url, headers=HEADERS, stream=True, timeout=60)
        response.raise_for_status()

        # Save file
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded media file to {filepath}")
        else:
            print(f"Failed to download media file: {response.status_code}")
            return None, None

        # Convert audio files to mp3
        if is_audio_file(mime_type):
            print("Inside is audio file function : ", mime_type, filepath)
            filepath = convert_ogg_to_wav(filepath)
            print("After convert_to_mp3 function : ", filepath)

        return filepath, True

    except Exception as e:
        print(f"Error downloading media: {str(e)}")
        return None, False
