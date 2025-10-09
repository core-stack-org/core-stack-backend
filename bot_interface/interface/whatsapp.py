import json
from typing import Dict, Any
from django.utils import timezone
from django.conf import settings

import bot_interface.interface.generic
import bot_interface.models
import bot_interface.utils
import bot_interface.api
import bot_interface.auth

import requests

from geoadmin.models import State, District, Block

import logging

logger = logging.getLogger(__name__)


class WhatsAppInterface(bot_interface.interface.generic.GenericInterface):
    """WhatsApp interface implementation for handling WhatsApp Business API interactions"""

    BUCKET_NAME = "your-s3-bucket-name"

    @staticmethod
    def create_event_packet(json_obj: Any, bot_id: int, event: str = "start") -> Dict[str, Any]:
        """
        Create an event packet from WhatsApp webhook data.
        
        Args:
            json_obj: JSON string or dict containing webhook data
            bot_id: Bot instance ID
            event: Event type (default: "start")
            
        Returns:
            dict: Event packet dictionary
        """
        print("create_event_packet called with bot_id:", bot_id, type(bot_id))

        try:
            bot_interface.models.Bot.objects.get(id=bot_id)
        except bot_interface.models.Bot.DoesNotExist:
            raise ValueError(f"Bot with id {bot_id} not found")

        print("Json Obj in create_event_packet", json_obj, type(json_obj))

        # Parse JSON if it's a string
        if isinstance(json_obj, str):
            try:
                json_obj = json.loads(json_obj)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON string provided: {json_obj}") from exc

        print("Parsed Json Obj in create_event_packet", json_obj, type(json_obj))

        # Ensure we have a valid data structure
        if not isinstance(json_obj, (dict, list)):
            raise ValueError(f"Expected dict or list, got {type(json_obj)}")

        # If it's a list, it might be the WhatsApp webhook entry format
        if isinstance(json_obj, list) and len(json_obj) > 0:
            # WhatsApp webhook format is typically: [{"changes": [{"value": {...}}]}]
            if isinstance(json_obj[0], dict) and "changes" in json_obj[0]:
                # Extract the actual message data from WhatsApp webhook format
                changes = json_obj[0]["changes"]
                if len(changes) > 0 and "value" in changes[0]:
                    json_obj = changes[0]["value"]
                    print("Extracted value from WhatsApp webhook format:", json_obj)
        # Initialize event packet with defaults
        event_packet = {
            "event": event,
            "bot_id": bot_id,
            "data": "",
            "timestamp": "",
            "message_id": "",
            "media_id": "",
            "wa_id": "",
            "misc": "",
            "type": "",
            "user_number": "",
            "smj_id": "",
            "state": ""
        }

        # Process different types of incoming data
        if "contacts" in json_obj:
            WhatsAppInterface._process_message_data(json_obj, event_packet, bot_id)
        elif WhatsAppInterface._is_interactive_message(json_obj):
            WhatsAppInterface._process_interactive_message(json_obj, event_packet)

        # Preserve current user context for button events and other interactions
        WhatsAppInterface._preserve_user_context(event_packet, bot_id)

        return event_packet

    @staticmethod
    def _process_message_data(json_obj: Dict, event_packet: Dict, bot_id: int) -> None:
        """Process regular WhatsApp message data"""
        contacts = json_obj["contacts"][0]
        wa_id = contacts.get("wa_id", "")
        event_packet["user_number"] = wa_id
        event_packet["wa_id"] = wa_id

        if "messages" not in json_obj:
            return

        message = json_obj["messages"][0]
        data_type = message.get("type", "")
        event_packet["timestamp"] = message.get("timestamp", "")
        event_packet["message_id"] = message.get("id", "")

        if data_type == "text":
            WhatsAppInterface._process_text_message(message, event_packet)
        elif data_type == "interactive":
            WhatsAppInterface._process_interactive_response(message, event_packet)
        elif data_type == "location":
            WhatsAppInterface._process_location_message(message, event_packet)
        elif data_type == "image":
            WhatsAppInterface._process_image_message(message, event_packet, bot_id)
        elif data_type in ("audio", "voice"):
            WhatsAppInterface._process_audio_message(message, event_packet, bot_id)
        elif data_type == "video":
            event_packet["type"] = data_type

    @staticmethod
    def _process_text_message(message: Dict, event_packet: Dict) -> None:
        """Process text message"""
        event_packet["type"] = "text"
        event_packet["data"] = message["text"]["body"]

    @staticmethod
    def _process_interactive_response(message: Dict, event_packet: Dict) -> None:
        """Process interactive message response"""
        event_packet["type"] = "button"
        interactive = message["interactive"]

        if interactive.get("list_reply"):
            event_packet["data"] = interactive["list_reply"]["title"]
            event_packet["misc"] = interactive["list_reply"]["id"]
        else:
            event_packet["data"] = interactive["button_reply"]["id"]

        if message.get("context"):
            event_packet["context_id"] = message["context"]["id"]

    @staticmethod
    def _process_location_message(message: Dict, event_packet: Dict) -> None:
        """Process location message"""
        event_packet["type"] = "location"
        location = message["location"]

        # Extract latitude and longitude
        latitude = location.get("latitude", "")
        longitude = location.get("longitude", "")

        # Store as formatted string or coordinate object
        event_packet["data"] = f"{latitude},{longitude}"
        event_packet["misc"] = {
            "latitude": latitude,
            "longitude": longitude,
            "name": location.get("name", ""),
            "address": location.get("address", "")
        }
        print(f"Processed location message: {latitude}, {longitude}")

    @staticmethod
    def _preserve_user_context(event_packet: Dict, bot_id: int) -> None:
        """Preserve current user context for proper state transitions"""
        try:
            user_number = event_packet.get("user_number")
            if not user_number:
                return

            # Find the current user session to preserve context
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_id)

            # Check if user exists in our system
            try:
                bot_user = bot_interface.models.BotUsers.objects.get(user__contact_number=user_number)
                user_session = bot_interface.models.UserSessions.objects.get(user_id=bot_user.id, bot=bot_instance)

                # Preserve current SMJ and state context
                if user_session.current_smj and user_session.current_state:
                    event_packet["smj_id"] = user_session.current_smj.id
                    event_packet["state"] = user_session.current_state
                    print(
                        f"Preserved user context - SMJ: {user_session.current_smj.id}, State: {user_session.current_state}")

            except (bot_interface.models.BotUsers.DoesNotExist, bot_interface.models.UserSessions.DoesNotExist):
                # User or session doesn't exist yet, will be handled in session creation
                print("No existing user session found, will use default context")

        except Exception as e:
            print(f"Error preserving user context: {e}")
            # Don't fail the whole process if context preservation fails

    @staticmethod
    def _process_image_message(message: Dict, event_packet: Dict, bot_id: int) -> None:
        """Process image message"""
        event_packet["type"] = "image"
        mime_type = message["image"]["mime_type"]
        media_id = message["image"]["id"]
        event_packet["media_id"] = media_id

        # Download and process image
        filepath = WhatsAppInterface._download_and_upload_media(
            bot_id, mime_type, media_id, "image"
        )
        event_packet["data"] = filepath

    @staticmethod
    def _process_audio_message(message: Dict, event_packet: Dict, bot_id: int) -> None:
        """Process audio/voice message"""
        event_packet["type"] = "audio"

        if message.get("voice"):
            media_id = message["voice"]["id"]
            mime_type = message["voice"]["mime_type"]
        else:
            media_id = message["audio"]["id"]
            mime_type = message["audio"]["mime_type"]

        event_packet["media_id"] = media_id

        # Download and process audio
        filepath = WhatsAppInterface._download_and_upload_media(
            bot_id, mime_type, media_id, "audio"
        )
        event_packet["data"] = filepath

    @staticmethod
    def _download_and_upload_media(bot_id: int, mime_type: str, media_id: str,
                                   media_type: str) -> str:
        """Download media from WhatsApp and upload to S3"""
        # You need to implement these functions
        if media_type == "image":
            filepath = WhatsAppInterface._download_image(bot_id, mime_type, media_id)
        else:
            filepath = WhatsAppInterface._download_audio(bot_id, mime_type, media_id)

        # Upload to S3
        file_extension = bot_interface.utils.get_filename_extension(filepath)[1]
        s3_folder = "docs/images/" if media_type == "image" else "docs/audios/"
        file_name = s3_folder + filepath.split("/")[-1]

        bot_interface.utils.push_to_s3(
            filepath, WhatsAppInterface.BUCKET_NAME, file_name, file_extension
        )

        return filepath

    @staticmethod
    def _download_image(bot_id: int, mime_type: str, media_id: str) -> str:
        """Download image from WhatsApp API using proper API flow"""
        try:
            print(f"Downloading image: bot_id={bot_id}, mime_type={mime_type}, media_id={media_id}")
            response, filepath = bot_interface.api.download_image(bot_id, mime_type, media_id)

            if response and response.status_code == 200 and filepath:
                print(f"Successfully downloaded image to: {filepath}")
                return filepath
            else:
                error_msg = f"Image download failed - response: {response.status_code if response else 'None'}, filepath: {filepath}"
                print(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            error_msg = f"Error downloading image: {e}"
            print(error_msg)
            raise Exception(error_msg)

    @staticmethod
    def _download_audio(bot_id: int, mime_type: str, media_id: str) -> str:
        """Download audio from WhatsApp API using proper API flow"""
        try:
            print(f"Downloading audio: bot_id={bot_id}, mime_type={mime_type}, media_id={media_id}")
            response, filepath = bot_interface.api.download_audio(bot_id, mime_type, media_id)

            if response and response.status_code == 200 and filepath:
                print(f"Successfully downloaded audio to: {filepath}")
                return filepath
            else:
                error_msg = f"Audio download failed - response: {response.status_code if response else 'None'}, filepath: {filepath}"
                print(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            error_msg = f"Error downloading audio: {e}"
            print(error_msg)
            raise Exception(error_msg)

    @staticmethod
    def _is_interactive_message(json_obj: Dict) -> bool:
        """Check if this is an interactive message"""
        return bool(json_obj.get("id") and
                    json_obj.get("type") == "interactive")

    @staticmethod
    def _process_interactive_message(json_obj: Dict, event_packet: Dict) -> None:
        """Process interactive message"""
        event_packet["message_id"] = json_obj["id"]
        event_packet["message_to"] = json_obj.get("to", "")
        event_packet["type"] = json_obj["type"]

    def sendText(self, bot_id, data_dict):
        # 1 : user profile lnguage 2: config language(app_config_json) 3: default
        logger.info("data_dict in sendText: %s", data_dict)
        data = data_dict.get("text")

        bot_instance = bot_interface.models.Bot.objects.get(id=bot_id)
        if bot_instance:
            logger.info("bot_instance language in sendText: %s", bot_instance.language)

        user_id = data_dict.get("user_id")
        print("user in sendText", user_id)

        text = data[0].get(bot_instance.language)
        print(text, bot_id)

        try:
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            response = bot_interface.api.send_text(
                bot_instance_id=bot_id,
                contact_number=user.phone,
                text=text)
            print("Text message response:", response)

            # Update user session state
            print("user", user.current_session)
            user.expected_response_type = "text"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smjid = data_dict.get("smj_id")
            user.current_smj = bot_instance.smj
            user.save()
            print("SMJ : ", bot_instance.smj)
            logger.info("sendText response: %s", response)
            logger.info("Exiting sendText with response: ")

            # Return success/failure based on API response
            if response and response.get('messages'):
                return "success"
            else:
                return "failure"

        except Exception as e:
            print(f"Error in sendText: {e}")
            logger.error(f"Error in sendText: {e}")
            return "failure"

    def sendButton(self, bot_instance_id, data_dict):
        print("in sendButton")
        logger.info("data_dict in sendButton: %s", data_dict)
        bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
        if bot_instance:
            print("bot_instance", bot_instance.language)

        user_id = data_dict.get("user_id")
        print("user in sendButton", user_id)

        data = data_dict.get("menu")
        # Extract caption from first menu item or use data_dict caption or default
        caption = "Select an option:"  # Default
        if data_dict.get("caption"):
            caption = data_dict.get("caption")
        elif data and len(data) > 0 and "caption" in data[0]:
            caption = data[0]["caption"]

        print(data)
        print("caption", caption)

        try:
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print("user in sendButton", user)
            user.expected_response_type = "button"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            user.save()

            print("length of data ::", len(data))
            response = None

            if len(data) > 3:
                print("in send_list msg ::")
                label = "Select Here"
                response = bot_interface.api.send_list_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=caption,
                    menu_list=data,
                    button_label=label
                )
                print("List message without description response :", response)
            elif len(data) <= 3 and ("description" in data[0]):
                print("in send_list msg with less than equals to 3 options- labels with description::")
                label = "Select Here"
                response = bot_interface.api.send_list_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=caption,
                    menu_list=data,
                    button_label=label
                )
                print("List message with description response :", response)
            else:
                print("in send_list msg labels with description::")
                label = "Select Here"
                response = bot_interface.api.send_button_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=caption,
                    menu_list=data
                )
                print("Button message response:", response)

            # Return success/failure based on API response
            if response and response.get('messages'):
                return "success"
            else:
                return "failure"

        except Exception as e:
            print(f"Error in sendButton: {e}")
            return "failure"

    def sendLocationRequest(self, bot_instance_id, data_dict):
        """
        Send a location request to the user.
        This function prepares the user session and sends a request for location sharing.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            response: Response from the WhatsApp API after sending the location request.
        """
        print("in sendLocationRequest")
        bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
        if bot_instance:
            print("bot_instance", bot_instance.language)

        user_id = data_dict.get("user_id")
        print("user in sendLocationRequest", user_id)

        # check if user is created
        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
        print("user in sendLocationRequest", user)
        user.expected_response_type = "location"
        user.current_state = data_dict.get("state")

        # Handle SMJ object lookup with error handling
        smj_id = data_dict.get("smj_id")
        user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
        user.save()

        response = bot_interface.api.send_location_request(
            bot_instance_id=bot_instance_id,
            contact_number=user.phone,
            text="कृपया स्थान भेजें"
        )

        print("Location request response:", response)

        # Return success/failure based on API response
        if response and response.get('messages'):
            return "success"
        else:
            return "failure"

    def sendCommunityByLocation(self, bot_instance_id, data_dict):
        """
        Send community options based on user's location.
        This function prepares the user session and sends a request for community selection.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure" based on operation result.
        """
        print("in sendCommunityByLocation")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            if bot_instance:
                print("bot_instance", bot_instance.language)

            user_id = data_dict.get("user_id")
            print("user in sendCommunityByLocation", user_id)
            logger.info(data_dict)
            # assert False
            data = data_dict.get("data", {})
            print("data in sendCommunityByLocation:", data)

            # Initialize variables
            get_data_from_state = None
            get_data_from_state_field = None

            # check if data_dict has 'getDataFrom' key
            if 'getDataFrom' in data:
                # getDataFrom has the statename from which we need to get the data received to use it here
                get_data_from = data['getDataFrom']
                get_data_from_state = get_data_from.get("state", "")
                get_data_from_state_field = get_data_from.get("field", "data")
                print("State name from data_dict:", get_data_from_state)
                print("Field name from data_dict:", get_data_from_state_field)

            # check if user is created
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print("user in sendCommunityByLocation", user)
            # user.expected_response_type = "community"
            user.current_state = data_dict.get("state")

            # load user's current session
            current_session = user.current_session
            print("Current session in sendCommunityByLocation:", current_session)

            # Initialize latitude and longitude
            latitude = ""
            longitude = ""

            # get the data field from getDataFrom state in current session
            location_data_found = False

            if get_data_from_state and current_session:
                # current_session is a list of dictionaries, need to search through them
                for session_dict in current_session:
                    if isinstance(session_dict, dict) and get_data_from_state in session_dict:
                        data = session_dict[get_data_from_state].get(get_data_from_state_field, {})
                        print("Data from current session for state:", data)
                        location_data_found = True

                        # lat lon is stored as a string in data separated by comma eg (25.2048, 55.2708)
                        if isinstance(data, str):
                            lat_lon = data.split(",")
                            if len(lat_lon) == 2:
                                latitude = lat_lon[0].strip()
                                longitude = lat_lon[1].strip()
                                print("Latitude and Longitude from string:", latitude, longitude)
                                break  # Found the data, exit the loop
                            else:
                                print("Invalid latitude and longitude format in string")
                        else:
                            print("Latitude and Longitude not found in current session data - data is not string")

            if not location_data_found or not latitude or not longitude:
                print(f"Required location data not found in session: {get_data_from_state}")
                return "failure"

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

            response = None
            # try:
            from public_api.views import get_location_info_by_lat_lon
            print("Fetching community by location...", latitude, longitude)
            response = get_location_info_by_lat_lon(
                lat=float(latitude),
                lon=float(longitude)
            )
            logger.info("Location info response:", response)
            # except Exception as e:
            #     logger.error(f"Error fetching community using get_community_by_lat_lon: {e}")
            #     # except 500 error
            #     try:
            #         print("Fetching community by location...")
            #         response = requests.get(
            #             url="https://uat.core-stack.org/api/v1/get_admin_details_by_latlon/?latitude=25.1369&longitude=85.4516",
            #             # params={"latitude": latitude, "longitude": longitude},
            #             timeout=30
            #         ).json()
            #     except requests.exceptions.RequestException as e:
            #         print(f"Error fetching community by location: {e}")
            #         return "failure"

            print("Community by location response:", response)

            if response:
                from community_engagement.utils import get_communities
                # Handle response based on its type
                if isinstance(response, tuple):
                    # If response is a tuple (success, data), extract the data
                    success, location_data = response
                    if success and isinstance(location_data, dict):
                        community_data = get_communities(
                            state_name=location_data.get("State", ""),
                            district_name=location_data.get("District", ""),
                            block_name=location_data.get("Block", "")
                        )
                    else:
                        return "failure"
                elif isinstance(response, dict):
                    # If response is a dict, use it directly
                    community_data = get_communities(
                        state_name=response.get("State", ""),
                        district_name=response.get("District", ""),
                        block_name=response.get("Block", "")
                    )
                else:
                    return "failure"
                if community_data == "no_communities":
                    return "no_communities"
                print("Community list:", community_data)
                communities_list = []
                if community_data:
                    for community in community_data:
                        communities_list.append({
                            "value": community.get("community_id"),
                            "label": community.get("name"),
                            "description": community.get("description", "")
                        })

                    print("Communities list for WhatsApp:", communities_list)

                    # Send the communities list to user
                    if communities_list:
                        send_communities_response = bot_interface.api.send_list_msg(
                            bot_instance_id=bot_instance_id,
                            contact_number=user.phone,
                            text="कृपया अपना समुदाय चुनें",
                            menu_list=communities_list
                        )

                        print("Communities list message response:", send_communities_response)
                    user.expected_response_type = "community"
                    user.save()
                    if send_communities_response and send_communities_response.get('messages'):
                        return "success"
                    else:
                        return "failure"
                else:
                    return "failure"
            else:
                return "failure"

        except Exception as e:
            print(f"Error in sendCommunityByLocation: {e}")
            return "failure"

    def sendStates(self, bot_instance_id, data_dict):
        """
        Send state options to the user.
        This function prepares the user session and sends a request for state selection.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            response: Response from the WhatsApp API after sending the state options.
        """
        print("in sendStates")
        bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
        if bot_instance:
            print("bot_instance", bot_instance.language)

        user_id = data_dict.get("user_id")
        print("user in sendStates", user_id)

        # check if user is created
        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
        print("user in sendStates", user)
        user.expected_response_type = "button"
        user.current_state = data_dict.get("state")

        # Handle SMJ object lookup with error handling
        smj_id = data_dict.get("smj_id")
        user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
        user.save()

        from community_engagement.models import Location
        state_ids_with_community = Location.objects.filter(communities__isnull=False).values_list('state_id',
                                                                                                  flat=True).distinct()
        states = State.objects.filter(pk__in=state_ids_with_community).order_by('state_name')

        print("States to be sent:", states)
        states_list = [{"value": state.pk, "label": state.state_name, "description": ""} for state in states]
        # states_list = [{"value": state.pk, "label": state.state_name}]
        print("States list to be sent:", states_list)
        response = bot_interface.api.send_list_msg(
            bot_instance_id=bot_instance_id,
            contact_number=user.phone,
            text="कृपया अपना राज्य चुनें",
            menu_list=states_list
        )
        print("List message response:", response)

        # Return success/failure based on API response
        if response and response.get('messages'):
            return "success"
        else:
            return "failure"

    def sendDistricts(self, bot_instance_id, data_dict):
        """
        Send district options based on selected state.
        This function prepares the user session and sends a request for district selection.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure" based on operation result.
        """
        print("in sendDistricts")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            if bot_instance:
                print("bot_instance", bot_instance.language)

            user_id = data_dict.get("user_id")
            print("user in sendDistricts", user_id)

            # check if user is created
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print("user in sendDistricts", user)

            # get the state name from data_dict
            user.expected_response_type = "button"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

            data = data_dict.get("data", {})
            print("data in sendDistricts:", data)

            # Initialize variables
            get_data_from_state = None
            get_data_from_state_field = "data"  # Default value

            # check if data_dict has 'getDataFrom' key
            if 'getDataFrom' in data:
                # getDataFrom has the statename from which we need to get the data received to use it here
                get_data_from = data['getDataFrom']
                get_data_from_state = get_data_from.get("state", "")
                get_data_from_state_field = get_data_from.get("field", "data")
                print("State name from data_dict:", get_data_from_state)
                print("Field name from data_dict:", get_data_from_state_field)

            # Check if we have the required state data
            if not get_data_from_state:
                print("No state data found in configuration")
                return "failure"

            current_session = user.current_session
            print("Current session in sendDistricts:", type(current_session), current_session)

            # Validate session structure and extract state ID
            state_id = None
            if current_session and isinstance(current_session, list) and len(current_session) > 0:
                if get_data_from_state in current_session[0]:
                    state_data = current_session[0][get_data_from_state]
                    state_id = state_data.get(get_data_from_state_field, "")
                    print(f"State ID extracted from session: {state_id}")
                else:
                    print(f"State '{get_data_from_state}' not found in session data")
            else:
                print("Session data is empty or malformed")

            if not state_id:
                print("No state ID found in session data")
                return "failure"
            # get districts based on the selected state
            try:
                response = requests.get(
                    # url=f"{settings.COMMUNITY_ENGAGEMENT_API_URL}get_districts_with_community/",
                    url=f"http://localhost:8000/api/v1/get_districts_with_community/",
                    params={"state_id": state_id},
                    timeout=30
                )
                response.raise_for_status()  # Raise an error for bad responses
                print("Response from get_districts_with_community:", response.json())
            except requests.exceptions.RequestException as e:
                print(f"Error fetching districts by state: {e}")
                return "failure"
            print("Response from get_districts_with_community:", response.json())
            # Parse the response to get districts
            districts_data = response.json().get("data", [])
            print("Districts data:", districts_data)
            for district in districts_data:
                print("District:", district, district.get("id"), district.get("name"))
            districts_list = [{"value": district.get("id"), "label": district.get("name"), "description": ""} for
                              district in districts_data]

            send_districts_response = bot_interface.api.send_list_msg(
                bot_instance_id=bot_instance_id,
                contact_number=user.phone,
                text="कृपया अपना जिला चुनें",
                menu_list=districts_list
            )

            print("List message response:", send_districts_response)

            # Save user session
            user.save()

            # Return success/failure based on API response
            if send_districts_response and send_districts_response.get('messages'):
                return "success"
            else:
                return "failure"


        except Exception as e:
            print(f"Error in sendDistricts: {e}")
            return "failure"

    def sendCommunityByStateDistrict(self, bot_instance_id, data_dict):
        """
        Send community options based on selected state and district.
        This function prepares the user session and sends a request for community selection.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure" based on operation result.
        """
        print("in sendCommunityByStateDistrict")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            if bot_instance:
                print("bot_instance", bot_instance.language)

            user_id = data_dict.get("user_id")
            print("user in sendCommunityByStateDistrict", user_id)

            # check if user is created
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print("user in sendCommunityByStateDistrict", user)

            # Set user session properties
            user.expected_response_type = "community"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

            data = data_dict.get("data", {})
            print("data in sendCommunityByStateDistrict:", data)

            # Initialize variables
            state_id = None
            district_id = None

            # Check if data_dict has 'getDataFrom' key
            if 'getDataFrom' in data:
                get_data_from_list = data['getDataFrom']
                print("getDataFrom configuration:", get_data_from_list)

                current_session = user.current_session
                print("Current session in sendCommunityByStateDistrict:", type(current_session), current_session)

                # Extract state and district IDs from session data
                for get_data_from in get_data_from_list:
                    state_name = get_data_from.get("state", "")
                    field_name = get_data_from.get("field", "data")

                    print(f"Looking for state: {state_name}, field: {field_name}")

                    if state_name == "SendState":
                        # Get state_id from SendState session data
                        if current_session and len(current_session) > 0 and state_name in current_session[0]:
                            state_id = current_session[0][state_name].get(field_name, "")
                            print(f"Extracted state_id: {state_id}")
                    elif state_name == "SendDistrict":
                        # Get district_id from SendDistrict session data
                        if current_session and len(current_session) > 0 and state_name in current_session[0]:
                            district_id = current_session[0][state_name].get(field_name, "")
                            print(f"Extracted district_id: {district_id}")

            # Check if we have the required state and district data
            if not state_id or not district_id:
                print(f"Missing required data - state_id: {state_id}, district_id: {district_id}")
                return "failure"

            # # Get state and district names for API call
            # try:
            #     state_obj = State.objects.get(pk=state_name)
            #     district_obj = District.objects.get(pk=district_name)
            #     state_name = state_obj.state_name
            #     district_name = district_obj.district_name
            #     print(f"State name: {state_name}, District name: {district_name}")
            # except (State.DoesNotExist, District.DoesNotExist) as e:
            #     print(f"Error getting state/district names: {e}")
            #     return "failure"

            # Call the API to get communities by location
            try:
                response = requests.get(
                    # url=f"{settings.COMMUNITY_ENGAGEMENT_API_URL}get_communities_by_location/",
                    url=f"http://localhost:8000/api/v1/get_communities_by_location/",
                    params={
                        "state_id": state_id,
                        "district_id": district_id
                    },
                    timeout=30
                )
                response.raise_for_status()
                api_response = response.json()
                print("API response from get_communities_by_location:", api_response)

                # Check if the API call was successful
                if api_response.get("success") and api_response.get("data"):
                    communities_data = api_response["data"]
                    print("Communities data:", communities_data)

                    # Format communities for WhatsApp list message
                    communities_list = []
                    for community in communities_data:
                        communities_list.append({
                            "value": community.get("community_id"),
                            "label": community.get("name"),
                            "description": community.get("description", "")
                        })

                    print("Communities list for WhatsApp:", communities_list)

                    # Send the communities list to user
                    if communities_list:
                        send_communities_response = bot_interface.api.send_list_msg(
                            bot_instance_id=bot_instance_id,
                            contact_number=user.phone,
                            text="कृपया अपना समुदाय चुनें",
                            menu_list=communities_list
                        )

                        print("Communities list message response:", send_communities_response)

                        # Save user session
                        user.save()

                        # Return success/failure based on API response
                        if send_communities_response and send_communities_response.get('messages'):
                            return "success"
                        else:
                            return "failure"
                    else:
                        print("No communities found for the selected state and district")
                        return "failure"
                else:
                    print("API response indicates failure or no data")
                    return "failure"

            except requests.exceptions.RequestException as e:
                print(f"Error calling get_communities_by_location API: {e}")
                return "failure"

        except Exception as e:
            print(f"Error in sendCommunityByStateDistrict: {e}")
            return "failure"

    def addUserToCommunity(self, bot_instance_id, data_dict):
        """
        Add user to a community.
        This function prepares the user session and adds the user to the selected community.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure" based on operation result.
        """
        print("in addUserToCommunity")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            if bot_instance:
                print("bot_instance", bot_instance.language)

            user_id = data_dict.get("user_id")
            print("user in addUserToCommunity", user_id)

            # check if user is created
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print("user in addUserToCommunity", user)

            # Set user session properties
            user.expected_response_type = "button"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

            data = data_dict.get("data", {})
            print("data in addUserToCommunity:", data)

            # Initialize variable
            community_id = None

            # Check if data_dict has 'getDataFrom' key
            if 'getDataFrom' in data:
                get_data_from_config = data['getDataFrom']
                print("getDataFrom configuration:", get_data_from_config)

                # Normalize getDataFrom to list format for consistent processing
                if isinstance(get_data_from_config, dict):
                    # Single object - convert to list
                    get_data_from_list = [get_data_from_config]
                elif isinstance(get_data_from_config, list):
                    # Already a list
                    get_data_from_list = get_data_from_config
                else:
                    print(f"Invalid getDataFrom format: {type(get_data_from_config)}")
                    return "failure"

                print("Normalized getDataFrom list:", get_data_from_list)

                current_session = user.current_session
                print("Current session in addUserToCommunity:", type(current_session), current_session)

                # Extract community ID from session data
                for get_data_from in get_data_from_list:
                    state_name = get_data_from.get("state", "")
                    field_name = get_data_from.get("field", "misc")

                    print(f"Looking for state: {state_name}, field: {field_name}")

                    if state_name == "CommunityByStateDistrict":
                        # Get community_id from CommunityByStateDistrict session data
                        if current_session and len(current_session) > 0:
                            print(f"Session structure check - type: {type(current_session[0])}")
                            if state_name in current_session[0]:
                                state_data = current_session[0][state_name]
                                print(f"State data type: {type(state_data)}, content: {state_data}")

                                if isinstance(state_data, dict):
                                    community_id = state_data.get(field_name)
                                    print(f"Extracted community_id: {community_id}")
                                else:
                                    print(f"ERROR: State data is not a dict, it's {type(state_data)}: {state_data}")
                                    return "failure"
                            else:
                                print(f"State {state_name} not found in session")
                        else:
                            print("No current session data available")
                    elif state_name == "CommunityByLocation":
                        if current_session and len(current_session) > 0:
                            print(f"Session structure check - type: {type(current_session[0])}")
                            if state_name in current_session[0]:
                                state_data = current_session[0][state_name]
                                print(f"State data type: {type(state_data)}, content: {state_data}")

                                if isinstance(state_data, dict):
                                    community_id = state_data.get(field_name)
                                    print(f"Extracted community_id: {community_id}")
                                else:
                                    print(f"ERROR: State data is not a dict, it's {type(state_data)}: {state_data}")
                                    return "failure"
                # add user to community
                if community_id:
                    print(f"Adding user {user.phone} to community {community_id}")
                    try:
                        response = requests.post(
                            # url=f"{settings.COMMUNITY_ENGAGEMENT_API_URL}add_user_to_community/",
                            url="http://localhost:8000/api/v1/add_user_to_community/",
                            data={
                                "community_id": community_id,
                                "number": user.phone
                            },
                            timeout=30
                        )
                        response.raise_for_status()
                        api_response = response.json()
                        print("Add user to community API response:", api_response)

                        # Return success/failure based on API response
                        if api_response.get('success'):
                            # Save community membership data locally
                            try:
                                from bot_interface.utils import add_community_membership

                                # Extract community data from API response
                                community_data = {
                                    'community_id': community_id,
                                    'community_name': api_response.get('community_name', ''),
                                    'community_description': api_response.get('community_description', ''),
                                    'organization': api_response.get('organization', '')
                                }

                                # If API didn't return complete data, try database fallback
                                if not community_data['community_name'] or not community_data['organization']:
                                    print("API returned incomplete data, trying database fallback...")
                                    try:
                                        from community_engagement.models import Community_user_mapping
                                        from users.models import User

                                        # Get user's phone number
                                        bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                                        phone_number = bot_user.user.contact_number

                                        # Find user and community mapping
                                        user_obj = User.objects.get(contact_number=phone_number)
                                        community_mapping = Community_user_mapping.objects.filter(
                                            user=user_obj,
                                            community_id=community_id
                                        ).select_related('community', 'community__project').first()

                                        if community_mapping:
                                            # Update with database data
                                            if not community_data[
                                                'community_name'] and community_mapping.community.project:
                                                community_data[
                                                    'community_name'] = community_mapping.community.project.name
                                            if not community_data[
                                                'organization'] and community_mapping.community.project and community_mapping.community.project.organization:
                                                community_data[
                                                    'organization'] = community_mapping.community.project.organization.name
                                            if not community_data[
                                                'community_description'] and community_mapping.community.project:
                                                community_data['community_description'] = getattr(
                                                    community_mapping.community.project, 'description', '')

                                            print(f"Enhanced community data from database: {community_data}")

                                    except Exception as db_e:
                                        print(f"Database fallback failed: {db_e}")
                                        # Use defaults if database lookup fails
                                        if not community_data['community_name']:
                                            community_data['community_name'] = f"Community {community_id}"
                                        if not community_data['organization']:
                                            community_data['organization'] = "Unknown Organization"

                                # Get the BotUsers object from user_id
                                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)

                                # Add community membership to user's local data
                                add_community_membership(bot_user, community_data)
                                print(
                                    f"Successfully added community membership data for user {user.phone}: {community_data}")

                            except Exception as e:
                                print(f"Error saving community membership data: {e}")
                                # Continue with success even if local tracking fails

                            # Save user session only on success
                            user.save()
                            return "success"
                        else:
                            print("API response indicates failure")
                            return "failure"

                    except requests.exceptions.RequestException as e:
                        print(f"Error calling add_user_to_community API: {e}")
                        return "failure"
                    except json.JSONDecodeError as e:
                        print(f"Error parsing API response: {e}")
                        return "failure"
                else:
                    print("No community ID found in session data")
                    return "failure"

        except Exception as e:
            print(f"Error in addUserToCommunity: {e}")
            return "failure"

    def get_user_communities(self, bot_instance_id, data_dict):
        """
        Get user communities using hybrid API + database fallback approach.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "single_community", "multiple_communities", or "failure"
        """
        print("in get_user_communities")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            user.save()

            # Get phone number from BotUser
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            phone_number = bot_user.user.contact_number

            print(f"Getting communities for phone number: {phone_number}")

            # Sync community data from database for existing users (if needed)
            try:
                from bot_interface.utils import sync_community_data_from_database
                sync_community_data_from_database(bot_user)
            except Exception as sync_e:
                print(f"Community data sync failed (non-critical): {sync_e}")

            # First try API approach
            communities = self._get_communities_via_api(phone_number)

            # If API fails, fallback to database approach
            if not communities:
                print("API approach failed, trying database fallback...")
                communities = self._get_communities_via_database(phone_number)

            # Determine community flow based on count
            return self._determine_community_flow(communities, user_id)

        except Exception as e:
            print(f"Error in get_user_communities: {e}")
            return "failure"

    def _get_communities_via_api(self, phone_number):
        """
        Get communities using API approach.
        Args:
            phone_number (str): User's phone number
        Returns:
            list: List of communities or empty list if failed
        """
        try:
            print(f"Trying API approach for phone number: {phone_number}")
            # base_url = settings.COMMUNITY_ENGAGEMENT_API_URL.rstrip('/')
            response = requests.get(
                f"http://localhost:8000/api/v1/get_community_by_user/",
                params={"number": phone_number},
                timeout=10
            )

            print(f"API response status: {response.status_code}")

            if response.status_code == 200:
                response_data = response.json()
                print(f"API response data: {response_data}")

                if response_data.get("success"):
                    communities = response_data.get("data", [])
                    print(f"API returned {len(communities)} communities: {communities}")
                    return communities
                else:
                    print(f"API returned success=False: {response_data}")
            else:
                print(f"API returned non-200 status: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
        except Exception as e:
            print(f"Error in API approach: {e}")

        return []

    def _get_communities_via_database(self, phone_number):
        """
        Get communities using direct database queries as fallback.
        Args:
            phone_number (str): User's phone number
        Returns:
            list: List of communities or empty list if failed
        """
        try:
            print(f"Trying database approach for phone number: {phone_number}")

            # Import required models
            from community_engagement.models import Community_user_mapping
            from users.models import User

            # Find user by contact number
            try:
                user_obj = User.objects.get(contact_number=phone_number)
                print(f"Found user in database: {user_obj.id}")
            except User.DoesNotExist:
                print(f"User not found in database for phone number: {phone_number}")
                return []

            # Get community mappings for this user
            community_mappings = Community_user_mapping.objects.filter(user=user_obj).select_related('community',
                                                                                                     'community__project')

            communities = []
            for mapping in community_mappings:
                # Get community name from the related project
                community_name = mapping.community.project.name if mapping.community.project else f"Community {mapping.community.id}"

                community_data = {
                    'community_id': mapping.community.id,
                    'community_name': community_name,
                    'community_description': getattr(mapping.community.project, 'description',
                                                     '') if mapping.community.project else '',
                    'organization': mapping.community.project.organization.name if (
                                mapping.community.project and mapping.community.project.organization) else '',
                    'created_at': mapping.created_at.isoformat() if hasattr(mapping, 'created_at') else None
                }
                communities.append(community_data)

            print(f"Database returned {len(communities)} communities: {communities}")
            return communities

        except Exception as e:
            print(f"Error in database approach: {e}")
            return []

    def _determine_community_flow(self, communities, user_id):
        """
        Determine community flow based on community count.
        Args:
            communities (list): List of user communities
            user_id (int): User ID for logging
        Returns:
            str: "single_community", "multiple_communities", or "failure"
        """
        community_count = len(communities)
        print(f"Determining flow for {community_count} communities for user {user_id}")

        if community_count == 1:
            print("User has single community")
            return "single_community"
        elif community_count > 1:
            print("User has multiple communities")
            return "multiple_communities"
        else:
            print("User has no communities - this shouldn't happen in community features flow")
            return "failure"

    def display_single_community_message(self, bot_instance_id, data_dict):
        """
        Display welcome message for users with single community.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in display_single_community_message")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get BotUsers object to access user_misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get('community_membership', {}).get('current_communities', [])

            if len(current_communities) > 0:
                community_name = current_communities[0].get('community_name', 'Unknown Community')

                # Create welcome message
                welcome_text = f"🏠 आप {community_name} समुदाय का हिस्सा हैं।\n\nआप कैसे आगे बढ़ना चाहेंगे?"

                # Send text message
                response = bot_interface.api.send_text(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=welcome_text
                )

                print(f"Single community welcome message sent: {response}")

                if response and response.get('messages'):
                    return "success"
                else:
                    return "failure"
            else:
                print("No communities found for user")
                return "failure"

        except Exception as e:
            print(f"Error in display_single_community_message: {e}")
            return "failure"

    def display_multiple_community_message(self, bot_instance_id, data_dict):
        """
        Display welcome message for users with multiple communities.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in display_multiple_community_message")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get BotUsers object to access user_misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get('community_membership', {}).get('current_communities', [])

            if len(current_communities) > 0:
                # Get fresh community data with last accessed info
                success, api_response = bot_interface.utils.check_user_community_status_http(user.phone)
                if success and api_response.get('success'):
                    community_data = api_response.get('data', {})
                    last_accessed_id = community_data.get('misc', {}).get('last_accessed_community_id')

                    # Find the last accessed community name
                    communities_list = community_data.get('data', [])
                    last_community_name = "Unknown Community"
                    for community in communities_list:
                        if community.get('community_id') == last_accessed_id:
                            last_community_name = community.get('name', 'Unknown Community')
                            break
                else:
                    # Fallback to first community
                    last_community_name = current_communities[0].get('community_name', 'Unknown Community')

                # Create welcome message
                welcome_text = f"🏠 आपने पिछली बार {last_community_name} समुदाय का उपयोग किया था।"

                # Send text message
                response = bot_interface.api.send_text(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=welcome_text
                )

                print(f"Multiple community welcome message sent: {response}")

                if response and response.get('messages'):
                    return "success"
                else:
                    return "failure"
            else:
                print("No communities found for user")
                return "failure"

        except Exception as e:
            print(f"Error in display_multiple_community_message: {e}")
            return "failure"

    def generate_community_menu(self, bot_instance_id, data_dict):
        """
        Generate dynamic menu from user's communities.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in generate_community_menu")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            user.expected_response_type = "button"
            user.current_state = data_dict.get("state")

            # Handle SMJ object lookup
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            user.save()

            # Get BotUsers object to access user_misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get('community_membership', {}).get('current_communities', [])

            if len(current_communities) > 0:
                # Get fresh community data with last accessed info
                success, api_response = bot_interface.utils.check_user_community_status_http(user.phone)
                if success and api_response.get('success'):
                    community_data = api_response.get('data', {})
                    last_accessed_id = community_data.get('misc', {}).get('last_accessed_community_id')
                    communities_list = community_data.get('data', [])

                    # Create menu excluding the last accessed community
                    communities_menu_list = []
                    for community in communities_list:
                        community_id = community.get('community_id')
                        if community_id != last_accessed_id:  # Exclude last accessed
                            communities_menu_list.append({
                                "value": f"community_{community_id}",
                                "label": community.get('name', 'Unknown Community'),
                                "description": f"Select {community.get('name', 'Unknown Community')}"
                            })
                else:
                    # Fallback to existing logic (show all communities)
                    communities_menu_list = []
                    for community in current_communities:
                        community_id = community.get('community_id')
                        community_name = community.get('community_name', 'Unknown Community')

                        communities_menu_list.append({
                            "value": f"community_{community_id}",
                            "label": community_name,
                            "description": f"Select {community_name}"
                        })

                # Add option to continue with last accessed community
                communities_menu_list.append({
                    "value": "continue_last_accessed",
                    "label": "पिछला समुदाय चुनें",
                    "description": "अपने पिछले समुदाय के साथ वापस जाएं"
                })

                print(f"Generated community menu: {communities_menu_list}")

                # Send community selection menu
                response = bot_interface.api.send_list_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text="कृपया अपना समुदाय चुनें:",
                    menu_list=communities_menu_list,
                    button_label="समुदाय चुनें"
                )

                print(f"Community menu sent: {response}")

                if response and response.get('messages'):
                    return "success"
                else:
                    return "failure"
            else:
                print("No communities found for user")
                return "failure"

        except Exception as e:
            print(f"Error in generate_community_menu: {e}")
            return "failure"

    def store_active_community_and_context(self, bot_instance_id, data_dict):
        """
        Store active community ID and navigation context.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in store_active_community_and_context")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get BotUsers object
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get('community_membership', {}).get('current_communities', [])

            # Get the event to determine which community to store
            # First try direct event, then extract from event_data for button events
            event = data_dict.get("event", "")
            if not event:
                # For button events, extract from event_data.misc
                event_data = data_dict.get("event_data", {})
                event = event_data.get("misc", "")
                print(f"Extracted event from button data: {event}")

            print(f"Storing community for event: '{event}'")

            if event == "continue_single":
                # Single community - store the only community
                if len(current_communities) > 0:
                    community_id = current_communities[0].get('community_id')
                    context = "single_community"
                else:
                    print("No communities found for single community user")
                    return "failure"

            elif event == "continue_last":
                # Multiple communities - store the actual last accessed community
                success, api_response = bot_interface.utils.check_user_community_status_http(user.phone)
                if success and api_response.get('success'):
                    community_data = api_response.get('data', {})
                    last_accessed_id = community_data.get('misc', {}).get('last_accessed_community_id')
                    if last_accessed_id:
                        community_id = str(last_accessed_id)
                        context = "multiple_community"
                    else:
                        print("No last accessed community ID found in API response")
                        return "failure"
                else:
                    # Fallback to first community if API fails
                    if len(current_communities) > 0:
                        community_id = current_communities[0].get('community_id')
                        context = "multiple_community"
                    else:
                        print("No communities found for multiple community user")
                        return "failure"

            elif event == "join_new":
                # User wants to join a new community - return original event for proper transition
                print("User selecting to join new community - no community storage required")
                return "join_new"

            elif event == "choose_other":
                # User wants to choose from multiple communities - return original event for proper transition
                print("User selecting to choose from other communities - no community storage required")
                return "choose_other"

            else:
                print(f"Unknown event for community storage: '{event}'")
                return "failure"

            # Store in UserSessions.misc_data
            if not user.misc_data:
                user.misc_data = {}

            user.misc_data['active_community_id'] = community_id
            user.misc_data['navigation_context'] = context
            user.misc_data['last_service_event'] = event
            user.save()

            print(f"Stored active community {community_id} with context {context}")
            # Return the original event for proper state transitions
            return event

        except Exception as e:
            print(f"Error in store_active_community_and_context: {e}")
            return "failure"

    def store_selected_community_and_context(self, bot_instance_id, data_dict):
        """
        Store selected community from menu and context.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "community_selected" or "failure"
        """
        print(f"DEBUG: store_selected_community_and_context called with bot_instance_id={bot_instance_id}")
        print(f"DEBUG: data_dict keys: {list(data_dict.keys())}")
        print(f"DEBUG: data_dict contents: {data_dict}")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")
            print(f"DEBUG: bot_instance={bot_instance}, user_id={user_id}")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            print(f"DEBUG: Found user session: {user}")

            # Extract community ID from button data or event
            community_id = None
            event_data = data_dict.get("event_data", {})
            print(f"DEBUG: event_data: {event_data}")
            print(f"DEBUG: event_data type: {event_data.get('type')}")

            if event_data.get("type") == "button":
                # For button events, extract community ID from button value (misc field)
                button_value = event_data.get("misc") or event_data.get("data")
                print(f"DEBUG: Button event detected - button_value: {button_value}")

                if button_value == "continue_last_accessed":
                    # User wants to continue with last accessed community
                    print(f"DEBUG: User chose to continue with last accessed community")
                    # Get last accessed community from API or user data
                    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                    success, api_response = bot_interface.utils.check_user_community_status_http(user.phone)
                    if success and api_response.get('success'):
                        community_data = api_response.get('data', {})
                        community_id = community_data.get('misc', {}).get('last_accessed_community_id')
                        print(f"DEBUG: Got last accessed community ID from API: {community_id}")
                    else:
                        # Fallback to stored data
                        community_id = bot_user.user_misc.get('community_membership', {}).get(
                            'last_accessed_community_id')
                        print(f"DEBUG: Got last accessed community ID from stored data: {community_id}")
                elif button_value and button_value.startswith("community_"):
                    community_id = button_value.split("_")[1]
                    print(f"DEBUG: Extracted community ID from button: {community_id}")
                else:
                    print(f"DEBUG: Button value doesn't start with 'community_': {button_value}")
            else:
                # For non-button events, extract from event field
                event = data_dict.get("event", "")
                print(f"DEBUG: Non-button event - processing event: {event}")

                if event == "continue_last_accessed":
                    # User wants to continue with last accessed community
                    print(f"DEBUG: User chose to continue with last accessed community (event)")
                    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                    success, api_response = bot_interface.utils.check_user_community_status_http(user.phone)
                    if success and api_response.get('success'):
                        community_data = api_response.get('data', {})
                        community_id = community_data.get('misc', {}).get('last_accessed_community_id')
                        print(f"DEBUG: Got last accessed community ID from API: {community_id}")
                    else:
                        # Fallback to stored data
                        community_id = bot_user.user_misc.get('community_membership', {}).get(
                            'last_accessed_community_id')
                        print(f"DEBUG: Got last accessed community ID from stored data: {community_id}")
                elif event.startswith("community_"):
                    community_id = event.split("_")[1]
                    print(f"DEBUG: Extracted community ID from event: {community_id}")

            print(f"DEBUG: Final community_id: {community_id}")

            if community_id:
                # Store in UserSessions.misc_data
                if not user.misc_data:
                    user.misc_data = {}

                user.misc_data['active_community_id'] = community_id
                user.misc_data['navigation_context'] = "community_selection"
                user.misc_data['last_service_event'] = "choose_other"
                user.save()

                print(f"DEBUG: Stored selected community {community_id} with context community_selection")
                print(f"DEBUG: Returning 'community_selected'")
                return "community_selected"
            else:
                print(f"DEBUG: Could not extract community ID from event data, returning 'failure'")
                return "failure"

        except Exception as e:
            print(f"DEBUG: Exception in store_selected_community_and_context: {e}")
            import traceback
            traceback.print_exc()
            return "failure"

    def display_service_menu_message(self, bot_instance_id, data_dict):
        """
        Display contextual service menu message.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in display_service_menu_message")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get active community name for context
            active_community_id = user.misc_data.get('active_community_id') if user.misc_data else None

            if active_community_id:
                # Get BotUsers object to find community name
                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                current_communities = bot_user.user_misc.get('community_membership', {}).get('current_communities', [])

                # Find the active community name
                community_name = "आपके समुदाय"  # Default fallback
                for community in current_communities:
                    if str(community.get('community_id')) == str(active_community_id):
                        community_name = community.get('community_name', community_name)
                        break

                # Create contextual service menu message
                service_text = f"📋 {community_name} के लिए सेवाएं\n\nआप क्या करना चाहते हैं:"
            else:
                # Fallback message if no active community
                service_text = "📋 समुदाय सेवाएं\n\nआप क्या करना चाहते हैं:"

            # Send service menu message
            response = bot_interface.api.send_text(
                bot_instance_id=bot_instance_id,
                contact_number=user.phone,
                text=service_text
            )

            print(f"Service menu message sent: {response}")

            if response and response.get('messages'):
                return "success"
            else:
                return "failure"

        except Exception as e:
            print(f"Error in display_service_menu_message: {e}")
            return "failure"

    def handle_service_selection(self, bot_instance_id, data_dict):
        """
        Handle back navigation based on stored context.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "back_from_single", "back_from_multiple", "back_from_selection", or event passed through
        """
        print("in handle_service_selection")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get the event
            event = data_dict.get("event", "")
            print(f"Handling service selection for event: {event}")

            # For all events (work_demand, grievance, exit_session, etc.), pass through the event
            return event

        except Exception as e:
            print(f"Error in handle_service_selection: {e}")
            return "failure"

    def store_location_data(self, bot_instance_id, data_dict):
        """
        Store location data from SendLocationRequest in work demand flow.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in store_location_data")

        try:
            # Get bot instance with better error handling
            try:
                bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            except bot_interface.models.Bot.DoesNotExist:
                print(f"Bot instance with ID {bot_instance_id} not found, trying to get any bot instance")
                bot_instance = bot_interface.models.Bot.objects.first()
                if not bot_instance:
                    print("No bot instances found in database")
                    return "failure"

            user_id = data_dict.get("user_id")
            if isinstance(user_id, str):
                user_id = int(user_id)

            # Get user session with better error handling
            try:
                user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            except bot_interface.models.UserSessions.DoesNotExist:
                print(f"UserSession not found for user_id: {user_id}, bot: {bot_instance}")
                # Try to get user session without bot constraint
                try:
                    user = bot_interface.models.UserSessions.objects.get(user=user_id)
                    print(f"Found user session for user_id: {user_id} without bot constraint")
                except bot_interface.models.UserSessions.DoesNotExist:
                    print(f"No user session found for user_id: {user_id}")
                    return "failure"

            # Get location data from current event packet instead of searching old session states
            location_data = None

            # Try to get location data from data_dict first (current event packet)
            if "location_data" in data_dict:
                location_raw_dict = data_dict.get("location_data")
                if location_raw_dict:
                    # Use the structured location data from event packet
                    location_data = {
                        "latitude": str(location_raw_dict.get("latitude", "")),
                        "longitude": str(location_raw_dict.get("longitude", "")),
                        "address": location_raw_dict.get("address", ""),
                        "name": location_raw_dict.get("name", "")
                    }
                    print(f"Found structured location data in data_dict: {location_data}")
            elif "event_data" in data_dict:
                # Extract from event_data if available
                event_data = data_dict.get("event_data")
                if event_data and event_data.get("type") == "location":
                    location_raw = event_data.get("data", "")
                    misc_data = event_data.get("misc", {})

                    if isinstance(misc_data, dict) and misc_data.get("latitude") and misc_data.get("longitude"):
                        # Use misc data (preferred for WhatsApp location data)
                        location_data = {
                            "latitude": str(misc_data.get("latitude", "")),
                            "longitude": str(misc_data.get("longitude", "")),
                            "address": misc_data.get("address", ""),
                            "name": misc_data.get("name", "")
                        }
                        print(f"Found location data in event_data misc: {location_data}")
                    elif location_raw and isinstance(location_raw, str) and "," in location_raw:
                        # Fallback to raw coordinate string
                        lat, lon = location_raw.split(",", 1)
                        location_data = {
                            "latitude": lat.strip(),
                            "longitude": lon.strip(),
                            "address": "",
                            "name": ""
                        }
                        print(f"Found location data in event_data raw: {location_data}")

            # Fallback: Look for location data in current session if not found in event packet
            if not location_data:
                current_session = user.current_session
                print(f"Location not found in event packet, checking current session: {current_session}")

                if current_session and len(current_session) > 0:
                    # Find the SendLocationRequest state in session
                    for session_item in current_session:
                        if "SendLocationRequest" in session_item:
                            location_raw = session_item["SendLocationRequest"].get("data", "")
                            misc_data = session_item["SendLocationRequest"].get("misc", {})

                            # Parse location data
                            if location_raw and isinstance(location_raw, str) and "," in location_raw:
                                # Format: "latitude,longitude"
                                lat, lon = location_raw.split(",", 1)
                                location_data = {
                                    "latitude": lat.strip(),
                                    "longitude": lon.strip(),
                                    "address": misc_data.get("address", ""),
                                    "name": misc_data.get("name", "")
                                }
                            elif isinstance(misc_data, dict):
                                # Use misc data if available
                                location_data = {
                                    "latitude": misc_data.get("latitude", ""),
                                    "longitude": misc_data.get("longitude", ""),
                                    "address": misc_data.get("address", ""),
                                    "name": misc_data.get("name", "")
                                }
                            break

            if location_data:
                # Initialize misc_data if needed
                if not user.misc_data:
                    user.misc_data = {}

                # Initialize work_demand structure
                if "work_demand" not in user.misc_data:
                    user.misc_data["work_demand"] = {}

                # Store location data
                user.misc_data["work_demand"]["location"] = location_data
                user.save()

                print(f"Successfully stored location data: {location_data}")
                return "success"
            else:
                print("No location data found in session")
                return "failure"

        except Exception as e:
            print(f"Error in store_location_data: {e}")
            return "failure"

    def store_audio_data(self, bot_instance_id, data_dict):
        """
        Store audio data from RequestAudio in work demand flow.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in store_audio_data")

        try:
            # Get bot instance with better error handling
            try:
                bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            except bot_interface.models.Bot.DoesNotExist:
                print(f"Bot instance with ID {bot_instance_id} not found, trying to get any bot instance")
                bot_instance = bot_interface.models.Bot.objects.first()
                if not bot_instance:
                    print("No bot instances found in database")
                    return "failure"

            user_id = data_dict.get("user_id")
            if isinstance(user_id, str):
                user_id = int(user_id)

            # Get user session with better error handling
            try:
                user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            except bot_interface.models.UserSessions.DoesNotExist:
                print(f"UserSession not found for user_id: {user_id}, bot: {bot_instance}")
                # Try to get user session without bot constraint
                try:
                    user = bot_interface.models.UserSessions.objects.get(user=user_id)
                    print(f"Found user session for user_id: {user_id} without bot constraint")
                except bot_interface.models.UserSessions.DoesNotExist:
                    print(f"No user session found for user_id: {user_id}")
                    return "failure"

            # Get audio data from current event packet instead of searching old session states
            audio_data = None

            # Try to get audio data from data_dict first (current event packet)
            if "audio_data" in data_dict:
                audio_raw_dict = data_dict.get("audio_data")
                if audio_raw_dict:
                    # Use the structured audio data from event packet
                    audio_data = audio_raw_dict.get("file_path") or audio_raw_dict.get("data")
                    print(f"Found structured audio data in data_dict: {audio_data}")
            elif "event_data" in data_dict:
                # Extract from event_data if available
                event_data = data_dict.get("event_data")
                if event_data and event_data.get("type") in ["audio", "voice"]:
                    audio_data = event_data.get("data", "")
                    print(f"Found audio data in event_data: {audio_data}")

            # Fallback: Look for audio data in current session if not found in event packet
            if not audio_data:
                current_session = user.current_session
                print(f"Audio not found in event packet, checking current session: {current_session}")

                if current_session and len(current_session) > 0:
                    # Find the RequestAudio state in session
                    for session_item in current_session:
                        if "RequestAudio" in session_item:
                            audio_data = session_item["RequestAudio"].get("data", "")
                            break

            if audio_data:
                # Initialize misc_data if needed
                if not user.misc_data:
                    user.misc_data = {}

                # Determine which flow we're in based on current SMJ
                flow_type = "work_demand"  # Default
                try:
                    smj_id = data_dict.get("smj_id")
                    if smj_id:
                        smj = bot_interface.models.SMJ.objects.get(id=smj_id)
                        if smj.name == "grievance":
                            flow_type = "grievance"
                        elif smj.name == "work_demand":
                            flow_type = "work_demand"
                        print(f"Detected flow type: {flow_type} (SMJ: {smj.name})")
                except Exception as e:
                    print(f"Could not determine flow type, using default: {e}")

                # Initialize structure based on flow type
                if flow_type not in user.misc_data:
                    user.misc_data[flow_type] = {}

                # Store audio data
                user.misc_data[flow_type]["audio"] = audio_data
                user.save()

                print(f"Successfully stored audio data for {flow_type}: {audio_data}")
                return "success"
            else:
                print("No audio data found in session")
                return "failure"

        except Exception as e:
            print(f"Error in store_audio_data: {e}")
            return "failure"

    def store_photo_data(self, bot_instance_id, data_dict):
        """
        Store photo data from RequestPhotos in work demand flow.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in store_photo_data")

        try:
            # Get bot instance with better error handling
            try:
                bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            except bot_interface.models.Bot.DoesNotExist:
                print(f"Bot instance with ID {bot_instance_id} not found, trying to get any bot instance")
                bot_instance = bot_interface.models.Bot.objects.first()
                if not bot_instance:
                    print("No bot instances found in database")
                    return "failure"

            user_id = data_dict.get("user_id")
            if isinstance(user_id, str):
                user_id = int(user_id)

            # Get user session with better error handling
            try:
                user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            except bot_interface.models.UserSessions.DoesNotExist:
                print(f"UserSession not found for user_id: {user_id}, bot: {bot_instance}")
                # Try to get user session without bot constraint
                try:
                    user = bot_interface.models.UserSessions.objects.get(user=user_id)
                    print(f"Found user session for user_id: {user_id} without bot constraint")
                except bot_interface.models.UserSessions.DoesNotExist:
                    print(f"No user session found for user_id: {user_id}")
                    return "failure"

            # Get photo data from current event packet instead of searching old session states
            photo_data = None

            # Try to get photo data from data_dict first (current event packet)
            if "photo_data" in data_dict:
                photo_raw_dict = data_dict.get("photo_data")
                if photo_raw_dict:
                    # Use the structured photo data from event packet
                    photo_data = photo_raw_dict.get("file_path") or photo_raw_dict.get("data")
                    print(f"Found structured photo data in data_dict: {photo_data}")
            elif "event_data" in data_dict:
                # Extract from event_data if available
                event_data = data_dict.get("event_data")
                if event_data and event_data.get("type") == "image":
                    photo_data = event_data.get("data", "")
                    print(f"Found photo data in event_data: {photo_data}")

            # Fallback: Look for RequestPhotos state data in session (old method)
            if not photo_data:
                current_session = user.current_session
                print(f"Current session for photo storage: {current_session}")

                if current_session and len(current_session) > 0:
                    # Find the RequestPhotos state in session
                    for session_item in current_session:
                        if "RequestPhotos" in session_item:
                            photo_data = session_item["RequestPhotos"].get("data", "")
                            break

            if photo_data:
                # Initialize misc_data if needed
                if not user.misc_data:
                    user.misc_data = {}

                # Determine which flow we're in based on current SMJ
                flow_type = "work_demand"  # Default
                try:
                    smj_id = data_dict.get("smj_id")
                    if smj_id:
                        smj = bot_interface.models.SMJ.objects.get(id=smj_id)
                        if smj.name == "grievance":
                            flow_type = "grievance"
                        elif smj.name == "work_demand":
                            flow_type = "work_demand"
                        print(f"Detected flow type for photos: {flow_type} (SMJ: {smj.name})")
                except Exception as e:
                    print(f"Could not determine flow type for photos, using default: {e}")

                # Initialize structure based on flow type
                if flow_type not in user.misc_data:
                    user.misc_data[flow_type] = {}

                # Initialize photos array if needed
                if "photos" not in user.misc_data[flow_type]:
                    user.misc_data[flow_type]["photos"] = []

                # Store photo data (append to list if multiple photos)
                if isinstance(photo_data, list):
                    user.misc_data[flow_type]["photos"].extend(photo_data)
                else:
                    user.misc_data[flow_type]["photos"].append(photo_data)

                user.save()

                print(f"Successfully stored photo data for {flow_type}: {photo_data}")
                return "success"
            else:
                print("No photo data found in session")
                return "failure"

        except Exception as e:
            print(f"Error in store_photo_data: {e}")
            return "failure"

    def archive_and_end_session(self, bot_instance_id, data_dict):
        """
        Archive current session and end it completely.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in archive_and_end_session")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)

            # Get BotUsers object for archiving
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)

            # Create archive entry
            archive_data = {
                "session_data": user.current_session,
                "misc_data": user.misc_data,
                "final_state": user.current_state,
                "session_duration": (user.last_updated_at - user.started_at).total_seconds(),
                "archived_reason": "work_demand_completion"
            }

            # Create UserArchive entry
            bot_interface.models.UserArchive.objects.create(
                app_type=user.app_type,
                bot=bot_instance,
                user=bot_user,
                session_data=archive_data
            )

            print(f"Successfully archived session for user {user_id}")

            # Clear session data
            user.current_session = {}
            user.current_smj = None
            user.current_state = ""
            user.expected_response_type = "text"
            user.misc_data = {}
            user.save()

            print(f"Successfully cleared session data for user {user_id}")

            return "success"

        except Exception as e:
            print(f"Error in archive_and_end_session: {e}")
            return "failure"

    def log_work_demand_completion(self, bot_instance_id, data_dict):
        """
        Log complete work demand data to UserLogs when RequestPhotos transitions to ThankYou.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in log_work_demand_completion")

        try:
            # Get bot instance with better error handling
            try:
                bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            except bot_interface.models.Bot.DoesNotExist:
                print(f"Bot instance with ID {bot_instance_id} not found, trying to get any bot instance")
                bot_instance = bot_interface.models.Bot.objects.first()
                if not bot_instance:
                    print("No bot instances found in database")
                    return "failure"

            user_id = data_dict.get("user_id")
            if isinstance(user_id, str):
                user_id = int(user_id)

            # Get user session with better error handling
            try:
                user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            except bot_interface.models.UserSessions.DoesNotExist:
                print(f"UserSession not found for user_id: {user_id}, bot: {bot_instance}")
                try:
                    user = bot_interface.models.UserSessions.objects.get(user=user_id)
                    print(f"Found user session for user_id: {user_id} without bot constraint")
                except bot_interface.models.UserSessions.DoesNotExist:
                    print(f"No user session found for user_id: {user_id}")
                    return "failure"

            # Get BotUsers object for UserLogs
            try:
                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            except bot_interface.models.BotUsers.DoesNotExist:
                print(f"BotUser not found for user_id: {user_id}")
                return "failure"

            # Get work_demand SMJ for reference
            try:
                smj = bot_interface.models.SMJ.objects.get(id=data_dict.get("smj_id"))
            except bot_interface.models.SMJ.DoesNotExist:
                print(f"SMJ not found for smj_id: {data_dict.get('smj_id')}")
                smj = None

            # Collect work demand data from misc_data
            work_demand_data = {}
            if user.misc_data and "work_demand" in user.misc_data:
                work_demand_data = user.misc_data["work_demand"]

                # Log photo paths to confirm HDPI paths are captured
                if "photos" in work_demand_data:
                    print(f"Photo paths being logged: {work_demand_data['photos']}")
                    # Add explicit note about HDPI paths in the data
                    work_demand_data["photos_note"] = "Photo paths are HDPI processed images from WhatsApp media"

            # Collect community context
            community_context = {}
            active_community_id = user.misc_data.get('active_community_id') if user.misc_data else None

            if active_community_id:
                try:
                    from community_engagement.models import Community
                    community = Community.objects.get(id=active_community_id)

                    # Get community details
                    community_context = {
                        "community_id": active_community_id,
                        "community_name": community.project.name if community.project else "Unknown",
                        "organization": community.project.organization.name if community.project and community.project.organization else "Unknown"
                    }

                    # Get location hierarchy from community locations
                    location_hierarchy = {}
                    for location in community.locations.all():
                        if location.state:
                            location_hierarchy["state"] = location.state.state_name
                        if location.district:
                            location_hierarchy["district"] = location.district.district_name
                        if location.block:
                            location_hierarchy["block"] = location.block.block_name

                    community_context["location_hierarchy"] = location_hierarchy

                except Exception as e:
                    print(f"Error getting community context: {e}")
                    community_context = {"community_id": active_community_id,
                                         "error": "Failed to load community details"}

            # Prepare comprehensive misc data
            from datetime import datetime
            comprehensive_misc_data = {
                "work_demand_data": work_demand_data,
                "community_context": community_context,
                "flow_metadata": {
                    "smj_name": "work_demand",
                    "completion_timestamp": datetime.now().isoformat(),
                    "user_number": bot_user.user.username if bot_user.user else "unknown",
                    "session_id": f"session_{user_id}_{getattr(bot_instance, 'id', 'unknown')}",
                    "app_type": user.app_type
                }
            }

            # Create UserLogs entry with specified structure
            user_log = bot_interface.models.UserLogs.objects.create(
                app_type=user.app_type,
                bot=bot_instance,
                user=bot_user,
                key1="useraction",
                value1="work_demand",
                key2="upload",
                value2="",
                key3="retries",
                value3="",
                key4="",  # Leave empty as not specified
                misc=comprehensive_misc_data,
                smj=smj
            )

            print(f"Successfully created UserLogs entry with ID: {getattr(user_log, 'id', 'unknown')}")
            print(f"Work demand data logged for user {user_id} in community {active_community_id}")

            # Additional logging for HDPI path verification
            if "photos" in work_demand_data:
                print(f"HDPI photo paths captured in UserLogs: {work_demand_data['photos']}")

            # Process and submit work demand to Community Engagement API
            try:
                import threading
                def async_submit():
                    try:
                        # Check if already processed (avoid duplicate processing from signal)
                        user_log.refresh_from_db()
                        if user_log.value2:  # If value2 is not empty, it's already been processed
                            print(f"🔄 UserLogs ID {user_log.id} already processed, skipping duplicate submission")
                            return

                        self.process_and_submit_work_demand(user_log.id)
                        print(f"✅ Work demand processing initiated for UserLogs ID: {user_log.id}")
                    except Exception as e:
                        print(f"❌ Error processing work demand for UserLogs ID {user_log.id}: {e}")

                # Run in background thread to avoid blocking SMJ flow
                thread = threading.Thread(target=async_submit, daemon=True)
                thread.start()
                print(f"🚀 Started background work demand processing for UserLogs ID: {user_log.id}")

            except Exception as e:
                print(f"❌ Failed to start work demand processing: {e}")

            return "success"

        except Exception as e:
            print(f"Error in log_work_demand_completion: {e}")
            return "failure"

    def log_grievance_completion(self, bot_instance_id, data_dict):
        """
        Log complete grievance data to UserLogs when RequestPhotos transitions to ThankYou.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print("in log_grievance_completion")

        try:
            # Get bot instance with better error handling
            try:
                bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            except bot_interface.models.Bot.DoesNotExist:
                print(f"Bot instance with ID {bot_instance_id} not found, trying to get any bot instance")
                bot_instance = bot_interface.models.Bot.objects.first()
                if not bot_instance:
                    print("No bot instances found in database")
                    return "failure"

            user_id = data_dict.get("user_id")
            if isinstance(user_id, str):
                user_id = int(user_id)

            # Get user session with better error handling
            try:
                user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            except bot_interface.models.UserSessions.DoesNotExist:
                print(f"UserSession not found for user_id: {user_id}, bot: {bot_instance}")
                try:
                    user = bot_interface.models.UserSessions.objects.get(user=user_id)
                    print(f"Found user session for user_id: {user_id} without bot constraint")
                except bot_interface.models.UserSessions.DoesNotExist:
                    print(f"No user session found for user_id: {user_id}")
                    return "failure"

            # Get BotUsers object for UserLogs
            try:
                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            except bot_interface.models.BotUsers.DoesNotExist:
                print(f"BotUser not found for user_id: {user_id}")
                return "failure"

            # Get grievance SMJ for reference
            try:
                smj = bot_interface.models.SMJ.objects.get(id=data_dict.get("smj_id"))
            except bot_interface.models.SMJ.DoesNotExist:
                print(f"SMJ not found for smj_id: {data_dict.get('smj_id')}")
                smj = None

            # Collect grievance data from misc_data
            grievance_data = {}
            if user.misc_data and "grievance" in user.misc_data:
                grievance_data = user.misc_data["grievance"]

                # Log photo paths to confirm HDPI paths are captured
                if "photos" in grievance_data:
                    print(f"Photo paths being logged: {grievance_data['photos']}")
                    # Add explicit note about HDPI paths in the data
                    grievance_data["photos_note"] = "Photo paths are HDPI processed images from WhatsApp media"

            # Collect community context
            community_context = {}
            active_community_id = user.misc_data.get('active_community_id') if user.misc_data else None

            if active_community_id:
                try:
                    from community_engagement.models import Community
                    community = Community.objects.get(id=active_community_id)

                    # Get community details
                    community_context = {
                        "community_id": active_community_id,
                        "community_name": community.project.name if community.project else "Unknown",
                        "organization": community.project.organization.name if community.project and community.project.organization else "Unknown"
                    }

                    # Get location hierarchy from community locations
                    location_hierarchy = {}
                    for location in community.locations.all():
                        if location.state:
                            location_hierarchy["state"] = location.state.state_name
                        if location.district:
                            location_hierarchy["district"] = location.district.district_name
                        if location.block:
                            location_hierarchy["block"] = location.block.block_name

                    community_context["location_hierarchy"] = location_hierarchy

                except Exception as e:
                    print(f"Error getting community context: {e}")
                    community_context = {"community_id": active_community_id,
                                         "error": "Failed to load community details"}

            # Prepare comprehensive misc data
            from datetime import datetime
            comprehensive_misc_data = {
                "grievance_data": grievance_data,
                "community_context": community_context,
                "flow_metadata": {
                    "smj_name": "grievance",
                    "completion_timestamp": datetime.now().isoformat(),
                    "user_number": bot_user.user.username if bot_user.user else "unknown",
                    "session_id": f"session_{user_id}_{getattr(bot_instance, 'id', 'unknown')}",
                    "app_type": user.app_type
                }
            }

            # Create UserLogs entry with specified structure
            user_log = bot_interface.models.UserLogs.objects.create(
                app_type=user.app_type,
                bot=bot_instance,
                user=bot_user,
                key1="useraction",
                value1="grievance",
                key2="upload",
                value2="",
                key3="retries",
                value3="",
                key4="",  # Leave empty as not specified
                misc=comprehensive_misc_data,
                smj=smj
            )

            print(f"Successfully created UserLogs entry with ID: {getattr(user_log, 'id', 'unknown')}")
            print(f"Grievance data logged for user {user_id} in community {active_community_id}")

            # Additional logging for HDPI path verification
            if "photos" in grievance_data:
                print(f"HDPI photo paths captured in UserLogs: {grievance_data['photos']}")

            return "success"

        except Exception as e:
            print(f"Error in log_grievance_completion: {e}")
            return "failure"

    def add_user_to_selected_community_join_flow(self, bot_instance_id, data_dict):
        """
        Add user to selected community in join community flow.
        Extracts community ID from session data (CommunityByLocation or CommunityByStateDistrict).
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        import json
        print(f"DEBUG: add_user_to_selected_community_join_flow called with bot_instance_id={bot_instance_id}")
        print(f"DEBUG: data_dict keys: {list(data_dict.keys())}")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user_session = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            bot_user = user_session.user

            # Extract community ID from session data (from either CommunityByLocation or CommunityByStateDistrict)
            community_id = None

            # Parse current session data - handle both string and object formats
            try:
                if isinstance(user_session.current_session, str):
                    current_session = json.loads(user_session.current_session or "[]")
                elif isinstance(user_session.current_session, (list, dict)):
                    current_session = user_session.current_session
                else:
                    current_session = []
                print(f"DEBUG: current_session data: {current_session}")

                # Look for community selection in either state
                for session_entry in current_session:
                    if isinstance(session_entry, dict):
                        # Check CommunityByStateDistrict first
                        if 'CommunityByStateDistrict' in session_entry:
                            community_id = session_entry['CommunityByStateDistrict'].get('misc')
                            print(f"DEBUG: Found community ID from CommunityByStateDistrict: {community_id}")
                            break
                        # Check CommunityByLocation second
                        elif 'CommunityByLocation' in session_entry:
                            community_id = session_entry['CommunityByLocation'].get('misc')
                            print(f"DEBUG: Found community ID from CommunityByLocation: {community_id}")
                            break

            except (json.JSONDecodeError, TypeError) as e:
                print(f"DEBUG: Error handling session data: {e}")
                current_session = []

            # Fallback: try to get from event_data if not found in session
            if not community_id:
                event_data = data_dict.get("event_data", {})
                print(f"DEBUG: Fallback - checking event_data: {event_data}")

                if event_data.get("type") == "button":
                    # For button events, extract community ID from button value (misc field)
                    button_value = event_data.get("misc") or event_data.get("data")
                    print(f"DEBUG: Button event detected - button_value: {button_value}")
                    if button_value:
                        community_id = button_value
                        print(f"DEBUG: Extracted community ID from button: {community_id}")

            if community_id:
                # Add user to selected community using existing API pattern
                user_phone = bot_user.user.contact_number
                print(f"DEBUG: Adding user {user_phone} to community {community_id}")

                try:
                    # Use similar pattern as existing addUserToCommunity function
                    response = requests.post(
                        url="http://localhost:8000/api/v1/add_user_to_community/",
                        data={
                            "community_id": community_id,
                            "number": int(user_phone)
                        },
                        timeout=30
                    )
                    response.raise_for_status()
                    api_response = response.json()
                    print("Add user to community API response:", api_response)

                    if api_response.get("success", False):
                        # Store community context in user session
                        if not user_session.misc_data:
                            user_session.misc_data = {}

                        user_session.misc_data['active_community_id'] = community_id
                        user_session.misc_data['navigation_context'] = "join_community"
                        user_session.misc_data['join_timestamp'] = str(timezone.now())
                        user_session.save()

                        print(f"DEBUG: Successfully added user to community {community_id}")
                        return "success"
                    else:
                        print(f"DEBUG: Failed to add user to community: {api_response}")
                        return "failure"

                except Exception as api_error:
                    print(f"DEBUG: API call failed: {api_error}")
                    return "failure"
            else:
                print(f"DEBUG: Could not extract community ID from session or event data")
                return "failure"

        except Exception as e:
            print(f"DEBUG: Exception in add_user_to_selected_community_join_flow: {e}")
            import traceback
            traceback.print_exc()
            return "failure"

    def send_join_success_message(self, bot_instance_id, data_dict):
        """
        Send success message after joining new community.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print(f"DEBUG: send_join_success_message called")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user_session = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
            bot_user = user_session.user

            # Get community name from misc_data
            community_id = user_session.misc_data.get('active_community_id') if user_session.misc_data else None
            community_name = "the community"

            if community_id:
                try:
                    # Try to get community name from existing patterns
                    from community_engagement.models import Community
                    community = Community.objects.get(id=community_id)
                    community_name = community.project
                except:
                    pass

            # Prepare success message
            success_text = f"✅ बहुत बढ़िया! आप सफलतापूर्वक {community_name} में शामिल हो गए हैं। अब आप समुदायिक सेवाओं का उपयोग कर सकते हैं।"

            # Send the message using bot_interface.api.send_text directly
            user_phone = bot_user.user.contact_number
            response = bot_interface.api.send_text(
                bot_instance_id=bot_instance_id,
                contact_number=user_phone,
                text=success_text
            )

            print(f"DEBUG: Join success message sent: {response}")
            return "success"

        except Exception as e:
            print(f"DEBUG: Exception in send_join_success_message: {e}")
            import traceback
            traceback.print_exc()
            return "failure"

    def return_to_community_services(self, bot_instance_id, data_dict):
        """
        Prepare return to community services menu after joining new community.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "success" or "failure"
        """
        print(f"DEBUG: return_to_community_services called")

        try:
            # Prepare SMJ jump back to community features
            jump_data = {
                "_smj_jump": {
                    "smj_name": "community_features",
                    "smj_id": 6,  # Assuming community features SMJ ID is 6
                    "init_state": "ServiceMenu",
                    "states": []  # Will be loaded from SMJ
                }
            }

            # Add jump data to data_dict for postAction processing
            data_dict.update(jump_data)

            print(f"DEBUG: Prepared return to community services: {jump_data}")
            return "success"

        except Exception as e:
            print(f"DEBUG: Exception in return_to_community_services: {e}")
            import traceback
            traceback.print_exc()
            return "failure"

    def process_and_submit_work_demand(self, user_log_id):
        """
        Processes work demand data from UserLogs and submits to Community Engagement API.
        
        Args:
            user_log_id (int): ID of the UserLogs record containing work demand data
            
        Returns:
            dict: API response from upsert_item endpoint or error dict
        """
        import requests
        import json
        import os
        from django.conf import settings
        from bot_interface.models import UserLogs

        try:
            # Get the UserLogs record
            try:
                user_log = UserLogs.objects.get(id=user_log_id)
            except UserLogs.DoesNotExist:
                return {"success": False, "message": f"UserLogs record with id {user_log_id} not found"}

            # Extract work demand data from misc field
            work_demand_data = user_log.misc.get("work_demand_data", {})
            if not work_demand_data:
                # Try alternative key structure
                work_demand_data = user_log.misc.get("work_demand", {})

            if not work_demand_data:
                return {"success": False, "message": "No work demand data found in UserLogs.misc"}

            print(f"Processing work demand data: {work_demand_data}")

            # Get user's community context from UserLogs misc data
            community_id = None
            try:
                # Get community_id from community_context in the UserLogs misc field
                if "community_context" in user_log.misc:
                    community_context = user_log.misc["community_context"]
                    community_id = community_context.get("community_id")
                    print(f"Found community_id in UserLogs: {community_id}")

                if not community_id:
                    return {"success": False, "message": "Could not find community_id in UserLogs data"}

            except Exception as e:
                print(f"Error getting community context from UserLogs: {e}")
                return {"success": False, "message": f"Error getting community context: {e}"}

            # Prepare files for upload from local filesystem
            files = {}

            # Handle audio file - use "audios" key for API
            if "audio" in work_demand_data:
                audio_path = work_demand_data["audio"]
                if audio_path and os.path.exists(audio_path):
                    try:
                        with open(audio_path, 'rb') as audio_file:
                            audio_content = audio_file.read()
                            # Determine file extension
                            file_ext = os.path.splitext(audio_path)[1] or '.ogg'
                            mime_type = 'audio/ogg' if file_ext == '.ogg' else 'audio/mpeg'
                            files['audios'] = (f'audio{file_ext}', audio_content, mime_type)
                            print(f"Added audio file: {audio_path}")
                    except Exception as e:
                        print(f"Error reading audio file {audio_path}: {e}")
                else:
                    print(f"Audio file not found or invalid path: {audio_path}")

            # Handle photo files - use indexed keys for multiple images
            if "photos" in work_demand_data and isinstance(work_demand_data["photos"], list):
                for i, photo_path in enumerate(work_demand_data["photos"]):
                    if photo_path and os.path.exists(photo_path):
                        try:
                            with open(photo_path, 'rb') as photo_file:
                                photo_content = photo_file.read()
                                # Determine file extension
                                file_ext = os.path.splitext(photo_path)[1] or '.jpg'
                                mime_type = 'image/jpeg' if file_ext.lower() in ['.jpg', '.jpeg'] else 'image/png'
                                files[f'images_{i}'] = (f'photo_{i}{file_ext}', photo_content, mime_type)
                                print(f"Added photo file {i}: {photo_path}")
                        except Exception as e:
                            print(f"Error reading photo file {photo_path}: {e}")
                    else:
                        print(f"Photo file not found or invalid path: {photo_path}")

            # Prepare coordinates from location data - use lat/lon format
            coordinates = {}
            if "location" in work_demand_data:
                location = work_demand_data["location"]
                if isinstance(location, dict):
                    coordinates = {
                        "lat": location.get("latitude"),
                        "lon": location.get("longitude")
                    }
                    # Only include if both lat and lon are available
                    if not (coordinates["lat"] and coordinates["lon"]):
                        coordinates = {}

            # Get user contact number through proper relationship chain
            try:
                # UserLogs.user_id -> BotUsers.id -> BotUsers.user_id -> Users.id -> Users.contact_number
                bot_user = user_log.user  # This is BotUsers instance
                actual_user = bot_user.user  # This is Users instance
                contact_number = actual_user.contact_number

                if not contact_number:
                    return {"success": False, "message": "Could not get user contact number"}

            except AttributeError as e:
                return {"success": False, "message": f"Could not get user contact number from relationship chain: {e}"}

            # Prepare API payload
            payload = {
                'item_type': 'WORK_DEMAND',
                'coordinates': json.dumps(coordinates) if coordinates else '',
                'number': contact_number,
                'community_id': community_id,
                'source': 'BOT',
                'bot_id': user_log.bot.id,
                'title': 'Work Demand Request',  # Auto-generated if not provided
                'transcript': work_demand_data.get('description', ''),  # If any description exists
            }

            print(f"API Payload: {payload}")
            print(f"Files to upload: {list(files.keys())}")

            # Submit to Community Engagement API
            api_url = f"http://localhost:8000/api/v1/upsert_item/"

            try:
                response = requests.post(
                    api_url,
                    data=payload,
                    files=files,
                    timeout=30  # 30 second timeout
                )

                print(f"API Response Status: {response.status_code}")
                print(f"API Response: {response.text}")

                if response.status_code == 200 or response.status_code == 201:
                    result = response.json()
                    if result.get("success"):
                        print(f"Successfully submitted work demand. Item ID: {result.get('item_id')}")

                        # Update UserLogs with success status
                        user_log.value2 = "success"
                        user_log.value3 = "0"  # No retries needed
                        user_log.key4 = "response"
                        user_log.value4 = response.text
                        user_log.save()
                        print(f"Updated UserLogs ID {user_log.id} with success status")

                        return result
                    else:
                        print(f"API returned success=False: {result}")

                        # Update UserLogs with API failure status
                        user_log.value2 = "failure"
                        user_log.value3 = "0"
                        user_log.key4 = "response"
                        user_log.value4 = response.text
                        user_log.save()
                        print(f"Updated UserLogs ID {user_log.id} with API failure status")

                        return result
                else:
                    # Update UserLogs with HTTP error status
                    user_log.value2 = "failure"
                    user_log.value3 = "0"
                    user_log.key4 = "error"
                    user_log.value4 = f"HTTP {response.status_code}: {response.text}"
                    user_log.save()
                    print(f"Updated UserLogs ID {user_log.id} with HTTP error status")

                    return {
                        "success": False,
                        "message": f"API call failed with status {response.status_code}: {response.text}"
                    }

            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")

                # Update UserLogs with request error status
                user_log.value2 = "failure"
                user_log.value3 = "0"
                user_log.key4 = "error"
                user_log.value4 = f"Request error: {e}"
                user_log.save()
                print(f"Updated UserLogs ID {user_log.id} with request error status")

                return {"success": False, "message": f"Request error: {e}"}

        except Exception as e:
            print(f"Error in process_and_submit_work_demand: {e}")
            import traceback
            traceback.print_exc()

            # Update UserLogs with internal error status
            try:
                user_log.value2 = "failure"
                user_log.value3 = "0"
                user_log.key4 = "error"
                user_log.value4 = f"Internal error: {e}"
                user_log.save()
                print(f"Updated UserLogs ID {user_log.id} with internal error status")
            except Exception as save_error:
                print(f"Failed to update UserLogs: {save_error}")

            return {"success": False, "message": f"Internal error: {e}"}

    def fetch_work_demand_status(self, bot_instance_id, data_dict):
        """
        Fetches work demand status for the current user from Community Engagement API.
        
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Contains user_id, bot_id, and other session data
            
        Returns:
            str: "has_work_demands" if user has work demands, "no_work_demands" if none found, "failure" on error
        """
        print(f"Fetching work demand status for bot_instance_id: {bot_instance_id} and data_dict: {data_dict}")
        try:
            import requests
            from django.conf import settings
            from bot_interface.models import BotUsers
            from community_engagement.models import Community_user_mapping

            # Get user information
            user_id = data_dict.get('user_id')
            bot_id = data_dict.get('bot_id', 1)

            if not user_id:
                print("No user_id found in data_dict")
                return "failure"

            # Get user's contact number
            try:
                bot_user = BotUsers.objects.get(pk=user_id)
                contact_number = bot_user.user.contact_number
            except BotUsers.DoesNotExist:
                print(f"BotUsers with id {user_id} not found")
                return "failure"

            # Get user's active community
            try:
                community_mapping = Community_user_mapping.objects.filter(
                    user=bot_user.user,
                    is_last_accessed_community=True
                ).first()

                if not community_mapping:
                    print(f"No active community found for user {contact_number}")
                    return "failure"

                community_id = community_mapping.community.id
            except Exception as e:
                print(f"Error getting community for user {contact_number}: {e}")
                return "failure"

            # Call Community Engagement API
            api_url = f"http://localhost:8000/api/v1/get_items_status/"
            params = {
                'number': contact_number,
                'bot_id': bot_instance_id,
                # 'community_id': str(community_id),
                'work_demand_only': 'true'
            }

            print(
                f"Fetching work demand status for user {contact_number} in community {community_id} and bot_id {bot_instance_id}")
            response = requests.get(api_url, params=params, timeout=30)
            print("response from GET request of get_items_status/ :", response)

            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    work_demands = result.get('data', [])
                    print(f"Found {len(work_demands)} work demands for user {contact_number}")

                    # Store work demands in user session for persistence between states
                    try:
                        from bot_interface.models import UserSessions
                        session_data = {
                            'work_demands': work_demands,
                            'community_id': community_id
                        }
                        UserSessions.objects.filter(user_id=user_id).update(misc_data=session_data)
                        print(f"Stored {len(work_demands)} work demands in session for user {user_id}")
                    except Exception as session_error:
                        print(f"Error storing work demands in session: {session_error}")

                    if work_demands:
                        return "has_work_demands"
                    else:
                        return "no_work_demands"
                else:
                    print(f"API returned error: {result.get('message', 'Unknown error')}")
                    return "failure"
            else:
                print(f"API request failed with status {response.status_code}: {response.text}")
                return "failure"

        except Exception as e:
            print(f"Error in fetch_work_demand_status: {e}")
            return "failure"

    def display_work_demands_text(self, bot_instance_id, data_dict):
        """
        Displays work demands as WhatsApp text message with Hindi format and character limit handling.
        
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Contains user_id, bot_id, and other session data
            
        Returns:
            str: "success" if message sent successfully, "failure" otherwise
        """
        try:
            from bot_interface.models import UserSessions, BotUsers

            # Get user information
            user_id = data_dict.get('user_id')
            bot_id = data_dict.get('bot_id', 1)

            if not user_id:
                print("No user_id found in data_dict")
                return "failure"

            # Retrieve work demands from user session
            try:
                user_session = UserSessions.objects.filter(user_id=user_id).first()
                if not user_session or not user_session.misc_data:
                    print(f"No session data found for user {user_id}")
                    return "failure"

                session_data = user_session.misc_data
                work_demands = session_data.get('work_demands', [])

                if not work_demands:
                    print(f"No work demands found in session for user {user_id}")
                    return "failure"

            except Exception as session_error:
                print(f"Error retrieving session data: {session_error}")
                return "failure"

            # Get user's contact number for WhatsApp
            try:
                bot_user = BotUsers.objects.get(pk=user_id)
                contact_number = bot_user.user.contact_number
            except BotUsers.DoesNotExist:
                print(f"BotUsers with id {user_id} not found")
                return "failure"

            # Send work demands with character limit handling
            try:
                success = self._send_work_demands_with_limit(work_demands, contact_number, bot_instance_id)
                if success:
                    print(f"Asset demands text sent successfully to {contact_number}")
                    return "success"
                else:
                    print(f"Failed to send asset demands text to {contact_number}")
                    return "failure"
            except Exception as send_error:
                print(f"Error sending WhatsApp message: {send_error}")
                return "failure"

        except Exception as e:
            print(f"Error in display_work_demands_text: {e}")
            return "failure"

    def _send_work_demands_with_limit(self, work_demands, contact_number, bot_instance_id, max_length=4000):
        """
        Send work demands with character limit handling, splitting into multiple messages if needed.
        
        Args:
            work_demands (list): List of work demand objects
            contact_number (str): User's contact number
            bot_instance_id (int): Bot instance ID
            max_length (int): Maximum characters per message
            
        Returns:
            bool: True if all messages sent successfully, False otherwise
        """
        try:
            # Create base header
            header = "📋 आपके संसाधन की मांग की स्थिति:\n\n"

            # Calculate approximate length per work demand entry
            sample_entry = "1. संसाधन मांग ID: 123\n   शीर्षक: Asset Demand Request\n   स्थिति: UNMODERATED\n\n"
            entry_length = len(sample_entry)

            # Calculate how many entries can fit in one message
            available_space = max_length - len(header) - 50  # 50 chars buffer for part indicator
            entries_per_message = max(1, available_space // entry_length)

            total_messages = (len(work_demands) + entries_per_message - 1) // entries_per_message

            # Send messages
            for msg_num in range(total_messages):
                start_idx = msg_num * entries_per_message
                end_idx = min(start_idx + entries_per_message, len(work_demands))

                # Create message text
                if total_messages == 1:
                    text = header
                else:
                    if msg_num == 0:
                        text = header
                    else:
                        text = f"📋 आपके संसाधन की मांग की स्थिति (जारी):\n\n"

                # Add work demand entries
                for i in range(start_idx, end_idx):
                    demand = work_demands[i]
                    demand_id = demand.get('id', 'N/A')
                    title = demand.get('title', 'Asset Demand Request')
                    status = demand.get('status', 'UNMODERATED')
                    transcription = demand.get('transcription', '')

                    text += f"{i + 1}. संसाधन मांग ID: {demand_id}\n"
                    text += f"   शीर्षक: {title}\n"
                    text += f"   स्थिति: {status}\n"

                    # Add transcription if available and not empty
                    if transcription and transcription.strip():
                        # Truncate long transcriptions
                        if len(transcription) > 50:
                            transcription = transcription[:50] + "..."
                        text += f"   विवरण: {transcription}\n"

                    text += "\n"

                # Add part indicator for multiple messages
                if total_messages > 1:
                    text += f"(भाग {msg_num + 1}/{total_messages})"

                # Send message
                response = bot_interface.api.send_text(
                    bot_instance_id=bot_instance_id,
                    contact_number=contact_number,
                    text=text
                )

                if not response or not response.get('messages'):
                    print(f"Failed to send message part {msg_num + 1}/{total_messages}")
                    return False

                print(f"Sent message part {msg_num + 1}/{total_messages} successfully")

            return True

        except Exception as e:
            print(f"Error in _send_work_demands_with_limit: {e}")
            return False
