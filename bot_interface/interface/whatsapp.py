import json
from typing import Dict, Any

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
                    print(f"Preserved user context - SMJ: {user_session.current_smj.id}, State: {user_session.current_state}")
                
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
        """Download image from WhatsApp API - implement this method"""
        # TODO: Implement actual image download logic
        raise NotImplementedError("download_image function needs to be implemented")
    
    @staticmethod
    def _download_audio(bot_id: int, mime_type: str, media_id: str) -> str:
        """Download audio from WhatsApp API - implement this method"""
        # TODO: Implement actual audio download logic
        raise NotImplementedError("download_audio function needs to be implemented")
    
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
        caption = data[0].get("caption")
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
        
        #check if user is created
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

            #check if user is created
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
            if get_data_from_state and get_data_from_state in current_session:
                data = current_session[get_data_from_state].get(get_data_from_state_field, {})
                print("Data from current session for state:", data)
                # lat lon is stored as a string in data seperated by comma eg (25.2048, 55.2708)
                if isinstance(data, str):
                    lat_lon = data.split(",")
                    if len(lat_lon) == 2:
                        latitude = lat_lon[0].strip()
                        longitude = lat_lon[1].strip()
                        print("Latitude and Longitude from string:", latitude, longitude)
                    else:
                        print("Invalid latitude and longitude format in string")
                        return "failure"
                else:
                    print("Latitude and Longitude not found in current session data")
                    return "failure"
            else:
                print(f"Required state data not found in session: {get_data_from_state}")
                return "failure"
            
            # print("Latitude and Longitude from string:", latitude, longitude)

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            user.save()

            response = None
            try: 
                response = bot_interface.utils.get_community_by_lat_lon(
                lat=latitude,
                lon=longitude
            )
            except Exception as e:
                logger.error(f"Error fetching community using get_community_by_lat_lon: {e}")
                # except 500 error
                try:
                    print("Fetching community by location...")
                    response = requests.get(
                        url="https://uat.core-stack.org/api/v1/get_admin_details_by_latlon/?latitude=25.1369&longitude=85.4516",
                        # params={"latitude": latitude, "longitude": longitude},
                        timeout=30
                    ).json()
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching community by location: {e}")
                    return "failure"

            print("Community by location response:", response)

            if response:
                from community_engagement.utils import get_communities
                # Handle response based on its type
                if isinstance(response, tuple):
                    # If response is a tuple (success, data), extract the data
                    success, location_data = response
                    if success and isinstance(location_data, dict):
                        community_list = get_communities(
                            state_name=location_data.get("State", ""), 
                            district_name=location_data.get("District", ""), 
                            block_name=location_data.get("Block", "")
                        )
                    else:
                        return "failure"
                elif isinstance(response, dict):
                    # If response is a dict, use it directly
                    community_list = get_communities(
                        state_name=response.get("State", ""), 
                        district_name=response.get("District", ""), 
                        block_name=response.get("Block", "")
                    )
                else:
                    return "failure"
                    
                print("Community list:", community_list)
                
                if community_list:
                    return "success"
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
        
        #check if user is created
        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
        print("user in sendStates", user)
        user.expected_response_type = "button"
        user.current_state = data_dict.get("state")
        
        # Handle SMJ object lookup with error handling
        smj_id = data_dict.get("smj_id")
        user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
        user.save()

        from community_engagement.models import Location
        state_ids_with_community = Location.objects.filter(communities__isnull=False).values_list('state_id', flat=True).distinct()
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
            
            #check if user is created
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
            
            # Initialize variable
            get_data_from_state = None
            
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
            state_id = current_session[0][get_data_from_state].get(get_data_from_state_field, "")
            print("State ID from current session:", state_id)
            # get districts based on the selected state
            try:
                response = requests.get(
                    url="http://localhost:8000/api/v1/get_districts_with_community/",
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
            districts_list = [{"value": district.get("id"), "label": district.get("name"), "description": ""} for district in districts_data]

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
            
            #check if user is created
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
                    url="http://localhost:8000/api/v1/get_communities_by_location/",
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
            
            #check if user is created
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

                # add user to community
                if community_id:
                    print(f"Adding user {user.phone} to community {community_id}")
                    try:
                        response = requests.post(
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
                                
                                # Get the BotUsers object from user_id
                                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                                
                                # Add community membership to user's local data
                                add_community_membership(bot_user, community_data)
                                print(f"Successfully added community membership data for user {user.phone}")
                                
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
