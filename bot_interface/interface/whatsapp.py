import json
from typing import Dict, Any

import bot_interface.interface.generic
import bot_interface.models
import bot_interface.utils
import bot_interface.api
import bot_interface.auth

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
        return (json_obj.get("id") and 
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

        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
        response = bot_interface.api.send_text(
            bot_instance_id=bot_id,
            contact_number=user.phone,
            text=text)
        print("Text message response:", response)

        # if bot_interface.utils.detect_url(text):
        #     # TODO: Implement send_text_url function or use send_text for URLs too
        #     response = bot_interface.api.send_text(app_instance_config_id, user.phone, text)
        #     # print(response)
        # else:
        #     response = bot_interface.api.send_text(app_instance_config_id, user.phone, text)
        #     print(response)

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


    def sendButton(self, bot_instance_id, data_dict):
        print("in sendButton")
        logger.info("data_dict in sendButton: %s", data_dict)
        bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
        if bot_instance:
            print("bot_instance", bot_instance.language)

        user_id = data_dict.get("user_id")
        print("user in sendButton", user_id)

        # bot_user = bot_interface.models.BotUsers.objects.get(user=user_id)
        # print("bot_user in sendButton", bot_user)
        data = data_dict.get("menu")
        caption = data[0].get("caption")
        print(data)
        print("caption", caption)

        # try:
        #     user = bot_interface.models.UserSessions.objects.get(user=bot_user, bot=bot_instance)
        # except bot_interface.models.UserSessions.DoesNotExist:
        #     # create a new user session if it doesn't exist with user = bot_user and bot = bot_instance
        #     user = bot_interface.models.UserSessions.objects.create(
        #         user=bot_user,
        #         bot=bot_instance,
        #         phone=bot_user.user.contact_number,
        #         app_type=bot_instance.app_type
        #     )
        #     print(f"Created new UserSession: {user}")
        #check if user is created
        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot_instance)
        print("user in sendButton", user)
        user.expected_response_type = "button"
        user.current_state = data_dict.get("state")
        
        # Handle SMJ object lookup with error handling
        smj_id = data_dict.get("smj_id")
        user.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

        user.save()
        print("length of data ::", len(data))
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
        return response
    
    def sendLocationRequest(self, bot_instance_id, data_dict):
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
        assert False
        return response