import json
from typing import Dict, Any, Tuple, Optional

import boto3
from django.db import models

import bot_interface.interface.generic
import bot_interface.models
import bot_interface.utils
import bot_interface.api


class WhatsAppInterface(bot_interface.interface.generic.GenericInterface):
    """WhatsApp interface implementation for handling WhatsApp Business API interactions"""
    
    # Define constants at class level
    BUCKET_NAME = "your-s3-bucket-name"  # Replace with actual bucket name
    
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
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_id)
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
            "msisdn": ""
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
        event_packet["msisdn"] = wa_id
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
            event_packet["data"] = interactive["button_reply"]["title"]
        
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
    def _is_shopify_order(json_obj: Dict) -> bool:
        """Check if the data is a Shopify order"""
        return (json_obj.get("source_name") == "shopify_draft_order")
    
    @staticmethod
    def _process_shopify_order(json_obj: Dict, event_packet: Dict, 
                              bot_instance: Any) -> None:
        """Process Shopify order data"""
        customer = json_obj.get("customer", {})
        customer_phone = customer.get("phone", "")
        
        event_packet["msisdn"] = str(customer_phone)[1:] if customer_phone else ""
        event_packet["type"] = "notification"
        event_packet["state"] = bot_instance.init_state
        event_packet["data"] = json_obj
    
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