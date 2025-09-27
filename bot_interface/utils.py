# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import json
import requests
from requests.auth import HTTPBasicAuth
import bot_interface.api
import bot_interface.models
import bot_interface.interface.generic, bot_interface.interface.whatsapp
import json
# from ai4bharat.transliteration import XlitEngine
from PIL import Image
import decimal
import ast
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import speech_recognition as sr
import subprocess
import mimetypes
# from gtts import gTTS
from collections import OrderedDict
import bot_interface.api
import bot_interface.models
import bot_interface.interface.generic, bot_interface.interface.whatsapp
# from WhatsappConnect.settings import BUCKET_URL, BUCKET_NAME, WHATSAPP_MEDIA_PATH
from bot_interface.api import WHATSAPP_MEDIA_PATH
from typing import Dict, Any, Tuple
from django.core.exceptions import ObjectDoesNotExist

import logging

logger = logging.getLogger(__name__)

# Community membership utility functions
def get_community_membership(bot_user):
    """Get the community_membership data from user_misc field"""
    if bot_user.user_misc and isinstance(bot_user.user_misc, dict):
        return bot_user.user_misc.get('community_membership', {})
    return {}

def add_community_membership(bot_user, community_data):
    """Add community membership data to user_misc field"""
    from datetime import datetime
    
    # Initialize user_misc if it's None
    if not bot_user.user_misc:
        bot_user.user_misc = {}
    
    # Initialize community_membership if it doesn't exist
    if 'community_membership' not in bot_user.user_misc:
        bot_user.user_misc['community_membership'] = {
            'current_communities': []
        }
    
    # Prepare the community data with joined_date
    new_community = {
        'community_id': community_data.get('community_id'),
        'community_name': community_data.get('community_name'),
        'community_description': community_data.get('community_description'),
        'organization': community_data.get('organization'),
        'joined_date': datetime.now().isoformat()
    }
    
    # Check if user is already in this community
    current_communities = bot_user.user_misc['community_membership']['current_communities']
    for community in current_communities:
        if community.get('community_id') == new_community['community_id']:
            logger.info(f"User {bot_user.user_id} already in community {new_community['community_id']}")
            return False
    
    # Add the new community
    current_communities.append(new_community)
    bot_user.save()
    logger.info(f"Added user {bot_user.user_id} to community {new_community['community_id']}")
    return True

def is_user_in_community(bot_user, community_id):
    """Check if user is already in a specific community"""
    membership = get_community_membership(bot_user)
    current_communities = membership.get('current_communities', [])
    
    for community in current_communities:
        if community.get('community_id') == community_id:
            return True
    return False

def sync_community_data_from_database(bot_user):
    """
    Sync community data from database to user_misc for existing users.
    This function updates incomplete or missing community data.
    
    Args:
        bot_user: BotUsers object
    
    Returns:
        bool: True if data was updated, False if no update needed
    """
    try:
        from community_engagement.models import Community_user_mapping
        from users.models import User
        
        phone_number = bot_user.user.contact_number
        print(f"Syncing community data from database for user: {phone_number}")
        
        # Get current communities from user_misc
        current_membership = get_community_membership(bot_user)
        current_communities = current_membership.get('current_communities', [])
        
        # Get communities from database
        user_obj = User.objects.get(contact_number=phone_number)
        community_mappings = Community_user_mapping.objects.filter(user=user_obj).select_related('community', 'community__project')
        
        updated = False
        
        # Check if we need to sync data
        if not current_communities or any(
            not comm.get('community_name') or not comm.get('organization') 
            for comm in current_communities
        ):
            print("Community data is incomplete, updating from database...")
            
            # Initialize if needed
            if not bot_user.user_misc:
                bot_user.user_misc = {}
            if 'community_membership' not in bot_user.user_misc:
                bot_user.user_misc['community_membership'] = {'current_communities': []}
            
            # Clear and rebuild community list
            bot_user.user_misc['community_membership']['current_communities'] = []
            
            for mapping in community_mappings:
                community_data = {
                    'community_id': str(mapping.community.id),
                    'community_name': mapping.community.project.name if mapping.community.project else f"Community {mapping.community.id}",
                    'community_description': getattr(mapping.community.project, 'description', '') if mapping.community.project else '',
                    'organization': mapping.community.project.organization.name if (mapping.community.project and mapping.community.project.organization) else 'Unknown Organization',
                    'joined_date': mapping.created_at.isoformat() if hasattr(mapping, 'created_at') else datetime.now().isoformat()
                }
                
                bot_user.user_misc['community_membership']['current_communities'].append(community_data)
                updated = True
            
            if updated:
                bot_user.save()
                print(f"Updated community data for user {phone_number}: {bot_user.user_misc['community_membership']['current_communities']}")
        
        return updated
        
    except Exception as e:
        print(f"Error syncing community data from database: {e}")
        return False

status_map = {
            "UNASSIGNED": "सौंपा नहीं गया",
            "ASSIGNED": "सौंप दिया",
            "INPROGRESS": "प्रक्रिया में",
            "CLOSED": "समाप्त",
            "MARKED_RESOLVED": "हल हो गया है",
            "UNMODERATED": "जाँच नहीं हुई है",
            "RESOLVED": "हल हो गया है",
            "REJECTED": "रद्द किया है"
        }

# # class Transliterate:
# #     def __init__(self, source_language, beam_width=10, rescore=True, source_script_type="en"):
# #         self.engine = XlitEngine(source_language, beam_width=beam_width, rescore=rescore, src_script_type=source_script_type)

# #     def translit_sentence(self, sentence, target_language):
# #         transcript = self.engine.translit_sentence(sentence,target_language)
# #         return transcript

# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             if o % 1 > 0:
#                 return float(o)
#             else:
#                 return int(o)
#         return super(DecimalEncoder, self).default(o)

# def convertTokenExpireTime(time_str):
#     time_str_formated = time_str.replace("Z", '').replace("T", ' ')
#     datetime_object = datetime.strptime(
#         time_str_formated, '%Y-%m-%d %H:%M:%S.%f')
#     return datetime_object


# def generateWhatsppToken(token_url, username, password):
#     print(str(username))
#     print(str(password))
#     print(str(token_url))
#     response = requests.post(token_url.strip(), auth=HTTPBasicAuth(
#         str(username.strip()), str(password.strip())))
#     print(response.text)
#     response_dict = json.loads(response.text)
#     print(response_dict)
#     print(type(response_dict))
#     return response_dict["users"][0]


# def getHeader(whatsappconfig):
#     headers = {}
#     token_string = "Bearer " + str(whatsappconfig.token)
#     headers['Authorization'] = token_string
#     headers['Content-Type'] = "application/json"
#     return headers


# def send_message(whatsappconfig, to_number, text, type="text"):
#     headers = getHeader(whatsappconfig)
#     print(headers)
#     url = whatsappconfig.app_message_url
#     print(url)
#     message_dict = {}
#     message_dict["body"] = text
#     print(message_dict)
#     caller_json = {}
#     caller_json["to"] = to_number
#     caller_json["text"] = message_dict
#     caller_json["type"] = type
#     print(caller_json)
#     response = requests.post(url=url, headers=headers, json=caller_json)
#     if response.status_code == 200:
#         print(response.text)


# def update_user_details(contact_number, name, whatsappconfig_obj):
#     print(contact_number)
#     whatsapp_user, created = Interface.models.Users.objects.get_or_create(
#         msisdn=contact_number, whatsappconfig=whatsappconfig_obj)
#     if created:
#         whatsapp_user.name = name
#         whatsapp_user.save()
#         return whatsapp_user
#     else:
#         print('Already Created')
#         if whatsapp_user.name != name:
#             whatsapp_user.name = name
#             whatsapp_user.save()
#             return whatsapp_user
#         return whatsapp_user


# def flat_message_log(whatsapp_user, event_json):
#     data_json = {}
#     message_text = event_json['messages'][0]['text']
#     message_data_json = event_json['messages'][0]['_vnd']['v1']
#     message_data_json['text'] = message_text
#     print(message_data_json)
#     whatsapp_user = WhatsappUserMessageLog(
#         whatsapp_user=whatsapp_user, data=message_data_json)
#     whatsapp_user.save()


# def create_user(app_type, app_instance_config, contact_number):
#     user = Interface.models.Users(app_type=app_type, app_instance_config=app_instance_config,
#                                   msisdn=contact_number, current_smj=app_instance_config.smj)
#     user.save()
#     return user


# def create_or_update_user_profile(event_packet, user, app_type, event):
#     if event:
#         return
#     user_profile = Interface.models.UserProfile.objects.filter(
#         app_type=app_type, user=user, msisdn=event_packet["msisdn"])
#     if not user_profile.exists():
#         user_profile = Interface.models.UserProfile(
#             app_type=app_type, user=user, msisdn=event_packet["msisdn"])
#     else:
#         user_profile = user_profile[0]

#     set_user_profile_data(user_profile, event_packet, user)


# def set_user_profile_data(user_profile, event_packet, user):
#     data = data_interactive = ''
#     data_type = event_packet['type']
#     data = event_packet['data']
#     # print("set_user_profile_data data:",data)
#     # print("data_interactive", data_interactive)
#     try:
#         if user.current_state == 'GetName':
#             user_profile.name = data
#         elif user.current_state == 'GetAge':
#             user_profile.age = data
#         elif user.current_state == 'GetGender':
#             if data == 'Male':
#                 data = 'M'
#             elif data == 'Female':
#                 data = 'F'
#             elif data == 'Others':
#                 data = 'O'
#             user_profile.gender = data
#         elif user.current_state == 'GetLocation':
#             user_profile.location_text = data
#         user_profile.save()
#     except Exception as e:
#         user_profile.save()
#         print("Exception in set_user_profile_data : ", str(e))


def check_event_type(event_packet, expected_response_type, user_session, event=None):
    """
    Validates if the user's response matches the expected response type.
    
    Args:
        event_packet (dict): The incoming message data
        expected_response_type (str): Expected response type (text, button, location, etc.)
        user_session (UserSessions): User's session object to get stored context_id
        event (str, optional): Event name, if present validation is bypassed
        
    Returns:
        bool: True if valid response, False if invalid (error message sent)
    """
    # Bypass validation if event is specified (e.g., start event, onboarding, community_member)
    if event:
        print(f"Bypassing validation for event: {event}")
        return True
    
    # Always allow notification type
    if event_packet.get('type') == 'notification':
        return True
    
    response_type = event_packet.get('type', '')
    print(f"Response type: {response_type}, Expected: {expected_response_type}")
    
    # Define valid input types for each expected response
    input_type_mapping = {
        'text': ['text'],
        'button': ['button', 'interactive'],
        'audio': ['audio', 'voice'],
        'image': ['image'],
        'location': ['location'],
        'audio_text': ['text', 'voice', 'audio'],
        'community': ['button', 'interactive']  # Community selection is also button-based
    }
    
    # Check if response type matches expected type
    valid_types = input_type_mapping.get(expected_response_type, [])
    if response_type in valid_types:
        
        # Special validation for button responses - check context_id
        if expected_response_type in ['button', 'community']:
            stored_context_id = user_session.misc_data.get('last_context_id') if user_session.misc_data else None
            incoming_context_id = event_packet.get('context_id')
            
            print(f"Context validation - Stored: {stored_context_id}, Incoming: {incoming_context_id}")
            
            # If we have both context IDs, validate they match
            if stored_context_id and incoming_context_id:
                if stored_context_id == incoming_context_id:
                    print(f"Context ID validation passed: {stored_context_id}")
                    return True
                else:
                    print(f"Context ID mismatch: stored={stored_context_id}, incoming={incoming_context_id}")
                    _send_validation_error(user_session, "button_wrong_menu")
                    return False
            elif stored_context_id and not incoming_context_id:
                # We expect context but didn't get it (user sent text instead of clicking button)
                print("Expected context ID but didn't receive one")
                _send_validation_error(user_session, "button_no_context")
                return False
            elif not stored_context_id and incoming_context_id:
                # We got context but didn't expect it (shouldn't happen often)
                print("Received unexpected context ID, allowing")
                return True
            else:
                # Neither stored nor incoming context (legacy or text-based button response)
                print("No context IDs available, allowing response")
                return True
        
        # For non-button types, allow without context validation
        print(f"Valid response type: {response_type} matches expected: {expected_response_type}")
        return True
    
    # Response type doesn't match expected - send error message
    print(f"Invalid response type: {response_type} does not match expected: {expected_response_type}")
    _send_validation_error(user_session, expected_response_type)
    return False


def _send_validation_error(user_session, error_type):
    """
    Send appropriate error message based on validation failure type.
    
    Args:
        user_session (UserSessions): User session object
        error_type (str): Type of validation error
    """
    try:
        bot_language = user_session.bot.language
        
        error_messages = {
            'text': {
                'hi': "अमान्य विकल्प!! कृपया लिख कर अपना सवाल या टिप्पणी भेजिए।",
                'en': "Sorry, we are expecting a text response from you."
            },
            'button': {
                'hi': "आपने हमें जो भेजा है वो इन विकल्पों में से एक नहीं है। आपको दिए गए विकल्पों में से ही कोई विकल्प का चुनाव करना है।",
                'en': "Sorry, we are expecting a button response from you."
            },
            'community': {
                'hi': "कृपया दिए गए समुदाय विकल्पों में से चुनें।",
                'en': "Please choose from the given community options."
            },
            'button_wrong_menu': {
                'hi': "आपने ग़लत मेनू से विकल्प चुना है। कृपया नवीनतम मेनू का उपयोग करें।",
                'en': "You have chosen an option from the wrong menu. Please use the latest menu."
            },
            'button_no_context': {
                'hi': "कृपया दिए गए बटन का उपयोग करें, अपना टेक्स्ट न भेजें।",
                'en': "Please use the provided buttons, don't send your own text."
            },
            'location': {
                'hi': "माफ़ कीजिये, कृपया अपना स्थान भेजें।",
                'en': "Sorry, we are expecting a location response from you."
            },
            'image': {
                'hi': "माफ़ कीजिये, कृपया फोटो अपलोड कर अपनी बात रखें।",
                'en': "Sorry, we are expecting an image response from you."
            },
            'audio': {
                'hi': "माफ़ कीजिये, कृपया ऑडियो रिकॉर्ड करके भेजें।",
                'en': "Sorry, we are expecting an audio response from you."
            },
            'audio_text': {
                'hi': "माफ़ कीजिये, अपनी बात लिखित में या फिर ऑडियो रिकॉर्ड करके बताएं।",
                'en': "Sorry, we are expecting a text or audio response from you."
            }
        }
        
        # Get message for the specific error type and language
        message_dict = error_messages.get(error_type, error_messages.get('button', {}))
        message = message_dict.get(bot_language, message_dict.get('hi', "अमान्य विकल्प!!"))
        
        print(f"Sending validation error message: {message}")
        
        # Send error message
        bot_interface.api.send_text(
            bot_instance_id=user_session.bot.id,
            contact_number=user_session.phone,
            text=message
        )
        
    except Exception as e:
        print(f"Error sending validation message: {e}")
        logger.error(f"Error sending validation message: {e}")


def check_event_type_(app_instance_config_id, event_packet, expected_response_type, start_session_flag, event, app_instance_language, context_id):
    """
    This function checks whether the expected input and user input is same. Check for ist time Hi
    """
    if event:
        return True

    if not start_session_flag:
        if event_packet['type'] == 'notification':
            return True
        else:
            response_type = event_packet['type']

            print("response type in check_event_type :: ", response_type)
            input_type = {
                'text': ['text'],
                'button': ['button', 'interactive'],
                'audio': ['audio', 'voice'],
                'image': ['image'],
                'location': ['location'],
                'audio_text': ['text', 'voice', 'audio']
            }
            is_response = False
            print("expected response type::::", expected_response_type)
            if response_type in input_type[expected_response_type]:
                if context_id and 'context_id' in event_packet and event_packet['context_id']:
                    is_response = True
                    if context_id == event_packet['context_id']:
                        print('expected_response_type matched: ', input_type[response_type])
                        return True
                else:
                    return True

            if expected_response_type == 'text':
                text = "अमान्य विकल्प!! कृपया लिख कर अपना सवाल या टिप्पणी भेजिए।"
                fp_text = "Sorry, we are expecting a text response from you."
            elif expected_response_type == 'button':
                if is_response:
                    text = "आपने ग़लत मेनू से विकल्प चुना है।"
                    fp_text = "You have chosen the option from the wrong menu."
                else:
                    text = "आपने हमें जो भेजा है वो इन विकल्पों में से एक नहीं है। आपको दिए गए विकल्पों में से ही कोई विकल्प का चुनाव करना है।"
                    fp_text = "Sorry, we are expecting a button response from you."
            elif expected_response_type == 'audio_text':
                text = "माफ़ कीजिये, अपनी बात लिखित में या फिर ऑडियो रिकॉर्ड करके बताएं।"
                fp_text = "Sorry, we are expecting a text response from you."
            elif expected_response_type == 'image':
                text = "माफ़ कीजिये, कृपया फोटो अपलोड कर अपनी बात रखें।"
                fp_text = "Sorry, we are expecting a image response from you."
            elif expected_response_type == 'location':
                text = "माफ़ कीजिये, कृपया अपना स्थान भेजें।"
                fp_text = "Sorry, we are expecting a location response from you."
            else:
                text = "अमान्य विकल्प!! "
                fp_text = "Sorry, we are expecting a different input."

            wa_id = event_packet['wa_id']
            if app_instance_language == 'hi':
                bot_interface.api.send_text(app_instance_config_id=app_instance_config_id, contact_number=wa_id, text=text)
            elif app_instance_language == 'en':
                bot_interface.api.send_text(app_instance_config_id=app_instance_config_id, contact_number=wa_id, text=fp_text)
            return False
    return True


def create_media_entry(user, media_path, media_type, app_type='WA'):
    from community_engagement.models import Media
    media_details = Media(
        app_type=app_type, app_instance_config=user.app_instance_config, user=user, media_type=media_type, media_path=media_path)
    media_details.save()


def detect_url(text):
    regex = "http"
    url = re.findall(regex, text)
    return url

def convert_image_hdpi(filepath):
    image_name = filepath.split("/")[-1]
    image_split = str(image_name).split(".")
    file_identifier = image_split[0]

    img = Image.open(filepath)
    print(str(file_identifier))
    img_format = img.format.lower()
    hdpi_im_key = file_identifier+'_hdpi.'+img_format
    print(str(hdpi_im_key))
    im_hdpi_file = WHATSAPP_MEDIA_PATH + 'hdpi/'+hdpi_im_key
    print(str(im_hdpi_file))
    width_0, height_0 = img.size
    hdpi_fixed_width_in_pixel = 480
    wpercent = hdpi_fixed_width_in_pixel / float(width_0)
    hsize = int(float(height_0) * float(wpercent))
    img.resize((hdpi_fixed_width_in_pixel, hsize),
               Image.ANTIALIAS).save(im_hdpi_file)
    return im_hdpi_file


def push_to_s3(local_file_path, bucket_name, s3_file_path, cType):
    import boto3
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(local_file_path, bucket_name,
                              s3_file_path, ExtraArgs={'ContentType': cType})
        exception = ''
        s3_url = BUCKET_URL + str(s3_file_path)
        # if msidn_obj:
        #     data_logger = S3_sync_log(user = msidn_obj, success = True)
        #     data_logger.save()
        return True, s3_url, exception
    except Exception as e:
        print(str(e))
        # if msidn_obj:
        #     data_logger = S3_sync_log(user = msidn_obj, success = False, exception = str(e))
        #     data_logger.save()
        return False, '', str(e)


def get_filename_extension(file_name):
    cType = mimetypes.guess_type(file_name)[0]
    return file_name, cType



        # if msidn_obj:
        #     data_logger = S3_sync_log(user = msidn_obj, success = False, exception = str(e))
        #     data_logger.save()
def get_s3_file_url(file_name):
    folder = "audios" if file_name.split(".")[-1] == "mp3" else "images"
    return BUCKET_URL + "docs/" + folder + "/" + file_name

def check_and_create_user(user_number: str, bot_instance) -> Tuple[str, bool]:
    """
    Check if user exists and create if needed. Also ensure BotUser entry exists.
    
    Args:
        user_number: User's phone number
        bot_instance: Bot instance object
        
    Returns:
        Tuple of (user_id, is_new_user)
    """
    try:
        from users.models import User
        
        # Check if user exists in users.User model
        try:
            user_obj = User.objects.get(contact_number=user_number)
            print(f"Existing user found: {user_obj.id}")
            is_new_user = False
            user_id = str(user_obj.id)
        except User.DoesNotExist:
            # Create new user
            user_obj = User.objects.create(
                contact_number=user_number,
                username=f"user_{user_number}",
                first_name=f"User {user_number[-4:]}"  # Use last 4 digits as name
            )
            print(f"New user created: {user_obj.id}")
            is_new_user = True
            user_id = str(user_obj.id)
        
        # Check if BotUser entry exists (for both new and existing users)
        try:
            bot_user = bot_interface.models.BotUsers.objects.get(  # type: ignore
                user=user_obj,
                bot=bot_instance
            )
            print(f"Existing BotUser found: {bot_user.id}")
        except bot_interface.models.BotUsers.DoesNotExist:  # type: ignore
            # Create BotUser entry
            bot_user = bot_interface.models.BotUsers.objects.create(  # type: ignore
                user=user_obj,
                bot=bot_instance
            )
            print(f"New BotUser created: {bot_user.id}")

        return bot_user.id, is_new_user

    except (ObjectDoesNotExist, ValueError) as e:
        logger.error("Error in check_and_create_user: %s", str(e))
        # Return empty user_id and assume new user on error
        return "", True


def check_user_community_status_direct(bot_number: str) -> tuple[bool, Dict[str, Any]]:
    """
    Check if a user (by phone number) is part of any community using direct function calls.
    
    Args:
        bot_number: User's phone number
        
    Returns:
        Tuple of (success, data) where data contains community information
    """
    try:
        from users.models import User
        from community_engagement.models import Community_user_mapping, Location
        from community_engagement.utils import get_community_summary_data
        from geoadmin.models import State
        
        if not bot_number:
            return False, {"success": False, "message": "Bot number is missing or empty"}

        user_objs = User.objects.get(contact_number=bot_number)
        print("User objects found:", user_objs)
        data = {}

        if user_objs:
            user = user_objs
            community_user_mapping_qs = Community_user_mapping.objects.filter(user=user)  # type: ignore

            if community_user_mapping_qs.exists():
                communities_list = []
                last_accessed_community_id = ""
                for mapping in community_user_mapping_qs:
                    communities_list.append(get_community_summary_data(mapping.community.id))
                    if mapping.is_last_accessed_community:
                        last_accessed_community_id = mapping.community.id

                data["is_in_community"] = True
                data["data_type"] = "community"
                data["data"] = communities_list
                data["misc"] = {"last_accessed_community_id": last_accessed_community_id}
                return True, {"success": True, "data": data}

        # User not in community - return available states for onboarding
        state_ids_with_community = Location.objects.filter(communities__isnull=False).values_list('state_id', flat=True).distinct()  # type: ignore
        states = State.objects.filter(pk__in=state_ids_with_community).order_by('state_name')  # type: ignore
        data["is_in_community"] = False
        data["data_type"] = "state"
        data["data"] = [{"id": state.pk, "name": state.state_name} for state in states]
        data["misc"] = {}
        return True, {"success": True, "data": data}
        
    except (ObjectDoesNotExist, AttributeError, ValueError) as model_error:
        logger.error("Exception in check_user_community_status_direct: %s", model_error)
        return False, {"success": False, "message": "Internal server error"}


def check_user_community_status_http(user_number: str, base_url: str = "http://localhost:8000/api/v1") -> Tuple[bool, Dict[str, Any]]:
    """
    Check if a user (by phone number) is part of any community using HTTP API calls.
    
    Args:
        user_number: User's phone number
        base_url: Base URL for the API (default: http://localhost:8000)
        
    Returns:
        Tuple of (success, data) where data contains community information
    """
    try:
        if not user_number:
            return False, {"success": False, "message": "User number is missing or empty"}

        # Make HTTP request to the community engagement API
        url = f"{base_url}/is_user_in_community/"
        payload = {"number": user_number}
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        logger.info("Making HTTP request to %s for user %s", url, user_number)

        response = requests.post(
            url, 
            json=payload, 
            headers=headers,
            timeout=30  # 30 second timeout
        )
        
        if response.status_code == 200:
            response_data = response.json()
            logger.info("HTTP response received: %s", response_data)
            return True, response_data
        else:
            logger.error("HTTP request failed with status %s: %s", response.status_code, response.text)
            return False, {"success": False, "message": f"HTTP error: {response.status_code}"}
            
    except requests.exceptions.Timeout:
        logger.error("HTTP request timed out")
        return False, {"success": False, "message": "Request timeout"}
    except requests.exceptions.ConnectionError:
        logger.error("HTTP connection error")
        return False, {"success": False, "message": "Connection error"}
    except requests.exceptions.RequestException as req_error:
        logger.error("HTTP request error: %s", req_error)
        return False, {"success": False, "message": "Request failed"}
    except json.JSONDecodeError as json_error:
        logger.error("JSON parsing error: %s", json_error)
        return False, {"success": False, "message": "Invalid JSON response"}
    except (ValueError, TypeError) as general_error:
        logger.error("Unexpected error in HTTP check: %s", general_error)
        return False, {"success": False, "message": "Unexpected error"}

def get_community_by_lat_lon(lat: str, lon: str, base_url: str = "http://localhost:8000/api/v1") -> Tuple[bool, Dict[str, Any]]:
    """Get community by latitude and longitude
    
    """
    try:
        response = requests.get(
            f"{base_url}/get_communities_by_lat_lon/",
            params={"latitude": "24.8000", "longitude": "85.0000"},
            timeout=30
        )
        logger.info("HTTP request and response to get community by lat/lon: %s", response.url)
        
        response.raise_for_status()
        return True, response.json()
    except requests.exceptions.Timeout:
        logger.error("HTTP request timed out")
        return False, {"success": False, "message": "Request timeout"}
    except requests.exceptions.ConnectionError:
        logger.error("HTTP connection error")
        return False, {"success": False, "message": "Connection error"}
    except requests.exceptions.RequestException as req_error:
        logger.error("HTTP request error: %s", req_error)
        return False, {"success": False, "message": "Request failed"}
    except json.JSONDecodeError as json_error:
        logger.error("JSON parsing error: %s", json_error)
        return False, {"success": False, "message": "Invalid JSON response"}
    except Exception as e:
        logger.error("Unexpected error in get_community_by_lat_lon: %s", e)
        return False, {"success": False, "message": "Unexpected error"}
    
def fetch_states(base_url: str = "http://localhost:8000/api/v1") -> Tuple[bool, Dict[str, Any]]:
    """Fetch all states from the API
    
    Args:
        base_url: Base URL for the API (default: http://localhost:8000)
        
    Returns:
        Tuple of (success, data) where data contains state information
    """
    try:
        response = requests.get(f"{base_url}/get_states/", timeout=30)
        logger.info("HTTP request to fetch states: %s", response.url)
        
        response.raise_for_status()
        return True, response.json()
    except requests.exceptions.Timeout:
        logger.error("HTTP request timed out")
        return False, {"success": False, "message": "Request timeout"}
    except requests.exceptions.ConnectionError:
        logger.error("HTTP connection error")
        return False, {"success": False, "message": "Connection error"}
    except requests.exceptions.RequestException as req_error:
        logger.error("HTTP request error: %s", req_error)
        return False, {"success": False, "message": "Request failed"}
    except json.JSONDecodeError as json_error:
        logger.error("JSON parsing error: %s", json_error)
        return False, {"success": False, "message": "Invalid JSON response"}
    
def check_user_community_status(bot_number: str, method: str = "direct") -> Tuple[bool, Dict[str, Any]]:
    """
    Check if a user (by phone number) is part of any community.
    
    Args:
        bot_number: User's phone number
        method: "direct" for direct function calls, "http" for HTTP API calls
        
    Returns:
        Tuple of (success, data) where data contains community information
    """
    if method == "direct":
        return check_user_community_status_direct(bot_number)
    else:
        return check_user_community_status_http(bot_number)



def jumpToSmj(data_dict):
    """
    Jump to a different SMJ by name.
    
    Args:
        data_dict: Contains 'data' with [{'smjName': 'onboarding', 'initState': 'Welcome'}]
    
    Returns:
        str: "success" or "error"
    """
    try:
        # Extract SMJ jump data
        jump_data = data_dict.get('data', [{}])
        if not jump_data:
            print("ERROR: No jump data provided")
            return "error"
            
        jump_info = jump_data[0] if isinstance(jump_data, list) else jump_data
        smj_name = jump_info.get('smjName')
        init_state = jump_info.get('initState')
        
        print(f"jumpToSmj called with smjName: {smj_name}, initState: {init_state}")
        
        if not smj_name or not init_state:
            print(f"ERROR: Missing smjName or initState in jump data: {jump_info}")
            return "error"
        
        # Check if SMJ exists in database
        try:
            new_smj = bot_interface.models.SMJ.objects.get(name=smj_name)
            new_smj_states = new_smj.smj_json
            
            # Handle Django JSONField - can be string or already parsed
            if isinstance(new_smj_states, str):
                new_smj_states = json.loads(new_smj_states)
                
            print(f"Found SMJ '{smj_name}' with {len(new_smj_states)} states")
            
            # Validate that the init_state exists in new SMJ
            state_exists = False
            for state in new_smj_states:
                if state.get('name') == init_state:
                    state_exists = True
                    break
            
            if not state_exists:
                print(f"ERROR: State '{init_state}' not found in SMJ '{smj_name}'")
                return "error"
            
            # Store jump information in data_dict for state machine to process
            # The state machine will handle the actual jump when it receives "success"
            data_dict['_smj_jump'] = {
                'smj_name': smj_name,
                'smj_id': new_smj.pk,
                'init_state': init_state,
                'states': new_smj_states
            }
            
            print(f"Prepared SMJ jump to '{smj_name}', state '{init_state}'")
            return "success"
            
        except bot_interface.models.SMJ.DoesNotExist:
            print(f"ERROR: SMJ '{smj_name}' not found in database")
            return "error"
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in SMJ '{smj_name}': {e}")
            return "error"
        except Exception as e:
            print(f"ERROR loading SMJ '{smj_name}': {e}")
            return "error"
            
    except Exception as e:
        print(f"ERROR in jumpToSmj: {e}")
        logger.error("Error in jumpToSmj: %s", str(e))
        return "error"

def callFunctionByName(funct_name, app_type, data_dict):
    event = ''
    genericInterface = bot_interface.interface.generic.GenericInterface()
    whatsappInterface = bot_interface.interface.whatsapp.WhatsAppInterface()
    bot_id = data_dict.get('bot_id', None)

    if funct_name == 'userInput':
        genericInterface.user_input(data_dict)
    elif funct_name == 'pick_img':
        genericInterface.pick_img(data_dict)
    elif funct_name == 'pick_audio':
        genericInterface.pick_audio(data_dict)
    elif funct_name == 'pick_audio_text':
        genericInterface.pick_audio_text(data_dict)
    elif funct_name == 'move_forward':
        print("calling move_forward and Data dict in move forward: ", data_dict)
        event = genericInterface.move_forward(data_dict)
        print(f"move_forward returned: {event}")
        return event
    elif funct_name == 'jumpToSmj':
        print(f"calling jumpToSmj with data_dict: {data_dict}")
        event = jumpToSmj(data_dict)
        print(f"jumpToSmj returned: {event}")
    elif funct_name == 'send_location_request':
        print(f"calling sendLocationRequest with data_dict: {data_dict}")
        event = whatsappInterface.sendLocationRequest(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"sendLocationRequest returned: {event}")
    elif funct_name == 'send_community_by_location':
        print(f"calling sendCommunityByLocation with data_dict: {data_dict}")
        event = whatsappInterface.sendCommunityByLocation(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"sendCommunityByLocation returned: {event}")
    elif funct_name == 'send_states':
        print(f"calling sendStates with data_dict: {data_dict}")
        event = whatsappInterface.sendStates(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"sendStates returned: {event}")
    elif funct_name == 'send_districts':
        print(f"calling sendDistricts with data_dict: {data_dict}")
        event = whatsappInterface.sendDistricts(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"sendDistricts returned: {event}")
    elif funct_name == 'send_community_by_state_district':
        print(f"calling sendCommunityByStateDistrict with data_dict: {data_dict}")
        event = whatsappInterface.sendCommunityByStateDistrict(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"sendCommunityByStateDistrict returned: {event}")
    elif funct_name == 'add_user_to_community':
        print(f"calling addUserToCommunity with data_dict: {data_dict}")
        event = whatsappInterface.addUserToCommunity(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"addUserToCommunity returned: {event}")
    elif funct_name == 'get_user_communities':
        print(f"calling get_user_communities with data_dict: {data_dict}")
        event = whatsappInterface.get_user_communities(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"get_user_communities returned: {event}")
    elif funct_name == 'display_single_community_message':
        print(f"calling display_single_community_message with data_dict: {data_dict}")
        event = whatsappInterface.display_single_community_message(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"display_single_community_message returned: {event}")
    elif funct_name == 'display_multiple_community_message':
        print(f"calling display_multiple_community_message with data_dict: {data_dict}")
        event = whatsappInterface.display_multiple_community_message(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"display_multiple_community_message returned: {event}")
    elif funct_name == 'generate_community_menu':
        print(f"calling generate_community_menu with data_dict: {data_dict}")
        event = whatsappInterface.generate_community_menu(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"generate_community_menu returned: {event}")
    elif funct_name == 'store_active_community_and_context':
        print(f"calling store_active_community_and_context with data_dict: {data_dict}")
        event = whatsappInterface.store_active_community_and_context(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"store_active_community_and_context returned: {event}")
    elif funct_name == 'store_selected_community_and_context':
        print(f"calling store_selected_community_and_context with data_dict: {data_dict}")
        event = whatsappInterface.store_selected_community_and_context(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"store_selected_community_and_context returned: {event}")
    elif funct_name == 'handle_service_selection':
        print(f"calling handle_service_selection with data_dict: {data_dict}")
        event = whatsappInterface.handle_service_selection(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"handle_service_selection returned: {event}")
    elif funct_name == 'store_location_data':
        print(f"calling store_location_data with data_dict: {data_dict}")
        event = whatsappInterface.store_location_data(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"store_location_data returned: {event}")
    elif funct_name == 'store_audio_data':
        print(f"calling store_audio_data with data_dict: {data_dict}")
        event = whatsappInterface.store_audio_data(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"store_audio_data returned: {event}")
    elif funct_name == 'store_photo_data':
        print(f"calling store_photo_data with data_dict: {data_dict}")
        event = whatsappInterface.store_photo_data(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"store_photo_data returned: {event}")
    elif funct_name == 'log_asset_demand_completion':
        print(f"calling log_asset_demand_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_asset_demand_completion(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"log_asset_demand_completion returned: {event}")
    elif funct_name == 'log_grievance_completion':
        print(f"calling log_grievance_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_grievance_completion(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"log_grievance_completion returned: {event}")
    elif funct_name == 'log_story_completion':
        print(f"calling log_story_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_story_completion(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"log_story_completion returned: {event}")
    elif funct_name == 'archive_and_end_session':
        print(f"calling archive_and_end_session with data_dict: {data_dict}")
        event = whatsappInterface.archive_and_end_session(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"archive_and_end_session returned: {event}")
    elif funct_name == 'add_user_to_selected_community_join_flow':
        print(f"calling add_user_to_selected_community_join_flow with data_dict: {data_dict}")
        event = whatsappInterface.add_user_to_selected_community_join_flow(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"add_user_to_selected_community_join_flow returned: {event}")
    elif funct_name == 'send_join_success_message':
        print(f"calling send_join_success_message with data_dict: {data_dict}")
        event = whatsappInterface.send_join_success_message(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"send_join_success_message returned: {event}")
    elif funct_name == 'fetch_asset_demand_status':
        print(f"calling fetch_asset_demand_status with data_dict: {data_dict}")
        event = whatsappInterface.fetch_asset_demand_status(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"fetch_asset_demand_status returned: {event}")
    elif funct_name == 'display_asset_demands_text':
        print(f"calling display_asset_demands_text with data_dict: {data_dict}")
        event = whatsappInterface.display_asset_demands_text(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"display_asset_demands_text returned: {event}")
    elif funct_name == 'fetch_story':
        print(f"calling fetch_story with data_dict: {data_dict}")
        event = whatsappInterface.fetch_story(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"fetch_story returned: {event}")
    elif funct_name == 'display_story':
        print(f"calling display_story with data_dict: {data_dict}")
        event = whatsappInterface.display_story(bot_instance_id=bot_id, data_dict=data_dict)
        print(f"display_story returned: {event}")
    return event
