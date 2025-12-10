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

from typing import Dict, Any, Tuple
from django.core.exceptions import ObjectDoesNotExist

import logging

logger = logging.getLogger(__name__)


# Community membership utility functions
def get_community_membership(bot_user):
    """Get the community_membership data from user_misc field"""
    if bot_user.user_misc and isinstance(bot_user.user_misc, dict):
        return bot_user.user_misc.get("community_membership", {})
    return {}


def add_community_membership(bot_user, community_data):
    """Add community membership data to user_misc field"""
    from datetime import datetime

    # Initialize user_misc if it's None
    if not bot_user.user_misc:
        bot_user.user_misc = {}

    # Initialize community_membership if it doesn't exist
    if "community_membership" not in bot_user.user_misc:
        bot_user.user_misc["community_membership"] = {"current_communities": []}

    # Prepare the community data with joined_date
    new_community = {
        "community_id": community_data.get("community_id"),
        "community_name": community_data.get("community_name"),
        "community_description": community_data.get("community_description"),
        "organization": community_data.get("organization"),
        "joined_date": datetime.now().isoformat(),
    }

    # Check if user is already in this community
    current_communities = bot_user.user_misc["community_membership"][
        "current_communities"
    ]
    for community in current_communities:
        if community.get("community_id") == new_community["community_id"]:
            logger.info(
                f"User {bot_user.user_id} already in community {new_community['community_id']}"
            )
            return False

    # Add the new community
    current_communities.append(new_community)
    bot_user.save()
    logger.info(
        f"Added user {bot_user.user_id} to community {new_community['community_id']}"
    )
    return True


def is_user_in_community(bot_user, community_id):
    """Check if user is already in a specific community"""
    membership = get_community_membership(bot_user)
    current_communities = membership.get("current_communities", [])

    for community in current_communities:
        if community.get("community_id") == community_id:
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
        current_communities = current_membership.get("current_communities", [])

        # Get communities from database
        user_obj = User.objects.get(contact_number=phone_number)
        community_mappings = Community_user_mapping.objects.filter(
            user=user_obj
        ).select_related("community", "community__project")

        updated = False

        # Check if we need to sync data
        if not current_communities or any(
            not comm.get("community_name") or not comm.get("organization")
            for comm in current_communities
        ):
            print("Community data is incomplete, updating from database...")

            # Initialize if needed
            if not bot_user.user_misc:
                bot_user.user_misc = {}
            if "community_membership" not in bot_user.user_misc:
                bot_user.user_misc["community_membership"] = {"current_communities": []}

            # Clear and rebuild community list
            bot_user.user_misc["community_membership"]["current_communities"] = []

            for mapping in community_mappings:
                community_data = {
                    "community_id": str(mapping.community.id),
                    "community_name": (
                        mapping.community.project.name
                        if mapping.community.project
                        else f"Community {mapping.community.id}"
                    ),
                    "community_description": (
                        getattr(mapping.community.project, "description", "")
                        if mapping.community.project
                        else ""
                    ),
                    "organization": (
                        mapping.community.project.organization.name
                        if (
                            mapping.community.project
                            and mapping.community.project.organization
                        )
                        else "Unknown Organization"
                    ),
                    "joined_date": (
                        mapping.created_at.isoformat()
                        if hasattr(mapping, "created_at")
                        else datetime.now().isoformat()
                    ),
                }

                bot_user.user_misc["community_membership"][
                    "current_communities"
                ].append(community_data)
                updated = True

            if updated:
                bot_user.save()
                print(
                    f"Updated community data for user {phone_number}: {bot_user.user_misc['community_membership']['current_communities']}"
                )

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
    "REJECTED": "रद्द किया है",
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


def check_event_type(
    app_instance_config_id,
    event_packet,
    expected_response_type,
    start_session_flag,
    event,
    app_instance_language,
    context_id,
):
    """
    This function checks whether the expected input and user input is same. Check for ist time Hi
    """
    if event:
        return True

    if not start_session_flag:
        if event_packet["type"] == "notification":
            return True
        else:
            response_type = event_packet["type"]

            print("response type in check_event_type :: ", response_type)
            input_type = {
                "text": ["text"],
                "button": ["button", "interactive"],
                "audio": ["audio", "voice"],
                "image": ["image"],
                "location": ["location"],
                "audio_text": ["text", "voice", "audio"],
            }
            is_response = False
            print("expected response type::::", expected_response_type)
            if response_type in input_type[expected_response_type]:
                if (
                    context_id
                    and "context_id" in event_packet
                    and event_packet["context_id"]
                ):
                    is_response = True
                    if context_id == event_packet["context_id"]:
                        print(
                            "expected_response_type matched: ",
                            input_type[response_type],
                        )
                        return True
                else:
                    return True

            if expected_response_type == "text":
                text = "अमान्य विकल्प!! कृपया लिख कर अपना सवाल या टिप्पणी भेजिए।"
                fp_text = "Sorry, we are expecting a text response from you."
            elif expected_response_type == "button":
                if is_response:
                    text = "आपने ग़लत मेनू से विकल्प चुना है।"
                    fp_text = "You have chosen the option from the wrong menu."
                else:
                    text = "आपने हमें जो भेजा है वो इन विकल्पों में से एक नहीं है। आपको दिए गए विकल्पों में से ही कोई विकल्प का चुनाव करना है।"
                    fp_text = "Sorry, we are expecting a button response from you."
            elif expected_response_type == "audio_text":
                text = "माफ़ कीजिये, अपनी बात लिखित में या फिर ऑडियो रिकॉर्ड करके बताएं।"
                fp_text = "Sorry, we are expecting a text response from you."
            elif expected_response_type == "image":
                text = "माफ़ कीजिये, कृपया फोटो अपलोड कर अपनी बात रखें।"
                fp_text = "Sorry, we are expecting a image response from you."
            elif expected_response_type == "location":
                text = "माफ़ कीजिये, कृपया अपना स्थान भेजें।"
                fp_text = "Sorry, we are expecting a location response from you."
            else:
                text = "अमान्य विकल्प!! "
                fp_text = "Sorry, we are expecting a different input."

            wa_id = event_packet["wa_id"]
            if app_instance_language == "hi":
                bot_interface.api.send_text(
                    app_instance_config_id=app_instance_config_id,
                    contact_number=wa_id,
                    text=text,
                )
            elif app_instance_language == "en":
                bot_interface.api.send_text(
                    app_instance_config_id=app_instance_config_id,
                    contact_number=wa_id,
                    text=fp_text,
                )
            return False
    return True


def create_media_entry(user, media_path, media_type, app_type="WA"):
    from community_engagement.models import Media

    media_details = Media(
        app_type=app_type,
        app_instance_config=user.app_instance_config,
        user=user,
        media_type=media_type,
        media_path=media_path,
    )
    media_details.save()


def detect_url(text):
    regex = "http"
    url = re.findall(regex, text)
    return url


# def get_items_query_by_params(param, data):
#     must_query_array = [
#         {
#             "query_string": {
#                 "fields": [
#                     "is_comment"
#                 ],
#                 "query": "false"
#             }
#         },
#         {
#             "query_string": {
#                 "fields": [
#                     param,
#                 ],
#                 "query": data,
#                 "default_operator": "AND"
#             }
#         },
#         {
#             "terms": {
#                 "state.keyword": [
#                     "PUB",
#                     "ARC"
#                 ]
#             }
#         }
#     ]

#     must_not_array = [
#         {
#             "query_string": {
#                 "fields": ["tags"],
#                 "query": "nfwa OR NFWA",
#                 "analyzer": "keyword"
#             }
#         }
#     ]

#     # must_query_array.append({"query_string": {"fields": ["state"], "query": ['PUB ARC'], "default_operator": "OR"}})
#     query = {"size": 200,
#              "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
#              "_source": ["skey", "pkey", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
#              "sort": [
#                  {
#                      "time": {
#                          "order": "desc"
#                      }
#                  }

#              ]
#              }
#     return query

# def get_items_by_ai_id(ai_id, channel_id):
#     must_query_array = [
#         {
#             "query_string": {
#                 "fields": [
#                     "is_comment"
#                 ],
#                 "query": "false"
#             }
#         },
#         {
#             "query_string": {
#                 "fields": [
#                     "ai",
#                 ],
#                 "query": ai_id,
#                 "default_operator": "AND"
#             }
#         },
#         {
#             "query_string": {
#                 "fields": [
#                     "channel",
#                 ],
#                 "query": channel_id,
#                 "default_operator": "AND"
#             }
#         },
#         {
#             "terms": {
#                 "state.keyword": [
#                     "PUB",
#                     "ARC"
#                 ]
#             }
#         }
#     ]
#     must_not_array = [
#         {
#             "query_string": {
#                 "fields": ["tags"],
#                 "query": "nfwa OR NFWA",
#                 "analyzer": "keyword"
#             }
#         }
#     ]
#     query = {"size": 200,
#              "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
#              "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
#              "sort": [
#                  {
#                      "time": {
#                          "order": "desc"
#                      }
#                  }

#              ]
#              }
#     return query

# def get_items_by_itemid_query(data):
#     must_query_array = [
#         {
#             "terms": {
#                 "skey": [str(data)]
#             },
#         }
#     ]

#     must_not_array = [
#         {
#             "query_string": {
#                 "fields": ["tags"],
#                 "query": "nfwa OR NFWA",
#                 "analyzer": "keyword"
#             }
#         }
#     ]

#     # must_query_array.append({"query_string": {"fields": ["state"], "query": ['PUB ARC'], "default_operator": "OR"}})
#     query = {"size": 5,
#              "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
#              "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key","item_modified_time"],
#              "sort": [
#                  {
#                      "time": {
#                          "order": "desc"
#                      }
#                  }

#              ]
#              }
#     return query

# def get_items_by_ai_id(ai_id, channel_id):
#     must_query_array = [
#         {
#             "query_string": {
#                 "fields": [
#                     "is_comment"
#                 ],
#                 "query": "false"
#             }
#         },
#         {
#             "query_string": {
#                 "fields": [
#                     "ai",
#                 ],
#                 "query": ai_id,
#                 "default_operator": "AND"
#             }
#         },
#         {
#             "query_string": {
#                 "fields": [
#                     "channel",
#                 ],
#                 "query": channel_id,
#                 "default_operator": "AND"
#             }
#         },
#         {
#             "terms": {
#                 "state.keyword": [
#                     "PUB",
#                     "ARC"
#                 ]
#             }
#         }
#     ]
#     must_not_array = [
#         {
#             "query_string": {
#                 "fields": ["tags"],
#                 "query": "nfwa OR NFWA",
#                 "analyzer": "keyword"
#             }
#         }
#     ]
#     query = {"size": 200,
#              "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
#              "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
#              "sort": [
#                  {
#                      "time": {
#                          "order": "desc"
#                      }
#                  }

#              ]
#              }
#     return query


# def create_dynamodb_connection():
#     import boto3
#     client = boto3.resource('dynamodb')
#     return client


# def get_items_query(item_count, theme, ai):
#     dynamodb = create_dynamodb_connection()

#     table = dynamodb.Table('Ai_properties')

#     items_list = []
#     channel_id = {
#         "Adolescent_Health_and_Safety": 1159,
#         "Relationship_amongst_youth": 1160,
#         "Married_Couples": 1161,
#         "Family_Planning": 1162,
#         "Education": 1163,
#         "Livelihood": 1164,
#         "Skill_Development": 1165,
#         "zd_tikakaran": 1419
#     }
#     channel = channel_id[theme]
#     publistch = "publistCh"+str(channel)
#     print("publistch : ", publistch)
#     try:
#         # Filtering the data
#         pkey = "TPH4-ai-" + ai
#         skey = 'General'
#         response = table.query(
#             KeyConditionExpression=Key('pkey').eq(pkey) & Key(
#                 'skey').eq(skey)  # & Key('channel').eq(channel)
#         )
#         item = response['Items']
#         print("item in get items query", item)

#         data = json.dumps(item, indent=4, cls=DecimalEncoder)
#         data = ast.literal_eval(data)
#         items_list = data[0][publistch]
#     except ClientError as e:
#         items_list = []
#         print(e.response['Error']['Message'])

#     must_query_array = [
#         # {
#         #   "query_string": {
#         #     "fields": [
#         #       "is_comment"
#         #     ],
#         #     "query": "false"
#         #   }
#         # },
#         {
#             "terms": {
#                 "skey": items_list
#             },
#         }

#     ]

#     query = {"size": item_count,
#              "query": {"bool": {"must": must_query_array}},
#              "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server", "misc", "channel","pri_key"],
#              #     "sort": [
#              #     {
#              #     "time": {
#              #         "order": "desc"
#              #     }
#              #     }

#              # ]
#              }
#     return publistch, query


# def create_item_url(user, items_list):
#     # items = "http://api3.gramvaani.org/user/share/audio?i=5052328.mp3&d=1cbd1fae55a04320&u=7645"
#     result_items_list = []
#     # u_data = "7645"
#     config_json = user.app_instance_config.config_json
#     if config_json:
#         config_json = json.loads(config_json)
#         u_data = config_json.get("u_data")

#     for i, item in enumerate(items_list):
#         print(item)
#         result_item = "http://api3.gramvaani.org/user/share/audio?i=" + \
#             str(item)+".mp3&d=1cbd1fae55a04320&u="+u_data
#         result_items_list.append(result_item)
#         if i >= 5:
#             break
#     return result_items_list


# def create_item_url_new(user, items_list_from_query, server,rl):
#     print("In create_item_url_new function::: ")
#     result_items_list = []
#     u_data = "7645"
#     ai = ""
#     config_json = user.app_instance_config.config_json
#     smj = Interface.models.SMJ.objects.get(pk=user.current_smj_id)
#     print("SMJ >>", smj)
#     smj_name = smj.name
#     print("smj name >>", smj_name)
#     if config_json:
#         config_json = json.loads(config_json)
#         print("config json :: ", config_json)
#         # u_data = config_json.get("u_data")
#         t_data = config_json.get("t_data")
#         print("t_data ::::", t_data)
#         # i_data = config_json.get("i_data")
#         app_data = config_json.get("app_data")
#         print("app_data :::", app_data)
#         # rl_data = config_json.get("rl")
#         # rl = rl_data.get(smj_name)
#         if config_json.get("ai"):
#             ai_data = config_json.get("ai")
#             ai = ai_data.get(smj_name)
#     print("ITEMS_LIST_FROM_QUERY ::::", items_list_from_query)
#     for item_tuple in items_list_from_query:
#         if len(item_tuple) == 5:
#             item, pri_key, title, s3_audio_url, is_youtube = item_tuple
#             print("ITEM ::::", item, pri_key)
#             item = str(item)
#             result_item = "https://appserver.gramvaani.org/share?t=item&i="+pri_key+"&rl="+rl+"&app="+app_data+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
#             result_items_list.append((result_item, title, s3_audio_url, is_youtube))
#         else:
#             item, title, s3_audio_url, is_youtube = item_tuple
#             print("ITEM ::::", item, ai)
#             item = str(item)
#             result_item = "https://appserver.gramvaani.org/share?t=item&i=TPH4-ai-"+str(ai)+"-"+item+"&rl="+rl+"&app="+app_data+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
#             result_items_list.append((result_item, title, s3_audio_url, is_youtube))
#     # for item, pri_key, title, s3_audio_url, is_youtube in items_list_from_query:
#     #     print("ITEM ::::", item, pri_key)
#     #     item = str(item)
#     #     # result_item = "http://appserver.gramvaani.org/share?t=item&i=TPH4-" + \
#     #     #     item + "&app=mvapp""&u="+u_data
#     #     result_item = "https://appserver.gramvaani.org/share?t=item&i="+pri_key+"&rl="+rl+"&app="+app_data+"&user_callerid="+user.msisdn+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
#     #     result_items_list.append(
#     #         (result_item, title, s3_audio_url, is_youtube))
#     return result_items_list


# def fetchItems(data_dict):
#     user = Interface.models.Users.objects.get(pk=data_dict.get('user_id'))
#     print("in fetchItems ::")
#     print("data dict ::", data_dict)
#     misc_data = {}
#     publist = []
#     server = ''
#     query = ''
#     theme_key = data_dict['data'][0]['getThemeFrom']
#     ai = str(data_dict['data'][0]['ai']) if data_dict['data'][0]['ai'] else ''
#     print("theme key ::", theme_key)
#     # if user.misc_data:
#     #     user_misc_data = json.loads(user.misc_data)
#     if user.current_session:
#         current_session = json.loads(user.current_session)[-1]
#         print("aaa > ",current_session)
#         if current_session.get(theme_key):
#             theme = current_session[theme_key]["event"]
#             print("theme :: ", theme)
#             if user.misc_data:
#                 user_misc_data = json.loads(user.misc_data)
#                 if user_misc_data.get("server") and user_misc_data.get(theme):
#                     print("inside if")
#                     server = user_misc_data.get("server")
#                     static_publist = user_misc_data.get(theme)
#                     # if static_publist.get(theme):
#                     #     theme_publist = static_publist.get(theme)
#                     return server, theme, static_publist
#                 elif user_misc_data.get("server") and not user_misc_data.get(theme):
#                     print("inside else")
#                     static_publist_dict = {}
#                     for data in data_dict.get("data"):
#                         source = data["source"]
#                         item_count = data["count"]
#                         if source.get("ES"):
#                             elasticSearchDict = source.get("ES")
#                             items_to_query = elasticSearchDict.get("count")
#                         if data.get("getContentBy") == "tags":
#                             param = data['getContentBy']
#                             print("data in getcontentby",  param , theme)
#                             query = get_items_query_by_params(param, theme)
#                             print("query in fetch items: PPPP", query)
#                             server, static_publist = Interface.api.getDataByElasticSearch(query)
#                             misc_data["server"] = server
#                             misc_data[theme] = static_publist
#                             user.misc_data = json.dumps(misc_data)
#                             user.save()
#                             return server, theme, static_publist

#                         elif data.get("getContentBy") == "ai_id": #start
#                             print("data in getcontentby", data['getContentBy'])
#                             ai_id_list = data[data.get("getContentBy")]
#                             ai_id = current_session[data['getThemeFrom']]["event"]
#                             channel_id = ai_id_list[ai_id]
#                             print("ai_id and channel_id ::: ", ai_id , channel_id)
#                             query = get_items_by_ai_id(ai_id,channel_id)
#                             print("QUERY >>>>>> ", query)
#                             server, static_publist = Interface.api.getDataByElasticSearch(query)
#                             print("static publist after using getDatabyElasticSearch ", static_publist)
#                             user_misc_data["server"] = server
#                             theme = str(ai_id)
#                             user_misc_data[theme] = static_publist
#                             user.misc_data = json.dumps(user_misc_data)
#                             user.save()
#                             return server, theme, static_publist
#                         else:
#                             publistch, query = get_items_query(
#                                 items_to_query, theme, ai)#end
#                             print("PUBLISTCH >>>>>", publistch)
#                             print("QUERY >>>>>> ", query)
#                         print("query in fetch items: ", query)
#                         server, static_publist = Interface.api.getDataByElasticSearch(query)
#                         print(
#                             "static publist after using getDatabyElasticSearch ", static_publist)

#                         dynamodb = create_dynamodb_connection()

#                         table = dynamodb.Table('Ai_properties')

#                         static_item_list = []

#                         try:
#                             pkey = "TPH4-ai-" + str(ai)
#                             skey = 'General'
#                             response = table.query(
#                                 KeyConditionExpression=Key('pkey').eq(
#                                     pkey) & Key('skey').eq(skey)
#                             )
#                             item = response['Items']
#                             print("items", item)

#                             data = json.dumps(
#                                 item, indent=4, cls=DecimalEncoder)
#                             data = ast.literal_eval(data)
#                             static_item_list = data[0][publistch]
#                             print("static_item_list in fetch items ::",
#                                   static_item_list)
#                         except ClientError as e:
#                             static_item_list = []
#                             print(e.response['Error']['Message'])

#                         final_item_list = []
#                         item_list_init = [a for a, b, c, d, e in static_publist]

#                         for elem in static_item_list:
#                             pos = item_list_init.index(elem)
#                             final_item_list.append(static_publist[pos])

#                         static_publist_dict["server"] = server
#                         # static_publist_dict[theme] = final_item_list
#                         # user_misc_data = static_publist_dict[theme]
#                         user_misc_data[theme] = final_item_list

#                         # misc_data[theme] = final_item_list
#                         user.misc_data = json.dumps(user_misc_data)
#                         user.save()
#                         return server, theme, final_item_list
#                 else:
#                     print(" when user.misc_data is present but server not in user.misc_data >>>>>")
#                     for data in data_dict.get("data"):
#                         source = data["source"]
#                         item_count = data["count"]
#                         if data.get("getContentBy") == "tags":
#                             param = data['getContentBy']
#                             print("Params to be passed in query :: >>> ", param, theme)
#                             query = get_items_query_by_params(param, theme)
#                             print("query in fetch items: PPPP", query)
#                             server, static_publist = Interface.api.getDataByElasticSearch(query)
#                             misc_data["server"] = server
#                             misc_data[theme] = static_publist
#                             user.misc_data = json.dumps(misc_data)
#                             user.save()
#                             return server, theme, static_publist

#                         elif data.get("getContentBy") == "ai_id": #start
#                             print("data in getcontentby", data['getContentBy'])
#                             ai_id_list = data[data.get("getContentBy")]
#                             ai_id = current_session[data['getThemeFrom']]["event"]
#                             channel_id = ai_id_list[ai_id]
#                             print("ai_id and channel_id ::: ", ai_id , channel_id)
#                             query = get_items_by_ai_id(ai_id,channel_id)
#                             print("QUERY >>>>>> ", query)
#                             server, static_publist = Interface.api.getDataByElasticSearch(query)
#                             print("static publist after using getDatabyElasticSearch ", static_publist)
#                             user_misc_data["server"] = server
#                             theme = str(ai_id)
#                             user_misc_data[theme] = static_publist
#                             user.misc_data = json.dumps(user_misc_data)
#                             user.save()
#                             return server, theme, static_publist
#                         else:
#                             items_to_query = 0
#                             if source.get("ES"):
#                                 elasticSearchDict = source.get("ES")
#                                 items_to_query = elasticSearchDict.get("count")
#                             publistch, query = get_items_query(items_to_query, theme,ai=ai)
#                             print("query in fetch items: ", query)
#                             server, static_publist = Interface.api.getDataByElasticSearch(query)
#                             dynamodb = create_dynamodb_connection()
#                             table = dynamodb.Table('Ai_properties')
#                             static_item_list = []
#                             try:
#                                 pkey = "TPH4-ai-" + str(ai)
#                                 skey = 'General'
#                                 response = table.query(
#                                     KeyConditionExpression=Key('pkey').eq(
#                                         pkey) & Key('skey').eq(skey)
#                                 )
#                                 item = response['Items']
#                                 print("items", item)

#                                 data = json.dumps(
#                                     item, indent=4, cls=DecimalEncoder)
#                                 data = ast.literal_eval(data)
#                                 static_item_list = data[0][publistch]
#                                 print("static_item_list in fetch items ::",
#                                     static_item_list)
#                             except ClientError as e:
#                                 static_item_list = []
#                                 print(e.response['Error']['Message'])

#                             final_item_list = []
#                             item_list_init = [a for a, b, c, d, e in static_publist]
#                             print("item_list_init :: ", item_list_init)
#                             print("static_publist :: ", static_publist)

#                             for elem in static_item_list:
#                                 pos = item_list_init.index(elem)
#                                 final_item_list.append(static_publist[pos])
#                             print("final item list :: ", final_item_list)
#                             misc_data["server"] = server
#                             misc_data[theme] = final_item_list
#                             user.misc_data = json.dumps(misc_data)
#                             user.save()
#                             return server, theme, final_item_list
#             else:
#                 # user_misc_data = json.loads(user.misc_data)
#                 print(" server not in user.misc_data >>>>>")
#                 for data in data_dict.get("data"):
#                     source = data["source"]
#                     item_count = data["count"]
#                     if data.get("getContentBy") == "tags":
#                         param = data['getContentBy']
#                         print("Params to be passed in query :: PPPPPPTTTTT ", param, theme)
#                         query = get_items_query_by_params(param, theme)
#                         print("query in fetch items: PPPP", query)
#                         server, static_publist = Interface.api.getDataByElasticSearch(query)
#                         print("static publist after using getDatabyElasticSearch ", static_publist)
#                         misc_data["server"] = server
#                         misc_data[theme] = static_publist
#                         user.misc_data = json.dumps(misc_data)
#                         user.save()
#                         return server, theme, static_publist
#                     else:
#                         items_to_query = 0
#                         if source.get("ES"):
#                             elasticSearchDict = source.get("ES")
#                             items_to_query = elasticSearchDict.get("count")
#                         publistch, query = get_items_query(items_to_query, theme,ai=ai)
#                         print("query in fetch items: ", query)
#                         server, static_publist = Interface.api.getDataByElasticSearch(query)
#                         dynamodb = create_dynamodb_connection()

#                         table = dynamodb.Table('Ai_properties')

#                         static_item_list = []

#                         try:
#                             pkey = "TPH4-ai-" + str(ai)
#                             skey = 'General'
#                             response = table.query(
#                                 KeyConditionExpression=Key('pkey').eq(
#                                     pkey) & Key('skey').eq(skey)
#                             )
#                             item = response['Items']
#                             print("items", item)

#                             data = json.dumps(
#                                 item, indent=4, cls=DecimalEncoder)
#                             data = ast.literal_eval(data)
#                             static_item_list = data[0][publistch]
#                             print("static_item_list in fetch items ::",
#                                   static_item_list)
#                         except ClientError as e:
#                             static_item_list = []
#                             print(e.response['Error']['Message'])

#                         final_item_list = []
#                         item_list_init = [a for a, b, c, d, e in static_publist]

#                         for elem in static_item_list:
#                             pos = item_list_init.index(elem)
#                             final_item_list.append(static_publist[pos])
#                         print("final item list :: ", final_item_list)
#                         misc_data["server"] = server
#                         misc_data[theme] = final_item_list
#                         user.misc_data = json.dumps(misc_data)
#                         user.save()
#                         return server, theme, final_item_list


# def get_jaccard_sim(a, b):
#     c = a.intersection(b)
#     return float(len(c)) / (len(a) + len(b) - len(c))


# def get_qna_model_result(in_ques, relevant_questions, sanitized_question_ids, sanitized_questions, comment_urls, similarity_threshold):
#     # ans_file = open("Ans.txt", "r", encoding='utf-8')
#     # answers = [x.strip() for x in ans_file.read().split("\n")]

#     # test_file = open("Test.txt", "r")
#     # test = test_file.read().strip()
#     user_query = in_ques
#     in_ques = in_ques.strip()
#     ques_response = {}
#     print('IN QUES:'+str(in_ques))
#     sw = ["अंदर","अत","अदि","अप","अपना","अपनि","अपनी","अपने","अभि","अभी","आदि","आप","इंहिं","इंहें","इंहों","इतयादि","इत्यादि","इन","इनका","इन्हीं","इन्हें","इन्हों","इस","इसका","इसकि","इसकी","इसके","इसमें","इसि","इसी","इसे","उंहिं","उंहें","उंहों","उन","उनका","उनकि","उनकी","उनके","उनको","उन्हीं","उन्हें","उन्हों","उस","उसके","उसि","उसी","उसे","एक","एवं","एस","एसे","ऐसे","ओर","और","कइ","कई","कर","करता","करते","करना","करने","करें","कहते","कहा","का","काफि","काफ़ी","कि","किंहें","किंहों","कितना","किन्हें","किन्हों","किया","किर","किस","किसि","किसी","किसे","की","कुछ","कुल","के","को","कोइ","कोई","कोन","कोनसा","कौन","कौनसा","गया","घर","जब","जहाँ","जहां","जा","जिंहें","जिंहों","जितना","जिधर","जिन","जिन्हें","जिन्हों","जिस","जिसे","जीधर","जेसा","जेसे","जैसा","जैसे","जो","तक","तब","तरह","तिंहें","तिंहों","तिन","तिन्हें","तिन्हों","तिस","तिसे","तो","था","थि","थी","थे","दबारा","दवारा","दिया","दुसरा","दुसरे","दूसरे","दो","द्वारा","न","नहिं","नहीं","ना","निचे","निहायत","नीचे","ने","पर","पहले","पुरा","पूरा","पे","फिर","बनि","बनी","बहि","बही","बहुत","बाद","बाला","बिलकुल","भि","भितर","भी","भीतर","मगर","मानो","मे","में","यदि","यह","यहाँ","यहां","यहि","यही","या","यिह","ये","रखें","रवासा","रहा","रहे","ऱ्वासा","लिए","लिये","लेकिन","व","वगेरह","वरग","वर्ग","वह","वहाँ","वहां","वहिं","वहीं","वाले","वुह","वे","वग़ैरह","संग","सकता","सकते","सबसे","सभि","सभी","साथ","साबुत","साभ","सारा","से","सो","हि","ही","हुअ","हुआ","हुइ","हुई","हुए","हे","हें","है","हैं","हो","होता","होति","होती","होते","होना","होने","मैं","मुझको","मेरा","अपने","आप को","हमने","हमारा","अपना","हम","आप","आपका","तुम्हारा","अपने","आप","स्वयं","वह","इसे","उसके","खुद","को","कि","वह","उसकी","उसका","खुद","ही","यह","इसके","उन्होने","अपने","क्या","जो","किसे","किसको","कि","ये","हूँ","होता","है","रहे","थी","थे","होना","गया","किया","जा रहा है","किया","है","है","पडा","होने","करना","करता","है","किया","रही","एक","लेकिन","अगर","या","क्यूंकि","जैसा","जब","तक","जबकि","की","पर","द्वारा","के","लिए","साथ","के","बारे","में","खिलाफ","बीच","में","के","माध्यम","से","दौरान","से","पहले","के","बाद","ऊपर","नीचे","को","से","तक","से","नीचे","करने","में","निकल","बंद","से","अधिक","तहत","दुबारा","आगे","फिर","एक","बार","यहाँ","वहाँ","कब","कहाँ","क्यों","कैसे","सारे","किसी","दोनो","प्रत्येक","ज्यादा","अधिकांश","अन्य","में","कुछ","ऐसा","में","कोई","मात्र","खुद","समान","इसलिए","बहुत","सकता","जायेंगे","जरा","चाहिए","अभी","और","कर","दिया","रखें","का","हैं","इस","होता","करने","ने","बनी","तो","ही","हो","इसका","था","हुआ","वाले","बाद","लिए","सकते","इसमें","दो","वे","करते","कहा","वर्ग","कई","करें","होती","अपनी","उनके","यदि","हुई","जा","कहते","जब","होते","कोई","हुए","व","जैसे","सभी","करता","उनकी","तरह","उस","आदि","इसकी","उनका","इसी","पे","तथा","भी","परंतु","इन","कम","दूर","पूरे","गये","तुम","मै","यहां","हुये","कभी","अथवा","गयी","प्रति","जाता","इन्हें","गई","अब","जिसमें","लिया","बड़ा","जाती","तब","उसे","जाते","लेकर","बड़े","दूसरे","जाने","बाहर","स्थान","उन्हें","गए","ऐसे","जिससे","समय","दोनों","किए","रहती","इनके","इनका","इनकी","सकती","आज","कल","जिन्हें","जिन्हों","तिन्हें","तिन्हों","किन्हों","किन्हें","इत्यादि","इन्हों","उन्हों","बिलकुल","निहायत","इन्हीं","उन्हीं","जितना","दूसरा","कितना","साबुत","वग़ैरह","कौनसा","लिये","दिया","जिसे","तिसे","काफ़ी","पहले","बाला","मानो","अंदर","भीतर","पूरा","सारा","उनको","वहीं","जहाँ","जीधर","के","एवं","कुछ","कुल","रहा","जिस","जिन","तिस","तिन","कौन","किस","संग","यही","बही","उसी","मगर","कर","मे","एस","उन","सो","अत"]#
#     scores = []
#     default_list = []
#     word_list1 = in_ques.split(' ')
#     if len(word_list1) <= 2:
#         default_ids = list(OrderedDict.fromkeys(
#             sanitized_question_ids))     # Get only unique IDs
#         default_ids = [int(i) for i in default_ids if i != '']
#         default_ids = default_ids[:3]   # take only top 3

#         default_sanitized_questions = list(OrderedDict.fromkeys(
#             sanitized_questions))     # Get only unique IDs
#         default_sanitized_questions = [
#             i for i in default_sanitized_questions if i != '']
#         # take only top 3
#         default_sanitized_questions = default_sanitized_questions[:3]

#         default_comment_urls = list(OrderedDict.fromkeys(
#             comment_urls))     # Get only unique IDs
#         default_comment_urls = [i for i in default_comment_urls if i != '']
#         default_comment_urls = default_comment_urls[:3]   # take only top 3

#         for i in range(len(default_sanitized_questions)):
#             default_list.append(
#                 {default_sanitized_questions[i]: comment_urls[i]})

#         ques_response['success'] = False
#         ques_response['similarity_score'] = 0
#         ques_response['entity'] = [{'list_items': default_list}]
#         ques_response['user_query'] = user_query
#         ques_response['error_type'] = "insuffient_words"
#         ques_response['error_msg'] = "आपने अपना सवाल तीन शब्दों से कम में लिखा है। कृपया अपना सवाल और विस्तार से लिखें।"

#         return json.dumps(ques_response, ensure_ascii=False)

#     word_list1 = {w for w in word_list1 if not w in sw}#
#     # print("WORD LIST 1 ::: ", word_list1)#
#     word_list1 = set(word_list1)
#     # print("WORD LIST 1 SET::: ", word_list1)#
#     # start_time = time.time()
#     for i in range(len(relevant_questions)):
#         word_list2 = relevant_questions[i].split(' ')
#         # print("WORD LIST 2 ::: ", word_list2)#
#         word_list2 = set(word_list2)
#         # print("WORD LIST 2 SET ::: ", word_list1)#
#         word_list2 = {w for w in word_list2 if not w in sw} #
#         # print("WORD LIST 2 after SW::: ", word_list2)#
#         similarity = get_jaccard_sim(word_list1, word_list2)
#         scores.append((i, similarity))
#     # print("{}s for jaccard similarity".format(time.time()-start_time))

#     sorted_score = sorted(scores, key=lambda x: -x[1])
#     print("SORTED SCORES AFTER JACARD   ::::: ", sorted_score)

#     # Currently there is no minimum threshold
#     if sorted_score[0][1] < similarity_threshold:
#         ques_response['similarity_score'] = 0
#         ques_response['success'] = False
#         ques_response['user_query'] = user_query
#         ques_response['error_type'] = "question_unclear"
#         ques_response['error_msg'] = "माफ़ करिए, आपके पूछे गए सवाल का जवाब इस वक़्त हमारे पास नहीं है"
#     elif sorted_score[0][1] < similarity_threshold:# == 0.0 it should be this
#         ques_response['similarity_score'] = 0
#         ques_response['success'] = False
#         ques_response['user_query'] = user_query
#         ques_response['error_type'] = "question_unclear"
#         ques_response['error_msg'] = "माफ़ करिए, आपके पूछे गए सवाल का जवाब इस वक़्त हमारे पास नहीं है"
#     else:
#         visited_sanitized_questions = set()
#         top_san_ques_ids = []
#         top_san_ques = []
#         top_comment_urls = []
#         for idx in range(len(sorted_score)):
#             if sanitized_question_ids[sorted_score[idx][0]] not in visited_sanitized_questions:
#                 visited_sanitized_questions.add(
#                     sanitized_question_ids[sorted_score[idx][0]])
#                 top_san_ques_ids.append(
#                     sanitized_question_ids[sorted_score[idx][0]])
#                 top_san_ques.append(sanitized_questions[sorted_score[idx][0]])
#                 top_comment_urls.append(comment_urls[sorted_score[idx][0]])
#         # top_item_ids = [int(re.search(r'\d+', re.search(r'detail/\d+', url).group()).group()) for url in top_san_ques_ids[:3]]
#         top_item_ids = [int(i) for i in top_san_ques_ids if i != '']
#         top_item_ids = top_item_ids[:3]             # Take only top 3

#         top_san_ques = [str(i) for i in top_san_ques if i != '']
#         top_san_ques = top_san_ques[:3]
#         top_comment_urls = [str(i) for i in top_comment_urls if i != '']
#         top_comment_urls = top_comment_urls[:3]
#         # [{"title_item1":"url_comment1", "title_item2":"url_comment2", "title_item3":"url_comment3"}]
#         list_items = []
#         for i in range(len(top_san_ques)):
#             list_items.append({top_san_ques[i]: top_comment_urls[i]})
#         ques_response['entity'] = [{'list_items': list_items}]
#         ques_response['similarity_score'] = sorted_score[0][1]
#         ques_response['success'] = True
#         # ques_response['answer_urls'] = top_ans_urls[:3]
#         # ques_response['sanitized_questions'] = top_san_ques[:3]
#     print("Question Response :   ", ques_response)
#     return json.dumps(ques_response, ensure_ascii=False)


# def get_transcript(media_path, app_instance_config_id):
#     app_instance_config = Interface.models.App_instance_config.objects.get(
#         pk=app_instance_config_id
#     )
#     app_config_json = json.loads(app_instance_config.config_json)
#     lang_model = app_config_json.get("lang_model")
#     if lang_model and lang_model == "bhashini":
#         text = Interface.bhashini.api.bhashini_asr(media_path, 'hi')
#         return text if text else google_speech_recognition(media_path)
#     else:
#         return google_speech_recognition(media_path)


# def google_speech_recognition(media_path):
#     stt = sr.Recognizer()
#     file = media_path[:-4] + '.wav'
#     print("File =", file)
#     subprocess.call(['ffmpeg', '-i', media_path,
#                      file])
#     print("Subprocess ffmpeg")
#     with sr.AudioFile(file) as source:
#         audio = stt.record(source)
#     text = stt.recognize_google(audio, language='hi-IN')
#     return text


def convert_image_hdpi(filepath):
    from bot_interface.api import WHATSAPP_MEDIA_PATH

    image_name = filepath.split("/")[-1]
    image_split = str(image_name).split(".")
    file_identifier = image_split[0]

    img = Image.open(filepath)
    print(str(file_identifier))
    img_format = img.format.lower()
    hdpi_im_key = file_identifier + "_hdpi." + img_format
    print(str(hdpi_im_key))
    im_hdpi_file = WHATSAPP_MEDIA_PATH + "hdpi/" + hdpi_im_key
    print(str(im_hdpi_file))
    width_0, height_0 = img.size
    hdpi_fixed_width_in_pixel = 480
    wpercent = hdpi_fixed_width_in_pixel / float(width_0)
    hsize = int(float(height_0) * float(wpercent))
    img.resize((800, 800), Image.Resampling.LANCZOS).save(im_hdpi_file)
    return im_hdpi_file


def push_to_s3(local_file_path, bucket_name, s3_file_path, cType):
    import boto3
    from nrm_app.settings import CE_BUCKET_URL

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_file_path, bucket_name, s3_file_path, ExtraArgs={"ContentType": cType}
        )
        exception = ""
        s3_url = CE_BUCKET_URL + str(s3_file_path)
        # if msidn_obj:
        #     data_logger = S3_sync_log(user = msidn_obj, success = True)
        #     data_logger.save()
        return True, s3_url, exception
    except Exception as e:
        print(str(e))
        # if msidn_obj:
        #     data_logger = S3_sync_log(user = msidn_obj, success = False, exception = str(e))
        #     data_logger.save()
        return False, "", str(e)


def get_filename_extension(file_name):
    cType = mimetypes.guess_type(file_name)[0]
    return file_name, cType

    # if msidn_obj:
    #     data_logger = S3_sync_log(user = msidn_obj, success = False, exception = str(e))
    #     data_logger.save()


def get_s3_file_url(file_name):
    folder = "audios" if file_name.split(".")[-1] == "mp3" else "images"
    return BUCKET_URL + "docs/" + folder + "/" + file_name


# def flat_df(df):
#     final_df = df.drop_duplicates(
#         subset=['Sanitized Question', 'Sanitized question ID', 'Comment_URL'], keep=False)
#     rows = []
#     for row in final_df.itertuples():
#         row_list = [row['Sanitized Question'],
#                     row['Sanitized question ID'], row['Comment_URL']]
#         rows.append(row_list)
#     return rows


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
                first_name=f"User {user_number[-4:]}",  # Use last 4 digits as name
            )
            print(f"New user created: {user_obj.id}")
            is_new_user = True
            user_id = str(user_obj.id)

        # Check if BotUser entry exists (for both new and existing users)
        try:
            bot_user = bot_interface.models.BotUsers.objects.get(  # type: ignore
                user=user_obj, bot=bot_instance
            )
            print(f"Existing BotUser found: {bot_user.id}")
        except bot_interface.models.BotUsers.DoesNotExist:  # type: ignore
            # Create BotUser entry
            bot_user = bot_interface.models.BotUsers.objects.create(  # type: ignore
                user=user_obj, bot=bot_instance
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
            return False, {
                "success": False,
                "message": "Bot number is missing or empty",
            }

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
                    communities_list.append(
                        get_community_summary_data(mapping.community.id)
                    )
                    if mapping.is_last_accessed_community:
                        last_accessed_community_id = mapping.community.id

                data["is_in_community"] = True
                data["data_type"] = "community"
                data["data"] = communities_list
                data["misc"] = {
                    "last_accessed_community_id": last_accessed_community_id
                }
                return True, {"success": True, "data": data}

        # User not in community - return available states for onboarding
        state_ids_with_community = (
            Location.objects.filter(communities__isnull=False)
            .values_list("state_id", flat=True)
            .distinct()
        )  # type: ignore
        states = State.objects.filter(pk__in=state_ids_with_community).order_by("state_name")  # type: ignore
        data["is_in_community"] = False
        data["data_type"] = "state"
        data["data"] = [{"id": state.pk, "name": state.state_name} for state in states]
        data["misc"] = {}
        return True, {"success": True, "data": data}

    except (ObjectDoesNotExist, AttributeError, ValueError) as model_error:
        logger.error("Exception in check_user_community_status_direct: %s", model_error)
        return False, {"success": False, "message": "Internal server error"}


def check_user_community_status_http(
        user_number: str, base_url: str = "https://geoserver.core-stack.org/api/v1"
) -> Tuple[bool, Dict[str, Any]]:
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
            return False, {
                "success": False,
                "message": "User number is missing or empty",
            }

        # Make HTTP request to the community engagement API
        url = f"{base_url}/is_user_in_community/"
        payload = {"number": user_number}
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        logger.info("Making HTTP request to %s for user %s", url, user_number)

        response = requests.post(
            url, json=payload, headers=headers, timeout=30  # 30 second timeout
        )

        if response.status_code == 200:
            response_data = response.json()
            logger.info("HTTP response received: %s", response_data)
            return True, response_data
        else:
            logger.error(
                "HTTP request failed with status %s: %s",
                response.status_code,
                response.text,
            )
            return False, {
                "success": False,
                "message": f"HTTP error: {response.status_code}",
            }

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


def get_community_by_lat_lon(
    lat: str, lon: str, base_url: str = "https://geoserver.core-stack.org/api/v1"
) -> Tuple[bool, Dict[str, Any]]:
    """Get community by latitude and longitude"""
    try:
        response = requests.get(
            f"{base_url}/get_communities_by_lat_lon/",
            params={"latitude": "24.8000", "longitude": "85.0000"},
            timeout=30,
        )
        logger.info(
            "HTTP request and response to get community by lat/lon: %s", response.url
        )

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


def fetch_states(
    base_url: str = "http://localhost:8000/api/v1",
) -> Tuple[bool, Dict[str, Any]]:
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


def check_user_community_status(
    bot_number: str, method: str = "direct"
) -> Tuple[bool, Dict[str, Any]]:
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
        jump_data = data_dict.get("data", [{}])
        if not jump_data:
            print("ERROR: No jump data provided")
            return "error"

        jump_info = jump_data[0] if isinstance(jump_data, list) else jump_data
        smj_name = jump_info.get("smjName")
        init_state = jump_info.get("initState")

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
                if state.get("name") == init_state:
                    state_exists = True
                    break

            if not state_exists:
                print(f"ERROR: State '{init_state}' not found in SMJ '{smj_name}'")
                return "error"

            # Store jump information in data_dict for state machine to process
            # The state machine will handle the actual jump when it receives "success"
            data_dict["_smj_jump"] = {
                "smj_name": smj_name,
                "smj_id": new_smj.pk,
                "init_state": init_state,
                "states": new_smj_states,
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
    event = ""
    genericInterface = bot_interface.interface.generic.GenericInterface()
    whatsappInterface = bot_interface.interface.whatsapp.WhatsAppInterface()
    bot_id = data_dict.get("bot_id", None)

    if funct_name == "userInput":
        genericInterface.user_input(data_dict)
    elif funct_name == "pick_img":
        genericInterface.pick_img(data_dict)
    elif funct_name == "pick_audio":
        genericInterface.pick_audio(data_dict)
    elif funct_name == "pick_audio_text":
        genericInterface.pick_audio_text(data_dict)
    elif funct_name == "move_forward":
        print("calling move_forward and Data dict in move forward: ", data_dict)
        event = genericInterface.move_forward(data_dict)
        print(f"move_forward returned: {event}")
        return event
    elif funct_name == "jumpToSmj":
        print(f"calling jumpToSmj with data_dict: {data_dict}")
        event = jumpToSmj(data_dict)
        print(f"jumpToSmj returned: {event}")
    elif funct_name == "send_location_request":
        print(f"calling sendLocationRequest with data_dict: {data_dict}")
        event = whatsappInterface.sendLocationRequest(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"sendLocationRequest returned: {event}")
    elif funct_name == "send_community_by_location":
        print(f"calling sendCommunityByLocation with data_dict: {data_dict}")
        event = whatsappInterface.sendCommunityByLocation(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"sendCommunityByLocation returned: {event}")
    elif funct_name == "send_states":
        print(f"calling sendStates with data_dict: {data_dict}")
        event = whatsappInterface.sendStates(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"sendStates returned: {event}")
    elif funct_name == "send_districts":
        print(f"calling sendDistricts with data_dict: {data_dict}")
        event = whatsappInterface.sendDistricts(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"sendDistricts returned: {event}")
    elif funct_name == "send_community_by_state_district":
        print(f"calling sendCommunityByStateDistrict with data_dict: {data_dict}")
        event = whatsappInterface.sendCommunityByStateDistrict(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"sendCommunityByStateDistrict returned: {event}")
    elif funct_name == "add_user_to_community":
        print(f"calling addUserToCommunity with data_dict: {data_dict}")
        event = whatsappInterface.addUserToCommunity(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"addUserToCommunity returned: {event}")
    elif funct_name == "get_user_communities":
        print(f"calling get_user_communities with data_dict: {data_dict}")
        event = whatsappInterface.get_user_communities(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"get_user_communities returned: {event}")
    elif funct_name == "display_single_community_message":
        print(f"calling display_single_community_message with data_dict: {data_dict}")
        event = whatsappInterface.display_single_community_message(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"display_single_community_message returned: {event}")
    elif funct_name == "display_multiple_community_message":
        print(f"calling display_multiple_community_message with data_dict: {data_dict}")
        event = whatsappInterface.display_multiple_community_message(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"display_multiple_community_message returned: {event}")
    elif funct_name == "generate_community_menu":
        print(f"calling generate_community_menu with data_dict: {data_dict}")
        event = whatsappInterface.generate_community_menu(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"generate_community_menu returned: {event}")
    elif funct_name == "store_active_community_and_context":
        print(f"calling store_active_community_and_context with data_dict: {data_dict}")
        event = whatsappInterface.store_active_community_and_context(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"store_active_community_and_context returned: {event}")
    elif funct_name == "store_selected_community_and_context":
        print(
            f"calling store_selected_community_and_context with data_dict: {data_dict}"
        )
        event = whatsappInterface.store_selected_community_and_context(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"store_selected_community_and_context returned: {event}")
    elif funct_name == "handle_service_selection":
        print(f"calling handle_service_selection with data_dict: {data_dict}")
        event = whatsappInterface.handle_service_selection(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"handle_service_selection returned: {event}")
    elif funct_name == "store_location_data":
        print(f"calling store_location_data with data_dict: {data_dict}")
        event = whatsappInterface.store_location_data(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"store_location_data returned: {event}")
    elif funct_name == "store_audio_data":
        print(f"calling store_audio_data with data_dict: {data_dict}")
        event = whatsappInterface.store_audio_data(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"store_audio_data returned: {event}")
    elif funct_name == "store_photo_data":
        print(f"calling store_photo_data with data_dict: {data_dict}")
        event = whatsappInterface.store_photo_data(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"store_photo_data returned: {event}")
    elif funct_name == "log_work_demand_completion":
        print(f"calling log_work_demand_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_work_demand_completion(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"log_work_demand_completion returned: {event}")
    elif funct_name == "log_story_completion":
        print(f"calling log_story_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_story_completion(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"log_work_demand_completion returned: {event}")
    elif funct_name == "log_grievance_completion":
        print(f"calling log_grievance_completion with data_dict: {data_dict}")
        event = whatsappInterface.log_grievance_completion(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"log_grievance_completion returned: {event}")
    elif funct_name == "archive_and_end_session":
        print(f"calling archive_and_end_session with data_dict: {data_dict}")
        event = whatsappInterface.archive_and_end_session(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"archive_and_end_session returned: {event}")
    elif funct_name == "add_user_to_selected_community_join_flow":
        print(
            f"calling add_user_to_selected_community_join_flow with data_dict: {data_dict}"
        )
        event = whatsappInterface.add_user_to_selected_community_join_flow(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"add_user_to_selected_community_join_flow returned: {event}")
    elif funct_name == "send_join_success_message":
        print(f"calling send_join_success_message with data_dict: {data_dict}")
        event = whatsappInterface.send_join_success_message(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"send_join_success_message returned: {event}")
    elif funct_name == "fetch_work_demand_status":
        print(f"calling fetch_work_demand_status with data_dict: {data_dict}")
        event = whatsappInterface.fetch_work_demand_status(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"fetch_work_demand_status returned: {event}")
    elif funct_name == "display_work_demands_text":
        print(f"calling display_work_demands_text with data_dict: {data_dict}")
        event = whatsappInterface.display_work_demands_text(
            bot_instance_id=bot_id, data_dict=data_dict
        )
        print(f"display_work_demands_text returned: {event}")
    return event
