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

# class Transliterate:
#     def __init__(self, source_language, beam_width=10, rescore=True, source_script_type="en"):
#         self.engine = XlitEngine(source_language, beam_width=beam_width, rescore=rescore, src_script_type=source_script_type)

#     def translit_sentence(self, sentence, target_language):
#         transcript = self.engine.translit_sentence(sentence,target_language)
#         return transcript

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def convertTokenExpireTime(time_str):
    time_str_formated = time_str.replace("Z", '').replace("T", ' ')
    datetime_object = datetime.strptime(
        time_str_formated, '%Y-%m-%d %H:%M:%S.%f')
    return datetime_object


def generateWhatsppToken(token_url, username, password):
    print(str(username))
    print(str(password))
    print(str(token_url))
    response = requests.post(token_url.strip(), auth=HTTPBasicAuth(
        str(username.strip()), str(password.strip())))
    print(response.text)
    response_dict = json.loads(response.text)
    print(response_dict)
    print(type(response_dict))
    return response_dict["users"][0]


def getHeader(whatsappconfig):
    headers = {}
    token_string = "Bearer " + str(whatsappconfig.token)
    headers['Authorization'] = token_string
    headers['Content-Type'] = "application/json"
    return headers


def send_message(whatsappconfig, to_number, text, type="text"):
    headers = getHeader(whatsappconfig)
    print(headers)
    url = whatsappconfig.app_message_url
    print(url)
    message_dict = {}
    message_dict["body"] = text
    print(message_dict)
    caller_json = {}
    caller_json["to"] = to_number
    caller_json["text"] = message_dict
    caller_json["type"] = type
    print(caller_json)
    response = requests.post(url=url, headers=headers, json=caller_json)
    if response.status_code == 200:
        print(response.text)


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


def create_user(app_type, app_instance_config, contact_number):
    user = Interface.models.Users(app_type=app_type, app_instance_config=app_instance_config,
                                  msisdn=contact_number, current_smj=app_instance_config.smj)
    user.save()
    return user


def create_or_update_user_profile(event_packet, user, app_type, event):
    if event:
        return
    user_profile = Interface.models.UserProfile.objects.filter(
        app_type=app_type, user=user, msisdn=event_packet["msisdn"])
    if not user_profile.exists():
        user_profile = Interface.models.UserProfile(
            app_type=app_type, user=user, msisdn=event_packet["msisdn"])
    else:
        user_profile = user_profile[0]

    set_user_profile_data(user_profile, event_packet, user)


def set_user_profile_data(user_profile, event_packet, user):
    data = data_interactive = ''
    data_type = event_packet['type']
    data = event_packet['data']
    # print("set_user_profile_data data:",data)
    # print("data_interactive", data_interactive)
    try:
        if user.current_state == 'GetName':
            user_profile.name = data
        elif user.current_state == 'GetAge':
            user_profile.age = data
        elif user.current_state == 'GetGender':
            if data == 'Male':
                data = 'M'
            elif data == 'Female':
                data = 'F'
            elif data == 'Others':
                data = 'O'
            user_profile.gender = data
        elif user.current_state == 'GetLocation':
            user_profile.location_text = data
        user_profile.save()
    except Exception as e:
        user_profile.save()
        print("Exception in set_user_profile_data : ", str(e))



def check_event_type(app_instance_config_id, event_packet, expected_response_type, start_session_flag, event, app_instance_language, context_id):
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
                'audio_text': ['text', 'voice', 'audio'],
                'order': ['order']
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


def get_items_query_by_params(param, data):
    must_query_array = [
        {
            "query_string": {
                "fields": [
                    "is_comment"
                ],
                "query": "false"
            }
        },
        {
            "query_string": {
                "fields": [
                    param,
                ],
                "query": data,
                "default_operator": "AND"
            }
        },
        {
            "terms": {
                "state.keyword": [
                    "PUB",
                    "ARC"
                ]
            }
        }
    ]

    must_not_array = [
        {
            "query_string": {
                "fields": ["tags"],
                "query": "nfwa OR NFWA",
                "analyzer": "keyword"
            }
        }
    ]

    # must_query_array.append({"query_string": {"fields": ["state"], "query": ['PUB ARC'], "default_operator": "OR"}})
    query = {"size": 200,
             "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
             "_source": ["skey", "pkey", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
             "sort": [
                 {
                     "time": {
                         "order": "desc"
                     }
                 }

             ]
             }
    return query

def get_items_by_ai_id(ai_id, channel_id):
    must_query_array = [
        {
            "query_string": {
                "fields": [
                    "is_comment"
                ],
                "query": "false"
            }
        },
        {
            "query_string": {
                "fields": [
                    "ai",
                ],
                "query": ai_id,
                "default_operator": "AND"
            }
        },
        {
            "query_string": {
                "fields": [
                    "channel",
                ],
                "query": channel_id,
                "default_operator": "AND"
            }
        },
        {
            "terms": {
                "state.keyword": [
                    "PUB",
                    "ARC"
                ]
            }
        }
    ]
    must_not_array = [
        {
            "query_string": {
                "fields": ["tags"],
                "query": "nfwa OR NFWA",
                "analyzer": "keyword"
            }
        }
    ]
    query = {"size": 200,
             "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
             "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
             "sort": [
                 {
                     "time": {
                         "order": "desc"
                     }
                 }

             ]
             }
    return query

def get_items_by_itemid_query(data):
    must_query_array = [
        {
            "terms": {
                "skey": [str(data)]
            },
        }
    ]

    must_not_array = [
        {
            "query_string": {
                "fields": ["tags"],
                "query": "nfwa OR NFWA",
                "analyzer": "keyword"
            }
        }
    ]

    # must_query_array.append({"query_string": {"fields": ["state"], "query": ['PUB ARC'], "default_operator": "OR"}})
    query = {"size": 5,
             "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
             "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key","item_modified_time"],
             "sort": [
                 {
                     "time": {
                         "order": "desc"
                     }
                 }

             ]
             }
    return query

def get_items_by_ai_id(ai_id, channel_id):
    must_query_array = [
        {
            "query_string": {
                "fields": [
                    "is_comment"
                ],
                "query": "false"
            }
        },
        {
            "query_string": {
                "fields": [
                    "ai",
                ],
                "query": ai_id,
                "default_operator": "AND"
            }
        },
        {
            "query_string": {
                "fields": [
                    "channel",
                ],
                "query": channel_id,
                "default_operator": "AND"
            }
        },
        {
            "terms": {
                "state.keyword": [
                    "PUB",
                    "ARC"
                ]
            }
        }
    ]
    must_not_array = [
        {
            "query_string": {
                "fields": ["tags"],
                "query": "nfwa OR NFWA",
                "analyzer": "keyword"
            }
        }
    ]
    query = {"size": 200,
             "query": {"bool": {"must": must_query_array, "must_not": must_not_array}},
             "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server","pri_key"],
             "sort": [
                 {
                     "time": {
                         "order": "desc"
                     }
                 }

             ]
             }
    return query


def create_dynamodb_connection():
    import boto3
    client = boto3.resource('dynamodb')
    return client


def get_items_query(item_count, theme, ai):
    dynamodb = create_dynamodb_connection()

    table = dynamodb.Table('Ai_properties')

    items_list = []
    channel_id = {
        "Adolescent_Health_and_Safety": 1159,
        "Relationship_amongst_youth": 1160,
        "Married_Couples": 1161,
        "Family_Planning": 1162,
        "Education": 1163,
        "Livelihood": 1164,
        "Skill_Development": 1165,
        "zd_tikakaran": 1419
    }
    channel = channel_id[theme]
    publistch = "publistCh"+str(channel)
    print("publistch : ", publistch)
    try:
        # Filtering the data
        pkey = "TPH4-ai-" + ai
        skey = 'General'
        response = table.query(
            KeyConditionExpression=Key('pkey').eq(pkey) & Key(
                'skey').eq(skey)  # & Key('channel').eq(channel)
        )
        item = response['Items']
        print("item in get items query", item)

        data = json.dumps(item, indent=4, cls=DecimalEncoder)
        data = ast.literal_eval(data)
        items_list = data[0][publistch]
    except ClientError as e:
        items_list = []
        print(e.response['Error']['Message'])

    must_query_array = [
        # {
        #   "query_string": {
        #     "fields": [
        #       "is_comment"
        #     ],
        #     "query": "false"
        #   }
        # },
        {
            "terms": {
                "skey": items_list
            },
        }

    ]

    query = {"size": item_count,
             "query": {"bool": {"must": must_query_array}},
             "_source": ["skey", "pkey", "ai", "sync_properties", "title", "tags", "location_string", "server", "misc", "channel","pri_key"],
             #     "sort": [
             #     {
             #     "time": {
             #         "order": "desc"
             #     }
             #     }

             # ]
             }
    return publistch, query


def create_item_url(user, items_list):
    # items = "http://api3.gramvaani.org/user/share/audio?i=5052328.mp3&d=1cbd1fae55a04320&u=7645"
    result_items_list = []
    # u_data = "7645"
    config_json = user.app_instance_config.config_json
    if config_json:
        config_json = json.loads(config_json)
        u_data = config_json.get("u_data")

    for i, item in enumerate(items_list):
        print(item)
        result_item = "http://api3.gramvaani.org/user/share/audio?i=" + \
            str(item)+".mp3&d=1cbd1fae55a04320&u="+u_data
        result_items_list.append(result_item)
        if i >= 5:
            break
    return result_items_list


def create_item_url_new(user, items_list_from_query, server,rl):
    print("In create_item_url_new function::: ")
    result_items_list = []
    u_data = "7645"
    ai = ""
    config_json = user.app_instance_config.config_json
    smj = Interface.models.SMJ.objects.get(pk=user.current_smj_id)
    print("SMJ >>", smj)
    smj_name = smj.name
    print("smj name >>", smj_name)
    if config_json:
        config_json = json.loads(config_json)
        print("config json :: ", config_json)
        # u_data = config_json.get("u_data")
        t_data = config_json.get("t_data")
        print("t_data ::::", t_data)
        # i_data = config_json.get("i_data")
        app_data = config_json.get("app_data")
        print("app_data :::", app_data)
        # rl_data = config_json.get("rl")
        # rl = rl_data.get(smj_name)
        if config_json.get("ai"):
            ai_data = config_json.get("ai")
            ai = ai_data.get(smj_name)
    print("ITEMS_LIST_FROM_QUERY ::::", items_list_from_query)
    for item_tuple in items_list_from_query:
        if len(item_tuple) == 5:
            item, pri_key, title, s3_audio_url, is_youtube = item_tuple
            print("ITEM ::::", item, pri_key)
            item = str(item)
            result_item = "https://appserver.gramvaani.org/share?t=item&i="+pri_key+"&rl="+rl+"&app="+app_data+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
            result_items_list.append((result_item, title, s3_audio_url, is_youtube))
        else:
            item, title, s3_audio_url, is_youtube = item_tuple
            print("ITEM ::::", item, ai)
            item = str(item)
            result_item = "https://appserver.gramvaani.org/share?t=item&i=TPH4-ai-"+str(ai)+"-"+item+"&rl="+rl+"&app="+app_data+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
            result_items_list.append((result_item, title, s3_audio_url, is_youtube))
    # for item, pri_key, title, s3_audio_url, is_youtube in items_list_from_query:
    #     print("ITEM ::::", item, pri_key)
    #     item = str(item)
    #     # result_item = "http://appserver.gramvaani.org/share?t=item&i=TPH4-" + \
    #     #     item + "&app=mvapp""&u="+u_data
    #     result_item = "https://appserver.gramvaani.org/share?t=item&i="+pri_key+"&rl="+rl+"&app="+app_data+"&user_callerid="+user.msisdn+"&u="+u_data+"&utm_source=gv_whatsapp&utm_medium=gv_whatsapp_bot&utm_campaign=gv_"+smj_name #eX0Mch4g"
    #     result_items_list.append(
    #         (result_item, title, s3_audio_url, is_youtube))
    return result_items_list


def fetchItems(data_dict):
    user = Interface.models.Users.objects.get(pk=data_dict.get('user_id'))
    print("in fetchItems ::")
    print("data dict ::", data_dict)
    misc_data = {}
    publist = []
    server = ''
    query = ''
    theme_key = data_dict['data'][0]['getThemeFrom']
    ai = str(data_dict['data'][0]['ai']) if data_dict['data'][0]['ai'] else ''
    print("theme key ::", theme_key)
    # if user.misc_data:
    #     user_misc_data = json.loads(user.misc_data)
    if user.current_session:
        current_session = json.loads(user.current_session)[-1]
        print("aaa > ",current_session)
        if current_session.get(theme_key):
            theme = current_session[theme_key]["event"]
            print("theme :: ", theme)
            if user.misc_data:
                user_misc_data = json.loads(user.misc_data)
                if user_misc_data.get("server") and user_misc_data.get(theme):
                    print("inside if")
                    server = user_misc_data.get("server")
                    static_publist = user_misc_data.get(theme)
                    # if static_publist.get(theme):
                    #     theme_publist = static_publist.get(theme)
                    return server, theme, static_publist
                elif user_misc_data.get("server") and not user_misc_data.get(theme):
                    print("inside else")
                    static_publist_dict = {}
                    for data in data_dict.get("data"):
                        source = data["source"]
                        item_count = data["count"]
                        if source.get("ES"):
                            elasticSearchDict = source.get("ES")
                            items_to_query = elasticSearchDict.get("count")
                        if data.get("getContentBy") == "tags":
                            param = data['getContentBy']
                            print("data in getcontentby",  param , theme)
                            query = get_items_query_by_params(param, theme)
                            print("query in fetch items: PPPP", query)
                            server, static_publist = Interface.api.getDataByElasticSearch(query)
                            misc_data["server"] = server
                            misc_data[theme] = static_publist
                            user.misc_data = json.dumps(misc_data)
                            user.save()
                            return server, theme, static_publist
                        
                        elif data.get("getContentBy") == "ai_id": #start
                            print("data in getcontentby", data['getContentBy'])
                            ai_id_list = data[data.get("getContentBy")]
                            ai_id = current_session[data['getThemeFrom']]["event"]
                            channel_id = ai_id_list[ai_id]
                            print("ai_id and channel_id ::: ", ai_id , channel_id)
                            query = get_items_by_ai_id(ai_id,channel_id)
                            print("QUERY >>>>>> ", query)
                            server, static_publist = Interface.api.getDataByElasticSearch(query)
                            print("static publist after using getDatabyElasticSearch ", static_publist)
                            user_misc_data["server"] = server
                            theme = str(ai_id)
                            user_misc_data[theme] = static_publist
                            user.misc_data = json.dumps(user_misc_data)
                            user.save()
                            return server, theme, static_publist
                        else:
                            publistch, query = get_items_query(
                                items_to_query, theme, ai)#end
                            print("PUBLISTCH >>>>>", publistch)
                            print("QUERY >>>>>> ", query)
                        print("query in fetch items: ", query)
                        server, static_publist = Interface.api.getDataByElasticSearch(query)
                        print(
                            "static publist after using getDatabyElasticSearch ", static_publist)

                        dynamodb = create_dynamodb_connection()

                        table = dynamodb.Table('Ai_properties')

                        static_item_list = []

                        try:
                            pkey = "TPH4-ai-" + str(ai)
                            skey = 'General'
                            response = table.query(
                                KeyConditionExpression=Key('pkey').eq(
                                    pkey) & Key('skey').eq(skey)
                            )
                            item = response['Items']
                            print("items", item)

                            data = json.dumps(
                                item, indent=4, cls=DecimalEncoder)
                            data = ast.literal_eval(data)
                            static_item_list = data[0][publistch]
                            print("static_item_list in fetch items ::",
                                  static_item_list)
                        except ClientError as e:
                            static_item_list = []
                            print(e.response['Error']['Message'])

                        final_item_list = []
                        item_list_init = [a for a, b, c, d, e in static_publist]

                        for elem in static_item_list:
                            pos = item_list_init.index(elem)
                            final_item_list.append(static_publist[pos])

                        static_publist_dict["server"] = server
                        # static_publist_dict[theme] = final_item_list
                        # user_misc_data = static_publist_dict[theme]
                        user_misc_data[theme] = final_item_list

                        # misc_data[theme] = final_item_list
                        user.misc_data = json.dumps(user_misc_data)
                        user.save()
                        return server, theme, final_item_list
                else:
                    print(" when user.misc_data is present but server not in user.misc_data >>>>>")
                    for data in data_dict.get("data"):
                        source = data["source"]
                        item_count = data["count"]
                        if data.get("getContentBy") == "tags":
                            param = data['getContentBy']
                            print("Params to be passed in query :: >>> ", param, theme)
                            query = get_items_query_by_params(param, theme)
                            print("query in fetch items: PPPP", query)
                            server, static_publist = Interface.api.getDataByElasticSearch(query)
                            misc_data["server"] = server
                            misc_data[theme] = static_publist
                            user.misc_data = json.dumps(misc_data)
                            user.save()
                            return server, theme, static_publist
                        
                        elif data.get("getContentBy") == "ai_id": #start
                            print("data in getcontentby", data['getContentBy'])
                            ai_id_list = data[data.get("getContentBy")]
                            ai_id = current_session[data['getThemeFrom']]["event"]
                            channel_id = ai_id_list[ai_id]
                            print("ai_id and channel_id ::: ", ai_id , channel_id)
                            query = get_items_by_ai_id(ai_id,channel_id)
                            print("QUERY >>>>>> ", query)
                            server, static_publist = Interface.api.getDataByElasticSearch(query)
                            print("static publist after using getDatabyElasticSearch ", static_publist)
                            user_misc_data["server"] = server
                            theme = str(ai_id)
                            user_misc_data[theme] = static_publist
                            user.misc_data = json.dumps(user_misc_data)
                            user.save()
                            return server, theme, static_publist
                        else:
                            items_to_query = 0
                            if source.get("ES"):
                                elasticSearchDict = source.get("ES")
                                items_to_query = elasticSearchDict.get("count")
                            publistch, query = get_items_query(items_to_query, theme,ai=ai)
                            print("query in fetch items: ", query)
                            server, static_publist = Interface.api.getDataByElasticSearch(query)
                            dynamodb = create_dynamodb_connection()
                            table = dynamodb.Table('Ai_properties')
                            static_item_list = []
                            try:
                                pkey = "TPH4-ai-" + str(ai)
                                skey = 'General'
                                response = table.query(
                                    KeyConditionExpression=Key('pkey').eq(
                                        pkey) & Key('skey').eq(skey)
                                )
                                item = response['Items']
                                print("items", item)

                                data = json.dumps(
                                    item, indent=4, cls=DecimalEncoder)
                                data = ast.literal_eval(data)
                                static_item_list = data[0][publistch]
                                print("static_item_list in fetch items ::",
                                    static_item_list)
                            except ClientError as e:
                                static_item_list = []
                                print(e.response['Error']['Message'])

                            final_item_list = []
                            item_list_init = [a for a, b, c, d, e in static_publist]
                            print("item_list_init :: ", item_list_init)
                            print("static_publist :: ", static_publist)

                            for elem in static_item_list:
                                pos = item_list_init.index(elem)
                                final_item_list.append(static_publist[pos])
                            print("final item list :: ", final_item_list)
                            misc_data["server"] = server
                            misc_data[theme] = final_item_list
                            user.misc_data = json.dumps(misc_data)
                            user.save()
                            return server, theme, final_item_list
            else:
                # user_misc_data = json.loads(user.misc_data)
                print(" server not in user.misc_data >>>>>")
                for data in data_dict.get("data"):
                    source = data["source"]
                    item_count = data["count"]
                    if data.get("getContentBy") == "tags":
                        param = data['getContentBy']
                        print("Params to be passed in query :: PPPPPPTTTTT ", param, theme)
                        query = get_items_query_by_params(param, theme)
                        print("query in fetch items: PPPP", query)
                        server, static_publist = Interface.api.getDataByElasticSearch(query)
                        print("static publist after using getDatabyElasticSearch ", static_publist)
                        misc_data["server"] = server
                        misc_data[theme] = static_publist
                        user.misc_data = json.dumps(misc_data)
                        user.save()
                        return server, theme, static_publist
                    else:
                        items_to_query = 0
                        if source.get("ES"):
                            elasticSearchDict = source.get("ES")
                            items_to_query = elasticSearchDict.get("count")
                        publistch, query = get_items_query(items_to_query, theme,ai=ai)
                        print("query in fetch items: ", query)
                        server, static_publist = Interface.api.getDataByElasticSearch(query)
                        dynamodb = create_dynamodb_connection()

                        table = dynamodb.Table('Ai_properties')

                        static_item_list = []

                        try:
                            pkey = "TPH4-ai-" + str(ai)
                            skey = 'General'
                            response = table.query(
                                KeyConditionExpression=Key('pkey').eq(
                                    pkey) & Key('skey').eq(skey)
                            )
                            item = response['Items']
                            print("items", item)

                            data = json.dumps(
                                item, indent=4, cls=DecimalEncoder)
                            data = ast.literal_eval(data)
                            static_item_list = data[0][publistch]
                            print("static_item_list in fetch items ::",
                                  static_item_list)
                        except ClientError as e:
                            static_item_list = []
                            print(e.response['Error']['Message'])

                        final_item_list = []
                        item_list_init = [a for a, b, c, d, e in static_publist]

                        for elem in static_item_list:
                            pos = item_list_init.index(elem)
                            final_item_list.append(static_publist[pos])
                        print("final item list :: ", final_item_list)
                        misc_data["server"] = server
                        misc_data[theme] = final_item_list
                        user.misc_data = json.dumps(misc_data)
                        user.save()
                        return server, theme, final_item_list


def get_jaccard_sim(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))


def get_qna_model_result(in_ques, relevant_questions, sanitized_question_ids, sanitized_questions, comment_urls, similarity_threshold):
    # ans_file = open("Ans.txt", "r", encoding='utf-8')
    # answers = [x.strip() for x in ans_file.read().split("\n")]

    # test_file = open("Test.txt", "r")
    # test = test_file.read().strip()
    user_query = in_ques
    in_ques = in_ques.strip()
    ques_response = {}
    print('IN QUES:'+str(in_ques))
    sw = ["अंदर","अत","अदि","अप","अपना","अपनि","अपनी","अपने","अभि","अभी","आदि","आप","इंहिं","इंहें","इंहों","इतयादि","इत्यादि","इन","इनका","इन्हीं","इन्हें","इन्हों","इस","इसका","इसकि","इसकी","इसके","इसमें","इसि","इसी","इसे","उंहिं","उंहें","उंहों","उन","उनका","उनकि","उनकी","उनके","उनको","उन्हीं","उन्हें","उन्हों","उस","उसके","उसि","उसी","उसे","एक","एवं","एस","एसे","ऐसे","ओर","और","कइ","कई","कर","करता","करते","करना","करने","करें","कहते","कहा","का","काफि","काफ़ी","कि","किंहें","किंहों","कितना","किन्हें","किन्हों","किया","किर","किस","किसि","किसी","किसे","की","कुछ","कुल","के","को","कोइ","कोई","कोन","कोनसा","कौन","कौनसा","गया","घर","जब","जहाँ","जहां","जा","जिंहें","जिंहों","जितना","जिधर","जिन","जिन्हें","जिन्हों","जिस","जिसे","जीधर","जेसा","जेसे","जैसा","जैसे","जो","तक","तब","तरह","तिंहें","तिंहों","तिन","तिन्हें","तिन्हों","तिस","तिसे","तो","था","थि","थी","थे","दबारा","दवारा","दिया","दुसरा","दुसरे","दूसरे","दो","द्वारा","न","नहिं","नहीं","ना","निचे","निहायत","नीचे","ने","पर","पहले","पुरा","पूरा","पे","फिर","बनि","बनी","बहि","बही","बहुत","बाद","बाला","बिलकुल","भि","भितर","भी","भीतर","मगर","मानो","मे","में","यदि","यह","यहाँ","यहां","यहि","यही","या","यिह","ये","रखें","रवासा","रहा","रहे","ऱ्वासा","लिए","लिये","लेकिन","व","वगेरह","वरग","वर्ग","वह","वहाँ","वहां","वहिं","वहीं","वाले","वुह","वे","वग़ैरह","संग","सकता","सकते","सबसे","सभि","सभी","साथ","साबुत","साभ","सारा","से","सो","हि","ही","हुअ","हुआ","हुइ","हुई","हुए","हे","हें","है","हैं","हो","होता","होति","होती","होते","होना","होने","मैं","मुझको","मेरा","अपने","आप को","हमने","हमारा","अपना","हम","आप","आपका","तुम्हारा","अपने","आप","स्वयं","वह","इसे","उसके","खुद","को","कि","वह","उसकी","उसका","खुद","ही","यह","इसके","उन्होने","अपने","क्या","जो","किसे","किसको","कि","ये","हूँ","होता","है","रहे","थी","थे","होना","गया","किया","जा रहा है","किया","है","है","पडा","होने","करना","करता","है","किया","रही","एक","लेकिन","अगर","या","क्यूंकि","जैसा","जब","तक","जबकि","की","पर","द्वारा","के","लिए","साथ","के","बारे","में","खिलाफ","बीच","में","के","माध्यम","से","दौरान","से","पहले","के","बाद","ऊपर","नीचे","को","से","तक","से","नीचे","करने","में","निकल","बंद","से","अधिक","तहत","दुबारा","आगे","फिर","एक","बार","यहाँ","वहाँ","कब","कहाँ","क्यों","कैसे","सारे","किसी","दोनो","प्रत्येक","ज्यादा","अधिकांश","अन्य","में","कुछ","ऐसा","में","कोई","मात्र","खुद","समान","इसलिए","बहुत","सकता","जायेंगे","जरा","चाहिए","अभी","और","कर","दिया","रखें","का","हैं","इस","होता","करने","ने","बनी","तो","ही","हो","इसका","था","हुआ","वाले","बाद","लिए","सकते","इसमें","दो","वे","करते","कहा","वर्ग","कई","करें","होती","अपनी","उनके","यदि","हुई","जा","कहते","जब","होते","कोई","हुए","व","जैसे","सभी","करता","उनकी","तरह","उस","आदि","इसकी","उनका","इसी","पे","तथा","भी","परंतु","इन","कम","दूर","पूरे","गये","तुम","मै","यहां","हुये","कभी","अथवा","गयी","प्रति","जाता","इन्हें","गई","अब","जिसमें","लिया","बड़ा","जाती","तब","उसे","जाते","लेकर","बड़े","दूसरे","जाने","बाहर","स्थान","उन्हें","गए","ऐसे","जिससे","समय","दोनों","किए","रहती","इनके","इनका","इनकी","सकती","आज","कल","जिन्हें","जिन्हों","तिन्हें","तिन्हों","किन्हों","किन्हें","इत्यादि","इन्हों","उन्हों","बिलकुल","निहायत","इन्हीं","उन्हीं","जितना","दूसरा","कितना","साबुत","वग़ैरह","कौनसा","लिये","दिया","जिसे","तिसे","काफ़ी","पहले","बाला","मानो","अंदर","भीतर","पूरा","सारा","उनको","वहीं","जहाँ","जीधर","के","एवं","कुछ","कुल","रहा","जिस","जिन","तिस","तिन","कौन","किस","संग","यही","बही","उसी","मगर","कर","मे","एस","उन","सो","अत"]#
    scores = []
    default_list = []
    word_list1 = in_ques.split(' ')
    if len(word_list1) <= 2:
        default_ids = list(OrderedDict.fromkeys(
            sanitized_question_ids))     # Get only unique IDs
        default_ids = [int(i) for i in default_ids if i != '']
        default_ids = default_ids[:3]   # take only top 3

        default_sanitized_questions = list(OrderedDict.fromkeys(
            sanitized_questions))     # Get only unique IDs
        default_sanitized_questions = [
            i for i in default_sanitized_questions if i != '']
        # take only top 3
        default_sanitized_questions = default_sanitized_questions[:3]

        default_comment_urls = list(OrderedDict.fromkeys(
            comment_urls))     # Get only unique IDs
        default_comment_urls = [i for i in default_comment_urls if i != '']
        default_comment_urls = default_comment_urls[:3]   # take only top 3

        for i in range(len(default_sanitized_questions)):
            default_list.append(
                {default_sanitized_questions[i]: comment_urls[i]})

        ques_response['success'] = False
        ques_response['similarity_score'] = 0
        ques_response['entity'] = [{'list_items': default_list}]
        ques_response['user_query'] = user_query
        ques_response['error_type'] = "insuffient_words"
        ques_response['error_msg'] = "आपने अपना सवाल तीन शब्दों से कम में लिखा है। कृपया अपना सवाल और विस्तार से लिखें।"

        return json.dumps(ques_response, ensure_ascii=False)

    word_list1 = {w for w in word_list1 if not w in sw}#
    # print("WORD LIST 1 ::: ", word_list1)#
    word_list1 = set(word_list1)
    # print("WORD LIST 1 SET::: ", word_list1)#
    # start_time = time.time()
    for i in range(len(relevant_questions)):
        word_list2 = relevant_questions[i].split(' ')
        # print("WORD LIST 2 ::: ", word_list2)#
        word_list2 = set(word_list2)
        # print("WORD LIST 2 SET ::: ", word_list1)#
        word_list2 = {w for w in word_list2 if not w in sw} #
        # print("WORD LIST 2 after SW::: ", word_list2)#
        similarity = get_jaccard_sim(word_list1, word_list2)
        scores.append((i, similarity))
    # print("{}s for jaccard similarity".format(time.time()-start_time)) 

    sorted_score = sorted(scores, key=lambda x: -x[1])
    print("SORTED SCORES AFTER JACARD   ::::: ", sorted_score)

    # Currently there is no minimum threshold
    if sorted_score[0][1] < similarity_threshold:
        ques_response['similarity_score'] = 0
        ques_response['success'] = False
        ques_response['user_query'] = user_query
        ques_response['error_type'] = "question_unclear"
        ques_response['error_msg'] = "माफ़ करिए, आपके पूछे गए सवाल का जवाब इस वक़्त हमारे पास नहीं है"
    elif sorted_score[0][1] < similarity_threshold:# == 0.0 it should be this
        ques_response['similarity_score'] = 0
        ques_response['success'] = False
        ques_response['user_query'] = user_query
        ques_response['error_type'] = "question_unclear"
        ques_response['error_msg'] = "माफ़ करिए, आपके पूछे गए सवाल का जवाब इस वक़्त हमारे पास नहीं है"
    else:
        visited_sanitized_questions = set()
        top_san_ques_ids = []
        top_san_ques = []
        top_comment_urls = []
        for idx in range(len(sorted_score)):
            if sanitized_question_ids[sorted_score[idx][0]] not in visited_sanitized_questions:
                visited_sanitized_questions.add(
                    sanitized_question_ids[sorted_score[idx][0]])
                top_san_ques_ids.append(
                    sanitized_question_ids[sorted_score[idx][0]])
                top_san_ques.append(sanitized_questions[sorted_score[idx][0]])
                top_comment_urls.append(comment_urls[sorted_score[idx][0]])
        # top_item_ids = [int(re.search(r'\d+', re.search(r'detail/\d+', url).group()).group()) for url in top_san_ques_ids[:3]]
        top_item_ids = [int(i) for i in top_san_ques_ids if i != '']
        top_item_ids = top_item_ids[:3]             # Take only top 3

        top_san_ques = [str(i) for i in top_san_ques if i != '']
        top_san_ques = top_san_ques[:3]
        top_comment_urls = [str(i) for i in top_comment_urls if i != '']
        top_comment_urls = top_comment_urls[:3]
        # [{"title_item1":"url_comment1", "title_item2":"url_comment2", "title_item3":"url_comment3"}]
        list_items = []
        for i in range(len(top_san_ques)):
            list_items.append({top_san_ques[i]: top_comment_urls[i]})
        ques_response['entity'] = [{'list_items': list_items}]
        ques_response['similarity_score'] = sorted_score[0][1]
        ques_response['success'] = True
        # ques_response['answer_urls'] = top_ans_urls[:3]
        # ques_response['sanitized_questions'] = top_san_ques[:3]
    print("Question Response :   ", ques_response)
    return json.dumps(ques_response, ensure_ascii=False)


def get_transcript(media_path, app_instance_config_id):
    app_instance_config = Interface.models.App_instance_config.objects.get(
        pk=app_instance_config_id
    )
    app_config_json = json.loads(app_instance_config.config_json)
    lang_model = app_config_json.get("lang_model")
    if lang_model and lang_model == "bhashini":
        text = Interface.bhashini.api.bhashini_asr(media_path, 'hi')
        return text if text else google_speech_recognition(media_path)
    else:
        return google_speech_recognition(media_path)


def google_speech_recognition(media_path):
    stt = sr.Recognizer()
    file = media_path[:-4] + '.wav'
    print("File =", file)
    subprocess.call(['ffmpeg', '-i', media_path,
                     file])
    print("Subprocess ffmpeg")
    with sr.AudioFile(file) as source:
        audio = stt.record(source)
    text = stt.recognize_google(audio, language='hi-IN')
    return text


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


def flat_df(df):
    final_df = df.drop_duplicates(
        subset=['Sanitized Question', 'Sanitized question ID', 'Comment_URL'], keep=False)
    rows = []
    for row in final_df.itertuples():
        row_list = [row['Sanitized Question'],
                    row['Sanitized question ID'], row['Comment_URL']]
        rows.append(row_list)
    return rows


def callFunctionByName(funct_name, app_type, data_dict):
    event = ''
    genericInterface = Interface.interfaces.generic.GenericInterface()
    whatsappInterface = Interface.interfaces.whatsapp.WhatsappInterface()
    bhashini = Interface.bhashini.bhashini
    if funct_name == 'uploadItem':
        genericInterface.uploadItem(data_dict)
    elif funct_name == 'uploadGrievance':
        genericInterface.uploadGrievance(data_dict)
    elif funct_name == 'pickImg':
        genericInterface.pickImg(data_dict)
    elif funct_name == 'pickAudio':
        genericInterface.pickAudio(data_dict)
    elif funct_name == 'pickAudio_Text':
        genericInterface.pickAudio_Text(data_dict)
    elif funct_name == 'pickOrder':
        genericInterface.pickOrder(data_dict)
    elif funct_name == 'userInput':
        genericInterface.userInput(data_dict)
    elif funct_name == 'moveForward':
        genericInterface.moveForward(data_dict)
    elif funct_name == 'sendAudioCardsProfile':
        event = genericInterface.sendAudioCardsProfile(data_dict)
    elif funct_name == 'sendAudioCardsSearch':
        event = genericInterface.sendAudioCardsSearch(data_dict)
    elif funct_name == 'sendAudio':
        event = genericInterface.sendAudio(data_dict)
    elif funct_name == 'ageValidator':
        event = genericInterface.ageValidator(data_dict)
    elif funct_name == 'find_keyword':
        genericInterface.find_keyword(data_dict)
    elif funct_name == 'find_order':
        genericInterface.find_order(data_dict)
    elif funct_name == 'satisfyCallPatch':
        genericInterface.satisfy_call_patch(data_dict)
    elif funct_name == 'endSession':
        genericInterface.endSession(data_dict)
    elif funct_name == 'GetQnAItems':
        genericInterface.GetQnAItems(data_dict)
    elif funct_name == 'GetQnAItemsByTheme':
        genericInterface.GetQnAItemsByTheme(data_dict)
    elif funct_name == 'sendWaitText':
        bhashini.send_wait_text(data_dict)
    elif funct_name == 'GetItemsinMenu':
        genericInterface.GetItemsinMenu(data_dict)
    elif funct_name == 'update_user_log':
        event = genericInterface.update_user_log(data_dict)
    elif funct_name == 'checkUserProfileStatus':
        whatsappInterface.checkUserProfileStatus(data_dict)
    elif funct_name == 'sendTemplate':
        whatsappInterface.sendTemplate(data_dict)
    elif funct_name == 'jumpToSmj':
        whatsappInterface.jumpToSmj(data_dict)
    elif funct_name == 'get_product_categories':
        Interface.farmphone.farmphone.get_product_categories(data_dict)
    elif funct_name == 'get_products_by_category':
        Interface.farmphone.farmphone.get_products_by_category(data_dict)
    elif funct_name == 'get_item_details':
        Interface.farmphone.farmphone.get_item_details(data_dict)
    elif funct_name == 'saveDataToUserLogs':
        genericInterface.save_data_to_user_logs(data_dict)
    elif funct_name == 'displayCartList':
        Interface.farmphone.farmphone.displayCartList(data_dict)
    elif funct_name == 'updateCart':
        Interface.farmphone.farmphone.updateCart(data_dict)
    elif funct_name == 'displayAddedItemDetails':
        Interface.farmphone.farmphone.displayAddedItemDetails(data_dict)
    elif funct_name == 'displayCartText':
        Interface.farmphone.farmphone.displayCartText(data_dict)
    elif funct_name == 'showCancellableOrders':
        Interface.farmphone.farmphone.show_cancellable_orders(data_dict)
    elif funct_name == 'checkCancellableOrders':
        Interface.farmphone.farmphone.check_cancellable_orders(data_dict)
    elif funct_name == 'showCancellableOrderDetails':
        Interface.farmphone.farmphone.show_cancellable_order_details(data_dict)
    elif funct_name == 'cancelOrder':
        Interface.farmphone.farmphone.cancel_order(data_dict)
    elif funct_name == 'pincodeValidator':
        Interface.farmphone.farmphone.pincode_validator(data_dict)
    elif funct_name == 'createDraftOrder':
        Interface.farmphone.farmphone.create_draft_order(data_dict)
    elif funct_name == 'checkUserExists':
        Interface.farmphone.farmphone.check_user_exists(data_dict)
    elif funct_name == 'checkShopifyUser':
        Interface.farmphone.farmphone.check_shopify_user(data_dict)
    elif funct_name == 'saveData':
        genericInterface.saveData(data_dict)
    elif funct_name == 'checkRefundableOrders':
        Interface.farmphone.farmphone.check_refundable_orders(data_dict)
    elif funct_name == 'showRefundableOrders':
        Interface.farmphone.farmphone.show_refundable_orders(data_dict)
    elif funct_name == 'showRefundableOrderDetails':
        Interface.farmphone.farmphone.show_refundable_order_details(data_dict)
    elif funct_name == 'checkActiveOrders':
        Interface.farmphone.farmphone.check_active_orders(data_dict)
    elif funct_name == 'showActiveOrders':
        Interface.farmphone.farmphone.show_active_orders(data_dict)
    elif funct_name == 'showActiveOrderDetails':
        Interface.farmphone.farmphone.show_active_order_details(data_dict)
    elif funct_name == 'checkOrderQuantity':
        Interface.farmphone.farmphone.check_order_quantity(data_dict)
    elif funct_name == 'checkCustomer':
        Interface.farmphone.farmphone.check_customer(data_dict)
    elif funct_name == 'createNewCustomer':
        Interface.farmphone.farmphone.create_new_customer(data_dict)
    elif funct_name == 'sendNotification':
        Interface.farmphone.farmphone.send_notification(data_dict)
    elif funct_name == 'shareContact':
        Interface.farmphone.farmphone.share_contact(data_dict)
    elif funct_name == 'emailValidator':
        Interface.farmphone.farmphone.email_validator(data_dict)
    elif funct_name == 'sendAudioFile':
        whatsappInterface.send_Audio_File(data_dict)
    elif funct_name == 'getResultFromGPT':
        bhashini.get_result_from_gpt(data_dict)
    elif funct_name == 'sendLangText':
        bhashini.send_lang_text(data_dict)
    elif funct_name == 'getGrievanceList':
        genericInterface.get_grievance_list(data_dict)
    elif funct_name == 'sendGrievanceList':
        genericInterface.send_grievance_list(data_dict)
    elif funct_name == 'getGrievanceUpdate':
        genericInterface.get_grievance_update(data_dict)
    elif funct_name == 'updatePreviousState':
        genericInterface.update_previous_state(data_dict)
    elif funct_name == 'sendProductCatalog':
        Interface.farmphone.farmphone.send_product_catalog(data_dict)
    elif funct_name == 'getCartDetails':
        Interface.farmphone.farmphone.get_cart_details(data_dict)
    elif funct_name == 'couponValidate':
        Interface.farmphone.farmphone.couponValidate(data_dict)
    elif funct_name == 'reopenGrievance':
        genericInterface.re_open_grievance(data_dict)


    return event


def text_to_speech(text, file_name, app_instance_config_id):
    app_instance_config = Interface.models.App_instance_config.objects.get(
        pk=app_instance_config_id
    )
    app_config_json = json.loads(app_instance_config.config_json)
    lang_model = app_config_json.get("lang_model")
    if lang_model and lang_model == "bhashini":
        filepath = Interface.bhashini.api.bhashini_tts(text, "hi", file_name)
        return filepath if filepath else google_tts(file_name, text)
    else:
        return google_tts(file_name, text)


def google_tts(file_name, text):
    audio = gTTS(text=text, slow=False)
    filepath = WHATSAPP_MEDIA_PATH + file_name + ".mp3"
    audio.save(filepath)
    file_identifier, cType = get_filename_extension(filepath)
    file_name = "docs/audios/" + filepath.split("/")[-1]
    status, s3_url, exc = push_to_s3(filepath, BUCKET_NAME, file_name, cType)
    return filepath


def convert_html_to_WA_text(text):
    # Replacing heading tags with asterisk for WA to make them bold
    text = re.sub(r'<h[^>]+>', '*', text)
    text = re.sub(r'<\/h[^>]+>', '*', text)
    # Removing all other html tags
    text = re.sub(r'<[^>]+>', '', text)
    return text


def getValueByDataType(datatype):
    try:
        data_type_obj = eval(datatype)
        return data_type_obj()
    except Exception as e:
        return ""
    # if datatype == "list":
    #     return []
    # elif datatype == "dict":
    #     return {}
    # else:
    #     return ""


def email_validator_check(email):
    print("Email Validator Check for the email")
    return re.match(r"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+", email)


def save_context_id_in_user_misc(msisdn, context_id, app_instance_config, app_type):
    print("In save_context_id_in_user_misc>>>")
    user = Interface.models.Users.objects.filter(
        app_type=app_type, app_instance_config=app_instance_config, msisdn=msisdn)
    print("user", user)
    if user:
        user = user[0]
        misc_data = json.loads(user.misc_data) if user.misc_data else {}
        misc_data["context_id"] = context_id
        user.misc_data = json.dumps(misc_data)
        user.save()


from rest_framework.response import Response
def isAuthenticated_user(func):
    import traceback
    def isAuth(request):
        try:
            print("request is", request)
            # from app_controller.models import uuid
            uuid = request.data.get('uuid') #uuid.objects.get(uuid=str(request.POST['uuid']), password = str(request.POST['password']))
            if uuid=="shiv":
                return func(request)
            else:
                return Response({"success": False, "message": "You dont have permission for access this API"},
                                status=403)
        except Exception as e:
            print ('Error:' +str(traceback.print_exc()))
            return Response({"success" : False, "message": "You dont have permission for access this API"}, status=403)
    return isAuth

def get_content_type(filepath):
    with open(filepath, 'rb') as file:
        num_byte = file.read(2)
    if num_byte == b'\xff\xfb' or num_byte == b'\x49\x44':
        content_type = 'audio'  # Content type for MP3 files
    else:
        content_type = 'image'

    print("Content Type:", content_type)
    return content_type


def get_item_by_skey_ES_query(skey):
    must_query_array = [
        {
            "terms": {"skey": [str(skey)]},
        }
    ]

    query = {
        "query": {"bool": {"must": must_query_array}},
        "_source": [
            "skey",
            "pkey",
            "ai",
            "sync_properties",
            "misc",
            "channel",
            "is_comment",
            "comments",
            "cm_properties",
            "time",
            "callerid"
        ],
    }
    return query


def get_grievance_ES_query(msisdn):
    date_prev_6month = datetime.now().date() - relativedelta(months=6)
    date_prev_6month = date_prev_6month.strftime("%Y-%m-%d")
    must_query_array = [
        {"query_string": {"fields": ["is_comment"], "query": "false"}},
        {"range": {"time": {"gte": date_prev_6month}}},
        {
            "terms": {"callerid": [msisdn]},
        },
        {"exists": {"field": "cm_properties"}},
        {"wildcard": {"cm_properties": "*gr_id*"}},
    ]

    query = {
        "query": {"bool": {"must": must_query_array}},
        "sort": [
            {"time": {"order": "desc"}}
        ],
        "_source": [
            "skey",
            "pkey",
            "ai",
            "sync_properties",
            "title",
            "tags",
            "server",
            "misc",
            "channel",
            "is_comment",
            "comments",
            "cm_properties",
            "time",
            "unix_ts_in_secs"
        ],
    }
    return query

