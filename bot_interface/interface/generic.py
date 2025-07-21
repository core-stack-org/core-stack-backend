import bot_interface.utils
import bot_interface.models
import bot_interface.api
import bot_interface.tasks

import json
from datetime import datetime
from django.apps import apps

class GenericInterface:
    def __init__(self) -> None:
        pass

    @staticmethod
    def user_input(datadict: dict) -> None:
        user_session = bot_interface.models.UserSessions.objects.get(user=datadict.get("user_id"))
        user_session.expected_response_type = "text"
        user_session.current_state = datadict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(
            id=datadict.get("smjid"))
        user_session.save()

    @staticmethod
    def pick_img(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_session.expected_response_type = "image"
        user_session.current_state = data_dict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(
            id=data_dict.get("smjid"))
        user_session.save()

    @staticmethod
    def pick_audio(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_session.expected_response_type = "audio"
        user_session.current_state = data_dict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(
            id=data_dict.get("smjid"))
        user_session.save()

    @staticmethod
    def pick_audio_text(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_session.expected_response_type = "audio_text"
        user_session.current_state = data_dict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(
            id=data_dict.get("smjid"))
        user_session.save()

    @staticmethod
    def move_forward(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_session.current_state = data_dict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(
            id=data_dict.get("smj_id"))
        user_session.save()
        print("DATA DICT IN moveForward : ", data_dict)
        
        # Instead of creating a new Celery task, prepare internal transition data
        event = data_dict["event"] if data_dict.get("event") else "success"
        
        # Store transition data in data_dict for the state machine to handle internally
        data_dict["_internal_transition"] = {
            "action": "continue",
            "event": event,
            "smj_id": data_dict.get("smj_id"),
            "state": data_dict.get("state"),
            "data": data_dict.get("data"),
            "user_number": user_session.phone,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message_id": data_dict.get("message_id", "")
        }
        
        print("INTERNAL TRANSITION DATA IN moveForward : ", data_dict["_internal_transition"])
        
        # Return success to indicate the transition is prepared
        return "internal_transition_prepared"

    @staticmethod
    def save_data(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_current_session = json.loads(user_session.current_session)[-1]
        print("DATA DICT in  saveData: ", data_dict)
        data = data_dict.get("data")[-1]
        print("data in saveData : ", data)
        getDataFrom = data.get("getDataFrom") if data.get(
            "getDataFrom") else ""
        data_to_save = user_current_session[getDataFrom]["data"]
        print("data_to_save :", data_to_save)
        saveIn = data.get("saveIn") if data.get("saveIn") else ""
        if "$" in saveIn:
            split_values = saveIn.split("$")
            # Assign the split values to individual variables
            model = split_values[0]
            field = split_values[1]
            print("model name :", model)
            print("field name :", field)
            model_class = apps.get_model(
                app_label="Interface", model_name=model)
            # Check if the field exists in the model
            if hasattr(model_class, field):
                model_instance = model_class.objects.get(
                    user=data_dict.get("user_id"))
                # Assign the value to the field
                setattr(model_instance, field, str(data_to_save))
                model_instance.save()
                print("email saved")

    @staticmethod
    def end_session(data_dict):
        user_session = bot_interface.models.UserSessions.objects.get(user=data_dict.get("user_id"))
        user_archive = bot_interface.models.UserArchive(
            app_type=user_session.app_type,
            bot=user_session.bot,
            user=user_session.user,
            time=datetime.now(),
            archived_session=user_session.current_session,
        )
        user_archive.save()
        user_session.current_session = ""
        user_session.expected_response_type = ""
        user_session.current_state = ""
        user_session.current_smj = None
        user_session.misc_data = ""
        user_session.save()

