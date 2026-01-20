from typing import Any, Dict
import json

import bot_interface
import logging

logger = logging.getLogger(__name__)


def _extract_whatsapp_value(data: Any) -> Any:
    """
    Extracts 'value' from WhatsApp webhook entry format.
    """
    if (
        isinstance(data, list)
        and data
        and isinstance(data[0], dict)
        and "changes" in data[0]
    ):
        changes = data[0].get("changes", [])
        if changes and "value" in changes[0]:
            return changes[0]["value"]

    return data


def _load_user_session(bot_id: int, user_id=None):
    bot = bot_interface.models.Bot.objects.get(id=bot_id)
    if user_id:
        user = bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot)
        return bot, user
    return bot


def _extract_lat_lon_from_session(session, state_name: str, field: str = "data"):
    if not session or not state_name:
        return None, None

    for entry in session:
        if isinstance(entry, dict) and state_name in entry:
            value = entry[state_name].get(field)
            if isinstance(value, str):
                parts = value.split(",")
                if len(parts) == 2:
                    return parts[0].strip(), parts[1].strip()

    return None, None


def _normalize_location_response(response):
    if isinstance(response, tuple):
        success, data = response
        return data if success and isinstance(data, dict) else None

    if isinstance(response, dict):
        return response

    return None


def _normalize_location(source):
    return {
        "latitude": str(source.get("latitude", "")).strip(),
        "longitude": str(source.get("longitude", "")).strip(),
        "address": source.get("address", ""),
        "name": source.get("name", ""),
    }


def _get_user_session(bot_instance_id, user_id):
    if isinstance(user_id, str):
        user_id = int(user_id)

    try:
        bot = bot_interface.models.Bot.objects.get(id=bot_instance_id)
    except bot_interface.models.Bot.DoesNotExist:
        bot = bot_interface.models.Bot.objects.first()
        if not bot:
            return None

    try:
        return bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot)
    except bot_interface.models.UserSessions.DoesNotExist:
        return bot_interface.models.UserSessions.objects.filter(user=user_id).first()


def _prepare_and_send_list(
    bot_instance_id: int,
    user_session,
    menu_list: list,
    text: str,
    response_type: str = "button",
):
    user_session.expected_response_type = response_type
    user_session.save()

    response = bot_interface.api.send_list_msg(
        bot_instance_id=bot_instance_id,
        contact_number=user_session.phone,
        text=text,
        menu_list=menu_list,
    )

    return "success" if response and response.get("messages") else "failure"


def _extract_ids_from_session(session, mappings):
    """
    Extract values from user session based on mapping config.
    mappings example:
    [
        {"state": "SendState", "field": "data"},
        {"state": "SendDistrict", "field": "data"}
    ]
    """
    result = {}

    if not session:
        return result

    for mapping in mappings:
        state_name = mapping.get("state")
        field = mapping.get("field", "data")

        for entry in session:
            if state_name in entry:
                result[state_name] = entry[state_name].get(field)
                break

    return result


def _build_community_data(user_id, community_id, api_response):
    from bot_interface.utils import add_community_membership
    from community_engagement.models import Community_user_mapping
    from users.models import User

    community_data = {
        "community_id": community_id,
        "community_name": api_response.get("community_name", ""),
        "community_description": api_response.get("community_description", ""),
        "organization": api_response.get("organization", ""),
    }

    if community_data["community_name"] and community_data["organization"]:
        return community_data

    try:
        bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
        phone = bot_user.user.contact_number
        user_obj = User.objects.get(contact_number=phone)

        mapping = (
            Community_user_mapping.objects.filter(
                user=user_obj, community_id=community_id
            )
            .select_related("community", "community__project")
            .first()
        )

        if mapping and mapping.community.project:
            project = mapping.community.project
            community_data["community_name"] = (
                community_data["community_name"] or project.name
            )
            community_data["organization"] = community_data["organization"] or getattr(
                project.organization, "name", ""
            )
            community_data["community_description"] = community_data[
                "community_description"
            ] or getattr(project, "description", "")

    except Exception:
        community_data["community_name"] = (
            community_data["community_name"] or f"Community {community_id}"
        )
        community_data["organization"] = (
            community_data["organization"] or "Unknown Organization"
        )

    return community_data


def _extract_community_id_from_session(session, mappings):
    """
    Extract community_id from session using getDataFrom config
    """
    if not session or not mappings:
        return None

    for mapping in mappings:
        state_name = mapping.get("state")
        field = mapping.get("field", "misc")

        for entry in session:
            if state_name in entry and isinstance(entry[state_name], dict):
                return entry[state_name].get(field)

    return None


def _load_bot_and_session(bot_instance_id, user_id):
    bot = bot_interface.models.Bot.objects.filter(id=bot_instance_id).first()
    if not bot:
        raise ValueError("Bot not found")

    session = bot_interface.models.UserSessions.objects.filter(
        user=user_id, bot=bot
    ).first()
    if not session:
        raise ValueError("UserSession not found")

    return bot, session


def _get_bot_user_and_phone(user_id):
    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
    return bot_user, bot_user.user.contact_number


def _send_text(bot_instance_id, phone, text):
    response = bot_interface.api.send_text(
        bot_instance_id=bot_instance_id,
        contact_number=phone,
        text=text,
    )
    return bool(response and response.get("messages"))


def _resolve_community_id(user, user_id, event, event_data):
    """
    Resolve community_id based on event or button interaction.
    """
    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
    current_communities = bot_user.user_misc.get("community_membership", {}).get(
        "current_communities", []
    )
    if current_communities:
        return current_communities[0]["community_id"]
    # Button click
    if event_data.get("type") == "button":
        value = event_data.get("misc") or event_data.get("data")

        if value == "continue_last_accessed":
            success, api = bot_interface.utils.check_user_community_status_http(
                user.phone
            )
            if success and api.get("success"):
                return api["data"].get("misc", {}).get("last_accessed_community_id")
            return bot_user.user_misc.get("community_membership", {}).get(
                "last_accessed_community_id"
            )

        if value and value.startswith("community_"):
            return value.split("_")[1]

    # Non-button / service event
    if event in ("continue_single", "continue_last"):
        if event == "continue_single" and current_communities:
            return current_communities[0].get("community_id")

        success, api = bot_interface.utils.check_user_community_status_http(user.phone)
        if success and api.get("success"):
            return api["data"].get("misc", {}).get("last_accessed_community_id")
        if current_communities:
            return current_communities[0].get("community_id")

    return None


def _get_user_session(self, bot_instance_id, user_id):
    if isinstance(user_id, str):
        user_id = int(user_id)

    try:
        bot = bot_interface.models.Bot.objects.get(id=bot_instance_id)
    except bot_interface.models.Bot.DoesNotExist:
        bot = bot_interface.models.Bot.objects.first()
        if not bot:
            return None

    try:
        return bot_interface.models.UserSessions.objects.get(user=user_id, bot=bot)
    except bot_interface.models.UserSessions.DoesNotExist:
        return bot_interface.models.UserSessions.objects.filter(user=user_id).first()


def _extract_location_data(user, data_dict):
    """
    Priority:
    1. data_dict.location_data
    2. data_dict.event_data (WhatsApp location)
    3. user.current_session fallback
    """

    # 1️⃣ Structured packet (best case)
    loc = data_dict.get("location_data")
    if loc:
        return _normalize_location(loc)

    # 2️⃣ WhatsApp event payload
    event = data_dict.get("event_data", {})
    if event.get("type") == "location":
        misc = event.get("misc", {})
        if misc.get("latitude") and misc.get("longitude"):
            return _normalize_location(misc)

        raw = event.get("data")
        if raw and "," in raw:
            lat, lon = raw.split(",", 1)
            return _normalize_location({"latitude": lat, "longitude": lon})

    # 3️⃣ Session fallback
    for item in user.current_session or []:
        if "SendLocationRequest" in item:
            data = item["SendLocationRequest"]
            misc = data.get("misc", {})
            raw = data.get("data")

            if misc.get("latitude") and misc.get("longitude"):
                return _normalize_location(misc)

            if raw and "," in raw:
                lat, lon = raw.split(",", 1)
                return _normalize_location({"latitude": lat, "longitude": lon})

    return None


def _archive_user_session(user, reason):
    """
    Create UserArchive entry from active session.
    """
    try:
        bot_user = bot_interface.models.BotUsers.objects.get(id=user.user_id)

        duration = None
        if user.started_at and user.last_updated_at:
            duration = (user.last_updated_at - user.started_at).total_seconds()

        archive_data = {
            "session_data": user.current_session,
            "misc_data": user.misc_data,
            "final_state": user.current_state,
            "session_duration": duration,
            "archived_reason": reason,
        }

        bot_interface.models.UserArchive.objects.create(
            app_type=user.app_type,
            bot=user.bot,
            user=bot_user,
            session_data=archive_data,
        )

        logger.info(f"Session archived for user {user.user_id}")

    except Exception:
        logger.exception(f"Failed to archive session for user {user.user_id}")
        raise


def _reset_user_session(user):
    """
    Clear all active session fields.
    """
    user.current_session = {}
    user.current_smj = None
    user.current_state = ""
    user.expected_response_type = "text"
    user.misc_data = {}
    user.save(
        update_fields=[
            "current_session",
            "current_smj",
            "current_state",
            "expected_response_type",
            "misc_data",
        ]
    )

    logger.info(f"Session reset completed for user {user.user_id}")


def _get_bot_instance(bot_instance_id):
    return (
        bot_interface.models.Bot.objects.filter(id=bot_instance_id).first()
        or bot_interface.models.Bot.objects.first()
    )


def _get_user_session(bot_instance, user_id):
    if not user_id:
        return None
    try:
        return bot_interface.models.UserSessions.objects.get(
            user=int(user_id), bot=bot_instance
        )
    except bot_interface.models.UserSessions.DoesNotExist:
        return bot_interface.models.UserSessions.objects.filter(
            user=int(user_id)
        ).first()


def _get_bot_user(user_id):
    return bot_interface.models.BotUsers.objects.filter(id=user_id).first()


def _get_smj(smj_id):
    return bot_interface.models.SMJ.objects.filter(id=smj_id).first()


def _detect_flow_type(data_dict):
    """
    Detects the flow type from incoming payload.
    Priority:
    1. Explicit flow_type
    2. Intent
    3. Current state
    4. Fallback: 'default'
    """

    if not isinstance(data_dict, dict):
        return "default"

    return (
        data_dict.get("flow_type")
        or data_dict.get("intent")
        or data_dict.get("current_state")
        or "default"
    )


def _extract_media_data(user, data_dict, media_type):
    """
    Extracts and normalizes media data for storage.

    Returns:
        dict  -> for audio
        list  -> for photos
        None  -> if media not found
    """
    print("insire meida data")
    if not isinstance(data_dict, dict):
        print("None data dict")
        return None

    # -------- AUDIO --------
    if media_type == "audio":
        print(f"Inside audio")
        print(data_dict)
        audio = data_dict.get("audio_data")

        if not audio:
            return None

        return {
            "media_id": audio.get("media_id"),
            "url": audio.get("data"),
            "timestamp": data_dict.get("timestamp"),
            "uploaded_by": user.id,
        }

    # -------- PHOTOS --------
    if media_type == "photo":
        print("photo")
        print(data_dict)
        photo = data_dict.get("photo_data")

        if not photo:
            return None

        extracted = []

        return {
            "media_id": photo.get("media_id"),
            "url": photo.get("data"),
            "timestamp": data_dict.get("timestamp"),
            "uploaded_by": user.id,
        }

    return None


def _build_misc_payload(
    self,
    *,
    flow_type,
    flow_data=None,
    community_context=None,
    bot_instance=None,
    user=None,
    bot_user=None,
):
    """
    Builds a structured misc_payload for storing contextual bot data.

    Args:
        flow_type (str): Current conversational flow (e.g. work_demand, feedback)
        flow_data (dict): Flow-specific data collected so far
        community_context (dict): Location / community metadata
        bot_instance: Bot instance model/object
        user: End user model/object
        bot_user: Bot user / system user

    Returns:
        dict: JSON-serializable misc payload
    """

    payload = {
        "flow_type": flow_type,
        "flow_data": flow_data or {},
        "community_context": community_context or {},
    }

    # -----------------------
    # USER CONTEXT
    # -----------------------
    if user:
        payload["user"] = {
            "id": getattr(user, "id", None),
            "username": getattr(user, "username", None),
            "phone": getattr(user, "phone", None),
        }

    # -----------------------
    # BOT CONTEXT
    # -----------------------
    if bot_user:
        payload["bot_user"] = {
            "id": getattr(bot_user, "id", None),
            "username": getattr(bot_user, "username", None),
        }

    if bot_instance:
        payload["bot_instance"] = {
            "id": getattr(bot_instance, "id", None),
            "name": getattr(bot_instance, "name", None),
            "channel": getattr(bot_instance, "channel", None),
        }

    # -----------------------
    # META / DEBUG INFO
    # -----------------------
    payload["meta"] = {
        "source": "bot",
        "version": "v1",
    }

    return payload
