import json
from typing import Dict, Any
from django.utils import timezone
import bot_interface.interface.generic
import bot_interface.models
import bot_interface.utils
import bot_interface.api
import bot_interface.auth
import requests

from bot_interface.data_classes import EventPacket
from bot_interface.helper import (
    _extract_whatsapp_value,
    _load_user_session,
    _normalize_location_response,
    _extract_lat_lon_from_session,
    _prepare_and_send_list,
    _extract_community_id_from_session,
    _build_community_data,
    _get_user_session,
    _extract_location_data,
    _archive_user_session,
    _reset_user_session,
    _get_bot_instance,
    _get_bot_user,
    _get_smj,
    _extract_ids_from_session,
    _extract_media_data,
    _detect_flow_type,
    _build_misc_payload,
    _resolve_community_id,
)
from geoadmin.models import State, District, Block
import logging
from nrm_app.settings import CE_API_URL, CE_BUCKET_NAME

logger = logging.getLogger(__name__)


class WhatsAppInterface(bot_interface.interface.generic.GenericInterface):
    """WhatsApp interface implementation for handling WhatsApp Business API interactions"""

    @staticmethod
    def create_event_packet(
        json_obj: Any, bot_id: int, event: str = "start"
    ) -> Dict[str, Any]:
        """
        Create an event packet from WhatsApp webhook data.
        """

        print("create_event_packet called with bot_id:", bot_id, type(bot_id))

        try:
            bot_interface.models.Bot.objects.get(id=bot_id)
        except bot_interface.models.Bot.DoesNotExist:
            raise ValueError(f"Bot with id {bot_id} not found")

        # Parse JSON if string
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        # Handle WhatsApp webhook list format
        if isinstance(json_obj, list) and len(json_obj) > 0:
            if "changes" in json_obj[0]:
                json_obj = json_obj[0]["changes"][0]["value"]

        # Base packet
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
            "state": "",
            "context_id": "",
        }

        # Process incoming message
        if "contacts" in json_obj:
            WhatsAppInterface._process_message_data(json_obj, event_packet, bot_id)

        # Preserve session context (smj_id, state, user_id)
        WhatsAppInterface._preserve_user_context(event_packet, bot_id)

        # 🔥 CRITICAL FIX: normalize interactive events
        WhatsAppInterface._normalize_interactive_event(event_packet, bot_id)

        return event_packet

    @staticmethod
    def _normalize_interactive_event(event_packet: Dict, bot_id: int) -> None:
        """
        Convert WhatsApp interactive/button replies into semantic SMJ events.
        """

        if event_packet.get("type") != "button":
            return

        user_number = event_packet.get("user_number")
        if not user_number:
            return

        try:
            bot = bot_interface.models.Bot.objects.get(id=bot_id)
            user_session = bot_interface.models.UserSessions.objects.get(
                user__phone=user_number, bot=bot
            )
        except Exception:
            return

        # 🔑 COMMUNITY SELECTION CONTRACT
        if user_session.expected_response_type == "community":
            event_packet["event"] = "success"
            # misc already has community_id
            return

        # Default fallback
        event_packet["event"] = "success"

    @staticmethod
    def _process_message_data(json_obj: Dict, event_packet: Dict, bot_id: int) -> None:
        """Process regular WhatsApp message data"""

        # Extract contact info
        contact = json_obj.get("contacts", [{}])[0]
        wa_id = contact.get("wa_id", "")
        event_packet["user_number"] = wa_id
        event_packet["wa_id"] = wa_id

        # No messages present
        messages = json_obj.get("messages")
        if not messages:
            logger.warning(f"No messages found in event packet")
            return

        message = messages[0]
        data_type = message.get("type", "")

        event_packet["timestamp"] = message.get("timestamp", "")
        event_packet["message_id"] = message.get("id", "")
        event_packet["type"] = data_type

        # Message type routing
        handlers = {
            "text": WhatsAppInterface._process_text_message,
            "interactive": WhatsAppInterface._process_interactive_response,
            "location": WhatsAppInterface._process_location_message,
            "image": lambda m, e: WhatsAppInterface._process_image_message(
                m, e, bot_id
            ),
            "audio": lambda m, e: WhatsAppInterface._process_audio_message(
                m, e, bot_id
            ),
            "voice": lambda m, e: WhatsAppInterface._process_audio_message(
                m, e, bot_id
            ),
        }

        handler = handlers.get(data_type)
        if handler:
            handler(message, event_packet)

    @staticmethod
    def _process_interactive_response(message: Dict, event_packet: Dict) -> None:
        event_packet["type"] = "button"
        interactive = message["interactive"]

        if interactive.get("list_reply"):
            title = interactive["list_reply"]["title"]
            reply_id = interactive["list_reply"]["id"]
        else:
            title = interactive["button_reply"]["title"]
            reply_id = interactive["button_reply"]["id"]

        # Always keep UI text separate
        event_packet["data"] = title

        # Always keep raw id available
        event_packet["misc"] = reply_id

        # 🔑 IMPORTANT: set semantic event
        event_packet["event"] = reply_id

        if message.get("context"):
            event_packet["context_id"] = message["context"]["id"]

    @staticmethod
    def _download_and_upload_media(
        bot_id: int, mime_type: str, media_id: str, media_type: str
    ) -> str:
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

        status, url, error = bot_interface.utils.push_to_s3(
            filepath, CE_BUCKET_NAME, file_name, file_extension
        )
        if status:
            print(f"URL:  {url}")
            return url
        else:
            return "Failed"

    @staticmethod
    def _process_text_message(message: Dict, event_packet: Dict) -> None:
        """Process text message"""
        event_packet["type"] = "text"
        event_packet["data"] = message["text"]["body"]

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
            "address": location.get("address", ""),
        }

        logger.info(f"Processed Location Message: {latitude} long: {longitude}")

    @staticmethod
    def _preserve_user_context(event_packet: Dict, bot_id: int) -> None:
        """Preserve current user context for proper state transitions"""
        try:
            user_number = event_packet["user_number"]
            if not user_number:
                logger.warning(f"No user number found in event packed")
                return

            # Find the current user session to preserve context
            bot_instance = _load_user_session(bot_id)

            # Check if user exists in our system
            try:
                bot_user = bot_interface.models.BotUsers.objects.get(
                    user__contact_number=user_number
                )
                user_session = bot_interface.models.UserSessions.objects.get(
                    user_id=bot_user.id, bot=bot_instance
                )

                # Preserve current SMJ and state context
                if user_session.current_smj and user_session.current_state:
                    event_packet["smj_id"] = user_session.current_smj.id
                    event_packet["state"] = user_session.current_state
                    logger.info(
                        f"Preserved user context - SMJ: {user_session.current_smj.id}, State: {user_session.current_state}"
                    )

            except (
                bot_interface.models.BotUsers.DoesNotExist,
                bot_interface.models.UserSessions.DoesNotExist,
            ):
                # User or session doesn't exist yet, will be handled in session creation
                logger.error("No existing user session found, will use default context")

        except Exception as e:
            logger.error(f"Error preserving user context: {e}")
            # Don't fail the whole process if context preservation fails

    @staticmethod
    def _process_media_message(
        message: Dict,
        event_packet: Dict,
        bot_id: int,
        media_type: str,
    ) -> None:
        """
        Process WhatsApp media messages (image, audio, voice).
        """

        event_packet["type"] = media_type

        # Extract media block safely
        media_block = message.get(media_type) or message.get("voice")
        if not media_block:
            return

        media_id = media_block.get("id", "")
        mime_type = media_block.get("mime_type", "")

        event_packet["media_id"] = media_id

        # Download and upload media
        filepath = WhatsAppInterface._download_and_upload_media(
            bot_id=bot_id,
            mime_type=mime_type,
            media_id=media_id,
            media_type=media_type,
        )

        event_packet["data"] = filepath

    @staticmethod
    def _process_image_message(message: Dict, event_packet: Dict, bot_id: int) -> None:
        WhatsAppInterface._process_media_message(
            message, event_packet, bot_id, media_type="image"
        )

    @staticmethod
    def _process_audio_message(message: Dict, event_packet: Dict, bot_id: int) -> None:
        WhatsAppInterface._process_media_message(
            message, event_packet, bot_id, media_type="audio"
        )

    def store_selected_community_and_context(self, bot_instance_id, data_dict):
        """
        Store selected community from menu and context.
        Args:
            bot_instance_id (int): The ID of the bot instance.
            data_dict (dict): Dictionary containing user and session data.
        Returns:
            str: "community_selected" or "failure"
        """
        print(
            f"DEBUG: store_selected_community_and_context called with bot_instance_id={bot_instance_id}"
        )
        print(f"DEBUG: data_dict keys: {list(data_dict.keys())}")
        print(f"DEBUG: data_dict contents: {data_dict}")

        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")
            print(f"DEBUG: bot_instance={bot_instance}, user_id={user_id}")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )
            print(f"DEBUG: Found user session: {user}")

            # Extract community ID from button data or event
            community_id = None
            event_data = data_dict.get("event_data", {})
            print(f"DEBUG: event_data: {event_data}")
            print(f"DEBUG: event_data type: {event_data.get('type')}")
            if event_data.get("type") == "button":
                button_value = event_data.get("misc") or event_data.get("data")
                print(f"DEBUG: Button event detected - button_value: {button_value}")

                # ✅ Always preserve semantic event
                event_to_process = event_data.get("event") or "success"

                # -------- Payload handling ONLY --------

                if button_value == "continue_last_accessed":
                    print("DEBUG: User chose to continue with last accessed community")

                    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                    success, api_response = (
                        bot_interface.utils.check_user_community_status_http(user.phone)
                    )

                    if success and api_response.get("success"):
                        community_id = (
                            api_response.get("data", {})
                            .get("misc", {})
                            .get("last_accessed_community_id")
                        )
                        print(
                            f"DEBUG: Got last accessed community ID from API: {community_id}"
                        )
                    else:
                        community_id = bot_user.user_misc.get(
                            "community_membership", {}
                        ).get("last_accessed_community_id")
                        print(
                            f"DEBUG: Got last accessed community ID from stored data: {community_id}"
                        )

                elif button_value and str(button_value).startswith("community_"):
                    community_id = button_value.split("_", 1)[1]
                    print(f"DEBUG: Extracted community ID from button: {community_id}")

                elif button_value and button_value.isdigit():
                    # Your current case: misc = "8"
                    community_id = button_value
                    print(f"DEBUG: Numeric community ID detected: {community_id}")
            else:
                # For non-button events, extract from event field
                event = data_dict.get("event", "")
                print(f"DEBUG: Non-button event - processing event: {event}")

                if event == "continue_last_accessed":
                    # User wants to continue with last accessed community
                    print(
                        f"DEBUG: User chose to continue with last accessed community (event)"
                    )
                    bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                    success, api_response = (
                        bot_interface.utils.check_user_community_status_http(user.phone)
                    )
                    if success and api_response.get("success"):
                        community_data = api_response.get("data", {})
                        community_id = community_data.get("misc", {}).get(
                            "last_accessed_community_id"
                        )
                        print(
                            f"DEBUG: Got last accessed community ID from API: {community_id}"
                        )
                    else:
                        # Fallback to stored data
                        community_id = bot_user.user_misc.get(
                            "community_membership", {}
                        ).get("last_accessed_community_id")
                        print(
                            f"DEBUG: Got last accessed community ID from stored data: {community_id}"
                        )
                elif event.startswith("community_"):
                    community_id = event.split("_")[1]
                    print(f"DEBUG: Extracted community ID from event: {community_id}")

            print(f"DEBUG: Final community_id: {community_id}")

            if community_id:
                # Store in UserSessions.misc_data
                if not user.misc_data:
                    user.misc_data = {}

                user.misc_data["active_community_id"] = community_id
                user.misc_data["navigation_context"] = "community_selection"
                user.misc_data["last_service_event"] = "choose_other"
                user.save()

                print(
                    f"DEBUG: Stored selected community {community_id} with context community_selection"
                )
                print(f"DEBUG: Returning 'community_selected'")
                return "community_selected"
            else:
                print(
                    f"DEBUG: Could not extract community ID from event data, returning 'failure'"
                )
                return "failure"

        except Exception as e:
            print(f"DEBUG: Exception in store_selected_community_and_context: {e}")
            import traceback

            traceback.print_exc()
            return "failure"

    def _store_media_data(
        self, bot_instance_id, data_dict, media_type, flow_type="work_demand"
    ):
        """
        Generic handler for audio/photo storage.
        """
        try:
            smj = _get_smj(data_dict.get("smj_id"))
            flow_type = getattr(smj, "name", None)
            print(f"Flow Type: {flow_type}")

            print(f"data dict : {data_dict}")
            user = _get_user_session(bot_instance_id, data_dict.get("user_id"))
            print(f"user: {user}")
            if not user:
                return "failure"

            media_data = _extract_media_data(user, data_dict, media_type)
            if not media_data:
                print(f"No {media_type} data found")
                return "failure"

            user.misc_data = user.misc_data or {}
            user.misc_data.setdefault(flow_type, {})

            if media_type == "audio":
                user.misc_data[flow_type]["audio"] = media_data
            else:
                user.misc_data[flow_type].setdefault("photos", [])
                if isinstance(media_data, list):
                    user.misc_data[flow_type]["photos"].extend(media_data)
                else:
                    user.misc_data[flow_type]["photos"].append(media_data)

            user.save()
            print(f"Stored {media_type} data for {flow_type}: {media_data}")
            return "success"

        except Exception:
            logger.exception(f"_store_media_data failed for {media_type}")
            return "failure"

    @staticmethod
    def _download_media(
        bot_id: int,
        mime_type: str,
        media_id: str,
        media_type: str,
    ) -> str:
        """
        Download media (image, audio, voice, video) from WhatsApp API.
        """

        try:
            logger.info(
                "Downloading %s | bot_id=%s | mime_type=%s | media_id=%s",
                media_type,
                bot_id,
                mime_type,
                media_id,
            )

            # Select correct API method
            download_fn_map = {
                "image": bot_interface.api.download_image,
                "audio": bot_interface.api.download_audio,
                "voice": bot_interface.api.download_audio,
            }

            download_fn = download_fn_map.get(media_type)
            if not download_fn:
                raise ValueError(f"Unsupported media type: {media_type}")

            response, filepath = download_fn(bot_id, mime_type, media_id)

            if response and response.status_code == 200 and filepath:
                logger.info(
                    "%s downloaded successfully: %s",
                    media_type.capitalize(),
                    filepath,
                )
                return filepath

            raise RuntimeError(
                f"{media_type.capitalize()} download failed | "
                f"status={response.status_code if response else 'None'} | "
                f"filepath={filepath}"
            )

        except Exception as exc:
            logger.exception("Error downloading %s media", media_type)
            raise

    @staticmethod
    def _download_image(bot_id: int, mime_type: str, media_id: str) -> str:
        return WhatsAppInterface._download_media(
            bot_id, mime_type, media_id, media_type="image"
        )

    @staticmethod
    def _download_audio(bot_id: int, mime_type: str, media_id: str) -> str:
        return WhatsAppInterface._download_media(
            bot_id, mime_type, media_id, media_type="audio"
        )

    @staticmethod
    def _is_interactive_message(json_obj: Dict) -> bool:
        """Check if this is an interactive message"""
        return bool(json_obj.get("id") and json_obj.get("type") == "interactive")

    @staticmethod
    def _process_interactive_message(json_obj: Dict, event_packet: Dict) -> None:
        """Process interactive message"""
        event_packet["message_id"] = json_obj["id"]
        event_packet["message_to"] = json_obj.get("to", "")
        event_packet["type"] = json_obj["type"]

    def sendText(self, bot_id, data_dict):
        logger.info("data_dict in sendText: %s", data_dict)
        data = data_dict.get("text")
        user_id = data_dict.get("user_id")
        bot_instance, user_session = _load_user_session(bot_id=bot_id, user_id=user_id)
        text = data[0].get(bot_instance.language)
        try:
            response = bot_interface.api.send_text(
                bot_instance_id=bot_id, contact_number=user_session.phone, text=text
            )
            user_session.expected_response_type = "text"
            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_instance.smj
            user_session.save()
            if response and response.get("messages"):
                logger.info(
                    f"Whatsapp Text Message {text} send to {user_session.phone} under {user_session.id} session"
                )
                return "success"
            else:
                logger.info(
                    f"Whatsapp Text Message {text} send to {user_session.phone} under {user_session.id} session Failed {response.text}"
                )
                return "failure"

        except Exception as e:
            logger.error(
                f"Whatsapp Text Message {text} send to {user_session.phone} under {user_session.id} session Failed {error}"
            )
            return "failure"

    def sendButton(self, bot_instance_id, data_dict):
        logger.info("data_dict in sendButton: %s", data_dict)
        logger.info("bot instance id: %s", bot_instance_id)
        user_id = data_dict.get("user_id")
        bot_instance, user_session = _load_user_session(bot_instance_id, user_id)
        data = data_dict.get("menu")
        # caption = "Select an option:"  # Default
        if data_dict.get("caption"):
            caption = data_dict.get("caption")
        elif data and len(data) > 0 and "caption" in data[0]:
            caption = data[0]["caption"]
        try:
            user_session.expected_response_type = "button"
            user_session.current_state = data_dict.get("state")

            # Handle SMJ object lookup with error handling
            smj_id = data_dict.get("smj_id")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            user_session.save()
            if len(data) > 3:
                print("in send_list msg ::")
                label = "Select Here"
                response = bot_interface.api.send_list_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user_session.phone,
                    text=caption,
                    menu_list=data,
                    button_label=label,
                )

            elif len(data) <= 3 and ("description" in data[0]):
                label = "Select Here"
                response = bot_interface.api.send_list_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user_session.phone,
                    text=caption,
                    menu_list=data,
                    button_label=label,
                )

            else:

                label = "Select Here"
                response = bot_interface.api.send_button_msg(
                    bot_instance_id=bot_instance_id,
                    contact_number=user_session.phone,
                    text=caption,
                    menu_list=data,
                )

            # Return success/failure based on API response
            if response and response.get("messages"):
                logger.info(
                    f"Menu Send to user {user_session.phone} under {user_session.id} session"
                )
                return "success"
            else:
                logger.error(
                    f"Failed to Send Menu to user {user_session.phone} under {user_session.id} session"
                )
                return "failure"

        except Exception as e:
            logger.error(f"Failed to Send Menu to user: {e}")
            return "failure"

    def sendLocationRequest(self, bot_instance_id, data_dict, text=None):
        user_id = data_dict.get("user_id")
        smj_id = data_dict.get("smj_id")
        bot_instance, user_session = _load_user_session(bot_instance_id, user_id)
        user_session.expected_response_type = "location"
        user_session.current_state = data_dict.get("state")
        user_session.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)
        user_session.save()
        if not text:
            text = "कृपया स्थान भेजें"

        # Handle SMJ object lookup with error handling

        response = bot_interface.api.send_location_request(
            bot_instance_id=bot_instance_id,
            contact_number=user_session.phone,
            text=text,
        )

        # Return success/failure based on API response
        if response and response.get("messages"):
            logger.info(f"Location message sent for session {user_session.id}")
            return "success"
        else:
            logger.error(f"Failed: Location message failed {response.text}")
            return "failure"

    def sendCommunityByLocation(self, bot_instance_id, data_dict):
        """
        Send community options based on user's location.
        """
        logger.debug("sendCommunityByLocation called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            data = data_dict.get("data", {})
            get_data_from = data.get("getDataFrom", {})

            state_name = get_data_from.get("state")
            field_name = get_data_from.get("field", "data")

            user_session.current_state = data_dict.get("state")

            latitude, longitude = _extract_lat_lon_from_session(
                user_session.current_session, state_name, field_name
            )

            if not latitude or not longitude:
                logger.error("Location data not found in user session")
                return "failure"

            # Set SMJ
            smj_id = data_dict.get("smj_id")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(id=smj_id)

            from public_api.views import get_location_info_by_lat_lon
            from community_engagement.utils import get_communities

            logger.debug("Fetching location info lat=%s lon=%s", latitude, longitude)
            raw_response = get_location_info_by_lat_lon(
                lat=float(latitude), lon=float(longitude)
            )

            location_data = _normalize_location_response(raw_response)
            if not location_data:
                return "no_communities"

            communities = get_communities(
                state_name=location_data.get("State", ""),
                district_name=location_data.get("District", ""),
                block_name=location_data.get("Block", ""),
            )

            if not communities or communities == "no_communities":
                return "no_communities"

            menu_list = [
                {
                    "value": c.get("community_id"),
                    "label": c.get("name"),
                    "description": c.get("description", ""),
                }
                for c in communities
            ]

            response = bot_interface.api.send_list_msg(
                bot_instance_id=bot_instance_id,
                contact_number=user_session.phone,
                text="कृपया अपना समुदाय चुनें",
                menu_list=menu_list,
            )

            user_session.expected_response_type = "community"
            user_session.save()

            return (
                "success" if response and response.get("messages") else "no_communities"
            )

        except Exception:
            logger.exception("Error in sendCommunityByLocation")
            return "no_communities"

    def sendStates(self, bot_instance_id, data_dict):
        logger.debug("sendStates called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )

            from community_engagement.models import Location
            from geoadmin.models import State

            state_ids = (
                Location.objects.filter(communities__isnull=False)
                .values_list("state_id", flat=True)
                .distinct()
            )

            states = State.objects.filter(pk__in=state_ids).order_by("state_name")

            menu_list = [
                {"value": s.pk, "label": s.state_name, "description": ""}
                for s in states
            ]

            return _prepare_and_send_list(
                bot_instance_id,
                user_session,
                menu_list,
                text="कृपया अपना राज्य चुनें",
            )

        except Exception:
            logger.exception("Error in sendStates")
            return "failure"

    def sendDistricts(self, bot_instance_id, data_dict):
        logger.debug("sendDistricts called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )

            data = data_dict.get("data", {})
            get_data_from = data.get("getDataFrom", {})
            state_name = get_data_from.get("state")
            field = get_data_from.get("field", "data")

            if not state_name:
                logger.error("Missing getDataFrom state config")
                return "failure"

            # Extract state_id from session
            state_id = None
            for entry in user_session.current_session or []:
                if state_name in entry:
                    state_id = entry[state_name].get(field)
                    break

            if not state_id:
                logger.error("State ID not found in session")
                return "failure"

            response = requests.get(
                f"{CE_API_URL}get_districts_with_community/",
                params={"state_id": state_id},
                timeout=30,
            )
            response.raise_for_status()

            districts = response.json().get("data", [])
            if not districts:
                return "failure"

            menu_list = [
                {"value": d.get("id"), "label": d.get("name"), "description": ""}
                for d in districts
            ]

            return _prepare_and_send_list(
                bot_instance_id,
                user_session,
                menu_list,
                text="कृपया अपना जिला चुनें",
            )

        except Exception:
            logger.exception("Error in sendDistricts")
            return "failure"

    def sendCommunityByStateDistrict(self, bot_instance_id, data_dict):
        """
        Send community options based on selected state and district.
        """
        logger.debug("sendCommunityByStateDistrict called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            # Update session context
            user_session.expected_response_type = "community"
            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )

            data = data_dict.get("data", {})
            mappings = data.get("getDataFrom", [])

            if not mappings:
                logger.error("Missing getDataFrom configuration")
                return "failure"

            # Extract state & district IDs
            extracted = _extract_ids_from_session(
                user_session.current_session, mappings
            )

            state_id = extracted.get("SendState")
            district_id = extracted.get("SendDistrict")

            if not state_id or not district_id:
                logger.error(
                    "Missing required IDs | state_id=%s district_id=%s",
                    state_id,
                    district_id,
                )
                return "failure"

            # Call community API
            response = requests.get(
                f"{CE_API_URL}get_communities_by_location/",
                params={"state_id": state_id, "district_id": district_id},
                timeout=30,
            )
            response.raise_for_status()

            api_response = response.json()
            communities = (
                api_response.get("data") if api_response.get("success") else None
            )

            if not communities:
                logger.info(
                    "No communities found for state=%s district=%s",
                    state_id,
                    district_id,
                )
                return "failure"

            menu_list = [
                {
                    "value": c.get("community_id"),
                    "label": c.get("name"),
                    "description": c.get("description", ""),
                }
                for c in communities
            ]

            send_response = bot_interface.api.send_list_msg(
                bot_instance_id=bot_instance_id,
                contact_number=user_session.phone,
                text="कृपया अपना समुदाय चुनें",
                menu_list=menu_list,
            )

            user_session.save()

            return (
                "success"
                if send_response and send_response.get("messages")
                else "failure"
            )

        except Exception:
            logger.exception("Error in sendCommunityByStateDistrict")
            return "failure"

    def addUserToCommunity(self, bot_instance_id, data_dict):
        """
        Add user to a community.
        """
        logger.debug("addUserToCommunity called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            # Update session
            user_session.expected_response_type = "button"
            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )

            data = data_dict.get("data", {})
            mappings = data.get("getDataFrom")

            if not mappings:
                logger.error("Missing getDataFrom configuration")
                return "failure"

            if isinstance(mappings, dict):
                mappings = [mappings]

            community_id = _extract_community_id_from_session(
                user_session.current_session, mappings
            )

            if not community_id:
                logger.error("Community ID not found in session")
                return "failure"

            logger.info(
                "Adding user %s to community %s",
                user_session.phone,
                community_id,
            )

            response = requests.post(
                f"{CE_API_URL}add_user_to_community/",
                data={"community_id": community_id, "number": user_session.phone},
                timeout=30,
            )
            response.raise_for_status()
            api_response = response.json()

            if not api_response.get("success"):
                logger.error("Community API returned failure")
                return "failure"

            # Build & store community membership
            community_data = _build_community_data(user_id, community_id, api_response)

            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            from bot_interface.utils import add_community_membership

            add_community_membership(bot_user, community_data)

            user_session.save()
            return "success"

        except Exception:
            logger.exception("Error in addUserToCommunity")
            return "failure"

    def get_user_communities(self, bot_instance_id, data_dict):
        """
        Determine whether user has single or multiple communities
        using API with DB fallback.
        """
        logger.debug("get_user_communities called")

        try:
            user_id = data_dict.get("user_id")
            bot_instance, user_session = _load_user_session(bot_instance_id, user_id)

            # Update session context
            user_session.current_state = data_dict.get("state")
            user_session.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )
            user_session.save()

            # Get phone number
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            phone_number = bot_user.user.contact_number

            logger.info("Fetching communities for phone=%s", phone_number)

            # Sync local cache (non-blocking)
            try:
                from bot_interface.utils import sync_community_data_from_database

                sync_community_data_from_database(bot_user)
            except Exception:
                logger.warning("Community sync failed (non-critical)", exc_info=True)

            communities = self._fetch_user_communities(phone_number)
            return self._determine_community_flow(communities, user_id)

        except Exception:
            logger.exception("Error in get_user_communities")
            return "failure"

    def _fetch_user_communities(self, phone_number: str) -> list:
        """
        Fetch user communities using API first, then DB fallback.
        """
        communities = self._get_communities_via_api(phone_number)

        if communities:
            return communities

        logger.info("API returned no communities, using DB fallback")
        return self._get_communities_via_database(phone_number)

    def _get_communities_via_api(self, phone_number: str) -> list:
        try:
            response = requests.get(
                f"{CE_API_URL}get_community_by_user/",
                params={"number": phone_number},
                timeout=10,
            )

            if response.status_code != 200:
                logger.warning("Community API returned status=%s", response.status_code)
                return []

            payload = response.json()
            return payload.get("data", []) if payload.get("success") else []

        except requests.exceptions.RequestException:
            logger.warning("Community API request failed", exc_info=True)
            return []

    def _get_communities_via_database(self, phone_number: str) -> list:
        try:
            from community_engagement.models import Community_user_mapping
            from users.models import User

            user = User.objects.filter(contact_number=phone_number).first()
            if not user:
                return []

            mappings = Community_user_mapping.objects.filter(user=user).select_related(
                "community", "community__project"
            )

            return [
                {
                    "community_id": m.community.id,
                    "community_name": (
                        m.community.project.name
                        if m.community.project
                        else f"Community {m.community.id}"
                    ),
                    "community_description": (
                        getattr(m.community.project, "description", "")
                        if m.community.project
                        else ""
                    ),
                    "organization": (
                        m.community.project.organization.name
                        if m.community.project and m.community.project.organization
                        else ""
                    ),
                    "created_at": (
                        m.created_at.isoformat() if hasattr(m, "created_at") else None
                    ),
                }
                for m in mappings
            ]

        except Exception:
            logger.exception("Database fallback failed")
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
            print(
                "User has no communities - this shouldn't happen in community features flow"
            )
            return "failure"

    def display_community_message(self, bot_instance_id, data_dict, mode="single"):
        """
        Display welcome message for users with single or multiple communities.

        Args:
            bot_instance_id (int): Bot instance ID
            data_dict (dict): User/session data
            mode (str): "single" or "multiple"

        Returns:
            str: "success" or "failure"
        """
        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            # Get user session
            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )

            # Get user communities
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            communities = bot_user.user_misc.get("community_membership", {}).get(
                "current_communities", []
            )

            if not communities:
                return "failure"

            # Resolve community name
            community_name = communities[0].get("community_name", "Unknown Community")

            if mode == "multiple":
                success, api_response = (
                    bot_interface.utils.check_user_community_status_http(user.phone)
                )
                if success and api_response.get("success"):
                    data = api_response.get("data", {})
                    last_id = data.get("misc", {}).get("last_accessed_community_id")

                    for c in data.get("data", []):
                        if c.get("community_id") == last_id:
                            community_name = c.get("name", community_name)
                            break

                welcome_text = (
                    f"🏠 आपने पिछली बार {community_name} समुदाय का उपयोग किया था।"
                )
            else:
                welcome_text = (
                    f"🏠 आप {community_name} समुदाय का हिस्सा हैं।\n\n"
                    "आप कैसे आगे बढ़ना चाहेंगे?"
                )

            response = bot_interface.api.send_text(
                bot_instance_id=bot_instance_id,
                contact_number=user.phone,
                text=welcome_text,
            )

            return "success" if response and response.get("messages") else "failure"

        except Exception:
            logger.exception("display_community_message failed")
            return "failure"

    def generate_community_menu(self, bot_instance_id, data_dict):
        """
        Generate dynamic menu from user's communities.
        """
        try:
            user_id = data_dict.get("user_id")
            bot_instance, user = _load_user_session(bot_instance_id, user_id)

            # Update session state
            user.expected_response_type = "button"
            user.current_state = data_dict.get("state")
            user.current_smj = bot_interface.models.SMJ.objects.get(
                id=data_dict.get("smj_id")
            )
            user.save()

            # Get user communities from misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get(
                "community_membership", {}
            ).get("current_communities", [])

            if not current_communities:
                return "failure"

            # Try API for last accessed community
            menu_items = []
            success, api_response = (
                bot_interface.utils.check_user_community_status_http(user.phone)
            )

            if success and api_response.get("success"):
                data = api_response.get("data", {})
                last_accessed_id = data.get("misc", {}).get(
                    "last_accessed_community_id"
                )
                api_communities = data.get("data", [])

                menu_items = [
                    {
                        "value": f"community_{c.get('community_id')}",
                        "label": c.get("name", "Unknown Community"),
                        "description": f"Select {c.get('name', 'Unknown Community')}",
                    }
                    for c in api_communities
                    if c.get("community_id") != last_accessed_id
                ]
            else:
                # Fallback: use stored communities
                menu_items = [
                    {
                        "value": f"community_{c.get('community_id')}",
                        "label": c.get("community_name", "Unknown Community"),
                        "description": f"Select {c.get('community_name', 'Unknown Community')}",
                    }
                    for c in current_communities
                ]

            # Add continue option
            menu_items.append(
                {
                    "value": "continue_last_accessed",
                    "label": "पिछला समुदाय चुनें",
                    "description": "अपने पिछले समुदाय के साथ वापस जाएं",
                }
            )

            # Send WhatsApp list
            response = bot_interface.api.send_list_msg(
                bot_instance_id=bot_instance_id,
                contact_number=user.phone,
                text="कृपया अपना समुदाय चुनें:",
                menu_list=menu_items,
                button_label="समुदाय चुनें",
            )

            return "success" if response and response.get("messages") else "failure"

        except Exception:
            logger.exception("generate_community_menu failed")
            return "failure"

    def store_active_community_and_context(self, bot_instance_id, data_dict):
        """
        Store active / selected community and navigation context.
        Handles:
        - single community auto-continue
        - last accessed community
        - menu-based community selection
        """
        try:
            user_id = data_dict.get("user_id")
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)

            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )

            event = data_dict.get("event") or data_dict.get("event_data", {}).get(
                "misc", ""
            )

            event_data = data_dict.get("event_data", {})

            # Explicit navigation events
            if event == "join_new":
                return "join_new"

            if event == "choose_other":
                return "choose_other"

            # Resolve community
            community_id = _resolve_community_id(user, user_id, event, event_data)

            if not community_id:
                return "failure"

            # Store context
            user.misc_data = user.misc_data or {}
            user.misc_data.update(
                {
                    "active_community_id": str(community_id),
                    "navigation_context": (
                        "community_selection"
                        if event_data.get("type") == "button"
                        else "auto_continue"
                    ),
                    "last_service_event": event,
                }
            )

            user.save()

            return "community_selected" if event_data.get("type") == "button" else event

        except Exception as e:
            logger.exception("store_active_community_and_context failed")
            logger.debug(str(e))

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

            user_id = data_dict.get("user_id")
            bot_instance, user = _load_user_session(bot_instance_id, user_id)
            active_community_id = (
                user.misc_data.get("active_community_id") if user.misc_data else None
            )

            if active_community_id:
                # Get BotUsers object to find community name
                bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
                current_communities = bot_user.user_misc.get(
                    "community_membership", {}
                ).get("current_communities", [])

                # Find the active community name
                community_name = "आपके समुदाय"  # Default fallback
                for community in current_communities:
                    if str(community.get("community_id")) == str(active_community_id):
                        community_name = community.get("community_name", community_name)
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
                text=service_text,
            )

            print(f"Service menu message sent: {response}")

            if response and response.get("messages"):
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
            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )

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
        Store location data from WhatsApp location event into work_demand context.
        """
        try:
            user = _get_user_session(bot_instance_id, data_dict.get("user_id"))
            if not user:
                return "failure"

            location_data = _extract_location_data(user, data_dict)

            if not location_data:
                print("No location data found")
                return "failure"
            smj = _get_smj(data_dict.get("smj_id"))

            flow_type = getattr(smj, "name", None)
            user.misc_data = user.misc_data or {}
            user.misc_data.setdefault(flow_type, {})
            user.misc_data[flow_type]["location"] = location_data
            user.save()

            print(f"Stored location data: {location_data}")
            return "success"

        except Exception:
            logger.exception("store_location_data failed")
            return "failure"

    def store_audio_data(self, bot_instance_id, data_dict):
        return self._store_media_data(
            bot_instance_id=bot_instance_id, data_dict=data_dict, media_type="audio"
        )

    def store_photo_data(self, bot_instance_id, data_dict, flow_type="work_demand"):
        return self._store_media_data(
            bot_instance_id=bot_instance_id, data_dict=data_dict, media_type="photo"
        )

    def archive_and_end_session(self, bot_instance_id, data_dict):
        """
        Archive current session and end it completely.
        """
        try:
            user = _get_user_session(bot_instance_id, data_dict.get("user_id"))
            if not user:
                return "failure"

            _archive_user_session(user, reason="work_demand_completion")
            _reset_user_session(user)

            return "success"

        except Exception:
            logger.exception("archive_and_end_session failed")
            return "failure"

    def _extract_flow_data(self, user, flow_type):
        data = user.misc_data.get(flow_type, {}) if user.misc_data else {}

        if "photos" in data:
            data["photos_note"] = (
                "Photo paths are HDPI processed images from WhatsApp media"
            )

        return data

    def _build_community_context(self, user):
        active_community_id = (
            user.misc_data.get("active_community_id") if user.misc_data else None
        )
        if not active_community_id:
            return {}

        try:
            from community_engagement.models import Community

            community = Community.objects.get(id=active_community_id)
            context = {
                "community_id": active_community_id,
                "community_name": (
                    community.project.name if community.project else "Unknown"
                ),
                "organization": (
                    community.project.organization.name
                    if community.project and community.project.organization
                    else "Unknown"
                ),
                "location_hierarchy": {},
            }

            for loc in community.locations.all():
                if loc.state:
                    context["location_hierarchy"]["state"] = loc.state.state_name
                if loc.district:
                    context["location_hierarchy"][
                        "district"
                    ] = loc.district.district_name
                if loc.block:
                    context["location_hierarchy"]["block"] = loc.block.block_name

            return context

        except Exception as e:
            logger.warning(f"Community context load failed: {e}")
            return {"community_id": active_community_id, "error": "load_failed"}

    def _build_community_context(self, user):
        active_community_id = (
            user.misc_data.get("active_community_id") if user.misc_data else None
        )
        if not active_community_id:
            return {}

        try:
            from community_engagement.models import Community

            community = Community.objects.get(id=active_community_id)
            context = {
                "community_id": active_community_id,
                "community_name": (
                    community.project.name if community.project else "Unknown"
                ),
                "organization": (
                    community.project.organization.name
                    if community.project and community.project.organization
                    else "Unknown"
                ),
                "location_hierarchy": {},
            }

            for loc in community.locations.all():
                if loc.state:
                    context["location_hierarchy"]["state"] = loc.state.state_name
                if loc.district:
                    context["location_hierarchy"][
                        "district"
                    ] = loc.district.district_name
                if loc.block:
                    context["location_hierarchy"]["block"] = loc.block.block_name

            return context

        except Exception as e:
            logger.warning(f"Community context load failed: {e}")
            return {"community_id": active_community_id, "error": "load_failed"}

    def _log_flow_completion(self, bot_instance_id, data_dict):
        smj = _get_smj(data_dict.get("smj_id"))
        flow_type = getattr(smj, "name", None)
        logger.info(f"Runnuing for flow type: {flow_type}")
        """
        Generic logger for work_demand / story / grievance flows.
        Creates UserLogs ONLY at the end, reading accumulated data
        from user.misc_data.
        """
        try:
            # ----------------------------
            # Load core objects
            # ----------------------------
            bot_instance = _get_bot_instance(bot_instance_id)
            if not bot_instance:
                return "failure"

            user = _get_user_session(bot_instance, data_dict.get("user_id"))
            print(f"User Session Data: {user.__dict__}")
            if not user:
                return "failure"

            bot_user = _get_bot_user(user.user_id)
            smj = _get_smj(data_dict.get("smj_id"))

            # ----------------------------
            # CRITICAL: Refresh user cache
            # ----------------------------
            user.refresh_from_db(fields=["misc_data"])
            print(user.__dict__)
            # ----------------------------
            # Find correct flow cache
            # (defensive against key mismatch)
            # ----------------------------
            flow_cache = {}
            misc_data = user.misc_data or {}

            if flow_type in misc_data:
                flow_cache = misc_data.get(flow_type, {})
            else:
                # fallback: partial match (debug safety)
                for key, value in misc_data.items():
                    if flow_type in key:
                        flow_cache = value
                        break

            # ----------------------------
            # Extract accumulated media
            # ----------------------------
            audio_data = flow_cache.get("audio")
            photo_data = flow_cache.get("photos")
            community_id = flow_cache.get("community_id")

            # ----------------------------
            # Extract structured flow data
            # ----------------------------
            flow_data = self._extract_flow_data(user, flow_type)

            # ----------------------------
            # Build community context
            # ----------------------------
            community_context = self._build_community_context(user)
            if community_id and isinstance(community_context, dict):
                community_context.setdefault("community_id", community_id)

            # ----------------------------
            # Build misc payload
            # ----------------------------
            misc_payload = _build_misc_payload(
                self,
                flow_type=flow_type,
                flow_data=flow_data,
                community_context=community_context,
                bot_instance=bot_instance,
                user=user,
                bot_user=bot_user,
            )

            misc_payload["audio_data"] = audio_data
            misc_payload["photo_data"] = photo_data

            # ----------------------------
            # Create final UserLog
            # ----------------------------
            bot_interface.models.UserLogs.objects.create(
                app_type=user.app_type,
                bot=bot_instance,
                user=bot_user,
                key1="useraction",
                value1=flow_type,
                misc=misc_payload,
                smj=smj,
            )

            # ----------------------------
            # Cleanup cached flow data
            # ----------------------------
            if flow_type in misc_data:
                misc_data.pop(flow_type, None)
                user.misc_data = misc_data
                user.save(update_fields=["misc_data"])

            return "success"

        except Exception:
            logger.exception(f"log_{flow_type}_completion failed")
            return "failure"

    def log_work_demand_completion(self, bot_instance_id, data_dict):
        print(f"Data Dict for work demand {data_dict}")
        return self._log_flow_completion(
            bot_instance_id=bot_instance_id, data_dict=data_dict
        )

    def log_story_completion(self, bot_instance_id, data_dict):
        return self._log_flow_completion(
            bot_instance_id=bot_instance_id, data_dict=data_dict
        )

    def _extract_community_id_for_join(self, user_session, event_data):
        """
        Extract community_id from session or button event.
        """
        import json

        current_session = user_session.current_session

        try:
            if isinstance(current_session, str):
                current_session = json.loads(current_session or "[]")
        except Exception:
            current_session = []

        # 1️⃣ Prefer session-based selection
        for entry in current_session or []:
            if not isinstance(entry, dict):
                continue

            if "CommunityByStateDistrict" in entry:
                return entry["CommunityByStateDistrict"].get("misc")

            if "CommunityByLocation" in entry:
                return entry["CommunityByLocation"].get("misc")

        # 2️⃣ Fallback: button click
        if event_data.get("type") == "button":
            return event_data.get("misc") or event_data.get("data")

        return None

    def _join_user_to_community(self, user_session, community_id, phone_number):
        """
        Calls CE API to join user to community and stores context locally.
        """
        try:
            response = requests.post(
                url=f"{CE_API_URL}add_user_to_community/",
                data={
                    "community_id": community_id,
                    "number": int(phone_number),
                },
                timeout=30,
            )
            response.raise_for_status()
            api_response = response.json()

            if not api_response.get("success"):
                logger.warning(f"Community join failed: {api_response}")
                return "failure"

            if not user_session.misc_data:
                user_session.misc_data = {}

            user_session.misc_data.update(
                {
                    "active_community_id": community_id,
                    "navigation_context": "join_community",
                    "join_timestamp": timezone.now().isoformat(),
                }
            )
            user_session.save()

            return "success"

        except Exception:
            logger.exception("Community join API failed")
            return "failure"

    def add_user_to_selected_community_join_flow(self, bot_instance_id, data_dict):
        """
        Add user to selected community in join community flow.
        Extracts community ID from:
          1. Session data (CommunityByStateDistrict / CommunityByLocation)
          2. Button event fallback
        """
        try:
            bot_instance = bot_interface.models.Bot.objects.get(id=bot_instance_id)
            user_id = data_dict.get("user_id")

            user_session = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )
            bot_user = user_session.user

            community_id = self._extract_community_id_for_join(
                user_session=user_session,
                event_data=data_dict.get("event_data", {}),
            )

            if not community_id:
                logger.warning("Community ID not found for join flow")
                return "failure"

            return self._join_user_to_community(
                user_session=user_session,
                community_id=community_id,
                phone_number=bot_user.user.contact_number,
            )

        except Exception:
            logger.exception("add_user_to_selected_community_join_flow failed")
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
            user_session = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )
            bot_user = user_session.user

            # Get community name from misc_data
            community_id = (
                user_session.misc_data.get("active_community_id")
                if user_session.misc_data
                else None
            )
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
                text=success_text,
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
                    "states": [],  # Will be loaded from SMJ
                }
            }

            # Add jump data to data_dict for store_active_community_and_context processing
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

        print(f"invoking proces and submit work demand")
        try:
            # Get the UserLogs record
            try:
                user_log = UserLogs.objects.get(id=user_log_id)
            except UserLogs.DoesNotExist:
                print ("user log not found") 
                return {
                    "success": False,
                    "message": f"UserLogs record with id {user_log_id} not found",
                }

            # Extract work demand data from misc field
            work_demand_data = user_log.misc.get("work_demand_data", {})
            if not work_demand_data:
                # Try alternative key structure
                work_demand_data = user_log.misc.get("work_demand", {})
            print(f"workd demand data {work_demand_data}")
            if not work_demand_data:
                return {
                    "success": False,
                    "message": "No work demand data found in UserLogs.misc",
                }

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
                    return {
                        "success": False,
                        "message": "Could not find community_id in UserLogs data",
                    }

            except Exception as e:
                print(f"Error getting community context from UserLogs: {e}")
                return {
                    "success": False,
                    "message": f"Error getting community context: {e}",
                }

            # Prepare files for upload from local filesystem
            files = {}

            # Handle audio file - use "audios" key for API
            if "audio" in work_demand_data:
                audio_path = work_demand_data["audio"]
                if audio_path and os.path.exists(audio_path):
                    try:
                        with open(audio_path, "rb") as audio_file:
                            audio_content = audio_file.read()
                            # Determine file extension
                            file_ext = os.path.splitext(audio_path)[1] or ".ogg"
                            mime_type = (
                                "audio/ogg" if file_ext == ".ogg" else "audio/mpeg"
                            )
                            files["audios"] = (
                                f"audio{file_ext}",
                                audio_content,
                                mime_type,
                            )
                            print(f"Added audio file: {audio_path}")
                    except Exception as e:
                        print(f"Error reading audio file {audio_path}: {e}")
                else:
                    print(f"Audio file not found or invalid path: {audio_path}")

            # Handle photo files - use indexed keys for multiple images
            if "photos" in work_demand_data and isinstance(
                work_demand_data["photos"], list
            ):
                for i, photo_path in enumerate(work_demand_data["photos"]):
                    if photo_path and os.path.exists(photo_path):
                        try:
                            with open(photo_path, "rb") as photo_file:
                                photo_content = photo_file.read()
                                # Determine file extension
                                file_ext = os.path.splitext(photo_path)[1] or ".jpg"
                                mime_type = (
                                    "image/jpeg"
                                    if file_ext.lower() in [".jpg", ".jpeg"]
                                    else "image/png"
                                )
                                files[f"images_{i}"] = (
                                    f"photo_{i}{file_ext}",
                                    photo_content,
                                    mime_type,
                                )
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
                        "lon": location.get("longitude"),
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
                    return {
                        "success": False,
                        "message": "Could not get user contact number",
                    }

            except AttributeError as e:
                return {
                    "success": False,
                    "message": f"Could not get user contact number from relationship chain: {e}",
                }

            # Prepare API payload
            payload = {
                "item_type": 'Asset_Demand',
                "coordinates": json.dumps(coordinates) if coordinates else "",
                "number": contact_number,
                "community_id": community_id,
                "source": "BOT",
                "bot_id": user_log.bot.id,
                "title": f"Asset_Demand",  # Auto-generated if not provided
                "transcript": work_demand_data.get(
                    "description", ""
                ),  # If any description exists
            }

            print(f"API Payload: {payload}")
            print(f"Files to upload: {list(files.keys())}")

            # Submit to Community Engagement API
            api_url = f"{CE_API_URL}upsert_item/"

            try:
                response = requests.post(
                    api_url, data=payload, files=files, timeout=30  # 30 second timeout
                )

                print(f"API Response Status: {response.status_code}")
                print(f"API Response: {response.text}")

                if response.status_code == 200 or response.status_code == 201:
                    result = response.json()
                    if result.get("success"):
                        print(
                            f"Successfully submitted work demand. Item ID: {result.get('item_id')}"
                        )

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
                        print(
                            f"Updated UserLogs ID {user_log.id} with API failure status"
                        )

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
                        "message": f"API call failed with status {response.status_code}: {response.text}",
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

    def process_and_submit_story(self, user_log_id):
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

        print(f"invoking proces and submit story")
        try:
            # Get the UserLogs record
            try:
                user_log = UserLogs.objects.get(id=user_log_id)
            except UserLogs.DoesNotExist:
                return {
                    "success": False,
                    "message": f"UserLogs record with id {user_log_id} not found",
                }

            # Extract work demand data from misc field
            story_data = user_log.misc.get("story_data", {})
            if not story_data:
                # Try alternative key structure
                work_demand_data = user_log.misc.get("story", {})
            print(f"story data {story_data}")
            if not story_data:
                return {
                    "success": False,
                    "message": "No work demand data found in UserLogs.misc",
                }

            print(f"Processing work demand data: {story_data}")

            # Get user's community context from UserLogs misc data
            community_id = None
            try:
                # Get community_id from community_context in the UserLogs misc field
                if "community_context" in user_log.misc:
                    community_context = user_log.misc["community_context"]
                    community_id = community_context.get("community_id")
                    print(f"Found community_id in UserLogs: {community_id}")

                if not community_id:
                    return {
                        "success": False,
                        "message": "Could not find community_id in UserLogs data",
                    }

            except Exception as e:
                print(f"Error getting community context from UserLogs: {e}")
                return {
                    "success": False,
                    "message": f"Error getting community context: {e}",
                }

            # Prepare files for upload from local filesystem
            files = {}

            # Handle audio file - use "audios" key for API
            if "audio" in story_data:
                audio_path = story_data["audio"]
                if audio_path and os.path.exists(audio_path):
                    try:
                        with open(audio_path, "rb") as audio_file:
                            audio_content = audio_file.read()
                            # Determine file extension
                            file_ext = os.path.splitext(audio_path)[1] or ".ogg"
                            mime_type = (
                                "audio/ogg" if file_ext == ".ogg" else "audio/mpeg"
                            )
                            files["audios"] = (
                                f"audio{file_ext}",
                                audio_content,
                                mime_type,
                            )
                            print(f"Added audio file: {audio_path}")
                    except Exception as e:
                        print(f"Error reading audio file {audio_path}: {e}")
                else:
                    print(f"Audio file not found or invalid path: {audio_path}")

            # Handle photo files - use indexed keys for multiple images
            if "photos" in story_data and isinstance(story_data["photos"], list):
                for i, photo_path in enumerate(story_data["photos"]):
                    if photo_path and os.path.exists(photo_path):
                        try:
                            with open(photo_path, "rb") as photo_file:
                                photo_content = photo_file.read()
                                # Determine file extension
                                file_ext = os.path.splitext(photo_path)[1] or ".jpg"
                                mime_type = (
                                    "image/jpeg"
                                    if file_ext.lower() in [".jpg", ".jpeg"]
                                    else "image/png"
                                )
                                files[f"images_{i}"] = (
                                    f"photo_{i}{file_ext}",
                                    photo_content,
                                    mime_type,
                                )
                                print(f"Added photo file {i}: {photo_path}")
                        except Exception as e:
                            print(f"Error reading photo file {photo_path}: {e}")
                    else:
                        print(f"Photo file not found or invalid path: {photo_path}")

            # Prepare coordinates from location data - use lat/lon format
            coordinates = {}
            if "location" in story_data:
                location = story_data["location"]
                if isinstance(location, dict):
                    coordinates = {
                        "lat": location.get("latitude"),
                        "lon": location.get("longitude"),
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
                    return {
                        "success": False,
                        "message": "Could not get user contact number",
                    }

            except AttributeError as e:
                return {
                    "success": False,
                    "message": f"Could not get user contact number from relationship chain: {e}",
                }

            # Prepare API payload
            payload = {
                "item_type": "Story",
                "coordinates": json.dumps(coordinates) if coordinates else "",
                "number": contact_number,
                "community_id": community_id,
                "source": "BOT",
                "bot_id": user_log.bot.id,
                "title": "Story",  # Auto-generated if not provided
                "transcript": story_data.get(
                    "description", ""
                ),  # If any description exists
            }

            print(f"API Payload: {payload}")
            print(f"Files to upload: {list(files.keys())}")

            # Submit to Community Engagement API
            api_url = f"{CE_API_URL}upsert_item/"

            try:
                response = requests.post(
                    api_url, data=payload, files=files, timeout=30  # 30 second timeout
                )

                print(f"API Response Status: {response.status_code}")
                print(f"API Response: {response.text}")

                if response.status_code == 200 or response.status_code == 201:
                    result = response.json()
                    if result.get("success"):
                        print(
                            f"Successfully submitted work demand. Item ID: {result.get('item_id')}"
                        )

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
                        print(
                            f"Updated UserLogs ID {user_log.id} with API failure status"
                        )

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
                        "message": f"API call failed with status {response.status_code}: {response.text}",
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
        print(
            f"Fetching work demand status for bot_instance_id: {bot_instance_id} and data_dict: {data_dict}"
        )
        try:
            import requests
            from django.conf import settings
            from bot_interface.models import BotUsers
            from community_engagement.models import Community_user_mapping

            # Get user information
            user_id = data_dict.get("user_id")
            bot_id = data_dict.get("bot_id", 1)

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
                    user=bot_user.user, is_last_accessed_community=True
                ).first()

                if not community_mapping:
                    print(f"No active community found for user {contact_number}")
                    return "failure"

                community_id = community_mapping.community.id
            except Exception as e:
                print(f"Error getting community for user {contact_number}: {e}")
                return "failure"

            # Call Community Engagement API
            api_url = f"{CE_API_URL}get_items_status/"
            params = {
                "number": contact_number,
                "bot_id": bot_instance_id,
                # 'community_id': str(community_id),
                "work_demand_only": "true",
            }

            print(
                f"Fetching work demand status for user {contact_number} in community {community_id} and bot_id {bot_instance_id}"
            )
            response = requests.get(api_url, params=params, timeout=30)
            print("response from GET request of get_items_status/ :", response)

            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    work_demands = result.get("data", [])
                    print(
                        f"Found {len(work_demands)} work demands for user {contact_number}"
                    )

                    # Store work demands in user session for persistence between states
                    try:
                        from bot_interface.models import UserSessions

                        session_data = {
                            "work_demands": work_demands,
                            "community_id": community_id,
                        }
                        UserSessions.objects.filter(user_id=user_id).update(
                            misc_data=session_data
                        )
                        print(
                            f"Stored {len(work_demands)} work demands in session for user {user_id}"
                        )
                    except Exception as session_error:
                        print(f"Error storing work demands in session: {session_error}")

                    if work_demands:
                        return "has_work_demands"
                    else:
                        return "no_work_demands"
                else:
                    print(
                        f"API returned error: {result.get('message', 'Unknown error')}"
                    )
                    return "failure"
            else:
                print(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                return "failure"

        except Exception as e:
            print(f"Error in fetch_work_demand_status: {e}")
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
            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )

            # Get BotUsers object to access user_misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)
            current_communities = bot_user.user_misc.get(
                "community_membership", {}
            ).get("current_communities", [])

            if len(current_communities) > 0:
                # Get fresh community data with last accessed info
                success, api_response = (
                    bot_interface.utils.check_user_community_status_http(user.phone)
                )
                if success and api_response.get("success"):
                    community_data = api_response.get("data", {})
                    last_accessed_id = community_data.get("misc", {}).get(
                        "last_accessed_community_id"
                    )

                    # Find the last accessed community name
                    communities_list = community_data.get("data", [])
                    last_community_name = "Unknown Community"
                    for community in communities_list:
                        if community.get("community_id") == last_accessed_id:
                            last_community_name = community.get(
                                "name", "Unknown Community"
                            )
                            break
                else:
                    # Fallback to first community
                    last_community_name = current_communities[0].get(
                        "community_name", "Unknown Community"
                    )

                # Create welcome message
                welcome_text = (
                    f"🏠 आपने पिछली बार {last_community_name} समुदाय का उपयोग किया था।"
                )

                # Send text message
                response = bot_interface.api.send_text(
                    bot_instance_id=bot_instance_id,
                    contact_number=user.phone,
                    text=welcome_text,
                )

                print(f"Multiple community welcome message sent: {response}")

                if response and response.get("messages"):
                    return "success"
                else:
                    return "failure"
            else:
                print("No communities found for user")
                return "failure"

        except Exception as e:
            print(f"Error in display_multiple_community_message: {e}")
            return "failure"

    def display_single_community_message(self, bot_instance_id, data_dict):
        """
        Display welcome message for users with a single community.
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
            user = bot_interface.models.UserSessions.objects.get(
                user=user_id, bot=bot_instance
            )

            # Get BotUsers object to access user_misc
            bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)

            current_communities = bot_user.user_misc.get(
                "community_membership", {}
            ).get("current_communities", [])

            # Single community expected
            if len(current_communities) != 1:
                print("User does not have exactly one community")
                return "failure"

            community_name = current_communities[0].get("community_name", "आपका समुदाय")

            # Try to refresh from API (optional but consistent)
            success, api_response = (
                bot_interface.utils.check_user_community_status_http(user.phone)
            )

            if success and api_response.get("success"):
                community_data = api_response.get("data", {})
                communities_list = community_data.get("data", [])

                # Prefer authoritative API name
                if communities_list:
                    community_name = communities_list[0].get("name", community_name)

            # Create welcome message
            welcome_text = f"🏠 आप {community_name} समुदाय से जुड़े हुए हैं।"

            # Send text message
            response = bot_interface.api.send_text(
                bot_instance_id=bot_instance_id,
                contact_number=user.phone,
                text=welcome_text,
            )

            print(f"Single community welcome message sent: {response}")

            if response and response.get("messages"):
                return "success"

            return "failure"

        except Exception as e:
            print(f"Error in display_single_community_message: {e}")
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
            user_id = data_dict.get("user_id")
            bot_id = data_dict.get("bot_id", 1)

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
                work_demands = session_data.get("work_demands", [])

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
                success = self._send_work_demands_with_limit(
                    work_demands, contact_number, bot_instance_id
                )
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

    def _send_work_demands_with_limit(
        self, work_demands, contact_number, bot_instance_id, max_length=4000
    ):
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
            available_space = (
                max_length - len(header) - 50
            )  # 50 chars buffer for part indicator
            entries_per_message = max(1, available_space // entry_length)

            total_messages = (
                len(work_demands) + entries_per_message - 1
            ) // entries_per_message

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
                    demand_id = demand.get("id", "N/A")
                    title = demand.get("title", "Asset Demand Request")
                    status = demand.get("status", "UNMODERATED")
                    transcription = demand.get("transcription", "")

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
                    text=text,
                )

                if not response or not response.get("messages"):
                    print(f"Failed to send message part {msg_num + 1}/{total_messages}")
                    return False

                print(f"Sent message part {msg_num + 1}/{total_messages} successfully")

            return True

        except Exception as e:
            print(f"Error in _send_work_demands_with_limit: {e}")
            return False
