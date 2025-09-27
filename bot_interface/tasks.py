import bot_interface.models
import bot_interface.statemachine
import bot_interface.utils

from nrm_app.celery import app
from typing import Dict, Any, Tuple
from django.core.exceptions import ObjectDoesNotExist
import requests
import json
import logging

logger = logging.getLogger(__name__)

@app.task(bind=True, name="StartUserSession")
def StartUserSession(self, event_packet: Dict[str, Any], event: str, bot_id: str, app_type: str) -> None:
    """
    Start a user session for WhatsApp bot interactions.
    
    Args:
        event_packet: Event data from WhatsApp webhook
        event: Event type
        bot_id: ID of the bot instance
        app_type: Type of the application
    """
    # logger.info(f"Starting user session using {method} method")
    # print(f"Starting user session using {method} method")
    # logger.info(f"Event packet: {event_packet}")
    print(f"Event packet: {event_packet}")
    try:
        bot_instance = bot_interface.models.Bot.objects.get(id=bot_id)  # type: ignore
    except ObjectDoesNotExist:
        logger.error("Bot instance with ID %s not found", bot_id)
        return
    
    # Check if the bot_number is available in the event packet
    bot_number = bot_instance.bot_number
    if not bot_number:
        logger.error("Bot number not found in event packet")
        return
    user_number = event_packet.get("user_number")
    print(f"User number from event packet: {user_number}")
    
    if not user_number:
        logger.error("User number not found in event packet")
        return

    
    # assert False  # For debugging purposes, remove in production
    # Check if the user_number is a user in the community
    try:
        import time
        start_time = time.time()
        success, response_json = bot_interface.utils.check_user_community_status_http(user_number)
        print("is_in_community", success, response_json)
        end_time = time.time()
        duration = end_time - start_time
        logger.info("Community check (HTTP) took %.3f seconds", duration)
        
        if success and response_json.get("success"):
            response_data = response_json.get("data", {})
            is_in_community = response_data.get("is_in_community", False)

            logger.info("User %s community status: %s", user_number, is_in_community)
            
            if is_in_community:
                logger.info("User %s is in community, proceeding with session", user_number)
                
                # Check and create user, ensure BotUser entry exists for community members too
                bot_user_id, is_new_user = bot_interface.utils.check_and_create_user(user_number, bot_instance)
                print("event_packet >>>", event_packet)
                # For button presses, use the button data as event, for location use "success", otherwise use "community_member"
                if event_packet.get("type") == "button":
                    current_event = event_packet.get("data")
                # elif event_packet.get("type") == "location":
                #     current_event = "success"

                else:
                    current_event = "community_member"
                current_state = event_packet.get("state") or bot_instance.init_state  # Preserve current state
                current_smj_id = event_packet.get("smj_id") or bot_instance.smj.id  # Preserve SMJ context
                
                print(f"Community user button event - Event: {current_event}, State: {current_state}, SMJ: {current_smj_id}")
                
                # User is in community, handle appropriately based on event type
                event_packet.update({
                    "event": current_event,  # Use button data for button presses
                    "smj_id": current_smj_id,  # Preserve SMJ context
                    "state": current_state,  # Preserve current state, don't reset to init
                    "user_id": bot_user_id,
                    "is_new_user": is_new_user
                })
                _load_or_create_user_session(event_packet, bot_instance, response_data)
            else:
                logger.info("User %s is not in community, handling onboarding", user_number)

                # Check and create user, ensure BotUser entry exists
                bot_user_id, is_new_user = bot_interface.utils.check_and_create_user(user_number, bot_instance)
                print(f"User check result - ID: {bot_user_id}, is_new_user: {is_new_user}")

                # Update event packet with user information
                event_packet.update({
                    "user_id": bot_user_id,
                    "is_new_user": is_new_user
                })
                print("Event packet after user check:", event_packet)
                
                # Handle UserSessions based on is_new_user flag
                start_session = False
                event_type = False

                current_event_packet = {}
                current_session = []  # Initialize current_session to avoid UnboundLocalError
                if is_new_user:
                    # Create new UserSessions entry for new user
                    try:
                        user_session = bot_interface.models.UserSessions.objects.create(
                            bot=bot_instance,
                            user_id=bot_user_id,
                            phone=event_packet.get("wa_id", user_number),
                            app_type=app_type
                        )
                        print(f"Created new UserSession for new user: {user_session}")
                        # Set onboarding event for parent SMJ to route to onboarding flow
                        event_packet.update({
                            "event": "onboarding",
                            "smj_id": bot_instance.smj.id,  # Start with parent SMJ
                            "state": bot_instance.init_state
                        })
                        start_session = True
                    except Exception as e:
                        logger.error("Error creating UserSession for new user: %s", str(e))
                        return
                else:
                    # Check if UserSessions exists for existing user
                    try:
                        user_session = bot_interface.models.UserSessions.objects.get(user_id=bot_user_id)
                        print(f"Found existing UserSession: {user_session}")
                        
                        # Check if current_session is empty - but preserve SMJ context if available
                        if len(user_session.current_session) == 0:
                            print("UserSession current_session is empty, updating event_packet")
                            
                            # Preserve current SMJ context if available, otherwise use bot instance defaults
                            current_smj_id = user_session.current_smj.id if user_session.current_smj else bot_instance.smj.id
                            current_state = user_session.current_state if user_session.current_state else bot_instance.init_state
                            
                            # Only set event to "start" if this is truly a new conversation
                            # For button/interactive events, preserve the original event type
                            if event_packet.get("type") in ["button", "interactive", "location", "audio", "voice"] and event_packet.get("event"):
                                # This is an existing interaction, don't force start
                                event_to_set = event_packet.get("event")
                                print(f"Preserving existing interaction event: {event_to_set}")
                            else:
                                # This is a new conversation start - set onboarding event for parent SMJ
                                event_to_set = "onboarding"
                                start_session = True
                                event_type = True
                                print("Setting event to onboarding for new conversation")
                            
                            # Update event packet for existing user with empty session
                            event_packet.update({
                                "event": event_to_set,
                                "smj_id": current_smj_id,  # Preserve current SMJ instead of resetting
                                "state": current_state     # Preserve current state instead of resetting
                            })
                            print(f"Preserved SMJ context - SMJ: {current_smj_id}, State: {current_state}")
                            logger.info("event packet when UserSessions already exists and is empty: %s", event_packet)
                            current_event_packet["InitState"] = event_packet
                            current_session = user_session.current_session or []
                            current_session.append(current_event_packet)
                            print("="*50)
                            print("UserSession current_session was empty, updated with:", current_session)
                            print("="*50)
                            user_session.current_session = current_session
                        else:
                            print(f"UserSession current_session is not empty: {len(user_session.current_session)} items : {user_session.current_session}")
                            print("UserSession current_session is not empty: event_packet: ", event_packet)
                            
                            # Preserve current SMJ context instead of reverting to bot instance SMJ
                            current_smj_id = user_session.current_smj.id if user_session.current_smj else bot_instance.smj.id
                            current_state = user_session.current_state if user_session.current_state else bot_instance.init_state
                            
                            logger.info("Event passed in check_event_type for existing user: %s", event_packet.get("event"), event)
                            # **VALIDATE ALL RESPONSE TYPES - Return failure to re-run current state**
                            if not bot_interface.utils.check_event_type(
                                event_packet=event_packet,
                                expected_response_type=user_session.expected_response_type,
                                user_session=user_session
                            ):
                                logger.info("Invalid response type, sending failure event to re-run state")
                                # Set failure event to re-run current state - DON'T return, continue processing
                                event_packet.update({
                                    "event": "failure",  # This will trigger state re-execution
                                    "smj_id": current_smj_id,
                                    "state": current_state
                                })
                                # Note: We continue processing with failure event instead of returning
                            
                            # Only update event if validation passed
                            if event_packet.get("event") != "failure":
                                updated_event = event if event_packet.get("event") else "success"                                
                                event_packet.update({
                                    "event": updated_event,
                                    "smj_id": current_smj_id,  # Preserve current SMJ instead of resetting
                                    "state": current_state  # Preserve current state instead of resetting
                                })
                            print("Updated event packet for existing usersession:", event_packet)
                            print("Current state in existing session:", user_session.current_state)
                            current_session = user_session.current_session
                            print("="*50)
                            print("Current session in existing session:", current_session)
                            print("="*50)
                            # current_session[-1].update({event_state: event_packet}) if current_session else current_session.append({event_state: event_packet})
                            # Always append a new dictionary for each interaction
                            current_session.append({current_state: event_packet})
                            print("="*50)
                            # Append location or audio data to session if present
                            event_type = event_packet.get("type")
                            if event_type == "location":
                                location_key = f"{current_state}_location"
                                location_data = {
                                    "data": event_packet.get("data", ""),
                                    "misc": event_packet.get("misc", {}),
                                    "latitude": event_packet.get("misc", {}).get("latitude", ""),
                                    "longitude": event_packet.get("misc", {}).get("longitude", ""),
                                    "address": event_packet.get("misc", {}).get("address", ""),
                                    "name": event_packet.get("misc", {}).get("name", "")
                                }
                                current_session[-1][location_key] = location_data
                                print(f"Appended location data to session: {location_key} = {location_data}")
                            elif event_type in ["audio", "voice"]:
                                audio_key = f"{current_state}_audio"
                                audio_data = {
                                    "data": event_packet.get("data", ""),
                                    "media_id": event_packet.get("media_id", ""),
                                    "file_path": event_packet.get("data", "")
                                }
                                current_session[-1][audio_key] = audio_data
                                print(f"Appended audio data to session: {audio_key} = {audio_data}")
                            
                            # Save updated session
                            user_session.current_session = current_session
                            user_session.save()
                            print("="*50)
                            print(f"Updated session saved for user: {user_session}")
                            print("="*50)
                            # assert False
                    except bot_interface.models.UserSessions.DoesNotExist:
                        logger.info("No existing UserSession found for user, creating new one")
                        try:
                            user_session = bot_interface.models.UserSessions.objects.create(
                                bot=bot_instance,
                                user_id=bot_user_id,
                                phone=event_packet.get("wa_id", user_number),
                                app_type=app_type
                            )
                            print(f"Created new UserSession for existing user: {user_session}")
                            event_packet.update({
                                "event": "onboarding",  # Set onboarding event for parent SMJ
                                "smj_id": bot_instance.smj.id,  # type: ignore
                                "state": bot_instance.init_state
                            })
                            start_session = True
                            event_type = True
                            logger.info("event packet after UserSessions creation: %s", event_packet)
                        except Exception as e:
                            logger.error("Error creating UserSession for existing user tasks.py line 135: %s", str(e))
                            return
                
                print(f"Session flags - start_session: {start_session}, event_type: {event_type}")
                logger.info("Current event packet after checking start session and event type flags: %s", event_packet)  # Debugging current session content
                print("="*50)
                print("Current session content:", user_session.current_session)  # Debugging current session content
                print("="*50)
                # Use event packet SMJ ID if available, otherwise use bot instance SMJ
                event_smj_id = event_packet.get("smj_id", bot_instance.smj.id)
                event_state = event_packet.get("state", bot_instance.init_state)
                
                print(f"Event packet SMJ ID: {event_smj_id}, State: {event_state}")
                print(f"Bot instance SMJ ID: {bot_instance.smj.id}, Init state: {bot_instance.init_state}")
                
                smj_id = event_smj_id  # Use event packet SMJ ID instead of bot instance
                try:
                    smj = bot_interface.models.SMJ.objects.get(id=smj_id)  # Use .get() to get single object
                    smj_states = smj.smj_json
                    # Handle Django JSONField - can be string or already parsed
                    if isinstance(smj_states, str):
                        smj_states = json.loads(smj_states)
                except json.JSONDecodeError as json_err:
                    logger.error("Error parsing SMJ JSON tasks.py line 148: %s", json_err)
                    return
                except ObjectDoesNotExist as obj_err:
                    logger.error("SMJ object not found tasks.py line 152: %s", obj_err)
                    return
                print("SMJ states loaded:", smj_states)
                
                # Enhanced context preservation: Update user session with current SMJ and state
                try:
                    current_smj_obj = bot_interface.models.SMJ.objects.get(id=smj_id)
                    user_session.current_smj = current_smj_obj
                    user_session.current_state = event_state
                    print(f"Updated user session context: SMJ ID {smj_id}, State: {event_state}")
                except bot_interface.models.SMJ.DoesNotExist:
                    print(f"Warning: SMJ with ID {smj_id} not found for context preservation")
                    
                # current_event_packet["InitState"] = event_packet
                # current_session.append(current_event_packet)
                user_session.current_session = current_session
                user_session.save()
                print("="*50)
                print("User session updated with current session:", user_session.current_session)
                print("="*50)

                print(f"Creating SmjController with smj_id: {smj_id}, app_type: {bot_instance.app_type}, user_id: {bot_user_id}, language: {bot_instance.language}, state: {event_state}")
                smj_controller = bot_interface.statemachine.SmjController(
                    states = smj_states,  # Use the extracted states list
                    smj_id = smj_id,  # Use event packet SMJ ID
                    app_type = bot_instance.app_type,
                    bot_id = bot_instance.id,  # type: ignore
                    user_id = bot_user_id,
                    language = bot_instance.language,
                    current_state = event_state,  # Use event packet state instead of bot init state
                    current_session = user_session.current_session if hasattr(user_session, 'current_session') else None
                )
                logger.info("SmjController created with parameters: %s", {
                    "states": smj_states,
                    "smj_id": smj_id,
                    "app_type": bot_instance.app_type,
                    "bot_id": bot_instance.id,
                    "user_id": bot_user_id,
                    "language": bot_instance.language,
                    "current_state": bot_instance.init_state,
                    "current_session": current_session
                })
                smj_controller.runSmj(event_packet)

                print('ENDTIME:', time.time())
                # assert False
                # Continue with onboarding flow if needed
                # if start_session or event_type:
                #     # User is not in community, handle onboarding flow
                #     smj_id = bot_instance.smj.id  # type: ignore
                #     try:
                #         smj = bot_interface.models.SMJ.objects.get(id=smj_id)  # Use .get() to get single object
                #         smj_states = smj.smj_json
                #         # Handle Django JSONField - can be string or already parsed
                #         if isinstance(smj_states, str):
                #             smj_states = json.loads(smj_states)
                #         _handle_user_onboarding(event_packet, bot_instance, smj_states)
                #     except json.JSONDecodeError as json_err:
                #         logger.error("Error parsing SMJ JSON: %s", json_err)
                #         return
                #     except ObjectDoesNotExist as obj_err:
                #         logger.error("SMJ object not found: %s", obj_err)
                #         return
        else:
            logger.error("Community check failed: %s", response_json)
            return
            
    except (ObjectDoesNotExist, AttributeError, ValueError) as community_error:
        logger.error("Error checking user community status: %s", str(community_error))
        return


def _handle_user_onboarding(event_packet: Dict[str, Any], bot_instance, smj_states: Dict[str, Any]) -> None:
    """Handle onboarding flow for users not in community."""
    try:
        # Get user information from event packet
        user_id = event_packet.get("user_id", "")
        is_new_user = event_packet.get("is_new_user", True)
        
        print("Handling user onboarding for user:", {
            "user_id": user_id,
            "is_new_user": is_new_user,
            "bot_instance": str(bot_instance),
            "smj_states_count": len(smj_states) if smj_states else 0
        })
        
        # Update event packet with bot and SMJ information
        event_packet.update({
            "event": "start",
            "is_new_user": is_new_user,
            "init_state": bot_instance.init_state,
            "state": bot_instance.init_state,
            "smj_id": bot_instance.smj.id,  # type: ignore
            "bot_id": bot_instance.id  # type: ignore
        })

        # Get fresh SMJ data
        smj = bot_interface.models.SMJ.objects.get(id=bot_instance.smj.id)  # type: ignore
        smj_states = smj.smj_json
        # Handle Django JSONField - can be string or already parsed
        if isinstance(smj_states, str):
            smj_states = json.loads(smj_states)
        
        # Extract states list from SMJ JSON structure
        # SMJ JSON might be a dict with a 'states' key, or directly a list
        # if isinstance(smj_states, dict):
        #     if 'states' in smj_states:
        #         states_list = smj_states['states']
        #         print(f"Extracted states from SMJ dict: {len(states_list)} states")
        #     else:
        #         # If no 'states' key, assume the whole dict is the state definition
        #         states_list = [smj_states]
        #         print("SMJ is a single state dict, wrapping in list")
        # elif isinstance(smj_states, list):
        #     states_list = smj_states
        #     print(f"SMJ is already a list: {len(states_list)} states")
        # else:
        #     logger.error("SMJ states is neither dict nor list: %s", type(smj_states))
        #     return

        print("Updated event packet for onboarding:", event_packet)

        # Initialize state machine controller
        current_session = None 
        smjController = bot_interface.statemachine.SmjController(
            smj_states,  # Use the extracted states list
            event_packet['smj_id'],
            bot_instance.app_type,
            bot_instance.id,  # type: ignore
            user_id,
            bot_instance.language, 
            event_packet['state'],
            current_session
        )

        print("SmjController created for user:", {
            "smj_id": event_packet['smj_id'],
            "app_type": bot_instance.app_type,
            "user_id": user_id,
            "language": bot_instance.language,
            "state": event_packet['state']
            # "states_count": len(states_list) if isinstance(states_list, list) else "not a list"
        })
        
        # Run state machine
        print("Running SMJ with event packet:", event_packet)
        smjController.runSmj(event_packet)
        
        # Remove assert for production use
        # assert False

    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.error("Error in _handle_user_onboarding: %s", str(e))
        return

def _load_or_create_user_session(event_packet, bot_instance, response_data):
    """Create user session for community users and run state machine."""
    user_id = event_packet.get("user_id")
    if not user_id:
        logger.error("User ID not found in event packet")
        return
    #fetch botuser by id (user_id in event_packet is actually bot_user.id)
    try:
        bot_user = bot_interface.models.BotUsers.objects.get(id=user_id)  # type: ignore
        user_id = str(bot_user.id)
    except bot_interface.models.BotUsers.DoesNotExist:
        logger.error("BotUser entry not found for user ID %s", user_id)
        return
    
    # Load existing user session or create a new one
    user_session, created = bot_interface.models.UserSessions.objects.get_or_create(
        user=bot_user,
        bot=bot_instance,
        defaults={
            "app_type": "WA",
            "phone": bot_user.user.contact_number,
            "current_state": event_packet.get("state", ""),
            "user_config": {"language": event_packet.get("language", "hi")},
        }
    )

    if created:
        logger.info("Created new user session for user_id: %s", user_id)
    else:
        logger.info("Loaded existing user session for user_id: %s", user_id)
        if len(user_session.current_session) > 0:
            logger.info("event passed in check_event_type for community user: %s", event_packet.get("event"))
            if not bot_interface.utils.check_event_type(
                event_packet=event_packet,
                expected_response_type=user_session.expected_response_type,
                user_session=user_session
            ):
                logger.info("Invalid response type for community user, sending failure event to re-run state")
                # Set failure event to re-run current state
                event_packet.update({
                    "event": "failure",  # This will trigger state re-execution
                    "smj_id": user_session.current_smj.id if user_session.current_smj else bot_instance.smj.id,
                    "state": user_session.current_state if user_session.current_state else bot_instance.init_state
                })

    # Get SMJ information
    smj_id = event_packet.get("smj_id", bot_instance.smj.id)
    event_state = event_packet.get("state", bot_instance.init_state)
    
    print(f"Community user SMJ execution - SMJ ID: {smj_id}, State: {event_state}")
    
    # Load SMJ states and run state machine (similar to non-community user flow)
    try:
        smj = bot_interface.models.SMJ.objects.get(id=smj_id)
        smj_states = smj.smj_json
        # Handle Django JSONField - can be string or already parsed
        if isinstance(smj_states, str):
            smj_states = json.loads(smj_states)
    except json.JSONDecodeError as json_err:
        logger.error("Error parsing SMJ JSON for community user: %s", json_err)
        return
    except bot_interface.models.SMJ.DoesNotExist as obj_err:
        logger.error("SMJ object not found for community user: %s", obj_err)
        return
    
    print("SMJ states loaded for community user:", len(smj_states) if isinstance(smj_states, list) else "not a list")
    
    # Update user session context
    # current_event_packet = {}
    current_session = user_session.current_session or []
    try:
        current_smj_obj = bot_interface.models.SMJ.objects.get(id=smj_id)
        user_session.current_smj = current_smj_obj
        user_session.current_state = event_state
        # Always append a new dictionary for each interaction
        current_session.append({event_state: event_packet})
        # current_event_packet["InitState"] = event_packet
        # current_session.append(current_event_packet)
        user_session.current_session = current_session
        user_session.save()
        print("\=/"*50)
        print(user_session.current_session,user_session.current_state,user_session.current_smj)
        print("\=/"*50)
        print(f"Updated community user session context: SMJ ID {smj_id}, State: {event_state}")
    except bot_interface.models.SMJ.DoesNotExist:
        print(f"Warning: SMJ with ID {smj_id} not found for community user context preservation")
    
    # Create and run SmjController
    print(f"Creating SmjController for community user - smj_id: {smj_id}, user_id: {user_id}, state: {event_state}")
    print("="*50)
    print("current_session in community user:", user_session, user_session.current_session)
    print("="*50)
    smj_controller = bot_interface.statemachine.SmjController(
        states=smj_states,
        smj_id=smj_id,
        app_type=bot_instance.app_type,
        bot_id=bot_instance.id,  # type: ignore
        user_id=user_id,
        language=bot_instance.language,
        current_state=event_state,
        current_session=user_session.current_session if hasattr(user_session, 'current_session') else None
    )
    print("="*50)
    print("current_session in community user:", user_session.current_session)
    print("="*50)
    print("Running SMJ for community user with event packet:", event_packet)
    smj_controller.runSmj(event_packet)
    
    return user_session