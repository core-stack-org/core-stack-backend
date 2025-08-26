import json
from datetime import datetime

class StateMapData(object):

    smj_id = ""
    app_type = ""
    bot_id = ""
    user_id = ""
    states = []
    current_state = ""
    language = ""
    current_session = ""

    def __init__(self, states, smj_id, app_type, bot_id, user_id, language, current_state, current_session):
        self.states = states
        self.smj_id = smj_id
        self.app_type = app_type
        self.bot_id = bot_id
        self.user_id = user_id
        self.language = language
        self.current_state = current_state
        self.current_session = current_session

    def jumpToSmj(self, states, smj_id, initState):
        """
        Original jumpToSmj method - simplified for compatibility.
        
        Args:
            states: Current states (unused - kept for compatibility)
            smj_id: SMJ ID
            initState: Initial state
        
        Returns:
            str: The initial state or "ERROR"
        """
        # Suppress unused argument warning - states parameter kept for compatibility
        _ = states
        
        try:
            self.setSmjId(smj_id)
            return initState
        except Exception as e:
            print(f"ERROR in jumpToSmj: {e}")
            return "ERROR"

    def jumpTodefaultSmj(self, states, smj_id, initState):
        """
        Jump to default SMJ - placeholder implementation.
        
        Args:
            states: Current states (unused - kept for compatibility)
            smj_id: SMJ ID
            initState: Initial state
        
        Returns:
            str: The initial state or "ERROR"
        """
        # Suppress unused argument warning - states parameter kept for compatibility
        _ = states
        
        try:
            self.setSmjId(smj_id)
            return initState
        except Exception as e:
            print(f"ERROR in jumpTodefaultSmj: {e}")
            return "ERROR"
    
    def setSmjId(self, smj_id):
        self.smj_id = smj_id

    def getSmjId(self):
        return self.smj_id

    def setCurrentState(self, current_state):
        self.current_state = current_state

    def getCurrentState(self):
        return self.current_state

    def getStates(self):
        return self.states

    def setStates(self, states):
        self.states = states

    def findStateinMap(self, state):
        for state_map_state in self.getStates():
            if state_map_state.get('name') == state:
                self.setCurrentState(state_map_state.get('name'))
                return True
        return False

    def ifStateExists(self, state):
        getstate = self.findStateinMap(state)
        return getstate

    def getpreActionList(self, current_state):
        pre_action_list = []
        for state_map_state in self.getStates():
            if state_map_state.get('name') == current_state:
                pre_action_list = state_map_state.get('preAction')
                break
        return pre_action_list

    
    def getpostActionList(self, current_state):
        post_action_list = []
        for state_map_state in self.getStates():
            if state_map_state.get('name') == current_state:
                post_action_list = state_map_state.get('postAction')
                break
        return post_action_list

    def findTransitiondict(self, current_state):
        transition_dict = {}
        for state_map_state in self.getStates():
            if state_map_state.get('name') == current_state:
                transition_dict = state_map_state.get('transition')
                break
        return transition_dict    

    def preAction(self):
        # print("STARTED PRE ACTION FUCTION")
        current_state = self.getCurrentState()
        print("current_state in pre:", current_state)
        pre_action_list = self.getpreActionList(current_state)
        # print("pre_action_list in pre:", pre_action_list) 
        # Import locally to avoid circular import
        from bot_interface.models import FactoryInterface
        factory_interface = FactoryInterface()
        print("preaction::",current_state, pre_action_list)
        for pre_action in pre_action_list:
            data_dict = {}
            #pass user.current_session for previously taken title, trancript, etc
            data_dict.update({"state":self.getCurrentState()})
            data_dict.update({"smj_id":self.getSmjId()})
            data_dict.update({"user_id":self.user_id})
            data_dict.update({"language":self.language})
            data_dict.update({"bot_id":self.bot_id})
            print("data_dict:: ",data_dict)
            if pre_action.get("text"):
                # response_type = "text"
                data_dict.update({"text":pre_action.get('text')})
                interface = factory_interface.build_interface(self.app_type)
                interface.sendText(self.bot_id, data_dict)
                #set expected response type in sendText

            elif pre_action.get("menu"):
                # response_type = "menu"
                menu_data = pre_action.get('menu')
                data_dict.update({"menu": menu_data})
                
                # Extract caption from first menu item if present
                if menu_data and len(menu_data) > 0 and "caption" in menu_data[0]:
                    data_dict.update({"caption": menu_data[0]["caption"]})
                
                interface = factory_interface.build_interface(self.app_type)
                interface.sendButton(self.bot_id, data_dict)
                #set expected response type in sendButton
 
            elif pre_action.get("function"):
                #pass user.current_session for previously taken title, trancript, etc
                print(f"data passed in {current_state} function {pre_action.get('data')}")
                if pre_action.get("data"):
                    data_dict.update({"data":pre_action.get("data")})
                #call generic interface fucntion:factory
                # Import locally to avoid circular import
                from bot_interface.utils import callFunctionByName
                function_result = callFunctionByName(pre_action.get("function"), self.app_type, data_dict)
                
                # Check if SMJ jump was prepared by the function
                if function_result == "success" and "_smj_jump" in data_dict:
                    jump_info = data_dict["_smj_jump"]
                    print(f"Executing SMJ jump to '{jump_info['smj_name']}' from preAction")
                    
                    # Update state machine with new SMJ
                    self.setStates(jump_info['states'])
                    self.setSmjId(jump_info['smj_id'])
                    self.setCurrentState(jump_info['init_state'])
                    
                    # Update user session to preserve SMJ context
                    self._update_user_session_context(jump_info['smj_id'], jump_info['init_state'])
                    
                    print(f"Successfully switched to SMJ '{jump_info['smj_name']}' (ID: {jump_info['smj_id']}), current state: '{jump_info['init_state']}'")
                    
                    # Execute preAction for the new state recursively
                    return self.preAction()
                
                # Check if internal transition was prepared by move_forward
                elif function_result == "internal_transition_prepared" and "_internal_transition" in data_dict:
                    transition_info = data_dict["_internal_transition"]
                    
                    # Check if current state has transitions defined - if yes, treat as state transition
                    current_state = self.getCurrentState()
                    transition_dict = self.findTransitiondict(current_state)
                    
                    if transition_dict:
                        # State has transitions defined - treat as state transition, return the event
                        event = transition_info.get("event", "success")
                        print(f"State '{current_state}' has transitions - treating move_forward as state transition event: {event}")
                        # For preAction, we need to handle this differently since it doesn't directly process events
                        # Return the transition info to be handled by the caller
                        return transition_info
                    else:
                        # No transitions defined - treat as internal transition
                        print(f"Handling internal transition from preAction: {transition_info}")
                        return self.handleInternalTransition(transition_info)
                
                # ENHANCED: Handle any function return values that match valid transitions
                elif function_result and isinstance(function_result, str):
                    current_state = self.getCurrentState()
                    transition_dict = self.findTransitiondict(current_state)
                    
                    if transition_dict and self._is_valid_transition_event(function_result, transition_dict):
                        # Check if this is the last function in preAction
                        current_function_index = pre_action_list.index(pre_action)
                        is_last_function = current_function_index == len(pre_action_list) - 1
                        
                        if is_last_function:
                            # Only return early for the last function
                            print(f"Last function '{pre_action.get('function')}' returned '{function_result}' - valid transition event for state '{current_state}'")
                            print(f"Available transitions for state '{current_state}': {[list(t.keys())[0] + ': ' + str(list(t.values())[0]) for t in transition_dict]}")
                            return {
                                "event": function_result,
                                "action": "state_transition",
                                "state": current_state
                            }
                        else:
                            # For intermediate functions, just log and continue
                            print(f"Intermediate function '{pre_action.get('function')}' returned '{function_result}' - valid transition event but continuing to next function")
                    else:
                        # Function result doesn't match any valid transition
                        if transition_dict:
                            print(f"Function '{pre_action.get('function')}' returned '{function_result}' - not a valid transition event for state '{current_state}'")
                            print(f"Valid transitions: {[list(t.keys())[0] + ': ' + str(list(t.values())[0]) for t in transition_dict]}")
                        else:
                            print(f"Function '{pre_action.get('function')}' returned '{function_result}' but state '{current_state}' has no transitions - continuing normally")
        
        # Return None if no SMJ jump was performed
        return None

    def _is_valid_transition_event(self, event, transition_dict):
        """
        Check if the event matches any valid transition for the current state.
        Args:
            event (str): The event to check
            transition_dict (list): Transition dictionary for current state
        Returns:
            bool: True if event is a valid transition
        """
        if not transition_dict or not event:
            return False
            
        for transition in transition_dict:
            for next_state, valid_events in transition.items():
                if event in valid_events:
                    return True
        return False

    def postAction(self, event, event_data=None):
        print(f"DEBUG: postAction called with event='{event}', current_state='{self.getCurrentState()}'")
        updatedEvent = ""
        data_dict = {}
        current_state = self.getCurrentState()
        post_action_list = self.getpostActionList(current_state)
        print(f"DEBUG: post_action_list for state '{current_state}': {post_action_list}")
        
        for post_action in post_action_list:
            # functionName = list(elem.keys())[0]
            # for eventName in list(elem.values())[0]:
            #     if (list(eventName.keys())[0] == event) or list(eventName.keys())[0] == "*":
            data_dict.update({"state":current_state})
            data_dict.update({"smj_id":self.getSmjId()})
            data_dict.update({"user_id":self.user_id})
            data_dict.update({"bot_id":self.bot_id})  # Add bot_id for storage functions
            
            # Pass event data for storage functions to access location/audio data
            if event_data:
                data_dict.update({"event_data": event_data})
                # Extract specific data types for storage functions
                if event_data.get("type") == "location":
                    data_dict.update({"location_data": {
                        "data": event_data.get("data", ""),
                        "misc": event_data.get("misc", {}),
                        "latitude": event_data.get("misc", {}).get("latitude", ""),
                        "longitude": event_data.get("misc", {}).get("longitude", ""),
                        "address": event_data.get("misc", {}).get("address", ""),
                        "name": event_data.get("misc", {}).get("name", "")
                    }})
                elif event_data.get("type") in ["audio", "voice"]:
                    data_dict.update({"audio_data": {
                        "data": event_data.get("data", ""),
                        "media_id": event_data.get("media_id", ""),
                        "file_path": event_data.get("data", "")
                    }})
                elif event_data.get("type") == "image":
                    data_dict.update({"photo_data": {
                        "data": event_data.get("data", ""),
                        "media_id": event_data.get("media_id", ""),
                        "file_path": event_data.get("data", "")
                    }})
            
            if post_action.get("function"):
                print(f"DEBUG: About to call function '{post_action.get('function')}' with app_type='{self.app_type}'")
                print(f"DEBUG: data_dict keys: {list(data_dict.keys())}")
                print(f"DEBUG: event_data in data_dict: {data_dict.get('event_data', 'NOT_FOUND')}")
                
                #pass user.current_session for previously taken title, trancript, etc
                if post_action.get("data"):
                    data_dict.update({"data":post_action.get("data")})
                #call generic interface fucntion:factory
                # Import locally to avoid circular import
                from bot_interface.utils import callFunctionByName
                
                try:
                    updatedEvent = callFunctionByName(post_action.get("function"), self.app_type, data_dict)
                    print(f"DEBUG: Function '{post_action.get('function')}' executed successfully, returned: '{updatedEvent}'")
                except Exception as e:
                    print(f"ERROR: Function '{post_action.get('function')}' execution failed: {e}")
                    import traceback
                    traceback.print_exc()
                    updatedEvent = "failure"
                
                # Check if SMJ jump was prepared by the function
                if updatedEvent == "success" and "_smj_jump" in data_dict:
                    jump_info = data_dict["_smj_jump"]
                    print(f"Executing SMJ jump to '{jump_info['smj_name']}'")
                    
                    # Update state machine with new SMJ
                    self.setStates(jump_info['states'])
                    self.setSmjId(jump_info['smj_id'])
                    self.setCurrentState(jump_info['init_state'])
                    
                    # Update user session to preserve SMJ context  
                    self._update_user_session_context(jump_info['smj_id'], jump_info['init_state'])
                    
                    print(f"Successfully switched to SMJ '{jump_info['smj_name']}' (ID: {jump_info['smj_id']}), current state: '{jump_info['init_state']}'")
                    
                    # Execute preAction for the new state
                    self.preAction()
                    return "smj_jump_complete"
                
                # Check if internal transition was prepared by move_forward
                elif updatedEvent == "internal_transition_prepared" and "_internal_transition" in data_dict:
                    transition_info = data_dict["_internal_transition"]
                    
                    # Check if current state has transitions defined - if yes, treat as state transition
                    current_state = self.getCurrentState()
                    transition_dict = self.findTransitiondict(current_state)
                    
                    if transition_dict:
                        # State has transitions defined - treat as state transition, return the event
                        event = transition_info.get("event", "success")
                        print(f"State '{current_state}' has transitions - treating move_forward as state transition event: {event}")
                        return event
                    else:
                        # No transitions defined - treat as internal transition
                        print(f"Handling internal transition from postAction: {transition_info}")
                        self.handleInternalTransition(transition_info)
                        return "internal_transition_complete"

            # print("updatedEvent:",updatedEvent)
            if updatedEvent:
                print(f"DEBUG: Returning updatedEvent from postAction: '{updatedEvent}'")
                return updatedEvent
        print("post action event: ", event)
        print(f"DEBUG: No updatedEvent found, returning original event: '{event}'")
        return event

    def findAndDoTransition(self, event, transition_dict_list):
        state = findTransitionState(event, transition_dict_list)
        self.setCurrentState(state)
        # print('findAndDoTransition::',self.current_state)
        return state

    def handleInternalTransition(self, transition_info):
        """
        Handle state transitions within the same execution context to prevent duplicate messages.
        
        Args:
            transition_info: Dictionary containing transition data from move_forward
        """
        print(f"Handling internal transition: {transition_info}")
        
        # Update current state
        new_state = transition_info.get("state")
        if new_state:
            self.setCurrentState(new_state)
            print(f"Updated current state to: {new_state}")
        
        # Continue with normal transition logic using the event
        event = transition_info.get("event", "success")
        
        # Get transition dictionary for current state
        transition_dict = self.findTransitiondict(self.getCurrentState())
        print(f"Transition dict for state '{self.getCurrentState()}': {transition_dict}")
        
        # Find and execute the transition
        state_new = self.findAndDoTransition(event, transition_dict)
        print(f"Transition resulted in new state: {state_new}")
        
        if state_new == "finish":
            print("State machine finished")
            return 0
        elif state_new != "defaultSMJ":
            if self.ifStateExists(state_new):
                print(f"Executing preAction for new state: {state_new}")
                self.preAction()  # This will send messages for the new state
                return 1
        
        return 1  # Success

def findTransitionState(event, transition_dict_list):
    print("event::", event)
    transitionState = ""
    default_transitionState = ""
    for transition_dict in transition_dict_list:
        for key, value in transition_dict.items():
            default_transitionState = key if "nomatch" in value else ""
            print("IOIOIO>>", key, value)
            if event in value:
                transitionState = key
                break
            if value == ["*"] and event != "success":  # TODO Think of some other condition to filter on
                default_transitionState = key

    if transitionState == "" and default_transitionState:
        transitionState = default_transitionState
    
    print("findTransitionState:: ", transitionState)    
    return transitionState


class SmjController(StateMapData):
    
    def __init__(self, states, smj_id, app_type, bot_id, user_id, language, current_state, current_session):
        super(SmjController, self).__init__(states, smj_id, app_type, bot_id, user_id, language, current_state, current_session)

    def _update_user_session_context(self, smj_id, current_state):
        """Update user session to preserve SMJ context after jumps"""
        try:
            import bot_interface.models
            
            # Get the user session
            user_session = bot_interface.models.UserSessions.objects.get(user_id=self.user_id, bot_id=self.bot_id)
            
            # Update with current SMJ and state
            smj_instance = bot_interface.models.SMJ.objects.get(id=smj_id)
            user_session.current_smj = smj_instance
            user_session.current_state = current_state
            user_session.save()
            
            print(f"Updated user session - SMJ: {smj_id}, State: {current_state}")
            
        except Exception as e:
            print(f"Error updating user session context: {e}")
            # Don't fail the whole process if session update fails

    def _load_correct_smj_states(self, smj_id, event_state=None):
        """Load the correct SMJ states when there's a mismatch"""
        try:
            import bot_interface.models
            import json
            
            # Load the correct SMJ
            smj = bot_interface.models.SMJ.objects.get(id=smj_id)
            smj_states = smj.smj_json
            
            # Handle Django JSONField - can be string or already parsed
            if isinstance(smj_states, str):
                smj_states = json.loads(smj_states)
            
            # Update state machine with correct SMJ
            self.setStates(smj_states)
            self.setSmjId(smj_id)
            
            # If event_state is provided, use it; otherwise use the first state as default
            if event_state:
                self.setCurrentState(event_state)
                print(f"Set current state to event state: {event_state}")
            elif smj_states:
                self.setCurrentState(smj_states[0]['name'])
                print(f"Set current state to first state: {smj_states[0]['name']}")
            
            print(f"Loaded correct SMJ states for SMJ ID: {smj_id}")
            print(f"Available states: {[state.get('name') for state in smj_states]}")
            
        except Exception as e:
            print(f"Error loading correct SMJ states: {e}")

    def runSmj(self, event_data):
        print("Event packet passed in runSmj  >> ",event_data)
        if event_data:
            if event_data.get("event") == "start":
                print("event is start..")
                
                # Check if we need to load correct SMJ states first
                event_smj_id = event_data.get("smj_id")
                event_state = event_data.get("state")
                
                if event_smj_id and str(event_smj_id) != str(self.getSmjId()):
                    print(f"Loading correct SMJ states for start event: SMJ {event_smj_id}, State {event_state}")
                    self._load_correct_smj_states(event_smj_id, event_state)
                
                # Additional check: Don't execute preAction if this is actually a button interaction
                # that was incorrectly marked as "start"
                if event_data.get("type") == "button" and event_data.get("context_id"):
                    print("Detected button interaction marked as start - skipping preAction to prevent duplicate messages")
                    print(f"Button data: {event_data.get('data')}, Context ID: {event_data.get('context_id')}")
                    # Process this as a normal transition instead
                    event_data["event"] = event_data.get("data", "success")  # Use button value as event
                    # Continue to else block for normal processing
                else:
                    state = self.jumpToSmj(self.states, event_data.get("smj_id"), event_data.get("state"))
                    if state != "ERROR":
                        getstate = self.ifStateExists(state)
                        print("state::", state, "getstate::", getstate)
                        if getstate:
                            start_time = datetime.now()
                            preaction_result = self.preAction()
                            end_time = datetime.now()
                            
                            # Handle transition info returned from preAction
                            if preaction_result and isinstance(preaction_result, dict):
                                if 'event' in preaction_result:
                                    print(f"PreAction returned transition info: {preaction_result}")
                                    event_to_process = preaction_result['event']
                                    
                                    # Find and execute transition
                                    current_state = self.getCurrentState()
                                    transition_dict = self.findTransitiondict(current_state)
                                    print(f"Transition dict for state '{current_state}': {transition_dict}")
                                    
                                    if transition_dict:
                                        state_new = self.findAndDoTransition(event_to_process, transition_dict)
                                        print("state_new from preAction transition::", state_new)
                                        if state_new != "finish" and state_new != "defaultSMJ":
                                            getstate = self.ifStateExists(state_new)
                                            print("getstate for new state::", getstate)
                                            if getstate:
                                                print(f"Executing preAction for new state: {state_new}")
                                                self.preAction()  # Execute preAction for new state
                                    else:
                                        print(f"WARNING: No transitions found for state '{current_state}' despite preAction returning transition info")
                                elif preaction_result.get('action') == 'continue':
                                    # Handle internal transitions returned from preAction
                                    print(f"Handling internal transition from preAction: {preaction_result}")
                                    return self.handleInternalTransition(preaction_result)
                     
                            print('Duration of PreAction: {}'.format(end_time - start_time))
                            return  # Exit early for true start events
            elif hasattr(event_data,'init_state'):  
                print("initial state event")
                default_state = self.jumpTodefaultSmj(self.states, event_data.get("smj_id"), event_data.get("init_state"))
                getstate = self.ifStateExists(default_state)
                if getstate:
                    start_time = datetime.now()
                    preaction_result = self.preAction()
                    end_time = datetime.now()
                    
                    # Handle transition info returned from preAction
                    if preaction_result and isinstance(preaction_result, dict):
                        if 'event' in preaction_result:
                            print(f"PreAction returned transition info for init_state: {preaction_result}")
                            event_to_process = preaction_result['event']
                            
                            # Find and execute transition
                            current_state = self.getCurrentState()
                            transition_dict = self.findTransitiondict(current_state)
                            print(f"Transition dict for state '{current_state}': {transition_dict}")
                            
                            if transition_dict:
                                state_new = self.findAndDoTransition(event_to_process, transition_dict)
                                print("state_new from init_state preAction transition::", state_new)
                                if state_new != "finish" and state_new != "defaultSMJ":
                                    getstate = self.ifStateExists(state_new)
                                    print("getstate for new state::", getstate)
                                    if getstate:
                                        print(f"Executing preAction for new state: {state_new}")
                                        self.preAction()  # Execute preAction for new state
                            else:
                                print(f"WARNING: No transitions found for state '{current_state}' despite preAction returning transition info")
                        elif preaction_result.get('action') == 'continue':
                            # Handle internal transitions returned from preAction
                            print(f"Handling internal transition from init_state preAction: {preaction_result}")
                            return self.handleInternalTransition(preaction_result)
                    
                    print('Duration of PreAction: {}'.format(end_time - start_time))
                    # print("ENDED PRE ACTION FUCTION")
            
            # Handle all non-start events (including button events that were converted from start)
            if event_data.get("event") != "start" or (event_data.get("event") == "start" and event_data.get("type") == "button"):
                # print("POSTTT")
                # print(self.getCurrentState(), event_data.get("state"))
                #do postAction and transition and next preAction
                # if self.getCurrentState() == event_data.get("state") and self.getSmjId() == event_data.get("smj_id"):
                   #updateEventLogs(event)
                # print("POST ACTION FUCTION")
                start_time = datetime.now()
                print("postaction event and state >> ",event_data.get("event"), event_data.get("state"))
                
                # Special handling for CommunitySelectionMenu state
                current_state = event_data.get("state")
                event_to_process = event_data.get("event")
                
                print(f"DEBUG: current_state = '{current_state}', type = {type(current_state)}")
                print(f"DEBUG: comparing with 'CommunitySelectionMenu', result = {current_state == 'CommunitySelectionMenu'}")
                
                if event_data.get("type") == "button":
                    if current_state == "CommunitySelectionMenu":
                        # For CommunitySelectionMenu, pass original event to let postAction handle button processing
                        print(f"CommunitySelectionMenu button event - passing original event '{event_to_process}' to postAction")
                    else:
                        # For all other states, use existing button mapping logic
                        button_value = event_data.get("misc") or event_data.get("data")
                        if button_value:
                            print(f"Button event detected - mapping button value '{button_value}' as transition event")
                            event_to_process = button_value
                
                updatedEvent = self.postAction(event_to_process, event_data)
                
                # Check if SMJ jump was completed - if so, skip normal transition logic
                if updatedEvent == "smj_jump_complete":
                    print("SMJ jump completed, skipping transition logic")
                    return 1  # Return success
                
                # Check if internal transition was completed - if so, skip normal transition logic
                if updatedEvent == "internal_transition_complete":
                    print("Internal transition completed, skipping transition logic")
                    return 1  # Return success
            
                # Ensure we're using the correct SMJ states for transition lookup
                current_state_for_transition = event_data.get("state")
                print(f"Looking for transitions in state: {current_state_for_transition}")
                print(f"Current SMJ ID: {self.getSmjId()}")
                print(f"Available states in SMJ: {[state.get('name') for state in self.getStates()]}")

                # Load correct SMJ states if current SMJ doesn't match event SMJ
                event_smj_id = event_data.get("smj_id")
                if event_smj_id and str(event_smj_id) != str(self.getSmjId()):
                    print(f"SMJ mismatch detected. Event SMJ: {event_smj_id}, Current SMJ: {self.getSmjId()}")
                    self._load_correct_smj_states(event_smj_id, current_state_for_transition)

                transition_dict = self.findTransitiondict(current_state_for_transition)
                print(f"Transition dict for state '{current_state_for_transition}': {transition_dict}")

                if not transition_dict:
                    print(f"WARNING: No transitions found for state '{current_state_for_transition}' in current SMJ")
                    return 0

                state_new = self.findAndDoTransition(updatedEvent, transition_dict)
                print("state_new::",state_new)
                if state_new == "finish":
                    #WHAT TO DO
                    return 0
                elif state_new != "defaultSMJ":
                    getstate = self.ifStateExists(state_new)
                    print("getstate::",getstate)
                    if getstate:          
                        preaction_result = self.preAction()
                        end_time = datetime.now()
                        
                        # Handle transition info returned from preAction
                        if preaction_result and isinstance(preaction_result, dict):
                            if 'event' in preaction_result:
                                print(f"PreAction returned transition info: {preaction_result}")
                                event_to_process = preaction_result['event']
                                
                                # Find and execute transition
                                current_state = self.getCurrentState()
                                transition_dict = self.findTransitiondict(current_state)
                                print(f"Transition dict for state '{current_state}': {transition_dict}")
                                
                                if transition_dict:
                                    state_new = self.findAndDoTransition(event_to_process, transition_dict)
                                    print("state_new from preAction transition::", state_new)
                                    if state_new != "finish" and state_new != "defaultSMJ":
                                        getstate = self.ifStateExists(state_new)
                                        print("getstate for new state::", getstate)
                                        if getstate:
                                            print(f"Executing preAction for new state: {state_new}")
                                            self.preAction()  # Execute preAction for new state
                                else:
                                    print(f"WARNING: No transitions found for state '{current_state}' despite preAction returning transition info")
                            elif preaction_result.get('action') == 'continue':
                                # Handle internal transitions returned from preAction
                                print(f"Handling internal transition from preAction: {preaction_result}")
                                self.handleInternalTransition(preaction_result)
                        
                        #print('Duration of PreAction: {}'.format(end_time - start_time))