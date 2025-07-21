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
                data_dict.update({"menu":pre_action.get('menu')})
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
                    
                    print(f"Successfully switched to SMJ '{jump_info['smj_name']}' (ID: {jump_info['smj_id']}), current state: '{jump_info['init_state']}'")
                    
                    # Execute preAction for the new state recursively
                    return self.preAction()
                
                # Check if internal transition was prepared by move_forward
                elif function_result == "internal_transition_prepared" and "_internal_transition" in data_dict:
                    transition_info = data_dict["_internal_transition"]
                    print(f"Handling internal transition from preAction: {transition_info}")
                    
                    # Handle the internal transition
                    return self.handleInternalTransition(transition_info)
        
        # Return None if no SMJ jump was performed
        return None


    def postAction(self, event):
        updatedEvent = ""
        data_dict = {}
        current_state = self.getCurrentState()
        post_action_list = self.getpostActionList(current_state)
        for post_action in post_action_list:
            # functionName = list(elem.keys())[0]
            # for eventName in list(elem.values())[0]:
            #     if (list(eventName.keys())[0] == event) or list(eventName.keys())[0] == "*":
            data_dict.update({"state":current_state})
            data_dict.update({"smj_id":self.getSmjId()})
            data_dict.update({"user_id":self.user_id})
            if post_action.get("function"):
                #pass user.current_session for previously taken title, trancript, etc
                if post_action.get("data"):
                    data_dict.update({"data":post_action.get("data")})
                #call generic interface fucntion:factory
                # Import locally to avoid circular import
                from bot_interface.utils import callFunctionByName
                updatedEvent = callFunctionByName(post_action.get("function"), self.app_type, data_dict)
                
                # Check if SMJ jump was prepared by the function
                if updatedEvent == "success" and "_smj_jump" in data_dict:
                    jump_info = data_dict["_smj_jump"]
                    print(f"Executing SMJ jump to '{jump_info['smj_name']}'")
                    
                    # Update state machine with new SMJ
                    self.setStates(jump_info['states'])
                    self.setSmjId(jump_info['smj_id'])
                    self.setCurrentState(jump_info['init_state'])
                    
                    print(f"Successfully switched to SMJ '{jump_info['smj_name']}' (ID: {jump_info['smj_id']}), current state: '{jump_info['init_state']}'")
                    
                    # Execute preAction for the new state
                    self.preAction()
                    return "smj_jump_complete"
                
                # Check if internal transition was prepared by move_forward
                elif updatedEvent == "internal_transition_prepared" and "_internal_transition" in data_dict:
                    transition_info = data_dict["_internal_transition"]
                    print(f"Handling internal transition from postAction: {transition_info}")
                    
                    # Handle the internal transition
                    self.handleInternalTransition(transition_info)
                    return "internal_transition_complete"

            # print("updatedEvent:",updatedEvent)
            if updatedEvent:
                return updatedEvent
        print("post action event: ", event)
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

    def runSmj(self, event_data):
        print("Event packet passed in runSmj  >> ",event_data)
        if event_data:
            if event_data.get("event") == "start":
                print("event is start..")
                state = self.jumpToSmj(self.states, event_data.get("smj_id"), event_data.get("state"))
                if state != "ERROR":
                    getstate = self.ifStateExists(state)
                    print("state::", state, "getstate::", getstate)
                    if getstate:
                        start_time = datetime.now()
                        self.preAction()
                        end_time = datetime.now()
                 
                        print('Duration of PreAction: {}'.format(end_time - start_time))
            elif hasattr(event_data,'init_state'):  
                print("initial state event")
                default_state = self.jumpTodefaultSmj(self.states, event_data.get("smj_id"), event_data.get("init_state"))
                getstate = self.ifStateExists(default_state)
                if getstate:
                    start_time = datetime.now()
                    self.preAction()
                    end_time = datetime.now()
                    print('Duration of PreAction: {}'.format(end_time - start_time))
                    # print("ENDED PRE ACTION FUCTION")
            else:
                # print("POSTTT")
                # print(self.getCurrentState(), event_data.get("state"))
                #do postAction and transition and next preAction
                # if self.getCurrentState() == event_data.get("state") and self.getSmjId() == event_data.get("smj_id"):
                   #updateEventLogs(event)
                # print("POST ACTION FUCTION")
                start_time = datetime.now()
                print("postaction event and state >> ",event_data.get("event"), event_data.get("state"))
                updatedEvent = self.postAction(event_data.get("event"))
                
                # Check if SMJ jump was completed - if so, skip normal transition logic
                if updatedEvent == "smj_jump_complete":
                    print("SMJ jump completed, skipping transition logic")
                    return 1  # Return success
                
                # Check if internal transition was completed - if so, skip normal transition logic
                if updatedEvent == "internal_transition_complete":
                    print("Internal transition completed, skipping transition logic")
                    return 1  # Return success
            
                # transition_dict = self.findTransitiondict(self.getCurrentState())
                transition_dict = self.findTransitiondict(event_data.get("state"))
                print(updatedEvent, transition_dict)
                state_new = self.findAndDoTransition(updatedEvent, transition_dict)
                print("state_new::",state_new)
                if state_new == "finish":
                    #WHAT TO DO
                    return 0
                elif state_new != "defaultSMJ":
                    getstate = self.ifStateExists(state_new)
                    print("getstate::",getstate)
                    if getstate:          
                        self.preAction()
                        end_time = datetime.now()
                        #print('Duration of PreAction: {}'.format(end_time - start_time))