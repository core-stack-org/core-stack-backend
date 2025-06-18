from django.db import models
from django.utils import timezone
import json
from typing import Dict, List, Any, Callable, Optional, Union

class StateMachine:
    """
    State Machine class for managing states and transitions in bot conversations.
    
    The state machine follows a structure where each state contains:
    - name: The name of the state
    - preaction: Action to perform when entering the state
    - postaction: Action to perform after receiving user response
    - transition: Rules for moving to the next state
    """
    def __init__(self, initial_state: str, states_json: List[Dict[str, Any]]):
        """
        Initialize a new state machine with a starting state and state definitions.
        
        Args:
            initial_state (str): The name of the initial state
            states_json (List[Dict]): List of state definitions
        """
        self.current_state = initial_state
        self.states = self._parse_states(states_json)
        self.history = []  # Track state transitions
        
    def _parse_states(self, states_json: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Parse the states from JSON format into a more accessible dictionary.
        
        Args:
            states_json (List[Dict]): List of state definitions
            
        Returns:
            Dict: Dictionary with state names as keys and state definitions as values
        """
        parsed_states = {}
        for state in states_json:
            if 'name' not in state:
                raise ValueError("State definition missing 'name' field")
            
            name = state['name']
            parsed_states[name] = {
                'preaction': state.get('preaction', None),
                'postaction': state.get('postaction', None),
                'transition': state.get('transition', {})
            }
        return parsed_states
    
    def get_current_state(self) -> Dict:
        """
        Get the current state definition.
        
        Returns:
            Dict: The current state definition
        """
        if self.current_state not in self.states:
            raise ValueError(f"Current state '{self.current_state}' not found in state definitions")
            
        return {
            'name': self.current_state,
            **self.states[self.current_state]
        }
    
    def execute_preaction(self) -> Any:
        """
        Execute the preaction of the current state.
        
        Returns:
            Any: Result of the preaction execution
        """
        state = self.get_current_state()
        preaction = state['preaction']
        
        if preaction is None:
            return None
            
        if callable(preaction):
            return preaction()
        
        return preaction
    
    def execute_postaction(self, user_response: Any) -> Any:
        """
        Execute the postaction of the current state.
        
        Args:
            user_response (Any): The user's response to process
            
        Returns:
            Any: Result of the postaction execution
        """
        state = self.get_current_state()
        postaction = state['postaction']
        
        if postaction is None:
            return None
            
        if callable(postaction):
            return postaction(user_response)
            
        return postaction
    
    def transition(self, outcome: str = 'success') -> str:
        """
        Transition to the next state based on the outcome.
        
        Args:
            outcome (str): The outcome of the current state ('success' or 'failure')
            
        Returns:
            str: The name of the new current state
        """
        state = self.get_current_state()
        transitions = state['transition']
        
        if outcome not in transitions:
            raise ValueError(f"Outcome '{outcome}' not found in transitions for state '{self.current_state}'")
            
        next_state = transitions[outcome]
        
        # Record the transition in history
        self.history.append({
            'from': self.current_state,
            'to': next_state,
            'outcome': outcome,
            'timestamp': timezone.now()
        })
        
        # Update the current state
        self.current_state = next_state
        
        return self.current_state
    
    def to_json(self) -> str:
        """
        Convert the state machine to a JSON string.
        
        Returns:
            str: JSON representation of the state machine
        """
        state_list = []
        for name, state in self.states.items():
            state_dict = {
                'name': name,
                **state
            }
            state_list.append(state_dict)
            
        return json.dumps({
            'current_state': self.current_state,
            'states': state_list,
            'history': self.history
        }, default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'StateMachine':
        """
        Create a state machine from a JSON string.
        
        Args:
            json_str (str): JSON representation of a state machine
            
        Returns:
            StateMachine: A new state machine instance
        """
        data = json.loads(json_str)
        current_state = data['current_state']
        states = data['states']
        
        sm = cls(current_state, states)
        sm.history = data.get('history', [])
        
        return sm


class StateMachineRunner:
    """
    Helper class for running a state machine with actual implemented actions.
    
    This class connects the state machine to actual implementations of
    preactions and postactions, as well as handling user responses.
    """
    def __init__(self, state_machine: StateMachine, action_handlers: Dict[str, Callable]):
        """
        Initialize a new state machine runner.
        
        Args:
            state_machine (StateMachine): The state machine to run
            action_handlers (Dict[str, Callable]): Dictionary mapping action names to handlers
        """
        self.state_machine = state_machine
        self.action_handlers = action_handlers
        
    def process_user_input(self, user_input: Any) -> Dict[str, Any]:
        """
        Process user input and return the next response.
        
        Args:
            user_input (Any): The user input to process
            
        Returns:
            Dict[str, Any]: The next response to send to the user
        """
        # Execute postaction with user input
        current_state = self.state_machine.get_current_state()
        postaction_name = current_state['postaction']
        
        # Determine outcome based on postaction
        outcome = 'success'  # Default
        if postaction_name and postaction_name in self.action_handlers:
            outcome = self.action_handlers[postaction_name](user_input)
        
        # Transition to next state
        next_state_name = self.state_machine.transition(outcome)
        
        # Execute preaction of the new state
        next_state = self.state_machine.get_current_state()
        preaction_name = next_state['preaction']
        
        response = {'state': next_state_name}
        if preaction_name and preaction_name in self.action_handlers:
            response['message'] = self.action_handlers[preaction_name]()
        
        return response
    