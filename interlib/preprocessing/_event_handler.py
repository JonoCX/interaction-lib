""" 

"""

from typing import List, Dict

class EventHandler():
 
    def __init__(self, aliases: Dict[str, str]):
        if not isinstance(aliases, dict):
            raise TypeError('aliases should be a dictionary, type: {0}'.format(type(aliases)))
        
        if len(aliases) == 0: raise ValueError('aliases is empty, it should be populated')

        self.aliases = aliases
        self._volume_tracker = 1.0

    def _video_scrub(self, event: Dict) -> str:
        """ 
        
        :params event:
        :returns:
        """
        # TODO Not yet implemented into the data.
        pass

    def _volume_change(self, event: Dict, action_name: str) -> str:
        """ """
        changed_to_level = float(event['data']['romper_to_state'].split(' ')[1])
        if changed_to_level > self._volume_tracker:
            result = self.aliases[action_name] + '_UP'
        elif changed_to_level < self._volume_tracker:
            result = self.aliases[action_name] + '_DOWN'
        else:
            result = self.aliases[action_name] + '_NO'
        
        self._volume_tracker = changed_to_level
        return result

    def _fullscreen(self, event: Dict, action_name: str) -> str:
        """ """
        if event['data']['romper_to_state'] == 'fullscreen':
            return 'TO_' + self.aliases[action_name]
        else:
            return 'FROM_' + self.aliases[action_name]

    def _browser_visibility_change(self, event: Dict, action_name: str) -> str:
        """ """
        if event['data']['romper_to_state'] == 'hidden':
            return self.aliases[action_name] + '_H'
        else:
            return self.aliases[action_name] + '_V'

    def _subtitles(self, event: Dict, action_name: str) -> str:
        """ """
        if event['data']['romper_to_state'] == 'showing':
            return self.aliases[action_name] + '_ON'
        else:
            return self.aliases[action_name] + '_OFF'

    def _window_orientation_change(self, event: Dict, action_name: str) -> str: 
        """
        """
        # states: not_set, 90, -90, 0, 180, "", or 65446 (happens twice)
        if event['data']['romper_to_state'] in {90, -90}:
            return self.aliases[action_name] + '_H' # horizontal
        elif event['data']['romper_to_state'] in {0, 180}:
            return self.aliases[action_name] + '_V' # vertical
        else: # the odd case where it's near horizontal or vertical ("" or 65446)
            return self.aliases[action_name]
 
    def process_event(self, event: Dict) -> str:
        """ 
        Given an event, get the short hand alias for it and 
        append any useful additional information to it.
        
        :params event: the event to process (the whole event data as
        a dictionary)
        :returns: the alias representation of the event
        """
        action_name = event['action_name']
        if action_name == 'VOLUME_CHANGED':
            return self._volume_change(event, action_name)
        elif action_name == 'FULLSCREEN_BUTTON_CLICKED':
            return self._fullscreen(event, action_name)
        elif action_name == 'BROWSER_VISIBILITY_CHANGE':
            return self._browser_visibility_change(event, action_name)
        elif action_name == 'WINDOW_ORIENTATION_CHANGE':
            return self._window_orientation_change(event, action_name)
        else:
            return self.aliases[action_name]

    def reset(self):
        """ 
            This function should be called to reset the tracker variables
            in the class, such as the volume tracker, whenever a user's
            events have been processed.
        """
        self._volume_tracker = 1.0
        return self 
    
        