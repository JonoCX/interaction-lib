"""

"""

from .base import BaseExtractor
from ._event_handler import EventHandler

from datetime import datetime as dt
from typing import Optional, Union, List, Set, Dict

class Sequences(BaseExtractor):

    def __init__(self, user_event_dict, completion_point=None, n_jobs=-1):
        super().__init__(user_event_dict, completion_point=completion_point, n_jobs=n_jobs)

    def _compress_events(self, sequence: List[str], compress_event: str = 'NEC') -> List[str]:
        """ """
        if len(sequence) == 0:
            return sequence

        updated_sequence = []
        just_seen_event = False
        for indx, event in enumerate(sequence): # for each event in the sequence

            # if the event is the event to be compressed and we've not just seen it
            if event == compress_event and not just_seen_event:
                count = 1
                for other_event in sequence[(indx + 1): ]: # scan forward to count
                    if other_event == compress_event: count += 1
                    else: break
                
                just_seen_event = True # indicate that we've just seen it
                updated_sequence.append(event + '_' + str(count)) # event_count format
            
            # if we've not just seen the event, the append the next one
            if not just_seen_event:
                updated_sequence.append(event)
            
            # if we have just seen the event and the event isn't the compress event
            if just_seen_event and event != compress_event:
                updated_sequence.append(event)
                just_seen_event = False # set just seen to false
        
        return updated_sequence

    def get_sequences(
        self,
        interaction_events: List[str],
        aliases: Dict[str, str],
        user_id: Optional[str] = None,
        compress: Optional[bool] = True,
        verbose: Optional[int] = 0
    ) -> Dict[str, Dict]:
        """ 
        
        """
        pass

""" 
include the pauses for between events like browser visibility
"""