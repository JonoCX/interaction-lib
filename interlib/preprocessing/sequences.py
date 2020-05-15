"""

"""

from .base import BaseExtractor
from ._event_handler import EventHandler

from datetime import datetime as dt
from typing import Optional, Union, List, Set, Dict

class Sequences(BaseExtractor):

    def __init__(self, user_event_dict, completion_point=None, n_jobs=-1):
        super().__init__(user_event_dict, completion_point=completion_point, n_jobs=n_jobs)

        self._sequences = {}

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

    def _categorize_sequence(self, sequence: List[str], categories: Dict[str, str]) -> List[str]:
        """ TODO """
        pass

    def get_sequences(
        self,
        interaction_events: Set[str],
        aliases: Dict[str, str],
        user_id: Optional[str] = None,
        compress: Optional[bool] = True,
        compress_event: Optional[str] = None,
        categories: Optional[Dict[str, str]] = None,
        verbose: Optional[int] = 0
    ) -> Dict[str, Dict]:
        """ 
        
        """
        def _seq(user_chunk, data_chunk, e_handler):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: [] for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # if there is no events, just continue
                    continue
                
                previous_timestamp = None
                for event in events: # for each event in the users events
                    # if the event is one that should be captured
                    if event['action_name'] in interaction_events:
                        
                        # pauses are tracked between the events that are being tracked and
                        # that are to be included in the sequence
                        if previous_timestamp == None: previous_timestamp = event['timestamp']
                        
                        pause_type, _ = self._type_of_pause(previous_timestamp, event['timestamp'])
                        if pause_type != 0:
                            results[user].append(pause_type)
                        previous_timestamp = event['timestamp']

                        if event['action_name'] == 'BROWSER_VISIBILITY_CHANGE':
                            # TODO: how long did they go hidden? Record that as a part
                            # of the sequence as well.
                            pass

                        results[user].append(e_handler.process_event(event))
                
                if compress:
                    results[user] = self._compress_events(results[user], compress_event)
                
                if categories:
                    results[user] = self._categorize_sequence(results[user], categories)

                e_handler = e_handler.reset()

            return results

        if not self._sequences:
            if user_id is not None: 
                if not isinstance(user_id, str):
                    raise TypeError('user_id should be a string: {0} (type: {1})'.format(
                        user_id, type(user_id)
                    ))

                if user_id not in self._users:
                    raise ValueError('Invalid user_id: {0}'.format(user_id))

                e_handler = EventHandler(aliases)
                return _seq(
                    user_chunk = [user_id], 
                    data_chunk = self.data[user_id],
                    e_handler = e_handler)[user_id]

""" 
include the pauses for between events like browser visibility
"""