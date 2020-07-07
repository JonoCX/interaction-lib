"""

"""

from .base import BaseExtractor
from ._event_handler import EventHandler

from datetime import datetime as dt
from collections import Counter, defaultdict
from typing import Optional, Union, List, Set, Dict, Counter
from joblib import Parallel, delayed
from nltk import ngrams


class SequenceError(Exception):
    """ 
        Custom error to raise when the sequences do not exist
        but the user is requesting an action to be performed
        using the sequences.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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

                        results[user].append(e_handler.process_event(event))
                
                if compress:
                    results[user] = self._compress_events(results[user], compress_event)
                
                if categories:
                    results[user] = self._categorize_sequence(results[user], categories)

                e_handler = e_handler.reset()

            return results

        # ERROR CHECKING
        if not isinstance(interaction_events, set):
            raise TypeError(
                f"interaction_events should be a set, current type: {type(interaction_events)}"
            )
        
        if not isinstance(aliases, dict):
            raise TypeError(f"aliases should be a dict, current type: {type(aliases)}")

        # check that all interaction events are in the aliases
        if not set(interaction_events) == set(aliases.keys()):
            raise ValueError('interaction events and aliases keys should be the same')

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
            
            self._sequences = {user: [] for user in self._users}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            e_handler = EventHandler(aliases)

            # runs the _seq function in parallel
            res = parallel(delayed(_seq) (u, e, e_handler) for u, e in self._split_users())

            # unpack the results and add them to the sequences dict
            for r in res:
                for u, s in r.items():
                    self._sequences[u] = s

            return self._sequences
        else:
            if user_id is not None:
                if not isinstance(user_id, str):
                    raise TypeError('user_id should be a string: {0} (type: {1}'.format(
                        user_id, type(user_id)
                    ))

                if user_id not in self._users:
                    raise ValueError('Invalid user_id: {0}'.format(user_id))

                return self._sequences[user_id]
            return self._sequences

    def get_ngrams(
        self, 
        n: Optional[int] = 3,
        counter: Optional[bool] = False, 
    ) -> Union[List, Counter]:
        """ 
            - if get_sequences hasn't be called, then we propagate an error.
            - should provide the option to say what the N is
            - Return just the ngrams or a counter object
        """
        if not self._sequences:
            raise SequenceError(
                'Sequences have not been extracted, call the '
                'get_sequences beforehand.'
            )

        if not isinstance(n, int):
            raise TypeError('n should be an int: {0} (type: {1}'.format(n, type(n)))

        if not isinstance(counter, bool):
            raise TypeError(
                'counter should be a bool: {0} (type: {1})'.format(counter, type(counter)))

        # Get the n-grams for all of the users
        ngrams_dict = defaultdict(list) # {user_id -> [n_grams, ...], ...}
        for user, sequence in self._sequences.items():
            calculate_ngrams = ngrams(sequence, n) # get the ngrams for this sequence
            for each_gram in calculate_ngrams:
                ngrams_dict[user].append(each_gram) # append each gram

        counts = Counter() # Count the n-grams (across all n-grams)
        user_counts = defaultdict(Counter) # Count the n-grams (for each user)
        for user, all_grams in ngrams_dict.items():
            # update the counter with the current list of n-grams
            counts.update(all_grams)
            user_counts[user].update(all_grams)

        if counter:
            return ngrams_dict, counts, user_counts
        else:
            return ngrams_dict
