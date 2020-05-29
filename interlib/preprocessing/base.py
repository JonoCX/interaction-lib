from joblib import cpu_count
from datetime import datetime as dt
from typing import Union, Dict, Optional, List

import numpy as np

from ..util.data import _get_users_clicked_start_button

class BaseExtractor():
    """ Base class for all of the extractors """
    
    def __init__(
        self, 
        user_event_dict: Dict[str, List], 
        completion_point: Optional[str] = None, 
        n_jobs: Optional[int] = -1
    ):
        if not isinstance(user_event_dict, dict):
            raise TypeError('User Event dictionary is not a dict')

        if len(user_event_dict) == 0:
            raise ValueError('User event dictionary must have at least one value or not None')

        if completion_point and not isinstance(completion_point, str):
            raise TypeError('completion_point should be a str')

        if not isinstance(n_jobs, int):
            raise TypeError('n_jobs should be an int')

        self.data = self._sort_events(user_event_dict)
        self.completion_point = completion_point
        self.n_jobs = n_jobs

        if self.completion_point:
            self._users_reached_completion_point = self._reached_completion_point()

        if self.n_jobs == -1: self._num_cpu = cpu_count()
        else: self._num_cpu = n_jobs

        self._users = set(self.data.keys())
        self._users_split = self._split_users()

    def _sort_events(self, user_event_dict):
        data = {}
        for user, events in user_event_dict.items():
            data[user] = sorted(events, key = lambda x: x['timestamp'])
        return data

    def _split_users(self):
        """ """
        split_events = [[] for _ in range(0, self._num_cpu)]
        splits = np.array_split(list(self._users), self._num_cpu)
        for idx, split in enumerate(splits):
            for u in split:
                for e in self.data[u]:
                    split_events[idx].append(e)
        
        return zip(splits, split_events)

    def _reached_completion_point(self):
        """ """
        reached_end = {}
        for user, events in self.data.items():
            ne_changes = [
                change 
                for change in events 
                if change['action_type'] == 'STORY_NAVIGATION'
            ]

            for ne_change in ne_changes:
                if ne_change['data']['romper_to_state'] == self.completion_point:
                    reached_end[user] = True 
                    break
            
            if user not in reached_end.keys(): reached_end[user] = False

        return reached_end

    def _type_of_pause(
        self, 
        timestamp: dt, 
        next_timestamp: dt
    ) -> Union[str, int]:
        """ 
            Determine the type of pause that has happened based on
            two timestamps.

            :params timestamp: the current event time
            :params next_timestamp: the next event time
            :returns: the type of pause (str) and the difference
            between the two parameters
        """
        if timestamp is None or next_timestamp is None:
            raise ValueError('Both timestamp parameters have to be initialised')
        
        if not isinstance(timestamp, dt) or not isinstance(next_timestamp, dt):
            raise TypeError('Timestamps is not a datetime object')

        if next_timestamp < timestamp:
            raise ValueError('Next timestamp cannot be before current timestamps')

        diff = (next_timestamp - timestamp).total_seconds()
        
        if 1 <= diff <= 5: return 'SP', diff # 1 -> 5
        elif 5 < diff <= 15: return 'MP', diff # 6 -> 15
        elif 15 < diff <= 30: return 'LP', diff # 16 -> 30
        elif diff > 30: return 'VLP', diff # more than 30
        else: return 0, diff # base case
