""" """

from .base import BaseExtractor
from abc import ABCMeta
from collections.abc import Mapping
from joblib import Parallel, delayed, cpu_count

import numpy as np 
import pandas as pd 

np.random.seed(42)

class Statistics(BaseExtractor):
    
    def __init__(self, user_event_dict, n_jobs = -1):
        if not isinstance(user_event_dict, dict):
            raise TypeError('User Event dictionary is not a dict')

        if len(user_event_dict) == 0:
            raise ValueError('User event dictionary must have at least one value or not None')
        
        super(Statistics, self).__init__(
            user_event_dict = user_event_dict,
            n_jobs = n_jobs
        )

        self._timestamps = None
        self._session_lengths = None

    def calculate_session_length(self, user_id = None):
        """ 
            Calculate the session length only.

            :params user_id: specify a user, default is to calculate
            for all users 
            :returns: the session length or 
            all session lengths as dictionary {user -> session length}
        """
        return None

    def calculate_session_length_statistics(self):
        """ 
            Given the dictionary of user events, calculate the session
            length statistics for each users.

            :returns: dictionary with user ids mapped to a inner dictionary
            of session length, session_length_minutes, raw_session_length,
            and hidden_time
        """
        def _get_timestamps(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            timestamps = {}
            for user, data in user_dict.items():
                timestamps[user] = [event['timestamp'] for event in data]
            return timestamps

        def _get_session_lengths(timestamps):
            session_lengths = {}
            for u, ts in timestamps.items():
                session_lengths[u] = (ts[-1] - ts[0]).total_seconds()
            return session_lengths

        def _calculate_hidden_time(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            return None
        
        parallel = Parallel(n_jobs = self._num_cpu)
        
        if not self._timestamps:
            timestamps = parallel(delayed(_get_timestamps) (u, e) for u, e in self._users_split)
            
            self._timestamps = {}
            for ts in timestamps:
                self._timestamps.update(ts)

        if not self._session_lengths:
            self._session_lengths = _get_session_lengths(self._timestamps)

        return None
        #     for user, events in self.data.items():

