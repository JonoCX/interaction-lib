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

        self._statistics = {user: {} for user, d in self.data.items()}
        self._time_statistics = {}

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
            return (timestamps[-1] - timestamps[0]).total_seconds()
            # session_lengths = {}
            # for u, ts in timestamps.items():
            #     session_lengths[u] = (ts[-1] - ts[0]).total_seconds()
            # return session_lengths

        def _calculate_hidden_time(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            return None

        if not self._time_statistics:
            self._time_statistics = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu)

            timestamps = {}
            for ts in parallel(delayed(_get_timestamps) (u, e) for u, e in self._users_split):
                timestamps.update(ts)

            # calculate the raw session length from the timestamps
            for u, ts in timestamps.items():
                self._time_statistics[u].update({'raw_session_length': _get_session_lengths(ts)})

            return self._time_statistics
        else:
            return self._time_statistics
        #     for user, events in self.data.items():

