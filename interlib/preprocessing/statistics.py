""" """

from .base import BaseExtractor
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
            user_event_dict = user_event_dict.copy(),
            n_jobs = n_jobs
        )

        self._statistics = {user: {} for user, d in self.data.items()}
        self._time_statistics = {}

    def calculate_time_statistics(self, verbose = 0):
        """ 
            Using the user event data, calculate the hidden time, raw
            session length, and session length of the users.

            :params verbose: passed to the joblib backend
            :returns: dictionary of results {user -> {hidden_time: 0...}}
        """
        def _get_stats(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            timestamps = {}
            no_events_set = set([])
            # calculate the hidden time (and get the timestamps)
            for user, events in user_dict.items():
                if len(events) < 1: # if there are no events
                    no_events_set.add(user) # essentially set a flag per user
                    results[user].update({'hidden_time': 0.0, 'raw_session_length': 0.0}) # set hidden time/raw to 0.0
                    continue # move onto the next user

                timestamps[user] = [event['timestamp'] for event in events] # collect timestamps

                hidden_times = []
                for index, event in enumerate(events): # for all events, if there is a visibility change to hidden
                    if event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and event['data']['romper_to_state'] == 'hidden':
                        hidden_ts = event['timestamp'] # record the timestamp

                        if (index + 1) == len(events): break # if it's at the end, exit

                        visible_ts = None
                        for f_index, f_event in enumerate(events[index:]): # otherwise, loop forward in the event to find next BVC
                            if f_event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and f_event['data']['romper_to_state'] == 'visible':
                                visible_ts = f_event['timestamp']
                                break

                        if visible_ts: # if found, calculate the difference
                            hidden_times.append((visible_ts - hidden_ts).total_seconds())
                
                # record the sum of the hidden times.
                results[user].update({'hidden_time': np.sum(hidden_times)})

            # calculate the raw session length
            for user, ts in timestamps.items():
                results[user].update({'raw_session_length': (ts[-1] - ts[0]).total_seconds()})

            for user, res in results.copy().items(): # update the results with the session length
                if user in no_events_set: sess_length = 0.0
                else: sess_length = res['raw_session_length'] - res['hidden_time']
                results[user].update({'session_length': sess_length})

            return results

        if not self._time_statistics: # we've not already calculated or we're being forced to do so
            self._time_statistics = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            # run the process to calculate the statistics
            res = parallel(delayed(_get_stats) (u, e) for u, e in self._users_split)
            
            # unpack the results into the time statistics dictionary
            for r in res:
                for u, s in r.items():
                    self._time_statistics[u].update(s)
                
            return self._time_statistics
        else: # otherwise just return the pre-calculate statistics
            return self._time_statistics

    def calculate_session_length(self, user_id = None, verbose = 0):
        """ 
            Calculate the session length only.

            :params user_id: specify a user, default is to calculate
            for all users 
            :returns: the session length or 
            all session lengths as dictionary {user -> session length}
        """
        if self._time_statistics: # if the statistics have already been written
            if user_id: # if the request is for a single user
                if user_id not in self._time_statistics.keys(): 
                    raise ValueError('Invalid user id (perhaps the user is not in the data)')

                return self._time_statistics[user_id]['session_length']
            return {user: stat['session_length'] for user, stat in self._time_statistics.items()}
        else:
            self.calculate_time_statistics(verbose = verbose)

            if user_id:
                if user_id not in self._time_statistics.keys():
                    raise ValueError('Invalid user id (perhaps the user is not in the data)')
                return self._time_statistics[user_id]['session_length']
            
            return {user: stat['session_length'] for user, stat in self._time_statistics.items()}

    def _type_of_pause(self, timestamp, next_timestamp):
        """ """
        return 0

    def calculate_pause_statistics(self, verbose = 0):
        return None

    def calculate_statistics(self, verbose = 0):
        """ Main function for calculating the statistics: Imp last. """
        return None

