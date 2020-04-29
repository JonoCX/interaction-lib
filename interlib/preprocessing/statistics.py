""" """

from .base import BaseExtractor
from ..util import get_hidden_time

from joblib import Parallel, delayed, cpu_count
from datetime import datetime as dt
from collections import Counter, defaultdict

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

        self._statistics = {}
        self._time_statistics = {}
        self._pause_statistics = {}
        self._event_statistics = {}
        self._user_event_frequencies = {}

    def calculate_time_statistics(self, verbose = 0, user_id = None):
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
                    
                    # set hidden time/raw to 0.0
                    results[user].update({'hidden_time': 0.0, 'raw_session_length': 0.0}) 
                    continue # move onto the next user

                timestamps[user] = [event['timestamp'] for event in events] # collect timestamps

                hidden_times = []
                # for all events, if there is a visibility change to hidden
                for index, event in enumerate(events): 
                    if (event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and 
                        event['data']['romper_to_state'] == 'hidden'):
                        hidden_ts = event['timestamp'] # record the timestamp

                        if (index + 1) == len(events): break # if it's at the end, exit

                        visible_ts = None
                        if (events[index + 1]['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and
                            events[index + 1]['data']['romper_to_state'] == 'visible'):
                            visible_ts = events[index + 1]['timestamp']
                        else: # otherwise, loop forward in the event to find next BVC
                            for f_index, f_event in enumerate(events[index:]): 
                                if (f_event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and 
                                    f_event['data']['romper_to_state'] == 'visible'):
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

        if not self._time_statistics: # we've not already calculated
            if user_id is not None: # if the user is wanting a specific user
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a String: {0}'.format(user_id))
                
                if user_id not in self.data.keys():
                    raise ValueError('Invalid User ID: {0}'.format(user_id))

                # calculate the statistics for that user
                return _get_stats(user_chunk = [user_id], data_chunk = self.data[user_id])[user_id]

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
            if user_id is not None: # if the user is wanting a specific user
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0}'.format(user_id))
            
                if user_id not in self._time_statistics.keys() or user_id not in self.data.keys():
                    raise ValueError('Invalid User ID: {0}'.format(user_id))
                
                # retrieve the statistics for that user.
                return self._time_statistics[user_id]
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
        
    def calculate_pause_statistics(self, verbose = 0, user_id = None):
        """ """
        def _get_pauses(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # if the user has no events
                    results[user].update({'SP': 0, 'MP': 0, 'LP': 0, 'VLP': 0})
                    continue

                pauses = []
                previous_timestamp = None
                for event in events:
                    # we only count pauses between user events, ignored variable 
                    # setting and link choices
                    if (event['action_type'] == 'USER_ACTION' and 
                        event['action_name'] != 'USER_SET_VARIABLE' and 
                        event['action_name'] != 'LINK_CHOICE_CLICKED'):
                        if previous_timestamp is None:
                            previous_timestamp = event['timestamp'] # no previous, first iteration
                        
                        # get the type of pause
                        pause_type, diff = self._type_of_pause(previous_timestamp, event['timestamp'])
                       
                        if pause_type != 0: # there is a pause
                            pauses.append(pause_type)

                        previous_timestamp = event['timestamp'] # update to the current
                
                pauses = Counter(pauses)
                results[user].update({
                    'SP': pauses['SP'], 'MP': pauses['MP'], 'LP': pauses['LP'],
                    'VLP': pauses['VLP']
                })
            
            return results

        # if the statistics haven't been previously calculated
        if not self._pause_statistics:
            if user_id is not None: # if the user is asking for the stats of a specific user
                if not isinstance(user_id, str):
                    raise TypeError('User Id should be a string: {0}'.format(user_id))

                if user_id not in self.data.keys():
                    raise ValueError('Invalid user id: {0}'.format(user_id))
                
                # calculate the pause statistics for that individual (non-parallel)
                return _get_pauses(user_chunk = [user_id], data_chunk = self.data[user_id])[user_id]

            self._pause_statistics = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            # run the pause statistics job in parallel
            res = parallel(delayed(_get_pauses) (u, e) for u, e in self._users_split)

            # unpack the results and add to the pause statistics dictionary
            for r in res:
                for u, p in r.items():
                    self._pause_statistics[u].update(p)

            return self._pause_statistics
        else:
            if user_id is not None:
                if not isinstance(user_id, str):
                    raise TypeError('User Id should be a string: {0}'.format(user_id))

                if user_id not in self._pause_statistics.keys() or user_id not in self.data.keys():
                    raise ValueError('Invalid user id: {0}'.format(user_id))

                return self._pause_statistics[user_id]
            return self._pause_statistics

    def calculate_event_statistics(
        self,
        interaction_events,
        include_link_choices = False,
        include_user_set_variables = False,
        verbose = 0,
        user_id = None):
        """ 
        
        :params event_mapping: all of the events that should be counted
        :params include_link_choices: whether to include LC in the total count
        :params include_user_set_variable: whether to include USV in the total count
        :params verbose: the level of output passed to the joblib backend
        :params user_id: the specific user to fetch statistics on
        :returns: event-based statistics (dictionary)
        """
        def _event_stats(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # if the user has no events
                    results[user].update({ev: 0 for ev in interaction_events})
                    continue

                ua_counter = defaultdict(int) # counter for all events

                # set the default for each of the events
                for event in interaction_events: ua_counter[event] = 0

                for event in events:
                    if event['action_name'] in interaction_events:
                        ua_counter[event['action_name']] += 1
                
                # subtract one from PLAY_PAUSE, there's always one at the beginning and
                # only if the value is not 0
                if ua_counter['PLAY_PAUSE_BUTTON_CLICKED'] != 0:
                    ua_counter['PLAY_PAUSE_BUTTON_CLICKED'] -= 1

                # calculate the total number of events
                total_events = sum(ua_counter.values())

                if not include_link_choices:
                    total_events -= ua_counter['LINK_CHOICE_CLICKED']
                
                if not include_user_set_variables:
                    total_events -= ua_counter['USER_SET_VARIABLE']

                results[user].update(dict(ua_counter))
                results[user].update({'total_events': total_events})
                    
            return results 

        # check that the interaction events is a set
        if not isinstance(interaction_events, set):
            raise TypeError('Interaction events should be a set of actions: {0}'.format(interaction_events))
    
        # check that the interaction events set contains something
        if len(interaction_events) == 0:
            raise ValueError('Interaction events cannot be empty: {0}'.format(interaction_events))

        if not self._event_statistics:
            if user_id is not None: # if a specific user is requested
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0}'.format(user_id))
            
                if user_id not in self.data.keys():
                    raise ValueError('Invalid user ID: {0}'.format(user_id))

                # calculate the event statistics for that user
                return _event_stats(user_chunk = [user_id], data_chunk = self.data[user_id])[user_id]
            
            self._event_statistics = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            # run the event extract in parallel
            results = parallel(delayed(_event_stats) (u, e) for u, e in self._users_split)

            # unpack the results and add to the event statistics dictionary
            for res in results:
                for user, event_stats in res.items():
                    self._event_statistics[user].update(event_stats)

            return self._event_statistics
        else:
            if user_id is not None: # if a specific user is requested
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0}'.format(user_id))
            
                if user_id not in self.data.keys():
                    raise ValueError('Invalid user ID: {0}'.format(user_id))

                return self._event_statistics[user_id]
            return self._event_statistics

    def calculate_event_frequencies(
        self, 
        frequencies_to_capture,
        interaction_events, 
        user_id = None, 
        verbose = 0):
        """ 
        
        :params frequencies_to_capture: a list of minutes as integers that you want
        to capture event frequencies for, e.g. [0, 1, 2, 3] would indicate that
        you want event frequencies for minutes 0 to 1, 1 to 2, and 2 to 3.
        :params interaction_events: a set of events that you want to capture
        frequencies for.
        :params user_id: a specific user to capture event frequencies for.
        :params verbose: the amount of std out (passed to joblib backend)
        :returns: ...TODO
        """
        def _subset(min_threshold, max_threshold, events, user = None):
            events_subset = []
            elapsed_time = 0

            previous_ts = None
            for idx, event in enumerate(events):
                # if it's the first loop, previous ts will be none
                if idx == 0: previous_ts = event['timestamp']

                hidden = 0 # record amount of time hidden, to subtract later
                if (event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and
                    event['data']['romper_to_state'] == 'hidden'):
                    hidden_ts = event['timestamp']

                    hidden = get_hidden_time(hidden_ts, idx, events) * 60
                
                elapsed_time += (event['timestamp'] - previous_ts).total_seconds() * 60
                elapsed_time -= hidden

                if user == '959c1a91-8b0f-4178-bc59-70499353204f':
                    print(elapsed_time)

                # if the elapsed time is between the min and max threshold
                if min_threshold <= elapsed_time < max_threshold:
                    events_subset.append(event)

                previous_ts = event['timestamp']

            return events_subset

        def _get_frequencies(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # the user has no events
                    continue

                for i in range(len(frequencies_to_capture) - 1):
                    event_subset = _subset(frequencies_to_capture[i], frequencies_to_capture[i + 1], events, user = user)
                
                    if len(event_subset) == 0:
                        continue

                    if user == '959c1a91-8b0f-4178-bc59-70499353204f':
                        print('\n', frequencies_to_capture[i], ' - ', frequencies_to_capture[i + 1])
                        print([(event['timestamp'], event['action_name']) for event in event_subset])

                    ua_counter = defaultdict(int) # counter for all events
                    
                    # set the default for each of the events
                    for event in interaction_events: ua_counter[event] = 0

                    for event in event_subset:
                        if event['action_name'] in interaction_events:
                            ua_counter[event['action_name']] += 1

                    results[user][str(frequencies_to_capture[i]) + '_' + str(frequencies_to_capture[i + 1])] = dict(ua_counter)

            return results
        
        # TODO add in the errors, etc.

        if not self._user_event_frequencies:
            self._user_event_frequencies = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            # run the event frequencies in parallel
            results = parallel(delayed(_get_frequencies) (u, e) for u, e in self._users_split)

            # unpack the results and add the frequencies into the dictionary
            for res in results:
                for user, event_freq in res.items():
                    self._user_event_frequencies[user].update(event_freq)

            return self._user_event_frequencies 

            
    def calculate_statistics(
        self, 
        interaction_events,
        user_id = None,
        include_link_choices = False,
        include_user_set_variables = False,
        verbose = 0):
        """ Main function for calculating the statistics: Imp last. """

        # check that the interaction events is a set
        if not isinstance(interaction_events, set):
            raise TypeError('Interaction events should be a set of actions: {0} ({1})'.format(
                interaction_events, type(interaction_events))
            )

        if len(interaction_events) == 0:
            raise ValueError('Interaction events cannot be empty: {0}'.format(interaction_events))

        # test that the values in a set are of type string, the process won't work with int/float
        if not all(isinstance(x, str) for x in interaction_events):
            raise TypeError('Contents of interaction_events is not string')
        
        if not self._statistics: # haven't been previously calculated
            if user_id is not None: # if statistics for a single user is requested
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0} ({1})'.format(user_id, type(user_id)))

                if user_id not in self.data.keys():
                    raise ValueError('Invalid User ID: {0} ({1})'.format(user_id, type(user_id)))

                # return a dict of results = {total_events: 24, pp: 1, etc..}
                individual_results = {
                    **self.calculate_time_statistics(user_id = user_id, verbose = verbose),
                    **self.calculate_pause_statistics(user_id = user_id, verbose = verbose),
                    **self.calculate_event_statistics(
                        interaction_events, user_id = user_id,
                        include_link_choices = include_link_choices,
                        include_user_set_variables = include_user_set_variables, 
                        verbose = verbose
                    )
                }
                
                return individual_results

            # ---- The below may not be the most optimal approach -----

            # calculate all of the statistics
            self.calculate_time_statistics(verbose = verbose)
            self.calculate_pause_statistics(verbose = verbose)
            self.calculate_event_statistics(
                interaction_events = interaction_events,
                include_link_choices = include_link_choices,
                include_user_set_variables = include_user_set_variables,
                verbose = verbose
            )

            self._statistics = { # build up the statistics dictionary
                user: { # for each of the users, get their statistics (O(1))
                    **self.calculate_time_statistics(user_id = user), 
                    **self.calculate_pause_statistics(user_id = user),
                    **self.calculate_event_statistics(
                        user_id = user, interaction_events = interaction_events
                    )
                } for user, d in self.data.items()
            }

            return self._statistics
        else: # else, the statistics have been previously calculated
            if user_id is not None: # if it's for a specific user
                if not isinstance(user_id, str): 
                    raise TypeError('User ID should be a string: {0} ({1})'.format(user_id, type(user_id)))
            
                if user_id not in self.data.keys():
                    raise ValueError('Invalid User ID: {0} ({1})'.format(user_id, type(user_id)))

                return self._statistics[user_id]

            return self._statistics

