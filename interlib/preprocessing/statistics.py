""" """

from .base import BaseExtractor
from ..util import get_hidden_time, missing_hidden_visibility_change

from joblib import Parallel, delayed, cpu_count
from datetime import datetime as dt
from collections import Counter, defaultdict
from typing import Optional, Union, List, Set, Dict

import numpy as np 
import pandas as pd 

np.random.seed(42)

class Statistics(BaseExtractor):
    
    def __init__(
        self, 
        user_event_dict: Dict[str, List[Dict]], 
        completion_point: Optional[str] = None,
        n_jobs: Optional[int] = -1
    ) -> None:
        if not isinstance(user_event_dict, dict):
            raise TypeError('User Event dictionary is not a dict')

        if len(user_event_dict) == 0:
            raise ValueError('User event dictionary must have at least one value or not None')

        if completion_point and not isinstance(completion_point, str):
            raise TypeError('completion_point should be a str')

        if not isinstance(n_jobs, int):
            raise TypeError('n_jobs should be an int')
        
        super(Statistics, self).__init__(
            user_event_dict = user_event_dict.copy(),
            completion_point = completion_point,
            n_jobs = n_jobs
        )

        self._statistics = {}
        self._time_statistics = {}
        self._pause_statistics = {}
        self._event_statistics = {}
        self._user_event_frequencies = {}

    def calculate_time_statistics(
        self, 
        verbose: Optional[int] = 0, 
        user_id: Optional[str] = None
    ) -> Dict:
        """ 
            Using the user event data, calculate the hidden time, raw
            session length, and session length of the users.

            :params verbose: passed to the joblib backend
            :params user_id: a specific user to get the statistics for
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
                hidden_time_completion_point = None 
                timestamp_reached_completion_point = None
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

                    # time of completion
                    if self.completion_point:
                        if (event['action_type'] == 'STORY_NAVIGATION' and 
                            event['data']['romper_to_state'] == self.completion_point and
                            self._users_reached_completion_point[user]):
                            hidden_time_completion_point = np.sum(hidden_times)
                            timestamp_reached_completion_point = event['timestamp']

                # record the sum of the hidden times.
                results[user].update({'hidden_time': np.sum(hidden_times)})

                # time to completion statistics
                if self.completion_point:
                    if not self._users_reached_completion_point[user]:
                        results[user].update({'time_to_completion': 0.0})
                    else:
                        # have to take into account the hidden time.
                        raw_time = (timestamp_reached_completion_point - timestamps[user][0]).total_seconds()
                        results[user].update({'time_to_completion': raw_time - hidden_time_completion_point})

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

    def calculate_session_length(
        self, 
        user_id: Optional[str] = None, 
        verbose: Optional[int] = 0
    ) -> Dict:
        """ 
            Calculate the session length only.

            :params user_id: specify a user, default is to calculate
            for all users
            :params verbose: verbosity passed to the joblib backend 
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

    def _pause_counts(
        self, 
        events: list
    ) -> Dict[str, int]:
        """
            Given a set of user actions, count the number of
            pauses that occur.

            :params events: the user events
        """
        pauses = []
        previous_timestamp = None
        for event in events:
            # we're only counting pauses between user events, ignoring
            # variable setting and link choices
            if (event['action_type'] == 'USER_ACTION' and
                event['action_name'] != 'USER_SET_VARIABLE' and
                event['action_name'] != 'LINK_CHOICE_CLICKED'):
                if previous_timestamp is None: # no previous, set first ts
                    previous_timestamp = event['timestamp']

                # get the type of pause, if any
                pause_type, diff = self._type_of_pause(previous_timestamp, event['timestamp'])

                if pause_type != 0: # if there is a pause
                    pauses.append(pause_type)

                # update to the current
                previous_timestamp = event['timestamp']

        # get a count of the pauses
        pauses = Counter(pauses)
        return {
            'SP': pauses['SP'], 'MP': pauses['MP'],
            'LP': pauses['LP'], 'VLP': pauses['VLP']
        }
        
    def calculate_pause_statistics(
        self, 
        verbose: Optional[int] = 0, 
        user_id: Optional[str] = None
    ) -> Dict[str, Dict]:
        """ 
            Based on the event data supplied, calculate the pause
            statistics for each of the users.

            :params verbose: verbosity level passed to the joblib backend
            :params user_id: a specific user to calculate the statistics for
            :returns: a dictionary with a mapping from user to statistics
        """
        def _get_pauses(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # if the user has no events
                    results[user].update({'SP': 0, 'MP': 0, 'LP': 0, 'VLP': 0})
                    continue

                results[user].update(
                    self._pause_counts(events)
                )
            
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
        interaction_events: List[str],
        include_link_choices: Optional[bool] = False,
        include_user_set_variables: Optional[bool] = False,
        verbose: Optional[int] = 0,
        user_id: Optional[str] = None
    ) -> Dict[str, Dict[str, int]]:
        """ 
        
        :params interaction_events: all of the events that should be counted
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
        frequencies: List[Union[int, float]],
        interaction_events: List[str], 
        user_id: Optional[str] = None, 
        include_pauses: Optional[bool] = False,
        verbose: Optional[int] = 0
    ) -> Dict[str, Dict[str, Dict[str, int]]]:
        """ 
        From a list of events and give a set of time thresholds,
        calculate the frequency that events happen in those periods.
        
        :params frequencies: a list of seconds as integers that you want
        to capture event frequencies for, e.g. [0, 60, 120, 180] would indicate that
        you want event frequencies for minutes 0 to 1, 1 to 2, and 2 to 3.
        :params interaction_events: a set of events that you want to capture
        frequencies for.
        :params user_id: a specific user to capture event frequencies for.
        :params verbose: the amount of std out (passed to joblib backend)
        :returns: a dictionary mapping users to an inner dictionary containing
        a mapping of time thresholds and the count of the interaction_events
        in that time threshold
        """
        def _subset(min_threshold, max_threshold, events, previous_subset_ids):
            events_subset = []
            elapsed_time = 0
            events_beyond_max_frequency = False

            previous_ts = None
            seen_hidden = False
            missing_visibility_change = False
            for idx, event in enumerate(events):
                # if it's the first loop, previous ts will be none
                if idx == 0: previous_ts = event['timestamp']

                hidden = 0 # record amount of time hidden, to subtract later
                if (event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and
                    event['data']['romper_to_state'] == 'hidden'):
                    hidden_ts = event['timestamp']

                    hidden = get_hidden_time(hidden_ts, idx, events)
                    seen_hidden = True
                elif (event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and 
                    event['data']['romper_to_state'] == 'visible' and
                    not seen_hidden):
                    visible_ts = event['timestamp']

                    # find the missing hidden visibility change
                    hidden = missing_hidden_visibility_change(visible_ts, idx, events)
                    seen_hidden = False # ensure that this is false
                    
                    # flag that this data querk has happened
                    missing_visibility_change = True 
                
                # update the elapsed time and subtract any hidden time
                elapsed_time += (event['timestamp'] - previous_ts).total_seconds()
                elapsed_time -= hidden
            
                between_threshold = min_threshold <= elapsed_time < max_threshold

                # if the elapsed time is between the min and max threshold
                if (between_threshold and event['id'] not in previous_subset_ids):
                    events_subset.append(event)
                # else if the value isn't between the threshold, the data querk happened
                # and the event hasn't been previously seen
                elif (not between_threshold and missing_visibility_change and event['id'] not in previous_subset_ids):
                    events_subset.append(event)
                    missing_visibility_change = False

                previous_ts = event['timestamp']

                # condition two: check if the user has events beyond the maximum threshold
                if elapsed_time < max_threshold:
                    events_beyond_max_frequency = True

            return events_subset, events_beyond_max_frequency

        def _get_frequencies(user_chunk, data_chunk):
            user_dict = {user: [] for user in user_chunk}
            for d in data_chunk: user_dict[d['user']].append(d)

            results = {user: {} for user in user_chunk}

            for user, events in user_dict.items():
                if len(events) < 1: # the user has no events
                    continue
                
                subset_ids = set([])
                for idx, i in enumerate(range(len(frequencies) - 1)):
                    event_subset, events_beyond_max_freq = _subset(
                        frequencies[i], frequencies[i + 1], events,
                        previous_subset_ids = subset_ids
                    )

                    # ids in subset
                    subset_ids.update([ev['id'] for ev in event_subset])
                
                    """ 
                    Two exit conditions:
                        1) the user has no events left
                        2) they have events but are beyond the current
                        max frequency. 
                    """
                    # if the length is zero and there's no events beyond the current
                    # max frequency (frequencies[i + 1])
                    if (len(event_subset) == 0 and not events_beyond_max_freq):
                        break # there's no more events

                    ua_counter = defaultdict(int) # counter for all events
                    
                    # set the default for each of the events
                    for event in interaction_events: ua_counter[event] = 0

                    for event in event_subset: # ignoring segmentCompletions events
                        if event['action_type'] == 'segmentCompletion': continue
                        if event['action_name'] in interaction_events:
                            ua_counter[event['action_name']] += 1

                    # if pauses need to be included
                    if include_pauses:
                        for pause in ['SP', 'MP', 'LP', 'VLP']:
                            ua_counter[pause] = 0

                        for pause, count in self._pause_counts(event_subset):
                            ua_counter[pause] = count

                    # need to drop the first play pause, it always happens at the start
                    if idx == 0 and ua_counter['PLAY_PAUSE_BUTTON_CLICKED'] != 0:
                        ua_counter['PLAY_PAUSE_BUTTON_CLICKED'] -= 1

                    results[user][str(frequencies[i]) + '_' + str(frequencies[i + 1])] = dict(ua_counter)

            return results
        
        if not isinstance(frequencies, list):
            raise TypeError('Event Frequencies should be a list of second intervals: {0} ({1}'.format(
                frequencies, type(frequencies)    
            ))
        
        if len(frequencies) == 0:
            raise ValueError('Event frequencies cannot be an empty list: {0}'.format(frequencies))

        if not all(isinstance(x, (int, float)) for x in frequencies):
            raise TypeError('Contents of event frequencies are not ints or floats.')

        if not self._user_event_frequencies:
            if user_id is not None: # if a specific user is requested
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0} ({1})'.format(user_id, type(user_id)))

                if user_id not in self.data.keys():
                    raise ValueError('Invalid user ID: {0}'.format(user_id))

                return _get_frequencies(user_chunk = [user_id], data_chunk = self.data[user_id])[user_id]

            self._user_event_frequencies = {user: {} for user, d in self.data.items()}
            parallel = Parallel(n_jobs = self._num_cpu, verbose = verbose)

            # run the event frequencies in parallel
            results = parallel(delayed(_get_frequencies) (u, e) for u, e in self._users_split)

            # unpack the results and add the frequencies into the dictionary
            for res in results:
                for user, event_freq in res.items():
                    self._user_event_frequencies[user].update(event_freq)

            return self._user_event_frequencies 
        else:
            if user_id is not None: # if a specific user is requested
                if not isinstance(user_id, str):
                    raise TypeError('User ID should be a string: {0} ({1})'.format(user_id, type(user_id)))

                if user_id not in self.data.keys():
                    raise ValueError('Invalid User ID: {0}'.format(user_id))

                return self._user_event_frequencies[user_id]
            return self._user_event_frequencies

            
    def calculate_statistics(
        self, 
        interaction_events: List[str],
        user_id: Optional[str] = None,
        include_link_choices: Optional[bool] = False,
        include_user_set_variables: Optional[bool] = False,
        verbose: Optional[int] = 0
    ) -> Dict[str, Dict[str, Union[int, float]]]:
        """ 
            The main function for calculating all statistics, excluding the 
            event frequencies.

            :params interaction_events: a list of events that you want to
            track in the statistics
            :params user_id: a specific user to calculate statistics for
            :params include_link_choices: whether to include LC in the statistics
            :params include_user_set_variables: whether to include USV in the statistics
            :params verbose: verbosity level passed to joblib backend
            :returns: a dictionary containing a mapping from users to their
            respective statistics.
        """

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

