"""
    
"""
import pandas as pd 

from .base import BaseExtractor
from .statistics import Statistics
from ..util import to_dataframe

class StatisticalSlices(BaseExtractor):

    def __init__(self, user_events, interaction_events):
        super().__init__(user_events)

        self._slices = []#{user: [] for user in self.data.keys()}
        self._interaction_events = interaction_events
        self._is_sliced = False

    def _window(self, events_arr, indices):
        event_slices = []

        for idx, val in enumerate(indices):
            chunk = []
            if val == indices[-1]: # last element in the list
                chunk = [ev for ev in events_arr[val:]]
            elif idx == 0 and val != 0: # first iteration
                chunk = [ev for ev in events_arr[0:val + 1]]
            else: # other it's in the middle
                chunk = [ev for ev in events_arr[val:indices[idx + 1] + 1]]
            
            event_slices.append(chunk)
        
        return event_slices

    def _get_indices(self, events_arr):
        return [
            idx 
            for idx, ev in enumerate(events_arr) 
            if ev['action_name'] == 'NARRATIVE_ELEMENT_CHANGE'
        ]

    def get_slices(self, as_df = False):
        if self._is_sliced:
            if as_df: return pd.DataFrame(self._slices)
            else: return self._slices

        for user, events in self.data.items():
            indices = self._get_indices(events)
            windows = self._window(events, indices)

            for wind in windows:
                # need to know what the completion point is... 
                if wind[-1]['action_name'] == 'NARRATIVE_ELEMENT_CHANGE':
                    end_point = wind[-1]['data']['romper_to_state']

                    s = Statistics({user: wind}, completion_point = end_point, n_jobs = 1)
                    wind_stats = s.calculate_statistics(
                        self._interaction_events, 
                        include_link_choices = True
                    )
                    wind_stats[user]['abandon'] = False
                    wind_stats[user]['start_nec'] = wind[0]['data']['romper_to_state']
                    wind_stats[user]['end_nec'] = end_point 
                    wind_stats[user]['user'] = user

                    # get the timestamp of the last element because the metrics are recorded
                    # between the first and the last, so it's the metrics that have happened
                    # in the build up to this timestamp
                    wind_stats[user]['timestamp'] = wind[-1]['timestamp']
                else:
                    # abandon
                    s = Statistics({user: wind}, n_jobs = 1)
                    wind_stats = s.calculate_statistics(
                        self._interaction_events,
                        include_link_choices = True 
                    )
                    wind_stats[user]['abandon'] = True 
                    wind_stats[user]['start_nec'] = wind[0]['data']['romper_to_state']
                    wind_stats[user]['end_nec'] = 'abandon'
                    wind_stats[user]['user'] = user

                    # time that the abandon happened.
                    wind_stats[user]['timestamp'] = wind[-1]['timestamp']

                # self._slices[user].append(wind_stats[user])
                self._slices.append(wind_stats[user])
        
        self._is_sliced = True

        if as_df: return pd.DataFrame(self._slices)
        else: return self._slices
        


