from joblib import cpu_count

import numpy as np

class BaseExtractor():
    """ Base class for all of the extractors """
    
    def __init__(self, user_event_dict, completion_point = None, n_jobs = -1):
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
            self._users_reached_completion_point = self.reached_completion_point()

        if self.n_jobs == -1: self._num_cpu = cpu_count()
        else: self._num_cpu = n_jobs

        self._users = set(self.data.keys())
        self._users_split = self.split_users()

    def _sort_events(self, user_event_dict):
        data = {}
        for user, events in user_event_dict.items():
            data[user] = sorted(events, key = lambda x: x['timestamp'])
        return data

    def split_users(self):
        """ """
        split_events = [[] for _ in range(0, self._num_cpu)]
        splits = np.array_split(list(self._users), self._num_cpu)
        for idx, split in enumerate(splits):
            for u in split:
                for e in self.data[u]:
                    split_events[idx].append(e)
        
        return zip(splits, split_events)

    def reached_completion_point(self):
        """ """
        reached_end = {}
        for user, events in self.data.items():
            ne_changes = [change for change in events if change['action_type'] == 'STORY_NAVIGATION']

            for ne_change in ne_changes:
                if ne_change['data']['romper_to_state'] == self.completion_point:
                    reached_end[user] = True 
                    break
            
            if user not in reached_end.keys(): reached_end[user] = False

        return reached_end
