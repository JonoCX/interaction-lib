from joblib import cpu_count

import numpy as np

class BaseExtractor():
    """ Base class for all of the extractors """
    
    def __init__(self, user_event_dict, n_jobs = -1):
        self.data = user_event_dict
        self.n_jobs = n_jobs

        if self.n_jobs == -1: self._num_cpu = cpu_count()
        else: self._num_cpu = n_jobs

        self._users = set(self.data.keys())
        self._users_split = self.split_users()

    def split_users(self):
        """ """
        split_events = [[] for _ in range(0, self._num_cpu)]
        splits = np.array_split(list(self._users), self._num_cpu)
        for idx, split in enumerate(splits):
            for u in split:
                for e in self.data[u]:
                    split_events[idx].append(e)
        
        return zip(splits, split_events)
