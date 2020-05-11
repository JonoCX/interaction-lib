""" 
Utility functions for processing raw data.
"""
from .helpers import parse_raw_data

from typing import (
    Optional,
    Union, 
    List,
    Dict,
    Set
)
from datetime import datetime as dt

import json, os
import numpy as np

def to_dict(
    path: str, 
    split: Optional[Union[bool, int]] = None,
    datetime_format: Optional[str] = "%Y-%m-%d %H:%M:%S.%f",
    include_narrative_element_id: Optional[bool] = False,
    sort: Optional[bool] = False,
    users_to_include: Optional[Set[str]] = None
) -> Union[Dict[str, List], List[Dict[str, List]]]:
    """
        Utility function to convert a raw dataset (in a json export from DB
        format) to the format that is internally used: {user -> events}

        :params path: the path to the data file (json)
        :params split: whether the data should be split (into 2, default) or
        the number of splits requested
        :params datetime_format: the format for the timestamp, compatiable with
        datetime
        :params include_narrative_element: whether to include narrative element
        changes
        :params sort: whether or not to sort the data by the timestamp.
        :returns: dictionary of values: {user -> events} or, if split, then
        a list of dictionaries in [{user -> events}] format
        TODO: only read in a select set of users.
    """
    if not isinstance(path, str):
        raise TypeError('Path is not a string: {0} ({1})'.format(path, type(path)))

    if not os.path.isfile(path):
        raise ValueError('File does not exist: {0}'.format(path))

    if split and not isinstance(split, (bool, int)):
        raise TypeError('Split must be a bool (uses default split of 2) or int ',
                        '(the number of splits): {0} ({1})'.format(split, type(split)))
    
    if not isinstance(include_narrative_element_id, bool):
        raise TypeError('include_narrative_element_id is not a bool: {0} ({1})'.format(
            include_narrative_element_id, type(include_narrative_element_id)    
        ))

    if not isinstance(sort, bool):
        raise TypeError('sort is not a bool: {0} ({1})'.format(sort, type(sort)))

    with open(path, 'r') as in_file: # read in the data provided
        data = parse_raw_data( # parse into our internal format at the same time
            json.load(in_file), 
            datetime_format, 
            include_narrative_element_id
        )
    
    if split:
        if isinstance(split, bool): # if it's a bool
            split = 2 # then just use 2 as the default
    
        # create a list of all user ids
        if users_to_include: # if we're looking for a subset
            user_ids = []
            for event in data:
                if event['user'] in users_to_include:
                    user_ids.append(event['user'])
        else: # otherwise, it's everyone
            user_ids = [event['user'] for event in data]

        # partition the user id's into the split value
        split_users = np.array_split(user_ids, split)
        split_list = []
        for item in split_users:
            split_list.append(set(item))
        
        main_list = []
        for part in split_list:
            segment = []

            for event in data:
                if event['user'] in part:
                    segment.append(event)

            main_list.append(segment)

        # transform into the {user -> events} format
        events = []
        for d in main_list:
            # get all of the users in the segment and build a user event dict
            user_ids = {event['user'] for event in d}
            user_events = {id: [] for id in user_ids}

            for event in d: # for each event
                if event['user'] in user_ids: # if that user is in this segment
                    user_events[event['user']].append(event)
            
            if sort: # if sort, then sort by timestamp
                for user, event in user_events.copy().items():
                    user_events[user] = sorted(events, key = lambda x: x['timestamp'])
            
            # build the returned list
            events.append(user_events)

        return events
    else:
        if users_to_include:
            user_ids = {
                event['user'] for event in data if event['user'] in users_to_include
            }
        else:
            user_ids = {event['user'] for event in data}

        # build up the user events dict {user -> [events]}
        user_events = {id: [] for id in user_ids}
        for event in data:
            if event['user'] in user_ids:
                user_events[event['user']].append(event)
        
        if sort:
            # sort the events by the timestamp
            for user, events in user_events.copy().items():
                user_events[user] = sorted(events, key = lambda x: x['timestamp'])

        return user_events
