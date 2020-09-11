""" 
Utility functions for processing raw data.
"""

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
import pandas as pd 


def parse_raw_data(
    raw_data: List[Dict], 
    datetime_format: str = "%Y-%m-%d %H:%M:%S.%f", 
    include_narrative_element_id: bool = False
) -> List[Dict]:
    """
        Given a list of raw data, parse it into the format that is used
        to user events.

        :params raw_data: a list of events (dictionaries)
        :params datetime_format: the format to parse the timestamp string
        :params include_narrative_element_id: do you want to include this field
        :returns: data parsed as a list of events
    """
    parsed_data = []

    for datum in raw_data:
        # parse the message data
        if 'message' in datum.keys():
            parse_message = json.loads(datum['message'])
        else:
            parse_message = json.loads(datum['data'])
        nested_data = {}
        for key in parse_message:
            nested_data[key] = parse_message[key]

        # parse the timestamp into a datetime object
        timestamp = datum['timestamp']
        if len(timestamp) < 24: timestamp = timestamp + '.000'
        timestamp = dt.strptime(timestamp[:23], datetime_format)

        p_data = {
            'id': datum['id'], 'user': datum['userid'],
            'timestamp': timestamp, 'action_type': datum['item'],
            'action_name': datum['action'], 'data': nested_data
        }

        if include_narrative_element_id:
            p_data.update({'narrative_element': datum['narrative_element']})

        parsed_data.append(p_data)

    return parsed_data


def parse_timestamp(
    data: List[Dict], 
    datetime_format: str = "%Y-%m-%d %H:%M:%S.%f"
) -> List[Dict]:
    """
        A function to parse the timestamp field into datetime objects

        :params data: un-parsed data, a list of dictionaries
        :params datetime_format: the format for the datetime object
        :returns: updated data parameter
    """
    for event in data:
        timestamp = event['timestamp']
        if len(timestamp) < 24: timestamp = timestamp + '.000'
        event.update(
            (k , dt.strptime(timestamp[:23], datetime_format))
            for k, v in event.items() if k == 'timestamp'
        )
    return data


def _get_users_clicked_start_button(events):
    """
        Fetches the set of users that have clicked the start
        button. This functionality is needed as the users that
        haven't clicked the start button haven't agreed to their
        data being processed.

        :params events: all captured events
        :returns: a set of users that clicked the start button.
    """
    return set([
        event['user'] for event in events if event['action_name'] == 'START_BUTTON_CLICKED'
    ])

def to_dict(
    path: str, 
    split: Optional[Union[bool, int]] = None,
    datetime_format: Optional[str] = "%Y-%m-%d %H:%M:%S.%f",
    include_narrative_element_id: Optional[bool] = False,
    sort: Optional[bool] = True,
    users_to_include: Optional[Set[str]] = None,
    start_button_filter: Optional[bool] = True,
    already_parsed: Optional[bool] = False
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
        :params users_to_include: a subset of user_ids that you want to extract the data for
        :params start_button_filter: only include users that have clicked the Start button, 
            indicating that they have accepted the data collection policy.
        :returns: dictionary of values: {user -> events} or, if split, then
            a list of dictionaries in [{user -> events}] format
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
        if already_parsed:
            data = parse_timestamp(json.load(in_file), datetime_format)
        else:
            data = parse_raw_data( # parse into our internal format at the same time
                json.load(in_file), 
                datetime_format, 
                include_narrative_element_id
            )
    
    if start_button_filter:
        clicked_start_button = _get_users_clicked_start_button(data)

    if split:
        if isinstance(split, bool): # if it's a bool
            split = 2 # then just use 2 as the default
    
        # create a list of all user ids
        if users_to_include: # if we're looking for a subset
            user_ids = []
            for event in data:
                if (event['user'] in users_to_include and event['user'] in clicked_start_button):
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
                for user in user_events.copy().keys():
                    user_events[user] = sorted(user_events[user], key = lambda x: x['timestamp'])


                # for user, event in user_events.copy().items():
                #     user_events[user] = sorted(user_events[user], key = lambda x: x['timestamp'])
            
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

        if start_button_filter:
            clicked_start_button = _get_users_clicked_start_button(data)

        # build up the user events dict {user -> [events]}
        user_events = {id: [] for id in user_ids}

        if start_button_filter:
            for event in data:
                if (event['user'] in user_ids and event['user'] in clicked_start_button):
                    user_events[event['user']].append(event)
        else:
            for event in data:
                if event['user'] in user_ids:
                    user_events[event['user']].append(event)
        
        if sort:
            # sort the events by the timestamp
            for user, events in user_events.copy().items():
                user_events[user] = sorted(events, key = lambda x: x['timestamp'])

        return user_events

def to_dataframe(
    result_dictionary: Dict[str, Dict], 
    key_name: Optional[str] = 'user'
) -> pd.DataFrame:
    """ 
        Given a dictionary of results, from the statistics package,
        convert it into a pandas dataframe format.

        :params result_dictionary: dictionary created as a result of calculating statistics
        :params key_name: what the index should be renamed to when the dataframe is reset
        :returns: pandas DataFrame
    """
    if not isinstance(result_dictionary, dict):
        raise TypeError(f"result_dictionary should be a dictionary and be the output from the " +
                        f"Statistics package, current type: {type(result_dictionary)}")

    return pd.DataFrame.from_dict(
        result_dictionary, orient = 'index'
    ).reset_index().rename(columns = {'index': key_name})

# TODO function to get the set of users that reached a particular
# point in the story (check seen_introduction in stats_time_thresholds.py)

def reached_point(
    user_events: Dict[str, List],
    point: str,
    filter: Optional[bool] = False
) -> Union[Set[str], Dict[str, List]]:
    """
        From a dictionary (user_id mapped to a list of events), find
        which users have passed through a particular point in the experience.

        The function also provides the ability to filter out users 
        that haven't passed through this point. This will return an updated
        version of the user_events parameter

        :params user_events:
        :params point:
        :params filter:
        :returns:
    """
    if not isinstance(user_events, dict): 
        raise ValueError(f"user_events must be a dictionary (current type: {type(user_events)}")
    elif len(user_events) == 0:
        raise ValueError(f"user_events is empty (len = {len(user_events)}")
    elif not all(isinstance(l, list) for l in user_events.values()):
        raise ValueError(f"the values in user_events should be lists of events")

    users_to_remove = set([])
    for user, events in user_events.items():
        # collect the narrative element changes into a list
        ne_changes = [nec for nec in events if nec['action_type'] == 'STORY_NAVIGATION']

        # indicators for: moving to the node and out of the node
        to_state = False 
        from_state = False 

        for nec in ne_changes:
            if nec['data']['romper_to_state'] == point:
                to_state = True 
            if nec['data']['romper_from_state'] == point:
                from_state = True 
        
        # if either are false, then the user didn't pass through this point
        if not to_state or not from_state:
            users_to_remove.add(user)

    # if we're asked to filter users out
    if filter:
        # then we'll remove these users from a copy of the events (avoids mutating the original)
        user_events_copy = user_events.copy()
        for user in users_to_remove:
            user_events_copy.pop(user, None)
        
        return user_events_copy
    
    # otherwise, we need to get the difference and return the users that passed through.
    return set(user_events.keys()) - users_to_remove

def events_between_two_points(
    user_events: Dict[str, List], 
    start_point: str, 
    end_point: str, 
    filter: bool = False,
    using_representation_id: bool = False
) -> Dict[str, List]:
    """
        Given all of the user events, return a filtered set of user events that were
        triggered between two points. This is useful for extracting events that occur
        during the substories of Click.

        TODO: implement the option to use representation ids rather than romper_to_states

        :params user_events: dictionary of user events ({user -> [events]})
        :params start_point: romper_to_state value
        :params end_point: romper_to_state value
        :params filter: remove users with empty event lists (default = False)
        :params using_representation_id: in newer experiences, each node is given a unique
            representation id, if you're passing ids to the start_point and end_point parameters
            then you need to set this to true (default = False)
        :returns: dictionary of user events
    """
    if not isinstance(user_events, dict): 
        raise ValueError(f"user_events must be a dictionary (current type: {type(user_events)}")
    elif len(user_events) == 0:
        raise ValueError(f"user_events is empty (len = {len(user_events)}")
    elif not all(isinstance(l, list) for l in user_events.values()):
        raise ValueError(f"the values in user_events should be lists of events")

    # add all users to dictionary with empty lists
    events_subset = {user: [] for user in user_events.keys()}

    for user, events in user_events.items():
        # find the starting point index
        start_idx = None 
        for idx, event in enumerate(events):
            if (event['action_type'] == 'STORY_NAVIGATION' and 
                event['data']['romper_to_state'] == start_point and 
                not using_representation_id
            ):
                start_idx = idx 
                break
            elif (event['action_type'] == 'STORY_NAVIGATION' and 
                using_representation_id and 
                event['data']['current_narrative_element'] == start_point
            ):
                start_idx = idx 
                break 
        
        # iterate from that index, adding events until you reach the end
        if start_idx is not None:
            for idx, event in enumerate(events[start_idx:]):
                # is the current event the one where they move to the end point?
                if (event['action_type'] == 'STORY_NAVIGATION' and 
                    event['data']['romper_to_state'] == end_point and 
                    not using_representation_id
                ):
                    events_subset[user].append(event) # add the event
                    break # exit, we've reached the endpoint
                elif (event['action_type'] == 'STORY_NAVIGATION' and 
                    using_representation_id and 
                    event['data']['current_narrative_element'] == end_point
                ):
                    events_subset[user].append(event) # add the event 
                    break # exit, we've reached the end point
                else:
                    events_subset[user].append(event)
    
    if filter: # if we're asked to remove empty lists
        return {
            user: events 
            for user, events in events_subset.items() # for all users -> events
            if events # if the list contains some events
        }
    else:
        return events_subset
