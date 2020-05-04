"""
Helper functions for processing the data
"""
from datetime import datetime as dt
import json

############################
# Common statistical tasks
############################

def get_hidden_time(hidden_ts, current_index, events):
    if (current_index + 1) == len(events): return 0

    visible_ts = None
    if (events[current_index + 1]['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and
        events[current_index + 1]['action_name'] == 'visible'):
        visible_ts = events[current_index + 1]['timestamp']
    else:
        for f_idx, f_event in enumerate(events[current_index:]):
            if (f_event['action_name'] == 'BROWSER_VISIBILITY_CHANGE' and 
                f_event['data']['romper_to_state'] == 'visible'):
                visible_ts = f_event['timestamp']
                break

    if visible_ts:
        return (visible_ts - hidden_ts).total_seconds()

def missing_hidden_visibility_change(visbile_ts, current_index, events):
    # nothing we can do at the start of the list
    if current_index == 0: return 0 

    pseudo_hidden_ts = events[current_index - 1]['timestamp']
    
    return (visbile_ts - pseudo_hidden_ts).total_seconds()

#####
##
#####
def parse_raw_data(raw_data, datetime_format, include_narrative_element):
    parsed_data = []

    for datum in raw_data:
        # parse the message data
        parse_message = json.loads(datum['message'])
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

        if include_narrative_element:
            p_data.update({'narrative_element': datum['narrative_element']})

        parsed_data.append(p_data)

    return parsed_data
