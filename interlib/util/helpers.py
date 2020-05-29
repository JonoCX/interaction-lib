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
    return 0 # couldn't find the issue

def missing_hidden_visibility_change(visbile_ts, current_index, events):
    # nothing we can do at the start of the list
    if current_index == 0: return 0 

    pseudo_hidden_ts = events[current_index - 1]['timestamp']
    
    return (visbile_ts - pseudo_hidden_ts).total_seconds()

