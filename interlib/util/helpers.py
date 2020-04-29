"""
Helper functions for processing the data
"""

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
