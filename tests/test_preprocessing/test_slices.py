import pytest 

import pickle, json, datetime 
import numpy as np 
import pandas as pd
import itertools

from interlib.preprocessing import StatisticalSlices

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data_files/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data

@pytest.fixture
def window_data():
    return [
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'NARRATIVE_ELEMENT_CHANGE'],
        ['NARRATIVE_ELEMENT_CHANGE', 'BUTTONS_DEACTIVATED']
    ]

@pytest.fixture
def interaction_events():
    return { # set of user actions we consider
        'PLAY_PAUSE_BUTTON_CLICKED', 'BACK_BUTTON_CLICKED', 
        'FULLSCREEN_BUTTON_CLICKED','NEXT_BUTTON_CLICKED', 
        'SUBTITLES_BUTTON_CLICKED', 'VOLUME_CHANGE',
        'VIDEO_SCRUBBED', 'SEEK_BACKWARD_BUTTON_CLICKED', 
        'SEEK_FORWARD_BUTTON_CLICKED', 'VOLUME_MUTE_TOGGLED', 
        'VARIABLE_PANEL_NEXT_CLICKED', 'VARIABLE_PANEL_BACK_CLICKED',
        'BROWSER_VISIBILITY_CHANGE', 'WINDOW_ORIENTATION_CHANGE',
        'LINK_CHOICE_CLICKED', 'USER_SET_VARIABLE'
    }

@pytest.fixture
def stats_ground_truth():
    with open('tests/test_data_files/test_statistics.json', 'r') as data_in:
        data = json.load(data_in)
    return data

def test_splitting_array(test_data, window_data, interaction_events):
    ss = StatisticalSlices(test_data, interaction_events)
    for user, events in test_data.items():
        nec_indexes = []
        for idx, event in enumerate(events):
            if event['action_name'] == 'NARRATIVE_ELEMENT_CHANGE':
                nec_indexes.append(idx)
        
        assert ss._get_indices(events) == nec_indexes
    

def test_get_slices(test_data, interaction_events, stats_ground_truth):
    ss = StatisticalSlices(test_data, interaction_events)
    ss_slices = ss.get_slices()

    total_session_length = 0
    total_total_events = 0
    for val in ss_slices:
        if val['user'] == '959c1a91-8b0f-4178-bc59-70499353204f':
            total_session_length += val['session_length']
            total_total_events += val['total_events']

    for user, gt in stats_ground_truth.items():
        user_spec = [s for s in ss_slices if s['user'] == user]
        sess_len, hidden_time = 0, 0
        for item in user_spec:
            sess_len += item['session_length']
            hidden_time += item['hidden_time']
        
        assert gt['session_length'] == pytest.approx(sess_len, 0.1)
        assert gt['hidden_time'] == pytest.approx(hidden_time, 0.1)

def test_get_slices_df(test_data, interaction_events):
    ss = StatisticalSlices(test_data, interaction_events)
    ss_slices = ss.get_slices(as_df = True)

    print(ss_slices)
    # dfs = [
    #     pd.DataFrame(val)
    #     for val in ss_slices
    # ]
    # print(pd.concat(dfs))