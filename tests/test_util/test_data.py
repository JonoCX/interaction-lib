import pytest 

import json
import pandas as pd
from numpy import delete

from interlib.util.data import to_dict, _get_users_clicked_start_button
from interlib.util.data import to_dataframe, reached_point, events_between_two_points
from interlib.util import parse_raw_data
from interlib.preprocessing.statistics import Statistics

@pytest.fixture
def user_ids():
    return {
        'b194b76c-7866-4b6d-8502-93ffe6322b64', '7b06a205-c793-4bdf-8533-013dc092d341',
        '015879da-4ee5-40c7-8826-5c323a0df742', '9760a350-b073-42de-b86a-3f4cfeecaf6e', 
        'b1728dff-021d-4b82-9afc-8a29264b53e4', '62d860e2-11ec-4a7c-82e2-c9bd3e369c83', 
        '74e368cf-7a39-443d-a3cb-002f6957c8a3', 'be3720be-3da1-419c-b912-cacc3f80a427',
        '0c5b7783-0320-4818-bcb8-e244de363591', 'b4588353-cecb-4dee-ae8b-833d7888dec5', 
        '21013769-f703-4531-9293-f2f4e114c248', '959c1a91-8b0f-4178-bc59-70499353204f'
    }

@pytest.fixture
def data_location(): return 'tests/test_data_files/raw_test_data.json'

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
        'NARRATIVE_ELEMENT_CHANGE', 'LINK_CHOICE_CLICKED',
        'USER_SET_VARIABLE'
    }

def test_to_dict_util(user_ids, data_location):
    user_events = to_dict(data_location)

    # check that the format is correct
    assert isinstance(user_events, dict)
    assert all(isinstance(x, list) for u, x in user_events.items())

    # check that all of the users are in the user events
    assert len(user_ids) == len(user_events.keys())
    for user, events in user_events.items():
        assert user in user_ids
    
def test_to_dict_split_util(user_ids, data_location):
    user_events = to_dict(data_location, split = True)

    # check that the format is correct: [{user -> [events]}, {user -> [events]}]
    assert isinstance(user_events, list)
    for chunk in user_events:
        assert isinstance(chunk, dict)
        assert all(isinstance(x, list) for u, x in chunk.items())

        # check whether they are sorted
        for user, sorted_events in chunk.items():
            assert all(
                sorted_events[i]['timestamp'] <= sorted_events[i + 1]['timestamp'] 
                for i in range(len(sorted_events) - 1)
            )

def test_to_dict_user_subset(user_ids, data_location):
    user_ids = list(user_ids)
    subset_include = user_ids[:len(user_ids) // 2]
    subset_exclude = user_ids[len(user_ids) // 2:]
    user_events = to_dict(data_location, users_to_include = set(subset_include))

    assert len(subset_include) == len(user_events.keys())
    for user, events in user_events.items():
        assert user in subset_include
        assert user not in subset_exclude

def test_get_users_clicked_start_button(data_location, user_ids):
    with open(data_location, 'r') as in_file:
        data = parse_raw_data(
            raw_data = json.load(in_file), 
            datetime_format = "%Y-%m-%d %H:%M:%S.%f", 
            include_narrative_element_id = False
        )
    
    assert len(_get_users_clicked_start_button(data)) == len(user_ids)

def test_get_users_clicked_start_button_users_without_start_button(data_location, user_ids):
    with open(data_location, 'r') as in_file:
        data = parse_raw_data(
            raw_data = json.load(in_file), 
            datetime_format = "%Y-%m-%d %H:%M:%S.%f", 
            include_narrative_element_id = False
        )

    # remove the start button from a couple of the test users
    users_to_remove_start_button = {
        '21013769-f703-4531-9293-f2f4e114c248', 
        '959c1a91-8b0f-4178-bc59-70499353204f'
    }
    
    # get the indexes of the two start button clickes
    del_idx = [
        idx 
        for idx, event in enumerate(data) 
        if event['user'] in users_to_remove_start_button and 
        event['action_name'] == 'START_BUTTON_CLICKED'
    ]

    # delete the selected indexes using the numpy function
    data = delete(data, del_idx).tolist()

    # should be two less users
    assert len(_get_users_clicked_start_button(data)) == len(user_ids) - 2

def test_to_dict_util_errors(data_location):
    # test that a type error is thrown when:
    with pytest.raises(TypeError):
        # a non-string path is passed
        to_dict(path = 150)

        # a non-bool or non-int split value is passed
        to_dict(data_location, split = 3.5)

        # a non bool include narrative element id is passed
        to_dict(data_location, include_narrative_element_id = 1)

        # a non-bool sort is passed
        to_dict(data_location, sort = 1)

    # test that a value error is thrown when:
    with pytest.raises(ValueError):
        # a non-existing path is passed
        to_dict(path = 'foo/bar.json')

# ----- to_dataframe tests ------
def test_to_dataframe(user_ids, data_location, interaction_events):
    user_events = to_dict(data_location)

    stats = Statistics(user_events)
    user_statistics = stats.calculate_statistics(interaction_events)
    df = to_dataframe(user_statistics)

    # check that it's a dataframe that is returned
    assert type(df) == pd.DataFrame

    # check that the columns are the same as the keys
    for col in df.columns:
        if col == 'user': continue # this won't be in the columns
        assert col in user_statistics['b194b76c-7866-4b6d-8502-93ffe6322b64'].keys()

    # check that the values in the cells match for a user
    for stat_name, stat_value in user_statistics['b194b76c-7866-4b6d-8502-93ffe6322b64'].items():
        assert df[df['user'] == 'b194b76c-7866-4b6d-8502-93ffe6322b64'][stat_name].item() == stat_value

# ----- reached point test -----
def test_reached_point(data_location):
    user_events = to_dict(data_location)

    point = 'CH00_Introduction'
    expected_users_removed = 'be3720be-3da1-419c-b912-cacc3f80a427'

    reached_point_users = reached_point(user_events, point)

    assert len(reached_point_users) == len(user_events.keys()) - 1
    assert expected_users_removed not in reached_point_users

def test_reached_point_filter(data_location):
    user_events = to_dict(data_location)

    point = 'CH00_Introduction'
    expected_users_removed = 'be3720be-3da1-419c-b912-cacc3f80a427'

    updated_user_events = reached_point(user_events, point, filter = True)

    assert len(updated_user_events.keys()) == len(user_events.keys()) - 1
    assert expected_users_removed not in updated_user_events.keys()

# ----- events between two points test -----
def test_events_between_two_points(data_location):
    user_events = to_dict(data_location)

    users_reached_end = set([ # determined from mysql query on data
        'b4588353-cecb-4dee-ae8b-833d7888dec5',
        'b1728dff-021d-4b82-9afc-8a29264b53e4',
        '959c1a91-8b0f-4178-bc59-70499353204f',
        'b194b76c-7866-4b6d-8502-93ffe6322b64'
    ])

    start = 'CH00_Introduction'
    end = '09_Ronaldo'

    updated_events = events_between_two_points(user_events, start, end)

    for user, events in updated_events.items():
        if len(events) > 0:
            if user in users_reached_end: # these are users that got to the end point
                assert (events[0]['data']['romper_to_state'] == start and 
                        events[-1]['data']['romper_to_state'] == end)
            else:
                assert events[0]['data']['romper_to_state'] == start


def test_events_between_two_points_alternative_end(data_location):
    user_events = to_dict(data_location)
    users_reached_end = set([ # determined from mysql query on raw data
        '21013769-f703-4531-9293-f2f4e114c248',
        '62d860e2-11ec-4a7c-82e2-c9bd3e369c83'
    ])

    start = 'CH00_Introduction'
    end = '05: Selena'

    updated_events = events_between_two_points(user_events, start, end)

    for user, events in updated_events.items():
        if len(events) > 0:
            if user in users_reached_end: # these are users that got to the end point
                assert (events[0]['data']['romper_to_state'] == start and 
                        events[-1]['data']['romper_to_state'] == end)
            else:
                assert events[0]['data']['romper_to_state'] == start
    

def test_events_between_two_points_filter(data_location):
    user_events = to_dict(data_location)

    start = 'CH00_Introduction'
    end = '09_Ronaldo'

    updated_events = events_between_two_points(user_events, start, end, filter = True)

    # there should be one less user
    assert len(updated_events.keys()) == len(user_events.keys()) - 1

    # we know that this user doesn't get past the start, so they should be in the keys
    assert 'be3720be-3da1-419c-b912-cacc3f80a427' not in updated_events.keys()


def test_events_between_two_points_using_representation_id(data_location):
    user_events = to_dict(data_location)
    users_reached_end = set([ # determined from mysql query on data
        'b4588353-cecb-4dee-ae8b-833d7888dec5',
        'b1728dff-021d-4b82-9afc-8a29264b53e4',
        '959c1a91-8b0f-4178-bc59-70499353204f',
        'b194b76c-7866-4b6d-8502-93ffe6322b64'
    ])

    start = '66f663b2-16ec-4321-abc3-5f582d0649ef' # CH00_Introduction
    end = '0d51a32c-d05c-439b-8317-8b36ef0e6d10' # 09_Ronaldo

    updated_events = events_between_two_points(
        user_events, start, end, using_representation_id = True)

    for user, events in updated_events.items():
        if len(events) > 0:
            if user in users_reached_end: # these are users that got to the end point
                assert (events[0]['data']['current_narrative_element'] == start and 
                        events[-1]['data']['current_narrative_element'] == end)
            else:
                assert events[0]['data']['current_narrative_element'] == start

# TODO: test for parse raw data

