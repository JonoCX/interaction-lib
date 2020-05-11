import pytest 

import json

from interlib.util.data import to_dict

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

def test_to_dict_user_subset(user_ids, data_location):
    user_ids = list(user_ids)
    subset_include = user_ids[:len(user_ids) // 2]
    subset_exclude = user_ids[len(user_ids) // 2:]
    user_events = to_dict(data_location, users_to_include = set(subset_include))

    assert len(subset_include) == len(user_events.keys())
    for user, events in user_events.items():
        assert user in subset_include
        assert user not in subset_exclude

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