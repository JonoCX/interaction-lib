import pytest

import pickle, json, datetime

from interlib.preprocessing.statistics import Statistics

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data

@pytest.fixture
def ground_truth():
    with open('tests/test_data/test_statistics.json', 'r') as data_in:
        data = json.load(data_in)
    return data

# ------ INIT ------
def test_init(test_data):
    with pytest.raises(ValueError):
        stats = Statistics(user_event_dict = {})

    with pytest.raises(TypeError):
        stats = Statistics(user_event_dict = [])

    stats = Statistics(test_data)
    assert stats.data == test_data
    assert stats.n_jobs == -1
    
# ----- SPLIT USERS ------
def test_split_users_correct_chunks(test_data):
    # test that the data is split up into the correct chunks
    stats = Statistics(test_data, n_jobs = 4)
    for i, (u_chunk, d_chunk) in enumerate(stats._users_split):
        if i == 0: assert len(u_chunk) == 4
        else: assert len(u_chunk) == 3

def test_split_users_single_array(test_data):
    # test that when the n jobs is 1 that the array remains a single array
    stats = Statistics(test_data, n_jobs = 1)
    for u_chunk, d_chunk in stats._users_split:
        assert len(u_chunk) == len(test_data)

def test_split_users_correspond_to_data_chunk(test_data):
    # test that all of the events in the data chunk correspond to that user
    stats = Statistics(test_data, n_jobs = -1)
    for u_chunk, d_chunk in stats._users_split: 
        # get the set of users in the d chunk and compare with the u_chunk
        users_in_d_chunk = set([e['user'] for e in d_chunk])
        assert len(users_in_d_chunk) == len(u_chunk)
        assert set(u_chunk.tolist()) == users_in_d_chunk

# ----- TIME STATISTICS -----
def test_time_statistics(test_data, ground_truth):
    stats = Statistics(test_data, n_jobs = -1)
    res = stats.calculate_time_statistics()
    
    # test the the statistics calculated (with some allowance in the precision)
    for u, s in ground_truth.items():
        raw_sess_len = res[u]['raw_session_length']
        hidden_time = res[u]['hidden_time']
        sess_len = res[u]['session_length']

        assert s['raw_session_length'] == pytest.approx(raw_sess_len, 0.1)
        assert s['hidden_time'] == pytest.approx(hidden_time, 0.1)
        assert s['session_length'] == pytest.approx(sess_len, 0.1)

def test_time_statistics_empty_events(test_data, ground_truth):
    """ Test that empty events are dealt with """
    test_data_copy = test_data.copy()
    user_to_delete = list(test_data_copy.keys())[0]
    test_data_copy[user_to_delete] = [] # remove their events

    stats = Statistics(test_data_copy)
    res = stats.calculate_time_statistics()

    assert res[user_to_delete]['raw_session_length'] == 0.0
    assert res[user_to_delete]['hidden_time'] == 0.0
    assert res[user_to_delete]['session_length'] == 0.0

def test_that_statistics_are_not_recalculated(test_data):
    stats = Statistics(test_data)
    res_one = stats.calculate_time_statistics()
    res_two = stats.calculate_time_statistics()

    assert res_one == res_two

def test_getting_only_session_length(test_data, ground_truth):
    stats = Statistics(test_data)
    res = stats.calculate_session_length()
    
    for user, stat in res.items():
        assert ground_truth[user]['session_length'] == pytest.approx(stat, 0.1)

def test_getting_only_session_length_one_user(test_data, ground_truth):
    stats = Statistics(test_data)
    
    user_to_retrieve = list(test_data.keys())[0]
    sess_len = stats.calculate_session_length(user_id = user_to_retrieve)

    assert ground_truth[user_to_retrieve]['session_length'] == pytest.approx(sess_len, 0.1)

    # test that value error is raised if the user doesn't exist
    with pytest.raises(ValueError):
        stats.calculate_session_length(user_id = 'user_id')

# ----- PAUSE STATISTICS -----
def test_pause_statistics(test_data, ground_truth):
    stats = Statistics(test_data)

    assert stats.calculate_pause_statistics() == None