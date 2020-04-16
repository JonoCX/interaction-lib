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

# Tests
def test_init(test_data):
    with pytest.raises(ValueError):
        stats = Statistics(user_event_dict = {})

    with pytest.raises(TypeError):
        stats = Statistics(user_event_dict = [])

    stats = Statistics(test_data)
    assert stats.data == test_data
    assert stats.n_jobs == -1
    
def test_split_users(test_data):
    stats = Statistics(test_data, n_jobs = 4)
    for i, (u_chunk, d_chunk) in enumerate(stats._users_split):
        if i == 0: assert len(u_chunk) == 4
        else: assert len(u_chunk) == 3

def test_calculate_session_length_statistics(test_data, ground_truth):
    stats = Statistics(test_data, n_jobs = -1)
    res = stats.calculate_session_length_statistics()

    # test the raw session length
    for u, s in ground_truth.items():
        raw_sess_len = res[u]['raw_session_length']
        
        assert s['raw_session_length'] == pytest.approx(raw_sess_len, 0.1)

