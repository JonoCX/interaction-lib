import pytest

import pickle

from interlib.preprocessing.statistics import *

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data

# Tests
def test_init(test_data):
    with pytest.raises(ValueError):
        s = Statistics(user_event_dict = {})

    with pytest.raises(TypeError):
        s = Statistics(user_event_dict = [])

    s = Statistics(test_data)
    assert s.data == test_data
    assert s.n_jobs == -1
    
def test_split_users(test_data):
    s = Statistics(test_data, n_jobs = 4)
    for u_chunk, d_chunk in s._users_split:
        assert len(u_chunk) == 3

def test_calculate_session_length_statistics(test_data):
    s = Statistics(test_data, n_jobs = -1)
    res = s.calculate_session_length_statistics()

    assert len(s._timestamps) == 12

