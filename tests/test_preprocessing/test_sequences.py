import pytest 
import pickle, json, datetime

from interlib.preprocessing.sequences import Sequences

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data_files/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data