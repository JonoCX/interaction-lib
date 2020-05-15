import pytest 
import pickle, json, datetime

from interlib.preprocessing.sequences import Sequences

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data_files/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data

# ------ COMPRESS SEQUENCE TESTS ------
def test_compress_sequence(test_data):
    sequence = ['NEC', 'PP', 'NEC', 'NEC', 'NEC', 'BB', 'NB', 'NEC']
    expected_sequence = ['NEC_1', 'PP', 'NEC_3', 'BB', 'NB', 'NEC_1']

    seq = Sequences(test_data, n_jobs = 1)
    compressed_sequence = seq._compress_events(sequence)

    for indx, val in enumerate(compressed_sequence):
        assert val == expected_sequence[indx]

def test_compress_sequence_link_choice(test_data):
    sequence = ['LC', 'LC', 'LC', 'WOC', 'LC', 'NEC', 'LC', 'NB', 'BB', 'PP']
    expected_sequence = ['LC_3', 'WOC', 'LC_1', 'NEC', 'LC_1', 'NB', 'BB', 'PP']

    seq = Sequences(test_data, n_jobs = 1)
    compressed_sequence = seq._compress_events(sequence, compress_event = 'LC')

    for indx, val in enumerate(compressed_sequence):
        assert val == expected_sequence[indx]

def test_compress_sequence_no_compression_needed(test_data):
    sequence = ['LC', 'NEC', 'PP', 'NB', 'BB', 'SP', 'LP', 'MP', 'VLP']
    expected_sequence = sequence

    seq = Sequences(test_data, n_jobs = 1)
    compressed_sequence = seq._compress_events(sequence, compress_event = '')

    for indx, val in enumerate(compressed_sequence):
        assert val == expected_sequence[indx]