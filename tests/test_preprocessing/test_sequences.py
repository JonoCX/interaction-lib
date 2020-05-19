import pytest 
import pickle, json, datetime

from collections import Counter, defaultdict

from interlib.preprocessing.sequences import Sequences

# Fixtures
@pytest.fixture
def test_data():
    with open('tests/test_data_files/test_data.p', 'rb') as data_in:
        data = pickle.load(data_in)
    return data

@pytest.fixture
def sequence_data():
    with open('tests/test_data_files/test_sequences.json', 'rb') as data_in:
        data = json.load(data_in)
    return data

@pytest.fixture
def interaction_events():
    return { # set of user actions we consider
        'PLAY_PAUSE_BUTTON_CLICKED', 'BACK_BUTTON_CLICKED', 
        'FULLSCREEN_BUTTON_CLICKED', 'NEXT_BUTTON_CLICKED', 
        'SUBTITLES_BUTTON_CLICKED', 'VOLUME_CHANGE',
        'VIDEO_SCRUBBED', 'SEEK_BACKWARD_BUTTON_CLICKED', 
        'SEEK_FORWARD_BUTTON_CLICKED', 'VOLUME_MUTE_TOGGLED', 
        'VARIABLE_PANEL_NEXT_CLICKED', 'VARIABLE_PANEL_BACK_CLICKED',
        'BROWSER_VISIBILITY_CHANGE', 'WINDOW_ORIENTATION_CHANGE',
        'NARRATIVE_ELEMENT_CHANGE', 'LINK_CHOICE_CLICKED',
        'USER_SET_VARIABLE'
    }

@pytest.fixture
def aliases():
    return {
        "PLAY_PAUSE_BUTTON_CLICKED": "PP", "LINK_CHOICE_CLICKED": "LC",
        "FULLSCREEN_BUTTON_CLICKED": "FS", "NEXT_BUTTON_CLICKED": "NB",
        "VIDEO_SCRUBBED": "VS", "SEEK_FORWARD_BUTTON_CLICKED": "SFW",
        "BACK_BUTTON_CLICKED": "BB", "SEEK_BACKWARD_BUTTON_CLICKED": "SBK",
        "USER_SET_VARIABLE": "US", "VOLUME_CHANGE":  "VC",
        "BROWSER_VISIBILITY_CHANGE": "BVC", "WINDOW_ORIENTATION_CHANGE": "WOC",
        "NARRATIVE_ELEMENT_CHANGE": "NEC", "VOLUME_MUTE_TOGGLED": 'VM', 
        "VARIABLE_PANEL_NEXT_CLICKED": "VPN",  "VARIABLE_PANEL_BACK_CLICKED": "VPB",
        "SUBTITLES_BUTTON_CLICKED": "SUB"
    }

@pytest.fixture
def ngrams():
    return [
        ('NEC', 'SP'), ('SP', 'PP'), ('PP', 'SP'), ('SP', 'LC'), ('LC', 'SP'),
        ('SP', 'NEC'), ('NEC', 'SP'), ('SP', 'BVC_H'), ('BVC_H', 'VLP'),
        ('VLP', 'BVC_V'), ('BVC_V', 'SP'), ('SP', 'US'), ('US', 'VPN'),
        ('VPN', 'SP'), ('SP', 'US'), ('US', 'VPN'), ('VPN', 'SP'), ('SP', 'US'),
        ('US', 'VPN'), ('VPN', 'SP'), ('SP', 'VPN'), ('VPN', 'SP'), ('SP', 'VPN'),
        ('VPN', 'SP'), ('SP', 'NEC'), ('NEC', 'SP'), ('SP', 'NEC')
    ]

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

# ----- SEQUENCES TEST ------
def test_sequences_single_user(test_data, sequence_data, interaction_events, aliases):
    seq = Sequences(test_data, n_jobs = 1)
    extracted_seq = seq.get_sequences(
        interaction_events = interaction_events,
        aliases = aliases,
        user_id = '0c5b7783-0320-4818-bcb8-e244de363591'
    )

    assert extracted_seq == sequence_data['0c5b7783-0320-4818-bcb8-e244de363591']['non_compressed']

def test_sequences_all_users(test_data, sequence_data, interaction_events, aliases):
    seq = Sequences(test_data, n_jobs = 1)
    extracted_seqs = seq.get_sequences(
        interaction_events = interaction_events,
        aliases = aliases
    )

    for user, seq_types in sequence_data.items():
        assert extracted_seqs[user] == seq_types['non_compressed']

def test_sequences_compressed_single_user(test_data, sequence_data, interaction_events, aliases):
    seq = Sequences(test_data, n_jobs = 1)
    extracted_seq = seq.get_sequences(
        interaction_events = interaction_events,
        aliases = aliases,
        user_id = 'b194b76c-7866-4b6d-8502-93ffe6322b64',
        compress = True,
        compress_event = 'SFW'
    )

    assert extracted_seq == sequence_data['b194b76c-7866-4b6d-8502-93ffe6322b64']['compressed']

def test_sequences_ngrams(test_data, interaction_events, aliases, ngrams):
    seq = Sequences(test_data, n_jobs = 1)
    extracted_seq = seq.get_sequences(
        interaction_events = interaction_events,
        aliases = aliases
    )

    user_ngrams = seq.get_ngrams(n = 2)
    assert ngrams == user_ngrams['0c5b7783-0320-4818-bcb8-e244de363591']
    
def test_sequences_ngrams_counts(test_data, interaction_events, aliases, ngrams):
    seq = Sequences(test_data, n_jobs = 1)
    extracted_seq = seq.get_sequences(
        interaction_events = interaction_events,
        aliases = aliases
    )

    user_ngrams, counts, user_counts = seq.get_ngrams(n = 2, counter = True)

    # create a counter to automate the testing a bit
    gt_counts = defaultdict(int)
    for each_gram in ngrams:
        gt_counts[each_gram] += 1
    
    for pair, value in user_counts['0c5b7783-0320-4818-bcb8-e244de363591'].items():
        assert gt_counts[pair] == value
