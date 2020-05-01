import pytest

import pickle, json, datetime
from datetime import datetime as dt 
from datetime import timedelta
from collections import defaultdict

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

# ------ INIT ------
def test_init(test_data):
    with pytest.raises(ValueError):
        stats = Statistics(user_event_dict = {})

    with pytest.raises(TypeError):
        stats = Statistics(user_event_dict = [])

    stats = Statistics(test_data)
    assert stats.data.keys() == test_data.keys()
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
def test_type_of_pause(test_data):
    stats = Statistics(test_data)

    ts = dt(2020, 1, 1, 10, 00, 00) # define a base timestamp to compare to

    sp_res = stats._type_of_pause(ts, ts + timedelta(0, 3))
    mp_res = stats._type_of_pause(ts, ts + timedelta(0, 10))
    lp_res = stats._type_of_pause(ts, ts + timedelta(0, 20))
    vlp_res = stats._type_of_pause(ts, ts + timedelta(0, 50))

    assert sp_res[0] == 'SP' and sp_res[1] == 3.0
    assert mp_res[0] == 'MP' and mp_res[1] == 10.0
    assert lp_res[0] == 'LP' and lp_res[1] == 20.0
    assert vlp_res[0] == 'VLP' and vlp_res[1] == 50.0

def test_errors_type_of_pause(test_data):
    stats = Statistics(test_data)

    ts = dt(2020, 1, 1, 10, 00, 00)
    ts_behind = dt(2020, 1, 1, 9, 00, 00)

    with pytest.raises(ValueError):
        stats._type_of_pause(None, None)
        stats._type_of_pause(ts, ts_behind)

    with pytest.raises(TypeError):
        stats._type_of_pause(1.0, 2.0)

def test_pause_statistics(test_data, ground_truth):
    stats = Statistics(test_data)
    res = stats.calculate_pause_statistics()

    for user, stat in ground_truth.items():
        assert res[user]['SP'] == stat['SP']
        assert res[user]['MP'] == stat['MP']
        assert res[user]['LP'] == stat['LP']
        assert res[user]['VLP'] == stat['VLP']

def test_empty_pauses_statistics(test_data):
    test_data_copy = test_data.copy()
    user_to_delete = list(test_data_copy.keys())[0]
    test_data_copy[user_to_delete] = [] # remove their events

    stats = Statistics(test_data_copy)
    res = stats.calculate_pause_statistics()

    assert res[user_to_delete]['SP'] == 0
    assert res[user_to_delete]['MP'] == 0
    assert res[user_to_delete]['LP'] == 0
    assert res[user_to_delete]['VLP'] == 0

def test_pauses_single_user(test_data, ground_truth):
    stats = Statistics(test_data)
    user = '959c1a91-8b0f-4178-bc59-70499353204f'
    
    result = stats.calculate_pause_statistics(user_id = user)

    assert result['SP'] == ground_truth[user]['SP']
    assert result['MP'] == ground_truth[user]['MP']
    assert result['LP'] == ground_truth[user]['LP']
    assert result['VLP'] == ground_truth[user]['VLP']

def test_pauses_single_user_errors(test_data):
    stats = Statistics(test_data)

    # test value error when pause statistics haven't already been calculated
    with pytest.raises(ValueError):
        stats.calculate_pause_statistics(user_id = '150b')
    
    # test type error when something other than a string is passed
    with pytest.raises(TypeError):
        stats.calculate_pause_statistics(user_id = 150)
    
    # calculate statistics to test errors in retrieval 
    res = stats.calculate_pause_statistics()

    # test value error for when the user isn't in the stats or data
    with pytest.raises(ValueError):
        stats.calculate_pause_statistics(user_id = '150b')
    
    # test type error
    with pytest.raises(TypeError):
        stats.calculate_pause_statistics(user_id = 150)

# ----- EVENT STATISTICS -----
def test_event_statistics(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)
    res = stats.calculate_event_statistics(interaction_events)

    # assert that the ground truth statistics are the same
    for user, stat in ground_truth.items():
        for event in interaction_events:
            assert res[user][event] == stat[event]

def test_include_lcc_and_usv(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)

    # add in the lcc and usv counts into the total events
    for user, stat in ground_truth.copy().items():
        ground_truth[user]['total_events'] += ground_truth[user]['LINK_CHOICE_CLICKED']
        ground_truth[user]['total_events'] += ground_truth[user]['USER_SET_VARIABLE']

    res = stats.calculate_event_statistics(
        interaction_events, include_link_choices = True, include_user_set_variables = True
    )

    assert len(res.keys()) == len(ground_truth.keys())

    # test that the stat's still match when LCC and USV are included in the total count
    for user, stat in ground_truth.items():
        for event in interaction_events:
            assert res[user][event] == stat[event]

def test_event_statistics_single_user(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)
    user = '959c1a91-8b0f-4178-bc59-70499353204f'

    # test that the stats are calculated for a single user and that they're correct
    res = stats.calculate_event_statistics(interaction_events, user_id = user)

    # test that each stat is correct
    for u, stat in ground_truth.items():
        if u == user:
            for event in interaction_events:
                assert res[event] == stat[event]

    res = Statistics(test_data).calculate_event_statistics(
        interaction_events, include_link_choices = True, 
        include_user_set_variables = True, user_id = user
    )

    # add in the lcc and usv counts into the total events
    ground_truth[user]['total_events'] += ground_truth[user]['LINK_CHOICE_CLICKED']
    ground_truth[user]['total_events'] += ground_truth[user]['USER_SET_VARIABLE']

    # test that each stat is correct
    for u, stat in ground_truth.items():
        if u == user:
            for event in interaction_events:
                assert res[event] == stat[event]

def test_event_statistics_errors(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)

    # test type error is thrown when a non-set object is pass for 
    # the interaction events
    with pytest.raises(TypeError):
        stats.calculate_event_statistics(interaction_events = [])

    # test that value error is thrown when an empty set is passed
    with pytest.raises(ValueError):
        stats.calculate_event_statistics(interaction_events = set([]))

    # test that a type error is thrown when a non-string user_id is passed
    with pytest.raises(TypeError):
        stats.calculate_event_statistics(interaction_events, user_id = 150)

    # test that a value error is thrown when user_id is not in data
    with pytest.raises(ValueError):
        stats.calculate_event_statistics(interaction_events, user_id = '150b')

    # calculate the statistics to test for retrieval errors
    res = stats.calculate_event_statistics(interaction_events)

    # test that a type error is thrown when user_id is not a string
    with pytest.raises(TypeError):
        stats.calculate_event_statistics(interaction_events, user_id = 150)

    # test that a value error is thrown when user_id is not in the data
    with pytest.raises(ValueError):
        stats.calculate_event_statistics(interaction_events, user_id = '150b')

# ------ OVERALL STATISTICS ------
def test_overall_statistics(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)
    res = stats.calculate_statistics(interaction_events, verbose = 0)

    for user, stat in ground_truth.items():
        for s, value in stat.items():
            assert value == ground_truth[user][s]

def test_overall_statistics_single_user(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)
    user = '959c1a91-8b0f-4178-bc59-70499353204f'

    individual_results = stats.calculate_statistics(
        interaction_events, 
        user_id = user,
        include_link_choices = True,
        include_user_set_variables = True
    )
    individual_ground_truth_results = ground_truth[user]

    # assert that the length is the same (-1 because GT has 'reach_end')
    assert len(individual_results) == len(individual_ground_truth_results) - 1

    # time statistics should be approx equal
    time_stats = {'hidden_time', 'raw_session_length', 'session_length'}

    for stat, value in individual_results.items():
        if stat in time_stats:
            assert individual_ground_truth_results[stat] == pytest.approx(value, 0.1)
        else:
            assert individual_ground_truth_results[stat] == value
    
def test_overall_statistics_single_user_without_lcc_usv(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)
    user = 'b194b76c-7866-4b6d-8502-93ffe6322b64'

    # time statistics shouldbe approx equal
    time_stats = {'hidden_time', 'raw_session_length', 'session_length'}

    # LCC and USV are included in the ground truth total events by default
    gt_individual = ground_truth[user]
    for stat, value in gt_individual.copy().items():
        if stat == 'LINK_CHOICE_CLICKED' or stat == 'USER_SET_VARIABLE':
            gt_individual['total_events'] -= gt_individual[stat]
    
    res_individual = stats.calculate_statistics(interaction_events, user_id = user)

    for stat, value in res_individual.items():
        if stat in time_stats:
            assert gt_individual[stat] == pytest.approx(value, 0.1)
        else:
            assert gt_individual[stat] == value
        
def test_overall_statistics_errors(test_data, ground_truth, interaction_events):
    stats = Statistics(test_data)

    # test that a type error is throw for passing a non-set interaction_events
    with pytest.raises(TypeError):
        stats.calculate_statistics(interaction_events = [])

    # test that a value error is thrown when passing an empty set
    with pytest.raises(ValueError):
        stats.calculate_statistics(interaction_events = set([]))

    # test that a type error is thrown when a set not containing strings is passed
    with pytest.raises(TypeError):
        stats.calculate_statistics(interaction_events = set([1, 2, 3, 4]))

    # if stats hasn't be calculated, test that TypeError is thrown when
    # a non-string user_id is passed
    with pytest.raises(TypeError):
        stats.calculate_statistics(
            interaction_events = interaction_events,
            user_id = 150
        )

    # if stats hasn't been calculated, test that a ValueError is thrown
    # when an invalid user_id is passed.
    with pytest.raises(ValueError):
        stats.calculate_statistics(
            interaction_events = interaction_events,
            user_id = '150b'
        )

    stats.calculate_statistics(interaction_events)

    # test the same two previous errors now the stats have been
    # calculated.
    with pytest.raises(TypeError):
        stats.calculate_statistics(
            interaction_events = interaction_events,
            user_id = 150
        )

    with pytest.raises(ValueError):
        stats.calculate_statistics(
            interaction_events = interaction_events,
            user_id = '150b'
        )

# ------ EVENT FREQUENCIES ------
def test_event_frequencies(test_data, ground_truth, interaction_events):
    frequencies = [0, 1, 2, 3, 4, 5] # up to 10 minutes
    # frequencies = [i for i in range(0, )]
    frequencies = [v * 60 for v in frequencies]
    print(frequencies)

    stats = Statistics(test_data)
    res = stats.calculate_event_frequencies(frequencies, interaction_events)

    # for user, freq in res.items():
    #     # print(user, freq.keys())
    #     # for t, counts in freq.items():
    #     #     print(t, all(value == 0 for value in counts.values()))
    #     if user == 'be3720be-3da1-419c-b912-cacc3f80a427':
    #         for t, counts in freq.items():
    #             print(t, counts)
    # for user, freq in res.items():
    #     if user == '959c1a91-8b0f-4178-bc59-70499353204f':
    #         for time_freq, counts in freq.items():
    #             print('\n', time_freq)
    #             for event, count in counts.items():
    #                 if count != 0:
    #                     print(event, count, end=' ')

    # # print(res['1e82e0fe-71f5-4a65-a0c3-02e70e564d3e'])

    # # aggregate the counts over the frequencies
    # user_agg_counts = {user: {} for user, v in test_data.items()}
    # for user, freq in res.items():
    #     ev_counter = defaultdict(int)
    #     for time_freq, event_count in freq.items():
    #         # print(time_freq, event_count)
    #         for event, count in event_count.items():
    #             ev_counter[event] += count

    #     user_agg_counts[user] = dict(ev_counter)


    # # print(user_agg_counts['1e82e0fe-71f5-4a65-a0c3-02e70e564d3e'])
    # # for u, v in user_agg_counts.items():
    # #     print(u, v, '\n')

    # # assert that the summary counts are the same as the ground truth
    # for user, counts in user_agg_counts.items():
    #     # print(user)
    #     gt_stats = ground_truth[user]
    #     for event in interaction_events:
    #         print(user, event, counts[event], gt_stats[event])
    #         assert counts[event] == gt_stats[event]