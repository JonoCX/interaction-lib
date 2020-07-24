import pytest 
import pickle, json, datetime

from interlib.preprocessing._event_handler import EventHandler

@pytest.fixture
def aliases():
    return {
        "START_BUTTON_CLICKED": "SB", "PLAY_PAUSE_BUTTON_CLICKED": "PP",
        "BEHAVIOUR_CONTINUE_BUTTON_CLICKED": "BC","LINK_CHOICE_CLICKED": "LC",
        "FULLSCREEN_BUTTON_CLICKED": "FS", "NEXT_BUTTON_CLICKED": "NB",
        "VIDEO_SCRUBBED": "VS", "SEEK_FORWARD_BUTTON_CLICKED": "SFW",
        "BACK_BUTTON_CLICKED": "BB", "SEEK_BACKWARD_BUTTON_CLICKED": "SBK",
        "USER_SET_VARIABLE": "US", "OVERLAY_BUTTON_CLICKED": "OB",
        "VOLUME_CHANGED":  "VC", "OVERLAY_DEACTIVATED": "OD",
        "BROWSER_VISIBILITY_CHANGE": "BVC", "WINDOW_ORIENTATION_CHANGE": "WOC",
        "NARRATIVE_ELEMENT_CHANGE": "NEC"
    }

# ------ EVENT TESTS -------
def test_volume_change(aliases):
    eh = EventHandler(aliases)
    test_event = {
        'action_name': 'VOLUME_CHANGED',
        'data': {'romper_to_state': 'Background: 0.5'}
    }
    # default value is 1.0, so the volume has been decreased
    assert eh.process_event(test_event) == 'VC_DOWN'

    # increase the volumne
    test_event['data']['romper_to_state'] = 'Background: 0.7'
    assert eh._volume_tracker == 0.5
    assert eh.process_event(test_event) == 'VC_UP'
    assert eh._volume_tracker == 0.7

    # don't change the volume
    assert eh.process_event(test_event) == 'VC_NO'

    # reset the volume
    eh = eh.reset()
    assert eh._volume_tracker == 1.0

def test_fullscreen(aliases):
    eh = EventHandler(aliases)
    test_event = {
        'action_name': 'FULLSCREEN_BUTTON_CLICKED',
        'data': {'romper_to_state': 'fullscreen'}
    }
    assert eh.process_event(test_event) == 'TO_FS'

    # come out of full screen
    test_event['data']['romper_to_state'] = 'not-fullscreen'
    assert eh.process_event(test_event) == 'FROM_FS'

def test_browser_visibility_change(aliases):
    eh = EventHandler(aliases)
    test_event = {
        'action_name': 'BROWSER_VISIBILITY_CHANGE',
        'data': {'romper_to_state': 'hidden'}
    }
    assert eh.process_event(test_event) == 'BVC_H'

    # come to visible
    test_event['data']['romper_to_state'] = 'visible'
    assert eh.process_event(test_event) == 'BVC_V'

def test_window_orientation_change(aliases):
    eh = EventHandler(aliases)
    test_event = {
        'action_name': 'WINDOW_ORIENTATION_CHANGE',
        'data': {'romper_to_state': 90}
    }
    assert eh.process_event(test_event) == 'WOC_H'

    test_event['data']['romper_to_state'] = -90
    assert eh.process_event(test_event) == 'WOC_H'

    # to vertical
    test_event['data']['romper_to_state'] = 0
    assert eh.process_event(test_event) == 'WOC_V'

    test_event['data']['romper_to_state'] = 180
    assert eh.process_event(test_event) == 'WOC_V'

    # odd case
    test_event['data']['romper_to_state'] = ""
    assert eh.process_event(test_event) == 'WOC'

def test_all_events(aliases):
    eh = EventHandler(aliases)
    test_events = [{'action_name': ev} for ev in aliases.keys()]

    exclude = {
        'VOLUME_CHANGED', 'FULLSCREEN_BUTTON_CLICKED', 
        'BROWSER_VISIBILITY_CHANGE', 'WINDOW_ORIENTATION_CHANGE'
    }
    for ev in test_events:
        if ev['action_name'] in exclude:
            continue
        assert eh.process_event(ev) == aliases[ev['action_name']]
