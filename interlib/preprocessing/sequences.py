"""

"""

from .base import BaseExtractor
from ._event_handler import EventHandler

from datetime import datetime as dt
from typing import Optional, Union, List, Set, Dict

class Sequences(BaseExtractor):

    def __init__(self, user_event_dict, completion_point=None, n_jobs=-1):
        super().__init__(user_event_dict, completion_point=completion_point, n_jobs=n_jobs)

    def get_sequences(
        self,
        interaction_events: List[str],
        aliases: Dict[str, str],
        user_id: Optional[str] = None,
        compress: Optional[bool] = True,
        verbose: Optional[int] = 0
    ) -> Dict[str, Dict]:
        """ 
        
        """
        pass

""" 
include the pauses for between events like browser visibility
"""