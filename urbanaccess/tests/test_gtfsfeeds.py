import pytest
import pandas as pd
import numpy as np

import urbanaccess as UA

def test_search_data_exchange():

    result = UA.gtfsfeeds.search(
        api='gtfs_data_exchange'

        # Relevant to GTFS Data Exchange
        search_text=None,
        search_field=None,
        match='contains',

        # Relevant to TransitLand
        bounding_box=None,

        # Settings
        add_feed=False,
        overwrite_feed=False)

    print('Done. ----')
    print(result)
