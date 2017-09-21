import pytest
import pandas as pd
import numpy as np

import urbanaccess as UA

def test_search_data_exchange():

    UA.gtfsfeeds.search()
        

    assert True