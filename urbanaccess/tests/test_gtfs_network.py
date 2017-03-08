import pytest
import pandas as pd
import numpy as np
from urbanaccess.gtfs import network


@pytest.fixture
def stop_times():
    data = {
        'unique_agency_id': ['citytrains'] * 20,
        'trip_id': ['a', 'a', 'a', 'a', 'a',
                    'b', 'b', 'b', 'b', 'b',
                    'c', 'c', 'c', 'c', 'c',
                    'd', 'd', 'd', 'd', 'd'],
        'stop_id': range(20),
        'departure_time_sec': [1, 2, np.nan, np.nan, 5,
                               1, 2, 3, 4, np.nan,
                               np.nan, np.nan, 3, 4, np.nan,
                               1, 2, 3, 4, 5],
        'stop_sequence': [1, 2, 3, 4, 5] * 4
    }
    index = range(20)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar():
    data = {
        'unique_agency_id': ['citytrains'] * 3,
        'trip_id': ['a', 'b', 'c']
    }
    index = range(3)

    df = pd.DataFrame(data, index)
    return df


def test_interpolator(stop_times, calendar):
    df = network.interpolatestoptimes(stop_times, calendar, day='monday')

    # unique_trip_id should be generated
    assert df.loc[1, 'unique_trip_id'] == 'a_citytrains'

    # trip 'a' should be interpolated fully
    assert df.loc[df.trip_id == 'a',
                  'departure_time_sec_interpolate'].tolist() == [1, 2, 3, 4, 5]

    # trip 'b' should be skipped because it has only one null value
    # but its null value should be removed
    assert df.loc[df.trip_id == 'b',
                  'departure_time_sec_interpolate'].tolist() == [1, 2, 3, 4]

    # trip 'c' should be interpolated
    # no starting value, so first two times removed
    # missing value at end should be equal to last existing value
    assert df.loc[df.trip_id == 'c',
                  'departure_time_sec_interpolate'].tolist() == [3, 4, 4]

    # trip 'd' should be removed because it's not in the calendar df
    assert len(df.loc[df.trip_id == 'd']) == 0
