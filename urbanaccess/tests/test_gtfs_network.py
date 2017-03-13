import pytest
import pandas as pd
import numpy as np
from urbanaccess.gtfs import network


@pytest.fixture
def stop_times():
    data = {
        'unique_agency_id': ['citytrains'] * 25,
        'trip_id': ['a', 'a', 'a', 'a', 'a',
                    'b', 'b', 'b', 'b', 'b',
                    'c', 'c', 'c', 'c', 'c',
                    'd', 'd', 'd', 'd', 'd',
                    'e', 'e', 'e', 'e', 'e'],
        'stop_id': range(25),
        'departure_time_sec': [1, 2, np.nan, np.nan, 5,
                               1, 2, 3, 4, np.nan,
                               np.nan, np.nan, 3, 4, np.nan,
                               1, 2, 3, 4, 5,
                               1, np.nan, 3, 4, np.nan],
        'stop_sequence': [1, 2, 3, 4, 5] * 5
    }
    index = range(25)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar():
    data = {
        'unique_agency_id': ['citytrains'] * 4,
        'trip_id': ['a', 'b', 'c', 'e']
    }
    index = range(4)

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
    # NaN values should be removed from start and end
    assert df.loc[df.trip_id == 'c',
                  'departure_time_sec_interpolate'].tolist() == [3, 4]

    # trip 'd' should be removed because it's not in the calendar df
    assert len(df.loc[df.trip_id == 'd']) == 0

    # trip 'e' should interpolate the second row but leave off the trailing NA
    assert df.loc[df.trip_id == 'e',
                  'departure_time_sec_interpolate'].tolist() == [1, 2, 3, 4]
