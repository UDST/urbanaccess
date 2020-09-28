import pytest
import pandas as pd
import numpy as np

import urbanaccess.gtfs.network as gtfs_network
import urbanaccess.gtfs.load as gtfs_load
from urbanaccess.network import urbanaccess_network


@pytest.fixture
def expected_urbanaccess_network_keys():
    expected_keys = ['transit_nodes', 'transit_edges', 'net_connector_edges',
                     'osm_nodes', 'osm_edges', 'net_nodes', 'net_edges']
    return expected_keys.sort()


@pytest.fixture
def gtfs_feed_wo_calendar_dates(
        tmpdir, agency_a_feed_on_disk_wo_calendar_dates):
    feed_dir = agency_a_feed_on_disk_wo_calendar_dates
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=feed_dir,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    return loaded_feeds


@pytest.fixture
def stop_times():
    data = {
        'unique_agency_id': ['citytrains'] * 25,
        'trip_id': ['a', 'a', 'a', 'a', 'a',
                    'b', 'b', 'b', 'b', 'b',
                    'c', 'c', 'c', 'c', 'c',
                    'd', 'd', 'd', 'd', 'd',
                    'e', 'e', 'e', 'e', 'e'],
        'stop_id': str(range(25)),
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


@pytest.fixture
def stop_times_interpolated():
    data = {
        'unique_agency_id': ['citytrains', 'citytrains', 'citytrains',
                             'citytrains', 'citytrains', 'citytrains',
                             'citytrains', 'citytrains', 'citytrains',
                             'citytrains', 'citybuses', 'citybuses',
                             'citybuses', 'citybuses', 'citybuses',
                             'citybuses', 'citybuses', 'citybuses',
                             'citybuses', 'citybuses'],
        'unique_trip_id': ['a_citytrains', 'a_citytrains', 'a_citytrains',
                           'a_citytrains', 'a_citytrains', 'b_citytrains',
                           'b_citytrains', 'b_citytrains', 'b_citytrains',
                           'b_citytrains', 'c_citybuses', 'c_citybuses',
                           'c_citybuses', 'c_citybuses', 'c_citybuses',
                           'd_citybuses', 'd_citybuses', 'd_citybuses',
                           'd_citybuses', 'd_citybuses'],
        'timediff': [90, 20, 10, 15, 5,
                     10, 10, 10, 10, 10,
                     np.nan, 5, 5, 5, 5,
                     30, 40, 50, 60, 70],
        'trip_id': ['a', 'a', 'a', 'a', 'a',
                    'b', 'b', 'b', 'b', 'b',
                    'c', 'c', 'c', 'c', 'c',
                    'd', 'd', 'd', 'd', 'd'],
        'unique_stop_id': ['10_citytrains', '11_citytrains', '12_citytrains',
                           '13_citytrains', '14_citytrains', '10_citytrains',
                           '11_citytrains', '12_citytrains', '13_citytrains',
                           '14_citytrains', '10_citybuses', '11_citybuses',
                           '12_citybuses', '13_citybuses', '14_citybuses',
                           '10_citybuses', '11_citybuses', '12_citybuses',
                           '13_citybuses', '14_citybuses'],
        'stop_id': ['10', '11', '12', '13', '14'] * 4,
        'stop_sequence': [1, 2, 3, 4, 5] * 4
    }

    index = range(20)

    df = pd.DataFrame(data, index)
    return df


def test_create_transit_net_wo_calendar_dates(
        tmpdir, gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys):
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=tmpdir,
        save_filename=None)
    assert isinstance(transit_net, urbanaccess_network)
    urbanaccess_network_info = vars(transit_net)
    expected_dfs = ['transit_nodes', 'transit_edges']
    assert expected_urbanaccess_network_keys == list(
        urbanaccess_network_info.keys()).sort()
    for key, value in urbanaccess_network_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False


def test_create_transit_net_wo_req_file(
        tmpdir, gtfs_feed_wo_calendar_dates):
    # set trips df to blank df for test
    gtfs_feed_wo_calendar_dates.trips = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=tmpdir,
            save_filename=None)
    expected_error = (
        "one of the following gtfsfeeds_dfs objects trips, stops, "
        "or stop_times were found to be empty.")
    assert expected_error in str(excinfo.value)


def test_create_transit_net_wo_calendar_and_calendar_dates(
        tmpdir, gtfs_feed_wo_calendar_dates):
    # set calendar_dates and calendar dfs to blank df for test
    gtfs_feed_wo_calendar_dates.calendar_dates = pd.DataFrame()
    gtfs_feed_wo_calendar_dates.calendar = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=tmpdir,
            save_filename=None)
    expected_error = (
        "one of the following gtfsfeeds_dfs objects calendar or "
        "calendar_dates were found to be empty.")
    assert expected_error in str(excinfo.value)


def test_interpolator(stop_times, calendar):
    df = gtfs_network._interpolate_stop_times(stop_times, calendar)

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


def test_skip_interpolator(stop_times, calendar):
    series = pd.Series(data=[1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5],
                       index=range(25),
                       name='departure_time_sec')

    stop_times['departure_time_sec'] = series

    df = gtfs_network._interpolate_stop_times(stop_times, calendar)

    # everything should be the same,
    # with one row dropped for calendar day filter
    assert df.departure_time_sec_interpolate.tolist() == [1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5]


def test_edge_reformatter(stop_times_interpolated):
    df = gtfs_network._format_transit_net_edge(stop_times_interpolated)

    # length of edge df should be 16
    assert len(df) == 16

    # sequence id should be numeric starting at 1 and end at 4 for each trip
    assert df['sequence'][0] == 1 and df['sequence'][3] == 4

    # edge df should have these columns and no null values
    for col in ['node_id_from', 'node_id_to', 'weight']:
        assert col in df.columns and df[col].isnull().values.any() == False  # noqa

    # there should be 4 edges per trip id
    for i, row in df.groupby('unique_trip_id').size().iteritems():
        assert row == 4

    # check if the values in edge df were obtained from the correct
    # positions in the original stop times df
    assert df['node_id_from'][0] == stop_times_interpolated[
        'unique_stop_id'][0] and \
           df['node_id_to'][0] == stop_times_interpolated[
               'unique_stop_id'][1] and \
           df['weight'][0] == stop_times_interpolated['timediff'][1]  # noqa

    assert df['unique_trip_id'][8] == stop_times_interpolated[
        'unique_trip_id'][11] and \
           df['unique_agency_id'][8] == stop_times_interpolated[
               'unique_agency_id'][11]  # noqa
