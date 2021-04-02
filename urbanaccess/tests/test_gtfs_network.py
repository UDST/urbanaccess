import pytest
import os
import glob
import pandas as pd
import numpy as np

import urbanaccess.gtfs.network as gtfs_network
import urbanaccess.gtfs.load as gtfs_load
from urbanaccess.network import urbanaccess_network
from urbanaccess.gtfs.gtfsfeeds_dataframe import urbanaccess_gtfs_df


@pytest.fixture
def expected_urbanaccess_network_keys():
    expected_keys = ['transit_nodes', 'transit_edges', 'net_connector_edges',
                     'osm_nodes', 'osm_edges', 'net_nodes', 'net_edges']
    return sorted(expected_keys)


@pytest.fixture
def expected_gtfsfeeds_dfs_keys():
    expected_keys = ['stops', 'routes', 'trips', 'stop_times',
                     'calendar_dates', 'calendar', 'stop_times_int',
                     'headways']
    return sorted(expected_keys)


@pytest.fixture
def gtfs_feed_wo_calendar_dates(
        agency_a_feed_on_disk_wo_calendar_dates):
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=agency_a_feed_on_disk_wo_calendar_dates,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    return loaded_feeds


@pytest.fixture
def gtfs_feed_wo_calendar(
        agency_a_feed_on_disk_wo_calendar):
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=agency_a_feed_on_disk_wo_calendar,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    return loaded_feeds


@pytest.fixture
def gtfs_feed_w_calendar_and_calendar_dates(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates):
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=agency_a_feed_on_disk_w_calendar_and_calendar_dates,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    return loaded_feeds


@pytest.fixture
def selected_int_stop_times_from_feed_wo_calendar_dates(
        gtfs_feed_wo_calendar_dates):
    # reproduce what is expected as the 'selected_interpolated_stop_times_df'
    stop_times = gtfs_feed_wo_calendar_dates.stop_times.copy()
    stop_times = stop_times.loc[stop_times['trip_id'] == 'a3']
    stop_times['unique_stop_id'] = (
        stop_times['stop_id'].str.cat(
            stop_times['unique_agency_id'].astype('str'), sep='_'))
    stop_times['unique_trip_id'] = (
        stop_times['trip_id'].str.cat(
            stop_times['unique_agency_id'].astype('str'), sep='_'))
    data = {
        'departure_time_sec_interpolate': [29700, 30000, 30300,
                                           30600, 30900, 31200],
        'timediff': [np.nan, 300.0, 300.0, 300.0, 300.0, 300.0]
    }
    index = range(12, 18)
    df = pd.DataFrame(data, index)
    stop_times = pd.concat([stop_times, df], axis=1)

    return stop_times


@pytest.fixture
def selected_stops_from_feed_wo_calendar_dates(gtfs_feed_wo_calendar_dates):
    # create 'final_selected_stops' df that is used as input to test function
    stops_df = gtfs_feed_wo_calendar_dates.stops.copy()
    stops_df = stops_df.iloc[0:6]
    stops_df['unique_stop_id'] = (
        stops_df['stop_id'].str.cat(
            stops_df['unique_agency_id'].astype('str'), sep='_'))
    stops_df.set_index('unique_stop_id', drop=True, inplace=True)
    stops_df.index.name = 'node_id'
    return stops_df


@pytest.fixture
def stop_times():
    data = {
        'unique_agency_id': ['citytrains'] * 35,
        'trip_id': ['a', 'a', 'a', 'a', 'a',
                    'b', 'b', 'b', 'b', 'b',
                    'c', 'c', 'c', 'c', 'c',
                    'd', 'd', 'd', 'd', 'd',
                    'e', 'e', 'e', 'e', 'e',
                    'f', 'f', 'f', 'f', 'f',
                    'g', 'g', 'g', 'g', 'g'],
        'stop_id': range(1, 36),
        'departure_time_sec': [1, 2, np.nan, np.nan, 5,
                               1, 2, 3, 4, np.nan,
                               np.nan, np.nan, 3, 4, np.nan,
                               1, 2, 3, 4, 5,
                               1, np.nan, 3, 4, np.nan,
                               1, np.nan, 3, 4, 5,
                               np.nan, 2, 3, 4, 5],
        'stop_sequence': [1, 2, 3, 4, 5] * 7
    }
    index = range(35)

    df = pd.DataFrame(data, index)
    df['stop_id'] = df['stop_id'].astype('str')

    return df


@pytest.fixture
def calendar():
    data = {
        'unique_agency_id': ['citytrains'] * 6,
        'trip_id': ['a', 'b', 'c', 'e', 'f', 'g']
    }
    index = range(6)

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


@pytest.fixture
def transit_edge_from_feed_wo_calendar_dates():
    data = {
        'node_id_from': ['1_agency_a_city_a', '2_agency_a_city_a',
                         '3_agency_a_city_a', '4_agency_a_city_a',
                         '5_agency_a_city_a'],
        'node_id_to': ['2_agency_a_city_a', '3_agency_a_city_a',
                       '4_agency_a_city_a', '5_agency_a_city_a',
                       '6_agency_a_city_a'],
        'weight': [300.0] * 5,
        'unique_agency_id': ['agency_a_city_a'] * 5,
        'unique_trip_id': ['a3_agency_a_city_a'] * 5,
        'sequence': range(1, 6),
        'id': ['a3_agency_a_city_a_1', 'a3_agency_a_city_a_2',
               'a3_agency_a_city_a_3', 'a3_agency_a_city_a_4',
               'a3_agency_a_city_a_5'],
    }
    index = range(5)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1(
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2):
    # represents df prior to being post-processed downstream
    df = expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2.copy()
    df.drop(columns=['route_type'], inplace=True)
    # convert weight from min to sec to represent df prior to post-process step
    df['weight'] = 300.0
    return df


@pytest.fixture
def expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2():
    # represents df after it has been post-processed downstream
    data = {
        'node_id_from': ['1_agency_a_city_a', '2_agency_a_city_a',
                         '3_agency_a_city_a', '4_agency_a_city_a',
                         '5_agency_a_city_a'],
        'node_id_to': ['2_agency_a_city_a', '3_agency_a_city_a',
                       '4_agency_a_city_a', '5_agency_a_city_a',
                       '6_agency_a_city_a'],
        'weight': [5.0] * 5,
        'unique_agency_id': ['agency_a_city_a'] * 5,
        'unique_trip_id': ['a3_agency_a_city_a'] * 5,
        'sequence': range(1, 6),
        'id': ['a3_agency_a_city_a_1', 'a3_agency_a_city_a_2',
               'a3_agency_a_city_a_3', 'a3_agency_a_city_a_4',
               'a3_agency_a_city_a_5'],
        'route_type': [3] * 5
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    # raw data are read as int32
    df['sequence'] = df['sequence'].astype('int32')
    return df


@pytest.fixture
def expected_final_transit_edge_from_feed_wo_calendar_dates(
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2):
    data = {
        'unique_route_id': ['10-101_agency_a_city_a'] * 5,
        'net_type': ['transit'] * 5
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    df = pd.concat(
        [expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2, df],
        axis=1)
    return df


@pytest.fixture
def expected_transit_node_from_feed_wo_calendar_dates():
    data = {
        'node_id': ['1_agency_a_city_a', '2_agency_a_city_a',
                    '3_agency_a_city_a', '4_agency_a_city_a',
                    '5_agency_a_city_a', '6_agency_a_city_a'],
        'x': [-122.265609, -122.224274, -122.271604, -122.269029, -122.267227,
              -122.251793],
        'y': [37.797484, 37.774963, 37.803664, 37.80787, 37.828415, 37.844601],
        'unique_agency_id': ['agency_a_city_a'] * 6,
        'route_type': [3] * 6,
        'stop_id': range(1, 7),
        'stop_name': ['ave a', 'ave b', 'ave c', 'ave d', 'ave e', 'ave f'],
        'wheelchair_boarding': [1, 0, 0, 0, 0, 0],
        'location_type': [1] * 6
    }
    index = range(6)

    df = pd.DataFrame(data, index)
    df['stop_id'] = df['stop_id'].astype('str')
    df.set_index('node_id', drop=True, inplace=True)
    return df


@pytest.fixture
def edge_route_type_impedance_df():
    data = {
        'weight': [2, 2, 2, 3, 3, 3, 5, 5, 5, 5],
        'route_type': [1, 1, 1, 2, 2, 2, 3, 3, 3, 3]
    }
    index = range(10)

    df = pd.DataFrame(data, index)

    return df


@pytest.fixture()
def hdf5_file_on_disk_gtfsfeeds_dfs(
        tmpdir,
        gtfs_feed_wo_calendar_dates,
        selected_int_stop_times_from_feed_wo_calendar_dates):
    hdf5_dict = {'stop_times': gtfs_feed_wo_calendar_dates.stop_times,
                 'stops': gtfs_feed_wo_calendar_dates.stops,
                 'routes': gtfs_feed_wo_calendar_dates.routes,
                 'trips': gtfs_feed_wo_calendar_dates.trips,
                 'calendar': gtfs_feed_wo_calendar_dates.calendar,
                 'stop_times_int':
                     selected_int_stop_times_from_feed_wo_calendar_dates}
    hdf5_save_path = os.path.join(tmpdir.strpath, 'test_hdf5_load')
    hdf5_file = os.path.join(hdf5_save_path, 'test_file.h5')
    os.makedirs(hdf5_save_path)
    print('writing test HDF5 to: {}'.format(hdf5_file))
    # create the HDF5
    store = pd.HDFStore(hdf5_file)
    store.close()
    # add keys and DFs to HDF5
    for key, df in hdf5_dict.items():
        store = pd.HDFStore(hdf5_file, mode='r')
        store.close()
        df.to_hdf(hdf5_file, key=key, mode='a', format='table')
    return hdf5_save_path


def test_create_transit_net_wo_calendar_dates(
        gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar_dates):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar_dates.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None)
    assert isinstance(transit_net, urbanaccess_network)
    urbanaccess_network_info = vars(transit_net)
    expected_dfs = ['transit_nodes', 'transit_edges']
    assert expected_urbanaccess_network_keys == sorted(list(
        urbanaccess_network_info.keys()))
    for key, value in urbanaccess_network_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False

    result_edge = transit_net.transit_edges.copy()
    # test that output df is identical to expected df
    result_edge = result_edge.reindex(
        sorted(result_edge.columns), axis=1)
    expected_result = expected_result.reindex(
        sorted(expected_result.columns), axis=1)
    assert result_edge.equals(expected_result)


def test_create_transit_net_wo_direction_id(
        gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar_dates):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar_dates.copy()
    # remove 'direction_id' col for test
    gtfs_feed_wo_calendar_dates.trips.drop(
        columns=['direction_id'], inplace=True)
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None)
    assert isinstance(transit_net, urbanaccess_network)
    urbanaccess_network_info = vars(transit_net)
    expected_dfs = ['transit_nodes', 'transit_edges']
    assert expected_urbanaccess_network_keys == sorted(list(
        urbanaccess_network_info.keys()))
    for key, value in urbanaccess_network_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False

    result_edge = transit_net.transit_edges.copy()
    # test that output df is identical to expected df
    result_edge = result_edge.reindex(
        sorted(result_edge.columns), axis=1)
    expected_result = expected_result.reindex(
        sorted(expected_result.columns), axis=1)
    assert result_edge.equals(expected_result)


def test_create_transit_net_wo_req_file(gtfs_feed_wo_calendar_dates):
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
            save_dir=None,
            save_filename=None)
    expected_error = (
        "One of the following gtfsfeeds_dfs objects: trips, stops, "
        "or stop_times were found to be empty.")
    assert expected_error in str(excinfo.value)


def test_create_transit_net_wo_calendar_and_calendar_dates(
        gtfs_feed_wo_calendar_dates):
    # set calendar_dates and calendar dfs to blank df for test
    gtfs_feed_wo_calendar_dates.calendar_dates = pd.DataFrame()
    gtfs_feed_wo_calendar_dates.calendar = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '11:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = (
        "One of the following gtfsfeeds_dfs objects: calendar or "
        "calendar_dates were found to be empty.")
    assert expected_error in str(excinfo.value)


def test_create_transit_net_invalid_params(gtfs_feed_wo_calendar_dates):
    msg = ('starttime and endtime are not in the correct format. '
           'Format should be a 24 hour clock in the following format: '
           '08:00:00 or 17:00:00.')
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['7:00:0', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ("['7:00:0', '10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ("['10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange='10:00:00',
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ("10:00:00 {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=[100000, 170000],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ("[100000, 170000] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['10:00:00', '07:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ("['10:00:00', '07:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=2,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = "overwrite_existing_stop_times_int must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=2,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = "use_existing_stop_times_int must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=2,
            save_dir=None,
            save_filename=None)
    expected_error = "save_processed_gtfs must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            None, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = "gtfsfeeds_dfs must be an urbanaccess_gtfs_df object."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=True,
            use_existing_stop_times_int=True,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None)
    expected_error = ('overwrite_existing_stop_times_int and '
                      'use_existing_stop_times_int cannot both be True.')
    assert expected_error in str(excinfo.value)


def test_create_transit_net_overwrite_stop_times_int_True(
        gtfs_feed_wo_calendar_dates,
        selected_int_stop_times_from_feed_wo_calendar_dates):
    # populate stop_times_int for test that is different than the one that
    # would be calculated
    df = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    df['timediff'] = df['timediff'] * 2
    gtfs_feed_wo_calendar_dates.stop_times_int = df
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=True,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None)
    # values should be different given overwrite_existing_stop_times_int = True
    assert gtfs_feed_wo_calendar_dates.stop_times_int['timediff'].equals(
        df['timediff']) is False


def test_create_transit_net_use_existing_stop_times_int_True(
        gtfs_feed_wo_calendar_dates,
        selected_int_stop_times_from_feed_wo_calendar_dates):
    # populate stop_times_int for test that is different than the one that
    # would be calculated
    df = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    df['timediff'] = df['timediff'] * 2
    gtfs_feed_wo_calendar_dates.stop_times_int = df
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=True,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None)
    # values should be the the same since use_existing_stop_times_int = True
    assert gtfs_feed_wo_calendar_dates.stop_times_int['timediff'].equals(
        df['timediff'])


def test_create_transit_net_save_processed_gtfs_True(
        tmpdir, gtfs_feed_wo_calendar_dates):
    dir_path = os.path.join(tmpdir.strpath, 'test_hdf5_save')
    os.makedirs(dir_path)
    print('preparing test dir: {}'.format(dir_path))

    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=True,
        save_dir=dir_path,
        save_filename='test_file.h5')

    # test that file was written as expected
    file_list = glob.glob(r"{}/*.h5".format(dir_path))
    file_path = file_list[0]
    file_name = os.path.basename(file_path)
    assert file_name == 'test_file.h5'
    # test HDF5 store
    expected_keys = {'/calendar', '/routes', '/stop_times', '/stop_times_int',
                     '/stops', '/trips'}
    with pd.HDFStore(file_path) as store:
        result_keys = set(store.keys())
        assert result_keys == expected_keys
        # check that data exists in each DataFrame
        for key in expected_keys:
            df = store[key]
            assert df.empty is False


def test_interpolator(stop_times, calendar):
    df = gtfs_network._interpolate_stop_times(stop_times, calendar)

    # unique_trip_id should be generated
    assert df.loc[1, 'unique_trip_id'] == 'a_citytrains'

    # trip 'a' should be interpolated fully
    assert df.loc[df.trip_id == 'a',
                  'departure_time_sec_interpolate'].tolist() == [1, 2, 3, 4, 5]

    # trip 'b' should be skipped because it has only one null value and
    # its in the last position but its null value should be removed
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

    # TODO: This is a rare and unlikely case that should be supported
    #  in the future and when addressed we expect [1, 2, 3, 4, 5] for trip 'f'
    #  trip 'f' should be interpolated fully,
    #  the one NA in the middle of the sequence should be filled
    # trip 'f' should be skipped because it has only one null value and
    # its not a first or last value in sequence, but its null value should
    # be removed
    assert df.loc[df.trip_id == 'f',
                  'departure_time_sec_interpolate'].tolist() == [1, 3, 4, 5]

    # trip 'g' should be interpolated
    # no starting value, so first time removed
    # NaN values should be removed from start
    assert df.loc[df.trip_id == 'g',
                  'departure_time_sec_interpolate'].tolist() == [2, 3, 4, 5]


def test_interpolator_w_missing_stop_sequence(stop_times, calendar):
    # create nulls in stop_times 'stop_sequence' col
    stop_times['stop_sequence'][1:4] = np.nan
    stop_times['stop_sequence'][10:12] = np.nan
    with pytest.raises(ValueError) as excinfo:
        df = gtfs_network._interpolate_stop_times(stop_times, calendar)
    expected_error = ("Found duplicate values when values from stop_sequence "
                      "and unique_trip_id are combined. Check values in "
                      "these columns for trip_id(s): "
                      "['a_citytrains', 'c_citytrains'].")
    assert expected_error in str(excinfo.value)


def test_interpolator_w_mismatch_trip_ids(stop_times, calendar):
    # create nulls in stop_times 'stop_sequence' col
    stop_times['trip_id'] = stop_times['trip_id'] + ' '

    with pytest.raises(ValueError) as excinfo:
        df = gtfs_network._interpolate_stop_times(stop_times, calendar)
    expected_error = ("No matching trip_ids where found. "
                      "Suggest checking for differences between trip_id "
                      "values in stop_times and trips GTFS files.")
    assert expected_error in str(excinfo.value)


def test_interpolator_w_index_as_col(stop_times, calendar):
    # set name on index that also exists as a col to run test
    stop_times.index.rename('unique_agency_id', inplace=True)
    df = gtfs_network._interpolate_stop_times(stop_times, calendar)
    # no errors should occur so only need to check df is not empty
    assert df.empty is False


def test_skip_interpolator(stop_times, calendar):
    series = pd.Series(data=[1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5,
                             1, 2, 3, 4, 5],
                             index = range(35),
                                     name = 'departure_time_sec')
    stop_times['departure_time_sec'] = series
    df = gtfs_network._interpolate_stop_times(stop_times, calendar)

    # everything should be the same,
    # with one row dropped for calendar day filter
    assert df.departure_time_sec_interpolate.tolist() == [1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5,
                                                          1, 2, 3, 4, 5]


def test_trip_schedule_selector_wo_cal_dates(gtfs_feed_wo_calendar_dates):
    expected_result = gtfs_feed_wo_calendar_dates.trips.copy()
    # create expected trips result
    expected_result.reset_index(drop=True, inplace=True)
    expected_result = expected_result.iloc[0:8]
    result = gtfs_network._trip_schedule_selector(
        input_trips_df=gtfs_feed_wo_calendar_dates.trips,
        input_calendar_df=gtfs_feed_wo_calendar_dates.calendar,
        input_calendar_dates_df=gtfs_feed_wo_calendar_dates.calendar_dates,
        day='monday',
        calendar_dates_lookup=None)

    assert len(result) == 8
    assert result.equals(expected_result)


def test_trip_schedule_selector_wo_cal_dates_wo_direction_id(
        gtfs_feed_wo_calendar_dates):
    # remove 'direction_id' col for test
    trips_df = gtfs_feed_wo_calendar_dates.trips.copy()
    trips_df.drop(columns=['direction_id'], inplace=True)
    expected_result = gtfs_feed_wo_calendar_dates.trips.copy()
    # create expected trips result
    expected_result.reset_index(drop=True, inplace=True)
    expected_result.drop(columns=['direction_id'], inplace=True)
    expected_result = expected_result.iloc[0:8]

    result = gtfs_network._trip_schedule_selector(
        input_trips_df=trips_df,
        input_calendar_df=gtfs_feed_wo_calendar_dates.calendar,
        input_calendar_dates_df=gtfs_feed_wo_calendar_dates.calendar_dates,
        day='monday',
        calendar_dates_lookup=None)

    assert len(result) == 8
    assert result.equals(expected_result)


def test_trip_schedule_selector_w_cal_dates(gtfs_feed_wo_calendar):
    expected_result = gtfs_feed_wo_calendar.trips.copy()
    # create expected trips result
    expected_result = expected_result.iloc[4:10]
    expected_result.reset_index(drop=True, inplace=True)
    result = gtfs_network._trip_schedule_selector(
        input_trips_df=gtfs_feed_wo_calendar.trips,
        input_calendar_df=gtfs_feed_wo_calendar.calendar,
        input_calendar_dates_df=gtfs_feed_wo_calendar.calendar_dates,
        day='sunday',
        calendar_dates_lookup={'schedule_type': 'WE',
                               'service_id': ['weekday-3', 'weekday-2']})

    assert len(result) == 6
    assert result.equals(expected_result)


def test_trip_schedule_selector_w_cal_and_cal_dates(
        gtfs_feed_w_calendar_and_calendar_dates):
    trips_df = gtfs_feed_w_calendar_and_calendar_dates.trips.copy()
    cal_df = gtfs_feed_w_calendar_and_calendar_dates.calendar.copy()
    cal_dates_df = gtfs_feed_w_calendar_and_calendar_dates.calendar_dates \
        .copy()
    expected_result = gtfs_feed_w_calendar_and_calendar_dates.trips.copy()
    result = gtfs_network._trip_schedule_selector(
        input_trips_df=trips_df,
        input_calendar_df=cal_df,
        input_calendar_dates_df=cal_dates_df,
        day='monday',
        calendar_dates_lookup={'schedule_type': 'WE'})

    assert len(result) == 10
    assert result.equals(expected_result)


def test_trip_schedule_selector_w_cal_and_cal_dates_wo_lookup(
        gtfs_feed_w_calendar_and_calendar_dates):
    trips_df_1 = gtfs_feed_w_calendar_and_calendar_dates.trips.copy()
    cal_df = gtfs_feed_w_calendar_and_calendar_dates.calendar.copy()
    cal_dates_df_1 = gtfs_feed_w_calendar_and_calendar_dates.calendar_dates \
        .copy()
    # create extra records in trips and calendar_dates for a different agency
    # that do not exist in the calendar table
    trips_df_2 = trips_df_1.copy()
    trips_df_2['unique_agency_id'] = trips_df_2['unique_agency_id'] + '_x'
    trips_df_2['unique_feed_id'] = trips_df_2['unique_feed_id'] + '_x'
    trips_df_2 = trips_df_2.iloc[0:8]
    trips_df_x2 = pd.concat(
        [trips_df_1, trips_df_2], axis=0,
        ignore_index=True)
    cal_dates_df_2 = cal_dates_df_1.copy()
    cal_dates_df_2['unique_agency_id'] = \
        cal_dates_df_2['unique_agency_id'] + '_x'
    cal_dates_df_2['unique_feed_id'] = \
        cal_dates_df_2['unique_feed_id'] + '_x'
    cal_dates_df_x2 = pd.concat(
        [cal_dates_df_1, cal_dates_df_2], axis=0,
        ignore_index=True)
    # create expected trips result
    expected_result = trips_df_1.copy()
    expected_result = expected_result.iloc[0:8]
    result = gtfs_network._trip_schedule_selector(
        input_trips_df=trips_df_x2,
        input_calendar_df=cal_df,
        input_calendar_dates_df=cal_dates_df_x2,
        day='monday',
        calendar_dates_lookup=None)

    assert len(result) == 8
    assert result.equals(expected_result)


def test_trip_schedule_selector_wo_cal_dates_invalid_params(
        gtfs_feed_wo_calendar_dates):
    gtfs_feed = gtfs_feed_wo_calendar_dates
    # test with invalid 'day' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed.trips,
            input_calendar_df=gtfs_feed.calendar,
            input_calendar_dates_df=gtfs_feed.calendar_dates,
            day='monday ',
            calendar_dates_lookup=None)
    expected_error = (
        "Incorrect day specified. Must be one of lowercase strings: 'monday',"
        " 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'.")
    assert expected_error in str(excinfo.value)
    # test with invalid 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed.trips,
            input_calendar_df=gtfs_feed.calendar,
            input_calendar_dates_df=gtfs_feed.calendar_dates,
            day='monday',
            calendar_dates_lookup=['invalid'])
    expected_error = "calendar_dates_lookup parameter must be a dictionary."
    assert expected_error in str(excinfo.value)
    # test with invalid 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed.trips,
            input_calendar_df=gtfs_feed.calendar,
            input_calendar_dates_df=gtfs_feed.calendar_dates,
            day='monday',
            calendar_dates_lookup={1: 'WD'})
    expected_error = "calendar_dates_lookup key: 1 must be a string."
    assert expected_error in str(excinfo.value)
    # test with invalid 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed.trips,
            input_calendar_df=gtfs_feed.calendar,
            input_calendar_dates_df=gtfs_feed.calendar_dates,
            day='monday',
            calendar_dates_lookup={'schedule_type': 1})
    expected_error = ("calendar_dates_lookup value: 1 must be a "
                      "string or a list of strings.")
    assert expected_error in str(excinfo.value)
    # test with invalid 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed.trips,
            input_calendar_df=gtfs_feed.calendar,
            input_calendar_dates_df=gtfs_feed.calendar_dates,
            day='monday',
            calendar_dates_lookup={'schedule_type': ['WD', 1]})
    expected_error = ("calendar_dates_lookup value: ['WD', 1] "
                      "must contain strings.")
    assert expected_error in str(excinfo.value)


def test_trip_schedule_selector_w_cal_dates_invalid_params_1(
        gtfs_feed_wo_calendar_dates):
    # test with empty 'calendar_dates'df
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed_wo_calendar_dates.trips,
            input_calendar_df=gtfs_feed_wo_calendar_dates.calendar,
            input_calendar_dates_df=gtfs_feed_wo_calendar_dates.calendar_dates,
            day='monday',
            calendar_dates_lookup={'schedule_type': 'WD'})
    expected_error = ("calendar_dates_df is empty. Unable to use the "
                      "calendar_dates_lookup parameter.")
    assert expected_error in str(excinfo.value)


def test_trip_schedule_selector_w_cal_dates_invalid_params_2(
        gtfs_feed_wo_calendar):
    # create invalid data in calendar dates file
    cal_dates_df = gtfs_feed_wo_calendar.calendar_dates.copy()
    series = pd.Series(
        data=[1, 1, 2, 2], index=range(4),
        name='invalid_dtype')
    cal_dates_df['invalid_dtype'] = series
    series = pd.Series(
        data=[10, 11, 10, 'aa'], index=range(4),
        name='day_type')
    cal_dates_df['day_type'] = series

    # test with invalid col in 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed_wo_calendar.trips,
            input_calendar_df=gtfs_feed_wo_calendar.calendar,
            input_calendar_dates_df=cal_dates_df,
            day='monday',
            calendar_dates_lookup={'invalid_col': 'WD'})
    expected_error = ("Column: invalid_col not found in calendar_dates "
                      "dataframe.")
    assert expected_error in str(excinfo.value)
    # test with invalid col dtype in 'calendar_dates_lookup' param
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._trip_schedule_selector(
            input_trips_df=gtfs_feed_wo_calendar.trips,
            input_calendar_df=gtfs_feed_wo_calendar.calendar,
            input_calendar_dates_df=cal_dates_df,
            day='monday',
            calendar_dates_lookup={'invalid_dtype': '1'})
    expected_error = ("Column: invalid_dtype must be object type.")
    assert expected_error in str(excinfo.value)


def test_time_selector(selected_int_stop_times_from_feed_wo_calendar_dates):
    timerange = ['08:20:00', '08:35:00']
    stop_times_int = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    result = gtfs_network._time_selector(
        df=stop_times_int,
        starttime=timerange[0],
        endtime=timerange[1])

    # create expected subset result
    expected_result = stop_times_int.loc[14:15]
    assert len(result) == 2
    assert result.equals(expected_result)


def test_time_difference(selected_int_stop_times_from_feed_wo_calendar_dates):
    expected_result = \
        selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    stop_times_int = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    # create the 'stop_times_int' df expected
    stop_times_int.drop(columns=['timediff'], inplace=True)
    result = gtfs_network._time_difference(stop_times_df=stop_times_int)

    assert 'timediff' in result.columns
    # all rows in sequence should not be null
    assert result['timediff'][1:6].isnull().sum() == 0
    # only the first row in sequence should be null
    assert result['timediff'][0:1].isnull().sum() == 1
    assert result.equals(expected_result)


def test_format_transit_net_edge_test_1(stop_times_interpolated):
    df = gtfs_network._format_transit_net_edge(stop_times_interpolated)

    # length of edge df should be 16
    assert len(df) == 16

    # sequence ID should be numeric starting at 1 and end at 4 for each trip
    assert df['sequence'][0] == 1 and df['sequence'][3] == 4

    # edge df should have these columns and no null values
    for col in ['node_id_from', 'node_id_to', 'weight']:
        assert col in df.columns and df[
            col].isnull().values.any() == False  # noqa

    # there should be 4 edges per trip ID
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


def test_format_transit_net_edge_test_2(
        selected_int_stop_times_from_feed_wo_calendar_dates,
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1):
    expected_result = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1.copy()

    # create the 'selected_interpolated_stop_times_df' that is expected
    stop_times_int = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    # there are no missing time values in the test data so just use
    # 'departure_time_sec' to generate the timediff col for the test
    stop_times_int['timediff'] = stop_times_int.groupby('unique_trip_id')[
        'departure_time_sec'].diff()
    result = gtfs_network._format_transit_net_edge(stop_times_int)

    # test that output df is identical to expected df
    result = result.reindex(
        sorted(result.columns), axis=1)
    expected_result = expected_result.reindex(
        sorted(expected_result.columns), axis=1)
    assert result.equals(expected_result)


def test_convert_imp_time_units(
        transit_edge_from_feed_wo_calendar_dates):
    # test with minutes
    result_min = gtfs_network._convert_imp_time_units(
        df=transit_edge_from_feed_wo_calendar_dates,
        time_col='weight', convert_to='minutes')
    expected_weight_as_min = pd.Series(
        data=[5.0] * 5, index=range(5), name='weight')
    assert result_min['weight'].equals(expected_weight_as_min)

    # test with seconds
    # convert original weight of min to sec
    transit_edge_from_feed_wo_calendar_dates['weight'] = expected_weight_as_min
    result_sec = gtfs_network._convert_imp_time_units(
        df=transit_edge_from_feed_wo_calendar_dates,
        time_col='weight', convert_to='seconds')
    expected_weight_as_sec = pd.Series(
        data=[300.0] * 5, index=range(5), name='weight')
    assert result_sec['weight'].equals(expected_weight_as_sec)


def test_convert_imp_time_units_invalid_params(
        transit_edge_from_feed_wo_calendar_dates):
    # test with invalid 'convert_to' param name
    with pytest.raises(ValueError) as excinfo:
        result_min = gtfs_network._convert_imp_time_units(
            df=transit_edge_from_feed_wo_calendar_dates,
            time_col='weight', convert_to='minutes_invalid')
    expected_error = ("minutes_invalid is not a valid value "
                      "or is not a string.")
    assert expected_error in str(excinfo.value)
    # test with invalid 'convert_to' dtype
    with pytest.raises(ValueError) as excinfo:
        result_min = gtfs_network._convert_imp_time_units(
            df=transit_edge_from_feed_wo_calendar_dates,
            time_col='weight', convert_to=22)
    expected_error = "22 is not a valid value or is not a string."
    assert expected_error in str(excinfo.value)


def test_stops_in_edge_table_selector(
        gtfs_feed_wo_calendar_dates,
        selected_int_stop_times_from_feed_wo_calendar_dates):
    # created expected result
    expected_result = gtfs_feed_wo_calendar_dates.stops[0:6]
    expected_result['unique_stop_id'] = (
        expected_result['stop_id'].str.cat(
            expected_result['unique_agency_id'].astype('str'), sep='_'))

    result = gtfs_network._stops_in_edge_table_selector(
        input_stops_df=gtfs_feed_wo_calendar_dates.stops,
        input_stop_times_df=selected_int_stop_times_from_feed_wo_calendar_dates
    )

    assert 'unique_stop_id' in result.columns
    assert result['unique_stop_id'].isnull().sum() == 0
    assert result.equals(expected_result)


def test_format_transit_net_nodes(
        selected_stops_from_feed_wo_calendar_dates,
        expected_transit_node_from_feed_wo_calendar_dates):
    expected_result = expected_transit_node_from_feed_wo_calendar_dates.copy()
    expected_cols = ['x', 'y', 'unique_agency_id', 'route_type', 'stop_id',
                     'stop_name']

    result = gtfs_network._format_transit_net_nodes(
        df=selected_stops_from_feed_wo_calendar_dates)

    for col in expected_cols:
        assert col in result.columns
        assert result[col].isnull().sum() == 0
    assert result.index.name == 'node_id'
    assert result.index.isnull().sum() == 0
    # round result to ensure decimal place match
    result['x'] = result['x'].round(decimals=6)
    result['y'] = result['y'].round(decimals=6)
    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result.sort_index(axis=1, inplace=True)
    assert result.equals(expected_result)


def test_route_type_to_edge(
        gtfs_feed_wo_calendar_dates,
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2):
    expected_result = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2.copy()
    input_edge_df = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2.copy()

    # 'route_type' is added in this function and is not expected to already
    # exist
    input_edge_df.drop(columns=['route_type'], inplace=True)

    result = gtfs_network._route_type_to_edge(
        transit_edge_df=input_edge_df,
        stop_time_df=gtfs_feed_wo_calendar_dates.stop_times)
    assert 'route_type' in result.columns
    assert result['route_type'].isnull().sum() == 0
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result.sort_index(axis=1, inplace=True)
    assert result.equals(expected_result)


def test_route_id_to_edge(
        gtfs_feed_wo_calendar_dates,
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2):
    expected_result = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2.copy()
    series = pd.Series(
        data=['10-101_agency_a_city_a'] * 5, index=range(5),
        name='unique_route_id')
    expected_result['unique_route_id'] = series
    input_edge_df = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2.copy()

    result = gtfs_network._route_id_to_edge(
        transit_edge_df=input_edge_df,
        trips_df=gtfs_feed_wo_calendar_dates.trips)
    assert 'unique_route_id' in result.columns
    assert result['unique_route_id'].isnull().sum() == 0
    assert result.equals(expected_result)


def test_check_if_index_name_in_cols_False(
        selected_stops_from_feed_wo_calendar_dates):
    result = gtfs_network._check_if_index_name_in_cols(
        selected_stops_from_feed_wo_calendar_dates)
    assert isinstance(result, bool)
    assert result is False


def test_check_if_index_name_in_cols_True(
        selected_stops_from_feed_wo_calendar_dates):
    selected_stops_from_feed_wo_calendar_dates.reset_index(inplace=True)
    selected_stops_from_feed_wo_calendar_dates.set_index(
        'node_id', drop=False, inplace=True)

    result = gtfs_network._check_if_index_name_in_cols(
        selected_stops_from_feed_wo_calendar_dates)
    assert isinstance(result, bool)
    assert result is True


def test_edge_impedance_by_route_type(edge_route_type_impedance_df):
    df = edge_route_type_impedance_df.copy()
    result = gtfs_network.edge_impedance_by_route_type(
        edge_route_type_impedance_df,
        underground_rail=0.5,
        intercity_rail=-0.5)
    # route_id 1 weight should increase via multiplier
    assert (result.weight.iloc[0:3] == df.weight.iloc[0:3] + (
            df.weight.iloc[0:3] * 0.5)).all()
    # route_id 2 weight should decrease via multiplier
    assert (result.weight.iloc[3:6] == df.weight.iloc[3:6] + (
            df.weight.iloc[3:6] * -0.5)).all()
    # route_id 3 weight should not change
    assert (result.weight.iloc[6:9] == df.weight.iloc[6:9]).all()


def test_edge_impedance_by_route_type_invalid_params(
        edge_route_type_impedance_df):
    # test with multiplier outside of optimal range
    result = gtfs_network.edge_impedance_by_route_type(
        edge_route_type_impedance_df,
        underground_rail=2.0,
        intercity_rail=-3.0)
    # should return a result even if multiplier is not in optimal range
    assert result.empty is False
    # test with weight param as invalid dtype
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network.edge_impedance_by_route_type(
            edge_route_type_impedance_df,
            travel_time_col_name=2,
            underground_rail=0.5,
            intercity_rail=-0.5)
    expected_error = "travel_time_col_name must be a string."
    assert expected_error in str(excinfo.value)
    # test with weight param as invalid dtype
    # create str weight column
    edge_route_type_impedance_df['travel_time'] = '1'
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network.edge_impedance_by_route_type(
            edge_route_type_impedance_df,
            travel_time_col_name='travel_time',
            underground_rail=0.5,
            intercity_rail=-0.5)
    expected_error = "travel_time must be a number."
    assert expected_error in str(excinfo.value)
    # test with weight column that cant be found in DataFrame
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network.edge_impedance_by_route_type(
            edge_route_type_impedance_df,
            travel_time_col_name='time',
            underground_rail=0.5,
            intercity_rail=-0.5)
    expected_error = ("Column: time was not found in "
                      "transit_edge_df DataFrame and is required.")
    assert expected_error in str(excinfo.value)
    # test with multiplier value as str
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network.edge_impedance_by_route_type(
            edge_route_type_impedance_df,
            underground_rail='1',
            intercity_rail=-0.5)
    expected_error = "One or more multiplier variables are not float."
    assert expected_error in str(excinfo.value)
    # test with route type that is not found in DataFrame
    result = gtfs_network.edge_impedance_by_route_type(
        edge_route_type_impedance_df,
        underground_rail=0.5,
        funicular=-0.5)
    # should return a result even if route type is not found in DataFrame
    assert result.empty is False


def test_save_processed_gtfs_data(
        tmpdir,
        selected_int_stop_times_from_feed_wo_calendar_dates,
        gtfs_feed_wo_calendar_dates):
    # add stop_times_int to UA object which is required for saving HDF5
    gtfs_feed_wo_calendar_dates.stop_times_int = \
        selected_int_stop_times_from_feed_wo_calendar_dates
    dir_path = os.path.join(tmpdir.strpath, 'test_hdf5_save')
    os.makedirs(dir_path)
    print('preparing test dir: {}'.format(dir_path))

    gtfs_network.save_processed_gtfs_data(
        gtfs_feed_wo_calendar_dates,
        filename='test_file.h5', dir=dir_path)
    # test that file was written as expected
    file_list = glob.glob(r"{}/*.h5".format(dir_path))
    file_path = file_list[0]
    file_name = os.path.basename(file_path)
    assert file_name == 'test_file.h5'
    # test HDF5 store
    expected_keys = {'/calendar', '/routes', '/stop_times', '/stop_times_int',
                     '/stops', '/trips'}
    with pd.HDFStore(file_path) as store:
        result_keys = set(store.keys())
        assert result_keys == expected_keys
        # check that data exists in each DataFrame
        for key in expected_keys:
            df = store[key]
            assert df.empty is False


def test_save_processed_gtfs_data_invalid_params(
        tmpdir,
        gtfs_feed_wo_calendar_dates,
        selected_int_stop_times_from_feed_wo_calendar_dates):
    dir_path = os.path.join(tmpdir.strpath, 'test_hdf5_save')
    os.makedirs(dir_path)
    print('preparing test dir: {}'.format(dir_path))
    # test with missing req DataFrame: stop_times_int
    gtfs_feed_wo_calendar_dates.stop_times_int = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        gtfs_network.save_processed_gtfs_data(
            gtfs_feed_wo_calendar_dates,
            filename='test_file.h5', dir=dir_path)
    expected_error = ('gtfsfeeds_dfs is missing required '
                      'DataFrame: stop_times_int.')
    assert expected_error in str(excinfo.value)

    # set stop_times_int df for test
    gtfs_feed_wo_calendar_dates.stop_times_int = \
        selected_int_stop_times_from_feed_wo_calendar_dates
    # set calendar df to blank df for test
    gtfs_feed_wo_calendar_dates.calendar = pd.DataFrame()
    with pytest.raises(ValueError) as excinfo:
        gtfs_network.save_processed_gtfs_data(
            gtfs_feed_wo_calendar_dates,
            filename='test_file.h5', dir=dir_path)
    expected_error = ('gtfsfeeds_dfs is missing either the calendar or '
                      'calendar_dates DataFrame.')
    assert expected_error in str(excinfo.value)

    # test with incorrect dtype as param
    with pytest.raises(ValueError) as excinfo:
        gtfs_network.save_processed_gtfs_data(
            'invalid_param',
            filename='test_file.h5', dir=dir_path)
    expected_error = ('gtfsfeeds_dfs must be an urbanaccess_gtfs_df '
                      'object.')
    assert expected_error in str(excinfo.value)


def test_load_processed_gtfs_data(
        hdf5_file_on_disk_gtfsfeeds_dfs, expected_gtfsfeeds_dfs_keys):
    gtfsfeeds_dfs = gtfs_network.load_processed_gtfs_data(
        filename='test_file.h5', dir=hdf5_file_on_disk_gtfsfeeds_dfs)
    assert isinstance(gtfsfeeds_dfs, urbanaccess_gtfs_df)
    urbanaccess_gtfs_df_info = vars(gtfsfeeds_dfs)

    assert expected_gtfsfeeds_dfs_keys == sorted(
        list(urbanaccess_gtfs_df_info.keys()))
    # headways and calendar_dates were not written to HDF5 so we dont
    # expect them in this test
    expected_dfs = ['stops', 'routes', 'trips', 'stop_times', 'calendar',
                    'stop_times_int']
    expected_dfs_empty = ['calendar_dates', 'headways']
    for key, value in urbanaccess_gtfs_df_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False
        # check that df is empty
        if key in expected_dfs_empty:
            assert value.empty
