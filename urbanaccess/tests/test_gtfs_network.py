import pytest
import os
import time
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
def gtfs_feed_w_calendar_and_calendar_dates_multi_agency(
        agency_b_feed_on_disk_w_calendar_and_calendar_dates):
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=agency_b_feed_on_disk_w_calendar_and_calendar_dates,
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
def selected_int_stop_times_from_feed_wo_calendar_dates_for_timepad(
        gtfs_feed_wo_calendar_dates):
    stop_times = gtfs_feed_wo_calendar_dates.stop_times.copy()
    trip_ids = ['a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'c1', 'c2']
    stop_times_subset = stop_times.loc[stop_times['trip_id'].isin(trip_ids)]
    stop_times_subset = stop_times_subset.loc[
        ((stop_times_subset['departure_time_sec'] >= 22500) & (
                stop_times_subset['departure_time_sec'] <= 96000)) | (
            stop_times_subset['departure_time_sec'].isnull())]
    stop_times_subset['unique_stop_id'] = (
        stop_times_subset['stop_id'].str.cat(
            stop_times_subset['unique_agency_id'].astype('str'), sep='_'))
    stop_times_subset['departure_time_sec_interpolate'] = stop_times_subset[
        'departure_time_sec']
    update_dict = {2: 23100, 3: 23400, 26: 23100, 27: 23400, 38: 95100,
                   39: 95400}
    for index, value in update_dict.items():
        stop_times_subset.at[index, 'departure_time_sec_interpolate'] = value
    update_dict = {0: np.nan, 6: np.nan, 11: 600.0, 12: np.nan, 18: np.nan,
                   24: np.nan, 30: np.nan, 35: 600.0, 36: np.nan, 42: np.nan,
                   47: 600.0}
    stop_times_subset['timediff'] = 300.0
    for index, value in update_dict.items():
        stop_times_subset.at[index, 'timediff'] = value

    return stop_times_subset


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
        'stop_sequence': [1, 2, 3, 4, 5] * 4,
        'arrival_time': ['08:15:00', '08:20:00', '08:25:00', '08:30:00',
                         '08:35:00'] * 4,
        'departure_time': ['08:15:00', '08:20:00', '08:25:00', '08:30:00',
                           '08:35:00'] * 4
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
def expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2_timeaware():  # noqa
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
        'route_type': [3] * 5,
        'departure_time': ['08:15:00', '08:20:00', '08:25:00', '08:30:00',
                           '08:35:00'],
        'arrival_time': ['08:20:00', '08:25:00', '08:30:00', '08:35:00',
                         '08:40:00']
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    # raw data are read as int32
    df['sequence'] = df['sequence'].astype('int32')
    return df


@pytest.fixture
def expected_final_transit_edge_from_feed_wo_calendar():
    # represents df after it has been post-processed downstream
    data = {
        'node_id_from': ['6_agency_a_city_a', '5_agency_a_city_a',
                         '4_agency_a_city_a', '3_agency_a_city_a'],
        'node_id_to': ['5_agency_a_city_a', '4_agency_a_city_a',
                       '3_agency_a_city_a', '1_agency_a_city_a'],
        'weight': [5.0, 5.0, 5.0, 10.0],
        'unique_agency_id': ['agency_a_city_a'] * 4,
        'unique_trip_id': ['c2_agency_a_city_a'] * 4,
        'sequence': range(1, 5),
        'unique_route_id': ['12-101_agency_a_city_a'] * 4,
        'id': ['c2_agency_a_city_a_1', 'c2_agency_a_city_a_2',
               'c2_agency_a_city_a_3', 'c2_agency_a_city_a_4'],
        'route_type': [1] * 4,
        'net_type': ['transit'] * 4
    }
    index = range(4)
    df = pd.DataFrame(data, index)
    # raw data are read as int32
    df['sequence'] = df['sequence'].astype('int32')
    return df


@pytest.fixture
def expected_final_transit_edge_from_feed_w_cal_and_cal_dates(
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2):
    # represents df after it has been post-processed downstream
    data = {
        'unique_route_id': ['10-101_agency_a_city_a'] * 5,
        'net_type': ['transit'] * 5
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    df = pd.concat(
        [expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2,
         df],
        axis=1)
    return df


@pytest.fixture
def expected_transit_edge_from_agency_b_feed_w_cal_and_cal_dates():
    # represents df after it has been post-processed downstream
    data = {
        'node_id_from': ['60_agency_b_district_1', '61_agency_b_district_1',
                         '62_agency_b_district_1', '63_agency_b_district_1',
                         '64_agency_b_district_1'],
        'node_id_to': ['61_agency_b_district_1', '62_agency_b_district_1',
                       '63_agency_b_district_1', '64_agency_b_district_1',
                       '65_agency_b_district_1'],
        'weight': [5.0] * 5,
        'unique_agency_id': ['agency_b_district_1'] * 5,
        'unique_trip_id': ['13_agency_b_district_1'] * 5,
        'sequence': range(1, 6),
        'unique_route_id': ['40-4_agency_b_district_1'] * 5,
        'id': ['13_agency_b_district_1_1', '13_agency_b_district_1_2',
               '13_agency_b_district_1_3', '13_agency_b_district_1_4',
               '13_agency_b_district_1_5'],
        'route_type': [3] * 5,
        'net_type': ['transit'] * 5
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    # raw data are read as int32
    df['sequence'] = df['sequence'].astype('int32')
    return df


@pytest.fixture
def expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1_timeaware(
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2_timeaware):  # noqa
    # represents df prior to being post-processed downstream
    df = expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2_timeaware.copy()  # noqa
    df.drop(columns=['route_type'], inplace=True)
    # convert weight from min to sec to represent df prior to
    # post-process step
    df['weight'] = 300.0

    return df


@pytest.fixture
def expected_final_transit_edge_from_feed_wo_calendar_dates_timeaware(
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2_timeaware):  # noqa
    data = {
        'unique_route_id': ['10-101_agency_a_city_a'] * 5,
        'net_type': ['transit'] * 5
    }
    index = range(5)
    df = pd.DataFrame(data, index)
    df = pd.concat(
        [expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_2_timeaware,  # noqa
        df],
        axis=1)
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
def expected_final_transit_edge_from_feed_wo_calendar_dates_timepad(
        expected_final_transit_edge_from_feed_wo_calendar_dates):
    # create expected edge table which includes a trip in the reverse
    # direction from trip a4
    df = expected_final_transit_edge_from_feed_wo_calendar_dates.copy()
    df['node_id_from_2'] = df['node_id_to']
    df['node_id_to_2'] = df['node_id_from']
    df.drop(columns=['node_id_from', 'node_id_to'], inplace=True)
    df.rename(columns={'node_id_from_2': 'node_id_from',
                       'node_id_to_2': 'node_id_to'}, inplace=True)
    df.sort_values(by=['id'], inplace=True, ascending=False)
    df['sequence'] = [1, 2, 3, 4, 5]
    df['unique_trip_id'] = df['unique_trip_id'].str.slice_replace(0, 2, 'a4')
    df['id'] = (
        df['unique_trip_id'].str.cat(
            df['sequence'].astype('str'), sep='_'))
    df = pd.concat(
        [expected_final_transit_edge_from_feed_wo_calendar_dates, df],
        axis=0, ignore_index=True)
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


@pytest.fixture
def edges_empty(transit_edges_to_simplify):
    df_empty = transit_edges_to_simplify[0:0]
    return df_empty


@pytest.fixture
def transit_edges_to_simplify():
    data = {
        'node_id_from': [
            '1_agency_a_city_a', '2_agency_a_city_a', '3_agency_a_city_a',
            '4_agency_a_city_a', '3_agency_a_city_a', '2_agency_a_city_a',
            '5_agency_a_city_a', '6_agency_a_city_a', '7_agency_a_city_a',
            '5_agency_a_city_a', '6_agency_a_city_a', '7_agency_a_city_a',
            '8_agency_a_city_a', '9_agency_a_city_a', '10_agency_a_city_a',
            '8_agency_a_city_a', '9_agency_a_city_a', '10_agency_a_city_a'
        ],
        'node_id_to': [
            '2_agency_a_city_a', '3_agency_a_city_a', '4_agency_a_city_a',
            '3_agency_a_city_a', '2_agency_a_city_a', '1_agency_a_city_a',
            '6_agency_a_city_a', '7_agency_a_city_a', '8_agency_a_city_a',
            '6_agency_a_city_a', '7_agency_a_city_a', '8_agency_a_city_a',
            '9_agency_a_city_a', '10_agency_a_city_a', '11_agency_a_city_a',
            '9_agency_a_city_a', '10_agency_a_city_a', '11_agency_a_city_a'],
        'weight': [2.0, 4.0, 5.0,
                   5.0, 4.0, 2.0,
                   2.0, 2.0, 3.0,
                   2.0, 2.0, 3.0,
                   4.0, 3.0, 4.0,
                   4.0, 3.0, 4.0],
        'unique_agency_id': ['agency_a_city_a'] * 18,
        'unique_trip_id': [
            '1_agency_a_city_a', '1_agency_a_city_a', '1_agency_a_city_a',
            '2_agency_a_city_a', '2_agency_a_city_a', '2_agency_a_city_a',
            '3_agency_a_city_a', '3_agency_a_city_a', '3_agency_a_city_a',
            '4_agency_a_city_a', '4_agency_a_city_a', '4_agency_a_city_a',
            '5_agency_a_city_a', '5_agency_a_city_a', '5_agency_a_city_a',
            '6_agency_a_city_a', '6_agency_a_city_a', '6_agency_a_city_a'],
        'sequence': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3],
        'unique_route_id': [
            'R1_agency_a_city_a', 'R1_agency_a_city_a', 'R1_agency_a_city_a',
            'R1_agency_a_city_a', 'R1_agency_a_city_a', 'R1_agency_a_city_a',
            'A1_agency_a_city_a', 'A1_agency_a_city_a', 'A1_agency_a_city_a',
            'A1_agency_a_city_a', 'A1_agency_a_city_a', 'A1_agency_a_city_a',
            'B1_agency_a_city_a', 'B1_agency_a_city_a', 'B1_agency_a_city_a',
            'C1_agency_a_city_a', 'C1_agency_a_city_a', 'C1_agency_a_city_a'],
        'route_type': [1] * 18,
        'net_type': ['transit'] * 18
    }
    index = range(18)
    df = pd.DataFrame(data, index)
    df['id'] = (df['unique_trip_id'].str.cat(
        df['sequence'].astype('str'), sep='_'))
    return df


@pytest.fixture
def transit_nodes_to_simplify():
    data = {
        'node_id': [
            '1_agency_a_city_a', '2_agency_a_city_a', '3_agency_a_city_a',
            '4_agency_a_city_a', '5_agency_a_city_a', '6_agency_a_city_a',
            '7_agency_a_city_a', '8_agency_a_city_a', '9_agency_a_city_a',
            '10_agency_a_city_a', '11_agency_a_city_a'],
        'x': [-122.268962, -122.273627, -122.276709, -122.272114,
              -122.266589, -122.268842, -122.271760, -122.265859,
              -122.274823, -122.271057, -122.266551],
        'y': [37.807730, 37.800201, 37.795430, 37.793342,
              37.806846, 37.803413, 37.798581, 37.796250,
              37.805727, 37.804455, 37.802497],
        'unique_agency_id': ['agency_a_city_a'] * 11,
        'route_type': [1] * 11,
        'stop_id': range(1, 12),
        'stop_name': [
            'ave a', 'ave b', 'ave c', 'ave d', 'ave 1', 'ave 2',
            'ave 3', 'ave 4', 'st a', 'st b', 'st c'],
        'net_type': ['transit'] * 11
    }
    index = range(11)
    df = pd.DataFrame(data, index)
    df.set_index('node_id', drop=True, inplace=True)
    return df


@pytest.fixture
def transit_nodes_to_simplify_extra_nodes(transit_nodes_to_simplify):
    data = {
        'node_id': ['12_agency_a_city_a'],
        'x': [-122.266551],
        'y': [37.802497],
        'unique_agency_id': ['agency_a_city_a'],
        'route_type': [1],
        'stop_id': [13],
        'stop_name': ['st d'],
        'net_type': ['transit']
    }
    index = range(1)
    df = pd.DataFrame(data, index)
    df.set_index('node_id', drop=True, inplace=True)
    df = pd.concat([transit_nodes_to_simplify, df], axis=1)

    return df


@pytest.fixture
def stop_times_empty(selected_int_stop_times_from_feed_wo_calendar_dates):
    df_empty = selected_int_stop_times_from_feed_wo_calendar_dates[0:0]
    return df_empty


def test_create_transit_net_wo_calendar_dates_w_high_freq_trips(
        gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar_dates):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar_dates.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day=None,
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        timerange_pad=None,
        time_aware=False,
        date=None,
        date_range=None,
        use_highest_freq_trips_date=True,
        simplify=False)
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_wo_calendar(
        gtfs_feed_wo_calendar,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar
):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar, day=None,
        timerange=['00:00:00', '23:59:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        date='2016-04-24')
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_w_calendar_and_calendar_dates(
        gtfs_feed_w_calendar_and_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_w_cal_and_cal_dates):
    expected_result = \
        expected_final_transit_edge_from_feed_w_cal_and_cal_dates.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_w_calendar_and_calendar_dates, day=None,
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        date_range=['2016-12-24', '2017-03-18'])
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_w_calendar_and_calendar_dates_w_simplify(
        gtfs_feed_w_calendar_and_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_w_cal_and_cal_dates):
    # TODO: note that gtfs_feed_w_calendar_and_calendar_dates does not
    #  result in any edges that require simplification. Test can be improved
    #  by changing the test data to one that can be simplified.
    expected_result = \
        expected_final_transit_edge_from_feed_w_cal_and_cal_dates.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_w_calendar_and_calendar_dates, day=None,
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        date_range=['2016-12-24', '2017-03-18'],
        simplify=True)
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_w_calendar_and_calendar_dates_multi_agency(
        gtfs_feed_w_calendar_and_calendar_dates_multi_agency,
        expected_urbanaccess_network_keys,
        expected_transit_edge_from_agency_b_feed_w_cal_and_cal_dates):
    expected_result = \
        expected_transit_edge_from_agency_b_feed_w_cal_and_cal_dates.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_w_calendar_and_calendar_dates_multi_agency, day='monday',
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_wo_calendar_dates_timepad(
        gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar_dates_timepad):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar_dates_timepad.copy()
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        timerange_pad='06:00:00',
        time_aware=False)
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    expected_result['sequence'] = expected_result['sequence'].astype('int32')
    assert result_edge.equals(expected_result)


def test_create_transit_net_wo_calendar_dates_timeaware(
        gtfs_feed_wo_calendar_dates,
        expected_urbanaccess_network_keys,
        expected_final_transit_edge_from_feed_wo_calendar_dates_timeaware):
    expected_result = \
        expected_final_transit_edge_from_feed_wo_calendar_dates_timeaware\
            .copy()  # noqa
    transit_net = gtfs_network.create_transit_net(
        gtfs_feed_wo_calendar_dates, day='monday',
        timerange=['07:00:00', '10:00:00'],
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=None,
        save_filename=None,
        timerange_pad=None,
        time_aware=True)
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
    # check if expected timeware cols are in result
    expected_timeaware_cols = ['arrival_time', 'departure_time']
    assert all(col in result_edge.columns for col in expected_timeaware_cols)
    # test that output df is identical to expected df
    result_edge = result_edge.reindex(
        sorted(result_edge.columns), axis=1)
    expected_result = expected_result.reindex(
        sorted(expected_result.columns), axis=1)
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
    expected_result['sequence'] = expected_result['sequence'].astype('int32')
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result_edge['sequence'] = result_edge['sequence'].astype('int32')
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
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None,
            timerange_pad=None,
            time_aware=False,
            date=None,
            date_range=None,
            use_highest_freq_trips_date=True,
            simplify=False)
    expected_error = ("Only one parameter: 'use_highest_freq_trips_date' "
                      "or one of 'day', 'date', or 'date_range' can be "
                      "used at a time or both 'day' and 'date_range' can "
                      "be used.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None,
            timerange_pad=None,
            time_aware=6)
    expected_error = "time_aware must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        transit_net = gtfs_network.create_transit_net(
            gtfs_feed_wo_calendar_dates, day='monday',
            timerange=['07:00:00', '10:00:00'],
            calendar_dates_lookup=None,
            overwrite_existing_stop_times_int=False,
            use_existing_stop_times_int=False,
            save_processed_gtfs=False,
            save_dir=None,
            save_filename=None,
            timerange_pad=0.4,
            time_aware=False)
    expected_error = "timerange_pad must be string."
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
    file_list = glob.glob(os.path.join(dir_path, '*.h5'))
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
    # profile run times as _interpolate_stop_times() is a
    # function that is critical to have fast run times
    start_time = time.time()
    df = gtfs_network._interpolate_stop_times(stop_times, calendar)
    print('Run time: {}'.format(time.time() - start_time))

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
                       index=range(35),
                       name='departure_time_sec')
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


def test_time_selector_wo_timerange_pad(
        selected_int_stop_times_from_feed_wo_calendar_dates):
    timerange = ['08:20:00', '08:35:00']
    stop_times_int = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    result = gtfs_network._time_selector(
        df=stop_times_int,
        starttime=timerange[0],
        endtime=timerange[1])

    # create expected subset result
    expected_result = stop_times_int.loc[13:16]
    assert len(result) == 4
    assert result.equals(expected_result)


def test_time_selector_w_timerange_pad(
        selected_int_stop_times_from_feed_wo_calendar_dates_for_timepad):
    timerange = ['07:00:00', '10:00:00']
    stop_times_int = \
        selected_int_stop_times_from_feed_wo_calendar_dates_for_timepad.copy()
    result = gtfs_network._time_selector(
        df=stop_times_int,
        starttime=timerange[0],
        endtime=timerange[1],
        timerange_pad='06:00:00')
    # create expected subset result
    expected_result = stop_times_int.loc[
        stop_times_int.index.isin(
            [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23])]
    assert len(result) == 12
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


def test_format_transit_net_edge_test_1_timeaware_False(
        stop_times_interpolated):
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


def test_format_transit_net_edge_test_1_timeaware_True(
        stop_times_interpolated):
    df = gtfs_network._format_transit_net_edge(stop_times_interpolated,
                                               time_aware=True)

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

    assert df['departure_time'][0] == stop_times_interpolated[
        'departure_time'][0]  # noqa
    assert df['arrival_time'][0] == stop_times_interpolated[
        'arrival_time'][1]  # noqa


def test_format_transit_net_edge_test_2_timeaware_False(
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
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result['sequence'] = result['sequence'].astype('int32')
    assert result.equals(expected_result)


def test_format_transit_net_edge_timeaware_True(
        selected_int_stop_times_from_feed_wo_calendar_dates,
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1_timeaware):  # noqa
    expected_result = \
        expected_transit_edge_from_feed_wo_calendar_dates_process_lvl_1_timeaware.copy()  # noqa

    # create the 'selected_interpolated_stop_times_df' that is expected
    stop_times_int = selected_int_stop_times_from_feed_wo_calendar_dates.copy()
    # there are no missing time values in the test data so just use
    # 'departure_time_sec' to generate the timediff col for the test
    stop_times_int['timediff'] = stop_times_int.groupby('unique_trip_id')[
        'departure_time_sec'].diff()
    result = gtfs_network._format_transit_net_edge(stop_times_int,
                                                   time_aware=True)

    # check if expected timeware cols are in result
    expected_timeaware_cols = ['arrival_time', 'departure_time']
    assert all(col in result.columns for col in expected_timeaware_cols)

    # test that output df is identical to expected df
    result = result.reindex(
        sorted(result.columns), axis=1)
    expected_result = expected_result.reindex(
        sorted(expected_result.columns), axis=1)
    # ensure 'sequence' is int32 for test as other OS sometimes reads this as
    # int64 and will cause tests to fail when using equals()
    result['sequence'] = result['sequence'].astype('int32')
    assert result.equals(expected_result)


def test_format_transit_net_edge_empty_stop_times(stop_times_empty):
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_network._format_transit_net_edge(stop_times_empty)
    expected_error = (
        "Unable to continue processing transit network. stop_times table "
        "has 0 records.")
    assert expected_error in str(excinfo.value)


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
    file_list = glob.glob(os.path.join(dir_path, '*.h5'))
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


def test_simplify_transit_net(
        transit_edges_to_simplify, transit_nodes_to_simplify):
    remove_edge_ids = ['4_agency_a_city_a_1', '4_agency_a_city_a_2',
                       '4_agency_a_city_a_3']
    expected_edges = transit_edges_to_simplify.loc[
        ~transit_edges_to_simplify['id'].isin(remove_edge_ids)]
    expected_edges.reset_index(inplace=True, drop=True)
    result_edges, result_nodes = gtfs_network._simplify_transit_net(
        transit_edges_to_simplify, transit_nodes_to_simplify)

    # expect all nodes to persist
    assert len(result_nodes) == 11
    assert result_nodes.equals(transit_nodes_to_simplify)

    # expect 5_agency_a_city_a_1 to 3 and 6_agency_a_city_a_1 to 3 to
    # persist since they have different attributes in unique_route_id col
    # expect 1_agency_a_city_a_1 to 3 and 2_agency_a_city_a_1 to 3 to
    # persist since node_id_from and node_id_to are different
    # expect only one of 3_agency_a_city_a_1 to 3 or 4_agency_a_city_a_1 to 3
    # to persist but not both since they share the same attributes
    assert len(result_edges) == 15
    result_edges.reset_index(inplace=True, drop=True)
    assert result_edges.equals(expected_edges)


def test_simplify_transit_net_already_simplified(
        transit_edges_to_simplify, transit_nodes_to_simplify):
    remove_edge_ids = ['4_agency_a_city_a_1', '4_agency_a_city_a_2',
                       '4_agency_a_city_a_3']
    expected_edges = transit_edges_to_simplify.loc[
        ~transit_edges_to_simplify['id'].isin(remove_edge_ids)]
    expected_edges.reset_index(inplace=True, drop=True)

    # use the simplified edge table as input to test
    result_edges, result_nodes = gtfs_network._simplify_transit_net(
        expected_edges, transit_nodes_to_simplify)

    # expect all nodes to persist
    assert len(result_nodes) == 11
    assert result_nodes.equals(transit_nodes_to_simplify)

    # expect 5_agency_a_city_a_1 to 3 and 6_agency_a_city_a_1 to 3 to
    # persist since they have different attributes in unique_route_id col
    # expect 1_agency_a_city_a_1 to 3 and 2_agency_a_city_a_1 to 3 to
    # persist since node_id_from and node_id_to are different
    # expect only one of 3_agency_a_city_a_1 to 3 or 4_agency_a_city_a_1 to 3
    # to persist but not both since they share the same attributes
    assert len(result_edges) == 15
    result_edges.reset_index(inplace=True, drop=True)
    assert result_edges.equals(expected_edges)


def test_simplify_transit_net_remove_extra_nodes(
        transit_edges_to_simplify, transit_nodes_to_simplify_extra_nodes):
    remove_edge_ids = ['4_agency_a_city_a_1', '4_agency_a_city_a_2',
                       '4_agency_a_city_a_3']
    expected_edges = transit_edges_to_simplify.loc[
        ~transit_edges_to_simplify['id'].isin(remove_edge_ids)]
    expected_edges.reset_index(inplace=True, drop=True)

    remove_node_ids = ['12_agency_a_city_a']
    expected_nodes = transit_nodes_to_simplify_extra_nodes.loc[
        ~transit_nodes_to_simplify_extra_nodes.index.isin(remove_node_ids)]

    result_edges, result_nodes = gtfs_network._simplify_transit_net(
        transit_edges_to_simplify, transit_nodes_to_simplify_extra_nodes)

    # expect all nodes except for 12_agency_a_city_a to persist
    assert len(result_nodes) == 11
    assert result_nodes.equals(expected_nodes)

    # expect 5_agency_a_city_a_1 to 3 and 6_agency_a_city_a_1 to 3 to
    # persist since they have different attributes in unique_route_id col
    # expect 1_agency_a_city_a_1 to 3 and 2_agency_a_city_a_1 to 3 to
    # persist since node_id_from and node_id_to are different
    # expect only one of 3_agency_a_city_a_1 to 3 or 4_agency_a_city_a_1 to 3
    # to persist but not both since they share the same attributes
    assert len(result_edges) == 15
    result_edges.reset_index(inplace=True, drop=True)
    assert result_edges.equals(expected_edges)


def test_simplify_transit_net_empty_edges(
        edges_empty, transit_nodes_to_simplify):
    with pytest.raises(ValueError) as excinfo:
        result_edges, result_nodes = gtfs_network._simplify_transit_net(
            edges_empty, transit_nodes_to_simplify)
    expected_error = (
        'Unable to simplify transit network. Edges have 0 records to '
        'simplify.')
    assert expected_error in str(excinfo.value)


def test_remove_nodes_not_in_edges(
        transit_edges_to_simplify, transit_nodes_to_simplify_extra_nodes):
    remove_node_ids = ['12_agency_a_city_a']
    expected_nodes = transit_nodes_to_simplify_extra_nodes.loc[
        ~transit_nodes_to_simplify_extra_nodes.index.isin(remove_node_ids)]

    result_nodes = gtfs_network._remove_nodes_not_in_edges(
        transit_nodes_to_simplify_extra_nodes, transit_edges_to_simplify,
        from_id_col='node_id_from', to_id_col='node_id_to')

    # expect all nodes except for 12_agency_a_city_a to persist
    assert len(result_nodes) == 11
    assert result_nodes.equals(expected_nodes)


def test_remove_nodes_not_in_edges_none_to_remove(
        transit_edges_to_simplify, transit_nodes_to_simplify):
    result_nodes = gtfs_network._remove_nodes_not_in_edges(
        transit_nodes_to_simplify, transit_edges_to_simplify,
        from_id_col='node_id_from', to_id_col='node_id_to')

    # expect all nodes to persist
    assert len(result_nodes) == 11
    assert result_nodes.equals(transit_nodes_to_simplify)
