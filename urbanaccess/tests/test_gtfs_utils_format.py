import pytest
import pandas as pd
import os
from re import sub

from urbanaccess.gtfs import utils_format


@pytest.fixture
def folder_feed_1():
    return r'/data/gtfs_feeds/agency_a'


@pytest.fixture
def folder_feed_2():
    return r'/data/gtfs_feeds/agency_b'


@pytest.fixture
def folder_feed_4():
    return r'/data/gtfs_feeds/city'


@pytest.fixture()
def trips_feed_w_invalid_values(tmpdir):
    # create df with ints instead of str, col names with spaces, and
    # values with spaces before and after the value for relational columns
    data = {
        'route_id': ['10-101', '10-101', '10-101', '10-101',
                     111, '00111', '12-101', '12-101',
                     '13-101', '13-101'],
        'trip_id': ['a1   ', '   a2', '   a3   ', 'a   4',
                    'b1', 'b2', 'c1', 'c2', 'd1', 'd2'],
        'service_id  ': ['weekday   -1', 'weekday-1   ', 'weekday-1',
                         'weekday-1', 'weekday-2', 'weekday-2',
                         'weekday-3', 'weekday-3', 'weekend-1', 'weekend-1'],
        '    direction_id    ': [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
        'wheelchair_    accessible': [1, 1, 1, 1, 0, 0, 0, 0, 0, 0],
        'bikes_allowed': [1, 1, 1, 1, 0, 0, 0, 0, 0, 0]
    }
    index = range(10)
    raw_df = pd.DataFrame(data, index)

    feed_path = os.path.join(tmpdir.strpath, 'test_trips_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('trips')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    data = {
        'route_id': ['10-101', '10-101', '10-101', '10-101',
                     '111', '00111', '12-101', '12-101', '13-101', '13-101'],
        'trip_id': ['a1', 'a2', 'a3', 'a   4',
                    'b1', 'b2', 'c1', 'c2', 'd1', 'd2'],
        'service_id': ['weekday   -1', 'weekday-1', 'weekday-1',
                       'weekday-1', 'weekday-2', 'weekday-2',
                       'weekday-3', 'weekday-3', 'weekend-1', 'weekend-1'],
        'direction_id': [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
        'wheelchair_    accessible': [1, 1, 1, 1, 0, 0, 0, 0, 0, 0],
        'bikes_allowed': [1, 1, 1, 1, 0, 0, 0, 0, 0, 0]
    }
    index = range(10)
    expected_df = pd.DataFrame(data, index)

    return raw_df, expected_df, feed_path


def test_calendar_dates_agencyid_feed_1(calendar_dates_feed_1,
                                        routes_feed_1,
                                        trips_feed_1,
                                        agency_feed_1,
                                        folder_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 4}
    index = range(4)
    expected_result = pd.concat([calendar_dates_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._calendar_dates_agencyid(
        calendar_dates_df=calendar_dates_feed_1,
        routes_df=routes_feed_1,
        trips_df=trips_feed_1,
        agency_df=agency_feed_1,
        feed_folder=folder_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_dates_feed_1.columns
    assert calendar_dates_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_calendar_dates_agencyid_feed_2(calendar_dates_feed_2,
                                        routes_feed_2,
                                        trips_feed_2,
                                        agency_feed_2,
                                        folder_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1',
                                 'agency_b_district_1',
                                 'agency_b_district_2',
                                 'agency_b_district_2']}
    index = range(4)
    expected_result = pd.concat([calendar_dates_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._calendar_dates_agencyid(
        calendar_dates_df=calendar_dates_feed_2,
        routes_df=routes_feed_2,
        trips_df=trips_feed_2,
        agency_df=agency_feed_2,
        feed_folder=folder_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_dates_feed_2.columns
    assert calendar_dates_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_calendar_dates_agencyid_feed_4(calendar_dates_feed_4,
                                        routes_feed_4,
                                        trips_feed_4,
                                        agency_feed_4,
                                        folder_feed_4):
    data = {'service_id': ['wk-1'] * 3,
            'date': [20161224] * 3,
            'exception_type': [1] * 3,
            'unique_agency_id': ['agency_1_bus', 'agency_2_rail',
                                 'agency_3_metro']}
    index = range(3)
    expected_result = pd.DataFrame(data, index)

    result_df = utils_format._calendar_dates_agencyid(
        calendar_dates_df=calendar_dates_feed_4,
        routes_df=routes_feed_4,
        trips_df=trips_feed_4,
        agency_df=agency_feed_4,
        feed_folder=folder_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_dates_feed_4.columns
    dedup_df = result_df.drop_duplicates(subset='service_id',
                                         keep='first',
                                         inplace=False)
    dedup_df.reset_index(inplace=True)
    assert calendar_dates_feed_4.equals(dedup_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_calendar_agencyid_feed_1(calendar_feed_1,
                                  routes_feed_1,
                                  trips_feed_1,
                                  agency_feed_1,
                                  folder_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 4}
    index = range(4)
    expected_result = pd.concat([calendar_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._calendar_agencyid(calendar_df=calendar_feed_1,
                                                routes_df=routes_feed_1,
                                                trips_df=trips_feed_1,
                                                agency_df=agency_feed_1,
                                                feed_folder=folder_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_feed_1.columns
    assert calendar_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_calendar_agencyid_feed_2(calendar_feed_2,
                                  routes_feed_2,
                                  trips_feed_2,
                                  agency_feed_2,
                                  folder_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1',
                                 'agency_b_district_1',
                                 'agency_b_district_2',
                                 'agency_b_district_2']}
    index = range(4)
    expected_result = pd.concat([calendar_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._calendar_agencyid(calendar_df=calendar_feed_2,
                                                routes_df=routes_feed_2,
                                                trips_df=trips_feed_2,
                                                agency_df=agency_feed_2,
                                                feed_folder=folder_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_feed_2.columns
    assert calendar_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_calendar_agencyid_feed_4(calendar_feed_4,
                                  routes_feed_4,
                                  trips_feed_4,
                                  agency_feed_4,
                                  folder_feed_4):
    data = {'service_id': ['wk-1'] * 3,
            'monday': [1] * 3,
            'tuesday': [1] * 3,
            'wednesday': [1] * 3,
            'thursday': [1] * 3,
            'friday': [1] * 3,
            'saturday': [0] * 3,
            'sunday': [0] * 3,
            'start_date': [20161224] * 3,
            'end_date': [20170318] * 3,
            'unique_agency_id': ['agency_1_bus', 'agency_2_rail',
                                 'agency_3_metro']}
    index = range(3)
    expected_result = pd.DataFrame(data, index)

    result_df = utils_format._calendar_agencyid(calendar_df=calendar_feed_4,
                                                routes_df=routes_feed_4,
                                                trips_df=trips_feed_4,
                                                agency_df=agency_feed_4,
                                                feed_folder=folder_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = calendar_feed_4.columns
    dedup_df = result_df.drop_duplicates(subset='service_id',
                                         keep='first',
                                         inplace=False)
    dedup_df.reset_index(inplace=True)
    assert calendar_feed_4.equals(dedup_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_trips_agencyid_feed_1(trips_feed_1,
                               routes_feed_1,
                               agency_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 10}
    index = range(10)
    expected_result = pd.concat([trips_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._trips_agencyid(trips_df=trips_feed_1,
                                             routes_df=routes_feed_1,
                                             agency_df=agency_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = trips_feed_1.columns
    assert trips_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_trips_agencyid_feed_2(trips_feed_2,
                               routes_feed_2,
                               agency_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2']}
    index = range(10)
    expected_result = pd.concat([trips_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._trips_agencyid(trips_df=trips_feed_2,
                                             routes_df=routes_feed_2,
                                             agency_df=agency_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = trips_feed_2.columns
    assert trips_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_trips_agencyid_feed_4(trips_feed_4,
                               routes_feed_4,
                               agency_feed_4):
    data = {
        'unique_agency_id': ['agency_1_bus', 'agency_1_bus', 'agency_1_bus',
                             'agency_1_bus', 'agency_1_bus', 'agency_1_bus',
                             'agency_2_rail', 'agency_2_rail',
                             'agency_2_rail', 'agency_2_rail',
                             'agency_3_metro', 'agency_3_metro',
                             'agency_3_metro', 'agency_3_metro']}
    index = range(14)
    expected_result = pd.concat([trips_feed_4,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._trips_agencyid(trips_df=trips_feed_4,
                                             routes_df=routes_feed_4,
                                             agency_df=agency_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = trips_feed_4.columns
    assert trips_feed_4.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stops_agencyid_feed_1(stops_feed_1,
                               trips_feed_1,
                               routes_feed_1,
                               stop_times_feed_1,
                               agency_feed_1,
                               folder_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 9}
    index = range(9)
    expected_result = pd.concat([stops_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._stops_agencyid(stops_df=stops_feed_1,
                                             trips_df=trips_feed_1,
                                             routes_df=routes_feed_1,
                                             stop_times_df=stop_times_feed_1,
                                             agency_df=agency_feed_1,
                                             feed_folder=folder_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stops_feed_1.columns
    assert stops_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stops_agencyid_feed_2(stops_feed_2,
                               trips_feed_2,
                               routes_feed_2,
                               stop_times_feed_2,
                               agency_feed_2,
                               folder_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2']}
    index = range(16)
    expected_result = pd.concat([stops_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._stops_agencyid(stops_df=stops_feed_2,
                                             trips_df=trips_feed_2,
                                             routes_df=routes_feed_2,
                                             stop_times_df=stop_times_feed_2,
                                             agency_df=agency_feed_2,
                                             feed_folder=folder_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stops_feed_2.columns
    assert stops_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stops_agencyid_feed_4(stops_feed_4,
                               trips_feed_4,
                               routes_feed_4,
                               stop_times_feed_4,
                               agency_feed_4,
                               folder_feed_4):
    data = {
        'stop_id': ['70', '70', '71', '71', '72', '72', '73', '73', '74',
                    '74', '75', '75', '76', '76', '77', '77', '78', '78'],
        'stop_name': ['station 1', 'station 1', 'station 2', 'station 2',
                      'station 3', 'station 3', 'station 4', 'station 4',
                      'station 5', 'station 5', 'station 6', 'station 6',
                      'station 7', 'station 7', 'station 8', 'station 8',
                      'station 9', 'station 9'],
        'stop_lat': [20.797484, 20.797484, 20.774963, 20.774963, 20.803664,
                     20.803664, 20.80787, 20.80787, 20.828415, 20.828415,
                     20.844601, 20.844601, 20.664174, 20.664174, 20.591208,
                     20.591208, 20.905628, 20.905628],
        'stop_lon': [-100.265609, -100.265609, -100.224274, -100.224274,
                     -100.271604, -100.271604, -100.269029, -100.269029,
                     -100.267227, -100.267227, -100.251793, -100.251793,
                     -100.444116, -100.444116, -100.017867, -100.017867,
                     -100.067423, -100.067423],
        'unique_agency_id': ['agency_1_bus', 'agency_2_rail', 'agency_1_bus',
                             'agency_2_rail', 'agency_1_bus', 'agency_2_rail',
                             'agency_1_bus', 'agency_2_rail', 'agency_1_bus',
                             'agency_2_rail', 'agency_1_bus', 'agency_2_rail',
                             'agency_2_rail', 'agency_3_metro',
                             'agency_2_rail', 'agency_3_metro',
                             'agency_2_rail', 'agency_3_metro']
    }

    index = range(18)

    expected_result = pd.DataFrame(data, index)

    result_df = utils_format._stops_agencyid(stops_df=stops_feed_4,
                                             trips_df=trips_feed_4,
                                             routes_df=routes_feed_4,
                                             stop_times_df=stop_times_feed_4,
                                             agency_df=agency_feed_4,
                                             feed_folder=folder_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stops_feed_4.columns
    dedup_df = result_df.drop_duplicates(subset='stop_id',
                                         keep='first',
                                         inplace=False)
    dedup_df.reset_index(inplace=True)
    assert stops_feed_4.equals(dedup_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_routes_agencyid_feed_1(routes_feed_1,
                                agency_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 4}
    index = range(4)
    expected_result = pd.concat([routes_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._routes_agencyid(routes_df=routes_feed_1,
                                              agency_df=agency_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = routes_feed_1.columns
    assert routes_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_routes_agencyid_feed_2(routes_feed_2,
                                agency_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_2', 'agency_b_district_2']}
    index = range(4)
    expected_result = pd.concat([routes_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._routes_agencyid(routes_df=routes_feed_2,
                                              agency_df=agency_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = routes_feed_2.columns
    assert routes_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_routes_agencyid_feed_4(routes_feed_4,
                                agency_feed_4):
    data = {'unique_agency_id': ['agency_1_bus', 'agency_1_bus',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_3_metro', 'agency_3_metro']}
    index = range(6)
    expected_result = pd.concat([routes_feed_4,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._routes_agencyid(routes_df=routes_feed_4,
                                              agency_df=agency_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = routes_feed_4.columns
    assert routes_feed_4.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stop_times_agencyid_feed_1(stop_times_feed_1,
                                    routes_feed_1,
                                    trips_feed_1,
                                    agency_feed_1):
    data = {'unique_agency_id': ['agency_a_city_a'] * 54}
    index = range(54)
    expected_result = pd.concat([stop_times_feed_1,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._stop_times_agencyid(
        stop_times_df=stop_times_feed_1,
        routes_df=routes_feed_1,
        trips_df=trips_feed_1,
        agency_df=agency_feed_1)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stop_times_feed_1.columns
    assert stop_times_feed_1.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_1['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_1['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_1['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stop_times_agencyid_feed_2(stop_times_feed_2,
                                    routes_feed_2,
                                    trips_feed_2,
                                    agency_feed_2):
    data = {'unique_agency_id': ['agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_1', 'agency_b_district_1',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2',
                                 'agency_b_district_2', 'agency_b_district_2']}
    index = range(54)
    expected_result = pd.concat([stop_times_feed_2,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._stop_times_agencyid(
        stop_times_df=stop_times_feed_2,
        routes_df=routes_feed_2,
        trips_df=trips_feed_2,
        agency_df=agency_feed_2)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stop_times_feed_2.columns
    assert stop_times_feed_2.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_2['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_2['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_2['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_stop_times_agencyid_feed_4(stop_times_feed_4,
                                    routes_feed_4,
                                    trips_feed_4,
                                    agency_feed_4):
    data = {'unique_agency_id': ['agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_1_bus', 'agency_1_bus',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_2_rail', 'agency_2_rail',
                                 'agency_3_metro', 'agency_3_metro',
                                 'agency_3_metro', 'agency_3_metro',
                                 'agency_3_metro', 'agency_3_metro',
                                 'agency_3_metro', 'agency_3_metro',
                                 'agency_3_metro', 'agency_3_metro',
                                 'agency_3_metro', 'agency_3_metro']}
    index = range(66)
    expected_result = pd.concat([stop_times_feed_4,
                                 pd.DataFrame(data, index)],
                                axis=1)

    result_df = utils_format._stop_times_agencyid(
        stop_times_df=stop_times_feed_4,
        routes_df=routes_feed_4,
        trips_df=trips_feed_4,
        agency_df=agency_feed_4)

    # test that cols not touched by function in output df are
    # identical to the cols in input df
    original_cols = stop_times_feed_4.columns
    assert stop_times_feed_4.equals(result_df[original_cols])

    # test that output df is identical to expected df
    # re-sort cols so they are in same order for test
    expected_result.sort_index(axis=1, inplace=True)
    result_df.sort_index(axis=1, inplace=True)
    assert expected_result.equals(result_df)

    # test that all output unique_agency_id values match all agency_name
    # values in agency file
    col = agency_feed_4['agency_name'].astype(str)
    col_snake_case = col.str.replace(r'\s+', '_')
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    agency_feed_4['unique_agency_id'] = col_snake_no_amps.str.lower()

    assert all(agency_feed_4['unique_agency_id'].unique() == result_df[
        'unique_agency_id'].unique())


def test_add_unique_gtfsfeed_id(stops_feed_1, routes_feed_1, trips_feed_1,
                                stop_times_feed_1, calendar_feed_1,
                                calendar_dates_feed_1, folder_feed_1):

    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = (utils_format
                             ._add_unique_gtfsfeed_id(
                                stops_df=stops_feed_1,
                                routes_df=routes_feed_1,
                                trips_df=trips_feed_1,
                                stop_times_df=stop_times_feed_1,
                                calendar_df=calendar_feed_1,
                                calendar_dates_df=calendar_dates_feed_1,
                                feed_folder=folder_feed_1,
                                feed_number=1))

    df_dict = {'stops': [stops_df, stops_feed_1],
               'routes': [routes_df, routes_feed_1],
               'trips': [trips_df, trips_feed_1],
               'stop_times': [stop_times_df, stop_times_feed_1],
               'calendar': [calendar_df, calendar_feed_1],
               'calendar_dates': [calendar_dates_df, calendar_dates_feed_1]}

    feed_folder = sub(r'\s+', '_', os.path.split(folder_feed_1)[1]).replace(
        '&', 'and').lower()
    unique_feed_id = '_'.join([feed_folder, str(1)])

    for df in df_dict.keys():

        # create new unique_feed_id column based on the name of the feed folder
        assert df_dict[df][0]['unique_feed_id'].unique() == unique_feed_id

        # test that cols not touched by function in output df are
        # identical to the cols in input df
        original_cols = df_dict[df][1].columns
        assert df_dict[df][1].equals(df_dict[df][0][original_cols])


def test_remove_whitespace_from_values(trips_feed_w_invalid_values):
    raw_df, expected_df, feed_path = trips_feed_w_invalid_values

    # convert the one int record to str to match dtype of what would be read by
    # read_gtfs function
    raw_df['route_id'] = raw_df['route_id'].astype('str')

    # test when col_list is used
    result = utils_format._remove_whitespace(
        df=raw_df,
        textfile='trips.txt',
        col_list=['trip_id', 'service_id', 'route_id'])
    # re-sort cols so they are in same order for test
    expected_df.sort_index(axis=1, inplace=True)
    result.sort_index(axis=1, inplace=True)
    assert result.equals(expected_df)

    # test when no col_list is used
    result_no_col_list = utils_format._remove_whitespace(
        df=raw_df,
        textfile='trips.txt',
        col_list=None)
    # spaces in cols should be removed
    assert list(result_no_col_list.columns) == list(expected_df.columns)
    # spaces in values should remain
    assert result_no_col_list['trip_id'].str.len().sum() == raw_df[
        'trip_id'].str.len().sum()


def test_read_gtfs_trips_w_invalid_values(trips_feed_w_invalid_values):
    raw_df, expected_df, feed_path = trips_feed_w_invalid_values
    result = utils_format._read_gtfs_trips(
        textfile_path=feed_path, textfile='trips.txt')
    # re-sort cols so they are in same order for test
    expected_df.sort_index(axis=1, inplace=True)
    result.sort_index(axis=1, inplace=True)
    assert result.equals(expected_df)
