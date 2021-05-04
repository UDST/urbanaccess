import pytest
import pandas as pd
import numpy as np
import os
from re import sub

from urbanaccess.gtfs import utils_format
from urbanaccess import config


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
def agency_txt_w_invalid_values(tmpdir, agency_feed_1):
    # create df with col names with spaces
    raw_df = agency_feed_1.rename(
        columns={'agency_phone': '    agency_phone',
                 'agency_timezone': 'agency_timezone   '})

    feed_path = os.path.join(tmpdir.strpath, 'test_agency_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('agency')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, agency_feed_1, feed_path


@pytest.fixture()
def stops_txt_w_invalid_values(tmpdir, stops_feed_1):
    # create df with col names with spaces
    raw_df = stops_feed_1.rename(
        columns={'stop_name': '    stop_name',
                 'location_type': 'location_type   '})

    feed_path = os.path.join(tmpdir.strpath, 'test_stops_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('stops')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, stops_feed_1, feed_path


@pytest.fixture()
def routes_txt_w_invalid_values(tmpdir, routes_feed_1):
    # create df with col names with spaces
    raw_df = routes_feed_1.rename(
        columns={'route_short_name': '    route_short_name',
                 'route_long_name': 'route_long_name   '})

    feed_path = os.path.join(tmpdir.strpath, 'test_routes_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('routes')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, routes_feed_1, feed_path


@pytest.fixture()
def stop_times_txt_w_invalid_values(tmpdir, stop_times_feed_1):
    # create df with col names with spaces
    raw_df = stop_times_feed_1.rename(
        columns={'pickup_type': '    pickup_type',
                 'drop_off_type': 'drop_off_type   '})

    feed_path = os.path.join(tmpdir.strpath, 'test_stop_times_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('stop_times')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, stop_times_feed_1, feed_path


@pytest.fixture()
def calendar_txt_w_invalid_values(tmpdir, calendar_feed_1):
    # create df with col names with spaces
    raw_df = calendar_feed_1.rename(
        columns={'monday': '    monday',
                 'tuesday': 'tuesday   '})

    feed_path = os.path.join(tmpdir.strpath, 'test_calendar_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('calendar')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, calendar_feed_1, feed_path


@pytest.fixture()
def calendar_dates_txt_w_invalid_values(tmpdir, calendar_dates_feed_1):
    # create df with col names with spaces
    raw_df = calendar_dates_feed_1.rename(
        columns={'exception_type': '    exception_type',
                 'schedule_type': 'schedule_type   '})

    feed_path = os.path.join(tmpdir.strpath,
                             'test_calendar_dates_invalid_values')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    feed_file_name = '{}.txt'.format('calendar_dates')
    raw_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    return raw_df, calendar_dates_feed_1, feed_path


@pytest.fixture()
def trips_txt_w_invalid_values(tmpdir):
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
    expected_result = pd.concat(
        [trips_feed_4, pd.DataFrame(data, index)], axis=1)

    result_df = utils_format._trips_agencyid(
        trips_df=trips_feed_4, routes_df=routes_feed_4,
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
    expected_result = pd.concat(
        [stops_feed_1, pd.DataFrame(data, index)], axis=1)

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
    expected_result = pd.concat(
        [stops_feed_2, pd.DataFrame(data, index)], axis=1)

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
    expected_result = pd.concat(
        [routes_feed_1, pd.DataFrame(data, index)], axis=1)

    result_df = utils_format._routes_agencyid(
        routes_df=routes_feed_1, agency_df=agency_feed_1)

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
    expected_result = pd.concat(
        [routes_feed_2, pd.DataFrame(data, index)], axis=1)

    result_df = utils_format._routes_agencyid(
        routes_df=routes_feed_2, agency_df=agency_feed_2)

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
    expected_result = pd.concat(
        [routes_feed_4, pd.DataFrame(data, index)], axis=1)

    result_df = utils_format._routes_agencyid(
        routes_df=routes_feed_4, agency_df=agency_feed_4)

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
    expected_result = pd.concat(
        [stop_times_feed_1, pd.DataFrame(data, index)], axis=1)

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
    expected_result = pd.concat(
        [stop_times_feed_2, pd.DataFrame(data, index)], axis=1)

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
    expected_result = pd.concat(
        [stop_times_feed_4, pd.DataFrame(data, index)], axis=1)

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
        calendar_dates_df = utils_format._add_unique_gtfsfeed_id(
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=calendar_feed_1,
            calendar_dates_df=calendar_dates_feed_1,
            feed_folder=folder_feed_1,
            feed_number=1)

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


def test_remove_whitespace_from_values(trips_txt_w_invalid_values):
    raw_df, expected_df, feed_path = trips_txt_w_invalid_values

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


def test_read_gtfs_trips_w_invalid_values(trips_txt_w_invalid_values):
    raw_df, expected_df, feed_path = trips_txt_w_invalid_values
    result = utils_format._read_gtfs_trips(
        textfile_path=feed_path, textfile='trips.txt')
    # re-sort cols so they are in same order for test
    expected_df.sort_index(axis=1, inplace=True)
    result.sort_index(axis=1, inplace=True)
    assert result.equals(expected_df)


def test_read_gtfs_agency(agency_txt_w_invalid_values):
    raw_df, expected_df, feed_path = agency_txt_w_invalid_values
    result = utils_format._read_gtfs_agency(
        textfile_path=feed_path, textfile='agency.txt')
    assert result.equals(expected_df)


def test_read_gtfs_agency_invalid_params(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_agency(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: agency.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_agency(
            textfile_path=feed_path, textfile='agency.txt')
    expected_error = ('{} has no records.'.format(
        os.path.join(feed_path, 'agency.txt')))
    assert expected_error in str(excinfo.value)


def test_read_gtfs_stops(stops_txt_w_invalid_values):
    raw_df, expected_df, feed_path = stops_txt_w_invalid_values
    result = utils_format._read_gtfs_stops(
        textfile_path=feed_path, textfile='stops.txt')
    # ensure lat long precision is the same for equals()
    result['stop_lat'] = result['stop_lat'].round(6)
    result['stop_lon'] = result['stop_lon'].round(6)
    expected_df['stop_lat'] = expected_df['stop_lat'].round(6)
    expected_df['stop_lon'] = expected_df['stop_lon'].round(6)
    assert result.equals(expected_df)


def test_read_gtfs_stops_invalid_params(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_stops(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: stops.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_stops(
            textfile_path=feed_path, textfile='stops.txt')
    expected_error = ('{} has no records.'.format(
        os.path.join(feed_path, 'stops.txt')))
    assert expected_error in str(excinfo.value)


def test_read_gtfs_routes(routes_txt_w_invalid_values):
    raw_df, expected_df, feed_path = routes_txt_w_invalid_values
    result = utils_format._read_gtfs_routes(
        textfile_path=feed_path, textfile='routes.txt')
    assert result.equals(expected_df)


def test_read_gtfs_routes_invalid_params(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_routes(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: routes.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_routes(
            textfile_path=feed_path, textfile='routes.txt')
    expected_error = ('{} has no records.'.format(
        os.path.join(feed_path, 'routes.txt')))
    assert expected_error in str(excinfo.value)


def test_read_gtfs_trips(trips_txt_w_invalid_values):
    raw_df, expected_df, feed_path = trips_txt_w_invalid_values
    result = utils_format._read_gtfs_trips(
        textfile_path=feed_path, textfile='trips.txt')
    assert result.equals(expected_df)


def test_read_gtfs_trips_invalid_params(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_trips(
            textfile_path=feed_path, textfile='stops.txt')
    expected_error = ('stops.txt is not an expected GTFS file name. '
                      'Expected: trips.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_trips(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('{} has no records.'.format(
        os.path.join(feed_path, 'trips.txt')))
    assert expected_error in str(excinfo.value)


def test_read_gtfs_stop_times(stop_times_txt_w_invalid_values):
    raw_df, expected_df, feed_path = stop_times_txt_w_invalid_values
    result = utils_format._read_gtfs_stop_times(
        textfile_path=feed_path, textfile='stop_times.txt')
    assert result.equals(expected_df)


def test_read_gtfs_stop_times_invalid_params(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_stop_times(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: stop_times.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_stop_times(
            textfile_path=feed_path, textfile='stop_times.txt')
    expected_error = ('{} has no records.'.format(
        os.path.join(feed_path, 'stop_times.txt')))
    assert expected_error in str(excinfo.value)


def test_read_gtfs_calendar(calendar_txt_w_invalid_values):
    raw_df, expected_df, feed_path = calendar_txt_w_invalid_values
    result = utils_format._read_gtfs_calendar(
        textfile_path=feed_path, textfile='calendar.txt')
    assert result.equals(expected_df)


def test_read_gtfs_calendar_invalid_params(
        capsys,
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_calendar(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: calendar.txt.')
    assert expected_error in str(excinfo.value)
    result = utils_format._read_gtfs_calendar(
        textfile_path=feed_path, textfile='calendar.txt')
    # check that expected print prints
    captured = capsys.readouterr()
    assert 'service_ids' in captured.out


def test_read_gtfs_calendar_dates(calendar_dates_txt_w_invalid_values):
    raw_df, expected_df, feed_path = calendar_dates_txt_w_invalid_values
    result = utils_format._read_gtfs_calendar_dates(
        textfile_path=feed_path, textfile='calendar_dates.txt')
    assert result.equals(expected_df)


def test_read_gtfs_calendar_dates_invalid_params(
        capsys,
        agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt):
    feed_path = agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._read_gtfs_calendar_dates(
            textfile_path=feed_path, textfile='trips.txt')
    expected_error = ('trips.txt is not an expected GTFS file name. '
                      'Expected: calendar_dates.txt.')
    assert expected_error in str(excinfo.value)
    result = utils_format._read_gtfs_calendar_dates(
        textfile_path=feed_path, textfile='calendar_dates.txt')
    # check that expected print prints
    captured = capsys.readouterr()
    assert 'service_ids' in captured.out


def test_list_raw_txt_columns(calendar_dates_txt_w_invalid_values):
    raw_df, expected_df, feed_path = calendar_dates_txt_w_invalid_values
    file = os.path.join(feed_path, 'calendar_dates.txt')
    result_cols = utils_format._list_raw_txt_columns(file)
    assert isinstance(result_cols, list)
    expected_cols = ['service_id', 'date',
                     '    exception_type', 'schedule_type   ']
    assert sorted(result_cols) == sorted(expected_cols)


def test_timetoseconds(stop_times_feed_1):
    # create 1 record that is missing a 0 in the hr position
    stop_times_feed_1['departure_time'].iloc[8] = '1:20:00'
    result = utils_format._timetoseconds(
        stop_times_feed_1, time_cols=['departure_time'])
    # check that 'departure_time_sec' was created and is not empty
    assert 'departure_time_sec' in result.columns
    assert result['departure_time_sec'].empty is False
    # remainder of df should not have changed
    assert result[stop_times_feed_1.columns].equals(stop_times_feed_1)
    # ensure subset of values are correct
    # check conversion of 06:15:00 is 22500.0 sec past midnight
    assert result.iloc[0]['departure_time_sec'] == 22500.0
    # check nans stay as nans
    assert pd.isna(result.iloc[2]['departure_time_sec'])
    # check conversion of a time past midnight 26:20:00 is 22500.0 sec past
    # midnight of the prior day
    assert result.iloc[37]['departure_time_sec'] == 94800.0
    # check that value that was missing a 0 was fixed and it was converted
    # to seconds
    assert result.iloc[8]['departure_time'] == '01:20:00'
    assert result.iloc[8]['departure_time_sec'] == 4800.0


def test_timetoseconds_invalid_params(stop_times_feed_1):
    with pytest.raises(ValueError) as excinfo:
        result = utils_format._timetoseconds(
            stop_times_feed_1, time_cols='departure_time')
    expected_error = 'departure_time is not a list.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # create 1 record with invalid formatting
        stop_times_feed_1['departure_time'].iloc[0] = '100:90:80'
        result = utils_format._timetoseconds(
            stop_times_feed_1, time_cols=['departure_time'])
    expected_error = ('Check formatting of value: 100:90:80 as it is in '
                      'the incorrect format and should be 8 character '
                      'string 00:00:00.')
    assert expected_error in str(excinfo.value)


def test_timetoseconds_invalid_data(capsys, stop_times_feed_1):
    # add 2 records with invalid and large hr, min, sec values
    stop_times_feed_1['departure_time'].iloc[0] = '60:90:80'
    stop_times_feed_1['departure_time'].iloc[2] = '70:80:70'
    result = utils_format._timetoseconds(
        stop_times_feed_1, time_cols=['departure_time'])
    # test that warning prints were printed
    captured = capsys.readouterr()
    assert 'hour value(s) are greater' in captured.out
    assert 'minute value(s) are greater' in captured.out
    assert 'second value(s) are greater' in captured.out


def test_add_txt_definitions_no_cols(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    # drop cols that would be used to map values in function for test
    trips_feed_1.drop(columns=['bikes_allowed', 'wheelchair_accessible'],
                      inplace=True)
    stop_times_feed_1.drop(columns=['pickup_type', 'drop_off_type'],
                           inplace=True)
    routes_feed_1.drop(columns=['route_type'],
                       inplace=True)
    stops_feed_1.drop(columns=['location_type', 'wheelchair_boarding'],
                      inplace=True)

    stops_df, routes_df, stop_times_df, trips_df = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)

    # df should be identical to input
    assert stops_df.equals(stops_feed_1)
    assert routes_df.equals(routes_feed_1)
    assert stop_times_df.equals(stop_times_feed_1)
    assert trips_df.equals(trips_feed_1)


def test_add_txt_definitions_general(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    stops_df, routes_df, stop_times_df, trips_df = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)
    df_list = [stops_df, routes_df, stop_times_df, trips_df]
    # check that there are generated '_desc' cols in result df and they are not
    # empty or nulls
    for df in df_list:
        desc_cols = [col for col in df.columns if '_desc' in col]
        assert len(desc_cols) > 0
        for col in desc_cols:
            assert df[col].empty is False
            df[col].isnull().values.any() == False  # noqa


def test_add_txt_definitions_stops(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    result, routes_df, stop_times_df, trips_df = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)
    # check that expected columns were created and they are not empty
    expected_cols = ['location_type_desc', 'wheelchair_boarding_desc']
    for col in expected_cols:
        assert col in result.columns
        assert result[col].empty is False
    # check that mapped values are what is expected
    assert all(result['location_type_desc'].isin(
        config._STOPS_LOCATION_TYPE_LOOKUP.values()))
    assert all(result['wheelchair_boarding_desc'].isin(
        config._STOPS_WHEELCHAIR_BOARDINGS.values()))
    # check subset of values
    assert result.iloc[0]['location_type'] == 1 and result.iloc[0][
        'location_type_desc'] == 'station'
    assert result.iloc[0]['wheelchair_boarding'] == 1 and result.iloc[0][
        'wheelchair_boarding_desc'] == (
               'At least some vehicles at this stop can be boarded by a '
               'rider in a wheelchair')
    # remainder of df should not have changed
    assert result[stops_feed_1.columns].equals(stops_feed_1)


def test_add_txt_definitions_routes(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    stops_df, result, stop_times_df, trips_df = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)
    # check that expected columns were created and they are not empty
    assert 'route_type_desc' in result.columns
    assert result['route_type_desc'].empty is False
    # check that mapped values are what is expected
    assert all(result['route_type_desc'].isin(
        config._ROUTES_MODE_TYPE_LOOKUP.values()))
    assert result.iloc[0]['route_type'] == 3 and result.iloc[0][
        'route_type_desc'] == 'Bus'
    # remainder of df should not have changed
    assert result[routes_feed_1.columns].equals(routes_feed_1)


def test_add_txt_definitions_stop_times(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    # add optional timepoint col that is missing with nans
    stop_times_feed_1['timepoint'] = np.nan
    stops_df, routes_df, result, trips_df = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)
    # check that expected columns were created and they are not empty
    expected_cols = ['pickup_type_desc', 'drop_off_type_desc',
                     'timepoint_desc']
    for col in expected_cols:
        assert col in result.columns
        assert result[col].empty is False
    # check that mapped values are what is expected
    assert all(result['pickup_type_desc'].isin(
        config._STOP_TIMES_PICKUP_TYPE.values()))
    assert all(result['drop_off_type_desc'].isin(
        config._STOP_TIMES_DROP_OFF_TYPE.values()))
    assert all(result['timepoint_desc'].isin(
        config._STOP_TIMES_TIMEPOINT.values()))
    # check subset of values
    assert result.iloc[0]['pickup_type'] == 0 and result.iloc[0][
        'pickup_type_desc'] == 'Regularly scheduled pickup'
    assert result.iloc[0]['drop_off_type'] == 0 and result.iloc[0][
        'drop_off_type_desc'] == 'Regularly Scheduled'
    assert pd.isna(result.iloc[0]['timepoint']) and result.iloc[0][
        'timepoint_desc'] == 'Times are considered exact'
    # remainder of df should not have changed
    assert result[stop_times_feed_1.columns].equals(stop_times_feed_1)


def test_add_txt_definitions_trips(
        stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1):
    stops_df, routes_df, stop_times_df, result = \
        utils_format._add_txt_definitions(
            stops_feed_1, routes_feed_1, stop_times_feed_1, trips_feed_1)
    # check that expected columns were created and they are not empty
    expected_cols = ['bikes_allowed_desc', 'wheelchair_accessible_desc']
    for col in expected_cols:
        assert col in result.columns
        assert result[col].empty is False
    # check that mapped values are what is expected
    assert all(result['bikes_allowed_desc'].isin(
        config._TRIPS_BIKES_ALLOWED.values()))
    assert all(result['wheelchair_accessible_desc'].isin(
        config._TRIPS_WHEELCHAIR_ACCESSIBLE.values()))
    # check subset of values
    assert result.iloc[0]['bikes_allowed'] == 1 and result.iloc[0][
        'bikes_allowed_desc'] == (
               'Vehicle being used on this particular trip can accommodate '
               'at least one bicycle.')
    assert result.iloc[0]['wheelchair_accessible'] == 1 and result.iloc[0][
        'wheelchair_accessible_desc'] == (
        'Vehicle being used on this particular trip can accommodate at least '
        'one rider in a wheelchair')
    # remainder of df should not have changed
    assert result[trips_feed_1.columns].equals(trips_feed_1)


def test_apply_gtfs_definition(stops_feed_1):
    desc_dict = {'location_type': config._STOPS_LOCATION_TYPE_LOOKUP,
                 'wheelchair_boarding': config._STOPS_WHEELCHAIR_BOARDINGS}
    test_1 = stops_feed_1.copy()
    result = utils_format._apply_gtfs_definition(test_1, desc_dict)
    desc_cols = [col for col in result.columns if '_desc' in col]
    assert len(desc_cols) > 0
    for col in desc_cols:
        assert result[col].empty is False
        result[col].isnull().values.any() == False  # noqa
    # test when there is mismatch between value in table and value mapping
    # config

    # add unrecognized value to df
    test_2 = stops_feed_1.copy()
    test_2['location_type'].loc[0:1] = 40
    # test with nans
    test_2['location_type'].loc[2:3] = np.nan
    result = utils_format._apply_gtfs_definition(test_2, desc_dict)
    desc_cols = [col for col in result.columns if '_desc' in col]
    assert len(desc_cols) > 0
    for col in desc_cols:
        assert result[col].empty is False
    # location_type that was 40 we expect to be None given there is no
    # lookup value for it
    assert result.iloc[0]['location_type_desc'] is None
    # location_type that was nan we expect to be 'stop' given config maps
    # nans to 'stop'
    assert result.iloc[2]['location_type_desc'] == 'stop'


def test_append_route_type(stops_feed_1, stop_times_feed_1, routes_feed_1,
                           trips_feed_1):
    result_stops_df = utils_format._append_route_type(
        stops_df=stops_feed_1,
        stop_times_df=stop_times_feed_1,
        routes_df=routes_feed_1[['route_id', 'route_type']],
        trips_df=trips_feed_1[['trip_id', 'route_id']],
        info_to_append='route_type_to_stops')
    result_stop_times_df = utils_format._append_route_type(
        stops_df=stops_feed_1,
        stop_times_df=stop_times_feed_1,
        routes_df=routes_feed_1[['route_id', 'route_type']],
        trips_df=trips_feed_1[['trip_id', 'route_id']],
        info_to_append='route_type_to_stop_times')
    # check that route_type col was added and has no nulls
    assert 'route_type' in result_stops_df.columns
    assert result_stops_df[
               'route_type'].isnull().values.any() == False  # noqa
    assert 'route_type' in result_stop_times_df.columns
    assert result_stop_times_df[
               'route_type'].isnull().values.any() == False  # noqa
    # remainder of df should not have changed
    assert result_stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert result_stop_times_df[stop_times_feed_1.columns].equals(
        stop_times_feed_1)
    # check subset of values
    assert result_stops_df.iloc[0]['route_type'] == 3 and \
           result_stops_df.iloc[6]['route_type'] == 1
    assert result_stop_times_df.iloc[0]['route_type'] == 3 and \
           result_stop_times_df.iloc[36]['route_type'] == 1


def test_append_route_type_invalid_param(stops_feed_1, stop_times_feed_1,
                                         routes_feed_1, trips_feed_1):
    with pytest.raises(ValueError) as excinfo:
        result_stops_df = utils_format._append_route_type(
            stops_df=stops_feed_1,
            stop_times_df=stop_times_feed_1,
            routes_df=routes_feed_1[['route_id', 'route_type']],
            trips_df=trips_feed_1[['trip_id', 'route_id']],
            info_to_append='route_type_to_stops2')
    expected_error = 'route_type_to_stops2 is not a valid parameter.'
    assert expected_error in str(excinfo.value)


def test_add_unique_agencyid_case_1(
        agency_a_feed_on_disk_wo_agency, stops_feed_1, stop_times_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1, calendar_dates_feed_1):
    # case 1: no agency.txt file so we expect 'unique_agency_id' will be
    # generated using the GTFS feed folder name
    # a blank agency df will be generated by a prior function in workflow
    # so replicate this df for this test
    blank_agency = pd.DataFrame()
    # use test GTFS feed that has no agency.txt file in its dir
    feed_path = agency_a_feed_on_disk_wo_agency
    expected_unique_agency_id = os.path.split(feed_path)[1]
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=blank_agency,
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=calendar_feed_1,
            calendar_dates_df=calendar_dates_feed_1,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == \
               set([expected_unique_agency_id])
    # remainder of df should not have changed
    assert stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert routes_df[routes_feed_1.columns].equals(routes_feed_1)
    assert trips_df[trips_feed_1.columns].equals(trips_feed_1)
    assert stop_times_df[stop_times_feed_1.columns].equals(stop_times_feed_1)
    assert calendar_df[calendar_feed_1.columns].equals(calendar_feed_1)
    assert calendar_dates_df[calendar_dates_feed_1.columns].equals(
        calendar_dates_feed_1)


def test_add_unique_agencyid_case_2(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_3, stops_feed_1,
        stop_times_feed_1, routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    # case 2: has agency.txt but agency_id is missing and has 1 agency
    # (agency_feed_3) so we expect 'unique_agency_id' will be generated
    # using the agency_name in the agency.txt file
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_3,
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=calendar_feed_1,
            calendar_dates_df=calendar_dates_feed_1,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == set(['agency_c'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert routes_df[routes_feed_1.columns].equals(routes_feed_1)
    assert trips_df[trips_feed_1.columns].equals(trips_feed_1)
    assert stop_times_df[stop_times_feed_1.columns].equals(stop_times_feed_1)
    assert calendar_df[calendar_feed_1.columns].equals(calendar_feed_1)
    assert calendar_dates_df[calendar_dates_feed_1.columns].equals(
        calendar_dates_feed_1)


def test_add_unique_agencyid_case_3(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_2, stops_feed_2,
        stop_times_feed_2, routes_feed_2, trips_feed_2, calendar_feed_2,
        calendar_dates_feed_2):
    # case 3: has agency.txt and has agency_id and has 2 agencies
    # (agency_feed_2) so we expect 'unique_agency_id' will be generated using
    # the agency_id and agency_name in the agency.txt file
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_2,
            stops_df=stops_feed_2,
            routes_df=routes_feed_2,
            trips_df=trips_feed_2,
            stop_times_df=stop_times_feed_2,
            calendar_df=calendar_feed_2,
            calendar_dates_df=calendar_dates_feed_2,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == set(
            ['agency_b_district_2', 'agency_b_district_1'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_2.columns].equals(stops_feed_2)
    assert routes_df[routes_feed_2.columns].equals(routes_feed_2)
    assert trips_df[trips_feed_2.columns].equals(trips_feed_2)
    assert stop_times_df[stop_times_feed_2.columns].equals(stop_times_feed_2)
    assert calendar_df[calendar_feed_2.columns].equals(calendar_feed_2)
    assert calendar_dates_df[calendar_dates_feed_2.columns].equals(
        calendar_dates_feed_2)


def test_add_unique_agencyid_case_4(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_1, stops_feed_1,
        stop_times_feed_1, routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    # case 4: has agency.txt and has agency_id and has 1 agency
    # (agency_feed_1) so we expect 'unique_agency_id' will be generated using
    # the agency_name in the agency.txt file
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_1,
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=calendar_feed_1,
            calendar_dates_df=calendar_dates_feed_1,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == set(['agency_a_city_a'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert routes_df[routes_feed_1.columns].equals(routes_feed_1)
    assert trips_df[trips_feed_1.columns].equals(trips_feed_1)
    assert stop_times_df[stop_times_feed_1.columns].equals(stop_times_feed_1)
    assert calendar_df[calendar_feed_1.columns].equals(calendar_feed_1)
    assert calendar_dates_df[calendar_dates_feed_1.columns].equals(
        calendar_dates_feed_1)


def test_add_unique_agencyid_case_5(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_1, stops_feed_1,
        stop_times_feed_1, routes_feed_1, trips_feed_1, calendar_feed_1):
    # case 5: same as case 4 but with no calendar_dates.txt
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    # a blank calendar_dates df will be generated by a prior function in
    # workflow so replicate this df for this test
    blank_calendar_dates = pd.DataFrame()
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_1,
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=calendar_feed_1,
            calendar_dates_df=blank_calendar_dates,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df.empty is False
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == set(['agency_a_city_a'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert routes_df[routes_feed_1.columns].equals(routes_feed_1)
    assert trips_df[trips_feed_1.columns].equals(trips_feed_1)
    assert stop_times_df[stop_times_feed_1.columns].equals(stop_times_feed_1)
    assert calendar_df[calendar_feed_1.columns].equals(calendar_feed_1)
    assert calendar_dates_df.equals(blank_calendar_dates)


def test_add_unique_agencyid_case_6(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_1, stops_feed_1,
        stop_times_feed_1, routes_feed_1, trips_feed_1, calendar_dates_feed_1):
    # case 6: same as case 4 but with no calendar.txt
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    # a blank calendar_dates df will be generated by a prior function in
    # workflow so replicate this df for this test
    blank_calendar = pd.DataFrame()
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_1,
            stops_df=stops_feed_1,
            routes_df=routes_feed_1,
            trips_df=trips_feed_1,
            stop_times_df=stop_times_feed_1,
            calendar_df=blank_calendar,
            calendar_dates_df=calendar_dates_feed_1,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df,
               calendar_dates_feed_1]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df.empty is False
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check subset of values
        assert set(df['unique_agency_id'].unique()) == set(['agency_a_city_a'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_1.columns].equals(stops_feed_1)
    assert routes_df[routes_feed_1.columns].equals(routes_feed_1)
    assert trips_df[trips_feed_1.columns].equals(trips_feed_1)
    assert stop_times_df[stop_times_feed_1.columns].equals(stop_times_feed_1)
    assert calendar_dates_df[calendar_dates_feed_1.columns].equals(
        calendar_dates_feed_1)
    assert calendar_df.equals(blank_calendar)


def test_add_unique_agencyid_multi_agency_id_mismatch_via_agency_txt(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_2, stops_feed_2,
        stop_times_feed_2, routes_feed_2, trips_feed_2, calendar_feed_2,
        calendar_dates_feed_2):
    # has agency.txt and has agency_id and has 2 agencies (agency_feed_2)
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    # change one agency record 'agency_id' to something that will not
    # match existing records in routes.txt for test
    agency_feed_2['agency_id'].loc[1] = 'agency missing bus'
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_2,
            stops_df=stops_feed_2,
            routes_df=routes_feed_2,
            trips_df=trips_feed_2,
            stop_times_df=stop_times_feed_2,
            calendar_df=calendar_feed_2,
            calendar_dates_df=calendar_dates_feed_2,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check values
        assert set(df['unique_agency_id'].unique()) == set(
            ['multiple_operators_agency_a_wo_calendar_dates',
             'agency_b_district_1'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_2.columns].equals(stops_feed_2)
    assert routes_df[routes_feed_2.columns].equals(routes_feed_2)
    assert trips_df[trips_feed_2.columns].equals(trips_feed_2)
    assert stop_times_df[stop_times_feed_2.columns].equals(stop_times_feed_2)
    assert calendar_df[calendar_feed_2.columns].equals(calendar_feed_2)
    assert calendar_dates_df[calendar_dates_feed_2.columns].equals(
        calendar_dates_feed_2)


def test_add_unique_agencyid_multi_agency_id_mismatch_via_routes_txt(
        agency_a_feed_on_disk_wo_calendar_dates, agency_feed_2, stops_feed_2,
        stop_times_feed_2, routes_feed_2, trips_feed_2, calendar_feed_2,
        calendar_dates_feed_2):
    # has agency.txt and has agency_id and has 2 agencies (agency_feed_2)
    feed_path = agency_a_feed_on_disk_wo_calendar_dates
    # change one agency record 'agency_id' to something that will not
    # match existing records in agency.txt for test
    routes_feed_2['agency_id'].loc[0:1] = 'agency missing bus'
    stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
        calendar_dates_df = utils_format._add_unique_agencyid(
            agency_df=agency_feed_2,
            stops_df=stops_feed_2,
            routes_df=routes_feed_2,
            trips_df=trips_feed_2,
            stop_times_df=stop_times_feed_2,
            calendar_df=calendar_feed_2,
            calendar_dates_df=calendar_dates_feed_2,
            feed_folder=feed_path,
            nulls_as_folder=True)
    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df,
               calendar_dates_df]
    for df in df_list:
        # check that unique_agency_id column was created and there are no nulls
        assert 'unique_agency_id' in df.columns
        assert df['unique_agency_id'].isnull().values.any() == False  # noqa
        # check values
        assert set(df['unique_agency_id'].unique()) == set(
            ['multiple_operators_agency_a_wo_calendar_dates',
             'agency_b_district_2'])
    # remainder of df should not have changed
    assert stops_df[stops_feed_2.columns].equals(stops_feed_2)
    assert routes_df[routes_feed_2.columns].equals(routes_feed_2)
    assert trips_df[trips_feed_2.columns].equals(trips_feed_2)
    assert stop_times_df[stop_times_feed_2.columns].equals(stop_times_feed_2)
    assert calendar_df[calendar_feed_2.columns].equals(calendar_feed_2)
    assert calendar_dates_df[calendar_dates_feed_2.columns].equals(
        calendar_dates_feed_2)


def test_add_unique_agencyid_value_errors(
        agency_a_feed_on_disk_wo_calendar_dates,
        agency_a_feed_on_disk_wo_agency, agency_feed_1, stops_feed_1,
        stop_times_feed_1, routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    feed_path_wo_agency = agency_a_feed_on_disk_wo_agency
    feed_path_w_agency = agency_a_feed_on_disk_wo_calendar_dates
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and no agency.txt file is found
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_feed_1,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_wo_agency,
                nulls_as_folder=False)
    expected_error = (
        'No agency.txt file was found in {}. Add the missing file to '
        'folder or set nulls_as_folder to True.'.format(feed_path_wo_agency))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and both 'agency_id' and
        # 'agency_name' cols do not exist in agency.txt
        data = {
            'agency_url': 'http://www.agency_c.org',
            'agency_timezone': 'America/Los_Angeles',
            'agency_phone': '(000) 000-0000'}
        index = range(1)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = (
        'Both agency_name and agency_id columns were not found in agency.txt.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and 'agency_id' and
        # 'agency_name' with 1 record = '' in agency.txt
        data = {
            'agency_id': '',
            'agency_name': '',
            'agency_url': 'http://www.agency_c.org',
            'agency_timezone': 'America/Los_Angeles',
            'agency_phone': '(000) 000-0000'}
        index = range(1)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = 'agency.txt has no agency_id or agency_name values.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and both 'agency_id' and
        # 'agency_name' with 1 record  = '   ' in agency.txt
        data = {
            'agency_id': '   ',
            'agency_name': '        ',
            'agency_url': 'http://www.agency_c.org',
            'agency_timezone': 'America/Los_Angeles',
            'agency_phone': '(000) 000-0000'}
        index = range(1)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = 'agency.txt has no agency_id or agency_name values.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and both 'agency_id' and
        # 'agency_name' with 1 record = nan in agency.txt
        data = {
            'agency_id': np.nan,
            'agency_name': np.nan,
            'agency_url': 'http://www.agency_c.org',
            'agency_timezone': 'America/Los_Angeles',
            'agency_phone': '(000) 000-0000'}
        index = range(1)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = 'agency.txt has no agency_id or agency_name values.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and both 'agency_id' and
        # 'agency_name' with 1 record = nan in agency.txt
        data = {
            'agency_id': 'agency_a',
            'agency_name': '',
            'agency_url': 'http://www.agency_c.org',
            'agency_timezone': 'America/Los_Angeles',
            'agency_phone': '(000) 000-0000'}
        index = range(1)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = 'Null value in agency_name was found.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # throw error if nulls_as_folder=False and both 'agency_id' and
        # 'agency_name' with more than 1 record have nulls or blank strs
        # in agency.txt
        data = {
            'agency_id': ['agency_a', '   '],
            'agency_name': ['', np.nan],
            'agency_url': ['http://www.agency_c.org'],
            'agency_timezone': ['America/Los_Angeles', 'America/Los_Angeles'],
            'agency_phone': ['(000) 000-0000', '(000) 000-0000']}
        index = range(2)
        agency_df = pd.DataFrame(data, index)
        stops_df, routes_df, trips_df, stop_times_df, calendar_df, \
            calendar_dates_df = utils_format._add_unique_agencyid(
                agency_df=agency_df,
                stops_df=stops_feed_1,
                routes_df=routes_feed_1,
                trips_df=trips_feed_1,
                stop_times_df=stop_times_feed_1,
                calendar_df=calendar_feed_1,
                calendar_dates_df=calendar_dates_feed_1,
                feed_folder=feed_path_w_agency,
                nulls_as_folder=False)
    expected_error = 'Null values found in agency_id and agency_name.'
    assert expected_error in str(excinfo.value)
