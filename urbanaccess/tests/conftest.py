import pytest
import os
import pandas as pd
import numpy as np

from urbanaccess.tests.test_gtfs_headways import \
    _build_expected_hw_intermediate_stop_times_data


@pytest.fixture
def agency_feed_1():
    data = {
        'agency_id': 'agency a',
        'agency_name': 'agency a city a',
        'agency_url': 'http://www.agency_a.org',
        'agency_timezone': 'America/Los_Angeles',
        'agency_phone': '(000) 000-0000'
    }
    index = range(1)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def agency_feed_2():
    data = {
        'agency_id': ['agency b bus', 'agency b rail'],
        'agency_name': ['agency b district 1', 'agency b district 2'],
        'agency_url': ['http://www.agency_b.org', 'http://www.agency_b.org'],
        'agency_timezone': ['America/Los_Angeles', 'America/Los_Angeles'],
        'agency_phone': ['(000) 000-0000', '(000) 000-0000']
    }
    index = range(2)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def agency_feed_3():
    data = {
        'agency_id': '',
        'agency_name': 'agency c',
        'agency_url': 'http://www.agency_c.org',
        'agency_timezone': 'America/Los_Angeles',
        'agency_phone': '(000) 000-0000'
    }
    index = range(1)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def agency_feed_4():
    data = {
        'agency_id': ['agency 1', 'agency 2', 'agency 3'],
        'agency_name': ['agency 1 bus', 'agency 2 rail', 'agency 3 metro'],
        'agency_url': ['http://www.agency_1.org', 'http://www.agency_2.org',
                       'http://www.agency_2.org'],
        'agency_timezone': ['America/Los_Angeles', 'America/Los_Angeles',
                            'America/Los_Angeles'],
        'agency_phone': ['(000) 000-0000', '(000) 000-0000', '(000) 000-0000']
    }
    index = range(3)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def routes_feed_1():
    data = {
        'agency_id': ['agency a'] * 4,
        'route_id': ['10-101', '11-101', '12-101', '13-101'],
        'route_short_name': ['10', '11', 'red', 'blue'],
        'route_long_name': ['ave a local', 'ave a express', 'red line',
                            'blue line'],
        'route_type': [3, 3, 1, 1]
    }

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def routes_feed_2():
    data = {
        'agency_id': ['agency b bus', 'agency b bus', 'agency b rail',
                      'agency b rail'],
        'route_id': ['40-4', '40-4x', 'r-2', 'r-2ext'],
        'route_short_name': ['40', '40', 'red', 'red-ext'],
        'route_long_name': ['ave a local', 'ave a express', 'red line',
                            'red line extension'],
        'route_type': [3, 3, 1, 1]
    }

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def routes_feed_4():
    data = {
        'agency_id': ['agency 1', 'agency 1', 'agency 2', 'agency 2',
                      'agency 3', 'agency 3'],
        'route_id': ['a1x', 'a1', 'a2x', 'a2', 'a3x', 'a3'],
        'route_short_name': ['1x', '1', '2x', '2', '3x', '3'],
        'route_long_name': ['1 express', '1 local', '2 express',
                            '2 local', '3 express', '3 local'],
        'route_type': [3, 3, 3, 3, 3, 3]
    }

    index = range(6)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stops_feed_1():
    data = {
        'stop_id': ['1', '2', '3', '4', '5', '6',
                    '7', '8', '9'],
        'stop_name': ['ave a', 'ave b', 'ave c', 'ave d', 'ave e', 'ave f',
                      '1st st', '2nd st', '3rd st'],
        'stop_lat': [37.797484, 37.774963, 37.803664, 37.80787, 37.828415,
                     37.844601, 37.664174, 37.591208, 37.905628],
        'stop_lon': [-122.265609, -122.224274, -122.271604, -122.269029,
                     -122.267227, -122.251793, -122.444116, -122.017867,
                     -122.067423],
        'location_type': [1, 1, 1, 1, 1, 1,
                          2, 2, 2],
        'wheelchair_boarding': [1, 0, 0, 0, 0, 0,
                                1, 1, 1]
    }

    index = range(9)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stops_feed_2():
    data = {
        'stop_id': ['60', '61', '62', '63', '64', '65',
                    '66', '67', '68',
                    '600', '601', '602', '603', '604', '605', '606'],
        'stop_name': ['ave m', 'ave n', 'ave o', 'ave p', 'ave q', 'ave r',
                      '10th st', '11th st', '12th st',
                      '121th st', '122th st', '123th st', '124th st',
                      '125th st', '126th st', '127th st'],
        'stop_lat': [38.797484, 38.774963, 38.803664, 38.80787, 38.828415,
                     38.844601, 38.664174, 38.591208, 38.905628,
                     38.603664, 38.60787, 38.628415,
                     38.644601, 38.660000, 38.691208, 38.605628],
        'stop_lon': [-121.265609, -121.224274, -121.271604, -121.269029,
                     -121.267227, -121.251793, -121.444116, -121.017867,
                     -121.067423, -122.271604, -122.269029, -122.267227,
                     -122.251793, -122.444116, -122.017867, -122.067423],
        'location_type': [1, 1, 1, 1, 1, 1,
                          2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
        'wheelchair_boarding': [1, 0, 0, 0, 0, 0,
                                1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    }

    index = range(16)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stops_feed_4():
    data = {
        'stop_id': ['70', '71', '72', '73', '74', '75',
                    '76', '77', '78'],
        'stop_name': ['station 1', 'station 2', 'station 3', 'station 4',
                      'station 5', 'station 6',
                      'station 7', 'station 8', 'station 9'],
        'stop_lat': [20.797484, 20.774963, 20.803664, 20.80787, 20.828415,
                     20.844601, 20.664174, 20.591208, 20.905628],
        'stop_lon': [-100.265609, -100.224274, -100.271604, -100.269029,
                     -100.267227, -100.251793, -100.444116, -100.017867,
                     -100.067423]
    }

    index = range(9)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def trips_feed_1():
    data = {
        'route_id': ['10-101', '10-101', '10-101', '10-101',
                     '11-101', '11-101',
                     '12-101', '12-101',
                     '13-101', '13-101'],
        'trip_id': ['a1', 'a2', 'a3', 'a4',
                    'b1', 'b2',
                    'c1', 'c2',
                    'd1', 'd2'],
        'service_id': ['weekday-1', 'weekday-1', 'weekday-1', 'weekday-1',
                       'weekday-2', 'weekday-2',
                       'weekday-3', 'weekday-3',
                       'weekend-1', 'weekend-1'],
        'direction_id': [1, 0, 1, 0,
                         1, 0,
                         1, 0,
                         1, 0],
        'wheelchair_accessible': [1, 1, 1, 1,
                                  0, 0,
                                  0, 0,
                                  0, 0],
        'bikes_allowed': [1, 1, 1, 1,
                          0, 0,
                          0, 0,
                          0, 0]
    }

    index = range(10)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def trips_feed_2():
    data = {
        'route_id': ['40-4', '40-4', '40-4', '40-4',
                     '40-4x', '40-4x',
                     'r-2', 'r-2',
                     'r-2ext', 'r-2ext'],
        'trip_id': ['11', '12', '13', '14',
                    '21', '22',
                    '31', '32',
                    '41', '42'],
        'service_id': ['weekday-1', 'weekday-1', 'weekday-1', 'weekday-1',
                       'weekday-2', 'weekday-2',
                       'weekday-3', 'weekday-3',
                       'weekend-1', 'weekend-1']
    }

    index = range(10)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def trips_feed_4():
    data = {
        'route_id': ['a1x', 'a1x', 'a1x', 'a1x',
                     'a1', 'a1',
                     'a2x', 'a2x',
                     'a2', 'a2',
                     'a3x', 'a3x',
                     'a3', 'a3'],
        'trip_id': ['a131', 'a132', 'a133', 'a134',
                    'a135', 'a136',
                    'a237', 'a238',
                    'a239', 'a240',
                    'a341', 'a342',
                    'a343', 'a344'],
        'service_id': ['wk-1', 'wk-1', 'wk-1', 'wk-1',
                       'wk-1', 'wk-1',
                       'wk-1', 'wk-1',
                       'wk-1', 'wk-1',
                       'wk-1', 'wk-1',
                       'wk-1', 'wk-1']
    }

    index = range(14)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_feed_1():
    data = {
        'service_id': ['weekday-1',
                       'weekday-2',
                       'weekday-3',
                       'weekend-1'],
        'monday': [1, 1, 1, 0],
        'tuesday': [1, 1, 1, 0],
        'wednesday': [1, 1, 1, 0],
        'thursday': [1, 1, 1, 0],
        'friday': [1, 1, 1, 0],
        'saturday': [0, 0, 0, 1],
        'sunday': [0, 0, 0, 1],
        'start_date': ['20161224'] * 4,
        'end_date': ['20170318'] * 4}

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_feed_2():
    data = {
        'service_id': ['weekday-1',
                       'weekday-2',
                       'weekday-3',
                       'weekend-1'],
        'monday': [1, 1, 1, 0],
        'tuesday': [1, 1, 1, 0],
        'wednesday': [1, 1, 1, 0],
        'thursday': [1, 1, 1, 0],
        'friday': [1, 1, 1, 0],
        'saturday': [0, 0, 0, 1],
        'sunday': [0, 0, 0, 1],
        'start_date': ['20161224'] * 4,
        'end_date': ['20170318'] * 4}

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_feed_4():
    data = {
        'service_id': ['wk-1'],
        'monday': [1],
        'tuesday': [1],
        'wednesday': [1],
        'thursday': [1],
        'friday': [1],
        'saturday': [0],
        'sunday': [0],
        'start_date': ['20161224'],
        'end_date': ['20170318']}

    index = range(1)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_dates_feed_1():
    data = {
        'service_id': ['weekday-1',
                       'weekday-2',
                       'weekday-3',
                       'weekend-1'],
        'date': ['20161224', '20170318', '20160424', '20161230'],
        'exception_type': ['1', '2', '1', '1'],
        'schedule_type': ['WD', 'WD', 'WD', 'WE']}

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_dates_feed_2():
    data = {
        'service_id': ['weekday-1',
                       'weekday-2',
                       'weekday-3',
                       'weekend-1'],
        'date': ['20161224', '20170318', '20160424', '20161230'],
        'exception_type': ['1', '2', '1', '1'],
        'schedule_type': ['WD', 'WD', 'WD', 'SA']
    }

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_dates_feed_4():
    data = {
        'service_id': ['wk-1'],
        'date': ['20161224'],
        'exception_type': ['1']}

    index = range(1)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stop_times_feed_1():
    data = {
        'trip_id': ['a1', 'a1', 'a1', 'a1', 'a1', 'a1',
                    'a2', 'a2', 'a2', 'a2', 'a2', 'a2',
                    'a3', 'a3', 'a3', 'a3', 'a3', 'a3',
                    'a4', 'a4', 'a4', 'a4', 'a4', 'a4',
                    'b1', 'b1', 'b1', 'b1', 'b1', 'b1',
                    'b2', 'b2', 'b2', 'b2', 'b2', 'b2',
                    'c1', 'c1', 'c1', 'c1', 'c1', 'c1',
                    'c2', 'c2', 'c2', 'c2', 'c2', 'c2',
                    'd1', 'd1', 'd1',
                    'd2', 'd2', 'd2'],
        'stop_id': ['1', '2', '3', '4', '5', '6',
                    '6', '5', '4', '3', '2', '1',
                    '1', '2', '3', '4', '5', '6',
                    '6', '5', '4', '3', '2', '1',
                    '1', '2', '3', '4', '5', '6',
                    '6', '5', '4', '3', '2', '1',
                    '1', '2', '3', '4', '5', '6',
                    '6', '5', '4', '3', '2', '1',
                    '7', '8', '9',
                    '9', '8', '7'],
        'arrival_time': ['06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                         '08:35:00', '08:40:00',
                         '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                         '13:35:00', '13:40:00',
                         '06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '26:15:00', '26:20:00', np.nan, np.nan, '26:35:00',
                         '26:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00'],
        'departure_time': ['06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                           '08:35:00', '08:40:00',
                           '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                           '13:35:00', '13:40:00',
                           '06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '26:15:00', '26:20:00', np.nan, np.nan,
                           '26:35:00', '26:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00'],
        'stop_sequence': [1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3,
                          1, 2, 3],
        'pickup_type': [0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        1, 1, 1, 1, 1, 1,
                        0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        0, 0, 0,
                        0, 0, 0],
        'drop_off_type': [0, 0, 0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0,
                          1, 1, 1, 1, 1, 1,
                          0, 0, 0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0,
                          0, 0, 0,
                          0, 0, 0]
    }
    index = range(54)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stop_times_feed_2():
    data = {
        'trip_id': ['11', '11', '11', '11', '11', '11',
                    '12', '12', '12', '12', '12', '12',
                    '13', '13', '13', '13', '13', '13',
                    '14', '14', '14', '14', '14', '14',
                    '21', '21', '21', '21', '21', '21',
                    '22', '22', '22', '22', '22', '22',
                    '31', '31', '31', '31', '31', '31',
                    '32', '32', '32', '32', '32', '32',
                    '41', '41', '41',
                    '42', '42', '42'],
        'stop_id': ['60', '61', '62', '63', '64', '65',
                    '65', '64', '63', '62', '61', '60',
                    '60', '61', '62', '63', '64', '65',
                    '65', '64', '63', '62', '61', '60',
                    '60', '61', '62', '63', '64', '65',
                    '65', '64', '63', '62', '61', '60',
                    '600', '601', '602', '603', '604', '605',
                    '606', '605', '604', '603', '602', '601',
                    '66', '67', '68',
                    '68', '67', '66'],
        'arrival_time': ['06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                         '08:35:00', '08:40:00',
                         '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                         '13:35:00', '13:40:00',
                         '06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '26:15:00', '26:20:00', np.nan, np.nan, '26:35:00',
                         '26:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00'],
        'departure_time': ['06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                           '08:35:00', '08:40:00',
                           '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                           '13:35:00', '13:40:00',
                           '06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '26:15:00', '26:20:00', np.nan, np.nan,
                           '26:35:00', '26:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00'],
        'stop_sequence': [1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3,
                          1, 2, 3]
    }
    index = range(54)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def stop_times_feed_4():
    data = {
        'trip_id': ['a131', 'a131', 'a131', 'a131', 'a131', 'a131',
                    'a132', 'a132', 'a132', 'a132', 'a132', 'a132',
                    'a133', 'a133', 'a133', 'a133', 'a133', 'a133',
                    'a134', 'a134', 'a134', 'a134', 'a134', 'a134',
                    'a135', 'a135', 'a135', 'a135', 'a135', 'a135',
                    'a136', 'a136', 'a136', 'a136', 'a136', 'a136',
                    'a237', 'a237', 'a237', 'a237', 'a237', 'a237',
                    'a238', 'a238', 'a238', 'a238', 'a238', 'a238',
                    'a239', 'a239', 'a239',
                    'a240', 'a240', 'a240',
                    'a341', 'a341', 'a341',
                    'a342', 'a342', 'a342',
                    'a343', 'a343', 'a343',
                    'a344', 'a344', 'a344'],
        'stop_id': ['70', '71', '72', '73', '74', '75',
                    '75', '74', '73', '72', '71', '70',
                    '70', '71', '72', '73', '74', '75',
                    '75', '74', '73', '72', '71', '70',
                    '70', '71', '72', '73', '74', '75',
                    '75', '74', '73', '72', '71', '70',
                    '70', '71', '72', '73', '74', '75',
                    '75', '74', '73', '72', '71', '70',
                    '76', '77', '78',
                    '78', '77', '76',
                    '76', '77', '78',
                    '78', '77', '76',
                    '76', '77', '78',
                    '78', '77', '76'],

        'arrival_time': ['06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                         '08:35:00', '08:40:00',
                         '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                         '13:35:00', '13:40:00',
                         '06:15:00', '06:20:00', np.nan, np.nan, '06:35:00',
                         '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '26:15:00', '26:20:00', np.nan, np.nan, '26:35:00',
                         '26:40:00',
                         '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                         np.nan, '06:40:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00',
                         '06:15:00', '06:20:00', '06:25:00'],
        'departure_time': ['06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '08:15:00', '08:20:00', '08:25:00', '08:30:00',
                           '08:35:00', '08:40:00',
                           '13:15:00', '13:20:00', '13:25:00', '13:30:00',
                           '13:35:00', '13:40:00',
                           '06:15:00', '06:20:00', np.nan, np.nan,
                           '06:35:00', '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '26:15:00', '26:20:00', np.nan, np.nan,
                           '26:35:00', '26:40:00',
                           '06:15:00', '06:20:00', '06:25:00', '06:30:00',
                           np.nan, '06:40:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00',
                           '06:15:00', '06:20:00', '06:25:00'],
        'stop_sequence': [1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3, 4, 5, 6,
                          1, 2, 3,
                          1, 2, 3,
                          1, 2, 3,
                          1, 2, 3,
                          1, 2, 3,
                          1, 2, 3]
    }
    index = range(66)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def hw_stops_df_1_agency():
    # intended for use in headway tests
    data = {
        'stop_id': ['1', '2', '3', '4', '5', '6',
                    '7', '8', '9'],
        'stop_name': ['ave a', 'ave b', 'ave c', 'ave d', 'ave e', 'ave f',
                      '1st st', '2nd st', '3rd st'],
        'stop_lat': [37.797484, 37.774963, 37.803664, 37.80787, 37.828415,
                     37.844601, 37.664174, 37.591208, 37.905628],
        'stop_lon': [-122.265609, -122.224274, -122.271604, -122.269029,
                     -122.267227, -122.251793, -122.444116, -122.017867,
                     -122.067423],
        'location_type': [1, 1, 1, 1, 1, 1,
                          2, 2, 2],
        'wheelchair_boarding': [1, 0, 0, 0, 0, 0,
                                1, 1, 1],
        'unique_agency_id': ['agency_a_city_a'] * 9
    }
    index = range(9)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def hw_routes_df_1_agency():
    # intended for use in headway tests
    data = {
        'agency_id': ['agency a city a'] * 2,
        'route_id': ['10-101', 'B1'],
        'route_long_name': ['Route 10-101', 'B1 Express'],
        'route_type': [3, 2],
        'unique_agency_id': ['agency_a_city_a'] * 2,
        'unique_feed_id': ['feed_1'] * 2}
    index = range(2)
    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def hw_stop_times_int_df_1_agency():
    # intended for use in headway tests

    # only generate the data that is expected, leaving out: arrival_time,
    # departure_time

    # outbound direction
    data = {
        'trip_id': ['a1', 'a1',
                    'a1', 'a1',
                    'a1', 'a1',
                    'B1', 'B1',
                    'B1'],
        'stop_id': [1, 2, 3, 4, 5, 6,
                    7, 8, 9],
        'stop_sequence': [1, 2, 3, 4, 5, 6,
                          1, 2, 3],
        'unique_agency_id': ['agency_a_city_a'] * 9,
        'unique_feed_id': ['feed_1'] * 9,
        'route_type': [3, 3, 3, 3, 3, 3,
                       2, 2, 2],
        'departure_time_sec': [
            22500.0, 22620.0, 22860.0, 23100.0, 23580.0, 23940.0,
            22500.0, 22680.0, 22980.0],
        # nan, 2, 4, 4, 8, 6 # min
        # nan, 3, 5 # min
        'timediff': [
            np.nan, 120, 240, 240, 480, 360,
            np.nan, 180, 300]
    }
    index = range(9)
    direction_0 = pd.DataFrame(data, index)

    # inbound direction
    data = {
        'trip_id': ['a2', 'a2',
                    'a2', 'a2',
                    'a2', 'a2',
                    'B2', 'B2',
                    'B2'],
        'stop_id': [6, 5, 4, 3, 2, 1,
                    9, 8, 7],
        'stop_sequence': [1, 2, 3, 4, 5, 6,
                          1, 2, 3],
        'unique_agency_id': ['agency_a_city_a'] * 9,
        'unique_feed_id': ['feed_1'] * 9,
        'route_type': [3, 3, 3, 3, 3, 3,
                       2, 2, 2],
        'departure_time_sec': [
            23940.0, 23580.0, 23100.0, 22860.0, 22620.0, 22500.0,
            22980.0, 22680.0, 22500.0],
        # nan, 2, 4, 4, 8, 6 # min
        # nan, 3, 5 # min
        'timediff': [
            np.nan, 360, 480, 240, 240, 120,
            np.nan, 300, 180]
    }
    index = range(9)
    direction_1 = pd.DataFrame(data, index)

    df = pd.concat([direction_1, direction_0], ignore_index=True)

    # make multiple trips that stop at the same stops but at offset times
    # so we can replicate headways for each route
    merged_dfs = df.copy()
    trip_count = 5
    # set some trips to have even headways
    even_headway = 1200
    # set some trips to have uneven headways but predicable
    uneven_headway = {1: 900, 2: 1200, 3: 500, 4: 600, 5: 1800}
    for trip_id in range(trip_count):
        df_2 = df.copy()
        trip_num = trip_id + 1
        # 20 min headways for (1200 sec) with even spacing
        col = 'departure_time_sec'
        trip = 'a1'
        df_2.loc[df['trip_id'] == trip, col] = \
            df_2.loc[df['trip_id'] == trip, col] + even_headway
        trip = 'a2'
        df_2.loc[df['trip_id'] == trip, col] = \
            df_2.loc[df['trip_id'] == trip, col] + even_headway
        # 15-30 min headways for (900-1200 sec) with uneven spacing and
        trip = 'B1'
        df_2.loc[df_2['trip_id'] == trip, col] = \
            df_2.loc[df['trip_id'] == trip, col] + uneven_headway[trip_num]
        trip = 'B2'
        df_2.loc[df_2['trip_id'] == trip, col] = \
            df_2.loc[df['trip_id'] == trip, col] + uneven_headway[trip_num]
        df_2['trip_id'] = df_2['trip_id'] + '_{}'.format(trip_num)

        merged_dfs = merged_dfs.append(df_2, ignore_index=True)

    # add the other expected cols
    merged_dfs['departure_time_sec_interpolate'] = merged_dfs[
        'departure_time_sec'].astype('int')
    merged_dfs['unique_stop_id'] = (
        merged_dfs['stop_id'].astype('str').str.cat(
            merged_dfs['unique_agency_id'].astype('str'), sep='_'))
    merged_dfs['unique_trip_id'] = merged_dfs['trip_id'].str.cat(
        merged_dfs['unique_agency_id'].astype('str'), sep='_')

    return merged_dfs


@pytest.fixture
def hw_trips_df_1_agency():
    # intended for use in headway tests
    data = {
        'route_id': ['10-101', '10-101',
                     'B1', 'B1'],
        'trip_id': ['a1', 'a2',
                    'B1', 'B2'],
        'service_id': ['weekday-1'] * 4,
        'direction_id': [1, 0,
                         1, 0],
        'unique_agency_id': ['agency_a_city_a'] * 4,
        'unique_feed_id': ['feed_1'] * 4}
    index = range(4)
    df = pd.DataFrame(data, index)

    # expand trips to match what is in the test stop_times table
    merged_dfs = df.copy()
    trip_count = 5
    for trip_id in range(trip_count):
        df_2 = df.copy()
        trip_num = trip_id + 1
        df_2['trip_id'] = df_2['trip_id'] + '_{}'.format(trip_num)
        merged_dfs = merged_dfs.append(df_2, ignore_index=True)

    return merged_dfs


@pytest.fixture()
def hw_headways_df_1_agency(
        hw_trips_df_1_agency, hw_routes_df_1_agency,
        hw_stop_times_int_df_1_agency):
    # intended for use in headway tests

    # build expected headway table
    int_stop_times = _build_expected_hw_intermediate_stop_times_data(
        trips_df=hw_trips_df_1_agency,
        routes_df=hw_routes_df_1_agency,
        stop_times_df=hw_stop_times_int_df_1_agency)

    int_stop_times['unique_stop_route'] = (
        int_stop_times['unique_stop_id'].str.cat(
            int_stop_times['unique_route_id'].astype('str'), sep=','))

    stop_route_groups = int_stop_times.groupby('unique_stop_route')

    results = {}
    col = 'departure_time_sec_interpolate'
    for unique_stop_route, stop_route_group in stop_route_groups:
        stop_route_group.sort_values([col], ascending=True, inplace=True)
        next_veh_time = (stop_route_group[col].iloc[1:].values)
        prev_veh_time = (stop_route_group[col].iloc[:-1].values)
        stop_route_group_headways = (next_veh_time - prev_veh_time) / 60
        results[unique_stop_route] = (pd.Series(stop_route_group_headways)
                                      .describe())

    headway_by_routestop_df = pd.DataFrame(results).T

    headway_by_routestop_df = pd.merge(
        headway_by_routestop_df,
        int_stop_times[
            ['unique_stop_route', 'unique_stop_id', 'unique_route_id']],
        how='left', left_index=True, right_on='unique_stop_route', sort=False)
    headway_by_routestop_df.drop('unique_stop_route', axis=1, inplace=True)
    headway_by_routestop_df['node_id_route'] = (
        headway_by_routestop_df['unique_stop_id'].str.cat(
            headway_by_routestop_df['unique_route_id'].astype('str'), sep='_'))

    return headway_by_routestop_df


@pytest.fixture()
def agency_a_feed_on_disk_wo_calendar_dates(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_wo_calendar_dates')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_wo_calendar(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_dates_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar_dates': calendar_dates_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_wo_calendar')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_w_calendar_and_calendar_dates(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1, calendar_dates_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1,
                      'calendar_dates': calendar_dates_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_w_both_calendars')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_wo_calendar_and_calendar_dates(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_wo_both_calendar')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_wo_req_file(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, calendar_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'calendar': calendar_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_wo_req_file')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_wo_agency(
        tmpdir,
        stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1):
    feed_file_dict = {'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'agency_a_wo_agency')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_a_feed_on_disk_w_calendar_and_calendar_dates_empty_txt(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1, calendar_dates_feed_1):
    # create empty dfs but keep the headers
    agency_feed_1 = pd.DataFrame(columns=agency_feed_1.columns)
    stop_times_feed_1 = pd.DataFrame(columns=stop_times_feed_1.columns)
    stops_feed_1 = pd.DataFrame(columns=stops_feed_1.columns)
    routes_feed_1 = pd.DataFrame(columns=routes_feed_1.columns)
    trips_feed_1 = pd.DataFrame(columns=trips_feed_1.columns)
    calendar_feed_1 = pd.DataFrame(columns=calendar_feed_1.columns)
    calendar_dates_feed_1 = pd.DataFrame(columns=calendar_dates_feed_1.columns)
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1,
                      'calendar_dates': calendar_dates_feed_1}
    feed_path = os.path.join(tmpdir.strpath, 'empty_txt_test')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path


@pytest.fixture()
def agency_b_feed_on_disk_w_calendar_and_calendar_dates(
        tmpdir,
        agency_feed_2, stop_times_feed_2, stops_feed_2,
        routes_feed_2, trips_feed_2, calendar_feed_2, calendar_dates_feed_2):
    feed_file_dict = {'agency': agency_feed_2,
                      'stop_times': stop_times_feed_2,
                      'stops': stops_feed_2,
                      'routes': routes_feed_2,
                      'trips': trips_feed_2,
                      'calendar': calendar_feed_2,
                      'calendar_dates': calendar_dates_feed_2}
    feed_path = os.path.join(tmpdir.strpath, 'agency_b_w_both_calendars')
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)
    return feed_path
