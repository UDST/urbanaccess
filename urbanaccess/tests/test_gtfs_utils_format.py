import pytest
import pandas as pd
import numpy as np
import os
from re import sub

from urbanaccess.gtfs.utils import utils_format


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
        'start_date': [20161224] * 4,
        'end_date': [20170318] * 4}

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
        'start_date': [20161224] * 4,
        'end_date': [20170318] * 4}

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
        'start_date': [20161224],
        'end_date': [20170318]}

    index = range(1)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_empty():
    columns = {'service_id',
               'monday',
               'tuesday',
               'wednesday',
               'thursday',
               'friday',
               'saturday',
               'sunday',
               'start_date',
               'end_date'}

    df = pd.DataFrame(columns=columns)
    return df


@pytest.fixture
def calendar_dates_feed_1():
    data = {
        'service_id': ['weekday-1',
                       'weekday-2',
                       'weekday-3',
                       'weekend-1'],
        'date': [20161224, 20170318, 20160424, 20161230],
        'exception_type': [1, 2, 1, 1]}

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
        'date': [20161224, 20170318, 20160424, 20161230],
        'exception_type': [1, 2, 1, 1],
        'schedule_type': ['WD', 'WD', 'WD', 'SA']
    }

    index = range(4)

    df = pd.DataFrame(data, index)
    return df


@pytest.fixture
def calendar_dates_feed_4():
    data = {
        'service_id': ['wk-1'],
        'date': [20161224],
        'exception_type': [1]}

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
def folder_feed_1():
    return r'/data/gtfs_feeds/agency_a'


@pytest.fixture
def folder_feed_2():
    return r'/data/gtfs_feeds/agency_b'


@pytest.fixture
def folder_feed_4():
    return r'/data/gtfs_feeds/city'


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
