import pytest
import pandas as pd

import urbanaccess.gtfs.utils_calendar as gtfs_utils_cal


@pytest.fixture
def trips_agency_a():
    data = {
        'service_id': [
            'summer-weekday-1', 'summer-weekday-1', 'summer-weekday-1',
            'summer-weekday-2', 'summer-weekday-3', 'summer-weekend-1',
            'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
            'fall-weekend-1',
            'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
            'fall-weekend-1',
            'special-game-day-1', 'special-game-day-2', 'special-game-day-3',
            'special-game-day-1', 'special-game-day-2', 'special-game-day-3'],
        'trip_id': ['a1', 'a2', 'a3', 'a4', 'a5', 'a6',
                    'b1', 'b2', 'b3', 'b4', 'b1', 'b2', 'b3', 'b4',
                    's1', 's2', 's3', 's4', 's5', 's6'],
        'route_id': ['1R'] * 20,
        'direction_id': [0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
                         0, 1, 0, 1, 0, 1],
        'unique_agency_id': ['agency_a_city_a'] * 20,
    }
    index = range(20)
    df = pd.DataFrame(data, index)
    df['unique_service_id'] = (df['service_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_'))
    return df


@pytest.fixture
def trips_agency_b():
    data = {
        'service_id': [
            'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
            'fall-weekend-1',
            'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
            'fall-weekend-1',
            'special-game-day-1', 'special-game-day-2', 'special-game-day-3',
            'special-game-day-1', 'special-game-day-2', 'special-game-day-3'],
        'trip_id': ['b1', 'b2', 'b3', 'b4', 'b1', 'b2', 'b3', 'b4',
                    's1', 's2', 's3', 's4', 's5', 's6'],
        'route_id': ['1R'] * 14,
        'direction_id': [0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1],
        'unique_agency_id': ['agency_b_city_a'] * 14,
    }
    index = range(14)
    df = pd.DataFrame(data, index)
    df['unique_service_id'] = (df['service_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_'))
    return df


@pytest.fixture
def calendar_agency_a():
    # 'summer-weekday-2' only runs on wednesdays
    # 'fall-weekday-1' overlaps with summer sched for month of august
    data = {
        'service_id':
            ['summer-weekday-1', 'summer-weekday-2', 'summer-weekday-3',
             'summer-weekend-1',
             'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
             'fall-weekend-1'],
        'monday': [1, 0, 1, 0, 1, 1, 1, 0],
        'tuesday': [1, 0, 1, 0, 1, 1, 1, 0],
        'wednesday': [1, 1, 1, 0, 1, 1, 1, 0],
        'thursday': [1, 0, 1, 0, 1, 1, 1, 0],
        'friday': [1, 0, 1, 0, 1, 1, 1, 0],
        'saturday': [0, 0, 0, 1, 0, 0, 0, 1],
        'sunday': [0, 0, 0, 1, 0, 0, 0, 1],
        'start_date':
            ['2016-6-01', '2016-6-01', '2016-6-01', '2016-6-01',
             '2016-8-01', '2016-9-01', '2016-9-01', '2016-9-01'],
        'end_date':
            ['2016-8-31', '2016-8-31', '2016-8-31', '2016-8-31',
             '2016-11-30', '2016-11-30', '2016-11-30', '2016-11-30'],
        'unique_agency_id': ['agency_a_city_a'] * 8,
        'unique_feed_id': ['agency_a_city_a_feed'] * 8
    }
    index = range(8)
    df = pd.DataFrame(data, index)
    df['unique_service_id'] = (df['service_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_'))
    return df


@pytest.fixture
def calendar_dates_agency_a():
    # holiday '2016-7-04' falls on a monday but runs on 'summer-weekend-1'
    # sched
    # holiday '2016-9-05' falls on a monday but runs on 'fall-weekend-1' sched
    # game day '2016-8-02' falls on a tuesday and runs extra service
    data = {
        'service_id':
            ['summer-weekday-1', 'summer-weekday-3', 'summer-weekend-1',
             'fall-weekday-1', 'fall-weekday-2', 'fall-weekday-3',
             'fall-weekend-1',
             'special-game-day-1', 'special-game-day-2', 'special-game-day-3'],
        'date': ['2016-7-04', '2016-7-04', '2016-7-04',
                 '2016-9-05', '2016-9-05', '2016-9-05', '2016-9-05',
                 '2016-8-02', '2016-8-02', '2016-8-02'],
        'exception_type': ['2', '2', '1',
                           '2', '2', '2', '1',
                           '1', '1', '1'],
        'schedule_type': ['holiday', 'holiday', 'holiday',
                          'holiday', 'holiday', 'holiday', 'holiday',
                          'game day', 'game day', 'game day'],
        'unique_agency_id': ['agency_a_city_a'] * 10,
        'unique_feed_id': ['agency_a_city_a_feed'] * 10
    }
    index = range(10)
    df = pd.DataFrame(data, index)

    return df


@pytest.fixture
def trips_and_cal_with_multi_freq_dates(
        trips_agency_a, calendar_dates_agency_a):
    data = {
        'service_id': [
            'special-game-day-1_a', 'special-game-day-2_a',
            'special-game-day-3_a', 'special-game-day-1_a',
            'special-game-day-2_a', 'special-game-day-3_a'],
        'trip_id': ['s1_a', 's2_a', 's3_a', 's4_a', 's5_a', 's6_a'],
        'route_id': ['1R'] * 6,
        'direction_id': [0, 1, 0, 1, 0, 0],
        'unique_agency_id': ['agency_a_city_a'] * 6
    }
    index = range(6)
    extra_trips = pd.DataFrame(data, index)
    extra_trips['unique_service_id'] = (extra_trips['service_id'].str.cat(
        extra_trips['unique_agency_id'].astype('str'), sep='_'))
    trips_df = pd.concat([trips_agency_a, extra_trips], ignore_index=True)

    data = {
        'service_id':
            ['special-game-day-1_a', 'special-game-day-2_a',
             'special-game-day-3_a'],
        'date': ['2016-12-02', '2016-12-02', '2016-12-02'],
        'exception_type': ['1', '1', '1'],
        'schedule_type': ['game day', 'game day', 'game day'],
        'unique_agency_id': ['agency_a_city_a'] * 3,
        'unique_feed_id': ['agency_a_city_a_feed'] * 3
    }
    index = range(3)
    extra_cal_dates = pd.DataFrame(data, index)
    cal_dates_df = pd.concat([calendar_dates_agency_a, extra_cal_dates],
                             ignore_index=True)

    return trips_df, cal_dates_df


@pytest.fixture
def calendar_agency_a_datetime(calendar_agency_a):
    cols = ['start_date', 'end_date']
    for col in cols:
        calendar_agency_a[col] = pd.to_datetime(
            calendar_agency_a[col], format='%y%m%d',
            infer_datetime_format=True)
    return calendar_agency_a


@pytest.fixture
def calendar_dates_agency_a_datetime(calendar_dates_agency_a):
    col = 'date'
    calendar_dates_agency_a[col] = pd.to_datetime(
        calendar_dates_agency_a[col], format='%y%m%d',
        infer_datetime_format=True)
    return calendar_dates_agency_a


@pytest.fixture
def calendar_dates_w_uni_svc_id(calendar_dates_agency_a):
    df = calendar_dates_agency_a.copy()
    df['unique_service_id'] = (df['service_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_'))
    return df


@pytest.fixture
def calendar_dates_w_uni_svc_id_datetime(calendar_dates_agency_a_datetime):
    df = calendar_dates_agency_a_datetime.copy()
    df['unique_service_id'] = (df['service_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_'))
    return df


@pytest.fixture
def calendar_empty(calendar_agency_a):
    df_empty = calendar_agency_a[0:0]
    return df_empty


@pytest.fixture
def calendar_dates_empty(calendar_dates_agency_a):
    df_empty = calendar_dates_agency_a[0:0]
    return df_empty


@pytest.fixture
def msg_param():
    return 'Selecting service_ids from calendar'


@pytest.fixture
def day_param():
    return 'monday'


@pytest.fixture
def date_param_cal():
    return '2016-06-13'


@pytest.fixture
def date_param_cal_dates():
    return '2016-08-02'


@pytest.fixture
def daterange_param():
    return ['2016-06-01', '2016-08-31']


@pytest.fixture
def cal_dates_lookup_param():
    return {'schedule_type': 'holiday',
            'service_id': ['fall-weekday-1', 'fall-weekday-2']}


@pytest.fixture
def expected_result_cal_param_day():
    expected_active_srv_ids = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a',
         'fall-weekday-1_agency_a_city_a',
         'fall-weekday-2_agency_a_city_a',
         'fall-weekday-3_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_param_date():
    expected_active_srv_ids = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_param_daterange():
    expected_active_srv_ids = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'summer-weekday-2_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a',
         'summer-weekend-1_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_dates_param_day():
    expected_active_srv_ids = sorted(
        ['summer-weekend-1_agency_a_city_a',
         'fall-weekend-1_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_dates_param_date():
    expected_active_srv_ids = sorted(
        ['special-game-day-3_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'special-game-day-1_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_dates_param_daterange():
    expected_active_srv_ids = sorted(
        ['special-game-day-1_agency_a_city_a',
         'special-game-day-3_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'summer-weekend-1_agency_a_city_a'])
    return expected_active_srv_ids


@pytest.fixture
def expected_result_cal_dates_param_lookup():
    expected_srvc_ids_add = sorted(
        ['summer-weekend-1_agency_a_city_a', 'fall-weekend-1_agency_a_city_a'])
    expected_srvc_ids_del = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'fall-weekday-3_agency_a_city_a',
         'fall-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a',
         'fall-weekday-2_agency_a_city_a'])
    return expected_srvc_ids_add, expected_srvc_ids_del


def test_select_cal_service_ids_w_day(
        calendar_agency_a, expected_result_cal_param_day, day_param):
    result = gtfs_utils_cal._select_calendar_service_ids(
        calendar_agency_a,
        params={'day': day_param,
                'date': None,
                'date_range': None,
                'cal_dates_lookup': None,
                'has_cal': True,
                'has_day_param': True,
                'has_date_param': False,
                'has_date_range_param': False,
                'has_cal_dates': False,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, list)
    assert len(result) == 5
    # result should only include service IDs in calendar where monday = 1
    assert sorted(result) == expected_result_cal_param_day


def test_select_cal_service_ids_w_date(
        calendar_agency_a, date_param_cal, expected_result_cal_param_date):
    result = gtfs_utils_cal._select_calendar_service_ids(
        calendar_agency_a,
        params={'day': None,
                'date': date_param_cal,
                'date_range': None,
                'cal_dates_lookup': None,
                'has_cal': True,
                'has_day_param': False,
                'has_date_param': True,
                'has_date_range_param': False,
                'has_cal_dates': False,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar that are active on
    # '2016-06-13' representing a monday sched
    assert sorted(result) == expected_result_cal_param_date


def test_select_cal_service_ids_w_date_range(
        calendar_agency_a, daterange_param,
        expected_result_cal_param_daterange):
    result = gtfs_utils_cal._select_calendar_service_ids(
        calendar_agency_a,
        params={'day': None,
                'date': None,
                'date_range': daterange_param,
                'cal_dates_lookup': None,
                'has_cal': True,
                'has_day_param': False,
                'has_date_param': False,
                'has_date_range_param': True,
                'has_cal_dates': False,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, list)
    assert len(result) == 4
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31']
    assert sorted(result) == expected_result_cal_param_daterange


def test_select_cal_service_ids_w_day_and_date_range(
        calendar_agency_a, day_param, daterange_param):
    result = gtfs_utils_cal._select_calendar_service_ids(
        calendar_agency_a,
        params={'day': day_param,
                'date': None,
                'date_range': daterange_param,
                'cal_dates_lookup': None,
                'has_cal': True,
                'has_day_param': True,
                'has_date_param': False,
                'has_date_range_param': True,
                'has_cal_dates': False,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': True})

    expected_active_srv_ids = sorted(
        ['summer-weekday-3_agency_a_city_a',
         'summer-weekday-1_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31'] and 'monday'
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_day_param_w_cal_only(
        calendar_agency_a, calendar_dates_empty,
        expected_result_cal_param_day, day_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_empty,
        day=day_param,
        date=None,
        date_range=None,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 5
    # result should only include service IDs in calendar where monday = 1
    assert sorted(result) == expected_result_cal_param_day


def test_cal_srv_id_selector_day_param_w_cal_dates_only(
        calendar_empty, calendar_dates_agency_a, day_param,
        expected_result_cal_dates_param_day):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=day_param,
        date=None,
        date_range=None,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar dates whose date is a
    # monday and exception_type = 1
    assert sorted(result) == expected_result_cal_dates_param_day


def test_cal_srv_id_selector_day_param_w_cal_and_cal_dates(
        calendar_agency_a, calendar_dates_agency_a):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day='tuesday',
        date=None,
        date_range=None,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a',
         'fall-weekday-1_agency_a_city_a',
         'fall-weekday-2_agency_a_city_a',
         'fall-weekday-3_agency_a_city_a',
         'special-game-day-1_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'special-game-day-3_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 8
    # result should only include service IDs in calendar dates whose date is a
    # tuesday regardless of exception_type
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_date_param_w_cal_only(
        calendar_agency_a, calendar_dates_empty, date_param_cal):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_empty,
        day=None,
        date=date_param_cal,
        date_range=None,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(
        ['summer-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar that are active on
    # '2016-06-13' representing a monday sched
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_date_param_w_cal_dates_only_case_1(
        calendar_empty, calendar_dates_agency_a, date_param_cal_dates,
        expected_result_cal_dates_param_date):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date=date_param_cal_dates,
        date_range=None,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 3
    # result should only include service IDs in calendar dates that are
    # active on '2016-08-02' with exception_type = 1
    assert sorted(result) == expected_result_cal_dates_param_date


def test_cal_srv_id_selector_date_param_w_cal_dates_only_case_2(
        calendar_empty, calendar_dates_agency_a):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date='2016-07-04',
        date_range=None,
        cal_dates_lookup=None)

    expected_active_srv_ids = ['summer-weekend-1_agency_a_city_a']

    assert isinstance(result, list)
    assert len(result) == 1
    # result should only include service IDs in calendar dates that are
    # active on '2016-07-04' with exception_type = 1
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_date_param_w_cal_and_cal_dates_case_1(
        calendar_agency_a, calendar_dates_agency_a, date_param_cal_dates):
    # case 1: add service IDs via cal dates
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date=date_param_cal_dates,
        date_range=None,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(
        ['special-game-day-1_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'special-game-day-3_agency_a_city_a',
         'summer-weekday-1_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a',
         'fall-weekday-1_agency_a_city_a'
         ])

    assert isinstance(result, list)
    assert len(result) == 6
    # result should only include service IDs in calendar that are active
    # on '2016-08-02' and add service IDs from calendar dates where
    # exception = 1 and remove service IDs from calendar dates where
    # exception = 2
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_date_param_w_cal_and_cal_dates_case_2(
        calendar_agency_a, calendar_dates_agency_a):
    # case 2: remove service IDs via cal dates
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date='2016-07-04',
        date_range=None,
        cal_dates_lookup=None)

    expected_active_srv_ids = ['summer-weekend-1_agency_a_city_a']

    assert isinstance(result, list)
    assert len(result) == 1
    # result should only include service IDs in calendar that are active
    # on '2016-07-04' and add service IDs from calendar dates where
    # exception = 1 and remove service IDs from calendar dates where
    # exception = 2
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_date_range_param_w_cal_only(
        calendar_agency_a, calendar_dates_empty, daterange_param,
        expected_result_cal_param_daterange):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_empty,
        day=None,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 4
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31']
    assert sorted(result) == expected_result_cal_param_daterange


def test_cal_srv_id_selector_date_range_param_w_cal_dates_only(
        calendar_empty, calendar_dates_agency_a, daterange_param,
        expected_result_cal_dates_param_daterange):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 4
    # result should only include service IDs in calendar dates that are
    # active between ['2016-06-01', '2016-08-31'] with exception_type = 1
    assert sorted(result) == expected_result_cal_dates_param_daterange


def test_cal_srv_id_selector_date_range_param_w_cal_and_cal_dates(
        calendar_agency_a, calendar_dates_agency_a, daterange_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day=None,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(
        ['special-game-day-1_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'special-game-day-3_agency_a_city_a',
         'summer-weekend-1_agency_a_city_a',
         'summer-weekday-2_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 5
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31'] and in calendar dates that are
    # active between ['2016-06-01', '2016-08-31'] with exception_type = 1
    # and remove service IDs from calendar dates where exception = 2
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_day_and_date_range_param_w_cal_only(
        calendar_agency_a, calendar_dates_empty, day_param, daterange_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_empty,
        day=day_param,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(
        ['summer-weekday-3_agency_a_city_a',
         'summer-weekday-1_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31'] and on 'monday'
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_day_and_date_range_param_w_cal_dates_only(
        calendar_empty, calendar_dates_agency_a, day_param, daterange_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=day_param,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    expected_active_srv_ids = ['summer-weekend-1_agency_a_city_a']

    assert isinstance(result, list)
    assert len(result) == 1
    # result should only include service IDs in calendar dates that are
    # active between ['2016-06-01', '2016-08-31'] on a 'monday' with
    # exception_type = 1
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_day_date_range_param_w_cal_and_cal_dates(
        calendar_agency_a, calendar_dates_agency_a, day_param,
        daterange_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day=day_param,
        date=None,
        date_range=daterange_param,
        cal_dates_lookup=None)

    expected_active_srv_ids = sorted(['summer-weekend-1_agency_a_city_a',
                                      'summer-weekday-3_agency_a_city_a',
                                      'summer-weekday-1_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 3
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31'] on a monday and in calendar dates
    # that are active between ['2016-06-01', '2016-08-31'] on a monday with
    # exception_type = 1
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_lookup_param_w_cal_only_invalid(
        calendar_agency_a, calendar_dates_empty, day_param,
        cal_dates_lookup_param):
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_utils_cal._calendar_service_id_selector(
            calendar_df=calendar_agency_a,
            calendar_dates_df=calendar_dates_empty,
            day=day_param,
            date=None,
            date_range=None,
            cal_dates_lookup=cal_dates_lookup_param)
    expected_error = ("Calendar_dates is empty. Unable to use the "
                      "'calendar_dates_lookup' parameter. Set to None.")
    assert expected_error in str(excinfo.value)


def test_cal_srv_id_selector_lookup_param_w_cal_dates_only(
        calendar_empty, calendar_dates_agency_a, day_param,
        cal_dates_lookup_param,
        expected_result_cal_dates_param_day):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_empty,
        calendar_dates_df=calendar_dates_agency_a,
        day=day_param,
        date=None,
        date_range=None,
        cal_dates_lookup=cal_dates_lookup_param)

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar dates that match
    # the values in the query with exception_type = 1 and where date is a
    # 'monday'
    assert sorted(result) == expected_result_cal_dates_param_day


def test_cal_srv_id_selector_lookup_param_w_cal_and_cal_dates(
        calendar_agency_a, calendar_dates_agency_a, day_param,
        cal_dates_lookup_param):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_agency_a,
        day=day_param,
        date=None,
        date_range=None,
        cal_dates_lookup=cal_dates_lookup_param)

    expected_active_srv_ids = sorted(
        ['fall-weekend-1_agency_a_city_a',
         'summer-weekend-1_agency_a_city_a'])

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar dates that match
    # the values in the query with exception_type = 1 and remove service IDs
    # from calendar dates where exception = 2 and also service IDs from
    # calendar that are active for 'monday'
    assert sorted(result) == expected_active_srv_ids


def test_cal_srv_id_selector_warn(capsys,
                                  calendar_agency_a, calendar_dates_empty):
    result = gtfs_utils_cal._calendar_service_id_selector(
        calendar_df=calendar_agency_a,
        calendar_dates_df=calendar_dates_empty,
        day=None,
        date='2016-01-01',
        date_range=None,
        cal_dates_lookup=None)

    assert isinstance(result, list)
    assert len(result) == 0

    # check that expected print prints
    captured = capsys.readouterr()
    assert ('Warning: No active service_ids were found matching the specified '
            'parameters.') in captured.out


def test_add_unique_service_id(
        calendar_dates_agency_a, calendar_agency_a):
    # remove the existing column from the test data
    calendar_agency_a.drop(columns=['unique_service_id'], inplace=True)
    df_list = [calendar_dates_agency_a, calendar_agency_a]
    result = gtfs_utils_cal._add_unique_service_id(df_list)
    assert isinstance(result, list)
    assert isinstance(result[0], pd.core.frame.DataFrame)
    assert isinstance(result[1], pd.core.frame.DataFrame)
    assert result[0].empty is False
    assert result[1].empty is False
    assert 'unique_service_id' in result[0].columns
    assert 'unique_service_id' in result[1].columns
    assert result[0]['unique_service_id'].iloc[0] == \
           'summer-weekday-1_agency_a_city_a'


def test_cal_date_dt_conversion_valid(calendar_agency_a):
    result = gtfs_utils_cal._cal_date_dt_conversion(
        df=calendar_agency_a,
        date_cols=['start_date', 'end_date'])
    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.dtypes['start_date'] == '<M8[ns]'
    assert result.dtypes['end_date'] == '<M8[ns]'


def test_cal_date_dt_conversion_invalid(calendar_agency_a):
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_utils_cal._cal_date_dt_conversion(
            df=calendar_agency_a,
            date_cols=['service_id'])
    expected_error = (
        "Column: service_id has values that are not in a supported date "
        "format. Expected format: 'YYYY-MM-DD'.")
    assert expected_error in str(excinfo.value)


def test_print_cal_service_ids_len(capsys):
    srvc_ids = ['service_id_1', 'service_id_2', 'service_id_3']
    gtfs_utils_cal._print_cal_service_ids_len(
        srvc_ids, table_name='calendar')
    # check that expected print prints
    captured = capsys.readouterr()
    assert '3 service_id(s) were selected from calendar.' in captured.out


def test_print_cal_service_ids_len_warn(capsys):
    gtfs_utils_cal._print_cal_service_ids_len(
        srvc_ids=[], table_name='calendar')
    # check that expected print prints
    captured = capsys.readouterr()
    assert 'Warning: No service_ids were selected from calendar.' \
           in captured.out


def test_intersect_cal_service_ids():
    dict_1 = {'day': ['service_id_1',
                      'service_id_2',
                      'service_id_3',
                      'service_id_4',
                      'service_id_5']}
    dict_2 = {'date range': ['service_id_1',
                             'service_id_2',
                             'service_id_6',
                             'service_id_7']}
    result = gtfs_utils_cal._intersect_cal_service_ids(
        dict_1, dict_2, verbose=True)

    expected_active_srv_ids = sorted(['service_id_1', 'service_id_2'])

    assert isinstance(result, list)
    assert len(result) == 2
    # result should be the intersection of the two service ID lists
    assert sorted(result) == expected_active_srv_ids


def test_calendar_date_ranges_wo_print(calendar_agency_a_datetime):
    result = gtfs_utils_cal._calendar_date_ranges(
        calendar_agency_a_datetime, for_print=False)

    expected_active_srv_ids = [
        {'start_date': pd.Timestamp('2016-06-01 00:00:00'),
         'end_date': pd.Timestamp('2016-08-31 00:00:00')},
        {'start_date': pd.Timestamp('2016-08-01 00:00:00'),
         'end_date': pd.Timestamp('2016-11-30 00:00:00')},
        {'start_date': pd.Timestamp('2016-09-01 00:00:00'),
         'end_date': pd.Timestamp('2016-11-30 00:00:00')}]

    assert isinstance(result, list)
    assert len(result) == 3
    assert result == expected_active_srv_ids


def test_calendar_date_ranges_w_print(calendar_agency_a_datetime):
    result = gtfs_utils_cal._calendar_date_ranges(
        calendar_agency_a_datetime, for_print=True)

    expected_active_srv_ids = sorted(
        ['2016-06-01 to 2016-08-31',
         '2016-08-01 to 2016-11-30',
         '2016-09-01 to 2016-11-30'])

    assert isinstance(result, list)
    assert len(result) == 3
    assert sorted(result) == expected_active_srv_ids


def test_parse_cal_dates_exception_type(calendar_dates_w_uni_svc_id):
    srvc_ids_add, srvc_ids_del = \
        gtfs_utils_cal._parse_cal_dates_exception_type(
            calendar_dates_w_uni_svc_id, verbose=True)

    expected_srvc_ids_add = sorted(
        ['summer-weekend-1_agency_a_city_a',
         'special-game-day-1_agency_a_city_a',
         'special-game-day-2_agency_a_city_a',
         'fall-weekend-1_agency_a_city_a',
         'special-game-day-3_agency_a_city_a'])
    expected_srvc_ids_del = sorted(
        ['fall-weekday-2_agency_a_city_a',
         'fall-weekday-1_agency_a_city_a',
         'summer-weekday-1_agency_a_city_a',
         'fall-weekday-3_agency_a_city_a',
         'summer-weekday-3_agency_a_city_a'])

    assert isinstance(srvc_ids_add, list)
    assert len(srvc_ids_add) == 5
    assert sorted(srvc_ids_add) == expected_srvc_ids_add

    assert isinstance(srvc_ids_del, list)
    assert len(srvc_ids_del) == 5
    assert sorted(srvc_ids_del) == expected_srvc_ids_del


def test_add_exception_type_service_id_lists():
    srvc_ids_list_dict = {
        'add': [
            ['service_id_1', 'service_id_2'],
            ['service_id_1', 'service_id_2', 'service_id_3']],
        'del': [
            [],
            ['service_id_1', 'service_id_4', 'service_id_5']]}
    srvc_ids_add, srvc_ids_del = \
        gtfs_utils_cal._add_exception_type_service_id_lists(
            srvc_ids_list_dict)

    expected_srvc_ids_add = sorted(
        ['service_id_1', 'service_id_2', 'service_id_3'])
    expected_srvc_ids_del = sorted(
        ['service_id_1', 'service_id_4', 'service_id_5'])

    assert isinstance(srvc_ids_add, list)
    assert len(srvc_ids_add) == 3
    assert sorted(srvc_ids_add) == expected_srvc_ids_add

    assert isinstance(srvc_ids_del, list)
    assert len(srvc_ids_del) == 3
    assert sorted(srvc_ids_del) == expected_srvc_ids_del


def test_merge_service_ids_cal_dates_w_cal():
    srvc_ids = ['service_id_1', 'service_id_2', 'service_id_3']
    srvc_ids_add = ['service_id_1', 'service_id_4', 'service_id_5']
    srvc_ids_del = ['service_id_1', 'service_id_2', 'service_id_6']
    result = gtfs_utils_cal._merge_service_ids_cal_dates_w_cal(
        srvc_ids, srvc_ids_add, srvc_ids_del, verbose=True)

    expected_srvc_ids = sorted(
        ['service_id_5', 'service_id_3', 'service_id_4'])

    assert isinstance(result, list)
    assert len(result) == 3
    assert sorted(result) == expected_srvc_ids


def test_select_calendar_dates_str_match(
        calendar_dates_w_uni_svc_id, cal_dates_lookup_param,
        expected_result_cal_dates_param_lookup):
    msg = '     Selecting service_ids from calendar_dates'
    srvc_ids_add, srvc_ids_del = \
        gtfs_utils_cal._select_calendar_dates_str_match(
            calendar_dates_w_uni_svc_id, msg, cal_dates_lookup_param)

    assert isinstance(srvc_ids_add, list)
    assert len(srvc_ids_add) == 2
    assert sorted(srvc_ids_add) == expected_result_cal_dates_param_lookup[0]

    assert isinstance(srvc_ids_del, list)
    assert len(srvc_ids_del) == 5
    assert sorted(srvc_ids_del) == expected_result_cal_dates_param_lookup[1]


def test_select_calendar_dates_str_match_warn(capsys,
                                              calendar_dates_w_uni_svc_id):
    msg = '     Selecting service_ids from calendar_dates'
    srvc_ids_add, srvc_ids_del = \
        gtfs_utils_cal._select_calendar_dates_str_match(
            calendar_dates_w_uni_svc_id, msg,
            cal_dates_lookup={'service_id': ['value1', 'value2']})

    assert isinstance(srvc_ids_add, list)
    assert len(srvc_ids_add) == 0

    assert isinstance(srvc_ids_del, list)
    assert len(srvc_ids_del) == 0

    # check that expected print prints
    captured = capsys.readouterr()
    assert ("Warning: No active service_ids were found with query: "
            "{'service_id': ['value1', 'value2']}") in captured.out


def test_print_count_service_ids(
        calendar_dates_w_uni_svc_id, capsys):
    df_dict = {'calendar_dates': calendar_dates_w_uni_svc_id}
    subset_ids = ['summer-weekend-1_agency_a_city_a',
                  'fall-weekend-1_agency_a_city_a']
    gtfs_utils_cal._print_count_service_ids(df_dict, subset_ids)
    # check that expected print prints
    captured = capsys.readouterr()
    assert '2 out of 10 unique service_id(s) from calendar_dates.' in \
           captured.out


def test_select_calendar_service_ids_by_day(
        calendar_agency_a, msg_param, day_param,
        expected_result_cal_param_day):
    result = gtfs_utils_cal._select_calendar_service_ids_by_day(
        calendar_agency_a, msg_param, day=day_param, verbose=True)

    assert isinstance(result, list)
    assert len(result) == 5
    # result should only include service IDs in calendar where monday = 1
    assert sorted(result) == expected_result_cal_param_day


def test_select_calendar_service_ids_by_day_empty(
        calendar_empty, msg_param, day_param):
    result = gtfs_utils_cal._select_calendar_service_ids_by_day(
        calendar_empty, msg_param, day=day_param, verbose=True)

    assert isinstance(result, list)
    assert len(result) == 0
    assert result == []


def test_select_calendar_service_ids_by_date(
        calendar_agency_a_datetime, msg_param, date_param_cal,
        expected_result_cal_param_date):
    result = gtfs_utils_cal._select_calendar_service_ids_by_date(
        calendar_agency_a_datetime, msg_param, date=date_param_cal,
        verbose=True)

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar that are active on
    # '2016-06-13' representing a monday sched
    assert sorted(result) == expected_result_cal_param_date


def test_select_calendar_service_ids_by_date_warn(capsys,
                                                  calendar_agency_a_datetime,
                                                  msg_param):
    result = gtfs_utils_cal._select_calendar_service_ids_by_date(
        calendar_agency_a_datetime, msg_param, date='2017-06-13',
        verbose=True)

    # check that expected print prints
    captured = capsys.readouterr()
    assert ("Warning: Date: 2017-06-13 does not fall within any date range(s) "
            "available in the calendar.") in captured.out


def test_select_calendar_service_ids_by_date_range(
        calendar_agency_a_datetime, msg_param, daterange_param,
        expected_result_cal_param_daterange):
    result = gtfs_utils_cal._select_calendar_service_ids_by_date_range(
        calendar_agency_a_datetime, msg_param, date_range=daterange_param)

    assert isinstance(result, list)
    assert len(result) == 4
    # result should only include service IDs in calendar that are active
    # between ['2016-06-01', '2016-08-31']
    assert sorted(result) == expected_result_cal_param_daterange


def test_select_calendar_service_ids_by_date_range_warn(
        capsys, calendar_agency_a_datetime, msg_param):
    result = gtfs_utils_cal._select_calendar_service_ids_by_date_range(
        calendar_agency_a_datetime, msg_param,
        date_range=['2016-01-01', '2016-02-01'])

    assert isinstance(result, list)
    assert len(result) == 0

    # check that expected print prints
    captured = capsys.readouterr()
    assert ("Warning: Date range: ['2016-01-01', '2016-02-01'] does not "
            "contain any of the date ranges available in the "
            "calendar.") in captured.out


def test_select_calendar_dates_service_ids_by_day(
        calendar_dates_w_uni_svc_id_datetime, msg_param, day_param,
        expected_result_cal_dates_param_day):
    result = gtfs_utils_cal._select_calendar_dates_service_ids_by_day(
        calendar_dates_w_uni_svc_id_datetime, msg_param, day=day_param)

    assert isinstance(result, list)
    assert len(result) == 2
    # result should only include service IDs in calendar dates whose date is a
    # monday and exception_type = 1
    assert sorted(result) == expected_result_cal_dates_param_day


def test_select_calendar_dates_service_ids_by_date(
        calendar_dates_w_uni_svc_id_datetime, msg_param, date_param_cal_dates,
        expected_result_cal_dates_param_date):
    result = gtfs_utils_cal._select_calendar_dates_service_ids_by_date(
        calendar_dates_w_uni_svc_id_datetime, msg_param,
        date=date_param_cal_dates,
        verbose=True)

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    assert len(result[0]) == 3
    assert len(result[1]) == 0
    # result should only include service IDs in calendar dates that are
    # active on '2016-08-02' with exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_date


def test_select_calendar_dates_service_ids_by_date_empty(
        calendar_dates_w_uni_svc_id_datetime, msg_param, date_param_cal,
        expected_result_cal_dates_param_date):
    result = gtfs_utils_cal._select_calendar_dates_service_ids_by_date(
        calendar_dates_w_uni_svc_id_datetime, msg_param, date=date_param_cal,
        verbose=True)

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should be empty
    assert len(result[0]) == 0
    assert len(result[1]) == 0


def test_select_calendar_dates_service_ids_by_daterange(
        calendar_dates_w_uni_svc_id_datetime, msg_param, daterange_param,
        expected_result_cal_dates_param_daterange):
    result = gtfs_utils_cal._select_calendar_dates_service_ids_by_date_range(
        calendar_dates_w_uni_svc_id_datetime, msg_param,
        date_range=daterange_param, verbose=True)

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that are
    # active between ['2016-06-01', '2016-08-31'] with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_daterange
    # exception_type = 2
    assert sorted(result[1]) == sorted(['summer-weekday-3_agency_a_city_a',
                                        'summer-weekday-1_agency_a_city_a'])


def test_select_calendar_dates_service_ids_by_daterange_warn(
        capsys, calendar_dates_w_uni_svc_id_datetime, msg_param):
    result = gtfs_utils_cal._select_calendar_dates_service_ids_by_date_range(
        calendar_dates_w_uni_svc_id_datetime, msg_param,
        date_range=['2016-01-01', '2016-02-01'], verbose=True)

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    assert len(result[0]) == 0
    assert len(result[1]) == 0

    # check that expected print prints
    captured = capsys.readouterr()
    assert ("Warning: No active service_ids were found within date range: "
            "['2016-01-01', '2016-02-01'].") in captured.out


def test_select_cal_dates_service_ids_w_day(
        calendar_dates_w_uni_svc_id, expected_result_cal_dates_param_day,
        day_param):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': day_param,
                'date': None,
                'date_range': None,
                'cal_dates_lookup': None,
                'has_cal': False,
                'has_day_param': True,
                'has_date_param': False,
                'has_date_range_param': False,
                'has_cal_dates': True,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that are
    # found on a 'monday' with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_day
    # exception_type = 2
    assert result[1] == []


def test_select_cal_dates_service_ids_w_date(
        calendar_dates_w_uni_svc_id, expected_result_cal_dates_param_date,
        date_param_cal_dates):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': None,
                'date': date_param_cal_dates,
                'date_range': None,
                'cal_dates_lookup': None,
                'has_cal': False,
                'has_day_param': False,
                'has_date_param': True,
                'has_date_range_param': False,
                'has_cal_dates': True,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that are
    # found on '2016-08-02' with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_date
    # exception_type = 2
    assert result[1] == []


def test_select_cal_dates_service_ids_w_date_range(
        calendar_dates_w_uni_svc_id, daterange_param,
        expected_result_cal_dates_param_daterange):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': None,
                'date': None,
                'date_range': daterange_param,
                'cal_dates_lookup': None,
                'has_cal': False,
                'has_day_param': False,
                'has_date_param': False,
                'has_date_range_param': True,
                'has_cal_dates': True,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': False})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that are
    # between ['2016-06-01', '2016-08-31'] with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_daterange
    # exception_type = 2
    assert sorted(result[1]) == sorted(['summer-weekday-1_agency_a_city_a',
                                        'summer-weekday-3_agency_a_city_a'])


def test_select_cal_dates_service_ids_w_day_and_date_range(
        calendar_dates_w_uni_svc_id, day_param, daterange_param):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': day_param,
                'date': None,
                'date_range': daterange_param,
                'cal_dates_lookup': None,
                'has_cal': False,
                'has_day_param': True,
                'has_date_param': False,
                'has_date_range_param': True,
                'has_cal_dates': True,
                'has_cal_dates_param': False,
                'has_day_and_date_range_param': True})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that are
    # between ['2016-06-01', '2016-08-31'] and are on a 'monday'
    # with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == ['summer-weekend-1_agency_a_city_a']
    # exception_type = 2
    assert result[1] == []


def test_select_cal_dates_service_ids_w_cal_dates_lookup_add_existing(
        calendar_dates_w_uni_svc_id, date_param_cal_dates,
        cal_dates_lookup_param,
        expected_result_cal_dates_param_lookup,
        expected_result_cal_dates_param_date):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': None,
                'date': date_param_cal_dates,
                'date_range': None,
                'cal_dates_lookup': cal_dates_lookup_param,
                'has_cal': False,
                'has_day_param': False,
                'has_date_param': True,
                'has_date_range_param': False,
                'has_cal_dates': True,
                'has_cal_dates_param': True,
                'has_day_and_date_range_param': False})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that match
    # the query with exception_type = 1 and 2 and also service IDs that have
    # exception_type = 1 on date '2016-08-02'

    # add the two exception_type = 1 lists together to get expected result
    expected_result_cal_dates_param_lookup[0].extend(
        expected_result_cal_dates_param_date)

    # exception_type = 1
    assert sorted(result[0]) == sorted(
        expected_result_cal_dates_param_lookup[0])
    # exception_type = 2
    assert sorted(result[1]) == expected_result_cal_dates_param_lookup[1]


def test_select_cal_dates_service_ids_w_cal_dates_lookup_only(
        calendar_dates_w_uni_svc_id, cal_dates_lookup_param,
        expected_result_cal_dates_param_lookup):
    result = gtfs_utils_cal._select_calendar_dates_service_ids(
        calendar_dates_w_uni_svc_id,
        params={'day': None,
                'date': None,
                'date_range': None,
                'cal_dates_lookup': cal_dates_lookup_param,
                'has_cal': False,
                'has_day_param': False,
                'has_date_param': False,
                'has_date_range_param': False,
                'has_cal_dates': True,
                'has_cal_dates_param': True,
                'has_day_and_date_range_param': False})

    assert isinstance(result, tuple)
    assert isinstance(result[0], list)
    assert isinstance(result[1], list)
    # result should only include service IDs in calendar dates that match
    # the query with exception_type = 1 and 2

    # exception_type = 1
    assert sorted(result[0]) == expected_result_cal_dates_param_lookup[0]
    # exception_type = 2
    assert sorted(result[1]) == expected_result_cal_dates_param_lookup[1]


def test_trip_selector_1_agency(trips_agency_a):
    selected_unique_srv_ids = ['summer-weekday-1_agency_a_city_a',
                               'special-game-day-1_agency_a_city_a']
    result = gtfs_utils_cal._trip_selector(
        trips_agency_a, service_ids=selected_unique_srv_ids, verbose=True)

    # build expected result
    expected_result = trips_agency_a.loc[
        trips_agency_a['unique_service_id'].isin(selected_unique_srv_ids)]
    expected_result.drop(columns=['unique_service_id'], inplace=True)
    expected_result.reset_index(drop=True, inplace=True)

    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.equals(expected_result)


def test_trip_selector_multi_agency_w_trips(trips_agency_a, trips_agency_b):
    selected_unique_srv_ids = ['summer-weekday-1_agency_a_city_a',
                               'special-game-day-1_agency_a_city_a',
                               'fall-weekday-1_agency_b_city_a']
    trips_multi_agency = pd.concat([trips_agency_a, trips_agency_b],
                                   ignore_index=True)
    result = gtfs_utils_cal._trip_selector(
        trips_multi_agency, service_ids=selected_unique_srv_ids, verbose=True)

    # build expected result
    expected_result = trips_multi_agency.loc[
        trips_multi_agency['unique_service_id'].isin(selected_unique_srv_ids)]
    expected_result.drop(columns=['unique_service_id'], inplace=True)

    assert isinstance(result, pd.core.frame.DataFrame)

    # ensure dfs are equal
    result.sort_values(by=['service_id'], inplace=True)
    result.reset_index(drop=True, inplace=True)
    expected_result.sort_values(by=['service_id'], inplace=True)
    expected_result.reset_index(drop=True, inplace=True)

    assert result.equals(expected_result)


def test_trip_selector_multi_agency_wo_trips(trips_agency_a, trips_agency_b):
    selected_unique_srv_ids = ['summer-weekday-1_agency_a_city_a',
                               'special-game-day-1_agency_a_city_a']
    trips_multi_agency = pd.concat(
        [trips_agency_a, trips_agency_b], ignore_index=True)
    result = gtfs_utils_cal._trip_selector(
        trips_multi_agency, service_ids=selected_unique_srv_ids, verbose=True)

    # build expected result
    expected_result = trips_multi_agency.loc[
        trips_multi_agency['unique_service_id'].isin(selected_unique_srv_ids)]
    expected_result.drop(columns=['unique_service_id'], inplace=True)

    assert isinstance(result, pd.core.frame.DataFrame)

    # ensure dfs are equal
    result.sort_values(by=['service_id'], inplace=True)
    result.reset_index(drop=True, inplace=True)
    expected_result.sort_values(by=['service_id'], inplace=True)
    expected_result.reset_index(drop=True, inplace=True)

    assert result.equals(expected_result)


def test_trip_selector_no_trips(trips_agency_a):
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_utils_cal._trip_selector(
            trips_agency_a, service_ids=['non-existent-id'], verbose=True)
    expected_error = (
        "No trips were found that matched the active service_ids "
        "identified by the specified calendar parameters.")
    assert expected_error in str(excinfo.value)


def test_trip_selector_no_trips_for_1_agency(
        capsys, trips_agency_a, trips_agency_b):
    trips_multi_agency = pd.concat(
        [trips_agency_a, trips_agency_b], ignore_index=True)
    result = gtfs_utils_cal._trip_selector(
        trips_multi_agency,
        service_ids=['summer-weekday-1_agency_a_city_a'],
        verbose=True)

    assert isinstance(result, pd.core.frame.DataFrame)
    assert len(result) == 3

    # check that expected print prints
    captured = capsys.readouterr()
    assert (
               "Warning: For agency: agency_b_city_a, no trips were found "
               "that "
               "matched the active service_ids identified by the specified "
               "calendar "
               "parameters.") in captured.out


def test_highest_freq_trips_date_w_cal_and_cal_dates_1_date(
        trips_agency_a, calendar_agency_a, calendar_dates_agency_a):
    result = gtfs_utils_cal._highest_freq_trips_date(
        trips_agency_a, calendar_agency_a, calendar_dates_agency_a)
    assert isinstance(result, str)
    assert result == '2016-08-02'


def test_highest_freq_trips_date_w_cal_dates_only_1_date(
        trips_agency_a, calendar_empty, calendar_dates_agency_a):
    result = gtfs_utils_cal._highest_freq_trips_date(
        trips_agency_a, calendar_empty, calendar_dates_agency_a)
    assert isinstance(result, str)
    assert result == '2016-08-02'


def test_highest_freq_trips_date_w_cal_only_multi_date_same_srv_ids(
        trips_agency_a, calendar_agency_a, calendar_dates_empty):
    result = gtfs_utils_cal._highest_freq_trips_date(
        trips_agency_a, calendar_agency_a, calendar_dates_empty)
    assert isinstance(result, str)
    assert result == '2016-08-03'


def test_highest_freq_trips_date_multi_date_diff_srv_ids_raiseerror(
        trips_and_cal_with_multi_freq_dates, calendar_empty):
    trips_df, cal_dates_df = trips_and_cal_with_multi_freq_dates
    with pytest.raises(ValueError) as excinfo:
        result = gtfs_utils_cal._highest_freq_trips_date(
            trips_df, calendar_empty, cal_dates_df)
    expected_error = ("The following dates: ['2016-08-02', '2016-12-02'] "
                      "have different service ids representing the date with "
                      "the most frequent trips (6 active trips). Unable to "
                      "use the 'use_highest_freq_trips_date' parameter. "
                      "Pick one date from this list as the 'date' parameter "
                      "to proceed.")
    assert expected_error in str(excinfo.value)


def test_trip_sched_selector_valid_params_valid(
        calendar_dates_agency_a, calendar_dates_empty):
    params = {'day': 'monday',
              'date': None,
              'date_range': None,
              'cal_dates_lookup': None,
              'has_cal': True,
              'has_day_param': True,
              'has_date_param': False,
              'has_date_range_param': False,
              'has_cal_dates': False,
              'has_cal_dates_param': False,
              'has_day_and_date_range_param': False}
    gtfs_utils_cal._trip_schedule_selector_validate_params(
        calendar_dates_agency_a, params)
    params = {'day': None,
              'date': '2016-07-04',
              'date_range': None,
              'cal_dates_lookup': None,
              'has_cal': True,
              'has_day_param': False,
              'has_date_param': True,
              'has_date_range_param': False,
              'has_cal_dates': False,
              'has_cal_dates_param': False,
              'has_day_and_date_range_param': False}
    gtfs_utils_cal._trip_schedule_selector_validate_params(
        calendar_dates_agency_a, params)
    params = {'day': None,
              'date': None,
              'date_range': ['2016-07-04', '2016-08-04'],
              'cal_dates_lookup': None,
              'has_cal': True,
              'has_day_param': False,
              'has_date_param': False,
              'has_date_range_param': True,
              'has_cal_dates': False,
              'has_cal_dates_param': False,
              'has_day_and_date_range_param': False}
    gtfs_utils_cal._trip_schedule_selector_validate_params(
        calendar_dates_agency_a, params)
    params = {'day': None,
              'date': None,
              'date_range': None,
              'cal_dates_lookup':
                  {'schedule_type': 'holiday',
                   'service_id': ['fall-weekday-1', 'fall-weekday-2']},
              'has_cal': True,
              'has_day_param': False,
              'has_date_param': False,
              'has_date_range_param': False,
              'has_cal_dates': True,
              'has_cal_dates_param': True,
              'has_day_and_date_range_param': False}
    gtfs_utils_cal._trip_schedule_selector_validate_params(
        calendar_dates_agency_a, params)


def test_trip_sched_selector_valid_params_invalid(
        calendar_dates_agency_a, calendar_dates_empty):
    # test more than one param
    with pytest.raises(ValueError) as excinfo:
        params = {'day': 'monday',
                  'date': '2016-07-04',
                  'date_range': None,
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': True,
                  'has_date_param': True,
                  'has_date_range_param': False,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "Only one parameter: 'day', 'date', or 'date_range' can be used at "
        "a time or both 'day' and 'date_range' can be used.")
    assert expected_error in str(excinfo.value)

    # test day param
    with pytest.raises(ValueError) as excinfo:
        params = {'day': 13,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': True,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "Day must be a string."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': 'mondays',
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': True,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "Day: mondays is not a supported day. Must be one of lowercase "
        "strings: 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', "
        "'saturday', 'sunday'.")
    assert expected_error in str(excinfo.value)

    # test date param
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': 20160704,
                  'date_range': None,
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': False,
                  'has_date_param': True,
                  'has_date_range_param': False,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "Date must be a string."
    assert expected_error in str(excinfo.value)

    invalid_dates = ['201607-04', '07-04-2016']
    for d in invalid_dates:
        with pytest.raises(ValueError) as excinfo:
            params = {'day': None,
                      'date': d,
                      'date_range': None,
                      'cal_dates_lookup': None,
                      'has_cal': True,
                      'has_day_param': False,
                      'has_date_param': True,
                      'has_date_range_param': False,
                      'has_cal_dates': False,
                      'has_cal_dates_param': False,
                      'has_day_and_date_range_param': False}
            gtfs_utils_cal._trip_schedule_selector_validate_params(
                calendar_dates_agency_a, params)
        expected_error = ("Date: {} is not a supported date format. "
                          "Expected format: 'YYYY-MM-DD'.".format(d))
        assert expected_error in str(excinfo.value)

    # test date_range
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': ['2016-07-04'],
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': True,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "Date range ['2016-07-04'] must have a length of 2."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': [20160704, 20160804],
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': True,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "Dates in date range must be a string."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': ['201-07-04', '07-04-2016'],
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': True,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "Date: 201-07-04 in date range: ['201-07-04', '07-04-2016'] "
        "is not a supported date format. Expected format: 'YYYY-MM-DD'.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': ['2016-10-04', '2016-06-04'],
                  'cal_dates_lookup': None,
                  'has_cal': True,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': True,
                  'has_cal_dates': False,
                  'has_cal_dates_param': False,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "First date in date range ['2016-10-04', '2016-06-04'] must be "
        "less than the last date.")
    assert expected_error in str(excinfo.value)
    # test date lookup param
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup': 'service_type',
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "calendar_dates_lookup parameter must be a dictionary."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'schedule_type': 'holiday',
                       'service_id': {'fall-weekday-1': True}},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "calendar_dates_lookup value: {'fall-weekday-1': True} must be a "
        "string or a list of strings.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'schedule_type': 'holiday',
                       12: 'weekend'},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = "calendar_dates_lookup key: 12 must be a string."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'schedule_type': 'holiday',
                       'weekend': [11, 'weekend']},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "calendar_dates_lookup value: [11, 'weekend'] must contain strings.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'schedule_type': 'holiday',
                       'schedule_restriction': ['True']},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_agency_a, params)
    expected_error = (
        "Column: schedule_restriction not found in calendar_dates DataFrame.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        df = calendar_dates_agency_a.copy()
        df['exception_type'] = df['exception_type'].astype(int)
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'exception_type': '1'},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': True,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            df, params)
    expected_error = ("Calendar_dates column: exception_type must be object "
                      "type.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        params = {'day': None,
                  'date': None,
                  'date_range': None,
                  'cal_dates_lookup':
                      {'schedule_type': 'holiday'},
                  'has_cal': False,
                  'has_day_param': False,
                  'has_date_param': False,
                  'has_date_range_param': False,
                  'has_cal_dates': False,
                  'has_cal_dates_param': True,
                  'has_day_and_date_range_param': False}
        gtfs_utils_cal._trip_schedule_selector_validate_params(
            calendar_dates_empty, params)
    expected_error = ("Calendar_dates is empty. Unable to use the "
                      "'calendar_dates_lookup' parameter. Set to None.")
    assert expected_error in str(excinfo.value)
