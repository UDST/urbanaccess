import pytest
import pandas as pd

import urbanaccess.gtfs.headways as headways
from urbanaccess.gtfs.gtfsfeeds_dataframe import urbanaccess_gtfs_df \
    as gtfsfeeds_df


def _build_expected_hw_intermediate_stop_times_data(
        trips_df, routes_df, stop_times_df):
    trips_df['unique_trip_id'] = trips_df['trip_id'].str.cat(
        trips_df['unique_agency_id'].astype('str'), sep='_')
    trips_df['unique_route_id'] = trips_df['route_id'].str.cat(
        trips_df['unique_agency_id'].astype('str'), sep='_')

    columns = ['unique_route_id', 'service_id', 'unique_trip_id',
               'unique_agency_id', 'direction_id']
    trips_df = trips_df[columns]

    routes_df['unique_route_id'] = routes_df['route_id'].str.cat(
        routes_df['unique_agency_id'].astype('str'), sep='_')
    columns = ['unique_route_id', 'route_long_name', 'route_type',
               'unique_agency_id']
    routes_df = routes_df[columns]

    tmp1 = pd.merge(trips_df, routes_df, how='left', left_on='unique_route_id',
                    right_on='unique_route_id', sort=False)
    merge_df = pd.merge(stop_times_df, tmp1, how='left',
                        left_on='unique_trip_id', right_on='unique_trip_id',
                        sort=False)
    cols_to_drop = ['unique_agency_id_y', 'unique_agency_id_x']
    merge_df.drop(cols_to_drop, axis=1, inplace=True)
    return merge_df


def _check_headway_result(
        result, hw_stop_times_int_df_1_agency, expected_route_stop_ids,
        expected_route_stop_headway_result_sample_1,
        expected_route_stop_headway_result_sample_2):
    expected_cols = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max',
                     'unique_stop_id', 'unique_route_id', 'node_id_route']
    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.empty is False
    # each stop_time has a corresponding headway so len should be the same
    # but this can be simplified in the future to return only unique data
    assert len(result) == len(hw_stop_times_int_df_1_agency)
    for col in expected_cols:
        assert col in result.columns
    assert sorted(result['node_id_route'].unique()) == expected_route_stop_ids

    # check subset of values
    subset = result.loc[result['node_id_route'] ==
                        '1_agency_a_city_a_10-101_agency_a_city_a']
    subset.reset_index(inplace=True, drop=True)
    subset = subset.round(2)
    assert len(subset) == 12
    assert subset[0:1].equals(expected_route_stop_headway_result_sample_1)

    subset = result.loc[result['node_id_route'] ==
                        '7_agency_a_city_a_B1_agency_a_city_a']
    subset.reset_index(inplace=True, drop=True)
    subset = subset.round(2)
    assert len(subset) == 12
    assert subset[0:1].equals(expected_route_stop_headway_result_sample_2)


@pytest.fixture
def ua_gtfsfeeds_df(
        hw_routes_df_1_agency, hw_trips_df_1_agency,
        hw_stop_times_int_df_1_agency):
    gtfsfeeds_df.routes = hw_routes_df_1_agency.copy()
    gtfsfeeds_df.trips = hw_trips_df_1_agency.copy()
    gtfsfeeds_df.stop_times_int = hw_stop_times_int_df_1_agency.copy()
    return gtfsfeeds_df


@pytest.fixture
def interm_stop_times_df_1_agency(
        hw_trips_df_1_agency, hw_routes_df_1_agency,
        hw_stop_times_int_df_1_agency):
    int_stop_times = _build_expected_hw_intermediate_stop_times_data(
        trips_df=hw_trips_df_1_agency,
        routes_df=hw_routes_df_1_agency,
        stop_times_df=hw_stop_times_int_df_1_agency)
    return int_stop_times


@pytest.fixture
def expected_route_stop_headway_result_sample_1():
    data = {
        'count': [11.0],
        'mean': [1.818182],
        'std': [6.030227],
        'min': [0.0],
        '25%': [0.0], '50%': [0.0], '75%': [0.0],
        'max': [20.0],
        'unique_stop_id': ['1_agency_a_city_a'],
        'unique_route_id': ['10-101_agency_a_city_a'],
        'node_id_route': ['1_agency_a_city_a_10-101_agency_a_city_a']
    }
    index = range(1)
    expected_subset = pd.DataFrame(data, index)
    expected_subset = expected_subset.round(2)
    return expected_subset


@pytest.fixture
def expected_route_stop_headway_result_sample_2():
    data = {
        'count': [11.0],
        'mean': [2.73],
        'std': [3.75],
        'min': [0.0],
        '25%': [0.0], '50%': [0.0], '75%': [5.0],
        'max': [10.0],
        'unique_stop_id': ['7_agency_a_city_a'],
        'unique_route_id': ['B1_agency_a_city_a'],
        'node_id_route': ['7_agency_a_city_a_B1_agency_a_city_a']
    }
    index = range(1)
    expected_subset = pd.DataFrame(data, index)
    expected_subset = expected_subset.round(2)
    return expected_subset


@pytest.fixture
def expected_route_stop_ids():
    expected_route_stop_ids = sorted([
        '1_agency_a_city_a_10-101_agency_a_city_a',
        '2_agency_a_city_a_10-101_agency_a_city_a',
        '3_agency_a_city_a_10-101_agency_a_city_a',
        '4_agency_a_city_a_10-101_agency_a_city_a',
        '5_agency_a_city_a_10-101_agency_a_city_a',
        '6_agency_a_city_a_10-101_agency_a_city_a',
        '7_agency_a_city_a_B1_agency_a_city_a',
        '8_agency_a_city_a_B1_agency_a_city_a',
        '9_agency_a_city_a_B1_agency_a_city_a'])
    return expected_route_stop_ids


def test_calc_headways_by_route_stop(
        interm_stop_times_df_1_agency,
        expected_route_stop_headway_result_sample_1,
        expected_route_stop_headway_result_sample_2,
        expected_route_stop_ids):
    result = headways._calc_headways_by_route_stop(
        df=interm_stop_times_df_1_agency)
    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.empty is False
    assert len(result) == 9

    expected_cols = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
    for col in expected_cols:
        assert col in result.columns
    # check route stop ids
    assert sorted(result.index.str.replace(',', '_').to_list()) == \
           expected_route_stop_ids

    # check subset of values
    subset = result.loc[
        result.index == '1_agency_a_city_a,10-101_agency_a_city_a']
    subset.reset_index(inplace=True, drop=True)
    subset = subset.round(2)
    assert len(subset) == 1
    assert subset.equals(
        expected_route_stop_headway_result_sample_1[expected_cols])

    subset = result.loc[result.index == '7_agency_a_city_a,B1_agency_a_city_a']
    subset.reset_index(inplace=True, drop=True)
    subset = subset.round(2)
    assert len(subset) == 1
    assert subset.equals(
        expected_route_stop_headway_result_sample_2[expected_cols])


def test_headway_handeler(
        hw_routes_df_1_agency, hw_stop_times_int_df_1_agency,
        hw_trips_df_1_agency,
        expected_route_stop_headway_result_sample_1,
        expected_route_stop_headway_result_sample_2,
        expected_route_stop_ids):
    result = headways._headway_handler(
        interpolated_stop_times_df=hw_stop_times_int_df_1_agency,
        trips_df=hw_trips_df_1_agency,
        routes_df=hw_routes_df_1_agency,
        headway_timerange=['06:00:00', '12:00:00'])

    _check_headway_result(result,
                          hw_stop_times_int_df_1_agency,
                          expected_route_stop_ids,
                          expected_route_stop_headway_result_sample_1,
                          expected_route_stop_headway_result_sample_2)


def test_headways(ua_gtfsfeeds_df,
                  hw_routes_df_1_agency, hw_stop_times_int_df_1_agency,
                  hw_trips_df_1_agency,
                  expected_route_stop_headway_result_sample_1,
                  expected_route_stop_headway_result_sample_2,
                  expected_route_stop_ids):
    ua_gtfsfeeds_df = headways.headways(
        gtfsfeeds_df=ua_gtfsfeeds_df,
        headway_timerange=['06:00:00', '12:00:00'])
    # add expected cols to tables
    hw_trips_df_1_agency['unique_trip_id'] = (
        hw_trips_df_1_agency['trip_id'].str.cat(
            hw_trips_df_1_agency['unique_agency_id'].astype('str'), sep='_'))
    hw_trips_df_1_agency['unique_route_id'] = (
        hw_trips_df_1_agency['route_id'].str.cat(
            hw_trips_df_1_agency['unique_agency_id'].astype('str'), sep='_'))
    hw_routes_df_1_agency['unique_route_id'] = (
        hw_routes_df_1_agency['route_id'].str.cat(
            hw_routes_df_1_agency['unique_agency_id'].astype('str'), sep='_'))

    assert ua_gtfsfeeds_df.trips.equals(hw_trips_df_1_agency)
    assert ua_gtfsfeeds_df.stop_times_int.equals(hw_stop_times_int_df_1_agency)
    assert ua_gtfsfeeds_df.routes.equals(hw_routes_df_1_agency)

    _check_headway_result(ua_gtfsfeeds_df.headways,
                          hw_stop_times_int_df_1_agency,
                          expected_route_stop_ids,
                          expected_route_stop_headway_result_sample_1,
                          expected_route_stop_headway_result_sample_2)


def test_headways_invalid(ua_gtfsfeeds_df):
    with pytest.raises(ValueError) as excinfo:
        ua_gtfsfeeds_df.trips = pd.DataFrame()
        ua_gtfsfeeds_df = headways.headways(
            gtfsfeeds_df=ua_gtfsfeeds_df,
            headway_timerange=['06:00:00', '12:00:00'])
    expected_error = (
        'One of the following gtfsfeeds_dfs objects: stop_times_int, '
        'trips, or routes were found to be empty.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        ua_gtfsfeeds_df.trips = pd.DataFrame()
        ua_gtfsfeeds_df = headways.headways(
            gtfsfeeds_df=None,
            headway_timerange=['06:00:00', '12:00:00'])
    expected_error = 'gtfsfeeds_df cannot be None.'
    assert expected_error in str(excinfo.value)


def test_add_unique_stop_route():
    data = {
        'unique_route_id': ['Route_A', 'Route_B'],
        'unique_stop_id': ['Stop_1', 'Stop_2'],
        'unique_stop_route': ['Stop_1,Route_A', 'Stop_2,Route_B']
    }
    index = range(2)
    df = pd.DataFrame(data, index)

    result = headways._add_unique_stop_route(
        df[['unique_route_id', 'unique_stop_id']])
    assert result.equals(df)


def test_add_node_id_route():
    data = {
        'unique_route_id': ['Route_A', 'Route_B'],
        'unique_stop_id': ['Stop_1', 'Stop_2'],
        'node_id_route': ['Stop_1_Route_A', 'Stop_2_Route_B']
    }
    index = range(2)
    df = pd.DataFrame(data, index)

    result = headways._add_node_id_route(
        df[['unique_route_id', 'unique_stop_id']])
    assert result.equals(df)
