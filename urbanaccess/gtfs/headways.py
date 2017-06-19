import warnings
import pandas as pd
import time
import logging as lg

from urbanaccess.utils import log
from urbanaccess.gtfs.network import _time_selector

warnings.simplefilter(action="ignore", category=FutureWarning)


def _calc_headways_by_route_stop(df):
    """
    Calculate headways by route stop

    Parameters
    ----------
    df : pandas.DataFrame
        interpolated stop times dataframe for stop times within the time
        range with appended trip and route information

    Returns
    -------
    dataframe : pandas.DataFrame
        dataframe of statistics of route stop headways in units of minutes
    """

    # TODO: Optimize for speed

    start_time = time.time()

    df['unique_stop_route'] = (
        df['unique_stop_id'].str.cat(
            df['unique_route_id'].astype('str'), sep=','))

    stop_route_groups = df.groupby('unique_stop_route')
    log('Starting route stop headway calculation for {:,} route '
        'stops...'.format(len(stop_route_groups)))

    results = {}

    # suppress RuntimeWarning: Mean of empty slice. for this code block
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category='RuntimeWarning')

        for unique_stop_route, stop_route_group in stop_route_groups:
            stop_route_group.sort_values(['departure_time_sec_interpolate'],
                                         ascending=True, inplace=True)
            next_bus_time = (stop_route_group['departure_time_sec_interpolate']
                             .iloc[1:].values)
            prev_bus_time = (stop_route_group['departure_time_sec_interpolate']
                             .iloc[:-1].values)
            stop_route_group_headways = (next_bus_time - prev_bus_time) / 60
            results[unique_stop_route] = (pd.Series(stop_route_group_headways)
                                          .describe())

    log('Route stop headway calculation complete. Took {:,.2f} seconds'.format(
        time.time() - start_time))

    return pd.DataFrame(results).T


def _headway_handler(interpolated_stop_times_df, trips_df,
                     routes_df, headway_timerange):
    """
    route stop headway calculator handler

    Parameters
    ----------
    interpolated_stop_times_df : pandas.DataFrame
        interpolated stop times dataframe for stop times within the time range
    trips_df : pandas.DataFrame
        trips dataframe
    routes_df : pandas.DataFrame
        routes dataframe
    headway_timerange : list
        time range for which to calculate headways between as a
        list of time 1 and time 2 where times are 24 hour clock strings
        such as:
        ['07:00:00', '10:00:00']

    Returns
    -------
    headway_by_routestop_df : pandas.DataFrame
        dataframe of statistics of route stop headways in units of minutes
        with relevant route and stop information
    """
    start_time = time.time()

    # add unique trip and route id
    trips_df['unique_trip_id'] = (
        trips_df['trip_id'].str.cat(
            trips_df['unique_agency_id'].astype('str'), sep='_'))
    trips_df['unique_route_id'] = (
        trips_df['route_id'].str.cat(
            trips_df['unique_agency_id'].astype('str'), sep='_'))

    columns = ['unique_route_id', 'service_id', 'unique_trip_id',
               'unique_agency_id']
    # if these optional cols exist then keep those that do
    optional_cols = ['direction_id', 'shape_id']
    for item in optional_cols:
        if item in trips_df.columns:
            columns.append(item)

    trips_df = trips_df[columns]

    # add unique route id
    routes_df['unique_route_id'] = (
        routes_df['route_id'].str.cat(
            routes_df['unique_agency_id'].astype('str'), sep='_'))

    columns = ['unique_route_id', 'route_long_name', 'route_type',
               'unique_agency_id']
    routes_df = routes_df[columns]

    selected_interpolated_stop_times_df = _time_selector(
        df=interpolated_stop_times_df, starttime=headway_timerange[0],
        endtime=headway_timerange[1])

    tmp1 = pd.merge(trips_df, routes_df, how='left', left_on='unique_route_id',
                    right_on='unique_route_id', sort=False)
    merge_df = pd.merge(selected_interpolated_stop_times_df, tmp1, how='left',
                        left_on='unique_trip_id', right_on='unique_trip_id',
                        sort=False)
    cols_to_drop = ['unique_agency_id_y', 'unique_agency_id_x']
    merge_df.drop(cols_to_drop, axis=1, inplace=True)

    headway_by_routestop_df = _calc_headways_by_route_stop(df=merge_df)

    # add unique route stop node_id
    headway_by_routestop_df = pd.merge(
        headway_by_routestop_df,
        merge_df[['unique_stop_route', 'unique_stop_id', 'unique_route_id']],
        how='left', left_index=True, right_on='unique_stop_route', sort=False)
    headway_by_routestop_df.drop('unique_stop_route', axis=1, inplace=True)
    headway_by_routestop_df['node_id_route'] = (
        headway_by_routestop_df['unique_stop_id'].str.cat(
            headway_by_routestop_df['unique_route_id'].astype('str'), sep='_'))

    log('headway calculation complete. Took {:,.2f} seconds'.format(
        time.time() - start_time))

    return headway_by_routestop_df


def headways(gtfsfeeds_df, headway_timerange):
    """
    Calculate headways by route stop for a specific time range

    Parameters
    ----------
    gtfsfeeds_df : object
        gtfsfeeds_dfs object with all processed GTFS data tables
    headway_timerange : list
        time range for which to calculate headways between as a list of
        time 1 and time 2 where times are 24 hour clock strings such as:
        ['07:00:00', '10:00:00']

    Returns
    -------
    gtfsfeeds_dfs.headways : pandas.DataFrame
        gtfsfeeds_dfs object for the headways dataframe with statistics of
        route stop headways in units of minutes
        with relevant route and stop information
    """

    time_error_statement = (
        '{} starttime and endtime are not in the correct format. '
        'Format should be a 24 hour clock in following format: 08:00:00 '
        'or 17:00:00'.format(headway_timerange))
    if not isinstance(headway_timerange, list) or len(headway_timerange) != 2:
        raise ValueError('timerange must be a list of length 2')
    if headway_timerange[0].split(':')[0] > headway_timerange[1].split(':')[0]:
        raise ValueError('starttime is greater than endtime')

    for t in headway_timerange:
        if not isinstance(t, str):
            raise ValueError(time_error_statement)
        if len(t) != 8:
            raise ValueError(time_error_statement)
    if int(headway_timerange[1].split(':')[0]) - int(
            headway_timerange[0].split(':')[0]) > 3:
        long_time_range_msg = (
            'WARNING: Time range passed: {} is a {} hour period. Long periods '
            'over 3 hours may take a significant amount of time to process.')
        log(long_time_range_msg.format(headway_timerange,
                                       int(str(
                                           headway_timerange[1][0:2])) - int(
                                           str(headway_timerange[0][0:2]))),
            level=lg.WARNING)

    if gtfsfeeds_df is None:
        raise ValueError('gtfsfeeds_df cannot be None')
    if gtfsfeeds_df.stop_times_int.empty or gtfsfeeds_df.trips.empty or \
            gtfsfeeds_df.routes.empty:
        raise ValueError(
            'one of the gtfsfeeds_dfs objects: stop_times_int, trips, '
            'or routes were found to be empty.')

    headways_df = _headway_handler(
        interpolated_stop_times_df=gtfsfeeds_df.stop_times_int,
        trips_df=gtfsfeeds_df.trips,
        routes_df=gtfsfeeds_df.routes,
        headway_timerange=headway_timerange)

    gtfsfeeds_df.headways = headways_df

    return gtfsfeeds_df
