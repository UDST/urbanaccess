import warnings

import pandas as pd

from urbanaccess.gtfs.utils.gtfs_format import (_time_selector)
from urbanaccess.utils import _join_camel

# Note: The above imported logging funcs were modified from the OSMnx library
#       & used with permission from the author Geoff Boeing: log, get_logger
#       OSMnx repo: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

warnings.simplefilter(action = "ignore", category = FutureWarning)


def headways(gtfsfeeds_df, headway_timerange):
    """
    Calculate headways by route stop for a specific time range

    Parameters
    ----------
    gtfsfeeds_df : object
        gtfsfeeds_dfs object with all processed GTFS data tables
    headway_timerange : list
        time range for which to calculate headways between as a list of
        time 1 and time 2
        where times are 24 hour clock strings such as:
        ['07:00:00','10:00:00']

    Returns
    -------
    gtfsfeeds_dfs.headways : pandas.DataFrame
        gtfsfeeds_dfs object for the headways dataframe with statistics of
        route stop headways in units of minutes
        with relevant route and stop information
    """

    # This now becomes a wrapper around the new calc_period_headways func
    gtfs_dict = {
        'stops': gtfsfeeds_df.stops,
        'routes': gtfsfeeds_df.routes,
        'trips': gtfsfeeds_df.trips,
        'stop_times': gtfsfeeds_df.stop_times,
        'calendar': gtfsfeeds_df.calendar,
        'calendar_dates': gtfsfeeds_df.calendar_dates,
        'stop_times_int': gtfsfeeds_df.stop_times_int,
        'headways': gtfsfeeds_df.headways
    }
    
    res = calc_period_headways(gtfs_dict, headway_timerange)

    # convert back out to class format
    gtfsfeeds_df.stops = res['stops']
    gtfsfeeds_df.routes = res['routes']
    gtfsfeeds_df.trips = res['trips']
    gtfsfeeds_df.stop_times = res['stop_times']
    gtfsfeeds_df.calendar = res['calendar']
    gtfsfeeds_df.calendar_dates = res['calendar_dates']
    gtfsfeeds_df.stop_times_int = res['stop_times_int']
    gtfsfeeds_df.headways = res['headways']

    return gtfsfeeds_df


def calc_period_headways(gtfsfeeds_df, headway_timerange):
    trips_df = gtfsfeeds_df['trips']
    routes_df = gtfsfeeds_df['routes']

    # add unique trip and route id
    trip_uid_sub = trips_df[['trip_id', 'unique_agency_id']]
    trip_uids = trip_uid_sub.apply(lambda x: _join_camel(x), axis=1)
    trips_df['unique_trip_id'] = trip_uids

    trip_rte_uid_sub = trips_df[['route_id', 'unique_agency_id']]
    route_uids = trip_rte_uid_sub.apply(lambda x: _join_camel(x), axis=1)
    trips_df['unique_route_id'] = route_uids

    columns = ['unique_route_id',
               'service_id',
               'unique_trip_id',
               'unique_agency_id']

    # if these optional cols exist then keep those that do
    optional_cols = ['direction_id', 'shape_id']
    for item in optional_cols:
        if item in trips_df.columns:
            columns.append(item)

    trips_df = trips_df[columns]

    # add unique route id
    rte_uid = routes_df[['route_id', 'unique_agency_id']]
    routes_df['unique_route_id'] = rte_uid.apply(lambda x: _join_camel(x),
                                                 axis=1)

    columns = ['unique_route_id',
               'route_long_name',
               'route_type',
               'unique_agency_id']
    routes_df = routes_df[columns]

    st_times_interp = gtfsfeeds_df['stop_times_int']
    hdwy_start = headway_timerange[0]
    hdwy_end = headway_timerange[1]
    selected_interpolated_stop_times_df = _time_selector(st_times_interp,
                                                         hdwy_start,
                                                         hdwy_end)

    trips_and_routes = pd.merge(trips_df,
                                routes_df,
                                how='left',
                                left_on='unique_route_id',
                                right_on='unique_route_id',
                                sort=False)

    all_merged = pd.merge(selected_interpolated_stop_times_df,
                          trips_and_routes,
                          how='left',
                          left_on='unique_trip_id',
                          right_on='unique_trip_id',
                          sort=False)

    cols_to_drop = ['unique_agency_id_y', 'unique_agency_id_x']
    all_merged.drop(cols_to_drop, axis=1, inplace=True)

    merged_sub = all_merged[['unique_stop_id', 'unique_route_id']]
    new_uid = merged_sub.apply(lambda x: ', '.join([x[0], x[1]]), axis=1)
    all_merged['unique_stop_route'] = new_uid

    # then group by the new uid
    stop_route_groups = all_merged.groupby('unique_stop_route')

    # this is a long name, let's store as a var
    dep_time_name = 'departure_time_sec_interpolate'

    results = {}
    for unique_stop_route, stop_route_group in stop_route_groups:
        stop_route_group.sort_values([dep_time_name],
                                     ascending=True,
                                     inplace=True)

        # get times for upcoming and preceding
        next_bus_time = stop_route_group[dep_time_name].iloc[1:].values
        prev_bus_time = stop_route_group[dep_time_name].iloc[:-1].values

        # create a stop-route-group-headways var
        adjusted_time_diff = ((next_bus_time - prev_bus_time) / 60)
        results[unique_stop_route] = pd.Series(adjusted_time_diff).describe()

    # transport result to a headway dataframe
    headway_by_routestop_df = pd.DataFrame(results).T

    # add unique route stop node_id
    want_merge_cols = ['unique_stop_route',
                       'unique_stop_id',
                       'unique_route_id']
    merge_keep_df = all_merged[want_merge_cols]

    headway_by_routestop_df = pd.merge(headway_by_routestop_df,
                                       merge_keep_df,
                                       how='left',
                                       left_index=True,
                                       right_on='unique_stop_route',
                                       sort=False)

    # no longer need the join column name, can drop
    headway_by_routestop_df.drop('unique_stop_route', axis=1, inplace=True)

    # generate the new unique id
    hdwy_sub = headway_by_routestop_df[['unique_stop_id', 'unique_route_id']]
    node_rte_id = hdwy_sub.apply(lambda x: _join_camel(x), axis=1)
    headway_by_routestop_df['node_id_route'] = node_rte_id

    # update the overall gtfs' headways dataframe
    gtfsfeeds_df['headways'] = headway_by_routestop_df

    return gtfsfeeds_df
