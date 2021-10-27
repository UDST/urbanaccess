from __future__ import division
import os
import pandas as pd
import time
from datetime import datetime, timedelta
import logging as lg

from urbanaccess.utils import log, df_to_hdf5, hdf5_to_df
from urbanaccess.gtfs.utils_validation import _check_time_range_format
from urbanaccess.gtfs.utils_calendar import _calendar_service_id_selector, \
    _trip_selector, _highest_freq_trips_date
from urbanaccess.network import ua_network
from urbanaccess import config
from urbanaccess.gtfs.gtfsfeeds_dataframe import gtfsfeeds_dfs, \
    urbanaccess_gtfs_df

pd.options.mode.chained_assignment = None


def create_transit_net(
        gtfsfeeds_dfs,
        day=None,
        timerange=None,
        calendar_dates_lookup=None,
        overwrite_existing_stop_times_int=False,
        use_existing_stop_times_int=False,
        save_processed_gtfs=False,
        save_dir=config.settings.data_folder,
        save_filename=None,
        timerange_pad=None,
        time_aware=False,
        date=None,
        date_range=None,
        use_highest_freq_trips_date=False,
        simplify=False
):
    """
    Create a travel time weight network graph in units of
    minutes from GTFS data

    Parameters
    ----------
    gtfsfeeds_dfs : object
        urbanaccess_gtfs_df object with DataFrames of stops, routes, trips,
        stop_times, calendar, calendar_dates (optional) and
        stop_times_int (optional)
    day : {'monday', 'tuesday', 'wednesday', 'thursday',
    'friday', 'saturday', 'sunday'}, optional
        day of the week to extract active service IDs in calendar and or
        calendar_dates tables. If GTFS feeds have only calendar, service IDs
        that are active on day specified will be extracted. If GTFS feeds
        have only calendar_dates, service IDs that are active on dates
        that match the day of the week specified will be extracted where
        exception_type = 1 (service is added for date). If GTFS feeds have
        both calendar and calendar_dates, calendar service IDs that are
        active on day specified will be extracted and calendar_dates
        service IDs that are active on dates that match the day of the week
        specified will be added to those found in the calendar where
        exception_type = 1 (service is added for date).
    timerange : list
        time range to extract transit schedule from in a list with time
        1 and time 2 as strings. It is suggested the time range
        specified is large enough to allow for travel
        from one end of the transit network to the other but small enough
        to represent a relevant travel time period such as a 3 hour window
        for the AM Peak period. Must follow format
        of a 24 hour clock for example: 08:00:00 or 17:00:00
    calendar_dates_lookup : dict, optional
        dictionary of the lookup column (key) as a string and corresponding
        string (value) as string or list of strings to use to subset trips
        using the calendar_dates DataFrame. Parameter should only be used if
        there is a high degree of certainly that the GTFS feed in use has
        service_ids listed in calendar_dates that would otherwise not be
        selected if using any of the other calendar_dates parameters such as:
         'day', 'date' or 'date_range'. Search will be exact and will select
         all records that meet each key value pair criteria.
        Example: {'schedule_type' : 'WD'} or {'schedule_type' : ['WD', 'SU']}
    overwrite_existing_stop_times_int : bool, optional
        if true, and if there is an existing stop_times_int
        DataFrame stored in the gtfsfeeds_dfs object it will be
        overwritten
    use_existing_stop_times_int : bool, optional
        if true, and if there is an existing stop_times_int
        DataFrame for the same time period stored in the
        gtfsfeeds_dfs object it will be used instead of re-calculated
    save_processed_gtfs : bool, optional
        if true, all processed GTFS DataFrames will
        be stored to disk in a HDF5 file
    save_dir : str, optional
        directory to save the HDF5 file
    save_filename : str, optional
        name to save the HDF5 file as
    timerange_pad: str, optional
        string indicating the number of hours minutes seconds to pad after the
        end of the time interval specified in 'timerange'. Must follow format
        of a 24 hour clock for example: '02:00:00' for a two hour pad or
        '02:30:00' for a 2 hour and 30 minute pad.
    time_aware: bool, optional
        boolean to indicate whether the transit network should include
        time information. If True, 'arrival_time' and 'departure_time' columns
        from the stop_times table will be included in the transit edge table
        where 'departure_time' is the departure time at node_id_from stop and
        'arrival_time' is the arrival time at node_id_to stop
    date : str, optional
        date to extract active service IDs in calendar and or
        calendar_dates tables specified as a string in YYYY-MM-DD format,
        e.g. '2013-03-09'. If GTFS feeds have only calendar, service IDs
        that are active on the date specified (where the date is within the
        date range given by the start_date and end_date columns) and
        that date's day of the week will be extracted. If GTFS feeds have
        only calendar_dates, service IDs that are active on the date
        specified will be extracted where exception_type = 1
        (service is added for date). If GTFS feeds have both calendar
        and calendar_dates, calendar service IDs that are active on the date
        specified (where the date is within the date range given by the
        start_date and end_date columns) and that date's day of the week
        will be extracted and calendar_dates service IDs that are active
        on the date specified will be added to those found in the
        calendar where exception_type = 1 (service is added for date)
        and service IDs that are inactive on the date specified will be
        removed from those found in the calendar where exception_type = 2
        (service is removed for date).
    date_range : list, optional
        date range to extract active service IDs in calendar and or
        calendar_dates tables specified as a 2 element list of strings
        where the first element is the first date and the second element
        is the last date in the date range. Dates should be specified
        as strings in YYYY-MM-DD format, e.g. ['2013-03-09', '2013-09-01'].
        If GTFS feeds have only calendar, service IDs that are active
        within the date range specified (where the date range is within
        the date range given by the start_date and end_date columns)
        will be extracted. If GTFS feeds have only calendar_dates,
        service IDs that are active within the date range specified
        will be extracted where exception_type = 1
        (service is added for date). If GTFS feeds have both calendar
        and calendar_dates, calendar service IDs that are active within
        the date range specified (where the date range is within the date
        range given by the state_date and end_date columns) will be
        extracted and calendar_dates service IDs that are active within
        the date range specified will be added to those found in the
        calendar where exception_type = 1 (service is added for date).
        This parameter is best utilized if its known that the GTFS
        feed calendar table has seasonal service IDs.
    use_highest_freq_trips_date : boolean, optional
        when True, the date that contains the highest number of active trips
        will be determined and will be used to select active service IDs.
        See the docstring for the 'date' parameter to understand how the
        'date' is then used to select service IDs. If multiple dates are found
        to contain the highest number of trips, and they contain the same
        trip IDs, one date from those found will be used. If they do not
        contain the same trip IDs, an error will be raised to prompt you
        to explicitly set one of the dates found as the date in the 'date'
        parameter.
    simplify : boolean
        when True, the transit network is significantly reduced in size by
        collapsing trips that share identical characteristics including
        agency, travel time, stop sequence, and route into one unique
        representative trip with its corresponding edges. It is suggested to
        use simplification when using networks with network analysis tools
        such as Pandana for computational efficiency. Default is False.

    Returns
    -------
    ua_network : object
    ua_network.transit_edges : pandas.DataFrame
    ua_network.transit_nodes : pandas.DataFrame
    """
    start_time = time.time()

    _check_time_range_format(timerange)
    if not isinstance(gtfsfeeds_dfs, urbanaccess_gtfs_df):
        raise ValueError('gtfsfeeds_dfs must be an urbanaccess_gtfs_df '
                         'object.')
    error_msg = ('One of the following gtfsfeeds_dfs objects: {} were '
                 'found to be empty.')
    if gtfsfeeds_dfs.trips.empty or gtfsfeeds_dfs.stop_times.empty or \
            gtfsfeeds_dfs.stops.empty:
        error_msg_case_1 = 'trips, stops, or stop_times'
        raise ValueError(error_msg.format(error_msg_case_1))
    if gtfsfeeds_dfs.calendar.empty and gtfsfeeds_dfs.calendar_dates.empty:
        error_msg_case_2 = 'calendar or calendar_dates'
        raise ValueError(error_msg.format(error_msg_case_2))
    if not isinstance(overwrite_existing_stop_times_int, bool):
        raise ValueError('overwrite_existing_stop_times_int must be bool.')
    if not isinstance(use_existing_stop_times_int, bool):
        raise ValueError('use_existing_stop_times_int must be bool.')
    if not isinstance(save_processed_gtfs, bool):
        raise ValueError('save_processed_gtfs must be bool.')
    if timerange_pad and not isinstance(timerange_pad, str):
        raise ValueError('timerange_pad must be string.')
    if not isinstance(time_aware, bool):
        raise ValueError('time_aware must be bool.')
    if overwrite_existing_stop_times_int and use_existing_stop_times_int:
        raise ValueError('overwrite_existing_stop_times_int and '
                         'use_existing_stop_times_int cannot both be True.')
    if use_highest_freq_trips_date and any([day, date, date_range]):
        raise ValueError("Only one parameter: 'use_highest_freq_trips_date' "
                         "or one of 'day', 'date', or 'date_range' can be "
                         "used at a time or both 'day' and 'date_range' can "
                         "be used.")

    columns = ['route_id',
               'direction_id',
               'trip_id',
               'service_id',
               'unique_agency_id',
               'unique_feed_id']
    if 'direction_id' not in gtfsfeeds_dfs.trips.columns:
        columns.remove('direction_id')

    # since data can be composed of multiple feeds and agencies,
    # run calendar service ID operations on each individual agency to
    # be transparent on what service IDs are being extracted from
    # each agency
    active_service_ids = []
    agency_id_col = 'unique_agency_id'
    agency_ids = gtfsfeeds_dfs.trips[agency_id_col].unique()
    for agency in agency_ids:
        log('--------------------------------')
        log('Running active service ID selection operation for '
            'agency: {}'.format(agency))
        agency_trips = gtfsfeeds_dfs.trips.loc[
            gtfsfeeds_dfs.trips[agency_id_col] == agency]
        agency_calendar = gtfsfeeds_dfs.calendar.loc[
            gtfsfeeds_dfs.calendar[agency_id_col] == agency]
        agency_calendar_dates = gtfsfeeds_dfs.calendar_dates.loc[
            gtfsfeeds_dfs.calendar_dates[agency_id_col] == agency]

        if use_highest_freq_trips_date:
            date = _highest_freq_trips_date(
                agency_trips,
                agency_calendar,
                agency_calendar_dates)

        agency_active_service_ids = _calendar_service_id_selector(
            calendar_df=agency_calendar,
            calendar_dates_df=agency_calendar_dates,
            day=day,
            date=date,
            date_range=date_range,
            cal_dates_lookup=calendar_dates_lookup)
        # combine each agency's active service IDs
        active_service_ids.extend(agency_active_service_ids)

    # use aggregated active_service_ids to get all active trip IDs
    calendar_selected_trips_df = _trip_selector(
        trips_df=gtfsfeeds_dfs.trips[columns],
        service_ids=active_service_ids)

    # proceed to calc stop_times_int if stop_times_int is already empty, or
    # overwrite existing is True, or use existing is False
    existing_stop_times_int = gtfsfeeds_dfs.stop_times_int.empty
    if existing_stop_times_int or overwrite_existing_stop_times_int or \
            use_existing_stop_times_int is False:
        if overwrite_existing_stop_times_int:
            log('   Overwriting existing stop_times_int DataFrame...')
        gtfsfeeds_dfs.stop_times_int = _interpolate_stop_times(
            stop_times_df=gtfsfeeds_dfs.stop_times,
            calendar_selected_trips_df=calendar_selected_trips_df)

        gtfsfeeds_dfs.stop_times_int = _time_difference(
            stop_times_df=gtfsfeeds_dfs.stop_times_int)

        if save_processed_gtfs:
            save_processed_gtfs_data(gtfsfeeds_dfs=gtfsfeeds_dfs,
                                     dir=save_dir, filename=save_filename)

    if use_existing_stop_times_int:
        log('   Using existing stop_times_int DataFrame...')

    selected_interpolated_stop_times_df = _time_selector(
        df=gtfsfeeds_dfs.stop_times_int,
        starttime=timerange[0],
        endtime=timerange[1],
        timerange_pad=timerange_pad)

    final_edge_table = _format_transit_net_edge(
        stop_times_df=selected_interpolated_stop_times_df,
        time_aware=time_aware)

    transit_edges = _convert_imp_time_units(
        df=final_edge_table, time_col='weight', convert_to='minutes')

    final_selected_stops = _stops_in_edge_table_selector(
        input_stops_df=gtfsfeeds_dfs.stops,
        input_stop_times_df=selected_interpolated_stop_times_df)

    transit_nodes = _format_transit_net_nodes(df=final_selected_stops)

    transit_edges = _route_type_to_edge(
        transit_edge_df=transit_edges, stop_time_df=gtfsfeeds_dfs.stop_times)

    transit_edges = _route_id_to_edge(
        transit_edge_df=transit_edges, trips_df=gtfsfeeds_dfs.trips)

    transit_nodes = _remove_nodes_not_in_edges(
        nodes=transit_nodes, edges=transit_edges,
        from_id_col='node_id_from', to_id_col='node_id_to')

    if transit_edges.empty:
        msg = (
            "Resulting transit network edges have 0 record(s). Most likely "
            "the 'timerange' and or calendar selection parameters resulted in "
            "a network with no active trips within the schedule window "
            "specified. It is suggested to expand the 'timerange' and or "
            "alter the calendar selection parameters to enlarge the time "
            "window for selecting active trips.")
        raise ValueError(msg)

    if simplify:
        transit_edges, transit_nodes = _simplify_transit_net(
            transit_edges, transit_nodes)

    # assign node and edge net type
    transit_nodes['net_type'] = 'transit'
    transit_edges['net_type'] = 'transit'

    # set global ua_network edges and nodes
    ua_network.transit_edges = transit_edges
    ua_network.transit_nodes = transit_nodes

    log('Successfully created transit network. Took {:,.2f} seconds.'.format(
        time.time() - start_time))

    return ua_network


def _simplify_transit_net(edges, nodes):
    """
    Simplifies edge and node tables by removing trips from the edge table
    that have identical properties that result in duplicate edge attributes
    for use in routing engines such as Pandana. Edges are grouped by
    columns: 'node_id_from', 'node_id_to', 'weight', 'unique_agency_id',
    'sequence', 'route_type', and 'unique_route_id'. If multiple trip IDs are
    found to have the exact same attributes in those columns, only one
    representative trip ID and its corresponding edges will be preserved out
    of the group.

    Parameters
    ----------
    edges : pandas.DataFrame
        edges DataFrame to simplify
    nodes : pandas.DataFrame
        nodes DataFrame to simplify

    Returns
    -------
    simp_edges : pandas.DataFrame
        simplified edge DataFrame
    simp_nodes : pandas.DataFrame
        simplified node DataFrame
    """
    # TODO: add option for groups that are duplicates, place trip ids
    #  of the groups that are removed in a new column as list of
    #  strings for downstream debugging or for informative trip counts
    # TODO: can simplify even more by reducing along edge shared attr in
    #  addition to trips, must ensure that stop ids are connected
    start_time = time.time()

    log('Running transit network simplification...')

    if edges.empty:
        raise ValueError(
            'Unable to simplify transit network. Edges have 0 records to '
            'simplify.')

    # columns to use for group to group value comparison to identify
    # groups of similar values for simplification
    col_list = ['node_id_from', 'node_id_to', 'weight', 'unique_agency_id',
                'sequence', 'route_type', 'unique_route_id']

    # group records that have identical values for each col in col_list
    # tag each record in group as True if the group exists more than once
    id_col = 'unique_trip_id'
    edges_wdup = edges.groupby(id_col)[col_list].agg(tuple).sum(1).duplicated()

    # remove records where their group was duplicated
    simp_edges = edges.loc[edges[id_col].isin(edges_wdup[~edges_wdup].index)]

    # simplify nodes by removing nodes that do not exist in the simplified
    # edge table, this catches edges cases but normally there should never be
    # node ids in the node table that are not in the edge table
    simp_nodes = _remove_nodes_not_in_edges(
        nodes=nodes, edges=simp_edges,
        from_id_col='node_id_from', to_id_col='node_id_to')

    # calculate various data len and print statistics
    trip_remove_cnt = len(edges_wdup.loc[edges_wdup == True])  # noqa
    trip_org_tot_cnt = len(edges[id_col].unique())
    trip_proc_tot_cnt = len(simp_edges[id_col].unique())
    trip_remove_pct = (trip_remove_cnt / trip_org_tot_cnt) * 100
    edge_org_rec_tot_cnt = len(edges)
    edge_proc_rec_tot_cnt = len(simp_edges)
    edge_remove_cnt = edge_org_rec_tot_cnt - edge_proc_rec_tot_cnt
    edge_remove_pct = (edge_remove_cnt / edge_org_rec_tot_cnt) * 100
    msg = ('Transit edges have been simplified removing {:,} trip(s) '
           '({:.2f} percent) (reduced from {:,} to {:,} trip(s)) '
           'resulting in the removal of {:,} edge(s) ({:.2f} percent) '
           '(reduced from {:,} to {:,} edges(s)).')
    if edge_remove_cnt > 0:
        log(msg.format(trip_remove_cnt, trip_remove_pct,
                       trip_org_tot_cnt, trip_proc_tot_cnt,
                       edge_remove_cnt, edge_remove_pct,
                       edge_org_rec_tot_cnt, edge_proc_rec_tot_cnt))
    else:
        log('Transit edges cannot be further simplified.')

    node_org_tot_cnt = len(nodes)
    node_proc_tot_cnt = len(simp_nodes)
    # if nodes have a different count after simplification notify the user,
    # however this should never occur
    if node_org_tot_cnt != node_proc_tot_cnt:
        node_remove_cnt = node_org_tot_cnt - node_proc_tot_cnt
        node_remove_pct = (node_remove_cnt / node_org_tot_cnt) * 100
        msg = ('Transit nodes have been simplified removing {:,} nodes(s) '
               '({:.2f} percent) (reduced from {:,} to {:,} nodes(s)).')
        log(msg.format(node_remove_cnt, node_remove_pct, node_org_tot_cnt,
                       node_proc_tot_cnt))
    log('Transit edge simplification complete. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))
    return simp_edges, simp_nodes


def _interpolate_stop_times(stop_times_df, calendar_selected_trips_df):
    """
    Interpolate missing stop times using a linear
    interpolator between known stop times

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        stop times DataFrame
    calendar_selected_trips_df : pandas.DataFrame
        DataFrame of trips that run on specific day

    Returns
    -------
    final_stop_times_df : pandas.DataFrame

    """
    start_time = time.time()

    # create unique trip IDs
    df_list = [calendar_selected_trips_df, stop_times_df]

    for index, df in enumerate(df_list):
        df['unique_trip_id'] = (df['trip_id'].str.cat(
            df['unique_agency_id'].astype('str'), sep='_'))
        df_list[index] = df

    # sort stop times inplace based on first to last stop in
    # sequence -- required as the linear interpolator runs
    # from first value to last value
    if stop_times_df['stop_sequence'].isnull().sum() > 1:
        log('WARNING: There are {:,} stop_sequence records missing in the '
            'stop_times DataFrame. Please check these missing values. '
            'In order for interpolation to proceed correctly, all records '
            'must have a stop_sequence value.'.format(
             stop_times_df['stop_sequence'].isnull().sum()),
            level=lg.WARNING)

    stop_times_df.sort_values(by=['unique_trip_id', 'stop_sequence'],
                              inplace=True)
    # make list of unique trip IDs from the calendar_selected_trips_df
    uniquetriplist = calendar_selected_trips_df[
        'unique_trip_id'].unique().tolist()
    # select trip IDs that match the trips in the
    # calendar_selected_trips_df -- resulting df will be stop times
    # only for trips that run on the service day or dates of interest
    stop_times_df = stop_times_df[
        stop_times_df['unique_trip_id'].isin(uniquetriplist)]

    # if there were no records that match then do not proceed and throw error
    if len(stop_times_df) == 0:
        raise ValueError('No matching trip_ids where found. Suggest checking '
                         'for differences between trip_id values in '
                         'stop_times and trips GTFS files.')

    # count missing stop times
    missing_stop_times_count = stop_times_df[
        'departure_time_sec'].isnull().sum()

    # if there are stop times missing that need interpolation notify user
    if missing_stop_times_count > 0:

        log('Note: Processing may take a long time depending '
            'on the number of records. '
            'Total unique trips to assess: {:,}.'.format(
             len(stop_times_df['unique_trip_id'].unique())),
            level=lg.WARNING)
        log('Starting departure stop time interpolation...')
        log('Departure time records missing from trips following the '
            'specified schedule: {:,} ({:.2f} percent of {:,} total '
            'records.)'.format(
             missing_stop_times_count,
             (missing_stop_times_count / len(stop_times_df)) * 100,
             len(stop_times_df['departure_time_sec'])))

        log('Interpolating...')

    else:
        log('There are no departure time records missing from trips '
            'following the specified schedule. There are no records to '
            'interpolate.')

    # TODO: for the rare and unlikely case when there is 1 null record and
    #  its not the first or last stop in the stop sequence, that value
    #  should be interpolated and its trip ID should be added to those to be
    #  interpolated - this additional case would have to be benchmarked
    #  for speed to ensure it doesnt slow down existing process
    # Find trips with more than one missing time
    # Note: all trip IDs have at least 1 null departure time because the
    # last stop in a trip is always null
    null_times = stop_times_df[stop_times_df.departure_time_sec.isnull()]
    trips_with_null = null_times.unique_trip_id.value_counts()
    trips_with_more_than_one_null = trips_with_null[
        trips_with_null > 1].index.values

    # Subset stop times DataFrame to only those with >1 null time
    df_for_interpolation = stop_times_df.loc[
        stop_times_df.unique_trip_id.isin(trips_with_more_than_one_null)]

    if len(df_for_interpolation) > 0:
        # check for duplicate stop_sequence and unique_trip_id combination,
        # if dups are found this will throw an error during the pivot()
        # operation so catch and return to user instead
        dup_df = df_for_interpolation[df_for_interpolation.duplicated(
            subset=['stop_sequence', 'unique_trip_id'], keep='first')]
        if len(dup_df) != 0:
            dup_values = list(dup_df['unique_trip_id'].unique())
            raise ValueError('Found duplicate values when values from '
                             'stop_sequence and unique_trip_id are combined. '
                             'Check values in these columns for '
                             'trip_id(s): {}.'.format(dup_values))

        # Pivot to DataFrame where each unique trip has its own column
        # Index is stop_sequence
        pivot = df_for_interpolation.pivot(
            index='stop_sequence', columns='unique_trip_id',
            values='departure_time_sec')

        # Interpolate on the whole DataFrame at once
        interpolator = pivot.interpolate(
            method='linear', axis=0, limit_direction='forward')

        # Melt back into stacked format
        interpolator['stop_sequence_merge'] = interpolator.index
        melted = pd.melt(interpolator, id_vars='stop_sequence_merge')
        melted.rename(columns={'value': 'departure_time_sec_interpolate'},
                      inplace=True)

        # Get the last valid stop for each unique trip,
        # to filter out trailing NaNs
        last_valid_stop_series = pivot.apply(
            lambda col: col.last_valid_index(), axis=0)
        last_valid_stop_df = last_valid_stop_series.to_frame('last_valid_stop')

        df_for_interpolation = (
            df_for_interpolation.merge(
                last_valid_stop_df, left_on='unique_trip_id',
                right_index=True))
        trailing = (df_for_interpolation.stop_sequence >
                    df_for_interpolation.last_valid_stop)

        # Calculate a stop_sequence without trailing NaNs, to merge the correct
        # interpolated times back in
        df_for_interpolation['stop_sequence_merge'] = (
            df_for_interpolation[~trailing]['stop_sequence'])

        # Need to check if existing index is in column names and drop if
        # so (else a ValueError where Pandas can't insert
        # b/c col already exists will occur)
        drop_bool = False
        if _check_if_index_name_in_cols(df_for_interpolation):
            # move the current index to its own col named 'index'
            log('stop_times index name: {} is also a column name. '
                'Index will be dropped for interpolation and re-created '
                'afterwards to continue.'.format(
                 df_for_interpolation.index.name))
            col_name_to_copy = df_for_interpolation.index.name
            col_to_copy = df_for_interpolation[col_name_to_copy].copy()
            df_for_interpolation['index'] = col_to_copy
            drop_bool = True
        df_for_interpolation.reset_index(inplace=True, drop=drop_bool)

        # Merge back into original index
        interpolated_df = pd.merge(
            df_for_interpolation, melted, how='left',
            on=['stop_sequence_merge', 'unique_trip_id'])

        # set index back to what it was if it was removed above before merge
        if drop_bool is False:
            interpolated_df.set_index('index', inplace=True)

        interpolated_times = (
            interpolated_df[['departure_time_sec_interpolate']])

        final_stop_times_df = pd.merge(
            stop_times_df, interpolated_times, how='left',
            left_index=True, right_index=True, sort=False, copy=False)

    else:
        final_stop_times_df = stop_times_df
        final_stop_times_df['departure_time_sec_interpolate'] = (
            final_stop_times_df['departure_time_sec'])

    # fill in nulls in interpolated departure time column using trips that
    # did not need interpolation in order to create
    # one column with both original and interpolated times
    final_stop_times_df['departure_time_sec_interpolate'].fillna(
        final_stop_times_df['departure_time_sec'], inplace=True)

    num_not_interpolated = final_stop_times_df[
        'departure_time_sec_interpolate'].isnull().sum()
    if num_not_interpolated > 0:
        log('WARNING: Number of stop_time records unable to interpolate: {:,}.'
            ' These records likely had stops in either the start or '
            'end sequence that did not have time information avaiable to '
            'interpolate between. These records have been removed.'.format(
             num_not_interpolated),
            level=lg.WARNING)

    # convert the interpolated times (float) to integer so all times are
    # the same number format
    # first run int converter on non-null records (nulls here are the last
    # stop times in a trip because there is no departure)
    final_stop_times_df = final_stop_times_df[
        final_stop_times_df['departure_time_sec_interpolate'].notnull()]
    # convert float to int
    final_stop_times_df['departure_time_sec_interpolate'] = \
        final_stop_times_df['departure_time_sec_interpolate'].astype(int)

    # add unique stop ID
    final_stop_times_df['unique_stop_id'] = (
        final_stop_times_df['stop_id'].str.cat(
            final_stop_times_df['unique_agency_id'].astype('str'), sep='_'))

    if missing_stop_times_count > 0:
        log('Departure stop time interpolation complete. '
            'Took {:,.2f} seconds.'.format(time.time() - start_time))

    return final_stop_times_df


def _time_difference(stop_times_df):
    """
    Calculate the difference in departure_time between stops in stop times
    table to produce travel time

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        interpolated stop times DataFrame

    Returns
    -------
    stop_times_df : pandas.DataFrame

    """
    start_time = time.time()

    # calculate difference between consecutive records grouping by trip ID
    stop_times_df['timediff'] = stop_times_df.groupby('unique_trip_id')[
        'departure_time_sec_interpolate'].diff()
    log('Difference between stop times has been successfully calculated. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))

    return stop_times_df


def _time_selector(df, starttime, endtime, timerange_pad=None):
    """
    Select stop times that fall within a specified time range

    Parameters
    ----------
    df : pandas.DataFrame
        interpolated stop times DataFrame
    starttime : str
        24 hour clock formatted time 1
    endtime : str
        24 hour clock formatted time 2,
    timerange_pad: str, optional
        string indicating the number of hours minutes seconds to pad after the
        end of the time interval specified in 'timerange'. Must follow format
        of a 24 hour clock for example: '02:00:00' for a two hour pad or
        '02:30:00' for a 2 hour and 30 minute pad.
    Returns
    -------
    selected_stop_timesdf : pandas.DataFrame

    """
    start_time = time.time()

    # takes input start and end time range from 24 hour clock and converts
    # it to seconds past midnight
    # in order to select times that may be after midnight

    # convert string time components to integer and then calculate seconds
    # past midnight
    # convert starttime 24 hour to seconds past midnight
    # TODO: optimize for speed
    start_h = int(str(starttime[0:2]))
    start_m = int(str(starttime[3:5]))
    start_s = int(str(starttime[6:8]))
    starttime_sec = (start_h * 60 * 60) + (start_m * 60) + start_s
    # convert endtime 24 hour to seconds past midnight
    end_h = int(str(endtime[0:2]))
    end_m = int(str(endtime[3:5]))
    end_s = int(str(endtime[6:8]))
    endtime_sec = (end_h * 60 * 60) + (end_m * 60) + end_s

    # define timepad in seconds to include stops active after specified endtime
    if timerange_pad:
        # convert timerange_pad 24 hour to seconds
        pad_h = int(str(timerange_pad[0:2]))
        pad_m = int(str(timerange_pad[3:5]))
        pad_s = int(str(timerange_pad[6:8]))
        pad_sec = (pad_h * 60 * 60) + (pad_m * 60) + pad_s

        # add endtime and timerange_pad to get new endtime and convert to
        # str for informative print
        dt1 = datetime.strptime(endtime, '%H:%M:%S')
        dt2 = datetime.strptime(timerange_pad, '%H:%M:%S')
        dt2_delta = timedelta(hours=dt2.hour, minutes=dt2.minute,
                              seconds=dt2.second)
        dt3 = dt1 + dt2_delta
        str_t3 = datetime.strftime(dt3, '%H:%M:%S')
        log('   Additional stop times active between the specified end time: '
            '{} with timerange_pad of: {} (padded end time: {}) '
            'will be selected...'.format(endtime, timerange_pad, str_t3))
    pad = int(0 if timerange_pad is None else pad_sec)

    # create df of stops times that are within the requested range
    selected_stop_timesdf = df[(
            (starttime_sec <= df["departure_time_sec_interpolate"]) & (
             df["departure_time_sec_interpolate"] <= endtime_sec + pad))]

    subset_df_count = len(selected_stop_timesdf)
    df_count = len(df)
    if timerange_pad:
        log('Stop times from {} to {} (with time_pad end time: {}) '
            'successfully selected {:,} records out of {:,} total records '
            '({:.2f} percent of total). '
            'Took {:,.2f} seconds.'.format(
                starttime, endtime, str_t3, subset_df_count, df_count,
                (subset_df_count / df_count) * 100,
                time.time() - start_time))
    else:
        log('Stop times from {} to {} successfully selected {:,} records '
            'out of {:,} total records ({:.2f} percent of total). '
            'Took {:,.2f} seconds.'.format(
                starttime, endtime, subset_df_count, df_count,
                (subset_df_count / df_count) * 100,
                time.time() - start_time))

    return selected_stop_timesdf


def _format_transit_net_edge(stop_times_df, time_aware=False):
    """
    Format transit network data table to match the format required for edges
    in Pandana graph networks edges

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        interpolated stop times with travel time between stops for the subset
        time and day
    time_aware: bool, optional
        boolean to indicate whether the transit network should include
        time information. If True, 'arrival_time' and 'departure_time' columns
        from the stop_times table will be included in the transit edge table
        where 'departure_time' is the departure time at node_id_from stop and
        'arrival_time' is the arrival time at node_id_to stop

    Returns
    -------
    merged_edge_df : pandas.DataFrame

    """
    start_time = time.time()

    log('Starting transformation process for {:,} '
        'total trips...'.format(len(stop_times_df['unique_trip_id'].unique())))

    if stop_times_df.empty:
        raise ValueError(
            "Unable to continue processing transit network. stop_times "
            "table has 0 records. Most likely the 'timerange' and or "
            "calendar selection parameters resulted in a network with no "
            "active trips within the schedule window specified. It is "
            "suggested to expand the 'timerange' and or alter the calendar "
            "selection parameters to enlarge the time window for selecting "
            "active trips.")

    # subset to only columns needed for processing
    cols_of_interest = ['unique_trip_id', 'stop_id', 'unique_stop_id',
                        'timediff', 'stop_sequence', 'unique_agency_id',
                        'trip_id', 'arrival_time', 'departure_time']
    stop_times_df = stop_times_df[cols_of_interest]

    # set columns for new df for data needed by Pandana for edges
    merged_edge = []

    stop_times_df.sort_values(by=['unique_trip_id', 'stop_sequence'],
                              inplace=True)

    if time_aware:
        log('   time_aware is True, also adding arrival and departure '
            'stop times to edges...')

    for trip, tmp_trip_df in stop_times_df.groupby(['unique_trip_id']):
        # if 'time_aware', also create arrival and departure time cols
        if time_aware:
            edge_df = pd.DataFrame({
                "node_id_from": tmp_trip_df['unique_stop_id'].iloc[:-1].values,
                "node_id_to": tmp_trip_df['unique_stop_id'].iloc[1:].values,
                "weight": tmp_trip_df['timediff'].iloc[1:].values,
                "unique_agency_id":
                    tmp_trip_df['unique_agency_id'].iloc[1:].values,
                # set unique trip ID without edge order to join other data
                # later
                "unique_trip_id": trip,
                # departure_time at node_id_from stop
                "departure_time":
                    tmp_trip_df['departure_time'].iloc[:-1].values,
                # arrival_time at node_id_to stop
                "arrival_time":
                    tmp_trip_df['arrival_time'].iloc[1:].values
            })
        else:
            edge_df = pd.DataFrame({
                "node_id_from": tmp_trip_df['unique_stop_id'].iloc[:-1].values,
                "node_id_to": tmp_trip_df['unique_stop_id'].iloc[1:].values,
                "weight": tmp_trip_df['timediff'].iloc[1:].values,
                "unique_agency_id":
                    tmp_trip_df['unique_agency_id'].iloc[1:].values,
                # set unique trip ID without edge order to join other data
                # later
                "unique_trip_id": trip
            })

        # Set current trip ID to edge ID column adding edge order at
        # end of string
        edge_df['sequence'] = (edge_df.index + 1).astype(int)

        # append completed formatted edge table to master edge table
        merged_edge.append(edge_df)

    merged_edge_df = pd.concat(merged_edge, ignore_index=True)
    merged_edge_df['sequence'] = merged_edge_df['sequence'].astype(
        int, copy=False)
    # create a unique sequential edge ID
    # TODO: consider changing col name to 'edge_id' for clarity
    merged_edge_df['id'] = (
        merged_edge_df['unique_trip_id'].str.cat(
            merged_edge_df['sequence'].astype('str'), sep='_'))

    log('Stop time table transformation to Pandana format edge table '
        'completed. Took {:,.2f} seconds.'.format(time.time() - start_time))

    return merged_edge_df


def _convert_imp_time_units(df, time_col='weight', convert_to='minutes'):
    """
    Convert the travel time impedance units

    Parameters
    ----------
    df : pandas.DataFrame
        edge DataFrame with weight column
    time_col : str
        name of column that holds the travel impedance
    convert_to : {'seconds', 'minutes'}
        unit to convert travel time to. should always be set to 'minutes'

    Returns
    -------
    df : pandas.DataFrame

    """
    valid_convert_to = ['seconds', 'minutes']
    if convert_to not in valid_convert_to or not isinstance(convert_to, str):
        raise ValueError('{} is not a valid value or is not a string.'.format(
            convert_to))

    if convert_to == 'seconds':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] * 60
        log('Time conversion completed: minutes converted to seconds.')

    if convert_to == 'minutes':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] / 60.0
        log('Time conversion completed: seconds converted to minutes.')

    return df


def _stops_in_edge_table_selector(input_stops_df, input_stop_times_df):
    """
    Select stops that are active during the day and time period specified

    Parameters
    ----------
    input_stops_df : pandas.DataFrame
        stops DataFrame
    input_stop_times_df : pandas.DataFrame
        stop_times DataFrame

    Returns
    -------
    selected_stops_df : pandas.DataFrame

    """
    start_time = time.time()

    # add unique stop ID
    input_stops_df['unique_stop_id'] = (
        input_stops_df['stop_id'].str.cat(
            input_stops_df['unique_agency_id'].astype('str'), sep='_'))

    # Select stop IDs that match stop IDs in the subset stop time data that
    # match day and time selection
    selected_stops_df = input_stops_df.loc[
        input_stops_df['unique_stop_id'].isin(
            input_stop_times_df['unique_stop_id'])]

    log('{:,} of {:,} records selected from stops. '
        'Took {:,.2f} seconds.'.format(
         len(selected_stops_df), len(input_stops_df),
         time.time() - start_time))

    return selected_stops_df


def _format_transit_net_nodes(df):
    """
    Create transit node table from stops DataFrame and perform final formatting

    Parameters
    ----------
    df : pandas.DataFrame
        transit node DataFrame

    Returns
    -------
    final_node_df : pandas.DataFrame

    """
    start_time = time.time()

    # add unique stop ID
    if 'unique_stop_id' not in df.columns:
        df['unique_stop_id'] = (
            df['stop_id'].str.cat(
                df['unique_agency_id'].astype('str'), sep='_'))

    final_node_df = pd.DataFrame()
    final_node_df['node_id'] = df['unique_stop_id']
    final_node_df['x'] = df['stop_lon']
    final_node_df['y'] = df['stop_lat']

    # keep useful info from stops table
    col_list = ['unique_agency_id', 'route_type', 'stop_id', 'stop_name']
    # if these optional cols exist then keep those that do
    optional_gtfs_cols = ['parent_station', 'stop_code', 'wheelchair_boarding',
                          'zone_id', 'location_type']
    for item in optional_gtfs_cols:
        if item in df.columns:
            col_list.append(item)

    final_node_df = pd.concat([final_node_df, df[col_list]], axis=1)
    # set node index to be unique stop ID
    final_node_df = final_node_df.set_index('node_id')

    log('Stop time table transformation to Pandana format node table '
        'completed. Took {:,.2f} seconds.'.format(time.time() - start_time))

    return final_node_df


def _route_type_to_edge(transit_edge_df, stop_time_df):
    """
    Append route type information to transit edge table

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge DataFrame
    stop_time_df : pandas.DataFrame
        stop time DataFrame

    Returns
    -------
    transit_edge_df_w_routetype : pandas.DataFrame

    """
    start_time = time.time()

    # create unique trip IDs
    stop_time_df['unique_trip_id'] = (
        stop_time_df['trip_id'].str.cat(
            stop_time_df['unique_agency_id'].astype('str'), sep='_'))

    # join route_id to the edge table
    merged_df = pd.merge(
        transit_edge_df, stop_time_df[['unique_trip_id', 'route_type']],
        how='left', on='unique_trip_id', sort=False, copy=False)
    merged_df.drop_duplicates(
        subset='unique_trip_id', keep='first', inplace=True)
    # need to get unique records here to have a one to one join -
    # this serves as the look up table
    # join the look up table created above to the table of interest
    transit_edge_df_w_routetype = pd.merge(
        transit_edge_df, merged_df[['route_type', 'unique_trip_id']],
        how='left', on='unique_trip_id', sort=False, copy=False)

    log('Route type successfully joined to transit edges. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))

    return transit_edge_df_w_routetype


def _route_id_to_edge(transit_edge_df, trips_df):
    """
    Append route IDs to transit edge table

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge DataFrame
    trips_df : pandas.DataFrame
        trips DataFrame

    Returns
    -------
    transit_edge_df_with_routes : pandas.DataFrame

    """
    start_time = time.time()

    if 'unique_route_id' not in transit_edge_df.columns:
        # create unique trip and route IDs
        trips_df['unique_trip_id'] = (
            trips_df['trip_id'].str.cat(
                trips_df['unique_agency_id'].astype('str'), sep='_'))
        trips_df['unique_route_id'] = (
            trips_df['route_id'].str.cat(
                trips_df['unique_agency_id'].astype('str'), sep='_'))

        transit_edge_df_with_routes = pd.merge(
            transit_edge_df, trips_df[['unique_trip_id', 'unique_route_id']],
            how='left', on='unique_trip_id', sort=False, copy=False)

    log('Route ID successfully joined to transit edges. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))

    return transit_edge_df_with_routes


def edge_impedance_by_route_type(
        transit_edge_df,
        travel_time_col_name='weight',
        street_level_rail=None,
        underground_rail=None,
        intercity_rail=None,
        bus=None,
        ferry=None,
        cable_car=None,
        gondola=None,
        funicular=None,
        trolleybus=None,
        monorail=None):
    """
    Penalize transit edge travel time based on transit mode type

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge DataFrame
    travel_time_col_name : str, optional
        name of travel time column to apply multiplier factor,
        default column name is 'weight'
    street_level_rail : float, optional
        factor between -1 to 1 to multiply against travel time
    underground_rail : float, optional
        factor between -1 to 1 to multiply against travel time
    intercity_rail : float, optional
        factor between -1 to 1 to multiply against travel time
    bus : float, optional
        factor between -1 to 1 to multiply against travel time
    ferry : float, optional
        factor between -1 to 1 to multiply against travel time
    cable_car : float, optional
        factor between -1 to 1 to multiply against travel time
    gondola : float, optional
        factor between -1 to 1 to multiply against travel time
    funicular : float, optional
        factor between -1 to 1 to multiply against travel time
    trolleybus : float, optional
        factor between -1 to 1 to multiply against travel time
    monorail : float, optional
        factor between -1 to 1 to multiply against travel time

    Returns
    -------
    transit_edge_df : pandas.DataFrame
        Returns transit_edge_df with travel_time_col_name column weighted by
        specified coefficients by route type
    """
    req_cols = [travel_time_col_name, 'route_type']
    if not isinstance(travel_time_col_name, str):
        raise ValueError('travel_time_col_name must be a string.')
    for col in req_cols:
        if col in transit_edge_df.columns:
            if not pd.api.types.is_numeric_dtype(transit_edge_df[col]):
                raise ValueError('{} must be a number.'.format(col))
        else:
            raise ValueError('Column: {} was not found in transit_edge_df '
                             'DataFrame and is required.'.format(col))

    # build route type lookup dict
    route_type_dict = config._ROUTES_MODE_TYPE_LOOKUP.copy()
    var_mode_id_lookup = {0: street_level_rail,
                          1: underground_rail,
                          2: intercity_rail,
                          3: bus,
                          4: ferry,
                          5: cable_car,
                          6: gondola,
                          7: funicular,
                          11: trolleybus,
                          12: monorail}
    # ensure consistency btw the keys in the config obj and the keys
    # used in this function in case changes are made in the config obj
    if set(sorted(route_type_dict.keys())) != set(
            sorted(var_mode_id_lookup.keys())):
        ValueError('ROUTES_MODE_TYPE_LOOKUP keys do not match keys in '
                   'var_mode_id_lookup. Keys must match.')
    for key, value in route_type_dict.items():
        route_type_dict[key] = {'name': value,
                                'multiplier': var_mode_id_lookup[key]}

    # create the dict to pass to value_counts()
    route_type_desc = route_type_dict.copy()
    for key, val in route_type_dict.items():
        route_type_desc[key] = val['name']

    # check count of records for each route type
    log('Route type distribution as percentage of transit mode:')
    summary_stat = transit_edge_df['route_type'].map(
        route_type_desc.get).value_counts(normalize=True, dropna=False) * 100
    log(summary_stat)

    travel_time_col = transit_edge_df[travel_time_col_name]

    for route_type, route_vals in route_type_dict.items():
        if route_vals['multiplier'] is not None:
            if not isinstance(route_vals['multiplier'], float):
                raise ValueError('One or more multiplier variables are not '
                                 'float.')

            # warn if multiplier is not within optimal range
            if not -1 <= route_vals['multiplier'] <= 1:
                log('WARNING: Multiplier value of: {} should be a '
                    'value between -1 and 1.'.format(route_vals['multiplier']),
                    level=lg.WARNING)
            route_type_cnt = len(
                transit_edge_df[transit_edge_df['route_type'] == route_type])

            # warn if route type is not found in DataFrame
            if route_type_cnt == 0 and route_vals['multiplier'] is not None:
                log('WARNING: Route type: {} with specified multiplier value '
                    'of: {} was not found in the specified edge '
                    'DataFrame.'.format(
                     route_vals['name'], route_vals['multiplier']),
                    level=lg.WARNING)

            if route_type_cnt > 0:
                transit_edge_df[travel_time_col_name][
                    transit_edge_df['route_type'] == route_type] = \
                    travel_time_col + (
                            travel_time_col * route_vals['multiplier'])
                log('Adjusted {} transit edge impedance based on mode '
                    'type penalty coefficient: {}.'.format(
                        route_vals['name'], route_vals['multiplier']))

    log('Transit edge impedance mode type penalty calculation complete.')
    return transit_edge_df


def save_processed_gtfs_data(
        gtfsfeeds_dfs, filename, dir=config.settings.data_folder):
    """
    Write DataFrames in an urbanaccess_gtfs_df object to a HDF5 file

    Parameters
    ----------
    gtfsfeeds_dfs : object
        urbanaccess_gtfs_df object
    filename : string
        name of the HDF5 file to save with .h5 extension
    dir : string, optional
        directory to save HDF5 file

    Returns
    -------
    None
    """
    log('Writing HDF5 store...')
    if not isinstance(gtfsfeeds_dfs, urbanaccess_gtfs_df):
        raise ValueError('gtfsfeeds_dfs must be an urbanaccess_gtfs_df '
                         'object.')

    req_df_dict = {'stops': gtfsfeeds_dfs.stops,
                   'routes': gtfsfeeds_dfs.routes,
                   'trips': gtfsfeeds_dfs.trips,
                   'stop_times': gtfsfeeds_dfs.stop_times,
                   'stop_times_int': gtfsfeeds_dfs.stop_times_int}
    # calendar or calendar_dates are required but not both
    optional_df_dict = {'headways': gtfsfeeds_dfs.headways,
                        'calendar': gtfsfeeds_dfs.calendar,
                        'calendar_dates': gtfsfeeds_dfs.calendar_dates}

    for name, gtfs_df in req_df_dict.items():
        if gtfs_df.empty:
            raise ValueError('gtfsfeeds_dfs is missing required '
                             'DataFrame: {}.'.format(name))
    if gtfsfeeds_dfs.calendar.empty and gtfsfeeds_dfs.calendar_dates.empty:
        raise ValueError('gtfsfeeds_dfs is missing either the calendar or '
                         'calendar_dates DataFrame.')

    tables_saved = []
    for name, gtfs_df in req_df_dict.items():
        df_to_hdf5(data=gtfs_df, key=name,
                   overwrite_key=False, dir=dir, filename=filename,
                   overwrite_hdf5=False)
        tables_saved.extend([name])

    for name, gtfs_df in optional_df_dict.items():
        if gtfs_df.empty is False:
            df_to_hdf5(data=gtfs_df, key=name,
                       overwrite_key=False, dir=dir, filename=filename,
                       overwrite_hdf5=False)
            tables_saved.extend([name])

    log('Saved HDF5 store: {} with tables: {}.'.format(
        os.path.join(dir, filename), tables_saved))


def load_processed_gtfs_data(filename, dir=config.settings.data_folder):
    """
    Read data from a HDF5 file to an urbanaccess_gtfs_df object

    Parameters
    ----------
    filename : string
        name of the HDF5 file to read with .h5 extension
    dir : string, optional
        directory to read HDF5 file

    Returns
    -------
    gtfsfeeds_dfs : object
        urbanaccess_gtfs_df object
    """
    log('Loading HDF5 store...')
    req_df_dict = {'stops': gtfsfeeds_dfs.stops,
                   'routes': gtfsfeeds_dfs.routes,
                   'trips': gtfsfeeds_dfs.trips,
                   'stop_times': gtfsfeeds_dfs.stop_times,
                   'stop_times_int': gtfsfeeds_dfs.stop_times_int}
    # calendar or calendar_dates are required but not both
    optional_df_dict = {'headways': gtfsfeeds_dfs.headways,
                        'calendar': gtfsfeeds_dfs.calendar,
                        'calendar_dates': gtfsfeeds_dfs.calendar_dates}

    tables_read = []
    for name, gtfs_df in req_df_dict.items():
        vars(gtfsfeeds_dfs)[name] = hdf5_to_df(
            dir=dir, filename=filename, key=name)
        tables_read.extend([name])

    # open HDF5 to read keys
    hdf5_load_path = os.path.join(dir, filename)
    with pd.HDFStore(hdf5_load_path) as store:
        hdf5_keys = store.keys()
    hdf5_keys = [item.replace('/', '') for item in hdf5_keys]
    for name, gtfs_df in optional_df_dict.items():
        # if optional key exists, read it
        if name in hdf5_keys:
            vars(gtfsfeeds_dfs)[name] = hdf5_to_df(
                dir=dir, filename=filename, key=name)
            tables_read.extend([name])
    log('Read HDF5 store: {} tables: {}.'.format(
        hdf5_load_path, tables_read))

    return gtfsfeeds_dfs


def _check_if_index_name_in_cols(df):
    """
    Check if specified Dataframe has an index name that is also a column name

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe to check index and columns

    Returns
    -------
    iname : boolean
        True if index name is also a column name, else False
    """
    cols = df.columns.values
    iname = df.index.name
    return (iname in cols)


def _remove_nodes_not_in_edges(nodes, edges, from_id_col, to_id_col):
    """
    Helper function to remove nodes from node DataFrame that are not found
    in the edges DataFrame

    Parameters
    ----------
    nodes : pandas.DataFrame
        nodes DataFrame
    edges : pandas.DataFrame
        edges DataFrame
    from_id_col : str
        name of from ID column
    to_id_col : str
        name of to ID column

    Returns
    -------
    nodes_df : pandas.DataFrame
        node DataFrame with only the nodes that exist in the edge DataFrame
    """
    edge_node_list = set(edges[from_id_col]) | set(edges[to_id_col])
    nodes_df = nodes.loc[nodes.index.isin(edge_node_list)]
    return nodes_df
