import pandas as pd

from .gtfs_format import join_camel, time_selector, trip_sched_selector


def convert_transit_data_to_network(gtfsfeeds_df, day, timerange):
    # Create a travel time weight network graph in minutes

    # TODO: Should this be moved within below if statement?
    cal_select_trips = _generate_cal_select_trips(gtfsfeeds_df, day)

    # confirm if need to interpolate stop times
    if gtfsfeeds_df['stop_times_int'].empty:
        # TODO: Seems strange to need to rerun this as it is
        #       guaranteed to be run prior
        gtfsfeeds_df['stop_times_int'] = _interpolate_stop_times(
            gtfsfeeds_df['stop_times'],
            cal_select_trips)

    # select times
    sel_interp_stop_times_df = time_selector(
        gtfsfeeds_df['stop_times_int'],
        timerange[0],
        timerange[1])

    # generate final edges dataframe for transit
    desired_stops_int_cols = ['unique_trip_id',
                              'stop_id',
                              'unique_stop_id',
                              'timediff',
                              'stop_sequence',
                              'unique_agency_id',
                              'trip_id']
    sub_stops_int = sel_interp_stop_times_df[desired_stops_int_cols]
    formatted_edges = _format_transit_net_edge(sub_stops_int)

    # now convert to desired time format (hrs?) and 
    # apply to global network value
    time_col = 'weight'
    formatted_edges[time_col] = formatted_edges[time_col].astype('float')
    formatted_edges[time_col] = formatted_edges[time_col] / 60.0

    # similarly, finalize select transit network nodes and apply to network
    stops = gtfsfeeds_df['stops']
    stops['unique_stop_id'] = _generate_uid(stops,
                                            ['stop_id', 'unique_agency_id'])

    sel_interp_sub = sel_interp_stop_times_df['unique_stop_id']
    stops_are_in = stops['unique_stop_id'].isin(sel_interp_sub)
    final_select_stops = stops.loc[stops_are_in]

    # create formatted nodes just like did with edges
    # TODO: This check should not be an unknown, carried over from UA
    #       add unique stop id if needed
    if 'unique_stop_id' not in final_select_stops.columns:
        final_select_stops['unique_stop_id'] = _generate_uid(
            final_select_stops,
            ['stop_id', 'unique_agency_id'])

    # create a dataframe from select final selected stop df columns
    formatted_nodes = _generate_formatted_nodes_df(final_select_stops)

    # generate the formatted nodes data frame, as we did with edges
    relevant_subset = _generate_relevant_subset_select_stops(
        final_select_stops)

    # set node index to be unique stop id
    joined_with_nodes = pd.concat([formatted_nodes, relevant_subset], axis=1)
    transit_nodes = joined_with_nodes.set_index('node_id')

    # TODO: Undesirable to continually update networks.transit_edges
    #       Should reach a "final" state before updating (so fix above)
    stop_times = gtfsfeeds_df['stop_times']

    # TODO: Fully understand route_type issues, but in the meantime
    #       we will be using these cleaning steps.    
    
    edges_with_type_col = _route_type_to_edge(formatted_edges, stop_times)
    transit_edges = _route_id_to_edge(edges_with_type_col,
                                      gtfsfeeds_df['trips'])

    # assign node and edge net type
    transit_str = 'transit'
    transit_nodes['net_type'] = transit_str
    transit_edges['net_type'] = transit_str

    return {'nodes': transit_nodes, 'edges': transit_edges}


def add_time_difference(stop_times_df):
    """
    Calculate the difference in departure_time between stops in stop times
    table to produce travel time

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        interpolated stop times dataframe

    Returns
    -------
    stop_times_df : pandas.DataFrame

    """

    grouped = stop_times_df.groupby('unique_trip_id')
    diffed = grouped['departure_time_sec_interpolate'].diff()
    stop_times_df['timediff'] = diffed

    return stop_times_df


def interp_stop_times(stop_times_df, cal_sel_trips_df, day='wednesday'):
    """
    Interpolate missing stop times using a linear
    interpolator between known stop times

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        stop times dataframe
    calendar_selected_trips_df : pandas.DataFrame
        dataframe of trips that run on specific day
    day : {'friday','monday','saturday','sunday','thursday','tuesday',
    'wednesday'}
        day of the week to extract transit schedule from that corresponds
        to the day in the GTFS calendar

    Returns
    -------
    final_stop_times_df : pandas.DataFrame

    """
    # trip id can be an integer, in which case want to make sure it
    # is cast as a string before attempting to concatenate it
    cal_trip_ids = cal_sel_trips_df['trip_id'].astype(str)
    cal_uaid = cal_sel_trips_df['unique_agency_id'].astype(str)
    # create unique trip id for cal and stop times
    u_cal_trip_ids = cal_trip_ids.str.cat(cal_uaid, sep='_')
    cal_sel_trips_df['unique_trip_id'] = u_cal_trip_ids

    # same process again, this time for stop times
    st_times_trip_ids = stop_times_df['trip_id'].astype(str)
    st_times_uaid = stop_times_df['unique_agency_id'].astype(str)
    u_st_times_trip_id = st_times_trip_ids.str.cat(st_times_uaid, sep='_')
    stop_times_df['unique_trip_id'] = u_st_times_trip_id

    sort_cols = ['unique_trip_id', 'stop_sequence']
    stop_times_df.sort_values(by=sort_cols, inplace=True)

    # make list of unique trip ids from the cal_sel_trips_df
    unique_trips = cal_sel_trips_df['unique_trip_id'].unique()
    unique_trips_list = unique_trips.tolist()

    # select trip ids that match trips in cal_sel_trips_df
    # resulting df will be stop times only for trips
    # that run on the service day of interest
    in_unqiue_list = stop_times_df['unique_trip_id'].isin(unique_trips_list)
    stop_times_df = stop_times_df[in_unqiue_list]

    # find trips with more than one missing time
    # Note: all trip ids have at least 1 null departure time
    #       b/c last stop in a trip is always null
    null_times = stop_times_df[stop_times_df.departure_time_sec.isnull()]
    trips_with_null = null_times.unique_trip_id.value_counts()
    more_than_one_null = trips_with_null[trips_with_null > 1]
    trips_many_nulls = more_than_one_null.index.values

    # Subset stop times DataFrame to only those with >1 null time
    trip_uid = stop_times_df.unique_trip_id
    stops_with_many_nulls = trip_uid.isin(trips_many_nulls)
    df_for_interpolation = stop_times_df.loc[stops_with_many_nulls]

    # pivot df where each unique trip has its own column
    # with index set as stop_sequence
    pivot = df_for_interpolation.pivot(index='stop_sequence',
                                       columns='unique_trip_id',
                                       values='departure_time_sec')

    # interpolate on the whole DataFrame at once
    interpolator = pivot.interpolate(method='linear', axis=0,
                                     limit_direction='forward')

    # melt back into stacked format
    interpolator['stop_sequence_merge'] = interpolator.index
    melted = pd.melt(interpolator, id_vars='stop_sequence_merge')
    col_rename = {'value': 'departure_time_sec_interpolate'}
    melted.rename(columns=col_rename, inplace=True)

    # get last valid stop for unique trips,
    # then filter out trailing NaNs
    last_valid = pivot.apply(lambda col: col.last_valid_index(), axis=0)
    last_valid_stop_df = last_valid.to_frame('last_valid_stop')

    merged_df = df_for_interpolation.merge(last_valid_stop_df,
                                           left_on='unique_trip_id',
                                           right_index=True)

    trailing = merged_df.stop_sequence > merged_df.last_valid_stop

    # Calculate a stop_sequence w/out trailing
    # NaNs, to merge the correct interpolated times
    no_trailing = merged_df[~trailing]['stop_sequence']
    merged_df['stop_sequence_merge'] = no_trailing

    # Need to check if existing index in column names and drop if so (else
    # a ValueError where Pandas can't insert b/c col already exists will occur)
    drop_bool = False
    if _check_if_index_name_in_cols(merged_df):
        # move the current index to own col named 'index'
        col_name_to_copy = merged_df.index.name
        col_to_copy = merged_df[col_name_to_copy].copy()
        merged_df['index'] = col_to_copy
        drop_bool = True
    merged_df.reset_index(inplace=True, drop=drop_bool)

    # Merge back into original index
    merge_cols = ['stop_sequence_merge', 'unique_trip_id']
    interpolated_df = pd.merge(merged_df,
                               melted,
                               'left',
                               on=merge_cols)
    interpolated_df.set_index('index', inplace=True)

    # this is a long name, let's store as a var
    dep_time_name = 'departure_time_sec_interpolate'

    # get subset dataframe
    interpolated_times = interpolated_df[[dep_time_name]]

    final_stop_times = pd.merge(stop_times_df,
                                interpolated_times,
                                how='left',
                                left_index=True,
                                right_index=True,
                                sort=False,
                                copy=False)

    # merge known times with interpolated times for departures
    dep_time_sec = final_stop_times['departure_time_sec']
    final_stop_times[dep_time_name].fillna(dep_time_sec, inplace=True)

    no_null_dep_time = final_stop_times[dep_time_name].notnull()
    final_stop_times = final_stop_times[no_null_dep_time]

    # convert float to int
    final_stop_times[dep_time_name] = final_stop_times[
        dep_time_name].astype(int)

    # add unique stop id
    st_uid = ['stop_id', 'unique_agency_id']
    join_st_uids = final_stop_times[st_uid].apply(
        lambda x: join_camel(x), axis=1)
    final_stop_times['unique_stop_id'] = join_st_uids

    return final_stop_times


def _generate_expected_transit_nw_cols(gtfs_trips):
    # cols we expect in the dataframe
    columns = ['route_id',
               'direction_id',
               'trip_id',
               'service_id',
               'unique_agency_id']

    # direction_id is conditional on its inclusion in gtfs, though
    if 'direction_id' not in gtfs_trips.columns:
        columns.remove('direction_id')

    return columns


def _generate_cal_select_trips(gtfsfeeds_df, day):
    columns = _generate_expected_transit_nw_cols(gtfsfeeds_df['trips'])
    trips_sub_gdf = gtfsfeeds_df['trips'][columns]
    calendar_selected_trips_df = trip_sched_selector(
        trips_sub_gdf,
        gtfsfeeds_df['calendar'],
        day)

    return calendar_selected_trips_df


def _interpolate_stop_times(st_times, cal_select_trips):
    interp_stops = interp_stop_times(st_times, cal_select_trips)
    time_diff_interp_stops = add_time_difference(interp_stops)

    return time_diff_interp_stops


def _generate_uid(main_df, sub_selection):
    subset_df = main_df[sub_selection]
    resulting_uids = subset_df.apply(lambda x: join_camel(x), axis=1)

    return resulting_uids


def _generate_formatted_nodes_df(fin_sel_stops):
    # base case of empty dataframe
    formatted_nodes = pd.DataFrame()

    formatted_nodes['node_id'] = fin_sel_stops['unique_stop_id']
    formatted_nodes['x'] = fin_sel_stops['stop_lon']
    formatted_nodes['y'] = fin_sel_stops['stop_lat']

    return formatted_nodes


def _generate_relevant_subset_select_stops(fin_sel_stops):
    # keep useful info from stops table
    fin_cols_use = ['unique_agency_id',
                    'route_type',
                    'stop_id',
                    'stop_name']

    # if these optional cols exist then keep those that do
    opt_gtfs_cols = ['parent_station',
                     'stop_code',
                     'wheelchair_boarding',
                     'zone_id',
                     'location_type']

    # determine if we should include any of the optional gtfs columns
    # if they do exist in the gtfs feed being provided
    fss_cols = fin_sel_stops.columns
    [fin_cols_use.append(ea) for ea in opt_gtfs_cols if ea in fss_cols]

    # generate the formatted nodes data frame, as we did with edges
    fin_sub = fin_sel_stops[fin_cols_use]

    return fin_sub


def _route_type_to_edge(transit_edge_df, stop_time_df):
    # append route type information to transit edge table

    st_time_uid = _generate_uid(stop_time_df, ['trip_id', 'unique_agency_id'])
    stop_time_df['unique_trip_id'] = st_time_uid

    # join route_id to the edge table
    stop_time_df['unique_agency_id'] = stop_time_df['unique_agency_id'].astype(str)
    stop_time_df['route_type'] = stop_time_df['route_type'].astype(str)
    st_time_sub = stop_time_df[['unique_trip_id', 'route_type']]
    merged = pd.merge(transit_edge_df,
                      st_time_sub,
                      how='left',
                      on='unique_trip_id',
                      sort=False,
                      copy=False)

    # need to get unique records here to have a one to one join
    # this serves as the look up table
    merged.drop_duplicates(subset='unique_trip_id',
                           keep='first',
                           inplace=True)

    # join the look up table created above to the table of interest
    merged_sub = merged[['route_type', 'unique_trip_id']]
    transit_edge_df_w_routetype = pd.merge(transit_edge_df,
                                           merged_sub,
                                           how='left',
                                           on='unique_trip_id',
                                           sort=False,
                                           copy=False)

    return transit_edge_df_w_routetype


def _route_id_to_edge(transit_edge_df, trips_df):
    # append route ids to transit edge table

    # create unique trip and route ids
    trips_tr_sub = trips_df[['trip_id', 'unique_agency_id']]
    tr_uid = trips_tr_sub.apply(lambda x: join_camel(x), axis=1)
    trips_df['unique_trip_id'] = tr_uid

    trips_rt_sub = trips_df[['route_id', 'unique_agency_id']]
    rte_uid = trips_rt_sub.apply(lambda x: join_camel(x), axis=1)
    trips_df['unique_route_id'] = rte_uid

    trips_sub = trips_df[['unique_trip_id', 'unique_route_id']]
    transit_edge_df_with_routes = pd.merge(transit_edge_df,
                                           trips_sub,
                                           how='left',
                                           on='unique_trip_id',
                                           sort=False,
                                           copy=False)

    return transit_edge_df_with_routes


def _format_transit_net_edge(stop_times_df):
    # format transit network data table to match the format
    # required for edges in Pandana graph networks edges

    # set columns for new df for data needed by pandana for edges
    merged_edges = []

    # sort the stop_times_df so that stops are in order
    sort_cols = ['unique_trip_id', 'stop_sequence']
    stop_times_df.sort_values(by=sort_cols, inplace=True)

    # take the sorted df and group into their unique trips
    grouped = stop_times_df.groupby(['unique_trip_id'])

    # iterate through each unique trip to enable us to generate
    # a summary of each trip independently
    for trip_id, trip_df in grouped:
        edge_df = _generate_edge_from_single_trip(trip_id, trip_df)
        # set current trip id to edge id col adding edge order at string end
        edge_df['sequence'] = (edge_df.index + 1).astype(int)
        # append completed formatted edge table to master edge table
        merged_edges.append(edge_df)

    # combine all edges together
    merged_df = pd.concat(merged_edges, ignore_index=True)

    # recast the sequences as integers
    merged_df['sequence'] = merged_df['sequence'].astype(int, copy=False)

    # create a new unique id that accounts for both prev uid and sequence
    merged_df['id'] = _generate_uid(merged_df, ['unique_trip_id', 'sequence'])

    return merged_df


def _generate_edge_from_single_trip(trip_id, trip_df):
    # from and to ids need to be offset 1 from each other
    node_id_from = trip_df['unique_stop_id'].iloc[:-1].values
    node_id_to = trip_df['unique_stop_id'].iloc[1:].values

    # for the following, we use all but first in values because
    # start of trip is a null value (no time difference from the
    # first stop, for example)
    weight = trip_df['timediff'].iloc[1:].values
    unique_agency_id = trip_df['unique_agency_id'].iloc[1:].values

    edge_df = pd.DataFrame({
        'node_id_from': node_id_from,
        'node_id_to': node_id_to,
        'weight': weight,
        'unique_agency_id': unique_agency_id,
        'unique_trip_id': trip_id
    })

    return edge_df


def _check_if_index_name_in_cols(df):
    """
    Check if existing index is in the passed dataframe list of column names

    Parameters
    ----------
    df : pandas.DataFrame
        interpolated stop_time dataframe

    Returns
    -------
    iname : tuple
    """

    cols = df.columns.values
    iname = df.index.name
    return (iname in cols)
