from __future__ import division
import pandas as pd
import time
import numpy as np
import logging as lg

from urbanaccess.utils import log, df_to_hdf5, hdf5_to_df
from urbanaccess.network import ua_network
from urbanaccess import config
from urbanaccess.gtfs.gtfsfeeds_dataframe import gtfsfeeds_df


pd.options.mode.chained_assignment = None

def create_transit_net(gtfsfeeds_df=None,day=None,timerange=None,
                       overwrite_existing_stop_times_int=False,
                       use_existing_stop_times_int=False,
                       save_processed_gtfs=False,
                       save_dir=config.settings.data_folder,
                       save_filename=None):
    """
    Create a travel time weight network graph in units of
    minutes from GTFS data

    Parameters
    ----------
    gtfsfeeds_df : object
        gtfsfeeds_df object with dataframes of stops, routes, trips,
        stop_times, calendar, and stop_times_int (optional)
    day : {'friday','monday','saturday', 'sunday','thursday','tuesday','wednesday'}
        day of the week to extract transit schedule from that
        corresponds to the day in the GTFS calendar
    timerange : list
        time range to extract transit schedule from in a list with time
        1 and time 2. it is suggested the time range
        specified is large enough to allow for travel
        from one end of the transit network to the other but small enough
        to represent a relevant travel time period such as a 3 hour window
        for the AM Peak period. must follow format
        of a 24 hour clock for example: 08:00:00 or 17:00:00
    overwrite_existing_stop_times_int : bool
        if true, and if there is an existing stop_times_int
        dataframe stored in the gtfsfeeds_df object it will be
        overwritten
    use_existing_stop_times_int : bool
        if true, and if there is an existing stop_times_int
        dataframe for the same time period stored in the
        gtfsfeeds_df object it will be used instead of re-calculated
    save_processed_gtfs : bool
        if true, all processed gtfs dataframes will
        be stored to disk in a hdf5 file
    save_dir : str
        directory to save the hdf5 file
    save_filename : str
        name to save the hdf5 file as

    Returns
    -------
    ua_network : object
    ua_network.transit_edges : pandas.DataFrame
    ua_network.transit_nodes : pandas.DataFrame
    """
    start_time = time.time()

    time_error_statement = ('{} starttime and endtime are not in the correct format. '
                       'Format should be 24 hour clock in following format: 08:00:00 or 17:00:00'.format(timerange))
    assert isinstance(timerange,list) and len(timerange) == 2, time_error_statement
    assert timerange[0] < timerange[1], time_error_statement
    for t in timerange:
        assert isinstance(t,str), time_error_statement
        assert len(t) == 8, time_error_statement
    if int(str(timerange[1][0:2])) - int(str(timerange[0][0:2])) > 3:
        log('WARNING: Time range passed: {} is a {} hour period. Long periods over 3 hours may take a '
            'significant amount of time to process.'.format(timerange,
                                                            int(str(timerange[1][0:2])) - int(str(timerange[0][0:2]))),level=lg.WARNING)
    assert gtfsfeeds_df is not None
    if gtfsfeeds_df.trips.empty or gtfsfeeds_df.calendar.empty or gtfsfeeds_df.stop_times.empty or gtfsfeeds_df.stops.empty:
        raise ValueError('one of the gtfsfeeds_df object trips, calendar, stops, or stop_times were found to be empty.')
    assert isinstance(overwrite_existing_stop_times_int,bool)
    assert isinstance(use_existing_stop_times_int,bool)
    assert isinstance(save_processed_gtfs,bool)

    columns = ['route_id',
               'direction_id',
               'trip_id',
               'service_id',
               'unique_agency_id']
    if 'direction_id' not in gtfsfeeds_df.trips.columns:
        columns.remove('direction_id')
    calendar_selected_trips_df = tripschedualselector(input_trips_df=gtfsfeeds_df.trips[columns],
                                                      input_calendar_df=gtfsfeeds_df.calendar,
                                                      day=day)

    if gtfsfeeds_df.stop_times_int.empty or overwrite_existing_stop_times_int or use_existing_stop_times_int == False:
        gtfsfeeds_df.stop_times_int = interpolatestoptimes(stop_times_df=gtfsfeeds_df.stop_times,
                                                           calendar_selected_trips_df=calendar_selected_trips_df,
                                                           day=day)

        gtfsfeeds_df.stop_times_int = timedifference(stop_times_df=gtfsfeeds_df.stop_times_int)

        if save_processed_gtfs:
            save_processed_gtfs_data(gtfsfeeds_df=gtfsfeeds_df,dir=save_dir,filename=save_filename)

    if use_existing_stop_times_int:
        assert gtfsfeeds_df.stop_times_int.empty == False, 'existing stop_times_int is empty. ' \
                                                           'set use_existing_stop_times_int to False to create it.'

    selected_interpolated_stop_times_df = timeselector(df=gtfsfeeds_df.stop_times_int,
                                                       starttime=timerange[0],
                                                       endtime=timerange[1])

    final_edge_table = format_transit_net_edge(stop_times_df=selected_interpolated_stop_times_df[['unique_trip_id',
                                                                                                  'stop_id',
                                                                                                  'unique_stop_id',
                                                                                                  'timediff',
                                                                                                  'stop_sequence',
                                                                                                  'unique_agency_id',
                                                                                                  'trip_id']])

    ua_network.transit_edges = convert_imp_time_units(df=final_edge_table,
                                                      time_col='weight',
                                                      convert_to='minutes')

    final_selected_stops = stops_in_edge_table_selector(input_stops_df=gtfsfeeds_df.stops,
                                                        input_stop_times_df=selected_interpolated_stop_times_df)

    ua_network.transit_nodes = format_transit_net_nodes(df=final_selected_stops)

    ua_network.transit_edges = route_type_to_edge(transit_edge_df=ua_network.transit_edges,
                                                  stop_time_df=gtfsfeeds_df.stop_times)

    ua_network.transit_edges = route_id_to_edge(transit_edge_df=ua_network.transit_edges,
                                                trips_df=gtfsfeeds_df.trips)

    # assign node and edge net type
    ua_network.transit_nodes['net_type'] = 'transit'
    ua_network.transit_edges['net_type'] = 'transit'

    log('Successfully created transit network. Took {:,.2f} seconds'.format(time.time()-start_time))

    return ua_network

def tripschedualselector(input_trips_df=None,input_calendar_df=None,day=None):
    """
    Select trips that run on a specific day

    Parameters
    ----------
    input_trips_df : pandas.DataFrame
        trips dataframe
    input_calendar_df : pandas.DataFrame
        calendar dataframe
    day : {'friday','monday','saturday','sunday','thursday','tuesday','wednesday'}
        day of the week to extract transit schedule from that corresponds to the day in the GTFS calendar

    Returns
    -------
    calendar_selected_trips_df : pandas.DataFrame

    """
    start_time = time.time()

    valid_days = ['friday','monday','saturday','sunday','thursday','tuesday','wednesday']
    assert day in valid_days and isinstance(day, str),'Incorrect day specified. Must be lowercase string: ' \
                                                      'friday, monday, saturday, sunday, thursday, tuesday, wednesday.'

    # create unique service ids
    input_trips_df['unique_service_id'] = input_trips_df[['service_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)
    input_calendar_df['unique_service_id'] = input_calendar_df[['service_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

    # select service ids where day specified in function has a 1 = service runs on that day
    input_calendar_df = input_calendar_df[(input_calendar_df[day] == 1)] # subset calendar by the specified day
    input_calendar_df = input_calendar_df[['unique_service_id']]

    # select and create df of trips that match the service ids for the day of the week specified in function
    # merge calendar df that has service ids for specified day with trips df
    calendar_selected_trips_df = input_trips_df.loc[input_trips_df['unique_service_id'].isin(input_calendar_df['unique_service_id'])]

    sort_columns = ['route_id', 'trip_id', 'direction_id']
    if 'direction_id' not in calendar_selected_trips_df.columns:
        sort_columns.remove('direction_id')
    calendar_selected_trips_df.sort_values(by=sort_columns, inplace=True)
    calendar_selected_trips_df.reset_index(drop=True,inplace=True)
    calendar_selected_trips_df.drop('unique_service_id', axis=1, inplace=True)

    log('{:,} of {:,} total trips were extracted representing calendar day: {}. Took {:,.2f} seconds'.format(len(calendar_selected_trips_df),len(input_trips_df),day,time.time()-start_time))

    return calendar_selected_trips_df

def interpolatestoptimes(stop_times_df, calendar_selected_trips_df, day):
    """
    Interpolate missing stop times using a linear interpolator between known stop times

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        stop times dataframe
    calendar_selected_trips_df : pandas.DataFrame
        dataframe of trips that run on specific day
    day : {'friday','monday','saturday','sunday','thursday','tuesday','wednesday'}
        day of the week to extract transit schedule from that corresponds to the day in the GTFS calendar

    Returns
    -------
    final_stop_times_df : pandas.DataFrame

    """

    start_time = time.time()

    # create unique trip id
    calendar_selected_trips_df['unique_trip_id'] = (
        calendar_selected_trips_df.trip_id.str.cat(
            calendar_selected_trips_df.unique_agency_id.astype('str'),
            sep='_'))

    stop_times_df['unique_trip_id'] = (
        stop_times_df.trip_id.str.cat(
            stop_times_df.unique_agency_id.astype('str'),
            sep='_'))

    # sort stop times inplace based on first to last stop in sequence -- required as the linear interpolator runs from first value to last value
    if stop_times_df['stop_sequence'].isnull().sum() > 1:
        log('WARNING: There are {:,} '
            'stop_sequence records missing in the stop_times dataframe. '
            'Please check these missing values. In order for interpolation '
            'to proceed correctly, '
            'all records must have a stop_sequence value.'.format(stop_times_df['stop_sequence'].isnull().sum()),level=lg.WARNING)

    stop_times_df.sort_values(by=['unique_trip_id', 'stop_sequence'], inplace=True)
    # make list of unique trip ids from the calendar_selected_trips_df
    uniquetriplist = calendar_selected_trips_df['unique_trip_id'].unique().tolist()
    # select trip ids that match the trips in the calendar_selected_trips_df -- resulting df will be stop times only for trips that run on the service day of interest
    stop_times_df = stop_times_df[stop_times_df['unique_trip_id'].isin(uniquetriplist)]

    log('Note: Processing may take a long time depending'
        ' on the number of records. '
        'Total unique trips to assess: {:,}'.format(len(stop_times_df['unique_trip_id'].unique())),level=lg.WARNING)
    log('Starting departure stop time interpolation...')
    log('Departure time records missing from trips following'
        ' {} schedule: {:,} '
        '({:.2f} percent of {:,} total records)'.format(day,
                                                        stop_times_df['departure_time_sec'].isnull().sum(),
                                                        (stop_times_df['departure_time_sec'].isnull().sum() / len(stop_times_df)) *100,
                                                        len(stop_times_df['departure_time_sec'])))

    log('Interpolating...')

    # Find trips with more than one missing time
    # Note: all trip ids have at least 1 null departure time because the
    # last stop in a trip is always null
    null_times = stop_times_df[stop_times_df.departure_time_sec.isnull()]
    trips_with_null = null_times.unique_trip_id.value_counts()
    trips_with_more_than_one_null = trips_with_null[
        trips_with_null > 1].index.values

    # Subset stop times DataFrame to only those with >1 null time
    df_for_interpolation = stop_times_df.loc[
        stop_times_df.unique_trip_id.isin(trips_with_more_than_one_null)]

    # Pivot to DataFrame where each unique trip has its own column
    # Index is stop_sequence
    pivot = df_for_interpolation.pivot(index='stop_sequence',
                                       columns='unique_trip_id',
                                       values='departure_time_sec')

    # Interpolate on the whole DataFrame at once
    interpolator = pivot.interpolate(method='linear', axis=0,
                                     limit_direction='forward')

    # Melt back into stacked format
    interpolator['stop_sequence_merge'] = interpolator.index
    melted = pd.melt(interpolator, id_vars='stop_sequence_merge')
    melted.rename(columns={'value': 'departure_time_sec_interpolate'},
                  inplace=True)

    # Get the last valid stop for each unique trip, to filter out trailing NaNs
    last_valid_stop_series = pivot.apply(
        lambda col: col.last_valid_index(), axis=0)
    last_valid_stop_df = last_valid_stop_series.to_frame('last_valid_stop')

    df_for_interpolation = df_for_interpolation.merge(last_valid_stop_df,
                                                      left_on='unique_trip_id',
                                                      right_index=True)
    trailing = (df_for_interpolation.stop_sequence >
                df_for_interpolation.last_valid_stop)

    # Calculate a stop_sequence without trailing NaNs, to merge the correct
    # interpolated times back in
    df_for_interpolation['stop_sequence_merge'] = (
        df_for_interpolation[~trailing]['stop_sequence'])

    # Need to check if existing index in column names and drop if so (else
    # a ValueError where Pandas can't insert b/c col already exists will occur)
    drop_bool = False
    if _check_if_index_name_in_cols(df_for_interpolation):
        # move the current index to own col named 'index'
        col_name_to_copy = df_for_interpolation.index.name
        col_to_copy = df_for_interpolation[col_name_to_copy].copy()
        df_for_interpolation['index'] = col_to_copy
        drop_bool = True
    df_for_interpolation.reset_index(inplace=True, drop=drop_bool)

    # Merge back into original index
    interpolated_df = pd.merge(df_for_interpolation, melted, 'left',
                               on=['stop_sequence_merge', 'unique_trip_id'])
    interpolated_df.set_index('index', inplace=True)
    interpolated_times = interpolated_df[['departure_time_sec_interpolate']]

    final_stop_times_df = pd.merge(stop_times_df, interpolated_times,
                                   how='left', left_index=True,
                                   right_index=True, sort=False, copy=False)

    # fill in nulls in interpolated departure time column using trips that did not need interpolation in order to create
    # one column with both original and interpolated times
    final_stop_times_df['departure_time_sec_interpolate'].fillna(final_stop_times_df['departure_time_sec'], inplace=True)

    if final_stop_times_df['departure_time_sec_interpolate'].isnull().sum() > 0:
        log('WARNING: Number of records unable to interpolate: {:,}. '
            'These records have been removed.'.format(final_stop_times_df['departure_time_sec_interpolate'].isnull().sum()),level=lg.WARNING)

    ## convert the interpolated times (float) to integer so all times are the same number format
    # first run int converter on non-null records (nulls here are the last stop times in a trip because there is no departure)
    final_stop_times_df = final_stop_times_df[final_stop_times_df['departure_time_sec_interpolate'].notnull()]
    # convert float to int
    final_stop_times_df['departure_time_sec_interpolate'] = final_stop_times_df['departure_time_sec_interpolate'].astype(int)

    # add unique stop id
    final_stop_times_df['unique_stop_id'] = final_stop_times_df[['stop_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

    log('Departure stop time interpolation complete. Took {:,.2f} seconds'.format(time.time()-start_time))

    return final_stop_times_df

def timedifference(stop_times_df=None):
    """
    Calculate the difference in departure_time between stops in stop times table to produce travel time

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        interpolated stop times dataframe

    Returns
    -------
    stop_times_df : pandas.DataFrame

    """
    start_time = time.time()

    ## calculate difference between consecutive records grouping by trip id.
    stop_times_df['timediff'] = stop_times_df.groupby('unique_trip_id')['departure_time_sec_interpolate'].diff()
    log('Difference between stop times has been successfully calculated. Took {:,.2f} seconds'.format(time.time()-start_time))

    return stop_times_df

def timeselector(df=None,starttime=None,endtime=None):
    """
    Select stop times that fall within a specified time range

    Parameters
    ----------
    df : pandas.DataFrame
        interpolated stop times dataframe
    starttime : str
        24 hour clock formatted time 1
    endtime : str
        24 hour clock formatted time 2
    Returns
    -------
    selected_stop_timesdf : pandas.DataFrame

    """
    start_time = time.time()

    # takes input start and end time range from 24 hour clock and converts it to seconds past midnight
    # in order to select times that may be after midnight

    # convert string time components to integer and then calculate seconds past midnight
    # convert starttime 24 hour to seconds past midnight
    start_h = int(str(starttime[0:2]))
    start_m = int(str(starttime[3:5]))
    start_s = int(str(starttime[6:8]))
    starttime_sec = (start_h * 60 * 60) + (start_m * 60) + start_s
    # convert endtime 24 hour to seconds past midnight
    end_h = int(str(endtime[0:2]))
    end_m = int(str(endtime[3:5]))
    end_s = int(str(endtime[6:8]))
    endtime_sec = (end_h * 60 * 60) + (end_m * 60) + end_s

    # create df of stops times that are within the requested range
    selected_stop_timesdf = df[(( starttime_sec < df["departure_time_sec_interpolate"]) & (df["departure_time_sec_interpolate"] < endtime_sec ))]

    log('Stop times from {} to {} successfully selected {:,} records out of {:,} total records ({:.2f} percent of total). Took {:,.2f} seconds'.format(starttime,endtime,len(selected_stop_timesdf),len(df),(len(selected_stop_timesdf) / len(df)) * 100,time.time()-start_time))

    return selected_stop_timesdf


def format_transit_net_edge(stop_times_df=None):
    """
    Format transit network data table to match the format required for edges
    in Pandana graph networks edges

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        interpolated stop times with travel time between stops for the subset
        time and day

    Returns
    -------
    merged_edge_df : pandas.DataFrame

    """
    start_time = time.time()

    log('Starting transformation process for {:,} '
        'total trips...'.format(len(stop_times_df['unique_trip_id'].unique())))

    # set columns for new df for data needed by pandana for edges
    merged_edge = []

    stop_times_df.sort_values(by=['unique_trip_id', 'stop_sequence'],
                              inplace=True)

    for trip, tmp_trip_df in stop_times_df.groupby(['unique_trip_id']):

        edge_df = pd.DataFrame({
            "node_id_from": tmp_trip_df['unique_stop_id'].iloc[:-1].values,
            "node_id_to": tmp_trip_df['unique_stop_id'].iloc[1:].values,
            "weight": tmp_trip_df['timediff'].iloc[1:].values,
            "unique_agency_id": tmp_trip_df['unique_agency_id'].iloc[1:].values,
            # set unique trip id without edge order to join other data later
            "unique_trip_id": trip
        })

        # Set current trip id to edge id column adding edge order at
        # end of string
        edge_df['sequence'] = (edge_df.index+1).astype(int)

        # append completed formatted edge table to master edge table
        merged_edge.append(edge_df)

    merged_edge_df = pd.concat(merged_edge, ignore_index=True)
    merged_edge_df['sequence'] = merged_edge_df['sequence'].astype(int, copy=False)
    merged_edge_df['id'] = merged_edge_df[['unique_trip_id', 'sequence']].apply(lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

    log('stop time table transformation to '
        'Pandana format edge table completed. '
        'Took {:,.2f} seconds'.format(time.time()-start_time))

    return merged_edge_df


def convert_imp_time_units(df=None,time_col='weight',convert_to='minutes'):
    """
    Convert the travel time impedance units

    Parameters
    ----------
    df : pandas.DataFrame
        edge dataframe with weight column
    time_col : str
        name of column that holds the travel impedance
    convert_to : {'seconds','minutes'}
        unit to convert travel time to. should always be set to 'minutes'

    Returns
    -------
    df : pandas.DataFrame

    """
    valid_convert_to = ['seconds','minutes']
    assert convert_to in valid_convert_to and isinstance(convert_to,str)

    if convert_to == 'seconds':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] * 60
        log('Time conversion completed: minutes converted to seconds.')

    if convert_to == 'minutes':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] / 60.0
        log('Time conversion completed: seconds converted to minutes.')

    return df

def stops_in_edge_table_selector(input_stops_df=None,input_stop_times_df=None):
    """
    Select stops that are active during the day and time period specified

    Parameters
    ----------
    input_stops_df : pandas.DataFrame
        stops dataframe
    input_stop_times_df : pandas.DataFrame
        stop_times dataframe

    Returns
    -------
    selected_stops_df : pandas.DataFrame

    """
    start_time = time.time()

    # add unique stop id
    input_stops_df['unique_stop_id'] = input_stops_df[['stop_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

    #Select stop ids that match stop ids in the subset stop time data that match day and time selection
    selected_stops_df = input_stops_df.loc[input_stops_df['unique_stop_id'].isin(input_stop_times_df['unique_stop_id'])]

    log('{} of {} records selected from stops. Took {:,.2f} seconds'.format(len(selected_stops_df),len(input_stops_df),time.time()-start_time))

    return selected_stops_df

def format_transit_net_nodes(df=None):
    """
    Create transit node table from stops dataframe and perform final formatting

    Parameters
    ----------
    df : pandas.DataFrame
        transit node dataframe

    Returns
    -------
    final_node_df : pandas.DataFrame

    """
    start_time = time.time()

    # add unique stop id
    if 'unique_stop_id' not in df.columns:
        df['unique_stop_id'] = df[['stop_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

    final_node_df = pd.DataFrame()
    final_node_df['node_id'] = df['unique_stop_id']
    final_node_df['x'] = df['stop_lon']
    final_node_df['y'] = df['stop_lat']

    # keep useful info from stops table
    col_list = ['unique_agency_id','route_type','stop_id','stop_name']
    # if these optional cols exist then keep those that do
    optional_gtfs_cols = ['parent_station', 'stop_code', 'wheelchair_boarding', 'zone_id','location_type']
    for item in optional_gtfs_cols:
       if item in df.columns:
            col_list.append(item)

    final_node_df = pd.concat([final_node_df, df[col_list]], axis=1)
    # set node index to be unique stop id
    final_node_df = final_node_df.set_index('node_id')

    log('stop time table transformation to Pandana format node table completed. Took {:,.2f} seconds'.format(time.time()-start_time))

    return final_node_df

def route_type_to_edge(transit_edge_df=None,stop_time_df=None):
    """
    Append route type information to transit edge table

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge dataframe
    stop_time_df : pandas.DataFrame
        stop time dataframe

    Returns
    -------
    transit_edge_df_w_routetype : pandas.DataFrame

    """
    start_time = time.time()

    # create unique trip ids
    stop_time_df['unique_trip_id'] = stop_time_df[['trip_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

    #join route_id to the edge table
    merged_df = pd.merge(transit_edge_df, stop_time_df[['unique_trip_id','route_type']], how='left', on='unique_trip_id', sort=False, copy=False)
    merged_df.drop_duplicates(subset='unique_trip_id',keep='first',inplace=True)#need to get unique records here to have a one to one join - this serves as the look up table
    #join the look up table created above to the table of interest
    transit_edge_df_w_routetype = pd.merge(transit_edge_df, merged_df[['route_type','unique_trip_id']], how='left', on='unique_trip_id', sort=False, copy=False)

    log('route type successfully joined to transit edges. Took {:,.2f} seconds'.format(time.time()-start_time))

    return transit_edge_df_w_routetype

def route_id_to_edge(transit_edge_df=None,trips_df=None):
    """
    Append route ids to transit edge table

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge dataframe
    trips_df : pandas.DataFrame
        trips dataframe

    Returns
    -------
    transit_edge_df_with_routes : pandas.DataFrame

    """
    start_time = time.time()

    if 'unique_route_id' not in transit_edge_df.columns:

        # create unique trip and route ids
        trips_df['unique_trip_id'] = trips_df[['trip_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)
        trips_df['unique_route_id'] = trips_df[['route_id','unique_agency_id']].apply(lambda x : '{}_{}'.format(x[0],x[1]), axis=1)

        transit_edge_df_with_routes = pd.merge(transit_edge_df, trips_df[['unique_trip_id','unique_route_id']],
                                                how='left',
                                                on = 'unique_trip_id', sort=False, copy=False)

    log('route id successfully joined to transit edges. Took {:,.2f} seconds'.format(time.time()-start_time))

    return transit_edge_df_with_routes

def edge_impedance_by_route_type(transit_edge_df=None,
                                 street_level_rail=None,
                                 underground_rail=None,
                                 intercity_rail=None,
                                 bus=None,
                                 ferry=None,
                                 cable_car=None,
                                 gondola=None,
                                 funicular=None):
    """
    Penalize transit edge travel time based on transit mode type

    Parameters
    ----------
    transit_edge_df : pandas.DataFrame
        transit edge dataframe
    street_level_rail : float
        factor between -1 to 1 to multiply against travel time
    intercity_rail : float
        factor between -1 to 1 to multiply against travel time
    bus : float
        factor between -1 to 1 to multiply against travel time
    ferry : float
        factor between -1 to 1 to multiply against travel time
    cable_car : float
        factor between -1 to 1 to multiply against travel time
    gondola : float
        factor between -1 to 1 to multiply against travel time
    funicular : float
        factor between -1 to 1 to multiply against travel time

    Returns
    -------
    ua_network : object
    ua_network.transit_edges : pandas.DataFrame

    """
    assert 'route_type' in transit_edge_df.columns, 'No route_type column was found in dataframe'

    # check count of records for each route type
    route_type_desc = {0:'Street Level Rail: Tram Streetcar Light rail',1:'Underground rail: Subway or Metro',
                       2:'Rail: intercity or long-distance ',3:'Bus',4:'Ferry',5:'Cable Car',
                       6:'Gondola or Suspended cable car',7:'Steep incline: Funicular'}
    log('Route type distribution as percentage of '
        'transit mode: {:.2f}'.format(transit_edge_df['route_type'].map(route_type_desc.get).value_counts(normalize=True,dropna=False)*100))

    var_list = [street_level_rail,underground_rail,intercity_rail,bus,ferry,cable_car,gondola,funicular]

    for var in var_list:
        if var is not None:
            assert isinstance(var,float), 'One or more variables are not float'

    travel_time_col_name='weight'
    travel_time_col = transit_edge_df[travel_time_col_name]

    if street_level_rail is not None and len(transit_edge_df[transit_edge_df['route_type'] == 0]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==0] = travel_time_col+(travel_time_col*street_level_rail)
        log('Adjusted Street Level Rail transit edge impedance based on mode type penalty coefficient: {}'.format(street_level_rail))
    if underground_rail is not None and len(transit_edge_df[transit_edge_df['route_type'] == 1]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==1] = travel_time_col+(travel_time_col*underground_rail)
        log('Adjusted Underground rail transit edge impedance based on mode type penalty coefficient: {}'.format(underground_rail))
    if intercity_rail is not None and len(transit_edge_df[transit_edge_df['route_type'] == 2]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==2] = travel_time_col+(travel_time_col*intercity_rail)
        log('Adjusted Rail transit edge impedance based on mode type penalty coefficient: {}'.format(intercity_rail))
    if bus is not None and len(transit_edge_df[transit_edge_df['route_type'] == 3]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==3] = travel_time_col+(travel_time_col*bus)
        log('Adjusted Bus transit edge impedance based on mode type penalty coefficient: {}'.format(bus))
    if ferry is not None and len(transit_edge_df[transit_edge_df['route_type'] == 4]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==4] = travel_time_col+(travel_time_col*ferry)
        log('Adjusted Ferry transit edge impedance based on mode type penalty coefficient: {}'.format(ferry))
    if cable_car is not None and len(transit_edge_df[transit_edge_df['route_type'] == 5]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==5] = travel_time_col+(travel_time_col*cable_car)
        log('Adjusted Cable Car transit edge impedance based on mode type penalty coefficient: {}'.format(cable_car))
    if gondola is not None and len(transit_edge_df[transit_edge_df['route_type'] == 6]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==6] = travel_time_col+(travel_time_col*gondola)
        log('Adjusted Gondola or Suspended cable car transit edge impedance based on mode type penalty coefficient: {}'.format(gondola))
    if funicular is not None and len(transit_edge_df[transit_edge_df['route_type'] == 7]) > 0:
        transit_edge_df[travel_time_col_name][transit_edge_df['route_type']==7] = travel_time_col+(travel_time_col*funicular)
        log('Adjusted Funicular transit edge impedance based on mode type penalty coefficient: {}'.format(funicular))

    ua_network.transit_edges = transit_edge_df

    log('Transit edge impedance mode type penalty calculation complete')

    return ua_network

def save_processed_gtfs_data(gtfsfeeds_df=None,
                             dir=config.settings.data_folder,
                             filename=None):
    """
    Write dataframes in a gtfsfeeds_df object to a hdf5 file

    Parameters
    ----------
    gtfsfeeds_df : object
        gtfsfeeds_df object
    dir : string, optional
        directory to save hdf5 file
    filename : string
        name of the hdf5 file to save with .h5 extension

    Returns
    -------
    None
    """
    assert gtfsfeeds_df is not None \
           or gtfsfeeds_df.stops.empty == False \
           or gtfsfeeds_df.routes.empty == False \
           or gtfsfeeds_df.trips.empty == False \
           or gtfsfeeds_df.stop_times.empty == False \
           or gtfsfeeds_df.calendar.empty == False \
           or gtfsfeeds_df.stop_times_int.empty == False, 'gtfsfeeds_df is missing one of the required dataframes.'

    df_to_hdf5(data=gtfsfeeds_df.stops,key='stops',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_df.routes,key='routes',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_df.trips,key='trips',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_df.stop_times,key='stop_times',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_df.calendar,key='calendar',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_df.stop_times_int,key='stop_times_int',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)

    if gtfsfeeds_df.headways.empty == False:
        df_to_hdf5(data=gtfsfeeds_df.headways,key='headways',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)

    if gtfsfeeds_df.calendar_dates.empty == False:
        df_to_hdf5(data=gtfsfeeds_df.calendar_dates,key='calendar_dates',overwrite_key=False,dir=dir,filename=filename,overwrite_hdf5=False)

def load_processed_gtfs_data(dir=config.settings.data_folder,filename=None):
    """
    Read data from a hdf5 file to a gtfsfeeds_df object

    Parameters
    ----------
    dir : string, optional
        directory to read hdf5 file
    filename : string
        name of the hdf5 file to read with .h5 extension

    Returns
    -------
    gtfsfeeds_df : object
    """
    gtfsfeeds_df.stops = hdf5_to_df(dir=dir,filename=filename,key='stops')
    gtfsfeeds_df.routes = hdf5_to_df(dir=dir,filename=filename,key='routes')
    gtfsfeeds_df.trips = hdf5_to_df(dir=dir,filename=filename,key='trips')
    gtfsfeeds_df.stop_times = hdf5_to_df(dir=dir,filename=filename,key='stop_times')
    gtfsfeeds_df.calendar = hdf5_to_df(dir=dir,filename=filename,key='calendar')
    gtfsfeeds_df.stop_times_int = hdf5_to_df(dir=dir,filename=filename,key='stop_times_int')

    hdf5_load_path = '{}/{}'.format(dir,filename)
    with pd.HDFStore(hdf5_load_path) as store:

            if 'headways' in store.keys():
                gtfsfeeds_df.headways = hdf5_to_df(dir=dir,filename=filename,key='headways')
            if 'calendar_dates' in store.keys():
                gtfsfeeds_df.calendar_dates = hdf5_to_df(dir=dir,filename=filename,key='calendar_dates')

    return gtfsfeeds_df

# helper functions
def _check_if_index_name_in_cols(df):
    cols = df.columns.values
    iname = df.index.name
    return (iname in cols)
