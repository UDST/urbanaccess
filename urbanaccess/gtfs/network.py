import logging as lg
import time

import pandas as pd

from urbanaccess import config
from urbanaccess.gtfs.gtfsfeeds_dataframe import gtfsfeeds_dfs
from urbanaccess.gtfs.utils.gtfs_format import (_time_selector)
from urbanaccess.network import ua_network
from urbanaccess.utils import log, df_to_hdf5, hdf5_to_df
from .synthesize import convert_transit_data_to_network

# Note: The above imported logging funcs were modified from the OSMnx library
#       & used with permission from the author Geoff Boeing: log, get_logger
#       OSMnx repo: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

pd.options.mode.chained_assignment = None


def create_transit_net(gtfsfeeds_dfs, day,
                       calendar_dates_lookup=None,
                       timerange=None,
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
    gtfsfeeds_dfs : object
        gtfsfeeds_dfs object with dataframes of stops, routes, trips,
        stop_times, calendar, and stop_times_int (optional)
    day : {'friday','monday','saturday', 'sunday','thursday','tuesday',
    'wednesday'}
        day of the week to extract transit schedule from that
        corresponds to the day in the GTFS calendar
    calendar_dates_lookup : dict, optional
        dictionary of the lookup column (key) as a string and corresponding
        string (value) a s string or list of strings to use to subset trips
        using the calendar_dates dataframe. Search will be exact. If none,
        then the calendar_dates dataframe will not be used to select trips
        that are not in the calendar dataframe.
        Example: {'schedule_type' : 'WD'} or {'schedule_type' : ['WD','SU']}
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
        dataframe stored in the gtfsfeeds_dfs object it will be
        overwritten
    use_existing_stop_times_int : bool
        if true, and if there is an existing stop_times_int
        dataframe for the same time period stored in the
        gtfsfeeds_dfs object it will be used instead of re-calculated
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

    # Removing support for the following args:
    #   calendar_dates_lookup, overwrite_existing_stop_times_int,
    #   use_existing_stop_times_int, save_processed_gtfs
    #   save_dir, save_filename

    # This now becomes a wrapper around the new version of func
    # and requires modification of the input df format
    gtfs_dict = {
        'stops': gtfsfeeds_dfs.stops,
        'routes': gtfsfeeds_dfs.routes,
        'trips': gtfsfeeds_dfs.trips,
        'stop_times': gtfsfeeds_dfs.stop_times,
        'calendar': gtfsfeeds_dfs.calendar,
        'calendar_dates': gtfsfeeds_dfs.calendar_dates,
        'stop_times_int': gtfsfeeds_dfs.stop_times_int,
        'headways': gtfsfeeds_dfs.headways
    }
    trans_net = convert_transit_data_to_network(gtfs_dict, day, timerange)

    # assign node and edge net type
    ua_network.transit_nodes = trans_net['nodes']
    ua_network.transit_edges = trans_net['edges']

    # closing message logs
    fin_time = time.time() - start_time
    success_msg = ('Successfully created transit network. Took '
                   '{:,.2f} seconds').format(fin_time)
    log(success_msg)

    return ua_network


def _trip_schedule_selector(input_trips_df, input_calendar_df,
                            input_calendar_dates_df, day,
                            calendar_dates_lookup=None):
    """
    TODO: Deprecate this function

    Select trips that run on a specific day

    Parameters
    ----------
    input_trips_df : pandas.DataFrame
        trips dataframe
    input_calendar_df : pandas.DataFrame
        calendar dataframe
    input_calendar_dates_df : pandas.DataFrame
        calendar_dates dataframe
    day : {'friday','monday','saturday','sunday','thursday','tuesday',
    'wednesday'}
        day of the week to extract transit schedule that corresponds to the
        day in the GTFS calendar
    calendar_dates_lookup : dict, optional
        dictionary of the lookup column (key) as a string and corresponding
        string (value) a s string or list of strings to use to subset trips
        using the calendar_dates dataframe. Search will be exact. If none,
        then the calendar_dates dataframe will not be used to select trips
        that are not in the calendar dataframe.
        Example: {'schedule_type' : 'WD'} or {'schedule_type' : ['WD','SU']}

    Returns
    -------
    calendar_selected_trips_df : pandas.DataFrame

    """
    start_time = time.time()

    valid_days = ['friday', 'monday', 'saturday', 'sunday',
                  'thursday', 'tuesday', 'wednesday']

    if day not in valid_days:
        raise ValueError('Incorrect day specified. Must be one of lowercase '
                         'strings: friday, monday, saturday, sunday, '
                         'thursday, tuesday, wednesday.')

    # check format of calendar_dates_lookup
    if calendar_dates_lookup is not None:
        if not isinstance(calendar_dates_lookup, dict):
            raise ValueError('calendar_dates_lookup parameter is not a dict')
        for key in calendar_dates_lookup.keys():
            if not isinstance(key, str):
                raise ValueError('calendar_dates_lookup key {} must be a '
                                 'string'.format(key))

            if isinstance(calendar_dates_lookup[key], str):
                value = [calendar_dates_lookup[key]]
            else:
                if not isinstance(calendar_dates_lookup[key], list):
                    raise ValueError(
                        'calendar_dates_lookup value {} must be a string or a '
                        'list of strings'.format(
                            calendar_dates_lookup[key]))
                else:
                    value = calendar_dates_lookup[key]

            for string in value:
                if not isinstance(string, str):
                    raise ValueError('{} must be a string'.format(value))

    # create unique service ids
    df_list = [input_trips_df, input_calendar_df, input_calendar_dates_df]

    for df in df_list:
        # make sure that both components are strings
        uagcy_id = df['unique_agency_id'].astype(str)
        serv_id = df['service_id'].astype(str)
        # concatenate them to create a service uid
        df['unique_service_id'] = (serv_id.str.cat(uagcy_id, sep='_'))

    # select service ids where day specified has 1 = service runs on that day
    log('Using calendar to extract service_ids to select trips.')
    input_calendar_df = input_calendar_df[(input_calendar_df[day] == 1)]
    input_calendar_df = input_calendar_df[['unique_service_id']]
    log('{:,} service_ids were extracted from calendar'.format(len(
        input_calendar_df)))

    # generate information needed to tell user the status of their trips in
    # terms of service_ids in calendar and calendar_dates tables
    trips_in_calendar = input_trips_df.loc[input_trips_df[
        'unique_service_id'].isin(
        input_calendar_df['unique_service_id'])]
    trips_notin_calendar = input_trips_df.loc[~input_trips_df[
        'unique_service_id'].isin(input_calendar_df['unique_service_id'])]

    assert len(input_trips_df) > 0
    cal_per_input = len(trips_in_calendar) / len(input_trips_df) * 100
    pct_trips_in_calendar = round(cal_per_input, 3)
    pct_trips_notin_calendar = round(len(trips_notin_calendar) / len(
        input_trips_df) * 100, 3)

    msg = ('{:,} trip(s) {:.3f} percent of {:,} total trip records were '
           'found in calendar').format(len(trips_in_calendar),
                                       pct_trips_in_calendar,
                                       len(input_trips_df))
    log(msg)

    if len(trips_notin_calendar) > 0 and calendar_dates_lookup is None:
        warning_msg = (
            'NOTE: {:,} trip(s) {:.3f} percent of {:,} total trip records '
            'were found to be in calendar_dates and not calendar. The '
            'calendar_dates_lookup parameter is None. In order to use the '
            'trips inside the calendar_dates dataframe it is suggested you '
            'specify a search parameter in the calendar_dates_lookup dict. '
            'This should only be done if you know the corresponding '
            'GTFS feed is using calendar_dates instead of calendar to '
            'specify the service_ids. When in doubt do not use the '
            'calendar_dates_lookup parameter.')
        log(warning_msg.format(
            len(trips_notin_calendar),
            pct_trips_notin_calendar,
            len(input_trips_df)), level=lg.WARNING)

    # look for service_ids inside of calendar_dates if calendar does not
    # supply enough service_ids to select trips by
    if len(trips_notin_calendar) > 0 and calendar_dates_lookup is not None:

        log('Using calendar_dates to supplement service_ids extracted from '
            'calendar to select trips.')

        subset_result_df = pd.DataFrame()

        for col_name_key, string_value in calendar_dates_lookup.items():
            if col_name_key not in input_calendar_dates_df.columns:
                raise ValueError(('{} column not found in calendar_dates '
                                  'dataframe').format(col_name_key))

            if col_name_key not in input_calendar_dates_df.select_dtypes(
                    include=[object]).columns:
                raise ValueError('{} column is not object type'.format(
                    col_name_key))

            if not isinstance(string_value, list):
                string_value = [string_value]

            for text in string_value:

                subset_result = input_calendar_dates_df[
                    input_calendar_dates_df[col_name_key].str.match(
                        text, case=False, na=False)]
                log(('Found {:,} records that matched query: column: {} and '
                     'string: {}').format(len(subset_result),
                                          col_name_key,
                                          text))

                subset_result_df = subset_result_df.append(subset_result)

        subset_result_df.drop_duplicates(inplace=True)
        subset_result_df = subset_result_df[['unique_service_id']]

        log('{:,} service_ids were extracted from calendar_dates'.format(len(
            subset_result_df)))
        input_calendar_df = input_calendar_df.append(subset_result_df)
        input_calendar_df.drop_duplicates(inplace=True)

    # select and create df of trips that match the service ids for the day of
    # the week specified merge calendar df that has service ids for
    # specified day with trips df
    calendar_selected_trips_df = input_trips_df.loc[
        input_trips_df['unique_service_id'].isin(
            input_calendar_df['unique_service_id'])]

    sort_columns = ['route_id', 'trip_id', 'direction_id']
    if 'direction_id' not in calendar_selected_trips_df.columns:
        sort_columns.remove('direction_id')
    calendar_selected_trips_df.sort_values(by=sort_columns, inplace=True)
    calendar_selected_trips_df.reset_index(drop=True, inplace=True)
    calendar_selected_trips_df.drop('unique_service_id', axis=1, inplace=True)

    if calendar_dates_lookup is None:
        log(('{:,} of {:,} total trips were extracted representing calendar '
             'day: {}. Took {:,.2f} seconds').format(len(
            calendar_selected_trips_df), len(input_trips_df),
            day, time.time() - start_time))
    else:
        log(('{:,} of {:,} total trips were extracted representing calendar '
             'day: {} and calendar_dates search parameters: {}. Took {:,'
             '.2f} seconds').format(len(
            calendar_selected_trips_df), len(input_trips_df),
            day, calendar_dates_lookup, time.time() - start_time))

    return calendar_selected_trips_df


def _time_selector(df=None, starttime=None, endtime=None):
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
    assert len(df) > 0
    
    # TODO: Deprecated, should not be referenced anymore
    start_time = time.time()

    selected_stop_timesdf = _time_selector(df, starttime, endtime)


    log(
        'Stop times from {} to {} successfully selected {:,} records out of '
        '{:,} total records ({:.2f} percent of total). Took {:,'
        '.2f} seconds'.format(
            starttime, endtime, len(selected_stop_timesdf), len(df),
            (len(selected_stop_timesdf) / len(df)) * 100,
            time.time() - start_time))

    return selected_stop_timesdf


def _format_transit_net_edge(stop_times_df=None):
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
            "unique_agency_id": tmp_trip_df['unique_agency_id'].iloc[
                                1:].values,
            # set unique trip id without edge order to join other data later
            "unique_trip_id": trip
        })

        # Set current trip id to edge id column adding edge order at
        # end of string
        edge_df['sequence'] = (edge_df.index + 1).astype(int)

        # append completed formatted edge table to master edge table
        merged_edge.append(edge_df)

    merged_edge_df = pd.concat(merged_edge, ignore_index=True)
    merged_edge_df['sequence'] = merged_edge_df['sequence'].astype(int,
                                                                   copy=False)
    merged_edge_df['id'] = merged_edge_df[
        ['unique_trip_id', 'sequence']].apply(
        lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

    log('stop time table transformation to '
        'Pandana format edge table completed. '
        'Took {:,.2f} seconds'.format(time.time() - start_time))

    return merged_edge_df


def _convert_imp_time_units(df=None, time_col='weight', convert_to='minutes'):
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
    valid_convert_to = ['seconds', 'minutes']
    assert convert_to in valid_convert_to and isinstance(convert_to, str)

    if convert_to == 'seconds':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] * 60
        log('Time conversion completed: minutes converted to seconds.')

    if convert_to == 'minutes':
        df[time_col] = df[time_col].astype('float')
        df[time_col] = df[time_col] / 60.0
        log('Time conversion completed: seconds converted to minutes.')

    return df


def _stops_in_edge_table_selector(input_stops_df=None,
                                  input_stop_times_df=None):
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
    input_stops_df['unique_stop_id'] = input_stops_df[
        ['stop_id', 'unique_agency_id']].apply(
        lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

    # Select stop ids that match stop ids in the subset stop time data that
    # match day and time selection
    selected_stops_df = input_stops_df.loc[
        input_stops_df['unique_stop_id'].isin(
            input_stop_times_df['unique_stop_id'])]

    log(
        '{:,} of {:,} records selected from stops. Took {:,'
        '.2f} seconds'.format(
            len(selected_stops_df), len(input_stops_df),
            time.time() - start_time))

    return selected_stops_df


def _format_transit_net_nodes(df=None):
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
        df['unique_stop_id'] = df[['stop_id', 'unique_agency_id']].apply(
            lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

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
    # set node index to be unique stop id
    final_node_df = final_node_df.set_index('node_id')

    log(
        'stop time table transformation to Pandana format node table '
        'completed. Took {:,.2f} seconds'.format(
            time.time() - start_time))

    return final_node_df


def _route_type_to_edge(transit_edge_df=None, stop_time_df=None):
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
    stop_time_df['unique_trip_id'] = stop_time_df[
        ['trip_id', 'unique_agency_id']].apply(
        lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

    # join route_id to the edge table
    merged_df = pd.merge(transit_edge_df,
                         stop_time_df[['unique_trip_id', 'route_type']],
                         how='left', on='unique_trip_id', sort=False,
                         copy=False)
    merged_df.drop_duplicates(subset='unique_trip_id', keep='first', inplace
    =True)  # need to get unique records here to have a one to one join -
    # this serves as the look up table
    # join the look up table created above to the table of interest
    transit_edge_df_w_routetype = pd.merge(transit_edge_df, merged_df[
        ['route_type', 'unique_trip_id']], how='left', on='unique_trip_id',
                                           sort=False, copy=False)

    log(
        'route type successfully joined to transit edges. Took {:,'
        '.2f} seconds'.format(
            time.time() - start_time))

    return transit_edge_df_w_routetype


def _route_id_to_edge(transit_edge_df=None, trips_df=None):
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
        trips_df['unique_trip_id'] = trips_df[
            ['trip_id', 'unique_agency_id']].apply(
            lambda x: '{}_{}'.format(x[0], x[1]), axis=1)
        trips_df['unique_route_id'] = trips_df[
            ['route_id', 'unique_agency_id']].apply(
            lambda x: '{}_{}'.format(x[0], x[1]), axis=1)

        transit_edge_df_with_routes = pd.merge(transit_edge_df, trips_df[
            ['unique_trip_id', 'unique_route_id']],
                                               how='left',
                                               on='unique_trip_id', sort=False,
                                               copy=False)

    log(
        'route id successfully joined to transit edges. Took {:,'
        '.2f} seconds'.format(
            time.time() - start_time))

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
    assert 'route_type' in transit_edge_df.columns, 'No route_type column ' \
                                                    'was found in dataframe'

    # check count of records for each route type
    route_type_desc = {0: 'Street Level Rail: Tram Streetcar Light rail',
                       1: 'Underground rail: Subway or Metro',
                       2: 'Rail: intercity or long-distance ', 3: 'Bus',
                       4: 'Ferry', 5: 'Cable Car',
                       6: 'Gondola or Suspended cable car',
                       7: 'Steep incline: Funicular'}
    log('Route type distribution as percentage of '
        'transit mode: {:.2f}'.format(
        transit_edge_df['route_type'].map(route_type_desc.get).value_counts(
            normalize=True, dropna=False) * 100))

    var_list = [street_level_rail, underground_rail, intercity_rail, bus,
                ferry, cable_car, gondola, funicular]

    for var in var_list:
        if var is not None:
            assert isinstance(var,
                              float), 'One or more variables are not float'

    travel_time_col_name = 'weight'
    travel_time_col = transit_edge_df[travel_time_col_name]

    if street_level_rail is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 0]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 0] = travel_time_col + (
        travel_time_col * street_level_rail)
        log(
            'Adjusted Street Level Rail transit edge impedance based on mode'
            ' type penalty coefficient: {}'.format(
                street_level_rail))
    if underground_rail is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 1]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 1] = travel_time_col + (
        travel_time_col * underground_rail)
        log(
            'Adjusted Underground rail transit edge impedance based on mode '
            'type penalty coefficient: {}'.format(
                underground_rail))
    if intercity_rail is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 2]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 2] = travel_time_col + (
        travel_time_col * intercity_rail)
        log(
            'Adjusted Rail transit edge impedance based on mode type penalty '
            'coefficient: {}'.format(
                intercity_rail))
    if bus is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 3]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 3] = travel_time_col + (
        travel_time_col * bus)
        log(
            'Adjusted Bus transit edge impedance based on mode type penalty '
            'coefficient: {}'.format(
                bus))
    if ferry is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 4]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 4] = travel_time_col + (
        travel_time_col * ferry)
        log(
            'Adjusted Ferry transit edge impedance based on mode type '
            'penalty coefficient: {}'.format(
                ferry))
    if cable_car is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 5]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 5] = travel_time_col + (
        travel_time_col * cable_car)
        log(
            'Adjusted Cable Car transit edge impedance based on mode type '
            'penalty coefficient: {}'.format(
                cable_car))
    if gondola is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 6]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 6] = travel_time_col + (
        travel_time_col * gondola)
        log(
            'Adjusted Gondola or Suspended cable car transit edge impedance '
            'based on mode type penalty coefficient: {}'.format(
                gondola))
    if funicular is not None and len(
            transit_edge_df[transit_edge_df['route_type'] == 7]) > 0:
        transit_edge_df[travel_time_col_name][
            transit_edge_df['route_type'] == 7] = travel_time_col + (
        travel_time_col * funicular)
        log(
            'Adjusted Funicular transit edge impedance based on mode type '
            'penalty coefficient: {}'.format(
                funicular))

    ua_network.transit_edges = transit_edge_df

    log('Transit edge impedance mode type penalty calculation complete')

    return ua_network


def save_processed_gtfs_data(gtfsfeeds_dfs=None,
                             dir=config.settings.data_folder,
                             filename=None):
    """
    Write dataframes in a gtfsfeeds_dfs object to a hdf5 file

    Parameters
    ----------
    gtfsfeeds_dfs : object
        gtfsfeeds_dfs object
    dir : string, optional
        directory to save hdf5 file
    filename : string
        name of the hdf5 file to save with .h5 extension

    Returns
    -------
    None
    """
    
    assert gtfsfeeds_dfs is not None \
           or gtfsfeeds_dfs.stops.empty == False \
           or gtfsfeeds_dfs.routes.empty == False \
           or gtfsfeeds_dfs.trips.empty == False \
           or gtfsfeeds_dfs.stop_times.empty == False \
           or gtfsfeeds_dfs.calendar.empty == False \
           or gtfsfeeds_dfs.stop_times_int.empty == False, \
                                                    'gtfsfeeds_dfs is ' \
                                                    '' \
                                                    'missing one of ' \
                                                    'the required ' \
                                                    'dataframes.'

    df_to_hdf5(data=gtfsfeeds_dfs.stops, key='stops', overwrite_key=False,
               dir=dir, filename=filename, overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_dfs.routes, key='routes', overwrite_key=False,
               dir=dir, filename=filename, overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_dfs.trips, key='trips', overwrite_key=False,
               dir=dir, filename=filename, overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_dfs.stop_times, key='stop_times',
               overwrite_key=False, dir=dir, filename=filename,
               overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_dfs.calendar, key='calendar',
               overwrite_key=False, dir=dir, filename=filename,
               overwrite_hdf5=False)
    df_to_hdf5(data=gtfsfeeds_dfs.stop_times_int, key='stop_times_int',
               overwrite_key=False, dir=dir, filename=filename,
               overwrite_hdf5=False)

    if gtfsfeeds_dfs.headways.empty == False:
        df_to_hdf5(data=gtfsfeeds_dfs.headways, key='headways',
                   overwrite_key=False, dir=dir, filename=filename,
                   overwrite_hdf5=False)

    if gtfsfeeds_dfs.calendar_dates.empty == False:
        df_to_hdf5(data=gtfsfeeds_dfs.calendar_dates, key='calendar_dates',
                   overwrite_key=False, dir=dir, filename=filename,
                   overwrite_hdf5=False)


def load_processed_gtfs_data(dir=config.settings.data_folder, filename=None):
    """
    Read data from a hdf5 file to a gtfsfeeds_dfs object

    Parameters
    ----------
    dir : string, optional
        directory to read hdf5 file
    filename : string
        name of the hdf5 file to read with .h5 extension

    Returns
    -------
    gtfsfeeds_dfs : object
    """
    gtfsfeeds_dfs.stops = hdf5_to_df(dir=dir, filename=filename, key='stops')
    gtfsfeeds_dfs.routes = hdf5_to_df(dir=dir, filename=filename, key='routes')
    gtfsfeeds_dfs.trips = hdf5_to_df(dir=dir, filename=filename, key='trips')
    gtfsfeeds_dfs.stop_times = hdf5_to_df(dir=dir, filename=filename,
                                          key='stop_times')
    gtfsfeeds_dfs.calendar = hdf5_to_df(dir=dir, filename=filename,
                                        key='calendar')
    gtfsfeeds_dfs.stop_times_int = hdf5_to_df(dir=dir, filename=filename,
                                              key='stop_times_int')

    hdf5_load_path = '{}/{}'.format(dir, filename)
    with pd.HDFStore(hdf5_load_path) as store:

        if 'headways' in store.keys():
            gtfsfeeds_dfs.headways = hdf5_to_df(dir=dir,
                                                filename=filename,
                                                key='headways')
        if 'calendar_dates' in store.keys():
            gtfsfeeds_dfs.calendar_dates = hdf5_to_df(dir=dir,
                                                      filename=filename,
                                                      key='calendar_dates')

    return gtfsfeeds_dfs
