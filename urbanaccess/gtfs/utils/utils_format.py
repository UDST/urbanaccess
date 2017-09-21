import os
import os
import time
from re import sub

import numpy as np
import pandas as pd
from future.utils import raise_with_traceback

from urbanaccess.gtfs.utils.gtfs_format import (_calendar_agency_uids,
                                                _calendar_dates_agency_uids,
                                                _routes_agency_uids,
                                                _stop_times_agency_uids,
                                                _stops_agency_uids,
                                                _trips_agency_uids)
from urbanaccess.utils import log


def clean_gtfs_tables(gtfs_array):
    # the overall gtfs with the results from the first in list
    first_gtfs = gtfs_array.pop(0)
    all_gtfs_clean = _produce_cleaned_feed(first_gtfs)

    # now loop through and append all other feeds
    for gtfs in gtfs_array:
        gtfs_clean = _produce_cleaned_feed(gtfs)

        # append each of the cleaned results to the main return dict
        for key in gtfs_clean.keys():
            df1 = all_gtfs_clean[key]
            df2 = gtfs_clean[key]
            all_gtfs_clean[key] = df1.append(df2, ignore_index=True)

    return all_gtfs_clean


def _produce_cleaned_feed(gtfs):
    # these are the gtfs tables we will be keeping
    needed_files_from_gtfs = ['agency',
                              'stops',
                              'routes',
                              'trips',
                              'stop_times',
                              'calendar',
                              'calendar_dates']

    # base for return dictionary of various gtfs tables
    gtfs_clean = {}

    # a few cleaning steps for each of the required gtfs layers
    for topic in needed_files_from_gtfs:
        temp_df = gtfs[topic]

        # remove any extra whitespace in column names
        temp_df.rename(columns=lambda x: x.strip(), inplace=True)

        # add to new dict of cleaned tables
        gtfs_clean[topic] = temp_df

    # Note: with cleaned gtfs, there's a few table-specific custom
    #       modifications to the cleaning steps per specific table

    # 1. agency
    agency_cols_to_keep = ['agency_id', 'agency_name']
    gtfs_clean['agency'] = _produce_all_rows_df(gtfs_clean['agency'],
                                                agency_cols_to_keep)

    # 2. update stops
    #    make sure that lat and lon values are floats
    stops = gtfs_clean['stops']
    stops['stop_lat'] = pd.to_numeric(stops['stop_lat'])
    stops['stop_lon'] = pd.to_numeric(stops['stop_lon'])
    gtfs_clean['stops'] = stops

    stops_cols_to_keep = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon',
                          'agency_id', 'position', 'direction']
    gtfs_clean['stops'] = _produce_all_rows_df(gtfs_clean['stops'],
                                               stops_cols_to_keep)

    # 3. update routes
    routes_cols_to_keep = ['route_id', 'agency_id', 'route_short_name',
                           'route_long_name', 'route_type']
    gtfs_clean['routes'] = _produce_all_rows_df(gtfs_clean['routes'],
                                                routes_cols_to_keep)

    # 4. update trips
    trips = gtfs_clean['trips']
    recast = ['trip_id', 'service_id', 'route_id']
    for ea in recast:
        trips[ea] = trips[ea].astype(object)
    gtfs_clean['trips'] = trips

    trips_cols_to_keep = ['route_id', 'service_id', 'trip_id', 'trip_headsign',
                          'direction_id', 'direction_name', 'shape_id',
                          'shape_code', 'trip_type']
    gtfs_clean['trips'] = _produce_all_rows_df(gtfs_clean['trips'],
                                               trips_cols_to_keep)

    # 5. update stop_times
    stop_times = gtfs_clean['stop_times']
    recast = ['trip_id', 'stop_id', 'departure_time', 'arrival_time']
    for ea in recast:
        stop_times[ea] = stop_times[ea].astype(object)
    gtfs_clean['stop_times'] = stop_times

    stop_times_cols = ['trip_id', 'stop_sequence', 'stop_id',
                       'pickup_type', 'drop_off_type', 'arrival_time',
                       'departure_time', 'timepoint',
                       'shape_dist_traveled']
    gtfs_clean['stop_times'] = _produce_all_rows_df(gtfs_clean['stop_times'],
                                                    stop_times_cols)

    # 6. update calendar
    calendar = gtfs_clean['calendar']
    days = ['monday',
            'tuesday',
            'wednesday',
            'thursday',
            'friday',
            'saturday',
            'sunday']
    for day in days:
        calendar[day] = pd.to_numeric(calendar[day])
    gtfs_clean['calendar'] = calendar

    cal_cols_to_keep = ['service_id', 'service_name', 'monday', 'tuesday',
                        'wednesday', 'thursday', 'friday', 'saturday',
                        'sunday', 'start_date', 'end_date']
    gtfs_clean['calendar'] = _produce_all_rows_df(gtfs_clean['calendar'],
                                                  cal_cols_to_keep)

    # 7. update calendar dates (only if not empty)
    if not gtfs_clean['calendar_dates'].empty:
        cal_dates_cols = ['date', 'exception_type', 'service_id']
        clean_cal_dates = _produce_all_rows_df(gtfs_clean['calendar_dates'],
                                               cal_dates_cols)
        gtfs_clean['calendar_dates'] = clean_cal_dates

    return gtfs_clean


def _produce_all_rows_df(df, req_cols):
    # go through and make sure each column exists or, if not,
    # then is just filled with null values
    for col in req_cols:
        if col not in df.columns.values:
            df[col] = np.nan

    # now return a subset of the dataframe with just the requested cols
    return df[req_cols]


def _read_gtfs_agency(textfile_path=None, textfile=None):
    """
    Read gtfs agency.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    assert textfile == 'agency.txt'

    df = pd.read_csv(os.path.join(textfile_path, textfile))
    assert len(df) != 0, '{} has no records'.format(os.path.join(textfile_path, textfile))
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_stops(textfile_path=None, textfile=None):
    """
    Read gtfs stops.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    assert textfile == 'stops.txt'

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'stop_id': object})

    df['stop_lat'] = pd.to_numeric(df['stop_lat'])
    df['stop_lon'] = pd.to_numeric(df['stop_lon'])
    assert len(df) != 0, '{} has no records'.format(os.path.join(textfile_path, textfile))
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_routes(textfile_path=None, textfile=None):
    """
    Read gtfs routes.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    assert textfile == 'routes.txt'

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'route_id': object})
    assert len(df) != 0, '{} has no records'.format(os.path.join(textfile_path, textfile))
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_trips(textfile_path=None, textfile=None):
    """
    Read gtfs trips.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    assert textfile == 'trips.txt'

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'trip_id': object,
                            'service_id': object,
                            'route_id': object,
                            7: object})  # 7 is placeholder for shape id which may not exist in some txt files
    assert len(df) != 0, '{} has no records'.format(os.path.join(textfile_path, textfile))
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_stop_times(textfile_path=None, textfile=None):
    """
    Read stop_times.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    # TODO: If this textfile must be this name, then why have it as an arg?
    assert textfile == 'stop_times.txt'

    dtype_dict = {'trip_id': object,
                  'stop_id': object,
                  'departure_time': object,
                  'arrival_time': object}

    joined_filename = os.path.join(textfile_path, textfile)
    df = pd.read_csv(joined_filename, dtype=dtype_dict)
    assert len(df) != 0, '{} has no records'.format(joined_filename)
    
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    
    return df


def _read_gtfs_calendar(textfile_path=None, textfile=None):
    """
    Read gtfs calendar.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    # TODO: If this textfile must be this name, then why have it as an arg?
    assert textfile == 'calendar.txt'

    joined_filename = os.path.join(textfile_path, textfile)
    df = pd.read_csv(joined_filename, dtype={'service_id': object})
    
    columnlist = ['monday',
                  'tuesday',
                  'wednesday',
                  'thursday',
                  'friday',
                  'saturday',
                  'sunday']
    for col in columnlist:
        df[col] = pd.to_numeric(df[col])
    
    # make sure we didn't end up with an empty dataframe    
    assert len(df) != 0, '{} has no records'.format(joined_filename)
    
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    
    return df


def _read_gtfs_calendar_dates(textfile_path=None, textfile=None):
    """
    Read gtfs calendar_dates.txt as a pandas dataframe

    Parameters
    ----------
    textfile_path : str
        director of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    # TODO: If this textfile must be this name, then why have it as an arg?
    assert textfile == 'calendar_dates.txt'

    joined_filename = os.path.join(textfile_path, textfile)
    df = pd.read_csv(joined_filename, dtype={'service_id': object})

    # make sure we didn't end up with an empty dataframe
    assert len(df) != 0, '{} has no records'.format(joined_filename)
    
    
    # TODO: Make these done in a helper function
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    
    return df


def _calendar_dates_agencyid(calendar_dates_df=None, routes_df=None, trips_df=None, agency_df=None):
    """
    Assign unique agency id to calendar dates dataframe

    Parameters
    ----------
    calendar_dates_df : pandas:DataFrame
        calendar dates dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    trips_df : pandas:DataFrame
        trips dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    
    # create two temporary dataframes by merging two common df args
    tmp1 = pd.merge(routes_df,
                    agency_df,
                    how='left',
                    on='agency_id',
                    sort=False,
                    copy=False)
    tmp2 = pd.merge(trips_df,
                    tmp1,
                    how='left',
                    on='route_id',
                    sort=False,
                    copy=False)

    # now merge each of those two based on common cols
    merged_df = pd.merge(calendar_dates_df,
                         tmp2,
                         how='left',
                         on='service_id',
                         sort=False,
                         copy=False)

    # create the unique agency id for each and populate the new column
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    
    # remove any columns that might be repeated
    merged_df.drop_duplicates(subset='service_id',
                              keep='first',
                              inplace=True)

    # take resulting merged df and put back into the calendar df
    merged_df = pd.merge(calendar_dates_df,
                         merged_df[['unique_agency_id', 'service_id']],
                         how='left',
                         on='service_id',
                         sort=False,
                         copy=False)

    return merged_df


def _calendar_agencyid(calendar_df=None,
                       routes_df=None,
                       trips_df=None,
                       agency_df=None):
    """
    Assign unique agency id to calendar dataframe

    Parameters
    ----------
    calendar_df : pandas:DataFrame
        calendar dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    trips_df : pandas:DataFrame
        trips dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    
    tmp1 = pd.merge(routes_df,
                    agency_df,
                    how='left',
                    on='agency_id',
                    sort=False,
                    copy=False)
    
    tmp2 = pd.merge(trips_df,
                    tmp1,
                    how='left',
                    on='route_id',
                    sort=False,
                    copy=False)
    
    # do another merge to account for service ids that may not be utilized
    # across all GTFS files for accounting purposes so we keep those that
    # dont show up after merge
    merged_df = pd.merge(calendar_df[['service_id']],
                         tmp2,
                         how='left',
                         on='service_id',
                         sort=False,
                         copy=False)
    
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    
    merged_df.drop_duplicates(subset='service_id',
                              keep='first',
                              inplace=True)

    merged_df = pd.merge(calendar_df,
                         merged_df[['unique_agency_id',
                         'service_id']],
                         how='left',
                         on='service_id',
                         sort=False,
                         copy=False)

    return merged_df


def _trips_agencyid(trips_df=None, routes_df=None, agency_df=None):
    """
    Assign unique agency id to trips dataframe

    Parameters
    ----------
    trips_df : pandas:DataFrame
        trips dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    
    tmp1 = pd.merge(routes_df,
                    agency_df,
                    how='left',
                    on='agency_id',
                    sort=False,
                    copy=False)
    
    merged_df = pd.merge(trips_df[['trip_id', 'route_id']],
                         tmp1,
                         how='left',
                         on='route_id',
                         sort=False,
                         copy=False)
    
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    
    merged_df.drop_duplicates(subset='trip_id',
                              keep='first',
                              inplace=True)

    merged_df = pd.merge(trips_df,
                         merged_df[['unique_agency_id',
                         'trip_id']],
                         how='left',
                         on='trip_id',
                         sort=False,
                         copy=False)
    return merged_df


def _stops_agencyid(stops_df=None,
                    trips_df=None,
                    routes_df=None,
                    stop_times_df=None,
                    agency_df=None):
    """
    Assign unique agency id to stops dataframe

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops dataframe
    trips_df : pandas:DataFrame
        trips dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    stop_times_df : pandas:DataFrame
        stop times dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    
    tmp1 = pd.merge(routes_df,
                    agency_df,
                    how='left',
                    on='agency_id',
                    sort=False,
                    copy=False)
    
    tmp2 = pd.merge(trips_df,
                    tmp1,
                    how='left',
                    on='route_id',
                    sort=False,
                    copy=False)
    
    tmp3 = pd.merge(stop_times_df,
                    tmp2,
                    how='left',
                    on='trip_id',
                    sort=False,
                    copy=False)
    
    # do another merge to account for stops that may not be utilized across all
    # GTFS files for accounting purposes so we keep those that dont show up
    # after merge
    merged_df = pd.merge(stops_df[['stop_id']],
                         tmp3,
                         how='left',
                         on='stop_id',
                         sort=False,
                         copy=False)
    
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    
    merged_df.drop_duplicates(subset='stop_id',
                              keep='first',
                              inplace=True)

    merged_df = pd.merge(stops_df,
                         merged_df[['unique_agency_id',
                         'stop_id']],
                         how='left',
                         on='stop_id',
                         sort=False,
                         copy=False)
    
    return merged_df


def _routes_agencyid(routes_df=None, agency_df=None):
    """
    Assign unique agency id to routes dataframe

    Parameters
    ----------
    routes_df : pandas:DataFrame
        routes dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    merged_df = pd.merge(routes_df[['route_id', 'agency_id']],
                         agency_df,
                         how='left',
                         on='agency_id',
                         sort=False,
                         copy=False)
    
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    merged_df = pd.merge(routes_df,
                         merged_df[['unique_agency_id',
                         'route_id']],
                         how='left',
                         on='route_id',
                         sort=False,
                         copy=False)
    
    return merged_df


def _stop_times_agencyid(stop_times_df=None,
                         routes_df=None,
                         trips_df=None,
                         agency_df=None):
    """
    Assign unique agency id to stop times dataframe

    Parameters
    ----------
    stop_times_df : pandas:DataFrame
        stop times dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    trips_df : pandas:DataFrame
        trips dataframe
    agency_df : pandas:DataFrame
        agency dataframe

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    
    tmp1 = pd.merge(routes_df,
                    agency_df,
                    how='left',
                    on='agency_id',
                    sort=False,
                    copy=False)
    
    tmp2 = pd.merge(trips_df[['trip_id', 'route_id']],
                    tmp1,
                    how='left',
                    on='route_id',
                    sort=False,
                    copy=False)
    
    merged_df = pd.merge(stop_times_df,
                         tmp2,
                         how='left',
                         on='trip_id',
                         sort=False,
                         copy=False)
    
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    merged_df.drop_duplicates(subset='trip_id', keep='first',inplace=True)

    merged_df = pd.merge(stop_times_df,
                         merged_df[['unique_agency_id','trip_id']],
                         how='left',
                         on='trip_id',
                         sort=False,
                         copy=False)

    return merged_df


def _add_unique_agencyid(agency_df=None,
                         stops_df=None,
                         routes_df=None,
                         trips_df=None,
                         stop_times_df=None,
                         calendar_df=None,
                         calendar_dates_df=None,
                         nulls_as_folder=True,
                         feed_folder=None):
    """
    Create a unique agency id for all gtfs feed dataframes to 
    enable unique relational table keys

    Parameters
    ----------
    agency_df : pandas:DataFrame
        agency dataframe
    stops_df : pandas:DataFrame
        stops dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    trips_df : pandas:DataFrame
        trips dataframe
    stop_times_df : pandas:DataFrame
        stop times dataframe
    calendar_df : pandas:DataFrame
        calendar dataframe
    calendar_dates_df : pandas:DataFrame
        calendar dates dataframe
    nulls_as_folder : bool, optional
        if true, gtfs feeds where the agency id is null, the gtfs 
        folder name will be used as the unique agency id
    feed_folder : str
        name of gtfs feed folder
    Returns
    -------
    stops_df,routes_df,trips_df,stop_times_df,
        calendar_df,calendar_dates_df : pandas.DataFrame
    """
    start_time = time.time()

    df_list = [stops_df,
               routes_df,
               trips_df,
               stop_times_df,
               calendar_df,
               calendar_dates_df]

    # used for a number of checks
    path_to_check = os.path.join(feed_folder,'agency.txt')
    agency_path_exists = os.path.exists(path_to_check)
    agency_path_miss = agency_path_exists == False

    # checks for first if
    agency_absent = 'agency_id' not in agency_df.columns
    one_attr_absent = agency_path_miss or agency_absent
    absent_and_folder_nulls = one_attr_absent and nulls_as_folder

    # checks for second elif
    miss_file_and_folder_nulls = agency_path_miss and (nulls_as_folder == False)

    if absent_and_folder_nulls:
        # iterate through and add agency ids based on folder paths
        for index, df in enumerate(df_list):
            unique_agency_id = _generate_unique_from_folder(feed_folder)
            df['unique_agency_id'] = unique_agency_id
            # update positional dfs in top level list
            df_list[index] = df

        intro_msg = ('The agency.txt or agency_id column was not found. '
                     'The unique agency id: {} was generated using the '
                     'name of the folder containing the GTFS feed text '
                     'files.').format(unique_agency_id)
        log(intro_msg)

    elif miss_file_and_folder_nulls:
        err_text = ('No agency.txt file was found in {}. Add the '
                    'missing file to folder or set nulls_as_folder '
                    'to True').format(feed_folder)
        raise_with_traceback(ValueError(err_text))

    else:
        # start off by ensuring breaking cases are accounted for
        assert agency_path_exists == True
        assert 'agency_name' in agency_df.columns
        assert 'agency_id' in agency_df.columns
        assert len(agency_df[['agency_id','agency_name']]) != 0

    consolidated_gtfs = {
        'agency': agency_df,
        'stops': stops_df,
        'routes': routes_df,
        'trips': trips_df,
        'stop_times': stop_times_df,
        'calendar': calendar_df,
        'calendar_dates': calendar_dates_df
    }

    # update the df_list and replace it with a list that has 
    # each dataframe with a unique id included as a column
    df_list = make_agencies_unique(consolidated_gtfs)

    # final message to user at end of the function
    final_time_diff = time.time() - start_time
    final_msg = ('Unique agency id operation complete. Took '
                 '{:,.2f} seconds').format(final_time_diff)
    log(final_msg)

    return df_list


def make_agencies_unique(gtfs):
    # assigned each to a variable so format is similar afterwards
    # to what is in original function this process is based on
    agency_df = gtfs['agency']
    stops_df = gtfs['stops']
    routes_df = gtfs['routes']
    trips_df = gtfs['trips']
    stop_times_df = gtfs['stop_times']
    calendar_df = gtfs['calendar']
    calendar_dates_df = gtfs['calendar_dates']

    # when creating unique agency ids, we repeatedly use these subset dfs
    sub_stop_times_df = stop_times_df[['trip_id', 'stop_id']]

    # these three need to be combined to create a reference table
    sub_agency_df = agency_df[['agency_id', 'agency_name']]
    sub_routes_df = routes_df[['route_id', 'agency_id']]
    sub_trips_df = trips_df[['trip_id', 'route_id', 'service_id']]
    routes_agencies_trips = _merge_sub_routes_agencies_trips(
        sub_agency_df,
        sub_routes_df,
        sub_trips_df)

    calendar_dates_replacement_df = _calendar_dates_agency_uids(
        calendar_dates_df,
        routes_agencies_trips)

    calendar_replacement_df = _calendar_agency_uids(
        calendar_df,
        routes_agencies_trips)

    trips_replacement_df = _trips_agency_uids(trips_df, routes_agencies_trips)

    stops_replacement_df = _stops_agency_uids(
        stops_df,
        sub_stop_times_df,
        routes_agencies_trips)

    routes_replacement_df = _routes_agency_uids(routes_df, sub_agency_df)

    stop_times_replacement_df = _stop_times_agency_uids(
        stop_times_df,
        routes_agencies_trips)

    df_list = [stops_replacement_df,
               routes_replacement_df,
               trips_replacement_df,
               stop_times_replacement_df,
               calendar_replacement_df,
               calendar_dates_replacement_df]

    for index, df in enumerate(df_list):
        # for any missing operators, we just need a generic name
        mult_ops = 'multiple_operators_generic'
        df['unique_agency_id'].fillna(mult_ops, inplace=True)

        # again, update the position object in the top level list
        df_list[index] = df

    return df_list


def _merge_sub_routes_agencies_trips(agency_df, routes_df, trips_df):
    routes_agencies = pd.merge(routes_df,
                               agency_df,
                               how='left',
                               on='agency_id',
                               sort=False,
                               copy=False)

    routes_agencies_trips = pd.merge(trips_df,
                                     routes_agencies,
                                     how='left',
                                     on='route_id',
                                     sort=False,
                                     copy=False)
    
    return routes_agencies_trips


def _stops_definitions(df=None):
    """
    Append GTFS definitions for stop columns to stop dataframe

    Parameters
    ----------
    df : pandas:DataFrame
        stops dataframe

    Returns
    -------
    df : pandas.DataFrame
    """
    
    if 'location_type' in df.columns:
        stops_location_type = {
            0:'stop',
            1:'station',
            2:'station entrance'}
        stops_res = df['location_type'].map(stops_location_type.get)
        df['location_type_desc'] = stops_res
    
    if 'wheelchair_boarding' in df.columns:
        stops_wc_boardings = {
            0: 'No accessibility information available for the stop',
            1: ('At least some vehicles at this stop can be boarded by '
                'a rider in a wheelchair'),
            2: 'Wheelchair boarding is not possible at this stop'}
        wc_boarding = df['wheelchair_boarding'].map(stops_wc_boardings.get)
        df['wheelchair_boarding_desc'] = wc_boarding

    return df


def _routes_definitions(df=None):
    """
    Append GTFS definitions for route columns to route dataframe

    Parameters
    ----------
    df : pandas:DataFrame
        routes dataframe

    Returns
    -------
    df : pandas.DataFrame
    """
    if 'route_type' in df.columns:
        routes_route_type = {
            0: 'Street Level Rail: Tram Streetcar Light rail',
            1: 'Underground rail: Subway or Metro',
            2: 'Rail: intercity or long-distance ',
            3: 'Bus',
            4: 'Ferry',
            5: 'Cable Car',
            6: 'Gondola or Suspended cable car',
            7: 'Steep incline: Funicular'}
        df['route_type_desc'] = df['route_type'].map(routes_route_type.get)

    return df


def _stop_times_definitions(df=None):
    """
    Append GTFS definitions for stop time columns to stop time dataframe

    Parameters
    ----------
    df : pandas:DataFrame
        stop time dataframe

    Returns
    -------
    df : pandas.DataFrame
    """
    
    if 'pickup_type' in df.columns:
        stop_times_pickup_type = {
            0: 'Regularly Scheduled',
            1: 'Not available',
            2: 'Phone arrangement only',
            3: 'Driver arrangement only'}
        pickup_type = df['pickup_type'].map(stop_times_pickup_type.get)
        df['pickup_type_desc'] = pickup_type

    if 'drop_off_type' in df.columns:
        stop_times_drop_off_type = {
            0: 'Regularly Scheduled',
            1: 'Not available',
            2: 'Phone arrangement only',
            3: 'Driver arrangement only'}
        drop_type = df['drop_off_type'].map(stop_times_drop_off_type.get)
        df['drop_off_type_desc'] = drop_type

    if 'timepoint' in df.columns:
        stop_times_timepoint = {'': 'Exact times',
                                0: 'Approximate times',
                                1: 'Exact times'}
        df['timepoint_desc'] = df['timepoint'].map(stop_times_timepoint.get)

    return df


def _trips_definitions(df):
    """
    Append GTFS definitions for trip columns to trip dataframe

    Parameters
    ----------
    df : pandas:DataFrame
        trip dataframe

    Returns
    -------
    df : pandas.DataFrame
    """

    if 'bikes_allowed' in df.columns:
        trips_bikes_allowed = {
            1: 'can accommodate at least one bicycle',
            2: 'no bicycles are allowed on this trip'}
        bikes_allowed = df['bikes_allowed'].map(trips_bikes_allowed.get)
        df['bikes_allowed_desc'] = bikes_allowed

    if 'wheelchair_accessible' in df.columns:
        trips_wheelchair_accessible = {
            1: 'can accommodate at least one rider in a wheelchair',
            2: 'no riders in wheelchairs can be accommodated on this trip'}
        mapped_wheelchair_access = df['wheelchair_accessible'].map(
                                            trips_wheelchair_accessible.get)
        df['wheelchair_accessible_desc'] = mapped_wheelchair_access

    return df


def _add_txt_definitions(stops_df,
                         routes_df,
                         stop_times_df,
                         trips_df):
    """
    Append GTFS definitions to stops, routes, stop times, and trips dataframes

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    stop_times_df : pandas:DataFrame
        stop times dataframe
    trips_df : pandas:DataFrame
        trip dataframe

    Returns
    -------
    stops_df, routes_df, stop_times_df, trips_df : pandas.DataFrame
    """
    stops_df = _stops_definitions(df=stops_df)
    routes_df = _routes_definitions(df=routes_df)
    stop_times_df = _stop_times_definitions(df=stop_times_df)
    trips_df = _trips_definitions(df=trips_df)

    log('Added descriptive definitions to stops, '
        'routes, stop_times, and trips tables')

    return stops_df, routes_df, stop_times_df, trips_df


def _append_route_type(stops_df,
                       stop_times_df,
                       routes_df,
                       trips_df,
                       info_to_append):
    """
    Append GTFS route type definitions to stops and stop times dataframes

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops dataframe
    stop_times_df : pandas:DataFrame
        stop times dataframe
    routes_df : pandas:DataFrame
        routes dataframe
    trips_df : pandas:DataFrame
        trip dataframe
    info_to_append : {'route_type_to_stops','route_type_to_stop_times'}
        the type of information to append

    Returns
    -------
    stops_df or stop_times_df : pandas.DataFrame
    """
    valid_info_to_append = ['route_type_to_stops','route_type_to_stop_times']
    assert info_to_append in valid_info_to_append

    if info_to_append == 'route_type_to_stops':
        tmp1 = pd.merge(trips_df,
                        routes_df,
                        how='left',
                        on='route_id',
                        sort=False,
                        copy=False)
        
        merged_df = pd.merge(stop_times_df,
                             tmp1,
                             how='left',
                             on='trip_id',
                             sort=False,
                             copy=False)
        
        merged_df.drop_duplicates(subset='stop_id',
                                  keep='first',
                                  inplace=True)

        stops_df = pd.merge(stops_df,
                            merged_df[['route_type','stop_id']],
                            how='left',
                            on='stop_id',
                            sort=False,
                            copy=False)

        log('Appended route type to stops')

        return stops_df

    if info_to_append == 'route_type_to_stop_times':
        merged_df = pd.merge(trips_df,
                             routes_df,
                             how='left',
                             on='route_id',
                             sort=False,
                             copy=False)
        
        merged_df.drop_duplicates(subset='trip_id',
                                  keep='first',
                                  inplace=True)

        stop_times_df = pd.merge(stop_times_df,
                                 merged_df[['route_type','trip_id']],
                                 how='left',
                                 on='trip_id',
                                 sort=False,
                                 copy=False)

        log('Appended route type to stop_times')

        return stop_times_df


def _generate_unique_agency_id(df, col_name):
    """
    Generate unique agency id

    Parameters
    ----------
    df : pandas:DataFrame
    col_name : str
        typically will be 'agency_name'

    Returns
    -------
    col_snake_no_amps : str
    """

    col = df[col_name].astype(str)
    # replace all runs of spaces with a single underscore
    col_snake_case = col.str.replace(r'\s+', '_')
    # replace all ampersands
    col_snake_no_amps = col_snake_case.str.replace('&','and')
    return col_snake_no_amps.str.lower()


def _generate_unique_from_folder(feed_folder):
    path = os.path.split(feed_folder)[1]
    subbed = sub(r'\s+', '_', path)
    no_amps = subbed.replace('&','and')
    lowered = no_amps.lower()
    return lowered


def validate_and_trim_stops(stops_df, stop_times_df, bbox, dont_trim=False):
    lng_max, lat_min, lng_min, lat_max = bbox
    # craft booleans for longitude bounds check
    lng_max_check = (lng_max < stops_df['stop_lon'])
    lng_min_check = (lng_min > stops_df['stop_lon'])
    lng_check = (lng_max_check & lng_min_check)

    # craft booleans for latitude bounds check
    lat_max_check = (lat_max > stops_df['stop_lat'])
    lat_min_check = (lat_min < stops_df['stop_lat'])
    lat_check = (lat_max_check & lat_min_check)

    outside_bbox = stops_df.loc[~(lng_check & lat_check)]
    if len(outside_bbox.index) > 0:
        stops_df.drop(outside_bbox.index, inplace=True)

    return stops_df


def trim_stop_times(stops_df, stop_times_df):
    stops_in_bbox = list(stops_df['stop_id'])
    keep = stop_times_df['stop_id'].isin(stops_in_bbox)
    stop_times_df = stop_times_df[keep]
    return stop_times_df


def route_type_to_stops(stops_df, stop_times_df, routes_trips_df):
    merged_df = pd.merge(stop_times_df,
                         routes_trips_df,
                         how='left',
                         on='trip_id',
                         sort=False,
                         copy=False)
    merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)

    merged_sub = merged_df[['route_type', 'stop_id']]
    stops_df = pd.merge(stops_df,
                        merged_sub,
                        how='left',
                        on='stop_id',
                        sort=False,
                        copy=False)

    return stops_df


def route_type_to_stop_times(stops_df, stop_times_df, routes_trips_df):
    routes_trips_df.drop_duplicates(subset='trip_id',
                                    keep='first',
                                    inplace=True)

    rt_sub = routes_trips_df[['route_type', 'trip_id']]
    stop_times_df = pd.merge(stop_times_df,
                             rt_sub,
                             how='left',
                             on='trip_id',
                             sort=False,
                             copy=False)

    return stop_times_df


def time_to_seconds(stop_times_df):
    # start with an empty dataframe as base case
    concat_series_df = pd.DataFrame()

    # TODO: There are time libraries that handle these conversions far better
    #       than any hand rolled solution, suggest replacing this with one of
    #       those at some point to avoid unknown edge cases
    time_cols = ['departure_time', 'arrival_time']
    for col in time_cols:
        # convert series to a list to iterate through
        time_list = stop_times_df[col].fillna('').tolist()

        for n, i in enumerate(time_list):
            len_i = len(i)

            # handle if the time is missing a prended zero
            if len_i == 7:
                time_list[n] = str('0') + str(i)

        # update the dataframe with checked/passing times
        df_fixed = pd.DataFrame(time_list, columns=[col])
        stop_times_df[col] = df_fixed[col]

        # hours
        h = pd.to_numeric(stop_times_df[col].str[0:2])

        # minutes
        m = pd.to_numeric(stop_times_df[col].str[3:5])

        # seconds
        s = pd.to_numeric(stop_times_df[col].str[6:8])

        # convert down to seconds in total
        col_series = (h * 60 * 60) + (m * 60) + s
        sec_name = ''.join([col, '_sec'])
        series_df = col_series.to_frame(name=sec_name)

        series_arr = [concat_series_df, series_df]
        concat_series_df = pd.concat(series_arr, axis=1)

    final_df = pd.merge(stop_times_df,
                        concat_series_df,
                        how='left',
                        left_index=True,
                        right_index=True,
                        sort=False,
                        copy=False)

    return final_df
