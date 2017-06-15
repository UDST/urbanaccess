import logging as lg
import os
import pandas as pd
from future.utils import raise_with_traceback
from re import sub
import time

# Note: The above imported logging funcs were modified from the OSMnx library
#       & used with permission from the author Geoff Boeing: log, get_logger
#       OSMnx repo: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

from urbanaccess.utils import log


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

def _calendar_agencyid(calendar_df=None, routes_df=None, trips_df=None, agency_df=None):
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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id', sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False, copy=False)
    # do another merge to account for service ids that may not be utilized
    # across all GTFS files for accounting purposes so we keep those that
    # dont show up after merge
    merged_df = pd.merge(calendar_df[['service_id']], tmp2, how='left', on='service_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='service_id', keep='first', inplace=True)

    merged_df = pd.merge(calendar_df, merged_df[['unique_agency_id', 'service_id']], how='left',
                         on='service_id', sort=False, copy=False)

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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id', sort=False, copy=False)
    merged_df = pd.merge(trips_df[['trip_id', 'route_id']], tmp1, how='left', on='route_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

    merged_df = pd.merge(trips_df, merged_df[['unique_agency_id', 'trip_id']], how='left', on='trip_id',
                         sort=False, copy=False)
    return merged_df

def _stops_agencyid(stops_df=None, trips_df=None, routes_df=None, stop_times_df=None, agency_df=None):
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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id', sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False, copy=False)
    tmp3 = pd.merge(stop_times_df, tmp2, how='left', on='trip_id', sort=False, copy=False)
    # do another merge to account for stops that may not be utilized across all
    # GTFS files for accounting purposes so we keep those that dont show up
    # after merge
    merged_df = pd.merge(stops_df[['stop_id']], tmp3, how='left', on='stop_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)

    merged_df = pd.merge(stops_df, merged_df[['unique_agency_id', 'stop_id']], how='left', on='stop_id',
                         sort=False, copy=False)
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
    merged_df = pd.merge(routes_df[['route_id', 'agency_id']], agency_df, how='left', on='agency_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df, 'agency_name')

    merged_df = pd.merge(routes_df, merged_df[['unique_agency_id', 'route_id']], how='left', on='route_id',
                                     sort=False, copy=False)
    return merged_df

def _stop_times_agencyid(stop_times_df=None, routes_df=None, trips_df=None, agency_df=None):
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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id', sort=False, copy=False)
    tmp2 = pd.merge(trips_df[['trip_id', 'route_id']], tmp1, how='left', on='route_id', sort=False, copy=False)
    merged_df = pd.merge(stop_times_df, tmp2, how='left', on='trip_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first',inplace=True)

    merged_df = pd.merge(stop_times_df, merged_df[['unique_agency_id','trip_id']], how='left', on='trip_id', sort=False, copy=False)

    return merged_df

def _add_unique_agencyid(agency_df=None, stops_df=None, routes_df=None, trips_df=None, stop_times_df=None, calendar_df=None,
                         calendar_dates_df=None, nulls_as_folder=True, feed_folder=None):
    """
    Create a unique agency id for all gtfs feed dataframes to enable unique relational table keys

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
        if true, gtfs feeds where the agency id is null, the gtfs folder name will be used as the unique agency id
    feed_folder : str
        name of gtfs feed folder
    Returns
    -------
    stops_df,routes_df,trips_df,stop_times_df,calendar_df,calendar_dates_df : pandas.DataFrame
    """
    start_time = time.time()

    df_list = [stops_df,routes_df,trips_df,stop_times_df,calendar_df,calendar_dates_df]

    # used for a number of checks
    path_to_check = os.path.join(feed_folder,'agency.txt')
    agency_path_exists = os.path.exists(path_to_check)
    agency_path_miss = agency_path_exists == False

    # checks for first if
    agency_absent = 'agency_id' not in agency_df.columns
    one_attr_absent = agency_path_miss or agency_absent
    absent_and_folder_nulls = one_attr_absent and nulls_as_folder == True

    # checks for second elif
    miss_file_and_folder_nulls = agency_path_miss and nulls_as_folder == False

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

        # TODO: Shouldn't a dataframe return a series regardless
        #       of the num of rows? If so, then whether len of result
        #       is 1 or more should not matter...

        # different paths, depending on how many agencies
        if len(agency_df['agency_name']) == 1:
            # make sure that the one agency named is not a null name
            assert agency_df['agency_name'].isnull().values == False

            a_name = agency_df['agency_name'][0]
            a_subbed = sub(r'\s+', '_', a_name)
            unique_agency_id = a_subbed.replace('&','and').lower()

            for index, df in enumerate(df_list):
                df['unique_agency_id'] = unique_agency_id
                df_list[index] = df

            stat_msg = ('The unique agency id: {} was generated using the '
                        'name of the agency in the agency.txt '
                        'file.').format(unique_agency_id)
            log(stat_msg)

        elif len(agency_df['agency_name']) > 1:
            # TODO: Assertions shouldn't be so deeply nested 
            #       in runtime - validation should 
            #       either be prior to model execution or handled
            #       gracefully through caught errors/exceptions
            a_cols = ['agency_id','agency_name']
            assert agency_df[a_cols].isnull().values.any() == False

            # Only generate subset dataframes once, 
            # instead of for each keyword argument
            # in the below helper function
            sub_agency_df = agency_df[['agency_id','agency_name']]
            sub_routes_df = routes_df[['route_id', 'agency_id']]
            sub_stop_times_df = stop_times_df[['trip_id', 'stop_id']]
            sub_trips_df = trips_df[['trip_id', 'route_id']]
            sub_trips_df_w_sid = trips_df[['trip_id', 'route_id', 'service_id']]

            # TODO: In each of the steps, the functions foo_agencyid 
            #       ought be prepended with an underscore (e.g.
            #       foo_agencyid() to _foo_agencyid()) in order 
            #       to signify that these are helper functions for this
            #       step, and not exported out of this .py file
            calendar_dates_replacement_df = calendar_dates_agencyid(
                                            calendar_dates_df=calendar_dates_df,
                                            routes_df=sub_routes_df,
                                            trips_df=sub_trips_df_w_sid,
                                            agency_df=sub_agency_df)

            calendar_replacement_df = _calendar_agencyid(
                                            calendar_df=calendar_df,
                                            routes_df=sub_routes_df,
                                            trips_df=sub_trips_df_w_sid,
                                            agency_df=sub_agency_df)
            
            trips_replacement_df = trips_agencyid(
                                            trips_df=trips_df,
                                            routes_df=sub_routes_df,
                                            agency_df=sub_agency_df)

            stops_replacement_df = _stops_agencyid(
                                            stops_df=stops_df,
                                            trips_df=sub_trips_df,
                                            routes_df=sub_routes_df,
                                            stop_times_df=sub_stop_times_df,
                                            agency_df=sub_agency_df)

            routes_replacement_df = _routes_agencyid(
                                            routes_df=routes_df,
                                            agency_df=sub_agency_df)

            stop_times_replacement_df = _stop_times_agencyid(
                                            stop_times_df=stop_times_df,
                                            routes_df=sub_routes_df,
                                            trips_df=sub_trips_df,
                                            agency_df=sub_agency_df)

            df_list = [stops_replacement_df,
                       routes_replacement_df,
                       trips_replacement_df,
                       stop_times_replacement_df,
                       calendar_replacement_df,
                       calendar_dates_replacement_df]
            
            agency_file_msg = ('The agency.txt agency_name column has more '
                               'than one agency name listed. Unique agency '
                               'id was assigned using the agency id and '
                               'associated agency name.')
            log(agency_file_msg)

    for index, df in enumerate(df_list):
        if df['unique_agency_id'].isnull().values.any():
            unique_agency_id = _generate_unique_from_folder(feed_folder)
            mult_ops_id = ''.join(['multiple_operators_', unique_agency_id])
            df['unique_agency_id'].fillna(mult_ops_id, inplace=True)

            # again, update the position object in the top level list
            df_list[index] = df

            # TODO: Appears that there are 3 args being supplied but only
            #       2 points for insertion in the string
            agency_sum = df['unique_agency_id'].isnull().sum()
            u_a_id_sum = float(df['unique_agency_id'].isnull().sum())
            rounded = round((u_a_id_sum / float(len(df)) * 100))
            mult_ops_msg = ('There are {} null values ({}% of total) '
                            'without a unique agency id. '
                            'These records will be labeled as '
                            'multiple_operators_ with the GTFS file folder '
                            'name').format(agency_sum,
                                           len(df),
                                           rounded)
            log(mult_ops_msg)

    # final message to user at end of the function
    final_time_diff = time.time() - start_time
    final_msg = ('Unique agency id operation complete. Took '
                 '{:,.2f} seconds').format(final_time_diff)
    log(final_msg)

    return df_list


def timetoseconds(df=None, time_cols=None):
    """
    Convert default GTFS stop time departure and arrival times from 24 hour clock to seconds past midnight

    Parameters
    ----------
    df : pandas:DataFrame
        stop time dataframe
    time_cols : list
        list of columns to convert from 24 hour clock to seconds past midnight

    Returns
    -------
    final_df : pandas.DataFrame
    """
    start_time = time.time()

    # time frame needs to be a list
    assert isinstance(time_cols, list)

    # start with an empty dataframe as base case
    concat_series_df = pd.DataFrame()

    # TODO: There are time libraries that handle these conversions far better
    #       than any hand rolled solution, suggest replacing this with one of
    #       those at some point to avoid unknown edge cases
    for col in time_cols:
        # convert series to a list to iterate through
        time_list = df[col].fillna('').tolist()
        
        for n, i in enumerate(time_list):
            len_i = len(i)
            
            # handle if the time is missing a prended zero
            if len_i == 7:
                time_list[n] = str('0') + str(i)
            
            not_passable_len = (len_i != 8) & (len_i != 0) & (len_i != 7)
            if not_passable_len:
                err_msg = ('Check formating of value: {} as it is in the '
                           'incorrect format and should be 8 character '
                           'string 00:00:00.').format(i)
                raise_with_traceback(ValueError(err_msg))

        # update the dataframe with checked/passing times
        df_fixed = pd.DataFrame(time_list, columns=[col])
        df[col] = df_fixed[col]

        # hours
        h = pd.to_numeric(df[col].str[0:2])
        if h.any() > 48:
            warn_msg = ('Warning: {} hour value is large and may be '
                        'incorrect, please check '
                        'this.').format(df[col].str[0:2].max())
            log(warn_msg, level=lg.WARNING)
        
        # minutes
        m = pd.to_numeric(df[col].str[3:5])
        if m.any() > 60:
            warn_msg = ('Warning: {} minute value is large and may be '
                        'incorrect, please check '
                        'this.').format(df[col].str[3:5].max())
            log(warn_msg, level=lg.WARNING)
        
        # seconds
        s = pd.to_numeric(df[col].str[6:8])
        if s.any() > 60:
            warn_msg = ('Warning: {} second value is large and may be '
                        'incorrect, please check '
                        'this.').format(df[col].str[6:8].max())
            log(warn_msg, level=lg.WARNING)

        # convert down to seconds in total
        col_series = (h * 60 * 60) + (m * 60) + s
        sec_name = ''.join([col, '_sec'])
        series_df = col_series.to_frame(name=sec_name)

        series_arr = [concat_series_df, series_df]
        concat_series_df = pd.concat(series_arr, axis=1)

    final_df = pd.merge(df,
                        concat_series_df,
                        how='left',
                        left_index=True,
                        right_index=True,
                        sort=False,
                        copy=False)

    # final message about performance of this function to user
    end_func_time_diff = time.time() - start_time
    end_func_msg = ('Successfully converted {} to seconds past midnight '
                    'and appended new columns to stop_times. Took {:,.2f} '
                    'seconds').format(time_cols, end_func_time_diff)
    log(end_func_msg)

    return final_df

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
        stops_location_type = {0:'stop',1:'station',2:'station entrance'}
        df['location_type_desc'] = df['location_type'].map(stops_location_type.get)
    if 'wheelchair_boarding' in df.columns:
        stops_wheelchair_boardings = {0:'No accessibility information available for the stop',
                                      1:'At least some vehicles at this stop can be boarded by a rider in a wheelchair',
                                      2:'Wheelchair boarding is not possible at this stop'}
        df['wheelchair_boarding_desc'] = df['wheelchair_boarding'].map(stops_wheelchair_boardings.get)

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
        routes_route_type = {0:'Street Level Rail: Tram Streetcar Light rail',
                             1:'Underground rail: Subway or Metro',
                             2:'Rail: intercity or long-distance ',
                             3:'Bus',
                             4:'Ferry',
                             5:'Cable Car',
                             6:'Gondola or Suspended cable car',
                             7:'Steep incline: Funicular'}
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
        stop_times_pickup_type = {0:'Regularly Scheduled',
                                  1:'Not available',
                                  2:'Phone arrangement only',
                                  3:'Driver arrangement only'}
        df['pickup_type_desc'] = df['pickup_type'].map(stop_times_pickup_type.get)

    if 'drop_off_type' in df.columns:
        stop_times_drop_off_type = {0:'Regularly Scheduled',
                                    1:'Not available',
                                    2:'Phone arrangement only',
                                    3:'Driver arrangement only'}
        df['drop_off_type_desc'] = df['drop_off_type'].map(stop_times_drop_off_type.get)

    if 'timepoint' in df.columns:
        stop_times_timepoint = {'':'Exact times',
                                0:'Approximate times',
                                1:'Exact times'}
        df['timepoint_desc'] = df['timepoint'].map(stop_times_timepoint.get)

    return df

def _trips_definitions(df=None):
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
        trips_bikes_allowed  = {1:'can accommodate at least one bicycle',
                                2:'no bicycles are allowed on this trip'}
        df['bikes_allowed_desc'] = df['bikes_allowed'].map(trips_bikes_allowed.get)

    if 'wheelchair_accessible' in df.columns:
        trips_wheelchair_accessible = {1:'can accommodate at least one rider in a wheelchair',
                                       2:'no riders in wheelchairs can be accommodated on this trip'}
        df['wheelchair_accessible_desc'] = df['wheelchair_accessible'].map(trips_wheelchair_accessible.get)

    return df

def _add_txt_definitions(stops_df=None, routes_df=None, stop_times_df=None, trips_df=None):
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

    log('Added descriptive definitions to stops, routes, stop_times, and trips tables')

    return stops_df, routes_df, stop_times_df, trips_df

def _append_route_type(stops_df=None, stop_times_df=None, routes_df=None, trips_df=None, info_to_append=None):
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

        tmp1 = pd.merge(trips_df, routes_df, how='left', on='route_id', sort=False, copy=False)
        merged_df = pd.merge(stop_times_df, tmp1, how='left', on='trip_id', sort=False, copy=False)
        merged_df.drop_duplicates(subset='stop_id',keep='first',inplace=True)

        stops_df = pd.merge(stops_df, merged_df[['route_type','stop_id']], how='left', on='stop_id', sort=False, copy=False)

        log('Appended route type to stops')

        return stops_df

    if info_to_append == 'route_type_to_stop_times':

        merged_df = pd.merge(trips_df, routes_df, how='left', on='route_id', sort=False, copy=False)
        merged_df.drop_duplicates(subset='trip_id',keep='first',inplace=True)

        stop_times_df = pd.merge(stop_times_df, merged_df[['route_type','trip_id']], how='left', on='trip_id', sort=False, copy=False)

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

