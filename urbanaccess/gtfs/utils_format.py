import os
import time
import pandas as pd
from re import sub
import logging as lg

from urbanaccess.utils import log


def _read_gtfs_agency(textfile_path, textfile):
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
    if textfile != 'agency.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile), low_memory=False)
    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_stops(textfile_path, textfile):
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
    if textfile != 'stops.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'stop_id': object}, low_memory=False)

    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))

    df['stop_lat'] = pd.to_numeric(df['stop_lat'])
    df['stop_lon'] = pd.to_numeric(df['stop_lon'])

    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_routes(textfile_path, textfile):
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
    if textfile != 'routes.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'route_id': object}, low_memory=False)
    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_trips(textfile_path, textfile):
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
    if textfile != 'trips.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'trip_id': object,
                            'service_id': object,
                            'route_id': object,
                            7: object}, low_memory=False)
    # 7 is placeholder for shape id
    # which may not exist in some txt files
    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_stop_times(textfile_path, textfile):
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
    if textfile != 'stop_times.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'trip_id': object,
                            'stop_id': object,
                            'departure_time': object,
                            'arrival_time': object}, low_memory=False)
    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_calendar(textfile_path, textfile):
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
    if textfile != 'calendar.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'service_id': object}, low_memory=False)

    if len(df) == 0:
        error_msg = ('{} has no records. This could indicate that this feed '
                     'is using calendar_dates.txt for service_ids. If so, '
                     'make a dummy row in calendar.txt to proceed.')
        raise ValueError(error_msg.format(os.path.join(textfile_path,
                                                       textfile)))

    columnlist = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                  'saturday', 'sunday']
    for col in columnlist:
        df[col] = pd.to_numeric(df[col])
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _read_gtfs_calendar_dates(textfile_path, textfile):
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
    if textfile != 'calendar_dates.txt':
        raise ValueError('{} is not a proper GTFS file name'.format(textfile))

    df = pd.read_csv(os.path.join(textfile_path, textfile),
                     dtype={'service_id': object}, low_memory=False)
    if len(df) == 0:
        raise ValueError('{} has no records'.format(os.path.join(
            textfile_path, textfile)))
    # remove any extra whitespace in column names
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df


def _calendar_dates_agencyid(calendar_dates_df, routes_df,
                             trips_df, agency_df, feed_folder):
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
    feed_folder : str
        name of gtfs feed folder

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False,
                    copy=False)
    merged_df = pd.merge(calendar_dates_df, tmp2, how='left', on='service_id',
                         sort=False, copy=False)

    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    group = merged_df[['service_id', 'unique_agency_id']].groupby([
        'service_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if service_ids are associated with more than one agency
    if any(group_counts.index.value_counts().values > 1):
        log('GTFS feed: {!s}, calendar_dates uses the same service_id across '
            'multiple agency_ids. This feed calendar_dates table will be '
            'modified from its original format to provide service_ids for '
            'each agency using a one to many join'.format(os.path.split(
                feed_folder)[1]))

        tmp = merged_df[
            ['service_id', 'unique_agency_id']].drop_duplicates(
            ['service_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(calendar_dates_df, 'left',
                              on='service_id')

        return merged_df

    else:

        merged_df.drop_duplicates(subset='service_id', keep='first',
                                  inplace=True)
        merged_df = pd.merge(calendar_dates_df,
                             merged_df[['unique_agency_id', 'service_id']],
                             how='left',
                             on='service_id', sort=False, copy=False)

        return merged_df


def _calendar_agencyid(calendar_df, routes_df, trips_df,
                       agency_df, feed_folder):
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
    feed_folder : str
        name of gtfs feed folder

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False,
                    copy=False)
    # do another merge to account for service ids that may not be utilized
    # across all GTFS files for accounting purposes so we keep those that
    # dont show up after merge
    merged_df = pd.merge(calendar_df[['service_id']], tmp2, how='left',
                         on='service_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    group = merged_df[['service_id', 'unique_agency_id']].groupby([
        'service_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if service_ids are associated with more than one agency
    if any(group_counts.index.value_counts().values > 1):
        log('GTFS feed: {!s}, calendar uses the same service_id across '
            'multiple agency_ids. This feed calendar table will be '
            'modified from its original format to provide service_ids for '
            'each agency using a one to many join'.format(os.path.split(
                feed_folder)[1]))

        tmp = merged_df[
            ['service_id', 'unique_agency_id']].drop_duplicates(
            ['service_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(calendar_df, 'left',
                              on='service_id')

        return merged_df

    else:

        merged_df.drop_duplicates(subset='service_id',
                                  keep='first',
                                  inplace=True)
        merged_df = pd.merge(calendar_df,
                             merged_df[['unique_agency_id', 'service_id']],
                             how='left',
                             on='service_id', sort=False, copy=False)

        return merged_df


def _trips_agencyid(trips_df, routes_df, agency_df):
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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    merged_df = pd.merge(trips_df[['trip_id', 'route_id']], tmp1, how='left',
                         on='route_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

    merged_df = pd.merge(trips_df, merged_df[['unique_agency_id', 'trip_id']],
                         how='left', on='trip_id',
                         sort=False, copy=False)
    return merged_df


def _stops_agencyid(stops_df, trips_df, routes_df,
                    stop_times_df, agency_df, feed_folder):
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
    feed_folder : str
        name of gtfs feed folder

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False,
                    copy=False)
    tmp3 = pd.merge(stop_times_df, tmp2, how='left', on='trip_id', sort=False,
                    copy=False)
    # do another merge to account for stops that may not be utilized across all
    # GTFS files for accounting purposes so we keep those that dont show up
    # after merge
    merged_df = pd.merge(stops_df[['stop_id']], tmp3, how='left', on='stop_id',
                         sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    group = merged_df[['stop_id', 'unique_agency_id']].groupby([
        'stop_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if stop_ids are associated with more than one agency
    if any(group_counts.index.value_counts().values > 1):
        log('GTFS feed: {!s}, stops uses the same stop_id across '
            'multiple agency_ids. This feed stops table will be '
            'modified from its original format to provide stop_ids for '
            'each agency using a one to many join'.format(os.path.split(
                feed_folder)[1]))

        tmp = merged_df[
            ['stop_id', 'unique_agency_id']].drop_duplicates(
            ['stop_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(stops_df, 'left',
                              on='stop_id')

        return merged_df

    else:

        merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)
        merged_df = pd.merge(stops_df,
                             merged_df[['unique_agency_id', 'stop_id']],
                             how='left', on='stop_id',
                             sort=False, copy=False)
        return merged_df


def _routes_agencyid(routes_df, agency_df):
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
    merged_df = pd.merge(routes_df[['route_id', 'agency_id']], agency_df,
                         how='left', on='agency_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')

    merged_df = pd.merge(routes_df,
                         merged_df[['unique_agency_id', 'route_id']],
                         how='left', on='route_id',
                         sort=False, copy=False)
    return merged_df


def _stop_times_agencyid(stop_times_df, routes_df, trips_df,
                         agency_df):
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
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    tmp2 = pd.merge(trips_df[['trip_id', 'route_id']], tmp1, how='left',
                    on='route_id', sort=False, copy=False)
    merged_df = pd.merge(stop_times_df, tmp2, how='left', on='trip_id',
                         sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(merged_df,
                                                               'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

    merged_df = pd.merge(stop_times_df,
                         merged_df[['unique_agency_id', 'trip_id']],
                         how='left', on='trip_id', sort=False, copy=False)

    return merged_df


def _add_unique_agencyid(agency_df, stops_df, routes_df,
                         trips_df, stop_times_df, calendar_df, feed_folder,
                         calendar_dates_df, nulls_as_folder=True):
    """
    Create a unique agency id for all gtfs feed dataframes to enable unique
    relational table keys

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
    feed_folder : str
        name of gtfs feed folder
    calendar_dates_df : pandas:DataFrame
        calendar dates dataframe
    nulls_as_folder : bool, optional
        if true, gtfs feeds where the agency id is null, the gtfs folder
        name will be used as the unique agency id
    Returns
    -------
    stops_df, routes_df, trips_df, stop_times_df, calendar_df,
    calendar_dates_df : pandas.DataFrame
    """
    start_time = time.time()

    df_list = [stops_df, routes_df, trips_df, stop_times_df, calendar_df]
    # if calendar_dates_df is not empty then add it to the processing list
    if calendar_dates_df.empty is False:
        df_list.extend([calendar_dates_df])

    path_absent = os.path.exists(
        os.path.join(feed_folder, 'agency.txt')) is False
    agency_absent = 'agency_id' not in agency_df.columns

    if (path_absent or agency_absent) and nulls_as_folder is True:

        for index, df in enumerate(df_list):
            unique_agency_id = _generate_unique_feed_id(feed_folder)
            df['unique_agency_id'] = unique_agency_id
            df_list[index] = df

        log('The agency.txt or agency_id column was not found. The unique '
            'agency id: {} was generated using the name of the folder '
            'containing the GTFS feed text files.'.format(
                unique_agency_id))

    elif path_absent is False and nulls_as_folder is False:
        raise ValueError(
            'No agency.txt file was found in {}. Add the missing file to '
            'folder or set nulls_as_folder to True'.format(feed_folder))

    else:
        if path_absent:
            file_path = os.path.join(feed_folder, 'agency.txt')
            raise ValueError('{} not found.'.format(file_path))
        if 'agency_name' not in agency_df.columns or 'agency_id' not in \
                agency_df.columns:
            raise ValueError('both agency_name and agency_id columns were not '
                             'found in agency.txt')
        if len(agency_df[['agency_id', 'agency_name']]) == 0:
            raise ValueError('agency.txt has no agency_id or agency_name '
                             'values')

        if len(agency_df['agency_name']) == 1:
            if agency_df['agency_name'].isnull().values:
                raise ValueError('null values in agency_name were found')

            # could be added to helper function
            # take first agency
            agency_snake_case = sub(r'\s+', '_', agency_df['agency_name'][0])
            unique_agency_id = agency_snake_case.replace('&', 'and').lower()

            for index, df in enumerate(df_list):
                df['unique_agency_id'] = unique_agency_id
                df_list[index] = df
            log(
                'The unique agency id: {} was generated using the name of '
                'the agency in the agency.txt file.'.format(
                    unique_agency_id))

        elif len(agency_df['agency_name']) > 1:
            if agency_df[['agency_id',
                          'agency_name']].isnull().values.any():
                raise ValueError(
                    'null values were found in agency_id and agency_name')

            # subset dataframes
            subset_agency_df = agency_df[['agency_id', 'agency_name']]
            subset_routes_df = routes_df[['route_id', 'agency_id']]
            subset_stop_times_df = stop_times_df[['trip_id', 'stop_id']]
            subset_trips_df = trips_df[['trip_id', 'route_id']]
            subset_trips_df_w_sid = trips_df[
                ['trip_id', 'route_id', 'service_id']]

            # if calendar_dates_df is not empty then process it
            if calendar_dates_df.empty is False:
                calendar_dates_replacement_df = _calendar_dates_agencyid(
                    calendar_dates_df=calendar_dates_df,
                    routes_df=subset_routes_df,
                    trips_df=subset_trips_df_w_sid,
                    agency_df=subset_agency_df,
                    feed_folder=feed_folder)

            calendar_replacement_df = _calendar_agencyid(
                calendar_df=calendar_df,
                routes_df=subset_routes_df,
                trips_df=subset_trips_df_w_sid,
                agency_df=subset_agency_df,
                feed_folder=feed_folder)

            trips_replacement_df = _trips_agencyid(
                trips_df=trips_df,
                routes_df=subset_routes_df,
                agency_df=subset_agency_df)

            stops_replacement_df = _stops_agencyid(
                stops_df=stops_df,
                trips_df=subset_trips_df,
                routes_df=subset_routes_df,
                stop_times_df=subset_stop_times_df,
                agency_df=subset_agency_df,
                feed_folder=feed_folder)

            routes_replacement_df = _routes_agencyid(
                routes_df=routes_df,
                agency_df=subset_agency_df)

            stop_times_replacement_df = _stop_times_agencyid(
                stop_times_df=stop_times_df,
                routes_df=subset_routes_df,
                trips_df=subset_trips_df,
                agency_df=subset_agency_df)

            # update the df_list object with these new variable overrides
            df_list = [stops_replacement_df,
                       routes_replacement_df,
                       trips_replacement_df,
                       stop_times_replacement_df,
                       calendar_replacement_df]
            # if calendar_dates_df is not empty then add it to the
            # processing list
            if calendar_dates_df.empty is False:
                df_list.extend([calendar_dates_replacement_df])

            log(
                'agency.txt agency_name column has more than one agency name '
                'listed. Unique agency id was assigned using the agency id '
                'and associated agency name.')

    for index, df in enumerate(df_list):
        if df['unique_agency_id'].isnull().values.any():

            unique_agency_id = _generate_unique_feed_id(feed_folder)

            df['unique_agency_id'].fillna(
                ''.join(['multiple_operators_', unique_agency_id]),
                inplace=True)
            log(
                'There are {:,} null values ({:,.2f}% of {:,} total) without '
                'a unique agency id. These records will be labeled as '
                'multiple_operators_ with the GTFS file folder '
                'name'.format(df['unique_agency_id'].isnull().sum(),
                              len(df),
                              round((float(df['unique_agency_id'].isnull(
                              ).sum()) / float(len(df)) * 100))))
            df_list[index] = df

    # if calendar_dates_df is empty then return the original empty df
    if calendar_dates_df.empty:
        df_list.extend([calendar_dates_df])

    log('Unique agency id operation complete. Took {:,.2f} seconds'.format(
        time.time() - start_time))
    return df_list


def _add_unique_gtfsfeed_id(stops_df, routes_df, trips_df,
                            stop_times_df, calendar_df, calendar_dates_df,
                            feed_folder, feed_number):
    """
    Create a unique GTFS feed specific id for all gtfs feed dataframes to
    enable tracking of specific feeds

    Parameters
    ----------
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
    feed_folder : str
        name of gtfs feed folder
    feed_number : int
        current number iteration of gtfs feed being read in root directory
    Returns
    -------
    stops_df, routes_df, trips_df, stop_times_df, calendar_df,
    calendar_dates_df : pandas.DataFrame
    """
    start_time = time.time()

    df_list = [stops_df,
               routes_df,
               trips_df,
               stop_times_df,
               calendar_df]
    # if calendar_dates_df is not empty then add it to the processing list
    if calendar_dates_df.empty is False:
        df_list.extend([calendar_dates_df])

    # standardize feed_folder name
    feed_folder = _generate_unique_feed_id(feed_folder)

    for index, df in enumerate(df_list):
        # create new unique_feed_id column based on the name of the feed folder
        df['unique_feed_id'] = '_'.join([feed_folder, str(feed_number)])
        df_list[index] = df

    # if calendar_dates_df is empty then return the original empty df
    if calendar_dates_df.empty:
        df_list.extend([calendar_dates_df])

    log('Unique GTFS feed id operation complete. Took {:,.2f} seconds'.format(
        time.time() - start_time))
    return df_list


def _timetoseconds(df, time_cols):
    """
    Convert default GTFS stop time departure and arrival times from 24 hour
    clock to seconds past midnight

    Parameters
    ----------
    df : pandas:DataFrame
        stop time dataframe
    time_cols : list
        list of columns to convert from 24 hour clock to seconds past
        midnight. Default column is 'departure_time'.

    Returns
    -------
    final_df : pandas.DataFrame
    """
    start_time = time.time()

    concat_series_df = pd.DataFrame()
    if not isinstance(time_cols, list):
        raise ValueError('{} is not a list'.format(time_cols))

    for col in time_cols:

        time_list = df[col].fillna('').tolist()
        for n, i in enumerate(time_list):
            if len(i) == 7:
                time_list[n] = '0' + i
            if (len(i) != 8) & (len(i) != 0) & (len(i) != 7):
                raise ValueError(
                    'Check formatting of value: {} as it is in the incorrect '
                    'format and should be 8 character string 00:00:00.'.format(
                        i))
        df_fixed = pd.DataFrame(time_list, columns=[col])

        df[col] = df_fixed[col]

        h = pd.to_numeric(df[col].str[0:2])
        if h.any() > 48:
            log(
                'Warning: {} hour value is large and may be incorrect, '
                'please check this.'.format(
                    df[col].str[0:2].max()), level=lg.WARNING)
        m = pd.to_numeric(df[col].str[3:5])
        if m.any() > 60:
            log(
                'Warning: {} minute value is large and may be incorrect, '
                'please check this.'.format(
                    df[col].str[3:5].max()), level=lg.WARNING)
        s = pd.to_numeric(df[col].str[6:8])
        if s.any() > 60:
            log(
                'Warning: {} second value is large and may be incorrect, '
                'please check this.'.format(
                    df[col].str[6:8].max()), level=lg.WARNING)
        col_series = (h * 60 * 60) + (m * 60) + s
        series_df = col_series.to_frame(name=''.join([col, '_sec']))

        # check if times are negative if so display warning
        if series_df.values.any() < 0:
            log('Warning: Some stop times in {} column are negative. '
                'Time should be positive. Suggest checking original '
                'GTFS feed stop_time file before proceeding'.format(col),
                level=lg.WARNING)

        concat_series_df = pd.concat([concat_series_df, series_df], axis=1)

    final_df = pd.merge(df, concat_series_df, how='left', left_index=True,
                        right_index=True, sort=False, copy=False)

    log(
        'Successfully converted {} to seconds past midnight and appended new '
        'columns to stop_times. Took {:,.2f} seconds'.format(
            time_cols, time.time() - start_time))

    return final_df


def _stops_definitions(df):
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
        stops_location_type = {0: 'stop', 1: 'station', 2: 'station entrance'}
        df['location_type_desc'] = df['location_type'].map(
            stops_location_type.get)

    if 'wheelchair_boarding' in df.columns:
        stops_wheelchair_boardings = {
            0: 'No accessibility information available for the stop',
            1: 'At least some vehicles at this stop can be boarded by a '
               'rider in a wheelchair',
            2: 'Wheelchair boarding is not possible at this stop'}
        df['wheelchair_boarding_desc'] = df['wheelchair_boarding'].map(
            stops_wheelchair_boardings.get)

    return df


def _routes_definitions(df):
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
        routes_route_type = {0: 'Street Level Rail: Tram Streetcar Light rail',
                             1: 'Underground rail: Subway or Metro',
                             2: 'Rail: intercity or long-distance ',
                             3: 'Bus',
                             4: 'Ferry',
                             5: 'Cable Car',
                             6: 'Gondola or Suspended cable car',
                             7: 'Steep incline: Funicular'}
        df['route_type_desc'] = df['route_type'].map(routes_route_type.get)

    return df


def _stop_times_definitions(df):
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
        stop_times_pickup_type = {0: 'Regularly Scheduled',
                                  1: 'Not available',
                                  2: 'Phone arrangement only',
                                  3: 'Driver arrangement only'}
        df['pickup_type_desc'] = df['pickup_type'].map(
            stop_times_pickup_type.get)

    if 'drop_off_type' in df.columns:
        stop_times_drop_off_type = {0: 'Regularly Scheduled',
                                    1: 'Not available',
                                    2: 'Phone arrangement only',
                                    3: 'Driver arrangement only'}
        df['drop_off_type_desc'] = df['drop_off_type'].map(
            stop_times_drop_off_type.get)

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
        trips_bikes_allowed = {1: 'can accommodate at least one bicycle',
                               2: 'no bicycles are allowed on this trip'}
        df['bikes_allowed_desc'] = df['bikes_allowed'].map(
            trips_bikes_allowed.get)

    if 'wheelchair_accessible' in df.columns:
        trips_wheelchair_accessible = {
            1: 'can accommodate at least one rider in a wheelchair',
            2: 'no riders in wheelchairs can be accommodated on this trip'}
        df['wheelchair_accessible_desc'] = df['wheelchair_accessible'].map(
            trips_wheelchair_accessible.get)

    return df


def _add_txt_definitions(stops_df, routes_df, stop_times_df,
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

    log(
        'Added descriptive definitions to stops, routes, stop_times, '
        'and trips tables')

    return stops_df, routes_df, stop_times_df, trips_df


def _append_route_type(stops_df, stop_times_df, routes_df,
                       trips_df, info_to_append):
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
    info_to_append : {'route_type_to_stops', 'route_type_to_stop_times'}
        the type of information to append

    Returns
    -------
    stops_df or stop_times_df : pandas.DataFrame
    """
    valid_info_to_append = ['route_type_to_stops', 'route_type_to_stop_times']
    if info_to_append not in valid_info_to_append:
        raise ValueError('{} is not a valid parameter'.format(info_to_append))

    if info_to_append == 'route_type_to_stops':
        tmp1 = pd.merge(trips_df, routes_df, how='left', on='route_id',
                        sort=False, copy=False)
        merged_df = pd.merge(stop_times_df, tmp1, how='left', on='trip_id',
                             sort=False, copy=False)
        merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)

        stops_df = pd.merge(stops_df, merged_df[['route_type', 'stop_id']],
                            how='left', on='stop_id', sort=False, copy=False)

        log('Appended route type to stops')

        return stops_df

    if info_to_append == 'route_type_to_stop_times':
        merged_df = pd.merge(trips_df, routes_df, how='left', on='route_id',
                             sort=False, copy=False)
        merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

        stop_times_df = pd.merge(stop_times_df,
                                 merged_df[['route_type', 'trip_id']],
                                 how='left', on='trip_id', sort=False,
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
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    return col_snake_no_amps.str.lower()


def _generate_unique_feed_id(feed_folder):
    """
    Generate unique feed id

    Parameters
    ----------
    feed_folder : str
        typically will be the path ot the feed folder

    Returns
    -------
    col_snake_no_amps : str
    """

    folder_name = os.path.split(feed_folder)[1]
    # replace all runs of spaces with a single underscore and replace all
    # ampersands
    folder_snake_case_no_amps = sub(r'\s+', '_', folder_name).replace('&',
                                                                      'and')

    return folder_snake_case_no_amps.lower()
