import os
import time
import pandas as pd
import numpy as np
from re import sub
import logging as lg

from urbanaccess.utils import log
from urbanaccess import config


def _read_gtfs_file(textfile_path, textfile):
    """
    Read GTFS text file as a pandas.DataFrame

    Parameters
    ----------
    textfile_path : str
        directory of text file
    textfile : str
        name of text file

    Returns
    -------
    df : pandas.DataFrame
    """
    start_time = time.time()

    file_path = os.path.join(textfile_path, textfile)
    # check that file is a supported GTFS file
    expected_txt_files = config._GTFS_READ_TXT_CONFIG.keys()
    expected_txt_files = [name + '.txt' for name in expected_txt_files]
    if textfile not in expected_txt_files:
        raise ValueError(
            '{} is not a supported GTFS file. Supported files are: {}.'.format(
                textfile, expected_txt_files))

    # setup data schema variables for file
    file_name = textfile.rsplit('.', 1)[0]
    req_dtypes = config._GTFS_READ_TXT_CONFIG[
        file_name]['req_dtypes']
    opt_dtypes = config._GTFS_READ_TXT_CONFIG[
        file_name]['opt_dtypes']
    numeric_converter = config._GTFS_READ_TXT_CONFIG[
        file_name]['numeric_converter']
    remove_whitespace = config._GTFS_READ_TXT_CONFIG[
        file_name]['remove_whitespace']
    min_required_cols = config._GTFS_READ_TXT_CONFIG[
        file_name]['min_required_cols']

    if min_required_cols is not None or opt_dtypes is not None:
        # get list of cols in file
        col_list = _list_raw_txt_columns(file_path)
        # remove leading and trailing spaces in column names
        col_list = [col.strip() for col in col_list]

    # check if req cols exists
    if min_required_cols is not None:
        missing_req_cols = []
        for col in min_required_cols:
            if col not in col_list:
                missing_req_cols.append(col)
        if missing_req_cols:
            raise ValueError(
                '{} is missing required column(s): {}.'.format(
                    textfile, missing_req_cols))

    # if optional dtype col exists include it in req_dtypes dict
    if opt_dtypes is not None:
        for col_name, dtype in opt_dtypes.items():
            if col_name in col_list:
                req_dtypes.update({col_name: dtype})

    df = pd.read_csv(file_path, dtype=req_dtypes, low_memory=False)

    # print warning or raise error when table is empty depending on the table
    if df.empty:
        cal_file_warnings = {'calendar': 'calendar_dates.txt',
                             'calendar_dates': 'calendar.txt'}
        if file_name in cal_file_warnings.keys():
            warning_msg = (
                '     {} has no records. This could indicate that this feed '
                'is using {} instead of {}.txt for service_ids.')
            log(warning_msg.format(textfile, cal_file_warnings[file_name],
                                   file_name), level=lg.WARNING)
        else:
            raise ValueError('{} has no records. '
                             'This file cannot be empty.'.format(textfile))

    # remove extra whitespace that may exist in col names or before and
    # after the value for columns that are used across different GTFS files
    df = _remove_whitespace(
        df=df, textfile=textfile, col_list=remove_whitespace)

    # convert dtypes here in case col name was incorrect due to
    # whitespace when read in pd.read_csv()
    if req_dtypes is not None:
        for col_name, dtype in req_dtypes.items():
            if df[col_name].dtype != dtype:
                if dtype == object:
                    dtype = str  # force to string instead of object
                df[col_name] = df[col_name].astype(dtype)

    if numeric_converter is not None:
        for col in numeric_converter:
            df[col] = pd.to_numeric(df[col])

    record_cnt = len(df)
    msg = ('     Successfully read: {} with {:,} record(s). '
           'Took {:,.2f} seconds')
    log(msg.format(textfile, record_cnt, time.time() - start_time))

    return df


def _calendar_dates_agencyid(calendar_dates_df, routes_df,
                             trips_df, agency_df, feed_folder):
    """
    Assign unique agency ID to calendar dates DataFrame

    Parameters
    ----------
    calendar_dates_df : pandas:DataFrame
        calendar dates DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame
    feed_folder : str
        name of GTFS feed folder

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

    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')

    group = merged_df[['service_id', 'unique_agency_id']].groupby([
        'service_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if service_ids are associated with more than one agency

    if any(group_counts.index.value_counts().values > 1):
        feed_name = os.path.split(feed_folder)[1]
        log('GTFS feed: {!s}, calendar_dates uses the same service_id across '
            'multiple agency_ids. This feed calendar_dates table will be '
            'modified from its original format to provide service_ids for '
            'each agency using a one to many join.'.format(feed_name))

        tmp = merged_df[['service_id', 'unique_agency_id']].drop_duplicates(
            ['service_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(calendar_dates_df, 'left', on='service_id')

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
    Assign unique agency ID to calendar DataFrame

    Parameters
    ----------
    calendar_df : pandas:DataFrame
        calendar DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame
    feed_folder : str
        name of GTFS feed folder

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    tmp2 = pd.merge(trips_df, tmp1, how='left', on='route_id', sort=False,
                    copy=False)
    # do another merge to account for service IDs that may not be utilized
    # across all GTFS files for accounting purposes so we keep those that
    # dont show up after merge
    merged_df = pd.merge(calendar_df[['service_id']], tmp2, how='left',
                         on='service_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')

    group = merged_df[['service_id', 'unique_agency_id']].groupby([
        'service_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if service_ids are associated with more than one agency
    if any(group_counts.index.value_counts().values > 1):
        feed_name = os.path.split(feed_folder)[1]
        log('GTFS feed: {!s}, calendar uses the same service_id across '
            'multiple agency_ids. This feed calendar table will be '
            'modified from its original format to provide service_ids for '
            'each agency using a one to many join.'.format(feed_name))

        tmp = merged_df[
            ['service_id', 'unique_agency_id']].drop_duplicates(
            ['service_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(calendar_df, 'left', on='service_id')

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
    Assign unique agency ID to trips DataFrame

    Parameters
    ----------
    trips_df : pandas:DataFrame
        trips DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    tmp1 = pd.merge(routes_df, agency_df, how='left', on='agency_id',
                    sort=False, copy=False)
    merged_df = pd.merge(trips_df[['trip_id', 'route_id']], tmp1, how='left',
                         on='route_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

    merged_df = pd.merge(trips_df, merged_df[['unique_agency_id', 'trip_id']],
                         how='left', on='trip_id',
                         sort=False, copy=False)
    return merged_df


def _stops_agencyid(stops_df, trips_df, routes_df,
                    stop_times_df, agency_df, feed_folder):
    """
    Assign unique agency ID to stops DataFrame

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame
    feed_folder : str
        name of GTFS feed folder

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
    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')

    group = merged_df[['stop_id', 'unique_agency_id']].groupby([
        'stop_id', 'unique_agency_id']).size()
    group_counts = group.reset_index(level=1)
    # check if stop_ids are associated with more than one agency
    if any(group_counts.index.value_counts().values > 1):
        feed_name = os.path.split(feed_folder)[1]
        log('GTFS feed: {!s}, stops uses the same stop_id across '
            'multiple agency_ids. This feed stops table will be '
            'modified from its original format to provide stop_ids for '
            'each agency using a one to many join.'.format(feed_name))

        tmp = merged_df[['stop_id', 'unique_agency_id']].drop_duplicates(
            ['stop_id', 'unique_agency_id'], inplace=False)
        merged_df = tmp.merge(stops_df, 'left', on='stop_id')

    else:
        merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)
        merged_df = pd.merge(stops_df,
                             merged_df[['unique_agency_id', 'stop_id']],
                             how='left', on='stop_id',
                             sort=False, copy=False)
    return merged_df


def _routes_agencyid(routes_df, agency_df):
    """
    Assign unique agency ID to routes DataFrame

    Parameters
    ----------
    routes_df : pandas:DataFrame
        routes DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame

    Returns
    -------
    merged_df : pandas.DataFrame
    """
    merged_df = pd.merge(routes_df[['route_id', 'agency_id']], agency_df,
                         how='left', on='agency_id', sort=False, copy=False)
    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')

    merged_df = pd.merge(routes_df,
                         merged_df[['unique_agency_id', 'route_id']],
                         how='left', on='route_id',
                         sort=False, copy=False)
    return merged_df


def _stop_times_agencyid(stop_times_df, routes_df, trips_df,
                         agency_df):
    """
    Assign unique agency ID to stop times DataFrame

    Parameters
    ----------
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    agency_df : pandas:DataFrame
        agency DataFrame

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
    merged_df['unique_agency_id'] = _generate_unique_agency_id(
        merged_df, 'agency_name')
    merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

    merged_df = pd.merge(stop_times_df,
                         merged_df[['unique_agency_id', 'trip_id']],
                         how='left', on='trip_id', sort=False, copy=False)

    return merged_df


def _add_unique_agency_id(agency_df, stops_df, routes_df,
                         trips_df, stop_times_df, calendar_df,
                         calendar_dates_df, feed_folder,
                         nulls_as_folder=True):
    """
    Create an unique agency ID for all GTFS feed DataFrames to enable unique
    relational table keys. Pathways to create the unique agency ID are:
    1) GTFS feed has no agency.txt file or the agency.txt file has no
    'agency_id' column: If true, unique agency ID will be generated using
    the GTFS feed directory folder name; 2) If GTFS feed has an agency.txt
    file and it has only one agency (it must have an 'agency_id' column and
    value): If true, unique agency ID will be generated using the 'agency_id';
    3) If GTFS feed has an agency.txt file and it has more than one agency
    (it must have an 'agency_id' and 'agency_name' column and values):
    If true, unique agency ID will be generated using the 'agency_id' and
    'agency_name' using relational key 'agency_id' in the routes.txt table
    to broadcast all the correct unique agency ID values to all other GTFS
    tables; 4) If GTFS feed has an agency.txt file and it has more than
    one agency (it must have an 'agency_id' and 'agency_name' column and
    values), however if there is also a mismatch between the 'agency_id'
    in agency.txt and routes.txt, then assume records tied to the mismatched
    'agency_id'(s) are from multiple agencies and label the unique agency ID
    as such by concatenating 'multiple_operators_' and the GTFS feed directory
    folder name.

    Parameters
    ----------
    agency_df : pandas:DataFrame
        agency DataFrame
    stops_df : pandas:DataFrame
        stops DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    calendar_df : pandas:DataFrame
        calendar DataFrame
    calendar_dates_df : pandas:DataFrame
        calendar dates DataFrame
    feed_folder : str
        name of GTFS feed folder
    nulls_as_folder : bool, optional
        if true, GTFS feeds where the agency ID is null, the GTFS folder
        name will be used as the unique agency ID
    Returns
    -------
    stops_df, routes_df, trips_df, stop_times_df, calendar_df,
    calendar_dates_df : pandas.DataFrame
        Returns all input GTFS DataFrames with a unique agency ID column
        and value for all tables and records.
    """
    # TODO: cover edge cases such as in GTFS madison feed stops.txt comes
    #  with agency ID col that doesnt exist in routes.txt which means it
    #  never shows up in resulting stops agency ID columns. If file has
    #  agency_id then use it, if not then do the merges.

    start_time = time.time()
    feed_folder_name = os.path.basename(feed_folder)
    agency_file_path = os.path.join(feed_folder, 'agency.txt')

    df_dict = {'stops': stops_df,
               'routes': routes_df,
               'trips': trips_df,
               'stop_times': stop_times_df}

    optional_df_dict = {'calendar': calendar_df,
                        'calendar_dates': calendar_dates_df}
    # if optional calendar or calendar_dates_df are not empty then add it
    # to the processing list
    for name, df in optional_df_dict.items():
        if df.empty is False:
            df_dict.update({name: df})

    # check if agency.txt file exists
    path_absent = os.path.exists(agency_file_path) is False
    # check if 'agency_id' col exists in agency.txt
    agency_absent = 'agency_id' not in agency_df.columns

    # if agency.txt or 'agency_id' col does not exist, generate
    # 'unique_agency_id' using the name of the GTFS feed folder
    if (path_absent or agency_absent) and nulls_as_folder:
        # apply the unique agency ID to all GTFS dfs
        for name, df in df_dict.items():
            unique_agency_id = _generate_unique_feed_id(feed_folder)
            df['unique_agency_id'] = unique_agency_id
            df_dict[name] = df
        log('The agency.txt or agency_id column was not found. The unique '
            'agency ID: {} was generated using the name of the folder '
            'containing the GTFS feed text files for tables: {}.'.format(
                unique_agency_id, list(df_dict.keys())))

    # if agency.txt does not exist and nulls_as_folder is False throw error
    elif path_absent and nulls_as_folder is False:
        raise ValueError(
            'No agency.txt file was found in {}. Add the missing file to '
            'folder or set nulls_as_folder to True.'.format(feed_folder))

    # assumes agency.txt and 'agency_id' exists and begins process of
    # generating unique_agency_id using these data
    else:
        cols = ['agency_id', 'agency_name']
        # if 'agency_name' and 'agency_id' cols do not exist throw error
        if not all(col in agency_df.columns for col in cols):
            raise ValueError('Both agency_name and agency_id columns were not '
                             'found in agency.txt.')

        # replace nans with ''
        agency_df[cols] = agency_df[cols].fillna('')
        # determine extent of missing values
        agency_id_str = agency_df['agency_id'].astype(str)
        agency_name_str = agency_df['agency_name'].astype(str)
        all_missing = (agency_id_str.str.isspace().all() and
                       agency_name_str.str.isspace().all()) or \
                      (agency_df[cols].values == '').all()
        any_missing = (agency_id_str.str.isspace().any() and
                       agency_name_str.str.isspace().any()) or \
                      (agency_df[cols].values == '').any()

        # if 'agency_name' and 'agency_id' both have no records or both contain
        # nulls or blank str throw error
        cnt_agency_records = len(agency_df)
        if cnt_agency_records == 0 or all_missing:
            raise ValueError('agency.txt has no agency_id or agency_name '
                             'values.')

        # if 'agency_name' has only 1 record: use value in 'agency_name'
        # for 'unique_agency_id'
        if cnt_agency_records == 1:
            agency_txt_name = agency_df['agency_name'][0]
            # if 'agency_name' is null or a blank str throw error
            if agency_txt_name.isspace() or agency_txt_name == '':
                raise ValueError('Null value in agency_name was found.')

            # simplify and standardize the agency name
            agency_snake_case = sub(r'\s+', '_', agency_txt_name)
            unique_agency_id = agency_snake_case.replace('&', 'and').lower()

            # apply the unique agency ID to all GTFS dfs
            for name, df in df_dict.items():
                df['unique_agency_id'] = unique_agency_id
                df_dict[name] = df
            log('The unique agency ID: {} was generated using the name of '
                'the agency in the agency.txt file for tables: {}.'.format(
                    unique_agency_id, list(df_dict.keys())))

        # if 'agency_name' has more than 1 record: use value in 'agency_name'
        # and agency_id for 'unique_agency_id' and then use 'agency_id' in
        # routes table to join agency information downstream to all other
        # tables in GTFS feed
        elif cnt_agency_records > 1:
            # if any records in 'agency_id' and 'agency_name' have nulls
            # or blank strs throw error
            if any_missing:
                raise ValueError(
                    'Null values found in agency_id and agency_name.')

            # subset DataFrames to only cols of interest for processing
            subset_agency_df = agency_df[['agency_id', 'agency_name']]
            subset_routes_df = routes_df[['route_id', 'agency_id']]
            subset_stop_times_df = stop_times_df[['trip_id', 'stop_id']]
            subset_trips_df = trips_df[['trip_id', 'route_id']]
            subset_trips_df_w_sid = trips_df[
                ['trip_id', 'route_id', 'service_id']]

            # below, apply the unique agency ID to all GTFS dfs via
            # merge operations

            # if optional calendar_dates_df is not empty then process it
            if calendar_dates_df.empty is False:
                calendar_dates_replacement_df = _calendar_dates_agencyid(
                    calendar_dates_df=calendar_dates_df,
                    routes_df=subset_routes_df,
                    trips_df=subset_trips_df_w_sid,
                    agency_df=subset_agency_df,
                    feed_folder=feed_folder)

            # if optional calendar_df is not empty then process it
            if calendar_df.empty is False:
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

            # update the df_dict dfs with the new dfs with unique agency IDs
            df_dict = {'stops': stops_replacement_df,
                       'routes': routes_replacement_df,
                       'trips': trips_replacement_df,
                       'stop_times': stop_times_replacement_df}

            # if optional calendar or calendar_dates_df are not empty then
            # add it to the processing list
            if calendar_df.empty is False:
                df_dict.update(
                    {'calendar': calendar_replacement_df})
            if calendar_dates_df.empty is False:
                df_dict.update(
                    {'calendar_dates': calendar_dates_replacement_df})

            log('agency.txt agency_name column has more than one agency name '
                'listed. Unique agency ID was assigned using the agency ID '
                'and associated agency name.')

    # if there are still nulls in the generated 'unique_agency_id' column,
    # assign ID as concat of 'multiple_operators_' and GTFS folder name
    for name, df in df_dict.items():
        # if nans are string convert to null
        df['unique_agency_id'].replace('nan', np.nan, inplace=True)
        null_cnt = df['unique_agency_id'].isnull().sum()
        if null_cnt > 0:
            df_cnt = len(df)
            unique_feed_id = _generate_unique_feed_id(feed_folder)
            unique_agency_id = ''.join(
                ['multiple_operators_', unique_feed_id])
            df['unique_agency_id'].fillna(unique_agency_id, inplace=True)
            null_pct = round((float(null_cnt) / float(df_cnt)) * 100, 2)
            log('There are {:,} null values ({:,.2f}% of {:,} total) in {} '
                'table without an unique agency ID. These records will be '
                'labeled as multiple_operators_{} with the GTFS file folder '
                'name.'.format(null_cnt, null_pct, df_cnt, name,
                               feed_folder_name))
            df_dict[name] = df

    #  TODO: this dict may be redundant check if can refactor
    optional_df_dict = {
        'calendar': calendar_df,
        'calendar_dates': calendar_dates_df}
    # if optional calendar or calendar_dates_df are empty then return
    # the original empty df with an empty agency ID column
    for name, df in optional_df_dict.items():
        if df.empty:
            df['unique_agency_id'] = np.nan
            df_dict.update({name: df})

    # ensure returned items in list are always in the same expected order
    df_list = [df_dict['stops'], df_dict['routes'], df_dict['trips'],
               df_dict['stop_times'], df_dict['calendar'],
               df_dict['calendar_dates']]

    log('Unique agency ID operation complete. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))
    return df_list


def _add_unique_gtfsfeed_id(stops_df, routes_df, trips_df,
                            stop_times_df, calendar_df, calendar_dates_df,
                            feed_folder, feed_number):
    """
    Create an unique GTFS feed specific ID for all GTFS feed DataFrames to
    enable tracking of specific feeds

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trips DataFrame
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    calendar_df : pandas:DataFrame
        calendar DataFrame
    calendar_dates_df : pandas:DataFrame
        calendar dates DataFrame
    feed_folder : str
        name of GTFS feed folder
    feed_number : int
        current number iteration of GTFS feed being read in root directory
    Returns
    -------
    stops_df, routes_df, trips_df, stop_times_df, calendar_df,
    calendar_dates_df : pandas.DataFrame
    """
    start_time = time.time()

    df_dict = {'stops': stops_df,
               'routes': routes_df,
               'trips': trips_df,
               'stop_times': stop_times_df}

    optional_df_dict = {'calendar': calendar_df,
                        'calendar_dates': calendar_dates_df}
    # if optional calendar or calendar_dates_df are not empty then add it
    # to the processing list
    for name, df in optional_df_dict.items():
        if df.empty is False:
            df_dict.update({name: df})

    # standardize feed_folder name
    feed_folder = _generate_unique_feed_id(feed_folder)

    for name, df in df_dict.items():
        # create new unique_feed_id column based on the name of the feed folder
        df['unique_feed_id'] = '_'.join([feed_folder, str(feed_number)])
        df_dict[name] = df

    # if optional calendar or calendar_dates_df are empty then return
    # the original empty df with an empty agency ID column
    for name, df in optional_df_dict.items():
        if df.empty:
            df['unique_feed_id'] = np.nan
            df_dict.update({name: df})

    # ensure returned items in list are always in the same expected order
    df_list = [df_dict['stops'], df_dict['routes'], df_dict['trips'],
               df_dict['stop_times'], df_dict['calendar'],
               df_dict['calendar_dates']]

    log('Unique GTFS feed ID operation complete. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))
    return df_list


def _timetoseconds(df, time_cols):
    """
    Convert default GTFS stop time departure and arrival times from 24 hour
    clock to seconds past midnight

    Parameters
    ----------
    df : pandas:DataFrame
        stop time DataFrame
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
        raise ValueError('{} is not a list.'.format(time_cols))

    for col in time_cols:
        time_list = df[col].fillna('').tolist()
        for n, i in enumerate(time_list):
            if len(i) == 7:
                time_list[n] = '0' + i
            if (len(i) != 8) & (len(i) != 0) & (len(i) != 7):
                raise ValueError(
                    'Check formatting of value: {} as it is in the incorrect '
                    'format and should be 8 character '
                    'string 00:00:00.'.format(i))
        df_fixed = pd.DataFrame(time_list, columns=[col])
        df[col] = df_fixed[col]

        h = pd.to_numeric(df[col].str[0:2])
        invalid_vals = h.loc[h > 48]
        cnt = len(invalid_vals)
        if cnt > 0:
            log('Warning: {:,} hour value(s) are greater than 48. '
                'Max value is: {}. Check value(s) if this is '
                'unexpected.'.format(cnt, invalid_vals.max()),
                level=lg.WARNING)
        m = pd.to_numeric(df[col].str[3:5])
        invalid_vals = m.loc[m > 60]
        cnt = len(invalid_vals)
        if cnt > 0:
            log('Warning: {:,} minute value(s) are greater than 60. '
                'Max value is: {}. Check value(s) if this is '
                'unexpected.'.format(cnt, invalid_vals.max()),
                level=lg.WARNING)
        s = pd.to_numeric(df[col].str[6:8])
        invalid_vals = s.loc[s > 60]
        cnt = len(invalid_vals)
        if cnt > 0:
            log('Warning: {:,} second value(s) are greater than 60. '
                'Max value is: {}. Check value(s) if this is '
                'unexpected.'.format(cnt, invalid_vals.max()),
                level=lg.WARNING)
        col_series = (h * 60 * 60) + (m * 60) + s
        series_df = col_series.to_frame(name=''.join([col, '_sec']))

        # check if times are negative if so display warning
        if series_df.values.any() < 0:
            log('Warning: Some stop times in {} column are negative. '
                'Time should be positive. Suggest checking original '
                'GTFS feed stop_time file before proceeding.'.format(col),
                level=lg.WARNING)

        concat_series_df = pd.concat([concat_series_df, series_df], axis=1)

    final_df = pd.merge(df, concat_series_df, how='left', left_index=True,
                        right_index=True, sort=False, copy=False)

    # replace the '' inserted during processing with nans to ensure column
    # values are the same as they were before processing
    for col in time_cols:
        final_df[col] = final_df[col].replace({'': np.nan})

    log('Successfully converted {} to seconds past midnight and appended new '
        'columns to stop_times. '
        'Took {:,.2f} seconds.'.format(time_cols, time.time() - start_time))

    return final_df


def _apply_gtfs_definition(df, desc_dict):
    """
    Helper function to apply a dictionary with value to description mappings
    for columns in a DataFrame

    Parameters
    ----------
    df : pandas:DataFrame
        DataFrame to add description column to
    desc_dict : dict
        dictionary with mapping between df values and description values
        in desc_dict where: {keys are values found in df columns: values
         are string descriptions of those values}

    Returns
    -------
    df : pandas.DataFrame
    """
    for col_name, value_map in desc_dict.items():
        if col_name in df.columns:
            desc_col = '{}_desc'.format(col_name)
            if np.nan in value_map.keys():
                df[desc_col] = df[col_name].map(value_map.get)
                df.loc[df[col_name].isnull(), desc_col] = value_map[np.nan]
            else:
                df[desc_col] = df[col_name].map(value_map.get)
            missing_values = [item for item in df[col_name].dropna().unique()
                              if item not in value_map.keys()]
            # if there are values in df that config has no mapping for,
            # warn the user of this
            if len(missing_values):
                warning_msg = (
                    'Value(s): {} in {} are missing from configuration '
                    'description mapping and will not be defined.')
                log(warning_msg.format(missing_values, col_name),
                    level=lg.WARNING)
    return df


def _add_txt_definitions(stops_df, routes_df, stop_times_df, trips_df):
    """
    Append GTFS definitions to stops, routes, stop times, and trips DataFrames

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    trips_df : pandas:DataFrame
        trip DataFrame

    Returns
    -------
    stops_df, routes_df, stop_times_df, trips_df : pandas.DataFrame
    """
    # define description value mappings from config
    stops_desc = {'location_type': config._STOPS_LOCATION_TYPE_LOOKUP,
                  'wheelchair_boarding': config._STOPS_WHEELCHAIR_BOARDINGS}
    routes_desc = {'route_type': config._ROUTES_MODE_TYPE_LOOKUP}
    stop_times_desc = {'pickup_type': config._STOP_TIMES_PICKUP_TYPE,
                       'drop_off_type': config._STOP_TIMES_DROP_OFF_TYPE,
                       'timepoint': config._STOP_TIMES_TIMEPOINT}
    trips_desc = {'bikes_allowed': config._TRIPS_BIKES_ALLOWED,
                  'wheelchair_accessible': config._TRIPS_WHEELCHAIR_ACCESSIBLE}

    # apply value mappings to dfs
    stops_df = _apply_gtfs_definition(
        df=stops_df, desc_dict=stops_desc)
    routes_df = _apply_gtfs_definition(
        df=routes_df, desc_dict=routes_desc)
    stop_times_df = _apply_gtfs_definition(
        df=stop_times_df, desc_dict=stop_times_desc)
    trips_df = _apply_gtfs_definition(
        df=trips_df, desc_dict=trips_desc)

    log('Added descriptive definitions to stops, routes, stop_times, '
        'and trips tables.')

    return stops_df, routes_df, stop_times_df, trips_df


def _append_route_type(stops_df, stop_times_df, routes_df,
                       trips_df, info_to_append):
    """
    Append GTFS route type definitions to stops and stop times DataFrames

    Parameters
    ----------
    stops_df : pandas:DataFrame
        stops DataFrame
    stop_times_df : pandas:DataFrame
        stop times DataFrame
    routes_df : pandas:DataFrame
        routes DataFrame
    trips_df : pandas:DataFrame
        trip DataFrame
    info_to_append : {'route_type_to_stops', 'route_type_to_stop_times'}
        the type of information to append

    Returns
    -------
    stops_df or stop_times_df : pandas.DataFrame
    """
    valid_info_to_append = ['route_type_to_stops', 'route_type_to_stop_times']
    if info_to_append not in valid_info_to_append:
        raise ValueError('{} is not a valid parameter.'.format(info_to_append))

    if info_to_append == 'route_type_to_stops':
        tmp1 = pd.merge(trips_df, routes_df, how='left', on='route_id',
                        sort=False, copy=False)
        merged_df = pd.merge(stop_times_df, tmp1, how='left', on='trip_id',
                             sort=False, copy=False)
        merged_df.drop_duplicates(subset='stop_id', keep='first', inplace=True)

        stops_df = pd.merge(stops_df, merged_df[['route_type', 'stop_id']],
                            how='left', on='stop_id', sort=False, copy=False)

        log('Appended route type to stops.')

        return stops_df

    if info_to_append == 'route_type_to_stop_times':
        merged_df = pd.merge(trips_df, routes_df, how='left', on='route_id',
                             sort=False, copy=False)
        merged_df.drop_duplicates(subset='trip_id', keep='first', inplace=True)

        stop_times_df = pd.merge(stop_times_df,
                                 merged_df[['route_type', 'trip_id']],
                                 how='left', on='trip_id', sort=False,
                                 copy=False)

        log('Appended route type to stop_times.')

        return stop_times_df


def _generate_unique_agency_id(df, col_name):
    """
    Generate unique agency ID

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
    Generate unique feed ID from a GTFS feed directory

    Parameters
    ----------
    feed_folder : str
        full directory path for the GTFS feed folder

    Returns
    -------
    folder_snake_case_no_amps : str
    """

    folder_name = os.path.split(feed_folder)[1]
    # replace all runs of spaces with a single underscore and replace all
    # ampersands
    folder_snake_case_no_amps = sub(r'\s+', '_', folder_name).replace(
        '&', 'and')

    return folder_snake_case_no_amps.lower()


def _remove_whitespace(df, textfile, col_list=None):
    """
    Remove leading and trailing spaces in values in specified DataFrame
    columns and or also remove spaces in column names.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to process
    textfile : str
        name of text file
    col_list : list, optional
        If specified, list of column names as strings to check for
        whitespaces in values

    Returns
    -------
    df : pandas.DataFrame
    """

    # remove leading and trailing spaces in column names
    before_cols = sorted(list(df.columns))
    df.rename(columns=lambda x: x.strip(), inplace=True)
    after_cols = sorted(list(df.columns))
    if before_cols != after_cols:
        cols_with_spaces = list(set(before_cols) - set(after_cols))
        log('GTFS file: {} column(s): {} had leading and or trailing '
            'whitespace in column names. Spaces have been removed.'.format(
             textfile, cols_with_spaces))

    # remove leading and trailing spaces in values for columns in list
    if col_list:
        df_copy = df.copy()
        for col in col_list:
            before_count = df_copy[col].str.len().sum()
            df_copy[col] = df_copy[col].str.rstrip().str.lstrip()
            after_count = df_copy[col].str.len().sum()
            # only perform whitespace strip on columns that need it
            if before_count != after_count:
                df[col] = df[col].str.rstrip().str.lstrip()
                log('GTFS file: {} column: {} had leading and or trailing '
                    'whitespace in its values. Spaces have been '
                    'removed.'.format(textfile, col))
    return df


def _list_raw_txt_columns(file):
    """
    Return a list of columns names that exist in txt file.

    Parameters
    ----------
    file : str
        full path and filename of file to read

    Returns
    -------
    df : list
        list of columns in txt file
    """
    df = pd.read_csv(file)
    return list(df.columns)
