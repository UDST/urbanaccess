from __future__ import division
import pandas as pd
import time
from datetime import datetime as dt
import logging as lg

from urbanaccess.utils import log


def _trip_schedule_selector_validate_params(calendar_dates_df, params):
    """
    Validate parameters passed to the _calendar_service_id_selector()
    function.

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    params : dict
        Parameters to validate as a dict

    Returns
    -------
    Nothing

    """
    has_cal = params['has_cal']
    has_cal_dates = params['has_cal_dates']

    day = params['day']
    date = params['date']
    date_range = params['date_range']
    cal_dates_lookup = params['cal_dates_lookup']

    has_day_param = params['has_day_param']
    has_date_param = params['has_date_param']
    has_date_range_param = params['has_date_range_param']
    has_cal_dates_param = params['has_cal_dates_param']
    has_day_and_date_range_param = params['has_day_and_date_range_param']

    # only one search parameter can be used at a time
    all_param_list = [has_day_param, has_date_param, has_date_range_param]
    all_param_true_cnt = all_param_list.count(True)

    # Note: combination of day and date_range is valid
    if all_param_true_cnt > 1 and not has_day_and_date_range_param:
        raise ValueError(
            "Only one parameter: 'day', 'date', or 'date_range' can be used "
            "at a time or both 'day' and 'date_range' can be used.")

    if has_day_param:
        if not isinstance(day, str):
            raise ValueError('Day must be a string.')
        valid_days = ['monday', 'tuesday', 'wednesday', 'thursday',
                      'friday', 'saturday', 'sunday']
        if day not in valid_days:
            valid_days_str = str(valid_days).replace('[', '').replace(']', '')
            raise ValueError(
                'Day: {} is not a supported day. Must be one of lowercase '
                'strings: {}.'.format(day, valid_days_str))

    if has_date_param:
        if not isinstance(date, str):
            raise ValueError('Date must be a string.')
        try:
            dt.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Date: {} is not a supported date format. "
                             "Expected format: 'YYYY-MM-DD'.".format(date))

    if has_date_range_param:
        if len(date_range) != 2:
            raise ValueError("Date range {} must have a "
                             "length of 2.".format(date_range))
        for date_item in date_range:
            if not isinstance(date_item, str):
                raise ValueError('Dates in date range must be a string.')
            try:
                dt.strptime(date_item, '%Y-%m-%d')
            except ValueError:
                raise ValueError("Date: {} in date range: {} is not a "
                                 "supported date format. Expected format: "
                                 "'YYYY-MM-DD'.".format(date_item, date_range))
        item_1 = dt.strptime(date_range[0], '%Y-%m-%d')
        item_2 = dt.strptime(date_range[1], '%Y-%m-%d')
        if item_1 > item_2:
            raise ValueError("First date in date range {} must be less than "
                             "the last date.".format(date_range))

    if has_cal_dates_param:
        if has_cal_dates:
            if not isinstance(cal_dates_lookup, dict):
                raise ValueError(
                    'calendar_dates_lookup parameter must be a dictionary.')
            for key in cal_dates_lookup.keys():
                if not isinstance(key, str):
                    raise ValueError('calendar_dates_lookup key: {} '
                                     'must be a string.'.format(key))
                if isinstance(cal_dates_lookup[key], str):
                    value = [cal_dates_lookup[key]]
                else:
                    if not isinstance(cal_dates_lookup[key], list):
                        raise ValueError(
                            'calendar_dates_lookup value: {} must be a '
                            'string or a list of strings.'.format(
                                cal_dates_lookup[key]))
                    else:
                        value = cal_dates_lookup[key]

                for string in value:
                    if not isinstance(string, str):
                        raise ValueError('calendar_dates_lookup value: {} '
                                         'must contain strings.'.format(value))
            for col_name_key, string_value in cal_dates_lookup.items():
                if col_name_key not in calendar_dates_df.columns:
                    raise ValueError('Column: {} not found in calendar_dates '
                                     'DataFrame.'.format(col_name_key))
                if col_name_key not in calendar_dates_df.select_dtypes(
                        include=[object]).columns:
                    raise ValueError('Calendar_dates column: {} must be '
                                     'object type.'.format(col_name_key))
        else:
            raise ValueError("Calendar_dates is empty. Unable to use the "
                             "'calendar_dates_lookup' parameter. Set to None.")

    if not has_cal:
        log('     Calendar table has no records and will not be used to '
            'select service_ids.')
    if not has_cal_dates:
        log('     Calendar dates table has no records and will not be used to '
            'select service_ids.')


def _cal_date_dt_conversion(df, date_cols):
    """
    Convert columns with dates as strings to datetime64[ns] format. Expected
    incoming dates in format:'%y%m%d' (YYYY-MM-DD)

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame that contains date columns to convert to DateTime
        format. Typically this will be the calendar DataFrame
    date_cols : list
        List of strings denoting date columns with string values to convert
        to DateTime format. For example: ['start_date', 'end_date']

    Returns
    -------
    df : pandas.DataFrame
        Returns the same passed df DataFrame with the columns specified in
        date_cols converted to datetime64[ns] format.

    """
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(
                df[col], format='%y%m%d', infer_datetime_format=True)
        except ValueError:
            raise ValueError("Column: {} has values that are not in a "
                             "supported date format. Expected format: "
                             "'YYYY-MM-DD'.".format(col))
    return df


def _select_calendar_service_ids(calendar_df, params):
    """
    Wrapper function to select service IDs from the calendar.txt that are
    active according to the parameters specified

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        Calendar DataFrame
    params : dict
        Parameters denoting how service IDs should be selected from table
        and supporting information to guide the selection process

    Returns
    -------
    srvc_ids : list
        returns a list of service IDs from the calendar.txt that are
        active according to the parameters specified

    """
    # collect service IDs that match search parameters in calendar.txt
    msg = 'Selecting service_ids from calendar'
    srvc_ids = []

    day = params['day']
    date = params['date']
    date_range = params['date_range']
    has_day_param = params['has_day_param']
    has_date_param = params['has_date_param']
    has_date_range_param = params['has_date_range_param']
    has_day_and_date_range_param = params['has_day_and_date_range_param']

    # convert cols to datetime expected format: 'yyyymmdd' e.g.: '20130825'
    date_cols = ['start_date', 'end_date']
    calendar_df = _cal_date_dt_conversion(df=calendar_df, date_cols=date_cols)

    if has_day_param:
        srvc_ids_day = _select_calendar_service_ids_by_day(
            calendar_df, msg, day)
        srvc_ids = srvc_ids_day.copy()

    if has_date_range_param:
        srvc_ids_date_range = _select_calendar_service_ids_by_date_range(
            calendar_df, msg, date_range)
        srvc_ids = srvc_ids_date_range.copy()

    if has_date_param:
        srvc_ids_date = _select_calendar_service_ids_by_date(
            calendar_df, msg, date)
        srvc_ids = srvc_ids_date.copy()

    if has_day_and_date_range_param:
        # get the intersection between service IDs found via 'day' and
        # 'date_range' params
        print_msg = '{} that are active within date_range: {} on day: {}...'
        log(print_msg.format(msg, date_range, day))
        intersect_srvc_ids = _intersect_cal_service_ids(
            dict_1={'day': srvc_ids_day},
            dict_2={'date range': srvc_ids_date_range})
        srvc_ids = intersect_srvc_ids.copy()

    return srvc_ids


def _print_cal_service_ids_len(srvc_ids, table_name):
    """
    Helper function to print counts of service IDs

    Parameters
    ----------
    srvc_ids : list
        list of selected service IDs to generate counts for print message
    table_name : str
        name of table where service IDs were selected from to include
        in print message

    Returns
    -------
    Nothing

    """
    cnt_msg = '          {:,} service_id(s) were selected from {}.'
    warning_msg = '          Warning: No service_ids were selected from {}.'
    srvc_ids_cnt = len(srvc_ids)
    uni_srvc_ids_cnt = len(set(srvc_ids))
    # TODO: print count of records and unique IDs
    if srvc_ids_cnt > 0:
        log(cnt_msg.format(uni_srvc_ids_cnt, table_name))
    else:
        log(warning_msg.format(table_name), level=lg.WARNING)


def _intersect_cal_service_ids(dict_1, dict_2, verbose=True):
    """
    Return the intersection of two lists of service IDs that are set as
    the values in two dicts

    Parameters
    ----------
    dict_1 : dict
        dict 1 where the key is the origin of the service IDs for informative
        purposes and the value is a list of service IDs.
        Example: {'date': srvc_ids_date}
    dict_2 : dict
        dict 2 where the key is the origin of the service IDs for informative
        purposes and the value is a list of service IDs.
        Example: {'day': srvc_ids_date}
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids : list
        list of intersected and unique selected service IDs
    """
    srvc_id_1_name, srvc_id_1 = list(dict_1.items())[0]
    srvc_id_2_name, srvc_id_2 = list(dict_2.items())[0]
    srvc_ids = list(set(srvc_id_1) & set(srvc_id_2))
    if verbose:
        srvc_ids_len = len(srvc_ids)
        list_1_len = len(srvc_id_1)
        list_2_len = len(srvc_id_2)
        msg = ('          Intersection between service_ids selected with: '
               '{} parameter (count: {:,}) and {} parameter (count: {:,}) '
               'returned {:,} total service_id(s).'.format(
                srvc_id_1_name, list_1_len, srvc_id_2_name, list_2_len,
                srvc_ids_len))
        log(msg)
    return srvc_ids


def _select_calendar_service_ids_by_day(
        calendar_df, msg, day=None, verbose=True):
    """
    Global search using day in calendar.txt to select service IDs where
    day specified = 1 where 1 = service is active; 0 = service inactive

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        Calendar DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    day : {'monday', 'tuesday', 'wednesday', 'thursday',
    'friday', 'saturday', 'sunday'}
        day of the week to extract active service IDs in calendar.
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids : list
        list of active service IDs
    """
    if verbose:
        log('     {} that are active on day: {}...'.format(msg, day))
    subset_cal_df = calendar_df.loc[calendar_df[day] == 1]
    srvc_ids = subset_cal_df['unique_service_id'].to_list()

    if verbose:
        _print_cal_service_ids_len(srvc_ids, table_name='calendar')
    return srvc_ids


def _select_calendar_service_ids_by_date_range(
        calendar_df, msg, date_range=None):
    """
    Global search using date_range to subset records in calendar.txt
    using 'start_date' and 'end_date' columns when GTFS feed has
    seasonal service_ids select service IDs where
    'start_date' and 'end_date' are within the date_range

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        Calendar DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    date_range : list
        date range to extract active service IDs in calendar table
        specified as a 2 element list of strings where the first element
        is the first date and the second element is the last date in the
        date range. Dates should be specified as strings in YYYY-MM-DD format,
        e.g. ['2013-03-09', '2013-09-01']. Service IDs that are active
        within the date range specified (where the date range is within
        the date range given by the start_date and end_date columns)
        will be extracted.

    Returns
    -------
    srvc_ids : list
        list of active service IDs
    """
    log('     {} that are active within date_range: {}...'.format(
        msg, date_range))

    # select service_ids that are within the date range
    service_ids = calendar_df['unique_service_id']
    start_d_mask = service_ids.loc[calendar_df['start_date'].between(
        date_range[0], date_range[1], inclusive=True)].to_list()
    end_d_mask = service_ids.loc[calendar_df['end_date'].between(
        date_range[0], date_range[1], inclusive=True)].to_list()
    ids_in_range = service_ids.loc[
        service_ids.isin(start_d_mask) & service_ids.isin(end_d_mask)]
    subset_cal_df = calendar_df.loc[service_ids.isin(ids_in_range)]
    srvc_ids = subset_cal_df['unique_service_id'].to_list()

    if len(srvc_ids) == 0:
        date_rngs = _calendar_date_ranges(calendar_df)
        print_msg = (
            '          Warning: Date range: {} does not contain any of the '
            'date ranges available in the calendar. Date ranges available '
            'are: \n          {}'.format(date_range, date_rngs))
        log(print_msg, level=lg.WARNING)
    else:
        date_rngs = _calendar_date_ranges(
            calendar_df.loc[calendar_df['unique_service_id'].isin(srvc_ids)])
        print_msg = (
            '     Date range: {} contains the following date ranges '
            'available in the calendar. Date ranges available '
            'are: \n          {}'.format(date_range, date_rngs))
        log(print_msg)

    _print_cal_service_ids_len(srvc_ids, table_name='calendar')
    return srvc_ids


def _calendar_date_ranges(calendar_df, for_print=True):
    """
    Helper function to return date ranges inside of the calendar.txt table
    using the columns 'start_date' and 'end_date'. Returns date ranges
    in string format to be used in print statements and in pd.Timestamp
    format to be used in downstream functions.

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        Calendar DataFrame
    for_print : boolean, optional
        If True, returns date ranges in string format to be used in print
        statements where each element takes format:
        '{start timestamp as string} to {start timestamp as string}'.
        If False, returns date ranges in pd.Timestamp format as a
        list of dicts where keys are the 'start_date' and
        'end_date' for each range: {'start_date': pd.Timestamp,
        'end_date': pd.Timestamp}.
    Returns
    -------
    date_rngs : list
        If for_print is True, returns date ranges in string format to be used
        in print statements where each element takes format:
        '{start timestamp as string} to {end timestamp as string}'.
        If False, returns date ranges in pd.Timestamp format as a
        list of dicts where keys are the 'start_date' and
        'end_date' for each range: {'start_date': pd.Timestamp,
        'end_date': pd.Timestamp}.
    """
    date_cols = ['start_date', 'end_date']
    unique_range = calendar_df[date_cols].drop_duplicates(subset=date_cols)
    # build list of ranges that exist in table to print to user
    if for_print:
        for col in date_cols:
            # convert to string for prints
            unique_range[col] = unique_range[col].dt.strftime('%Y-%m-%d')
            unique_range[col] = unique_range[col].astype('str')
        date_rngs = list(unique_range['start_date'].str.cat(
            unique_range['end_date'], sep=' to ').values)
    # build dict of ranges that exist in table to use elsewhere
    else:
        # keep in datetime format
        date_rngs = unique_range.to_dict('records')
    return date_rngs


def _select_calendar_service_ids_by_date(
        calendar_df, msg, date=None, verbose=True):
    """
    Global search using date to subset records in calendar.txt
    using 'start_date' and 'end_date' columns when GTFS feed has
    seasonal service_ids, select service IDs where date is within the
    'start_date' and 'end_date' and then select IDs that are active on
    the day of the week the date represents

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        Calendar DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    date : str
        date to extract active service IDs in calendar table specified as
        a string in YYYY-MM-DD format, e.g. '2013-03-09'. service IDs
        that are active on the date specified (where the date is within the
        date range given by the start_date and end_date columns) and
        that date's day of the week will be extracted.
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids : list
        list of active service IDs
    """
    # convert date to day of the week
    day = dt.strptime(date, '%Y-%m-%d').strftime('%A').lower()

    if verbose:
        log('     {} that are active on date: {} '
            '(using a {} schedule)...'.format(msg, date, day))

    # select date ranges where the 'date' is within the bounds of
    # 'start_date' and 'end_date'
    service_ids = calendar_df['unique_service_id']
    date_dt = pd.to_datetime(date)
    start_d_mask = service_ids.loc[
        calendar_df['start_date'] <= date_dt].to_list()
    end_d_mask = service_ids.loc[
        calendar_df['end_date'] >= date_dt].to_list()
    ids_in_range = service_ids.loc[
        service_ids.isin(start_d_mask) & service_ids.isin(end_d_mask)]
    subset_cal_df = calendar_df.loc[service_ids.isin(ids_in_range)]
    srvc_ids_date = subset_cal_df['unique_service_id'].to_list()

    if len(srvc_ids_date) == 0:
        date_rngs = _calendar_date_ranges(calendar_df)
        if verbose:
            print_msg = (
                '          Warning: Date: {} does not fall within any '
                'date range(s) available in the calendar. '
                'Available date range(s) found in the calendar '
                'are: \n          {}')
            log(print_msg.format(date, date_rngs), level=lg.WARNING)
    else:
        date_rngs = _calendar_date_ranges(
            calendar_df.loc[
                calendar_df['unique_service_id'].isin(srvc_ids_date)])
        if verbose:
            print_msg = (
                '     Date: {} is within the available date range(s) '
                'in the calendar. Available date range(s) found in the '
                'calendar are: \n          {}')
            log(print_msg.format(date, date_rngs))

    # use the date's day of the week to then select service_ids that are
    # active on that day
    srvc_ids_day = _select_calendar_service_ids_by_day(
        calendar_df, msg, day, verbose=False)
    # find the intersection of where service_ids are active on the date and
    # the day of week to get the final list of IDs
    srvc_ids = _intersect_cal_service_ids(
        dict_1={'date': srvc_ids_date},
        dict_2={'day': srvc_ids_day},
        verbose=verbose)
    if verbose:
        _print_cal_service_ids_len(srvc_ids, table_name='calendar')
    return srvc_ids


def _parse_cal_dates_exception_type(cal_dates_df, verbose=True):
    """
    Helper function to parse selected calendar_dates.txt service IDs
    from unique_service_id column by exception_type

    Parameters
    ----------
    cal_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids_add : list
        list of selected service IDs to add
    srvc_ids_del : list
        list of selected service IDs to remove
    """
    # 1 - Service has been added for the specified date.
    # 2 - Service has been removed for the specified date.
    id_col = 'unique_service_id'
    exception_col = 'exception_type'

    srvc_ids_add = cal_dates_df[id_col].loc[
        cal_dates_df[exception_col] == '1'].to_list()
    srvc_ids_del = cal_dates_df[id_col].loc[
        cal_dates_df[exception_col] == '2'].to_list()
    srvc_ids_add = list(set(srvc_ids_add))
    srvc_ids_del = list(set(srvc_ids_del))
    if verbose:
        log('          {:,} service_id(s) were selected from calendar_dates '
            'with exception_type 1 '
            '(service added for specified date).'.format(len(srvc_ids_add)))
        log('          {:,} service_id(s) were selected from calendar_dates '
            'with exception_type 2 '
            '(service removed for specified date).'.format(len(srvc_ids_del)))
    return srvc_ids_add, srvc_ids_del


def _select_calendar_dates_service_ids_by_day(
        calendar_dates_df, msg, day=None):
    """
    Select service IDs in calendar_dates.txt that are active on dates
    that match the day of the week specified will be where
    exception_type = 1 (service is added for date).

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    day : {'monday', 'tuesday', 'wednesday', 'thursday',
    'friday', 'saturday', 'sunday'}
        day of the week to extract service IDs in calendar_dates table.
        Service IDs that are active on dates that match the day of the week
        specified will be extracted where exception_type = 1 (service is added
         for date).

    Returns
    -------
    srvc_ids : list
        list of active service IDs
    """

    # select all service ids that are active on this day of week,
    # that are inclusive

    # determine day of the week for each date
    log('     {} that are active on day: {}...'.format(msg, day))
    # TODO: check if there is an issue with other versions of pandas such that
    #  requiring pandas > 0.18.1 is needed to run the line below:
    calendar_dates_df['_ua_weekday'] = calendar_dates_df[
        'date'].dt.day_name().str.lower()
    subset_df = calendar_dates_df.loc[calendar_dates_df['_ua_weekday'] == day]
    srvc_ids = subset_df['unique_service_id'].loc[
        subset_df['exception_type'] == '1'].to_list()
    _print_cal_service_ids_len(srvc_ids, table_name='calendar dates')

    return srvc_ids


def _select_calendar_dates_service_ids_by_date(
        calendar_dates_df, msg, date=None, verbose=True):
    """
    Search using date to subset records in calendar_dates.txt
    using 'date' column to select service IDs regardless of exception_type

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    date : str
        date to extract service IDs in calendar dates table specified as
        a string in YYYY-MM-DD format, e.g. '2013-03-09'. calendar_dates
        service IDs that are active within the date range specified will
        be selected where exception_type = 1
        (service is added for date) and inactive within
        the date range specified will be selected where exception_type = 2
       (service is removed for date).
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids_add : list
        list of selected service IDs to add
    srvc_ids_del : list
        list of selected service IDs to remove
    """
    # select all that are on this day, that are inclusive
    # remove from those found in calendar that are exclusive
    if verbose:
        log('     {} that are active on date: {}...'.format(msg, date))
    subset_df = calendar_dates_df.loc[calendar_dates_df['date'] == date]

    srvc_ids_add, srvc_ids_del = _parse_cal_dates_exception_type(
        subset_df, verbose)

    if verbose:
        if len(srvc_ids_add) == 0:
            print_msg = ('          Warning: No active service_ids were '
                         'found on date: {}.')
            log(print_msg.format(date), level=lg.WARNING)

    return srvc_ids_add, srvc_ids_del


def _select_calendar_dates_service_ids_by_date_range(
        calendar_dates_df, msg, date_range=None, verbose=True):
    """
    Search using date_range to subset records in calendar_dates.txt
    using the 'date' column to select service IDs within the date range
    regardless of exception_type

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    msg : str
        string to prepend to print statement for informative purposes
    date_range : list
        date range to extract active service IDs in calendar_dates table
        specified as a 2 element list of strings where the first element
        is the first date and the second element is the last date in the date
        range. Dates should be specified as strings in YYYY-MM-DD format,
        e.g. ['2013-03-09', '2013-09-01'].
        calendar_dates service IDs that are active within
        the date range specified will be selected where exception_type = 1
        (service is added for date) and inactive within
        the date range specified will be selected where exception_type = 2
       (service is removed for date).
    verbose : boolean, optional
        if False, turns off logging and print statements in this function
    Returns
    -------
    srvc_ids_add : list
        list of selected service IDs to add
    srvc_ids_del : list
        list of selected service IDs to remove
    """
    if verbose:
        log('     {} that are active within date_range: {}...'.format(
            msg, date_range))
    subset_df = calendar_dates_df.loc[
        calendar_dates_df['date'].between(
            date_range[0], date_range[1], inclusive=True)]

    srvc_ids_add, srvc_ids_del = _parse_cal_dates_exception_type(subset_df)

    if verbose:
        if len(srvc_ids_add) == 0:
            earliest_date = calendar_dates_df[
                'date'].min().strftime('%Y-%m-%d')
            latest_date = calendar_dates_df[
                'date'].max().strftime('%Y-%m-%d')
            print_msg = ('          Warning: No active service_ids were '
                         'found within date range: {}. '
                         'Dates only exist between: {} and {}.')
            log(print_msg.format(date_range, earliest_date, latest_date),
                level=lg.WARNING)

    return srvc_ids_add, srvc_ids_del


def _add_exception_type_service_id_lists(srvc_ids_list_dict):
    """
    Helper function to combine selected calendar_dates.txt service IDs
    together by exception_type that have been selected with different
    parameters

    Parameters
    ----------
    srvc_ids_list_dict : dict
        dict with keys 'add' (exception_type = 1) and 'del'
        (exception_type = 2) with values as a list of lists of service IDs
        for example:
        {'add': [['service ID 1, 'service ID 2], ['service ID 3']],
        {'del': [['service ID 4, 'service ID 5], ['service ID 6']]}
    Returns
    -------
    srvc_ids_add : list
        list of combined selected service IDs to add
    srvc_ids_del : list
        list of combined selected service IDs to remove
    """
    print_dict = {}
    for list_type, srvc_id_lists in srvc_ids_list_dict.items():
        srvc_ids_cnt_1 = len(srvc_id_lists[0])
        srvc_id_lists[0].extend(srvc_id_lists[1])
        srvc_ids_wo_dups = set(srvc_id_lists[0])
        result_srvc_ids = list(srvc_ids_wo_dups)
        srvc_ids_cnt_2 = len(srvc_id_lists[0])
        result_cnt = abs(srvc_ids_cnt_1 - srvc_ids_cnt_2)
        print_dict.update(
            {list_type: [result_cnt, srvc_ids_cnt_1, srvc_ids_cnt_2]})

        if list_type == 'add':
            srvc_ids_add = result_srvc_ids
        if list_type == 'del':
            srvc_ids_del = result_srvc_ids

    msg_print = (
        '          Added {:,} active and {:,} inactive service_id(s) '
        'found with calendar_dates_lookup parameter to the existing {:,} '
        'active and {:,} inactive service_id(s) selected in calendar_'
        'dates to total: {:,} active and {:,} inactive service_id(s).')
    log(msg_print.format(
        print_dict['add'][0], print_dict['del'][0],
        print_dict['add'][1], print_dict['del'][1],
        print_dict['add'][2], print_dict['del'][2]))
    return srvc_ids_add, srvc_ids_del


def _select_calendar_dates_service_ids(calendar_dates_df, params):
    """
    Wrapper function to select service IDs from the calendar_dates.txt
    that are active according to the parameters specified

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    params : dict
        Parameters denoting how service IDs should be selected from table
        and supporting information to guide the selection process

    Returns
    -------
    srvc_ids_add : list
        list of selected service IDs to add
    srvc_ids_del : list
        list of selected service IDs to remove

    """
    # collect service IDs that match search parameters in calendar_dates.txt
    msg = '     Selecting service_ids from calendar_dates'

    day = params['day']
    date = params['date']
    date_range = params['date_range']
    cal_dates_lookup = params['cal_dates_lookup']
    has_day_param = params['has_day_param']
    has_date_param = params['has_date_param']
    has_date_range_param = params['has_date_range_param']
    has_cal_dates_param = params['has_cal_dates_param']
    has_day_and_date_range_param = params['has_day_and_date_range_param']

    # convert 'date' to datetime expected format: 'yyyymmdd' e.g.: '20130825'
    calendar_dates_df = _cal_date_dt_conversion(
        df=calendar_dates_df, date_cols=['date'])

    # No service_ids can be removed when using day of the week since its
    # a global selection parameter over the entire calendar
    srvc_ids_add = []
    srvc_ids_del = []

    if has_day_param:
        srvc_ids_add = _select_calendar_dates_service_ids_by_day(
            calendar_dates_df, msg, day)
        if has_day_and_date_range_param:
            srvc_ids_add_day = srvc_ids_add.copy()

    if has_date_param:
        srvc_ids_add, srvc_ids_del = \
            _select_calendar_dates_service_ids_by_date(
                calendar_dates_df, msg, date)

    if has_date_range_param:
        srvc_ids_add, srvc_ids_del = \
            _select_calendar_dates_service_ids_by_date_range(
                calendar_dates_df, msg, date_range)
        if has_day_and_date_range_param:
            srvc_ids_add_date_range = srvc_ids_add.copy()
            # since we are also using 'day' as a param we cannot remove
            # service_ids so set it to empty list
            srvc_ids_del = []

    if has_day_and_date_range_param:
        # get the intersection between service IDs found via 'day' and
        # 'date_range' params; since 'day' is a global param we
        # cannot use any exception_type = 2 records to remove records
        print_msg = '{} that are active within date_range: {} on day: {}...'
        log(print_msg.format(msg, date_range, day))
        intersect_srvc_ids = _intersect_cal_service_ids(
            dict_1={'day': srvc_ids_add_day},
            dict_2={'date range': srvc_ids_add_date_range})
        srvc_ids_add = intersect_srvc_ids.copy()

    if has_cal_dates_param:
        query_srvc_ids_add, query_srvc_ids_del = \
            _select_calendar_dates_str_match(
                calendar_dates_df, msg, cal_dates_lookup)
        # add the service_ids found from query to any existing
        # active service_ids or inactive service_ids
        merge_dict = {'add': [srvc_ids_add, query_srvc_ids_add],
                      'del': [srvc_ids_del, query_srvc_ids_del]}
        srvc_ids_add, srvc_ids_del = _add_exception_type_service_id_lists(
            merge_dict)

    return srvc_ids_add, srvc_ids_del


def _merge_service_ids_cal_dates_w_cal(
        srvc_ids, srvc_ids_add, srvc_ids_del, verbose=True):
    """
    Helper function to combine selected calendar.txt and calendar_dates.txt
    service IDs together where service IDs from calendar dates table with
    exception type 1 are added to those found in calendar table and those
    with exception type 2 are removed from those found in calendar table

    Parameters
    ----------
    srvc_ids : list
        list of selected service IDs from calendar table
    srvc_ids_add : list
        list of selected service IDs to add from calendar dates table
    srvc_ids_del : list
        list of selected service IDs to remove from calendar dates table
    verbose : boolean, optional
        if False, turns off logging and print statements in this function

    Returns
    -------
    active_srvc_ids : list
        list of combined selected service IDs
    """
    if verbose:
        log('     Reconciling service_id(s) between calendar and '
            'calendar_dates based on exception_type... ')
    srvc_ids_add = list(set(srvc_ids_add))
    srvc_ids_del = list(set(srvc_ids_del))
    srvc_ids_cnt_before_add = len(set(srvc_ids))

    # add IDs that had exception_type = 1 in calendar_dates
    srvc_ids.extend(srvc_ids_add)
    active_srvc_ids = list(set(srvc_ids))
    active_srvc_ids_cnt_1 = len(active_srvc_ids)

    diff_cnt = abs(active_srvc_ids_cnt_1 - srvc_ids_cnt_before_add)
    # TODO: clean up this section to make sure that the calculation of the
    #  subtraction works the same as the value difference shown in the print
    #  statements
    if verbose:
        msg_print = ('          Adding {:,} service_id(s) to those found in '
                     'calendar. Total active service_id(s): {:,}.')
        log(msg_print.format(diff_cnt, active_srvc_ids_cnt_1))
    # remove IDs that had exception_type = 2 in calendar_dates
    active_srvc_ids = list(set(active_srvc_ids) - set(srvc_ids_del))
    active_srvc_ids_cnt_2 = len(active_srvc_ids)
    if verbose:
        msg_print = ('          Removing {:,} service_id(s) from those found '
                     'in calendar. Total active service_id(s): {:,}.')
        log(msg_print.format(
            abs(active_srvc_ids_cnt_1 - active_srvc_ids_cnt_2),
            active_srvc_ids_cnt_2))
    return active_srvc_ids


def _select_calendar_dates_str_match(
        calendar_dates_df, msg, cal_dates_lookup):
    """
    Selects service IDs from calendar_dates.txt that match the specified
    query composed of a dict of column name and string value selection key
    value pair criteria. Search will be exact and will select all records
    that meet each key value pair criteria.

    Parameters
    ----------
    calendar_dates_df : pandas.DataFrame
        Calendar dates DataFrame
    msg : str
        message to prepend to print statement in function for informative
        purposes
    cal_dates_lookup : dict
        dictionary of the lookup column (key) as a string and corresponding
        string (value) as string or list of strings to use to subset service
        IDs using the calendar_dates DataFrame. Search will be exact and will
        select all records that meet each key value pair criteria.
        Example: {'schedule_type' : 'WD'} or {'schedule_type' : ['WD', 'SU']}

    Returns
    -------
    srvc_ids_add : list
        list of selected service IDs to add
    srvc_ids_del : list
        list of selected service IDs to remove
    """
    # collect service IDs that match search parameters in calendar_dates.txt
    log('{} that match calendar_date_lookup parameter(s) for '
        'column(s) and string(s): {}...'.format(msg, cal_dates_lookup))
    srvc_ids = []
    for col_name_key, string_value in cal_dates_lookup.items():
        if not isinstance(string_value, list):
            string_value = [string_value]
        for text in string_value:
            # TODO: support or/and condition
            result_df = calendar_dates_df[
                calendar_dates_df[col_name_key].str.match(
                    text, case=False, na=False)]
            result_srvc_ids = result_df['unique_service_id'].to_list()
            result_srvc_ids = list(set(result_srvc_ids))
            result_cnt = len(result_srvc_ids)
            log('          Found {:,} unique service_id(s) that matched query:'
                ' column: {} and string: {}.'.format(
                result_cnt, col_name_key, text))
            srvc_ids.extend(result_srvc_ids)
            srvc_ids = list(set(srvc_ids))

    subset_df = calendar_dates_df.loc[
        calendar_dates_df['unique_service_id'].isin(srvc_ids)]
    srvc_ids_add, srvc_ids_del = _parse_cal_dates_exception_type(subset_df)

    if len(srvc_ids_add) == 0:
        print_msg = ('          Warning: No active service_ids were '
                     'found with query: {}.')
        log(print_msg.format(cal_dates_lookup), level=lg.WARNING)

    return srvc_ids_add, srvc_ids_del


def _print_count_service_ids(df_dict, subset_ids):
    """
    Helper function to provide information to user on the results of the
    active service ID selection process by printing counts of service IDs
    in tables prior and post-processing.

    Parameters
    ----------
    df_dict : dict
        dict where key is a string denoting the name of the dataframe to use
        for prints and the value is a pandas.DataFrame containing the
        original DataFrame
    subset_ids : list
        list of processed active service IDs
    Returns
    -------
    Nothing
    """
    # TODO: flip this around to have:
    #  {agency: {table: {org_count: n, proc_count: n}}}}
    #  if agency has records in both tables print differently
    log('     In summary, active service IDs were derived from:')
    unique_col = 'unique_service_id'
    results = {'original_cnts': {}, 'processed_cnts': {}}
    for table, df in df_dict.items():
        if not df.empty:
            # only count unique IDs
            unique_ids = df.drop_duplicates(subset=unique_col)
            subset_df = unique_ids.loc[unique_ids[unique_col].isin(subset_ids)]
            original_cnts = len(unique_ids)
            processed_cnts = len(subset_df)
            results['original_cnts'].update({table: original_cnts})
            results['processed_cnts'].update({table: processed_cnts})
            # NOTE: for feeds that use both calendar and
            # calendar_dates tables, the sum between that agency's calendar
            # count and calendar_dates count will not total to the sum of
            # active service IDs given that calendar_dates IDs are used to add
            # or remove IDs from the calendar table
            start_count = results['original_cnts'][table]
            end_count = results['processed_cnts'][table]
            msg_print = (
                '          {:,} out of {:,} unique service_id(s) from {}.')
            log(msg_print.format(end_count, start_count, table))


def _calendar_service_id_selector(
        calendar_df,
        calendar_dates_df,
        day=None,
        date=None,
        date_range=None,
        cal_dates_lookup=None):
    """
    Select trips that correspond to a specific schedule in either calendar.txt
    and or calendar_dates.txt by finding service_ids that correspond to the
    specified search parameters and the trips related to those service_ids

    Parameters
    ----------
    calendar_df : pandas.DataFrame
        calendar DataFrame
    calendar_dates_df : pandas.DataFrame
        calendar_dates DataFrame
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
    cal_dates_lookup : dict, optional
        dictionary of the lookup column (key) as a string and corresponding
        string (value) as string or list of strings to use to subset trips
        using the calendar_dates DataFrame. Parameter should only be used if
        there is a high degree of certainly that the GTFS feed in use has
        service_ids listed in calendar_dates that would otherwise not be
        selected if using any of the other calendar_dates parameters such as:
        'day', 'date' or 'date_range'. Search will be exact and will select
        all records that meet each key value pair criteria.
        Example: {'schedule_type' : 'WD'} or {'schedule_type' : ['WD', 'SU']}

    Returns
    -------
    active_srvc_ids : list
        list of strings of active unique service IDs that match the calendar
        parameters to use to select active trip IDs

    """
    start_time = time.time()

    log('Processing active service_ids that match the specified '
        'parameters for trip selection...')

    # check if calendar dfs and related params are empty or not to determine
    # what will be used in processing
    has_cal = calendar_df.empty is False
    has_cal_dates = calendar_dates_df.empty is False
    has_day_param = day is not None
    has_date_param = date is not None
    has_date_range_param = date_range is not None
    has_cal_dates_param = cal_dates_lookup is not None
    has_day_and_date_range_param = \
        has_day_param and has_date_range_param is True

    params = {'day': day,
              'date': date,
              'date_range': date_range,
              'cal_dates_lookup': cal_dates_lookup,
              'has_cal': has_cal,
              'has_day_param': has_day_param,
              'has_date_param': has_date_param,
              'has_date_range_param': has_date_range_param,
              'has_cal_dates': has_cal_dates,
              'has_cal_dates_param': has_cal_dates_param,
              'has_day_and_date_range_param': has_day_and_date_range_param}

    _trip_schedule_selector_validate_params(calendar_dates_df, params)

    # create unique service IDs for dfs in list if they are not empty
    df_list = []
    if has_cal:
        df_list.extend([calendar_df])
    if has_cal_dates:
        df_list.extend([calendar_dates_df])
    df_list = _unique_service_id(df_list)

    if has_cal:
        srvc_ids = _select_calendar_service_ids(
            calendar_df, params)

    if has_cal_dates:
        srvc_ids_add, srvc_ids_del = _select_calendar_dates_service_ids(
            calendar_dates_df, params)

    if has_cal and has_cal_dates:
        active_srvc_ids = _merge_service_ids_cal_dates_w_cal(
            srvc_ids, srvc_ids_add, srvc_ids_del)
    elif has_cal:
        active_srvc_ids = srvc_ids.copy()
    elif has_cal_dates:
        active_srvc_ids = srvc_ids_add.copy()
    active_srvc_ids_cnt = len(active_srvc_ids)

    if active_srvc_ids_cnt == 0:
        msg = (
            'Warning: No active service_ids were found matching the specified '
            'parameters. No trips can be selected. Suggest modifying the '
            'calendar parameters and or reviewing the GTFS calendar and or '
            'calendar_dates tables.')
        log(msg, level=lg.WARNING)
    else:
        _print_count_service_ids(df_dict={'calendar': calendar_df,
                                          'calendar_dates': calendar_dates_df},
                                 subset_ids=active_srvc_ids)

        msg_print = (
            '{:,} active service_id(s) were found that match the specified '
            'parameters for trip selection. Took {:,.2f} seconds.')
        log(msg_print.format(active_srvc_ids_cnt, time.time() - start_time))

    return active_srvc_ids


def _trip_selector(trips_df, service_ids, verbose=True):
    """
    Select trips that correspond to a specific schedule in either calendar.txt
    and or calendar_dates.txt using the specified active service_ids

    Parameters
    ----------
    trips_df : pandas.DataFrame
        trips DataFrame
    service_ids : list
        list of strings that represent active service IDs that will be used
        to select trips. Trips that correspond to these service IDs will
        be exported and considered active during the specified schedule.
    verbose : boolean, optional
        if False, turns off logging and print statements in this function

    Returns
    -------
    subset_trip_df : pandas.DataFrame
        trip DataFrame comprised of trips that are active during the
        specified schedule represented in the passed list of active service IDs

    """
    start_time = time.time()
    if verbose:
        log('--------------------------------')
        log('Selecting trip_id(s) with active service_ids that match '
            'the specified calendar and or calendar date parameters...')
    df_list = _unique_service_id([trips_df])
    subset_trip_df = trips_df.loc[
        trips_df['unique_service_id'].isin(service_ids)]

    sort_columns = ['route_id', 'trip_id', 'direction_id']
    if 'direction_id' not in subset_trip_df.columns:
        sort_columns.remove('direction_id')
    subset_trip_df.sort_values(by=sort_columns, inplace=True)
    subset_trip_df.reset_index(drop=True, inplace=True)
    subset_trip_df.drop('unique_service_id', axis=1, inplace=True)

    tot_trip_cnt = len(trips_df)
    tot_subset_trip_cnt = len(subset_trip_df)
    tot_trip_pct = (tot_subset_trip_cnt / tot_trip_cnt) * 100

    if tot_subset_trip_cnt == 0:
        raise ValueError(
            'No trips were found that matched the active service_ids '
            'identified by the specified calendar parameters. Suggest '
            'modifying the calendar parameters and or reviewing the GTFS '
            'trips and calendar and or calendar_dates tables.')
    if verbose:
        agency_id_col = 'unique_agency_id'
        agency_ids = trips_df[agency_id_col].unique()
        for agency in agency_ids:
            agency_trip_cnt = len(subset_trip_df.loc[
                                      subset_trip_df[agency_id_col] == agency])
            agency_tot_trip_cnt = len(trips_df.loc[
                                          trips_df[agency_id_col] == agency])
            agency_trip_pct = (agency_trip_cnt / agency_tot_trip_cnt) * 100

            if agency_trip_cnt > 0:
                msg = ('{:,} of {:,} ({:.2f} percent) of trips for agency: {} '
                       'were selected using service_ids that matched the '
                       'specified calendar parameters.')
                log(msg.format(agency_trip_cnt, agency_tot_trip_cnt,
                               agency_trip_pct, agency))
            else:
                msg = ('Warning: For agency: {}, no trips were found that '
                       'matched the active service_ids identified by the '
                       'specified calendar parameters. Suggest modifying the '
                       'calendar parameters and or reviewing the GTFS '
                       'trips and calendar and or calendar_dates tables.')
                log(msg.format(agency), level=lg.WARNING)

        if len(agency_ids) != 1:
            msg = ('In total: {:,} of {:,} ({:.2f} percent) of trips were '
                   'selected using service_ids that matched the specified '
                   'calendar parameters.')
            log(msg.format(tot_subset_trip_cnt, tot_trip_cnt, tot_trip_pct))
        log('Took {:,.2f} seconds.'.format(time.time() - start_time))
        log('--------------------------------')
    return subset_trip_df


def _highest_freq_trips_date(trips_df, calendar_df, calendar_dates_df):
    """
    Counts the number of trips active on each calendar day of active service
    and returns the date with the maximum number of trips.

    Parameters
    ----------
    trips_df : pandas.DataFrame
        trips DataFrame
    calendar_df : pandas.DataFrame
        calendar DataFrame
    calendar_dates_df : pandas.DataFrame
        calendar dates DataFrame

    Returns
    -------
    max_date : str
        date as a string where the maximum number of trips occur in a schedule

    """
    start_time = time.time()
    # get service ids for each date
    # count the trips that have those service ids
    # the date that has the max is the date to choose
    # use to select service ids by date with the date found here
    log('Finding date with the most frequent trips...')
    has_cal = calendar_df.empty is False
    has_cal_dates = calendar_dates_df.empty is False

    df_list = [trips_df]
    if has_cal:
        df_list.extend([calendar_df])
    if has_cal_dates:
        df_list.extend([calendar_dates_df])
    df_list = _unique_service_id(df_list)

    if has_cal:
        # convert cols to datetime expected format: 'yyyymmdd' e.g.: '20130825'
        date_cols = ['start_date', 'end_date']
        calendar_df = _cal_date_dt_conversion(
            df=calendar_df, date_cols=date_cols)
        date_ranges = _calendar_date_ranges(calendar_df, for_print=False)
        date_list = []
        for date_range in date_ranges:
            dates = pd.date_range(
                date_range['start_date'],
                date_range['end_date'], closed=None, freq='d')
            dates_dt = dates.to_pydatetime()
            dates_str = [date.strftime('%Y-%m-%d') for date in dates_dt]
            date_list.extend(dates_str)
        unique_date_list = set(date_list)

        date_srv_id_dict = {}
        for date in unique_date_list:
            srvc_ids_date = _select_calendar_service_ids_by_date(
                calendar_df, msg='', date=date, verbose=False)
            date_srv_id_dict.update({date: srvc_ids_date})

    if has_cal_dates:
        calendar_dates_df = _cal_date_dt_conversion(
            df=calendar_dates_df, date_cols=['date'])
        date_add_rmv_srv_id_dict = {}
        for date in unique_date_list:
            srvc_ids_add, srvc_ids_del = \
                _select_calendar_dates_service_ids_by_date(
                    calendar_dates_df, msg='', date=date, verbose=False)
            date_add_rmv_srv_id_dict.update({date: {
                'add': srvc_ids_add, 'remove': srvc_ids_del}})

    if has_cal and has_cal_dates:
        for date in unique_date_list:
            srvc_ids = date_srv_id_dict[date].copy()
            srvc_ids_add = date_add_rmv_srv_id_dict[date]['add']
            srvc_ids_del = date_add_rmv_srv_id_dict[date]['remove']
            active_srvc_ids = _merge_service_ids_cal_dates_w_cal(
                srvc_ids, srvc_ids_add, srvc_ids_del, verbose=False)
            date_srv_id_dict[date] = active_srvc_ids.copy()

    # select the trips and count
    trips_df['unique_trip_id'] = trips_df['trip_id'].str.cat(
        trips_df['unique_agency_id'].astype('str'), sep='_')
    date_trip_cnt = {}
    for date in unique_date_list:
        subset_trips_df = _trip_selector(
            trips_df, service_ids=date_srv_id_dict[date], verbose=False)
        trip_id_cnt = len(subset_trips_df['unique_trip_id'].to_list())
        date_trip_cnt.update({date: trip_id_cnt})

    # find the date that has the max number of trips
    max_date = max(date_trip_cnt, key=lambda k: date_trip_cnt[k])
    # get the max number of trips number
    max_trips = max(date_trip_cnt.values())
    # check to see if there are more than 1 dates that have the same max number
    # of trips value
    dates_w_max_trips = [k for k, v in date_trip_cnt.items() if v == max_trips]
    # get the service_ids for dates that share the max number of trips value
    reduce_list = [date_srv_id_dict[x] for x in dates_w_max_trips]
    # check to see if any of the dates have different service_ids from
    # one another and make a list of the IDs that are not shared between dates
    service_ids_not_shared = list(set.difference(*map(set, reduce_list)))

    time_msg = 'Took {:,.2f} seconds.'.format(time.time() - start_time)
    if len(dates_w_max_trips) == 1:
        msg = ("The date with the most frequent trips ({:,} active trips): {} "
               "will be used to select service ids. {}".format(
                max_trips, max_date, time_msg))
        log(msg)
    # if there are more than 1 dates with the same max number of trips value,
    # print informative messages and ValueError depending on if service_ids
    # differ between the dates
    else:
        if len(service_ids_not_shared) == 0:
            msg = ("The following dates: {} have identical service_ids "
                   "representing the date with the most frequent trips "
                   "({:,} active trips). The service ids active on these dates"
                   " will be used to select active trips. Selection of "
                   "service ids will be made using date: {}. {}")
            log(msg.format(
                sorted(dates_w_max_trips), max_trips, max_date, time_msg))
        # if there are different service_ids between the dates that have the
        # same max number of trips value, throw error to make user explicitly
        # pick one of the dates in the list to use since we cannot arbitrary
        # pick one for them
        else:
            msg = ("The following dates: {} have different service ids "
                   "representing the date with the most frequent trips "
                   "({:,} active trips). Unable to use the "
                   "'use_highest_freq_trips_date' parameter. "
                   "Pick one date from this list as the 'date' parameter "
                   "to proceed.")
            raise ValueError(msg.format(sorted(dates_w_max_trips), max_trips))

    # return the date of the max trip value instead of the date's service IDs
    # in order to utilize the informative debugging prints in the
    # _calendar_service_id_selector() function to be transparent in how IDs
    # were selected and to help catch potential data issues
    return max_date


def _unique_service_id(df_list):
    """
    Create 'unique_service_id' column and values for a list of
    pandas.DataFrames

    Parameters
    ----------
    df_list : list
        list of pandas.DataFrames to generate 'unique_service_id' column for

    Returns
    -------
    df_list : list
        list of pandas.DataFrames with 'unique_service_id' column added

    """
    for index, df in enumerate(df_list):
        df['unique_service_id'] = (df['service_id'].str.cat(
            df['unique_agency_id'].astype('str'), sep='_'))
        df_list[index] = df
    return df_list
