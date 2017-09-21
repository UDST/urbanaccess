import pandas as pd

from urbanaccess.utils import _join_camel


def _calendar_dates_agency_uids(calendar_dates_df, routes_agencies_trips):
    # merge the three (combined) with calendar (all, with cal)
    all_w_cal = _left_merge(calendar_dates_df,
                            routes_agencies_trips,
                            'service_id')

    all_w_cal = _add_uid_and_clean(all_w_cal, 'agency_name', 'service_id')

    # take resulting merged df and put back into the calendar df
    all_w_cal_sub = all_w_cal[['unique_agency_id', 'service_id']]
    final_out = _left_merge(calendar_dates_df, all_w_cal_sub, 'service_id')

    return final_out


def _calendar_agency_uids(calendar_df, routes_agencies_trips):
    cal_sub = calendar_df[['service_id']]
    all_w_cal = _left_merge(cal_sub, routes_agencies_trips, 'service_id')

    merged_df = _add_uid_and_clean(all_w_cal, 'agency_name', 'service_id')

    merged_df_sub = merged_df[['unique_agency_id', 'service_id']]
    merged_df = _left_merge(calendar_df, merged_df_sub, 'service_id')

    return merged_df


def _trips_agency_uids(trips_df, routes_agencies_trips):
    merged_df = _add_uid_and_clean(
        routes_agencies_trips, 'agency_name', 'trip_id')

    merged_df_sub = merged_df[['unique_agency_id', 'trip_id']]
    merged_df = _left_merge(trips_df, merged_df_sub, 'trip_id')

    return merged_df


def _routes_agency_uids(routes_df, agency_df):
    routes_sub = routes_df[['route_id', 'agency_id']]
    merged_df = _left_merge(routes_sub, agency_df, 'agency_id')

    # no dropping dupes so don't use _add_uid_and_clean here
    unique_agency_ids = _generate_unique_agency_id(merged_df, 'agency_name')
    merged_df['unique_agency_id'] = unique_agency_ids

    merged_df_sub = merged_df[['unique_agency_id', 'route_id']]
    merged_df = _left_merge(routes_df, merged_df_sub, 'route_id')

    return merged_df


def _stops_agency_uids(stops_df, stop_times_df, routes_agencies_trips):
    all_w_stops = _left_merge(stop_times_df, routes_agencies_trips, 'trip_id')

    stops_sub = stops_df[['stop_id']]
    merged_df = _left_merge(stops_sub, all_w_stops, 'stop_id')

    merged_df = _add_uid_and_clean(merged_df, 'agency_name', 'stop_id')

    merged_df_sub = merged_df[['unique_agency_id', 'stop_id']]
    merged_df = _left_merge(stops_df, merged_df_sub, 'stop_id')

    return merged_df


def _stop_times_agency_uids(stop_times_df, routes_agencies_trips):
    merged_df = _left_merge(stop_times_df, routes_agencies_trips, 'trip_id')

    merged_df = _add_uid_and_clean(merged_df, 'agency_name', 'trip_id')

    merged_df_sub = merged_df[['unique_agency_id', 'trip_id']]
    merged_df = _left_merge(stop_times_df, merged_df_sub, 'trip_id')

    return merged_df


def _time_selector(df, start_time, end_time):
    # takes input start and end time range from 24 hour
    # clock and converts it to seconds past midnight
    # in order to select times that may be after midnight

    # convert string time components to integer and
    # then calculate seconds past midnight
    # convert start_time 24 hour to seconds past midnight
    start_h = int(str(start_time[0:2]))
    start_m = int(str(start_time[3:5]))
    start_s = int(str(start_time[6:8]))
    start_time_sec = (start_h * 60 * 60) + (start_m * 60) + start_s

    # convert end_time 24 hour to seconds past midnight
    end_h = int(str(end_time[0:2]))
    end_m = int(str(end_time[3:5]))
    end_s = int(str(end_time[6:8]))
    end_time_sec = (end_h * 60 * 60) + (end_m * 60) + end_s

    # create df of stops times that are within the requested range
    interp_deps = df['departure_time_sec_interpolate']
    over_start_times = (start_time_sec < interp_deps)
    before_end_times = (interp_deps < end_time_sec)
    times_clear = (over_start_times & before_end_times)
    selected_stop_times_df = df[times_clear]

    return selected_stop_times_df


def _trip_sched_selector(sub_trips_df, cal, day):
    # TODO: Eventually replace the above function, not replacing
    #       for now because other version handles a lot of edge
    #       cases we ignore

    # TODO: A lot of the unique id creation seems
    #       to be an opportunity to improve
    
    # create unique service ids by concatenating two col string
    want_cols = ['service_id', 'unique_agency_id']
    svc_id = sub_trips_df[want_cols]
    new_uid_svc = svc_id.apply(lambda x: _join_camel(x), axis=1)
    sub_trips_df['unique_service_id'] = new_uid_svc

    cal_id = cal[want_cols]
    new_uid_cal = cal_id.apply(lambda x: _join_camel(x), axis=1)
    cal['unique_service_id'] = new_uid_cal

    # select service ids where day specified in
    # function has a 1, which means service
    # runs on that day
    cal = cal[(cal[day] == 1)]  # subset cal by that day
    cal = cal[['unique_service_id']]

    uid_svc = cal['unique_service_id']
    sub_has = sub_trips_df['unique_service_id'].isin(uid_svc)
    cal_sel_trips_df = sub_trips_df.loc[sub_has]

    sort_columns = ['route_id', 'trip_id', 'direction_id']
    if 'direction_id' not in cal_sel_trips_df.columns:
        sort_columns.remove('direction_id')

    cal_sel_trips_df.sort_values(by=sort_columns, inplace=True)
    cal_sel_trips_df.reset_index(drop=True, inplace=True)
    cal_sel_trips_df.drop('unique_service_id', axis=1, inplace=True)

    return cal_sel_trips_df


def _generate_unique_agency_id(df, col_name):
    col = df[col_name].astype(str)
    # replace all runs of spaces with a single underscore
    col_snake_case = col.str.replace(r'\s+', '_')
    # replace all ampersands
    col_snake_no_amps = col_snake_case.str.replace('&', 'and')
    all_lowered = col_snake_no_amps.str.lower()

    return all_lowered


def _add_uid_and_clean(merged_df, col_id, subset_id):
    # create the unique agency id for each and populate the new column
    unique_agency_ids = _generate_unique_agency_id(merged_df, col_id)
    merged_df['unique_agency_id'] = unique_agency_ids

    # remove any columns that might be repeated
    merged_df.drop_duplicates(subset=subset_id, keep='first', inplace=True)

    return merged_df


def _left_merge(left, right, on):
    # just make sure that the typing is
    # consistent on the col being matches
    left[on] = left[on].astype(right[on].dtype)

    # now we can proceed with merge
    merged_df = pd.merge(left,
                         right,
                         how='left',
                         on=on,
                         sort=False,
                         copy=False)

    return merged_df
