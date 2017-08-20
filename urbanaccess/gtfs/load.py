import os
import codecs
import re
import time
import pandas as pd

from urbanaccess import config
from urbanaccess.utils import log
from urbanaccess.gtfs.gtfsfeeds_dataframe import gtfsfeeds_dfs
from urbanaccess.gtfs import utils_format

def _standardize_txt(csv_rootpath=os.path.join(config.settings.data_folder,
                                              'gtfsfeed_text')):
    """
    Standardize all text files inside a GTFS feed for machine readability

    Parameters
    ----------
    csv_rootpath : str, optional
        root path where all gtfs feeds that make up a contiguous metropolitan
        area are stored

    Returns
    -------
    None
    """
    _txt_encoder_check(csv_rootpath)
    _txt_header_whitespace_check(csv_rootpath)

def _txt_encoder_check(csv_rootpath=os.path.join(config.settings.data_folder,
                                                'gtfsfeed_text')):
    """
    Standardize all text files inside a GTFS feed for encoding problems

    Parameters
    ----------
    csv_rootpath : str, optional
        root path where all gtfs feeds that make up a contiguous metropolitan
        area are stored

    Returns
    -------
    None
    """
    
    # UnicodeDecodeError
    start_time = time.time()

    # TODO: This seems like a temp hack to deal with a specific case,
    #       Perhaps could be moved out of the load workflow?
    
    # use to ensure new folder_list creates is valid
    def path_ok(name):
        return os.path.isdir(os.path.join(csv_rootpath, name))

    folder_list = [name for name in os.listdir(csv_rootpath) if path_ok(name)]
    
    # otherwise default to the rootpath
    if not folder_list:
        folder_list = [csv_rootpath]

    for folder in folder_list:
        folder_loc = os.listdir(os.path.join(csv_rootpath, folder))
        tflist = [name for name in folder_loc if name.endswith(".txt")]
        
        for textfile in tflist:
            textfile_loc = os.path.join(csv_rootpath, folder, textfile)

            try:
                file_open = open(textfile_loc)
                raw = file_open.read()
                file_open.close()

                # TODO: This is no bueno, let's just open file as a txt file
                #       as this is hacky, won't work with Py3
                # don't check as string, check char as byte
                bom_l = len(codecs.BOM_UTF8)
                if raw[0:bom_l] == codecs.BOM_UTF8:
                    # set the first char to an empty byte
                    raw = raw[bom_l:]
                    # write to file
                    file_open = open(textfile_loc, 'w')
                    file_open.write(raw)
                    file_open.close()

            except UnicodeDecodeError:
                log('Failed to read this file: {}'.format(textfile_loc))
    
    time_diff = time.time() - start_time
    update_msg = ('GTFS text file encoding check completed. Took '
                  '{:,.2f} seconds').format(time_diff)
    log(update_msg)

def _txt_header_whitespace_check(csv_rootpath=os.path.join(
                                                config.settings.data_folder,
                                                'gtfsfeed_text')):
    """
    Standardize all text files inside a GTFS feed to remove whitespace
    in headers

    Parameters
    ----------
    csv_rootpath : str, optional
        root path where all gtfs feeds that make up a contiguous metropolitan
        area are stored

    Returns
    -------
    None
    """
    start_time = time.time()

    os_list = os.listdir(csv_rootpath)
    folder_list = [fname for fname in os_list if _is_dir(csv_rootpath, fname)]
    
    # fallback
    if not folder_list:
        folder_list = [csv_rootpath]

    # iterate through and check file types
    for folder in folder_list:
        dir_list = os.listdir(os.path.join(csv_rootpath,folder))
        tflist = [t_fname for t_fname in dir_list if t_fname.endswith(".txt")]
        for textfile in tflist:
            textfile_loc = os.path.join(csv_rootpath, folder, textfile)
            
            try:
                with open(textfile_loc) as f:
                    lines = f.readlines()
                lines[0] = re.sub(r'\s+', '', lines[0]) + '\n'
                
                with open(textfile_loc, 'w') as f:
                    f.writelines(lines)

            except UnicodeDecodeError:
                log('Failed to read this file: {}'.format(textfile_loc))

    time_diff = time.time() - start_time
    status_msg = ('GTFS text file header whitespace check completed. '
                  'Took {:,.2f} seconds').format(time_diff)
    log(status_msg)


def _is_dir(csv_rootpath, foldername):
    return os.path.isdir(os.path.join(csv_rootpath, foldername))


def gtfsfeed_to_df(gtfsfeed_path=None,
                   validation=False,
                   verbose=True,
                   bbox=None,
                   remove_stops_outsidebbox=True,
                   append_definitions=False):
    """
    Read all GTFS feed components as a dataframe in a gtfsfeeds_dfs object and
    merge all individual GTFS feeds into a regional metropolitan data table.
    Optionally, data can also be validated before its use.

    Parameters
    ----------
    gtfsfeed_path : str, optional
        root path where all gtfs feeds that make up a contiguous metropolitan
        area are stored
    validation : bool
        if true, the validation check on stops checking for stops outside
        of a bounding box and stop coordinate
        hemisphere will be run. this is required to remove stops outside of
        a bbox
    verbose : bool
        if true and stops are found outside of the bbox, the stops that are
        outside will be printed for your reference
    bbox : tuple
        Bounding box formatted as a 4 element tuple:
        (lng_max, lat_min, lng_min, lat_max)
        example: (-122.304611,37.798933,-122.263412,37.822802)
        a bbox can be extracted for an area using: the CSV format bbox
        from http://boundingbox.klokantech.com/
    remove_stops_outsidebbox : bool
        if true stops that are outside the bbox will be removed
    append_definitions : bool
        if true, columns that use the GTFS data schema for their attribute
        codes will have the corresponding GTFS definition information of
        that code appended to the resulting dataframes for reference

    Returns
    -------
    gtfsfeeds_dfs : object
        processed dataframes of corresponding GTFS feed text files
    gtfsfeeds_dfs.stops : pandas.DataFrame
    gtfsfeeds_dfs.routes : pandas.DataFrame
    gtfsfeeds_dfs.trips : pandas.DataFrame
    gtfsfeeds_dfs.stop_times : pandas.DataFrame
    gtfsfeeds_dfs.calendar : pandas.DataFrame
    gtfsfeeds_dfs.calendar_dates : pandas.DataFrame
    """

    merged_stops_df = pd.DataFrame()
    merged_routes_df = pd.DataFrame()
    merged_trips_df = pd.DataFrame()
    merged_stop_times_df = pd.DataFrame()
    merged_calendar_df = pd.DataFrame()
    merged_calendar_dates_df = pd.DataFrame()

    start_time = time.time()

    # assertion check to make sure we have valid path to gtfs
    if gtfsfeed_path is None:
        d_folder = config.settings.data_folder
        gtfsfeed_path = os.path.join(d_folder, 'gtfsfeed_text')
    assert_err_msg = '{} does not exist'.format(gtfsfeed_path)
    assert os.path.exists(gtfsfeed_path), assert_err_msg

    if validation:
        validation_note = ('Attempted to run validation but bbox, '
                           'verbose, and or remove_stops_outsidebbox '
                           'were set to None. These paramters must be '
                           'specified for validation.')
        bb_good = bbox is not None
        rm_good = remove_stops_outsidebbox is not None
        vb_good = verbose is not None
        all_good = (bb_good and rm_good and vb_good)
        assert all_good, validation_note

    # this step updates and rewrites to file the cleaned up txt

    # TODO: seems dangerous to write to file, seems like UA should only
    #       ever read in content and modify, not write out changes to 
    #       data that is used as reference content!
    _standardize_txt(gtfsfeed_path)

    folder_list = [foldername for foldername in os.listdir(gtfsfeed_path) if
                  os.path.isdir(os.path.join(gtfsfeed_path, foldername))]
    if not folder_list:
        folder_list = [gtfsfeed_path]
    for folder in folder_list:
        folder_path = os.path.join(gtfsfeed_path, folder)
        dir_list = os.listdir(folder_path)
        tflist = [t_fname for t_fname in dir_list if t_fname.endswith(".txt")]

        required_gtfsfiles = ['stops.txt',
                              'routes.txt',
                              'trips.txt',
                              'stop_times.txt',
                              'calendar.txt',
                              'calendar_dates.txt']
        # make sure all required files are present
        for req_file in required_gtfsfiles:
            assert_msg = ('{} is a required GTFS text file and not found '
                          'in folder {}').format(req_file, folder_path)
            assert req_file in tflist, assert_msg
        
        # TODO: This is now handles by the consolidated steps below
        #       so we can probably safely remove
        # handle read in of each required file
        for textfile in tflist:
            if textfile == 'agency.txt':
                agency_df = utils_format._read_gtfs_agency(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'stops.txt':
                stops_df = utils_format._read_gtfs_stops(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'routes.txt':
                routes_df = utils_format._read_gtfs_routes(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'trips.txt':
                trips_df = utils_format._read_gtfs_trips(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'stop_times.txt':
                stop_times_df = utils_format._read_gtfs_stop_times(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'calendar.txt':
                calendar_df = utils_format._read_gtfs_calendar(
                            textfile_path=folder_path,
                            textfile=textfile)
            if textfile == 'calendar_dates.txt':
                calendar_dates_df = utils_format._read_gtfs_calendar_dates(
                            textfile_path=folder_path,
                            textfile=textfile)

        # TODO: Can eventually replace the cleaning steps in the above for loop
        consolidated_unclean = _consolidate_gtfs_dfs(
                                            agency_df,
                                            stops_df,
                                            routes_df,
                                            trips_df,
                                            stop_times_df,
                                            calendar_df,
                                            calendar_dates_df)
        
        # should be able to work with an array, which would require
        # this to be moved around in this function
        cleaned_gtfs_tables = utils_format.clean_gtfs_tables(
                                            [consolidated_unclean])

        (agency_df,
         stops_df,
         routes_df,
         trips_df,
         stop_times_df,
         calendar_df,
         calendar_dates_df) = _explode_gtfs_dfs(cleaned_gtfs_tables)

        with_unique_ids = utils_format._add_unique_agencyid(
                            agency_df=agency_df,
                            stops_df=stops_df,
                            routes_df=routes_df,
                            trips_df=trips_df,
                            stop_times_df=stop_times_df,
                            calendar_df=calendar_df,
                            calendar_dates_df=calendar_dates_df,
                            nulls_as_folder=True,
                            feed_folder=folder_path)
        (stops_df,
         routes_df,
         trips_df,
         stop_times_df,
         calendar_df,
         calendar_dates_df) = with_unique_ids

        if validation:
            trim_bool = remove_stops_outsidebbox
            stops_df = utils_format.validate_and_trim_stops(
                                                stops_df,
                                                stop_times_df,
                                                bbox,
                                                dont_trim=trim_bool)
            stop_times_df = utils_format.trim_stop_times(stops_df,
                                                         stop_times_df)

        # common sub dataframes for both processes
        routes_sub = routes_df[['route_id', 'route_type']]
        trips_sub = trips_df[['trip_id', 'route_id']]
        routes_trips_df = pd.merge(routes_sub,
                                   trips_sub,
                                   how='left',
                                   on='route_id',
                                   sort=False,
                                   copy=False)

        # encode types of stops
        stops_df = utils_format.route_type_to_stops(
            stops_df,
            stop_times_df,
            routes_trips_df)

        # now use those stops to generate times for each stop
        stop_times_by_route = utils_format.route_type_to_stop_times(
            stops_df,
            stop_times_df,
            routes_trips_df)
        # reset stop_times with new df that has route type col
        stop_times_df = stop_times_by_route

        merged_stops_df = merged_stops_df.append(
                                            stops_df,
                                            ignore_index=True)
        merged_routes_df = merged_routes_df.append(
                                            routes_df,
                                            ignore_index=True)
        merged_trips_df = merged_trips_df.append(
                                            trips_df,
                                            ignore_index=True)
        merged_stop_times_df = merged_stop_times_df.append(
                                            stop_times_df,
                                            ignore_index=True)
        merged_calendar_df = merged_calendar_df.append(
                                            calendar_df,
                                            ignore_index=True)
        merged_calendar_dates_df = merged_calendar_dates_df.append(
                                            calendar_dates_df,
                                            ignore_index=True)

        # print break to visually separate each gtfs feed log
        log('completed one gtfs download loop...')

    merged_stop_times_df = utils_format.time_to_seconds(merged_stop_times_df)

    # set gtfsfeeds_dfs object to merged GTFS dfs
    gtfsfeeds_dfs.stops = merged_stops_df
    gtfsfeeds_dfs.routes = merged_routes_df
    gtfsfeeds_dfs.trips = merged_trips_df
    gtfsfeeds_dfs.stop_times = merged_stop_times_df
    gtfsfeeds_dfs.calendar = merged_calendar_df
    gtfsfeeds_dfs.calendar_dates = merged_calendar_dates_df

    fl_len = len(folder_list)
    time_diff = time.time()-start_time
    completed_msg = ('{} GTFS feed files successfully read as '
                     'dataframes: {}. Took {:,.2f} '
                     'seconds').format(fl_len, folder_list, time_diff)
    log(completed_msg)

    return gtfsfeeds_dfs


def _consolidate_gtfs_dfs(agency, stops, routes, trips,
                          stop_times, calendar, calendar_dates):
    return_gtfs = {}
    return_gtfs['agency'] = agency
    return_gtfs['stops'] = stops
    return_gtfs['routes'] = routes
    return_gtfs['trips'] = trips
    return_gtfs['stop_times'] = stop_times
    return_gtfs['calendar'] = calendar
    return_gtfs['calendar_dates'] = calendar_dates

    return return_gtfs

def _explode_gtfs_dfs(return_gtfs):
    agency = return_gtfs['agency']
    stops = return_gtfs['stops']
    routes = return_gtfs['routes']
    trips = return_gtfs['trips']
    stop_times = return_gtfs['stop_times']
    calendar = return_gtfs['calendar']
    calendar_dates = return_gtfs['calendar_dates']

    return agency, stops, routes, trips, stop_times, calendar, calendar_dates
