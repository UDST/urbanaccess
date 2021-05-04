from __future__ import division
import os
import logging as lg

from urbanaccess.utils import log


def _boundingbox_check(df, feed_folder, lat_min=None, lng_min=None,
                       lat_max=None, lng_max=None, bbox=None, remove=None,
                       verbose=None):
    """
    Check for and optionally remove stops that are found to be outside of a
    specified bounding box

    Parameters
    ----------
    df : pandas.DataFrame
        stops DataFrame
    feed_folder : str
        name of originating GTFS feed folder, will be the basename
        directory folder for the GTFS feed
    lat_min : float, optional
        southern latitude of bounding box
    lng_min : float, optional
        eastern longitude of bounding box
    lat_max : float, optional
        northern latitude of bounding box
    lng_max : float, optional
        western longitude of bounding box
    bbox : tuple, optional
        Bounding box formatted as a 4 element tuple: (lng_max, lat_min,
        lng_min, lat_max)
        example: (-122.304611, 37.798933, -122.263412, 37.822802)
        a bbox can be extracted for an area using: the CSV format bbox from
        http://boundingbox.klokantech.com/
    remove : bool
        if true stops that are outside the bbox will be removed
    verbose : bool
        if true and stops are found outside of the bbox, the stops that are
        outside will be printed for your reference

    Returns
    -------
    df : pandas.DataFrame
        stops DataFrame. If remove=True, DataFrame will contain only stops
        that are located within the bbox coordinates specified

    """
    if not isinstance(verbose, bool):
        raise ValueError('verbose must be bool.')
    if not isinstance(remove, bool):
        raise ValueError('remove must be bool.')

    if bbox is not None:
        if not isinstance(bbox, tuple) or len(bbox) != 4:
            raise ValueError('bbox must be a 4 element tuple.')
        if (lat_min is not None) or (lng_min is not None) or (
                    lat_max is not None) or (lng_max is not None):
            raise ValueError('lat_min, lng_min, lat_max and lng_max must be '
                             'None if using bbox.')

        lng_max, lat_min, lng_min, lat_max = bbox

    if lat_min is None:
        raise ValueError('lat_min cannot be None.')
    if lng_min is None:
        raise ValueError('lng_min cannot be None.')
    if lat_max is None:
        raise ValueError('lat_max cannot be None.')
    if lng_max is None:
        raise ValueError('lng_max cannot be None.')
    if not isinstance(lat_min, float) or not isinstance(lng_min, float) or\
            not isinstance(lat_max, float) or not isinstance(lng_max, float):
        raise ValueError('lng_min, lat_min, lng_min, lat_max, and lng_max '
                         'must be floats.')

    outside_boundingbox = df.loc[~(
        ((lng_max < df["stop_lon"]) & (df["stop_lon"] < lng_min)) & (
            (lat_min < df["stop_lat"]) & (df["stop_lat"] < lat_max)))]

    feed_name = os.path.split(feed_folder)[1]
    if len(outside_boundingbox) > 0:
        log('WARNING: {} GTFS feed stops: {:,} of {:,} ({:.2f} percent of '
            'total) record(s) are outside the bounding box '
            'coordinates.'.format(
             feed_name, len(outside_boundingbox),
             len(df), (len(outside_boundingbox) / len(df)) * 100),
            level=lg.WARNING)
        if verbose:
            log('Records:')
            log('{}'.format(outside_boundingbox))
        if remove:
            df = df.drop(outside_boundingbox.index)
            log('Removed identified stops that are outside of bounding box.')
        else:
            log('Stops found outside of bounding box were not removed. '
                'Set remove=True to remove stops outside of bounding box.')
    else:
        log('No GTFS feed stops were found to be outside the bounding box '
            'coordinates.')
    return df


def _checkcoordinates(df, feed_folder):
    """
    Check and print the hemisphere that stop coordinates are in

    Parameters
    ----------
    df : pandas.DataFrame
        stops DataFrame
    feed_folder : str
        name of originating GTFS feed folder, will be the basename
        directory folder for the GTFS feed

    Returns
    -------
    None

    """
    feed_name = os.path.split(feed_folder)[1]
    if (df['stop_lat'] > 0).values.any() & (df['stop_lon'] < 0).values.any():
        log('{} GTFS feed stops: coordinates are in northwest hemisphere. '
            'Latitude = North (90); Longitude = West (-90).'.format(feed_name))

    if (df['stop_lat'] < 0).values.any() & (df['stop_lon'] < 0).values.any():
        log('{} GTFS feed stops: coordinates are in southwest hemisphere. '
            'Latitude = South (-90); Longitude = West (-90).'.format(
             feed_name))

    if (df['stop_lat'] > 0).values.any() & (df['stop_lon'] > 0).values.any():
        log('{} GTFS feed stops: coordinates are in northeast hemisphere. '
            'Latitude = North (90); Longitude = East (90).'.format(feed_name))

    if (df['stop_lat'] < 0).values.any() & (df['stop_lon'] > 0).values.any():
        log('{} GTFS feed stops: coordinates are in southeast hemisphere. '
            'Latitude = South (-90); Longitude = East (90).'.format(feed_name))


def _validate_gtfs(stops_df, feed_folder,
                   verbose, bbox, remove_stops_outsidebbox):
    """
    Run validation checks on stops checking for stops outside of a bounding
    box and stop coordinate hemisphere

    Parameters
    ----------
    stops_df : pandas.DataFrame
        stop times DataFrame
    feed_folder : str
        name of originating GTFS feed folder
    verbose : bool
        if true and stops are found outside of the bbox, the stops that are
        outside will be printed for your reference
    bbox : tuple
        Bounding box formatted as a 4 element tuple: (lng_max, lat_min,
        lng_min, lat_max)
        example: (-122.304611, 37.798933, -122.263412, 37.822802)
        a bbox can be extracted for an area using: the CSV format bbox from
        http://boundingbox.klokantech.com/
    remove_stops_outsidebbox : bool
        if true stops that are outside the bbox will be removed

    Returns
    -------
    stops_df : pandas.DataFrame
        stops DataFrame. If remove_stops_outsidebbox=True, DataFrame will
        contain only stops that are located within the bbox coordinates
        specified
    """

    stops_df = _boundingbox_check(df=stops_df,
                                  feed_folder=feed_folder,
                                  lat_min=None,
                                  lng_min=None,
                                  lat_max=None,
                                  lng_max=None,
                                  bbox=bbox,
                                  remove=remove_stops_outsidebbox,
                                  verbose=verbose)

    _checkcoordinates(df=stops_df, feed_folder=feed_folder)

    return stops_df


def _check_time_range_format(timerange):
    """
    Check time range value format for expected schema

    Parameters
    ----------
    timerange : list
        time range as a list with time 1 and time 2 as strings.
        Must follow format of a 24 hour clock for example:
        08:00:00 or 17:00:00

    Returns
    -------
    None
    """
    time_error_statement = (
        '{} starttime and endtime are not in the correct format. '
        'Format should be a 24 hour clock in the following format: 08:00:00 '
        'or 17:00:00.'.format(timerange))
    if not isinstance(timerange, list) or len(timerange) != 2:
        raise ValueError(time_error_statement)
    if timerange[0] > timerange[1]:
        raise ValueError(time_error_statement)
    for t in timerange:
        if not isinstance(t, str):
            raise ValueError(time_error_statement)
        if len(t) != 8:
            raise ValueError(time_error_statement)
    timerange_hr_1 = int(str(timerange[0][0:2]))
    timerange_hr_2 = int(str(timerange[1][0:2]))
    if timerange_hr_2 - timerange_hr_1 > 3:
        log('WARNING: Time range passed: {} is a {} hour period. Long '
            'periods over 3 hours may take a significant amount of time to '
            'process.'.format(timerange, timerange_hr_2 - timerange_hr_1),
            level=lg.WARNING)
