import logging as lg
import os

# Note: The above imported logging funcs were modified from the OSMnx library
#       & used with permission from the author Geoff Boeing: log, get_logger
#       OSMnx repo: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

from urbanaccess.utils import log

def _boundingbox_check(df=None, feed_folder=None, lat_min=None, lng_min=None, lat_max=None, lng_max=None, bbox=None, remove=None, verbose=None):
    """
    Check for and optionally remove stops that are found to be outside of a specified bounding box

    Parameters
    ----------
    df : pandas.DataFrame
        stops dataframe
    feed_folder : str
        name of originating gtfs feed folder
    lat_min : float
        southern latitude of bounding box
    lng_min : float
        eastern longitude of bounding box
    lat_max : float
        northern latitude of bounding box
    lng_max : float
        western longitude of bounding box
    bbox : tuple
        Bounding box formatted as a 4 element tuple: (lng_max, lat_min, lng_min, lat_max)
        example: (-122.304611,37.798933,-122.263412,37.822802)
        a bbox can be extracted for an area using: the CSV format bbox from http://boundingbox.klokantech.com/
    remove : bool
        if true stops that are outside the bbox will be removed
    verbose : bool
        if true and stops are found outside of the bbox, the stops that are outside will be printed for your reference

    Returns
    -------
    df : pandas.DataFrame

    """
    assert isinstance(verbose,bool)
    assert isinstance(remove,bool)

    if bbox is not None:
        assert isinstance(bbox,tuple) and len(bbox) == 4, 'bbox must be a 4 element tuple'
        assert (lat_min is None) and (lng_min is None) and (lat_max is None) and \
               (lng_max is None), 'lat_min, lng_min, lat_max and lng_max must be None if you are using bbox'

        lng_max, lat_min, lng_min, lat_max = bbox

    assert lat_min is not None, 'lat_min cannot be None'
    assert lng_min is not None, 'lng_min cannot be None'
    assert lat_max is not None, 'lat_max cannot be None'
    assert lng_max is not None, 'lng_max cannot be None'
    assert isinstance(lat_min,float) and isinstance(lng_min,float) and isinstance(lat_max,float) and \
           isinstance(lng_max,float), 'lat_min, lng_min, lat_max, and lng_max must be floats'

    outside_boundingbox = df.loc[~((( lng_max < df["stop_lon"]) & (df["stop_lon"] <  lng_min)) & (( lat_min < df["stop_lat"]) & (df["stop_lat"] <  lat_max)))]

    if len(outside_boundingbox) > 0:
        log('{} GTFS feed stops: {:,} of {:,} ({:.2f} percent of total) record(s) are outside the bounding box coordinates'.format(os.path.split(feed_folder)[1],len(outside_boundingbox),len(df),(len(outside_boundingbox)/len(df))*100),level=lg.WARNING)
        if verbose:
            log('Records: {}'.format(outside_boundingbox))
        if remove:
            df_subset=df.drop(outside_boundingbox.index)
            log('Removed identified stops that are outside of bounding box.')
            return df_subset
    else:
        return df

def _checkcoordinates(df=None, feed_folder=None):
    """
    Check and print the hemisphere that stop coordinates are in

    Parameters
    ----------
    df : pandas.DataFrame
        stops dataframe
    feed_folder : str
        name of originating gtfs feed folder

    Returns
    -------
    None

    """
    if (df['stop_lat']> 0).values.any() & (df['stop_lon']< 0).values.any():
        log('{} GTFS feed stops: coordinates are in northwest hemisphere. '
            'Latitude = North (90); Longitude = West (-90).'.format(os.path.split(feed_folder)[1]))

    if (df['stop_lat']< 0).values.any() & (df['stop_lon']< 0).values.any():
        log('{} GTFS feed stops: coordinates are in southwest hemisphere. '
            'Latitude = South (-90); Longitude = West (-90).'.format(os.path.split(feed_folder)[1]))

    if (df['stop_lat']> 0).values.any() & (df['stop_lon']> 0).values.any():
        log('{} GTFS feed stops: coordinates are in northeast hemisphere. '
            'Latitude = North (90); Longitude = East (90).'.format(os.path.split(feed_folder)[1]))

    if (df['stop_lat']< 0).values.any() & (df['stop_lon']> 0).values.any():
        log('{} GTFS feed stops: coordinates are in southeast hemisphere. '
            'Latitude = South (-90); Longitude = East (90).'.format(os.path.split(feed_folder)[1]))

def _validate_gtfs(stop_times_df=None, stops_df=None, feed_folder=None, verbose=None, bbox=None, remove_stops_outsidebbox=None):
    """
    Run validation checks on stops checking for stops outside of a bounding box and stop coordinate hemisphere

    Parameters
    ----------
    stop_times_df : pandas.DataFrame
        stop times dataframe
    stops_df : pandas.DataFrame
        stop times dataframe
    feed_folder : str
        name of originating gtfs feed folder
    verbose : bool
        if true and stops are found outside of the bbox, the stops that are outside will be printed for your reference
    bbox : tuple
        Bounding box formatted as a 4 element tuple: (lng_max, lat_min, lng_min, lat_max)
        example: (-122.304611,37.798933,-122.263412,37.822802)
        a bbox can be extracted for an area using: the CSV format bbox from http://boundingbox.klokantech.com/
    remove_stops_outsidebbox : bool
        if true stops that are outside the bbox will be removed

    Returns
    -------
    stops_df : pandas.DataFrame
    """
    assert (stop_times_df['arrival_time']< 0).values.any() == False or (stop_times_df['departure_time']< 0).values.any() == False, 'stop_times.txt file in {} GTFS feed has negative stop times. Time must be positive.'.format(os.path.split(feed_folder)[1])

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
