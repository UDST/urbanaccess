import pandas as pd


# TODO: make class CamelCase
class urbanaccess_gtfs_df(object):
    """
    A collection of dataframes representing the standardized and
    merged metropolitan-wide transit network from multiple
    GTFS feeds.

    Parameters
    ----------
    gtfsfeeds_dfs : object
        processed dataframes of corresponding GTFS feed text files
    gtfsfeeds_dfs.stops : pandas.DataFrame
    gtfsfeeds_dfs.routes : pandas.DataFrame
    gtfsfeeds_dfs.trips : pandas.DataFrame
    gtfsfeeds_dfs.stop_times : pandas.DataFrame
    gtfsfeeds_dfs.calendar : pandas.DataFrame
    gtfsfeeds_dfs.calendar_dates : pandas.DataFrame
    gtfsfeeds_dfs.stop_times_int : pandas.DataFrame
    gtfsfeeds_dfs.headways : pandas.DataFrame
    """

    def __init__(self,
                 stops=pd.DataFrame(),
                 routes=pd.DataFrame(),
                 trips=pd.DataFrame(),
                 stop_times=pd.DataFrame(),
                 calendar=pd.DataFrame(),
                 calendar_dates=pd.DataFrame(),
                 stop_times_int=pd.DataFrame(),
                 headways=pd.DataFrame()):
        self.stops = stops
        self.routes = routes
        self.trips = trips
        self.stop_times = stop_times
        self.calendar = calendar
        self.calendar_dates = calendar_dates
        self.stop_times_int = stop_times_int
        self.headways = headways


# instantiate the UrbanAccess gtfs feed dataframe object
gtfsfeeds_dfs = urbanaccess_gtfs_df()
