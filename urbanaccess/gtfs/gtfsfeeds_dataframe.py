import pandas as pd

class urbanaccess_gtfs_df(object):
    """
    A collection of dataframes representing the standardized and
    merged metropolitan-wide transit network from multiple
    GTFS feeds.

    Parameters
    ----------
    gtfsfeeds_df : object
        processed dataframes of corresponding GTFS feed text files
    gtfsfeeds_df.stops : pandas.DataFrame
    gtfsfeeds_df.routes : pandas.DataFrame
    gtfsfeeds_df.trips : pandas.DataFrame
    gtfsfeeds_df.stop_times : pandas.DataFrame
    gtfsfeeds_df.calendar : pandas.DataFrame
    gtfsfeeds_df.calendar_dates : pandas.DataFrame
    gtfsfeeds_df.stop_times_int : pandas.DataFrame
    gtfsfeeds_df.headways : pandas.DataFrame
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
gtfsfeeds_df = urbanaccess_gtfs_df()