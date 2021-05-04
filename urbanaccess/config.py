import os
import yaml
import numpy as np


def _format_check(settings):
    """
    Check the format of an urbanaccess_config object.

    Parameters
    ----------
    settings : dict
        urbanaccess_config as a dictionary
    Returns
    -------
    Nothing
    """

    valid_keys = ['data_folder', 'logs_folder', 'log_file',
                  'log_console', 'log_name', 'log_filename',
                  'txt_encoding', 'gtfs_api']

    for key in settings.keys():
        if key not in valid_keys:
            raise ValueError('{} not found in list of valid configuration '
                             'keys'.format(key))
        if not isinstance(key, str):
            raise ValueError('{} must be a string'.format(key))
        if key == 'log_file' or key == 'log_console':
            if not isinstance(settings[key], bool):
                raise ValueError('{} must be boolean'.format(key))


# TODO: make class CamelCase
class urbanaccess_config(object):
    """
    A set of configuration variables to initiate the
    configuration settings for UrbanAccess.

    Parameters
    ----------
    data_folder : str
        location to save and load data files
    logs_folder : str
        location to write log files
    log_file : bool
        if True, save log output to a log file in logs_folder
    log_console : bool
        if True, print log output to the console
    log_name : str
        name of the logger
    log_filename : str
        name of the log file
    txt_encoding : str
        default text encoding used by the GTFS files, to be passed to
        Python's open() function. Must be a valid encoding recognized by
        Python codecs.
    gtfs_api : dict
        dictionary of the name of the GTFS API service as the key and
        the GTFS API server root URL as the value to pass to the GTFS loader
    """

    def __init__(self,
                 data_folder='data',
                 logs_folder='logs',
                 log_file=True,
                 log_console=False,
                 log_name='urbanaccess',
                 log_filename='urbanaccess',
                 txt_encoding='utf-8',
                 gtfs_api={'gtfsdataexch': (
                         'http://www.gtfs-data-exchange.com/'
                         'api/agencies?format=csv')}):

        self.data_folder = data_folder
        self.logs_folder = logs_folder
        self.log_file = log_file
        self.log_console = log_console
        self.log_name = log_name
        self.log_filename = log_filename
        self.txt_encoding = txt_encoding
        self.gtfs_api = gtfs_api

    @classmethod
    def from_yaml(cls, configdir='configs',
                  yamlname='urbanaccess_config.yaml'):
        """
        Create an urbanaccess_config instance from a saved YAML configuration.

        Parameters
        ----------
        configdir : str, optional
            Directory to load a YAML file.
        yamlname : str or file like, optional
            File name from which to load a YAML file.
        Returns
        -------
        urbanaccess_config
        """

        if not isinstance(configdir, str):
            raise ValueError('configdir must be a string')
        if not os.path.exists(configdir):
            raise ValueError('{} does not exist or was not found'.format(
                configdir))
        if not isinstance(yamlname, str):
            raise ValueError('yaml must be a string')

        yaml_file = os.path.join(configdir, yamlname)

        with open(yaml_file, 'r') as f:
            yaml_config = yaml.safe_load(f)

        settings = cls(data_folder=yaml_config.get('data_folder', 'data'),
                       logs_folder=yaml_config.get('logs_folder', 'logs'),
                       log_file=yaml_config.get('log_file', True),
                       log_console=yaml_config.get('log_console', False),
                       log_name=yaml_config.get('log_name', 'urbanaccess'),
                       log_filename=yaml_config.get('log_filename',
                                                    'urbanaccess'),
                       txt_encoding=yaml_config.get('txt_encoding', 'utf-8'),
                       gtfs_api=yaml_config.get('gtfs_api', {
                           'gtfsdataexch':
                               ('http://www.gtfs-data-exchange.com/'
                                'api/agencies?format=csv')}),
                       )

        return settings

    def to_dict(self):
        """
        Return a dict representation of an urbanaccess_config instance.
        """
        return {'data_folder': self.data_folder,
                'logs_folder': self.logs_folder,
                'log_file': self.log_file,
                'log_console': self.log_console,
                'log_name': self.log_name,
                'log_filename': self.log_filename,
                'txt_encoding': self.txt_encoding,
                'gtfs_api': self.gtfs_api,
                }

    def to_yaml(self, configdir='configs', yamlname='urbanaccess_config.yaml',
                overwrite=False):
        """
        Save an urbanaccess_config representation to a YAML file.

        Parameters
        ----------
        configdir : str, optional
            Directory to save a YAML file.
        yamlname : str or file like, optional
            File name to which to save a YAML file.
        overwrite : bool, optional
            if true, overwrite an existing same name YAML
            file in specified directory
        Returns
        -------
        Nothing
        """

        if not isinstance(configdir, str):
            raise ValueError('configdir must be a string')
        if not os.path.exists(configdir):
            raise ValueError('{} does not exist or was not found'.format(
                configdir))
            os.makedirs(configdir)
        if not isinstance(yamlname, str):
            raise ValueError('yaml must be a string')
        yaml_file = os.path.join(configdir, yamlname)
        if overwrite is False and os.path.isfile(yaml_file) is True:
            raise ValueError(
                '{} already exists. Rename or turn overwrite to True'.format(
                    yamlname))
        else:
            with open(yaml_file, 'w') as f:
                yaml.dump(self.to_dict(), f, default_flow_style=False)


# set global variables
# route types taken from 'route_type' definition on route.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#routestxt
_ROUTES_MODE_TYPE_LOOKUP = {
    0: 'Street Level Rail: Tram, Streetcar, or Light rail',
    1: 'Underground rail: Subway or Metro',
    2: 'Rail: intercity or long-distance',
    3: 'Bus',
    4: 'Ferry',
    5: 'Cable tram or car',
    6: 'Aerial lift: Gondola or Suspended cable car',
    7: 'Steep incline: Funicular',
    11: 'Trolleybus',
    12: 'Monorail'}

# location type taken from 'location_type' definition on stops.txt GTFS
# file: https://developers.google.com/transit/gtfs/reference#stopstxt
_STOPS_LOCATION_TYPE_LOOKUP = {
    np.nan: 'stop',
    0: 'stop',
    1: 'station',
    2: 'station entrance or exit',
    3: 'generic node',
    4: 'boarding area'}

# wheelchair boarding taken from 'wheelchair_boarding' definition on
# stops.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#stopstxt
_STOPS_WHEELCHAIR_BOARDINGS = {
    0: 'No accessibility information available for the stop',
    1: 'At least some vehicles at this stop can be boarded by a '
       'rider in a wheelchair',
    2: 'Wheelchair boarding is not possible at this stop'}

# pickup type taken from 'pickup_type' definition on
# stops_times.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#stop_timestxt
_STOP_TIMES_PICKUP_TYPE = {
    np.nan: 'Regularly scheduled pickup',
    0: 'Regularly scheduled pickup',
    1: 'No pickup available',
    2: 'Must phone agency to arrange pickup',
    3: 'Must coordinate with driver to arrange pickup'}

# dropoff type taken from 'dropoff_type' definition on
# stops_times.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#stop_timestxt
_STOP_TIMES_DROP_OFF_TYPE = {
    np.nan: 'Regularly scheduled drop off',
    0: 'Regularly Scheduled',
    1: 'No drop off available',
    2: 'Must phone agency to arrange drop off',
    3: 'Must coordinate with driver to arrange drop off'}

# timepoint taken from 'timepoint' definition on
# stops_times.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#stop_timestxt
_STOP_TIMES_TIMEPOINT = {
    0: 'Times are considered approximate',
    1: 'Times are considered exact',
    np.nan: 'Times are considered exact'}

# bikes allowed taken from 'bikes_allowed' definition on
# trips.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#tripstxt
_TRIPS_BIKES_ALLOWED = {
    np.nan: 'No bike information for the trip',
    0: 'No bike information for the trip',
    1: 'Vehicle being used on this particular trip can accommodate at least '
       'one bicycle.',
    2: 'No bicycles are allowed on this trip'}

# wheelchair accessible taken from 'wheelchair_accessible' definition on
# trips.txt GTFS file:
# https://developers.google.com/transit/gtfs/reference#tripstxt
_TRIPS_WHEELCHAIR_ACCESSIBLE = {
    np.nan: 'No accessibility information for the trip',
    0: 'No accessibility information for the trip',
    1: 'Vehicle being used on this particular trip can accommodate at least '
       'one rider in a wheelchair',
    2: 'No riders in wheelchairs can be accommodated on this trip'}


# instantiate the UrbanAccess configuration object and check format
settings = urbanaccess_config()
_format_check(settings.to_dict())
