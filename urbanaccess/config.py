import itertools
import numpy as np

from urbanaccess.utils import _dict_to_yaml, _yaml_to_dict


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
    gtfs_api_schema_keys = ['gtfsdataexch']
    str_keys = ['data_folder', 'images_folder', 'image_filename',
                'logs_folder', 'log_name', 'log_filename', 'txt_encoding']
    bool_keys = ['log_file', 'log_console']
    dict_keys = ['gtfs_api']
    valid_keys = list(itertools.chain(str_keys, bool_keys, dict_keys))
    valid_keys = sorted(valid_keys)
    settings_keys = list(settings.keys())

    if set(settings_keys) != set(valid_keys):
        raise ValueError("Configuration keys: {} do not match required "
                         "keys: {}.".format(settings_keys, valid_keys))
    for key in settings_keys:
        if key in str_keys:
            if not isinstance(settings[key], str):
                raise ValueError(
                    "Key: '{}' value must be string.".format(key))
        if key in bool_keys:
            if not isinstance(settings[key], bool):
                raise ValueError(
                    "Key: '{}' value must be boolean.".format(key))
        if key in dict_keys:
            for gtfs_api_key in settings['gtfs_api'].keys():
                if gtfs_api_key not in gtfs_api_schema_keys:
                    raise ValueError(
                        "gtfs_api key: '{}' does not match valid key(s):"
                        " {}.".format(gtfs_api_key, gtfs_api_schema_keys))
                if not isinstance(settings['gtfs_api'][gtfs_api_key], str):
                    raise ValueError(
                        "gtfs_api key: '{}' value must be string.".format(
                            gtfs_api_key))


# TODO: make class CamelCase
class urbanaccess_config(object):
    """
    A set of configuration variables to initiate the
    configuration settings for UrbanAccess.

    Parameters
    ----------
    data_folder : str
        location to save and load data files
    images_folder : str
        location to save images saved from plotting networks
    image_filename : str
        default name of image to use when saving plots
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

    def __init__(
            self,
            data_folder='data',
            images_folder='images',
            image_filename='urbanaccess_plot',
            logs_folder='logs',
            log_file=True,
            log_console=False,
            log_name='urbanaccess',
            log_filename='urbanaccess',
            txt_encoding='utf-8',
            gtfs_api={'gtfsdataexch': (
                'http://www.gtfs-data-exchange.com/api/agencies?format=csv')}):

        self.data_folder = data_folder
        self.images_folder = images_folder
        self.image_filename = image_filename
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
        yaml_config = _yaml_to_dict(yaml_dir=configdir, yaml_name=yamlname)

        settings = cls(
            data_folder=yaml_config.get('data_folder', 'data'),
            images_folder=yaml_config.get('images_folder', 'images'),
            image_filename=yaml_config.get(
                'image_filename', 'urbanaccess_plot'),
            logs_folder=yaml_config.get('logs_folder', 'logs'),
            log_file=yaml_config.get('log_file', True),
            log_console=yaml_config.get('log_console', False),
            log_name=yaml_config.get('log_name', 'urbanaccess'),
            log_filename=yaml_config.get('log_filename', 'urbanaccess'),
            txt_encoding=yaml_config.get('txt_encoding', 'utf-8'),
            gtfs_api=yaml_config.get(
                'gtfs_api', {'gtfsdataexch': (
                    'http://www.gtfs-data-exchange.com/api/agencies?format=csv'
                )
                }
            )
        )

        return settings

    def to_dict(self):
        """
        Return a dict representation of an urbanaccess_config instance.
        """
        return {'data_folder': self.data_folder,
                'images_folder': self.images_folder,
                'image_filename': self.image_filename,
                'logs_folder': self.logs_folder,
                'log_file': self.log_file,
                'log_console': self.log_console,
                'log_name': self.log_name,
                'log_filename': self.log_filename,
                'txt_encoding': self.txt_encoding,
                'gtfs_api': self.gtfs_api
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
            if true, will overwrite an existing YAML
            file in specified directory if file names are the same
        Returns
        -------
        Nothing
        """
        _dict_to_yaml(dictionary=self.to_dict(), yaml_dir=configdir,
                      yaml_name=yamlname, overwrite=overwrite)


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

# set category for GTFS file types for processing
_GTFS_TXT_FILE_TYPES = {
    'required_files': ['stops.txt', 'routes.txt', 'trips.txt',
                       'stop_times.txt'],
    'optional_files': ['agency.txt', 'shapes.txt'],
    'calendar_files': ['calendar.txt', 'calendar_dates.txt']}

_SUPPORTED_GTFS_TXT_FILES = []
for name, file_list in _GTFS_TXT_FILE_TYPES.items():
    _SUPPORTED_GTFS_TXT_FILES.extend(file_list)


# set data schema variables for when GTFS txt files are read as DataFrames
_GTFS_READ_TXT_CONFIG = {
    'agency': {'req_dtypes': None,
               'opt_dtypes': None,
               'numeric_converter': None,
               'remove_whitespace': None,
               'min_required_cols': None},
    'stops': {'req_dtypes': {'stop_id': object},
              'opt_dtypes': None,
              'numeric_converter': ['stop_lat', 'stop_lon'],
              'remove_whitespace': ['stop_id'],
              'min_required_cols': ['stop_id', 'stop_lat', 'stop_lon']},
    'routes': {'req_dtypes': {'route_id': object},
               'opt_dtypes': {'route_short_name': object,
                              'route_long_name': object,
                              'route_color': object,
                              'route_text_color': object},
               'numeric_converter': None,
               'remove_whitespace': ['route_id'],
               'min_required_cols': ['route_id']},
    'trips': {'req_dtypes': {'trip_id': object,
                             'service_id': object,
                             'route_id': object},
              'opt_dtypes': {'shape_id': object},
              'numeric_converter': None,
              'remove_whitespace': ['trip_id', 'service_id', 'route_id', 
                                    'shape_id'],
              'min_required_cols': ['trip_id', 'service_id', 'route_id']},

    'stop_times': {'req_dtypes': {'trip_id': object,
                                  'stop_id': object,
                                  'departure_time': object,
                                  'arrival_time': object},
                   'opt_dtypes': None,
                   'numeric_converter': None,
                   'remove_whitespace': ['trip_id', 'stop_id',
                                         'departure_time', 'arrival_time'],
                   'min_required_cols': ['trip_id', 'stop_id',
                                         'departure_time', 'arrival_time']},
    'shapes': {'req_dtypes': {'shape_id': object},
              'opt_dtypes': None,
              'numeric_converter': ['shape_pt_lat', 'shape_pt_lon',
                                    'shape_pt_sequence'],
              'remove_whitespace': ['shape_id'],
              'min_required_cols': ['shape_id', 'shape_pt_lat',
                                    'shape_pt_lon', 'shape_pt_sequence']},
    'calendar': {'req_dtypes': {'service_id': object,
                                'start_date': object,
                                'end_date': object},
                 'opt_dtypes': None,
                 'numeric_converter': ['monday', 'tuesday', 'wednesday',
                                       'thursday', 'friday', 'saturday',
                                       'sunday'],
                 'remove_whitespace': ['service_id'],
                 'min_required_cols': ['service_id', 'monday', 'tuesday',
                                       'wednesday', 'thursday', 'friday',
                                       'saturday', 'sunday', 'start_date',
                                       'end_date']},
    'calendar_dates': {'req_dtypes': {'service_id': object,
                                      'date': object,
                                      'exception_type': object},
                       'opt_dtypes': None,
                       'numeric_converter': None,
                       'remove_whitespace': ['service_id'],
                       'min_required_cols': ['service_id', 'date',
                                             'exception_type']}
}

# instantiate the UrbanAccess configuration object and check format
settings = urbanaccess_config()
_format_check(settings.to_dict())
