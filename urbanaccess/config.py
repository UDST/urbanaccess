import os
import yaml


def _format_check(settings):
    """
    Check the format of a urbanaccess_config object.

    Parameters
    ----------
    settings : dict
        urbanaccess_config as a dictionary
    Returns
    -------
    Nothing
    """

    valid_keys = ['data_folder', 'logs_folder', 'log_file',
                  'log_console', 'log_name', 'log_filename', 'gtfs_api']

    for key in settings.keys():
        if key not in valid_keys:
            raise ValueError('{} not found in list of valid configuation '
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
        if true, save log output to a log file in logs_folder
    log_console : bool
        if true, print log output to the console
    log_name : str
        name of the logger
    log_filename : str
        name of the log file
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
                 gtfs_api={'gtfsdataexch': (
                         'http://www.gtfs-data-exchange.com/'
                         'api/agencies?format=csv')}):

        self.data_folder = data_folder
        self.logs_folder = logs_folder
        self.log_file = log_file
        self.log_console = log_console
        self.log_name = log_name
        self.log_filename = log_filename
        self.gtfs_api = gtfs_api

    @classmethod
    def from_yaml(cls, configdir='configs',
                  yamlname='urbanaccess_config.yaml'):
        """
        Create a urbanaccess_config instance from a saved YAML configuration.

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
            yaml_config = yaml.load(f)

        settings = cls(data_folder=yaml_config.get('data_folder', 'data'),
                       logs_folder=yaml_config.get('logs_folder', 'logs'),
                       log_file=yaml_config.get('log_file', True),
                       log_console=yaml_config.get('log_console', False),
                       log_name=yaml_config.get('log_name', 'urbanaccess'),
                       log_filename=yaml_config.get('log_filename',
                                                    'urbanaccess'),
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
                'gtfs_api': self.gtfs_api,
                }

    def to_yaml(self, configdir='configs', yamlname='urbanaccess_config.yaml',
                overwrite=False):
        """
        Save a urbanaccess_config representation to a YAML file.

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


# instantiate the UrbanAccess configuration object and check format
settings = urbanaccess_config()
_format_check(settings.to_dict())
