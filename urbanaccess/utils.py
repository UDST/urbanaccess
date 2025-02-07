# The following logging functions were modified from the osmnx library and
# used with permission from the author Geoff Boeing:
# log, _get_logger: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

import logging as lg
import yaml
import unicodedata
import sys
import datetime as dt
import os
import pandas as pd

from urbanaccess import config


def log(message, level=None, name=None, filename=None):
    """
    Write a message to the log file and/or print to the console.

    Parameters
    ----------
    message : string
        the content of the message to log
    level : int, optional
        one of the logger.level constants
    name : string, optional
        name of the logger
    filename : string, optional
        name of the log file

    Returns
    -------
    None
    """

    if level is None:
        level = lg.INFO
    if name is None:
        name = config.settings.log_name
    if filename is None:
        filename = config.settings.log_filename

    if config.settings.log_file:
        # get the current logger or create a new one then log message at
        # requested level
        logger = _get_logger(level=level, name=name, filename=filename)
        if level == lg.DEBUG:
            logger.debug(message)
        elif level == lg.INFO:
            logger.info(message)
        elif level == lg.WARNING:
            logger.warning(message)
        elif level == lg.ERROR:
            logger.error(message)

    # if logging to console is turned on, convert message to ascii and print
    #  to the console only
    if config.settings.log_console:
        # capture current stdout, then switch it to the console, print the
        # message, then switch back to what had been the stdout
        # this prevents logging to notebook - instead, it goes to console
        standard_out = sys.stdout
        sys.stdout = sys.__stdout__

        # convert message to ascii for proper console display in windows
        # terminals
        message = unicodedata.normalize('NFKD', str(message)).encode(
            'ascii', errors='replace').decode()
        print(message)
        sys.stdout = standard_out
    # otherwise print out standard statement
    else:
        print(message)


def _get_logger(level=None, name=None, filename=None):
    """
    Create a logger or return the current one if already instantiated.

    Parameters
    ----------
    level : int, optional
        one of the logger.level constants
    name : string, optional
        name of the logger
    filename : string, optional
        name of the log file

    Returns
    -------
    logger : logger.logger
    """
    # TODO: consider placing default lg.INFO 'log_level' as new config setting
    if level is None:
        level = lg.INFO
    if name is None:
        name = config.settings.log_name
    if filename is None:
        filename = config.settings.log_filename

    logger = lg.getLogger(name)

    # if a logger with this name is not already established
    if not getattr(logger, 'handler_set', None):

        todays_date = dt.datetime.today().strftime('%Y_%m_%d')
        log_filename = '{}/{}_{}.log'.format(config.settings.logs_folder,
                                             filename, todays_date)

        if not os.path.exists(config.settings.logs_folder):
            os.makedirs(config.settings.logs_folder)

        # create file handler and log formatter and establish settings
        handler = lg.FileHandler(log_filename, encoding='utf-8')
        formatter = lg.Formatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.handler_set = True

    return logger


def create_hdf5(dir=None, filename=None, overwrite_hdf5=False):
    """
    Create an empty HDF5 file

    Parameters
    ----------
    dir : string, optional
        directory to save HDF5 file, if None defaults to dir set in
        config.settings.data_folder
    filename : string, optional
        name of the HDF5 file to save with .h5 extension, if None defaults
        to urbanaccess.h5
    overwrite_hdf5 : bool, optional
        if true any existing HDF5 file with the specified name in the
        specified directory will be overwritten

    Returns
    -------
    Nothing
    """
    if dir is None:
        dir = config.settings.data_folder
    else:
        if not isinstance(dir, str):
            raise ValueError('Directory must be a string.')

    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except Exception:
        raise ValueError('Unable to make directory {}.'.format(dir))

    if filename is None:
        filename = 'urbanaccess.h5'
    else:
        if not isinstance(filename, str):
            raise ValueError('Filename must be a string.')

    hdf5_save_path = os.path.join(dir, filename)
    if not filename.endswith('.h5'):
        raise ValueError('HDF5 filename extension must be "h5".')

    if not os.path.exists(hdf5_save_path):
        store = pd.HDFStore(hdf5_save_path)
        store.close()
        log('   New {} HDF5 store created in dir: {}.'.format(filename, dir))
    elif overwrite_hdf5 and os.path.exists(hdf5_save_path):
        store = pd.HDFStore(hdf5_save_path)
        store.close()
        log('   Existing {} HDF5 store in dir: {} has been '
            'overwritten.'.format(filename, dir))
    else:
        log('   Using existing HDF5 store: {}.'.format(hdf5_save_path))

    return hdf5_save_path


def df_to_hdf5(data=None, key=None, overwrite_key=False, dir=None,
               filename=None, overwrite_hdf5=False):
    """
    Write a pandas.DataFrame to a table in a HDF5 file

    Parameters
    ----------
    data : pandas.DataFrame
        pandas.DataFrame to save to a HDF5 table
    key : string
        name of table to save DataFrame as in the HDF5 file
    overwrite_key : bool, optional
        if true any existing table with the specified key name will be
        overwritten
    dir : string
        directory to save HDF5 file
    filename : string
        name of the HDF5 file to save with .h5 extension
    overwrite_hdf5 : bool, optional
        if true any existing HDF5 file with the specified name in the
        specified directory will be overwritten

    Returns
    -------
    Nothing
    """
    hdf5_save_path = create_hdf5(
        dir=dir, filename=filename, overwrite_hdf5=overwrite_hdf5)

    store = pd.HDFStore(hdf5_save_path, mode='r')
    h5_key_str = ''.join(['/', key])

    if h5_key_str not in store.keys():
        store.close()
        data.to_hdf(hdf5_save_path, key=key, mode='a', format='table')
        log('   DataFrame: {} saved in HDF5 store: {}.'.format(
            key, hdf5_save_path))

    elif h5_key_str in store.keys() and overwrite_key:
        store.close()
        data.to_hdf(hdf5_save_path, key=key, mode='a', format='table')
        log('   Existing DataFrame: {} overwritten in HDF5 store: {}.'.format(
            key, hdf5_save_path))

    else:
        store.close()
        log('   Key {} already exists in HDF5 store: {}. '
            'Set to overwrite_key = True to replace existing '
            'data in key.'.format(key, hdf5_save_path))


def hdf5_to_df(dir=None, filename=None, key=None):
    """
    Read data from a HDF5 file to a pandas.DataFrame

    Parameters
    ----------
    dir : string
        directory of the HDF5 file to read from
    filename : string
        name of the HDF5 file with .h5 extension to read from
    key : string
        table inside the HDF5 file to return as a pandas.DataFrame

    Returns
    -------
    df : pandas.DataFrame
    """
    if dir is None:
        dir = config.settings.data_folder
    else:
        if not isinstance(dir, str):
            raise ValueError('Directory must be a string.')

    if filename is None:
        filename = 'urbanaccess_net.h5'
    else:
        if not isinstance(filename, str):
            raise ValueError('Filename must be a string.')

    hdf5_load_path = os.path.join(dir, filename)

    if not filename.endswith('.h5'):
        raise ValueError('HDF5 filename extension must be "h5".')
    if not os.path.exists(hdf5_load_path):
        raise ValueError('Unable to find directory or file: {}.'.format(
            hdf5_load_path))

    with pd.HDFStore(hdf5_load_path) as store:
        log('   Reading HDF5 store: {}...'.format(hdf5_load_path))
        try:
            df = store[key]
            log('   Successfully returned: {} as DataFrame.'.format(key))
        except Exception:
            raise ValueError('Unable to find key: {}. Keys found: {}.'.format(
                key, store.keys()))

        return df


def _yaml_to_dict(yaml_dir, yaml_name):
    """
    Load a YAML file into a dictionary.

    Parameters
    ----------
    yaml_dir : str, optional
        Directory to load a YAML file.
    yaml_name : str or file like, optional
        File name from which to load a YAML file.

    Returns
    -------
    yaml_dict : dict
    """
    dtype_raise_error_msg = '{} must be a string.'
    if not isinstance(yaml_dir, str):
        raise ValueError(dtype_raise_error_msg.format('yaml directory'))
    if not os.path.exists(yaml_dir):
        raise ValueError('{} does not exist or was not found.'.format(
            yaml_dir))
    if not isinstance(yaml_name, str):
        raise ValueError(dtype_raise_error_msg.format('yaml file name'))

    yaml_file = os.path.join(yaml_dir, yaml_name)

    with open(yaml_file, 'r') as f:
        yaml_dict = yaml.safe_load(f)
    if not isinstance(yaml_dict, dict):
        raise ValueError('Yaml data must be a dictionary.')
    return yaml_dict


def _dict_to_yaml(dictionary, yaml_dir, yaml_name, overwrite=False):
    """
    Save dictionary to a YAML file.

    Parameters
    ----------
    dictionary : dict
        Dictionary to save in a YAML file.
    yaml_dir : str, optional
        Directory to save a YAML file.
    yaml_name : str or file like, optional
        File name to which to save a YAML file.
    overwrite : bool, optional
        if true, will overwrite an existing YAML
        file in specified directory if file names are the same
    Returns
    -------
    Nothing
    """
    if not isinstance(dictionary, dict):
        raise ValueError('Data to convert to YAML must be a dictionary.')
    dtype_raise_error_msg = '{} must be a string.'
    if not isinstance(yaml_dir, str):
        raise ValueError(dtype_raise_error_msg.format('yaml directory'))
    if not os.path.exists(yaml_dir):
        log('{} does not exist or was not found and will be '
            'created.'.format(yaml_dir))
        os.makedirs(yaml_dir)
    if not isinstance(yaml_name, str):
        raise ValueError(dtype_raise_error_msg.format('yaml file name'))
    yaml_file = os.path.join(yaml_dir, yaml_name)
    if overwrite is False and os.path.isfile(yaml_file) is True:
        raise ValueError(
            '{} already exists. Rename or set overwrite to True.'.format(
                yaml_name))
    else:
        with open(yaml_file, 'w') as f:
            yaml.dump(dictionary, f, default_flow_style=False)
        log('{} file successfully created.'.format(yaml_file))


def _add_unique_trip_id(df):
    """
    Create 'unique_trip_id' column and values in a pandas.DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame to generate 'unique_trip_id' column

    Returns
    -------
    df : pandas.DataFrame
        pandas.DataFrames with 'unique_trip_id' column added
    """
    df['unique_trip_id'] = df['trip_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_')
    return df


def _add_unique_route_id(df):
    """
    Create 'unique_route_id' column and values in a pandas.DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame to generate 'unique_route_id' column

    Returns
    -------
    df : pandas.DataFrame
        pandas.DataFrames with 'unique_route_id' column added
    """
    df['unique_route_id'] = df['route_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_')
    return df


def _add_unique_stop_id(df):
    """
    Create 'unique_stop_id' column and values in a pandas.DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame to generate 'unique_stop_id' column

    Returns
    -------
    df : pandas.DataFrame
        pandas.DataFrames with 'unique_stop_id' column added
    """
    df['unique_stop_id'] = df['stop_id'].str.cat(
        df['unique_agency_id'].astype('str'), sep='_')
    return df
