# The following logging functions were modified from the osmnx library and
# used with permission from the author Geoff Boeing:
# log, _get_logger: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

import logging as lg
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
        message = unicodedata.normalize('NFKD', unicode(message)).encode(
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

    if level is None:
        level = config.settings.log_level
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
    Create a empty hdf5 file

    Parameters
    ----------
    dir : string, optional
        directory to save hdf5 file, if None defaults to dir set in
        config.settings.data_folder
    filename : string, optional
        name of the hdf5 file to save with .h5 extension, if None defaults
        to urbanaccess.h5
    overwrite_hdf5 : bool, optional
        if true any existing hdf5 file with the specified name in the
        specified directory will be overwritten

    Returns
    -------
    None
    """
    if dir is None:
        dir = config.settings.data_folder
    else:
        if not isinstance(dir, str):
            raise ValueError('Directory must be a string')

    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except Exception:
        raise ValueError('Unable to make directory {}'.format(dir))

    if filename is None:
        filename = 'urbanaccess.h5'
    else:
        if not isinstance(filename, str):
            raise ValueError('Filename must be a string')

    hdf5_save_path = '{}/{}'.format(dir, filename)
    if not filename.endswith('.h5'):
        raise ValueError('hdf5 filename extension must be "h5"')

    if not os.path.exists(hdf5_save_path):
        store = pd.HDFStore(hdf5_save_path)
        store.close()
        log('New {} hdf5 store created in dir: {}'.format(filename, dir))
    elif overwrite_hdf5 and os.path.exists(hdf5_save_path):
        store = pd.HDFStore(hdf5_save_path)
        store.close()
        log('Existing {} hdf5 store in dir: has been overwritten.'.format(
            hdf5_save_path))
    else:
        log('Using existing {} hdf5 store.'.format(hdf5_save_path))

    return hdf5_save_path


def df_to_hdf5(data=None, key=None, overwrite_key=False, dir=None,
               filename=None, overwrite_hdf5=False):
    """
    Write a pandas dataframe to a table in a hdf5 file

    Parameters
    ----------
    data : pandas.DataFrame
        pandas dataframe to save to a hdf5 table
    key : string
        name of table to save dataframe as in the hdf5 file
    overwrite_key : bool, optional
        if true any existing table with the specified key name will be
        overwritten
    dir : string
        directory to save hdf5 file
    filename : string
        name of the hdf5 file to save with .h5 extension
    overwrite_hdf5 : bool, optional
        if true any existing hdf5 file with the specified name in the
        specified directory will be overwritten

    Returns
    -------
    None
    """
    hdf5_save_path = create_hdf5(dir=dir, filename=filename,
                                 overwrite_hdf5=overwrite_hdf5)

    store = pd.HDFStore(hdf5_save_path, mode='r')

    if not ''.join(['/', key]) in store.keys():
        store.close()
        data.to_hdf(hdf5_save_path, key=key, mode='a', format='table')
        log('{} saved in {} hdf5 store.'.format(key, hdf5_save_path))

    elif ''.join(['/', key]) in store.keys() and overwrite_key:
        store.close()
        data.to_hdf(hdf5_save_path, key=key, mode='a', format='table')
        log('Existing {} overwritten in {} hdf5 store.'.format(key,
                                                               hdf5_save_path))

    else:
        store.close()
        log(
            'Key {} already exists in {} hdf5 store. Set to overwrite_key = '
            'True to replace.'.format(
                key, hdf5_save_path))


def hdf5_to_df(dir=None, filename=None, key=None):
    """
    Read data from a hdf5 file to a pandas dataframe

    Parameters
    ----------
    dir : string
        directory of the hdf5 file to read from
    filename : string
        name of the hdf5 file with .h5 extension to read from
    key : string
        table inside the hdf5 file to return as a pandas dataframe

    Returns
    -------
    df : pandas.DataFrame
    """
    if dir is None:
        dir = config.settings.data_folder
    else:
        if not isinstance(dir, str):
            raise ValueError('Directory must be a string')

    if filename is None:
        filename = 'urbanaccess_net.h5'
    else:
        if not isinstance(filename, str):
            raise ValueError('Filename must be a string')

    hdf5_load_path = '{}/{}'.format(dir, filename)

    if not filename.endswith('.h5'):
        raise ValueError('hdf5 filename extension must be "h5"')
    if not os.path.exists(hdf5_load_path):
        raise ValueError('Unable to find directory or file: {}'.format(
            hdf5_load_path))

    with pd.HDFStore(hdf5_load_path) as store:
        # TODO: fix print statement to only display current key, not all keys
        log('Successfully read store: {} with the following keys: {}'.format(
            hdf5_load_path, store.keys()))
        try:
            df = store[key]
            ('Returned {} as dataframe'.format(key))
        except Exception:
            raise ValueError(
                'Unable to find key: {}. Keys found: {}'.format(key,
                                                                store.keys()))

        return df
