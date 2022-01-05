# The following logging functions were modified from the osmnx library and
# used with permission from the author Geoff Boeing:
# log, _get_logger: https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

import logging as lg
import yaml
import time
import unicodedata
import sys
import datetime as dt
import os
import pandas as pd

from urbanaccess import config


# networkx is an optional dependency so check if its installed and if not
# will raise import error downstream where it is used
try:
    import networkx
except ImportError:
    networkx = None


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


def df_to_networkx(
        nodes=None, edges=None,
        from_id_col=None, to_id_col=None,
        edge_id_col=None, edge_weight_col=None,
        node_id_col=None, node_x_col='x', node_y_col='y',
        node_attr=None, edge_attr=None,
        graph_name='urbanaccess network', crs={'init': 'epsg:4326'}):
    """
    Convert node and edge network DataFrames to a NetworkX MultiGraph. The
    resulting network can then be used for further network analysis using
    NetworkX.

    Parameters
    ----------
    nodes : pandas.DataFrame
        network nodes DataFrame
    edges : pandas.DataFrame
        network edges DataFrame
    from_id_col : str
        name of edge column holding the from node IDs that correspond to the
        node table node IDs, typically values are ints
    to_id_col : str
        name of edge column holding the to node IDs that correspond to the
        node table node IDs, typically values are ints
    edge_id_col : str
        name of edge column holding the unique edge IDs, typically values are
        sequential ints
    edge_weight_col : str
        name of edge column holding the network impedance
    node_id_col : str
        name of node column holding the unique node IDs that correspond to
        the edge from and to IDs, typically values are ints
    node_x_col : str, optional
        name of node column holding the x coordinates, default is 'x'
    node_y_col : str, optional
        name of node column holding the y coordinates, default is 'y'
    node_attr : list, optional
        list of column names as strings in the nodes table to add as graph
        attributes. By default all columns will be added unless a subset is
        specified in this parameter.
    edge_attr : list, optional
        list of column names as strings in the edges table to add as graph
        attributes. By default all columns will be added unless a subset is
        specified in this parameter.
    graph_name : str, optional
        optional NetworkX metadata of the name of the graph, default is
        'urbanaccess network'
    crs : dict, optional
        optional NetworkX metadata specifying the coordinate system of the
        'x' and 'y' coordinates, default is {'init': 'epsg:4326'}
    Returns
    -------
    nx_graph : networkx.MultiGraph
        node and edge network as a NetworkX MultiGraph
    """
    start_time = time.time()

    if networkx is None:
        raise ImportError("networkx must be installed to convert network "
                          "to a NetworkX graph.")

    req_params = [nodes, edges, from_id_col, to_id_col, edge_id_col,
                  edge_weight_col, node_id_col, node_x_col, node_y_col,
                  graph_name, crs]
    for param in req_params:
        if param is None:
            raise ValueError('required parameters cannot be None.')

    if len(nodes) == 0:
        raise ValueError('nodes DataFrame contains no records.')
    if len(edges) == 0:
        raise ValueError('edges DataFrame contains no records.')

    # by default use all columns in node and edge dfs, otherwise use cols
    # that were passed
    node_col_list = list(nodes.columns)
    edge_col_list = list(edges.columns)
    if node_attr is None:
        node_attrs = node_col_list.copy()
    else:
        for col in node_attr:
            if col not in node_col_list:
                raise ValueError(
                    "nodes DataFrame missing expected column(s) passed "
                    "in 'node_attr' parameter.")
    if edge_attr is None:
        edge_attrs = edge_col_list.copy()
    else:
        for col in edge_attr:
            if col not in edge_col_list:
                raise ValueError(
                    "edges DataFrame missing expected column(s) passed "
                    "in 'edge_attr' parameter.")

    # check that the minimum required cols are present and have no nulls
    node_min_req_cols = [node_id_col, node_x_col, node_y_col]
    edge_min_req_cols = [from_id_col, to_id_col, edge_id_col, edge_weight_col]
    for col in node_min_req_cols:
        if col not in node_col_list:
            raise ValueError(
                'nodes DataFrame missing a required column. Expected '
                'columns: {}.'.format(node_min_req_cols))
        else:
            if nodes[col].isnull().sum() != 0:
                raise ValueError(
                    'nodes DataFrame have nulls in required column: {}. '
                    'No nulls allowed.'.format(col))
    for col in edge_min_req_cols:
        if col not in edge_col_list:
            raise ValueError(
                'edges DataFrame missing a required column. Expected '
                'columns: {}.'.format(edge_min_req_cols))
        else:
            if edges[col].isnull().sum() != 0:
                raise ValueError(
                    'edges DataFrame have nulls in required column: {}. '
                    'No nulls allowed.'.format(col))

    # check that node and edge node IDs match: this catches case 1: where
    # node ids from and to id columns do not exist in node id table,
    # and case 2: where nodes ids in nodes table do not exist in edge from
    # and to ids
    edge_cols = [from_id_col, to_id_col]
    for col in edge_cols:
        missing_edge_ids = edges.loc[~edges[col].isin(
            nodes[node_id_col].unique())]
        if len(missing_edge_ids) != 0:
            raise ValueError(
                'node IDs in {} column in edges DataFrame do not match node '
                'IDs in nodes DataFrame.'.format(col))

    log('Converting UrbanAccess network to NetworkX graph...')
    # operate on a copy of the node and edge dfs
    nodes_df = nodes.copy()
    edges_df = edges.copy()
    # instantiate the networkx graph object as a MultiDiGraph type
    nx_graph = networkx.MultiDiGraph()

    # build nx node information
    nodes_df['nx_node'] = nodes_df[node_attrs].to_dict(orient='records')
    nodes_df['nx_node'] = list(
        nodes_df[[node_id_col, 'nx_node']].itertuples(index=False, name=None))
    # build nx edge information
    edges_df['nx_edge'] = edges_df[edge_attrs].to_dict(orient='records')
    edges_df['nx_edge'] = list(
        edges_df[[from_id_col, to_id_col, edge_id_col, 'nx_edge']].itertuples(
            index=False, name=None))
    # add node and edge nx information to nx graph
    nx_graph.add_nodes_from(nodes_df['nx_node'].to_list())
    nx_graph.add_edges_from(edges_df['nx_edge'].to_list())
    # set standard metadata
    nx_graph.graph['crs'] = crs
    nx_graph.graph['name'] = graph_name

    log('UrbanAccess network to NetworkX graph conversion complete. '
        'Took {:,.2f} seconds.'.format(time.time() - start_time))
    return nx_graph
