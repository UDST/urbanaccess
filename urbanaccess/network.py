import time
from sklearn.neighbors import KDTree
import pandas as pd
from geopy.distance import vincenty

from urbanaccess.utils import log, df_to_hdf5, hdf5_to_df
from urbanaccess import config


class urbanaccess_network(object):
    """
    A urbanaccess object of Pandas DataFrames representing
    the components of a graph network

    Parameters
    ----------
    transit_nodes : pandas.DataFrame
    transit_edges : pandas.DataFrame
    net_connector_edges : pandas.DataFrame
    osm_nodes : pandas.DataFrame
    osm_edges : pandas.DataFrame
    net_nodes : pandas.DataFrame
    net_edges : pandas.DataFrame
    """

    def __init__(self,
                 transit_nodes=pd.DataFrame(),
                 transit_edges=pd.DataFrame(),
                 net_connector_edges=pd.DataFrame(),
                 osm_nodes=pd.DataFrame(),
                 osm_edges=pd.DataFrame(),
                 net_nodes=pd.DataFrame(),
                 net_edges=pd.DataFrame()):
        self.transit_nodes = transit_nodes
        self.transit_edges = transit_edges
        self.net_connector_edges = net_connector_edges
        self.osm_nodes = osm_nodes
        self.osm_edges = osm_edges
        self.net_nodes = net_nodes
        self.net_edges = net_edges


# instantiate the UrbanAccess network object
ua_network = urbanaccess_network()


def _nearest_neighbor(df1, df2):
    """
    For a DataFrame of xy coordinates find the nearest xy
    coordinates in a subsequent DataFrame

    Parameters
    ----------
    df1 : pandas.DataFrame
        DataFrame of records to return as the nearest record to records in df2
    df2 : pandas.DataFrame
        DataFrame of records with xy coordinates for which to find the
        nearest record in df1 for
    Returns
    -------
    df1.index.values[indexes] : pandas.Series
        index of records in df1 that are nearest to the coordinates in df2
    """

    kdt = KDTree(df1.as_matrix())
    indexes = kdt.query(df2.as_matrix(), k=1, return_distance=False)
    return df1.index.values[indexes]


def integrate_network(urbanaccess_network, headways=False,
                      urbanaccess_gtfsfeeds_df=None, headway_statistic='mean'):
    """
    Create an integrated network comprised of transit and OSM nodes and edges
    by connecting the transit network with the osm network.
    travel time is in units of minutes

    Parameters
    ----------
    urbanaccess_network : object
        ua_network object with transit_edges, transit_nodes,
        osm_edges, osm_nodes
    headways : bool, optional
        if true, route stop level headways calculated in a previous step
        will be applied to the osm to transit connector
        edge travel time weights as an approximate measure
        of average passenger transit stop waiting time.
    urbanaccess_gtfsfeeds_df : object, optional
        required if headways is true; the gtfsfeeds_dfs object that holds
        the corresponding headways and stops DataFrames
    headway_statistic : {'mean', 'std', 'min', 'max'}, optional
        required if headways is true; route stop headway
        statistic to apply to the osm to transit connector edges:
        mean, std, min, max. Default is mean.

    Returns
    -------
    urbanaccess_network : object
    urbanaccess_network.transit_edges : pandas.DataFrame
    urbanaccess_network.transit_nodes : pandas.DataFrame
    urbanaccess_network.osm_edges : pandas.DataFrame
    urbanaccess_network.osm_nodes : pandas.DataFrame
    urbanaccess_network.net_connector_edges : pandas.DataFrame
    urbanaccess_network.net_edges : pandas.DataFrame
    urbanaccess_network.net_nodes : pandas.DataFrame
    """

    if urbanaccess_network is None:
        raise ValueError('urbanaccess_network is not specified')
    if urbanaccess_network.transit_edges.empty \
            or urbanaccess_network.transit_nodes.empty \
            or urbanaccess_network.osm_edges.empty \
            or urbanaccess_network.osm_nodes.empty:
        raise ValueError(
            'one of the network objects: transit_edges, transit_nodes, '
            'osm_edges, or osm_nodes were found to be empty.')

    log('Loaded UrbanAccess network components comprised of:')
    log('     Transit: {:,} nodes and {:,} edges;'.format(
        len(urbanaccess_network.transit_nodes),
        len(urbanaccess_network.transit_edges)))
    log('     OSM: {:,} nodes and {:,} edges'.format(
        len(urbanaccess_network.osm_nodes),
        len(urbanaccess_network.osm_edges)))

    if not isinstance(headways, bool):
        raise ValueError('headways must be bool type')

    if headways:

        if urbanaccess_gtfsfeeds_df is None or \
                urbanaccess_gtfsfeeds_df.headways.empty or \
                urbanaccess_gtfsfeeds_df.stops.empty:
            raise ValueError(
                'stops and headway DataFrames were not found in the '
                'urbanaccess_gtfsfeeds object. Please create these '
                'DataFrames in order to use headways.')

        valid_stats = ['mean', 'std', 'min', 'max']
        if headway_statistic not in valid_stats or not isinstance(
                headway_statistic, str):
            raise ValueError('{} is not a supported statistic or is not a '
                             'string'.format(headway_statistic))

        transit_edge_cols = urbanaccess_network.transit_edges.columns
        if 'node_id_from' not in transit_edge_cols or 'from' in \
                transit_edge_cols:
            urbanaccess_network.transit_edges.rename(
                columns={'from': 'node_id_from'}, inplace=True)
        if 'node_id_to' not in transit_edge_cols or 'to' in transit_edge_cols:
            urbanaccess_network.transit_edges.rename(
                columns={'to': 'node_id_to'}, inplace=True)

        urbanaccess_network.transit_edges['node_id_route_from'] = (
            urbanaccess_network.transit_edges['node_id_from'].str.cat(
                urbanaccess_network.transit_edges['unique_route_id'].astype(
                    'str'), sep='_'))
        urbanaccess_network.transit_edges['node_id_route_to'] = (
            urbanaccess_network.transit_edges['node_id_to'].str.cat(
                urbanaccess_network.transit_edges['unique_route_id'].astype(
                    'str'), sep='_'))

        urbanaccess_network.transit_nodes = _route_id_to_node(
            stops_df=urbanaccess_gtfsfeeds_df.stops,
            edges_w_routes=urbanaccess_network.transit_edges)

        net_connector_edges = _connector_edges(
            osm_nodes=urbanaccess_network.osm_nodes,
            transit_nodes=urbanaccess_network.transit_nodes,
            travel_speed_mph=3)

        urbanaccess_network.net_connector_edges = _add_headway_impedance(
            ped_to_transit_edges_df=net_connector_edges,
            headways_df=urbanaccess_gtfsfeeds_df.headways,
            headway_statistic=headway_statistic)

    else:
        urbanaccess_network.net_connector_edges = _connector_edges(
            osm_nodes=urbanaccess_network.osm_nodes,
            transit_nodes=urbanaccess_network.transit_nodes,
            travel_speed_mph=3)

    # change cols in transit edges and nodes
    if headways:
        urbanaccess_network.transit_edges.rename(columns={
            'node_id_route_from': 'from', 'node_id_route_to': 'to'},
            inplace=True)
        urbanaccess_network.transit_edges.drop(['node_id_from', 'node_id_to'],
                                               inplace=True, axis=1)
        urbanaccess_network.transit_nodes.reset_index(inplace=True, drop=False)
        urbanaccess_network.transit_nodes.rename(
            columns={'node_id_route': 'id'}, inplace=True)
    else:
        urbanaccess_network.transit_edges.rename(
            columns={'node_id_from': 'from', 'node_id_to': 'to'}, inplace=True)
        urbanaccess_network.transit_nodes.reset_index(inplace=True, drop=False)
        urbanaccess_network.transit_nodes.rename(columns={'node_id': 'id'},
                                                 inplace=True)

    # concat all network components
    urbanaccess_network.net_edges = pd.concat(
        [urbanaccess_network.transit_edges,
         urbanaccess_network.osm_edges,
         urbanaccess_network.net_connector_edges], axis=0)

    urbanaccess_network.net_nodes = pd.concat(
        [urbanaccess_network.transit_nodes,
         urbanaccess_network.osm_nodes], axis=0)

    urbanaccess_network.net_edges, urbanaccess_network.net_nodes = \
        _format_pandana_edges_nodes(edge_df=urbanaccess_network.net_edges,
                                    node_df=urbanaccess_network.net_nodes)

    success_msg_1 = ('Network edge and node network integration completed '
                     'successfully resulting in a total of {:,} nodes '
                     'and {:,} edges:')
    success_msg_2 = '     Transit: {:,} nodes {:,} edges;'
    success_msg_3 = '     OSM: {:,} nodes {:,} edges; and'
    success_msg_4 = '     OSM/Transit connector: {:,} edges.'
    log(success_msg_1.format(len(urbanaccess_network.net_nodes),
                             len(urbanaccess_network.net_edges)))
    log(success_msg_2.format(len(urbanaccess_network.transit_nodes),
                             len(urbanaccess_network.transit_edges)))
    log(success_msg_3.format(len(urbanaccess_network.osm_nodes),
                             len(urbanaccess_network.osm_edges)))
    log(success_msg_4.format(len(urbanaccess_network.net_connector_edges)))

    return urbanaccess_network


def _add_headway_impedance(ped_to_transit_edges_df, headways_df,
                           headway_statistic='mean'):
    """
    Add route stop level headways to the osm to transit connector
    travel time weight column

    Parameters
    ----------
    ped_to_transit_edges_df : pandas.DataFrame
        DataFrame of the osm to transit connectors
    headways_df : pandas.DataFrame
        headways DataFrame
    headway_statistic : {'mean', 'std', 'min', 'max'}, optional
        required if headways is true; route stop headway statistic to apply
        to the osm to transit connector edges:
        mean, std, min, max. Default is mean.

    Returns
    -------
    osm_to_transit_wheadway : pandas.DataFrame

    """

    start_time = time.time()

    log(
        '{} route stop headway will be used for pedestrian to transit edge '
        'impedance.'.format(
            headway_statistic))

    osm_to_transit_wheadway = pd.merge(ped_to_transit_edges_df, headways_df[
        [headway_statistic, 'node_id_route']],
                                       how='left', left_on=['to'],
                                       right_on=['node_id_route'], sort=False,
                                       copy=False)
    osm_to_transit_wheadway['weight_tmp'] = osm_to_transit_wheadway[
                                                'weight'] + (
                                            osm_to_transit_wheadway[
                                                headway_statistic] / 2.0)
    osm_to_transit_wheadway['weight_tmp'].fillna(
        osm_to_transit_wheadway['weight'], inplace=True)
    osm_to_transit_wheadway.drop('weight', axis=1, inplace=True)
    osm_to_transit_wheadway.rename(columns={'weight_tmp': 'weight'},
                                   inplace=True)

    log('Headway impedance calculation completed. Took {:,.2f} seconds'.format(
        time.time() - start_time))

    return osm_to_transit_wheadway


def _route_id_to_node(stops_df, edges_w_routes):
    """
    Assign route ids to the transit nodes table

    Parameters
    ----------
    stops_df : pandas.DataFrame
        processed gtfs stops DataFrame
    edges_w_routes : pandas.DataFrame
        transit edge DataFrame that has route id information

    Returns
    -------
    transit_nodes_wroutes : pandas.DataFrame

    """
    start_time = time.time()

    # create unique stop ids
    stops_df['unique_stop_id'] = (
        stops_df['stop_id'].str.cat(
            stops_df['unique_agency_id'].astype('str'), sep='_'))

    tmp1 = pd.merge(edges_w_routes[['node_id_from',
                                    'node_id_route_from']],
                    stops_df[['unique_stop_id',
                              'stop_lat',
                              'stop_lon']],
                    how='left', left_on='node_id_from',
                    right_on='unique_stop_id', sort=False, copy=False)
    tmp1.rename(columns={'node_id_route_from': 'node_id_route',
                         'stop_lon': 'x',
                         'stop_lat': 'y'},
                inplace=True)
    tmp2 = pd.merge(edges_w_routes[['node_id_to',
                                    'node_id_route_to']],
                    stops_df[['unique_stop_id',
                              'stop_lat',
                              'stop_lon']],
                    how='left',
                    left_on='node_id_to',
                    right_on='unique_stop_id', sort=False, copy=False)
    tmp2.rename(columns={'node_id_route_to': 'node_id_route',
                         'stop_lon': 'x',
                         'stop_lat': 'y'},
                inplace=True)

    transit_nodes_wroutes = pd.concat([tmp1[['node_id_route', 'x', 'y']],
                                       tmp2[['node_id_route', 'x', 'y']]],
                                      axis=0)

    transit_nodes_wroutes.drop_duplicates(subset='node_id_route',
                                          keep='first',
                                          inplace=True)
    # set node index to be unique stop id
    transit_nodes_wroutes = transit_nodes_wroutes.set_index('node_id_route')

    log(
        'routes successfully joined to transit nodes. Took {:,'
        '.2f} seconds'.format(
            time.time() - start_time))

    return transit_nodes_wroutes


def _connector_edges(osm_nodes, transit_nodes, travel_speed_mph=3):
    """
    Generate the connector edges between the osm and transit edges and
    weight by travel time

    Parameters
    ----------
    osm_nodes : pandas.DataFrame
        osm nodes DataFrame
    transit_nodes : pandas.DataFrame
        transit nodes DataFrame
    travel_speed_mph : int, optional
        travel speed to use to calculate travel time across a
        distance on a edge. units are in miles per hour (MPH)
        for pedestrian travel this is assumed to be 3 MPH

    Returns
    -------
    net_connector_edges : pandas.DataFrame

    """
    start_time = time.time()

    transit_nodes['nearest_osm_node'] = _nearest_neighbor(
        osm_nodes[['x', 'y']],
        transit_nodes[['x', 'y']])

    net_connector_edges = []

    for transit_node_id, row in transit_nodes.iterrows():
        # create new edge between the node in df2 (transit)
        # and the node in openstreetmap (pedestrian)

        osm_node_id = int(row['nearest_osm_node'])
        osm_row = osm_nodes.loc[osm_node_id]

        distance = vincenty((row['y'], row['x']),
                            (osm_row['y'], osm_row['x'])).miles
        time_ped_to_transit = distance / travel_speed_mph * 60
        time_transit_to_ped = distance / travel_speed_mph * 60

        # save the edge
        net_type = 'transit to osm'
        net_connector_edges.append((transit_node_id, osm_node_id,
                                    time_transit_to_ped, net_type))
        # make the edge bi-directional
        net_type = 'osm to transit'
        net_connector_edges.append((osm_node_id, transit_node_id,
                                    time_ped_to_transit, net_type))

    net_connector_edges = pd.DataFrame(net_connector_edges,
                                       columns=["from", "to",
                                                "weight", "net_type"])

    log(
        'Connector edges between the OSM and transit network nodes '
        'successfully completed. Took {:,.2f} seconds'.format(
            time.time() - start_time))

    return net_connector_edges


def _format_pandana_edges_nodes(edge_df, node_df):
    """
    Perform final formatting on nodes and edge DataFrames to prepare them
    for use in Pandana.
    Formatting mainly consists of creating a unique node id and edge from
    and to id that is an integer
    per Pandana requirements.

    Parameters
    ----------
    edge_df : pandas.DataFrame
        integrated transit and osm edge DataFrame
    node_df : pandas.DataFrame
        integrated transit and osm node DataFrame

    Returns
    -------
    edge_df_wnumericid, node_df : pandas.DataFrame

    """
    start_time = time.time()

    # pandana requires ids that are integer: for nodes - make it the index,
    # for edges make it the from and to columns
    node_df['id_int'] = range(1, len(node_df) + 1)

    edge_df.rename(columns={'id': 'edge_id'}, inplace=True)
    tmp = pd.merge(edge_df, node_df[['id', 'id_int']], left_on='from',
                   right_on='id', sort=False, copy=False, how='left')
    tmp['from_int'] = tmp['id_int']
    tmp.drop(['id_int', 'id'], axis=1, inplace=True)
    edge_df_wnumericid = pd.merge(tmp, node_df[['id', 'id_int']], left_on='to',
                                  right_on='id', sort=False, copy=False,
                                  how='left')
    edge_df_wnumericid['to_int'] = edge_df_wnumericid['id_int']
    edge_df_wnumericid.drop(['id_int', 'id'], axis=1, inplace=True)
    # turn mixed dtype cols into all same format
    col_list = edge_df_wnumericid.select_dtypes(include=['object']).columns
    for col in col_list:
        try:
            edge_df_wnumericid[col] = edge_df_wnumericid[col].astype(str)
        # deal with edge cases where typically the name of a street is not
        # in a uniform string encoding such as names with accents
        except UnicodeEncodeError:
            log('Fixed unicode error in {} column'.format(col))
            edge_df_wnumericid[col] = edge_df_wnumericid[col].str.encode(
                'utf-8')

    node_df.set_index('id_int', drop=True, inplace=True)
    # turn mixed dtype col into all same format
    node_df['id'] = node_df['id'].astype(str)
    if 'nearest_osm_node' in node_df.columns:
        node_df.drop(['nearest_osm_node'], axis=1, inplace=True)

    log('Edge and node tables formatted for Pandana with integer node ids: '
        'id_int, to_int, and from_int. Took {:,.2f} seconds'.format(
            time.time() - start_time))
    return edge_df_wnumericid, node_df


def save_network(urbanaccess_network, filename,
                 dir=config.settings.data_folder,
                 overwrite_key=False, overwrite_hdf5=False):
    """
    Write a urbanaccess_network integrated nodes and edges to a node and edge
    table in a hdf5 file

    Parameters
    ----------
    urbanaccess_network : object
        urbanaccess_network object with net_edges and net_nodes DataFrames
    filename : string
        name of the hdf5 file to save with .h5 extension
    dir : string, optional
        directory to save hdf5 file
    overwrite_key : bool, optional
        if true any existing table with the specified key name will be
        overwritten
    overwrite_hdf5 : bool, optional
        if true any existing hdf5 file with the specified name in the
        specified directory will be overwritten

    Returns
    -------
    None
    """
    if urbanaccess_network is None or urbanaccess_network.net_edges.empty or \
            urbanaccess_network.net_nodes.empty:
        raise ValueError('Either no urbanaccess_network specified or '
                         'net_edges or net_nodes are empty.')

    df_to_hdf5(data=urbanaccess_network.net_edges, key='edges',
               overwrite_key=overwrite_key, dir=dir,
               filename=filename, overwrite_hdf5=overwrite_hdf5)
    df_to_hdf5(data=urbanaccess_network.net_nodes, key='nodes',
               overwrite_key=overwrite_key, dir=dir, filename=filename,
               overwrite_hdf5=overwrite_hdf5)


def load_network(dir=config.settings.data_folder, filename=None):
    """
    Read an integrated network node and edge data from a hdf5 file to
    a urbanaccess_network object

    Parameters
    ----------
    dir : string, optional
        directory to read hdf5 file
    filename : string
        name of the hdf5 file to read with .h5 extension

    Returns
    -------
    ua_network : object
        urbanaccess_network object with net_edges and net_nodes DataFrames
    ua_network.net_edges : object
    ua_network.net_nodes : object
    """
    ua_network.net_edges = hdf5_to_df(dir=dir, filename=filename, key='edges')
    ua_network.net_nodes = hdf5_to_df(dir=dir, filename=filename, key='nodes')

    return ua_network
