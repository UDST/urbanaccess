import pytest
import os
import numpy as np
import pandas as pd
from geopy import distance
from sklearn.neighbors import KDTree

from urbanaccess import network
from urbanaccess.network import urbanaccess_network as ua_net
from urbanaccess.gtfs.gtfsfeeds_dataframe import urbanaccess_gtfs_df \
    as gtfsfeeds_df


def _build_expected_nearest_osm_node_data(osm_nodes=None, transit_nodes=None):
    # logic taken directly from urbanaccess\network_utils.py for
    # data replication
    df1_matrix = osm_nodes[['x', 'y']].values
    df2_matrix = transit_nodes[['x', 'y']].values
    kdt = KDTree(df1_matrix)
    indexes = kdt.query(df2_matrix, k=1, return_distance=False)
    transit_nodes['nearest_osm_node'] = osm_nodes.index.values[indexes]
    return transit_nodes


def _build_expected_connector_edges_data(osm_nodes=None, transit_nodes=None):
    # logic taken directly from urbanaccess\network_utils.py for
    # data replication
    travel_speed_mph = 3
    net_connector_edges = []
    for transit_node_id, row in transit_nodes.iterrows():
        osm_node_id = int(row['nearest_osm_node'])
        osm_row = osm_nodes.loc[osm_node_id]

        distance_mi = distance.geodesic(
            (row['y'], row['x']), (osm_row['y'], osm_row['x'])).miles
        travel_time = distance_mi / travel_speed_mph * 60

        net_type = 'transit to osm'
        net_connector_edges.append((transit_node_id, osm_node_id,
                                    travel_time, net_type))
        # make the edge bi-directional
        net_type = 'osm to transit'
        net_connector_edges.append((osm_node_id, transit_node_id,
                                    travel_time, net_type))

    net_connector_edges = pd.DataFrame(
        net_connector_edges, columns=["from", "to", "weight", "net_type"])
    return net_connector_edges


def _build_expected_intermediate_transit_node_edge_data(
        transit_edges=None, transit_nodes=None):
    # logic taken directly from urbanaccess\network.py for data replication

    # use copy to ensure we dont operate on in memory df
    tr_edges = transit_edges.copy()
    tr_nodes = transit_nodes.copy()
    tr_edges.rename(
        columns={'node_id_from': 'from', 'node_id_to': 'to'}, inplace=True)
    tr_nodes.reset_index(inplace=True, drop=False)
    tr_nodes.rename(columns={'node_id': 'id'}, inplace=True)
    return tr_edges, tr_nodes


def _build_expected_intermediate_transit_node_edge_data_w_headways(
        transit_edges=None, transit_nodes=None):
    # logic taken directly from urbanaccess\network.py for data replication

    # use copy to ensure we dont operate on in memory df
    tr_edges = transit_edges.copy()
    tr_nodes = transit_nodes.copy()
    tr_edges.rename(
        columns={'node_id_route_from': 'from', 'node_id_route_to': 'to'},
        inplace=True)
    tr_edges.drop(['node_id_from', 'node_id_to'], inplace=True, axis=1)
    tr_nodes.reset_index(inplace=True, drop=False)
    tr_nodes.rename(columns={'node_id_route': 'id'}, inplace=True)
    return tr_edges, tr_nodes


def _build_expected_node_edge_net_data(
        transit_edges=None, transit_nodes=None, walk_edges=None,
        walk_nodes=None, expected_connector_edges=None):
    # logic taken directly from urbanaccess\network.py for data replication
    edges_df = pd.concat([transit_edges, walk_edges, expected_connector_edges],
                         axis=0, ignore_index=True)
    nodes_df = pd.concat([transit_nodes, walk_nodes],
                         axis=0, ignore_index=True)

    nodes_df['id_int'] = range(1, len(nodes_df) + 1)

    edges_df.rename(columns={'id': 'edge_id'}, inplace=True)
    tmp = pd.merge(edges_df, nodes_df[['id', 'id_int']], left_on='from',
                   right_on='id', sort=False, copy=False, how='left')
    tmp['from_int'] = tmp['id_int']
    tmp.drop(['id_int', 'id'], axis=1, inplace=True)
    edges_df_wnumericid = pd.merge(
        tmp, nodes_df[['id', 'id_int']], left_on='to',
        right_on='id', sort=False, copy=False,
        how='left')
    edges_df_wnumericid['to_int'] = edges_df_wnumericid['id_int']
    edges_df_wnumericid.drop(['id_int', 'id'], axis=1, inplace=True)
    col_list = edges_df_wnumericid.select_dtypes(include=['object']).columns
    edges_df_wnumericid[col_list] = edges_df_wnumericid[col_list].astype(str)

    nodes_df.set_index('id_int', drop=True, inplace=True)
    nodes_df['id'] = nodes_df['id'].astype(str)
    nodes_df.drop(['nearest_osm_node'], axis=1, inplace=True)
    return edges_df_wnumericid, nodes_df


def _build_intermediate_transit_edges_w_routes(transit_edges=None):
    # logic taken directly from urbanaccess\network.py for data replication

    # use copy to ensure we dont operate on in memory df
    tr_edges = transit_edges.copy()
    tr_edges.rename(columns={'from': 'node_id_from'}, inplace=True)
    tr_edges.rename(columns={'to': 'node_id_to'}, inplace=True)
    tr_edges['node_id_route_from'] = tr_edges['node_id_from'].str.cat(
        tr_edges['unique_route_id'].astype('str'), sep='_')
    tr_edges['node_id_route_to'] = tr_edges['node_id_to'].str.cat(
        tr_edges['unique_route_id'].astype('str'), sep='_')
    return tr_edges


def _build_expected_transit_nodes_w_routes_data(
        stops_df=None, edges_w_routes=None):
    # logic taken directly from urbanaccess\network.py for data replication

    # use copy to ensure we dont operate on in memory df
    stops_df_copy = stops_df.copy()
    stops_df_copy['unique_stop_id'] = stops_df_copy['stop_id'].str.cat(
        stops_df_copy['unique_agency_id'].astype('str'), sep='_')
    tmp1 = pd.merge(edges_w_routes[['node_id_from', 'node_id_route_from']],
                    stops_df_copy,
                    how='left', left_on='node_id_from',
                    right_on='unique_stop_id', sort=False, copy=False)
    tmp1.rename(columns={'node_id_route_from': 'node_id_route',
                         'stop_lon': 'x',
                         'stop_lat': 'y'},
                inplace=True)
    tmp2 = pd.merge(edges_w_routes[['node_id_to', 'node_id_route_to']],
                    stops_df_copy,
                    how='left',
                    left_on='node_id_to',
                    right_on='unique_stop_id', sort=False, copy=False)
    tmp2.rename(columns={'node_id_route_to': 'node_id_route',
                         'stop_lon': 'x',
                         'stop_lat': 'y'},
                inplace=True)
    transit_nodes_wroutes = pd.concat([tmp1, tmp2], axis=0)
    transit_nodes_wroutes.drop_duplicates(
        subset='node_id_route', keep='first', inplace=True)
    transit_nodes_wroutes.drop(
        columns=['node_id_to', 'node_id_to'], inplace=True)
    transit_nodes_wroutes = transit_nodes_wroutes.set_index('node_id_route')
    transit_nodes_wroutes['net_type'] = 'transit'
    return transit_nodes_wroutes


@pytest.fixture
def transit_edges_1_agency():
    # edges for outbound direction
    data = {
        'node_id_from': ['1_agency_a_city_a', '2_agency_a_city_a',
                         '3_agency_a_city_a', '4_agency_a_city_a',
                         '5_agency_a_city_a',
                         '7_agency_a_city_a', '8_agency_a_city_a'],
        'node_id_to': ['2_agency_a_city_a', '3_agency_a_city_a',
                       '4_agency_a_city_a', '5_agency_a_city_a',
                       '6_agency_a_city_a',
                       '8_agency_a_city_a', '9_agency_a_city_a'],
        'weight': [2, 4, 4, 8, 6, 3, 5],
        'unique_agency_id': ['agency_a_city_a'] * 7,
        'unique_trip_id': ['a1_agency_a_city_a', 'a1_agency_a_city_a',
                           'a1_agency_a_city_a', 'a1_agency_a_city_a',
                           'a1_agency_a_city_a',
                           'b1_agency_a_city_a', 'b1_agency_a_city_a'],
        'sequence': [1, 2, 3, 4, 5, 1, 2],
        'route_type': [3, 3, 3, 3, 3, 2, 2],
        'unique_route_id': ['10-101_agency_a_city_a', '10-101_agency_a_city_a',
                            '10-101_agency_a_city_a', '10-101_agency_a_city_a',
                            '10-101_agency_a_city_a',
                            'B1_agency_a_city_a', 'B1_agency_a_city_a'],
        'net_type': ['transit'] * 7
    }
    index = range(7)
    direction_0 = pd.DataFrame(data, index)

    # edges for inbound direction
    data = {
        'node_id_from': ['5_agency_a_city_a', '4_agency_a_city_a',
                         '3_agency_a_city_a', '2_agency_a_city_a',
                         '1_agency_a_city_a',
                         '8_agency_a_city_a', '7_agency_a_city_a'],
        'node_id_to': ['6_agency_a_city_a', '5_agency_a_city_a',
                       '4_agency_a_city_a', '3_agency_a_city_a',
                       '2_agency_a_city_a',
                       '9_agency_a_city_a', '8_agency_a_city_a'],
        'weight': [6, 8, 4, 4, 2, 5, 3],
        'unique_agency_id': ['agency_a_city_a'] * 7,
        'unique_trip_id': ['a2_agency_a_city_a', 'a2_agency_a_city_a',
                           'a2_agency_a_city_a', 'a2_agency_a_city_a',
                           'a2_agency_a_city_a',
                           'b2_agency_a_city_a', 'b2_agency_a_city_a'],
        'sequence': [1, 2, 3, 4, 5, 1, 2],
        'route_type': [3, 3, 3, 3, 3, 2, 2],
        'unique_route_id': ['10-101_agency_a_city_a', '10-101_agency_a_city_a',
                            '10-101_agency_a_city_a', '10-101_agency_a_city_a',
                            '10-101_agency_a_city_a',
                            'B1_agency_a_city_a', 'B1_agency_a_city_a'],
        'net_type': ['transit'] * 7
    }
    index = range(7)
    direction_1 = pd.DataFrame(data, index)

    df = pd.concat([direction_1, direction_0], ignore_index=True)

    # add ID
    df['id'] = df['unique_trip_id'].str.cat(
        df['sequence'].astype('str'), sep='_')

    return df


@pytest.fixture
def transit_nodes_1_agency():
    data = {
        'stop_id': ['1', '2', '3', '4', '5', '6',
                    '7', '8', '9'],
        'stop_name': ['ave a', 'ave b', 'ave c', 'ave d', 'ave e', 'ave f',
                      '1st st', '2nd st', '3rd st'],
        'x': [-122.265609, -122.224274, -122.271604, -122.269029,
              -122.267227, -122.251793, -122.444116, -122.017867,
              -122.067423],
        'y': [37.797484, 37.774963, 37.803664, 37.80787, 37.828415,
              37.844601, 37.664174, 37.591208, 37.905628],
        'location_type': [1, 1, 1, 1, 1, 1,
                          2, 2, 2],
        'wheelchair_boarding': [1, 0, 0, 0, 0, 0,
                                1, 1, 1],
        'unique_agency_id': ['agency_a_city_a'] * 9,
        'route_type': [3] * 9,
        'net_type': ['transit'] * 9
    }
    index = range(9)
    df = pd.DataFrame(data, index)

    # add node ID as index
    df['node_id'] = (
        df['stop_id'].str.cat(
            df['unique_agency_id'].astype('str'), sep='_'))
    df.set_index('node_id', inplace=True, drop=True)

    return df


@pytest.fixture
def ua_gtfsfeeds_df(
        hw_routes_df_1_agency, hw_trips_df_1_agency,
        hw_stop_times_int_df_1_agency,
        hw_stops_df_1_agency, hw_headways_df_1_agency):
    # for testing only adding tables that are expected in tested functions,
    # leaving out others not used in tests
    gtfsfeeds_df.routes = hw_routes_df_1_agency.copy()
    gtfsfeeds_df.trips = hw_trips_df_1_agency.copy()
    gtfsfeeds_df.stop_times_int = hw_stop_times_int_df_1_agency.copy()
    gtfsfeeds_df.stops = hw_stops_df_1_agency.copy()
    gtfsfeeds_df.headways = hw_headways_df_1_agency.copy()
    return gtfsfeeds_df


@pytest.fixture
def walk_nodes():
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
        'x': [
            -122.265474, -122.272543, -122.273680, -122.262834, -122.269889,
            -122.271170, -122.268333, -122.266974, -122.264433],
        'y': [
            37.796897, 37.799683, 37.800206, 37.800964, 37.803884,
            37.804270, 37.809158, 37.808645, 37.807921],
        # name is not expected in OSM nodes but is used here as placeholder
        # for custom columns and as a reference for tests
        'name': [
            '1 8th & Oak', '2 8th & Franklin', '3 8th & Broadway',
            '4 14th & Oak', '5 14th & Franklin', '6 14th & Broadway',
            '7 Berkley & Broadway', '8 Berkley & Franklin',
            '9 Berkley & Harrison'],
        'net_type': ['walk'] * 9}
    index = range(9)
    df = pd.DataFrame(data, index)
    df.set_index('id', inplace=True, drop=False)

    return df


@pytest.fixture
def walk_edges():
    data = {
        'from': [1, 2, 3, 6, 7, 8, 9, 4, 4, 5, 2, 5],
        'to': [2, 3, 6, 7, 8, 9, 4, 1, 5, 6, 5, 8],
        'name': ['8th', '8th', 'Broadway', 'Broadway', 'Berkley', 'Berkley',
                 'Lakeside', 'Oak', '14th', '14th', 'Franklin', 'Franklin'],
        'highway': ['residential', 'residential', 'primary', 'primary',
                    'primary', 'primary', 'residential', 'residential',
                    'primary', 'primary', 'residential', 'residential'],
        'weight': [0.3, 0.3, 0.5, 0.5, 0.6, 0.6, 1, 0.8, 0.8, 0.8, 0.4, 0.4],
        'oneway': ['yes', 'yes', 'no', 'no', 'no', 'no', 'yes', 'yes', 'no',
                   'no', 'yes', 'yes'],
        'net_type': ['walk'] * 12,
    }
    index = range(12)
    df = pd.DataFrame(data, index)

    # since this is a walk network we generate a bi-directed graph
    twoway_df = df.copy()
    twoway_df.rename(columns={'from': 'to', 'to': 'from'}, inplace=True)

    edge_df = pd.concat([df, twoway_df], axis=0, ignore_index=True)
    edge_df.set_index(['from', 'to'], inplace=True, drop=False)

    return edge_df


@pytest.fixture
def expected_connector_edges(transit_nodes_1_agency, walk_nodes):
    # build expected connector edge df
    transit_nodes = _build_expected_nearest_osm_node_data(
        walk_nodes, transit_nodes_1_agency)
    net_connector_edges = _build_expected_connector_edges_data(
        walk_nodes, transit_nodes)

    return net_connector_edges


@pytest.fixture
def intermediate_transit_edges_nodes(
        transit_edges_1_agency, transit_nodes_1_agency, walk_nodes,
        expected_connector_edges):
    # build intermediate data used downstream
    transit_edges, transit_nodes = \
        _build_expected_intermediate_transit_node_edge_data(
            transit_edges_1_agency, transit_nodes_1_agency)
    # nodes are expected to have 'nearest_osm_node' column so generate the col
    transit_nodes = _build_expected_nearest_osm_node_data(
        walk_nodes, transit_nodes)
    return transit_edges, transit_nodes


@pytest.fixture
def expected_net_edges_nodes(
        intermediate_transit_edges_nodes,
        walk_edges, walk_nodes, expected_connector_edges):
    # build expected resulting integrated network edge and node tables
    transit_edges, transit_nodes = intermediate_transit_edges_nodes

    edges_df, nodes_df = _build_expected_node_edge_net_data(
        transit_edges, transit_nodes, walk_edges, walk_nodes,
        expected_connector_edges)

    return nodes_df, edges_df


@pytest.fixture
def hw_intermediate_transit_edges_w_routes(transit_edges_1_agency):
    transit_edges_w_routes = _build_intermediate_transit_edges_w_routes(
        transit_edges_1_agency)
    return transit_edges_w_routes


@pytest.fixture
def hw_intermediate_transit_nodes_w_routes(
        hw_intermediate_transit_edges_w_routes, hw_stops_df_1_agency):
    transit_nodes_wroutes = _build_expected_transit_nodes_w_routes_data(
        stops_df=hw_stops_df_1_agency,
        edges_w_routes=hw_intermediate_transit_edges_w_routes)
    return transit_nodes_wroutes


@pytest.fixture
def expected_connector_edges_w_headways(
        hw_intermediate_transit_nodes_w_routes, walk_nodes):
    # build expected connector edge df
    transit_nodes = _build_expected_nearest_osm_node_data(
        walk_nodes, hw_intermediate_transit_nodes_w_routes)
    net_connector_edges = _build_expected_connector_edges_data(
        walk_nodes, transit_nodes)
    return net_connector_edges


@pytest.fixture
def expected_connector_edges_w_headways_w_impedance(
        hw_headways_df_1_agency, expected_connector_edges_w_headways):
    osm_to_transit_wheadway = pd.merge(
        expected_connector_edges_w_headways,
        hw_headways_df_1_agency[['mean', 'node_id_route']],
        how='left', left_on=['to'], right_on=['node_id_route'],
        sort=False, copy=False)
    osm_to_transit_wheadway['weight_tmp'] = osm_to_transit_wheadway[
                                                'weight'] + (
                                                    osm_to_transit_wheadway[
                                                        'mean'] / 2.0)
    osm_to_transit_wheadway['weight_tmp'].fillna(
        osm_to_transit_wheadway['weight'], inplace=True)
    osm_to_transit_wheadway.drop('weight', axis=1, inplace=True)
    osm_to_transit_wheadway.rename(
        columns={'weight_tmp': 'weight'}, inplace=True)
    return osm_to_transit_wheadway


@pytest.fixture
def intermediate_transit_edges_nodes_w_headways(
        hw_intermediate_transit_edges_w_routes,
        hw_intermediate_transit_nodes_w_routes, walk_nodes):
    # build intermediate data used downstream
    tr_edges_w_routes = hw_intermediate_transit_edges_w_routes.copy()
    tr_nodes_w_routes = hw_intermediate_transit_nodes_w_routes.copy()

    transit_edges, transit_nodes = \
        _build_expected_intermediate_transit_node_edge_data_w_headways(
            tr_edges_w_routes, tr_nodes_w_routes)
    # nodes are expected to have 'nearest_osm_node' column so generate the col
    transit_nodes = _build_expected_nearest_osm_node_data(
        walk_nodes, transit_nodes)
    return transit_edges, transit_nodes


@pytest.fixture
def expected_net_edges_nodes_w_headways(
        intermediate_transit_edges_nodes_w_headways,
        walk_edges, walk_nodes,
        expected_connector_edges_w_headways_w_impedance):
    # build expected resulting integrated network edge and node tables
    transit_edges, transit_nodes = intermediate_transit_edges_nodes_w_headways

    edges_df, nodes_df = _build_expected_node_edge_net_data(
        transit_edges, transit_nodes, walk_edges, walk_nodes,
        expected_connector_edges_w_headways_w_impedance)
    return nodes_df, edges_df


@pytest.fixture
def expected_net_edges_nodes_h5(tmpdir, expected_net_edges_nodes):
    nodes_df, edges_df = expected_net_edges_nodes
    hdf_file_name = 'test_hdf.h5'
    hdf_file_path = os.path.join(tmpdir.strpath, hdf_file_name)
    edges_df.to_hdf(hdf_file_path, key='edges', mode='a', format='table')
    nodes_df.to_hdf(hdf_file_path, key='nodes', mode='a', format='table')
    return tmpdir.strpath, hdf_file_name


@pytest.fixture
def ua_net_object_w_transit_walk(
        transit_edges_1_agency, transit_nodes_1_agency,
        walk_edges, walk_nodes):
    ua_net.transit_edges = transit_edges_1_agency.copy()
    ua_net.transit_nodes = transit_nodes_1_agency.copy()
    ua_net.osm_edges = walk_edges.copy()
    ua_net.osm_nodes = walk_nodes.copy()
    return ua_net


@pytest.fixture
def ua_net_object_expected_result(expected_net_edges_nodes):
    nodes_df, edges_df = expected_net_edges_nodes
    ua_net.net_edges = edges_df.copy()
    ua_net.net_nodes = nodes_df.copy()
    return ua_net


def test_integrate_network_wo_headways(
        ua_net_object_w_transit_walk, intermediate_transit_edges_nodes,
        walk_edges, walk_nodes, expected_connector_edges,
        expected_net_edges_nodes):
    expected_net_nodes, expected_net_edges = expected_net_edges_nodes
    expected_transit_edges, expected_transit_nodes = \
        intermediate_transit_edges_nodes

    ua_net_object_result = network.integrate_network(
        urbanaccess_network=ua_net_object_w_transit_walk,
        headways=False,
        urbanaccess_gtfsfeeds_df=None)

    # check that results match expected tables
    assert ua_net_object_result.transit_edges.equals(expected_transit_edges)
    assert ua_net_object_result.transit_nodes.equals(expected_transit_nodes)
    assert ua_net_object_result.osm_edges.equals(walk_edges)
    assert ua_net_object_result.osm_nodes.equals(walk_nodes)
    assert ua_net_object_result.net_connector_edges.equals(
        expected_connector_edges)
    assert ua_net_object_result.net_edges.equals(expected_net_edges)
    assert ua_net_object_result.net_nodes.equals(expected_net_nodes)


def test_integrate_network_invalid_wo_headways(
        ua_net_object_w_transit_walk):
    with pytest.raises(ValueError) as excinfo:
        ua_net_object_result = network.integrate_network(
            urbanaccess_network=None,
            headways=False,
            urbanaccess_gtfsfeeds_df=None)
    expected_error = 'urbanaccess_network is not specified'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        ua_net_object_result = network.integrate_network(
            urbanaccess_network=ua_net_object_w_transit_walk,
            headways='test string',
            urbanaccess_gtfsfeeds_df=None)
    assert 'headways must be bool type' in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # set required table to be empty
        ua_net_object_w_transit_walk.transit_edges = pd.DataFrame()
        ua_net_object_result = network.integrate_network(
            urbanaccess_network=ua_net_object_w_transit_walk,
            headways=False,
            urbanaccess_gtfsfeeds_df=None)
    expected_error = (
        'one of the network objects: transit_edges, transit_nodes, '
        'osm_edges, or osm_nodes were found to be empty')
    assert expected_error in str(excinfo.value)


def test_format_pandana_edges_nodes(intermediate_transit_edges_nodes):
    # for testing purposes we will only use transit edge/node tables
    transit_edges, transit_nodes = intermediate_transit_edges_nodes
    expected_edge_id = transit_edges['id'].copy()
    result_edge, result_node = network._format_pandana_edges_nodes(
        edge_df=transit_edges, node_df=transit_nodes)
    assert isinstance(result_edge, pd.core.frame.DataFrame)
    assert isinstance(result_node, pd.core.frame.DataFrame)
    assert result_edge.empty is False
    assert result_node.empty is False
    # expected node index values
    expected_node_index = pd.Series(
        data=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        index=range(1, 10),
        name='id_int')
    expected_edge_cols = ['to_int', 'from_int']

    # check edge formatting
    for col in expected_edge_cols:
        assert col in result_edge.columns
        assert all(result_edge[col].map(type) == int) is True
    assert 'edge_id' in result_edge.columns
    assert result_edge['edge_id'].equals(expected_edge_id)

    # check node formatting
    assert result_node.index.name == 'id_int'
    assert result_node.index.to_series().equals(expected_node_index)
    assert isinstance(result_node['id'], object)
    assert all(result_node['id'].map(type) != str) is False
    assert 'nearest_osm_node' not in result_node.columns


def test_format_pandana_edges_nodes_unicode_format(
        intermediate_transit_edges_nodes):
    # for testing purposes we will only use transit edge/node tables
    transit_edges, transit_nodes = intermediate_transit_edges_nodes
    # set a string column to have characters that may generate encoding issues
    char_val = 'Circulação  , áéíóúüñ¿¡'
    transit_edges['name'] = 'Circulação  , áéíóúüñ¿¡'
    result_edge, result_node = network._format_pandana_edges_nodes(
        edge_df=transit_edges, node_df=transit_nodes)
    assert result_edge.empty is False
    assert result_edge['name'].iloc[0:1][0] == char_val


def test_save_network_wo_headways(
        tmpdir, ua_net_object_expected_result, expected_net_edges_nodes):
    expected_net_nodes, expected_net_edges = expected_net_edges_nodes
    # change 'nan' to np.nan to match expected h5 tables
    expected_net_edges = expected_net_edges.replace('nan', np.nan)
    expected_net_nodes = expected_net_nodes.replace('nan', np.nan)

    filename = 'test_hdf.h5'
    path = os.path.join(tmpdir.strpath, filename)
    network.save_network(
        urbanaccess_network=ua_net_object_expected_result,
        filename=filename,
        dir=tmpdir.strpath,
        overwrite_key=False, overwrite_hdf5=False)
    # test that file exists and read data make sure matches expected
    assert os.path.exists(path) is True
    with pd.HDFStore(path) as store:
        edges = store['edges']
        assert edges.equals(expected_net_edges)
        nodes = store['nodes']
        assert nodes.equals(expected_net_nodes)


def test_save_network_invalid(tmpdir, ua_net_object_expected_result):
    # remove one of the required tables
    ua_net_object_expected_result.net_edges = pd.DataFrame()
    filename = 'test_hdf.h5'
    with pytest.raises(ValueError) as excinfo:
        network.save_network(
            urbanaccess_network=ua_net_object_expected_result,
            filename=filename,
            dir=tmpdir.strpath,
            overwrite_key=False, overwrite_hdf5=False)
    expected_error = ('Either no urbanaccess_network specified or '
                      'net_edges or net_nodes are empty.')
    assert expected_error in str(excinfo.value)


def test_load_network_wo_headways(
        expected_net_edges_nodes_h5, expected_net_edges_nodes):
    expected_net_nodes, expected_net_edges = expected_net_edges_nodes
    hdf_file_path, hdf_file_name = expected_net_edges_nodes_h5

    ua_net_object_result = network.load_network(
        dir=hdf_file_path, filename=hdf_file_name)
    # change 'nan' to np.nan to match expected h5 tables
    expected_net_edges = expected_net_edges.replace('nan', np.nan)
    expected_net_nodes = expected_net_nodes.replace('nan', np.nan)
    # check that results match expected tables
    assert ua_net_object_result.net_edges.equals(expected_net_edges)
    assert ua_net_object_result.net_nodes.equals(expected_net_nodes)


def test_integrate_network_w_headways(
        ua_gtfsfeeds_df,
        ua_net_object_w_transit_walk,
        intermediate_transit_edges_nodes_w_headways,
        walk_edges, walk_nodes,
        expected_connector_edges_w_headways_w_impedance,
        expected_net_edges_nodes_w_headways):
    expected_net_nodes, expected_net_edges = \
        expected_net_edges_nodes_w_headways
    expected_transit_edges, expected_transit_nodes = \
        intermediate_transit_edges_nodes_w_headways

    ua_net_object_result = network.integrate_network(
        urbanaccess_network=ua_net_object_w_transit_walk,
        headways=True,
        urbanaccess_gtfsfeeds_df=ua_gtfsfeeds_df,
        headway_statistic='mean')

    # check that results match expected tables
    assert ua_net_object_result.transit_edges.equals(expected_transit_edges)
    assert ua_net_object_result.transit_nodes.equals(expected_transit_nodes)
    assert ua_net_object_result.osm_edges.equals(walk_edges)
    assert ua_net_object_result.osm_nodes.equals(walk_nodes)
    assert ua_net_object_result.net_connector_edges.equals(
        expected_connector_edges_w_headways_w_impedance)
    assert ua_net_object_result.net_edges.equals(expected_net_edges)
    assert ua_net_object_result.net_nodes.equals(expected_net_nodes)


def test_integrate_network_invalid_w_headways(
        ua_gtfsfeeds_df,
        ua_net_object_w_transit_walk):
    with pytest.raises(ValueError) as excinfo:
        ua_net_object_result = network.integrate_network(
            urbanaccess_network=ua_net_object_w_transit_walk,
            headways=True,
            urbanaccess_gtfsfeeds_df=ua_gtfsfeeds_df,
            headway_statistic='test')
    expected_error = 'test is not a supported statistic or is not a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # make headways invalid
        ua_gtfsfeeds_df.headways = pd.DataFrame()
        ua_net_object_result = network.integrate_network(
            urbanaccess_network=ua_net_object_w_transit_walk,
            headways=True,
            urbanaccess_gtfsfeeds_df=ua_gtfsfeeds_df,
            headway_statistic='mean')
    expected_error = ('stops and or headway DataFrames were not found in the '
                      'urbanaccess_gtfsfeeds object. Please create these '
                      'DataFrames in order to use headways.')
    assert expected_error in str(excinfo.value)


def test_route_id_to_node(
        hw_stops_df_1_agency, hw_intermediate_transit_edges_w_routes,
        hw_intermediate_transit_nodes_w_routes):
    result = network._route_id_to_node(
        stops_df=hw_stops_df_1_agency,
        edges_w_routes=hw_intermediate_transit_edges_w_routes)
    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.empty is False
    assert result.equals(hw_intermediate_transit_nodes_w_routes)


def test_add_headway_impedance(
        hw_headways_df_1_agency, expected_connector_edges_w_headways,
        expected_connector_edges_w_headways_w_impedance):
    result = network._add_headway_impedance(
        ped_to_transit_edges_df=expected_connector_edges_w_headways,
        headways_df=hw_headways_df_1_agency,
        headway_statistic='mean')
    assert isinstance(result, pd.core.frame.DataFrame)
    assert result.empty is False
    assert result.equals(expected_connector_edges_w_headways_w_impedance)
