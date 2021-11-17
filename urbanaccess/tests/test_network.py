import pytest
import pandas as pd
from geopy import distance
from sklearn.neighbors import KDTree

from urbanaccess import network
from urbanaccess.network import urbanaccess_network as ua_net


def _build_expected_nearest_osm_node_data(osm_nodes=None, transit_nodes=None):
    df1_matrix = osm_nodes[['x', 'y']].values
    df2_matrix = transit_nodes[['x', 'y']].values
    kdt = KDTree(df1_matrix)
    indexes = kdt.query(df2_matrix, k=1, return_distance=False)
    transit_nodes['nearest_osm_node'] = osm_nodes.index.values[indexes]
    return transit_nodes


def _build_expected_connector_edges_data(osm_nodes=None, transit_nodes=None):
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
    transit_edges.rename(
        columns={'node_id_from': 'from', 'node_id_to': 'to'}, inplace=True)
    transit_nodes.reset_index(inplace=True, drop=False)
    transit_nodes.rename(columns={'node_id': 'id'}, inplace=True)
    return transit_edges, transit_nodes


def _build_expected_node_edge_net_data(
        transit_edges=None, transit_nodes=None, walk_edges=None,
        walk_nodes=None, expected_connector_edges=None):
    edges_df = pd.concat(
        [transit_edges, walk_edges, expected_connector_edges],
        axis=0, ignore_index=True)
    nodes_df = pd.concat(
        [transit_nodes, walk_nodes],
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
    df['id'] = (
        df['unique_trip_id'].str.cat(
            df['sequence'].astype('str'), sep='_'))

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
    transit_nodes = _build_expected_nearest_osm_node_data \
        (walk_nodes, transit_nodes)
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
def ua_net_object_w_transit_walk(
        transit_edges_1_agency, transit_nodes_1_agency,
        walk_edges, walk_nodes):
    ua_net.transit_edges = transit_edges_1_agency.copy()
    ua_net.transit_nodes = transit_nodes_1_agency.copy()
    ua_net.osm_edges = walk_edges.copy()
    ua_net.osm_nodes = walk_nodes.copy()
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
