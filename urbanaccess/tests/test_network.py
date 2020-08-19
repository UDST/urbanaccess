import pytest
import pandas as pd
from urbanaccess import network


@pytest.fixture
def osm_nodes_df():
    data = {
        'id': (1, 2, 3),
        'x': [-122.267546, -122.264479, -122.219119],
        'y': [37.802919, 37.808042, 37.782288]
    }
    osm_nodes = pd.DataFrame(data).set_index('id')
    return osm_nodes


@pytest.fixture
def transit_nodes_df():
    data = {
        'node_id_route': ['1_transit_a', '2_transit_a',
                          '3_transit_a', '4_transit_a'],
        'x': [-122.265417, -122.266910, -122.269741, -122.238638],
        'y': [37.806372, 37.802687, 37.799480, 37.797234]
    }
    transit_nodes = pd.DataFrame(data).set_index('node_id_route')
    return transit_nodes


@pytest.fixture
def expected_transit_nodes_neighbor_df(transit_nodes_df):
    data = {'node_id_route': ['1_transit_a', '2_transit_a',
                              '3_transit_a', '4_transit_a'],
            'nearest_osm_node': [2, 1, 1, 3]}
    index = range(4)
    expected_transit_nodes = pd.concat(
        [transit_nodes_df,
         pd.DataFrame(data, index).set_index('node_id_route')],
        axis=1)
    return expected_transit_nodes


@pytest.fixture
def expected_connector_edge_df():
    data = {'from': ['1_transit_a', 2,
                     '2_transit_a', 1,
                     '3_transit_a', 1,
                     '4_transit_a', 3],
            'to': [2, '1_transit_a',
                   1, '2_transit_a',
                   1, '3_transit_a',
                   3, '4_transit_a'],
            'weight': [2.521901, 2.521901, 0.766106, 0.766106,
                       5.317242, 5.317242, 29.690540, 29.690540],
            'net_type': ['transit to osm', 'osm to transit',
                         'transit to osm', 'osm to transit',
                         'transit to osm', 'osm to transit',
                         'transit to osm', 'osm to transit']}

    index = range(8)
    expected_connector_edge = pd.DataFrame(data, index)
    return expected_connector_edge


def test_nearest_neighbor(osm_nodes_df, transit_nodes_df,
                          expected_transit_nodes_neighbor_df):
    transit_nodes_df['nearest_osm_node'] = network._nearest_neighbor(
        osm_nodes_df[['x', 'y']],
        transit_nodes_df[['x', 'y']])

    assert expected_transit_nodes_neighbor_df.equals(transit_nodes_df)


def test_connector_edges(osm_nodes_df, transit_nodes_df,
                         expected_connector_edge_df):
    net_connector_edges = network._connector_edges(osm_nodes_df,
                                                   transit_nodes_df,
                                                   travel_speed_mph=3)
    net_connector_edges['weight'] = net_connector_edges['weight'].round(6)
    expected_connector_edge_df['weight'] = \
        expected_connector_edge_df['weight'].round(6)
    print(expected_connector_edge_df)
    print(net_connector_edges)
    assert expected_connector_edge_df.equals(net_connector_edges)
