import pytest
import pandas as pd
from urbanaccess import network


@pytest.fixture
def nearest_neighbor_dfs():
    data = {
        'id': (1, 2, 3),
        'x': [-122.267546, -122.264479, -122.219119],
        'y': [37.802919, 37.808042, 37.782288]
    }
    osm_nodes = pd.DataFrame(data).set_index('id')

    data = {
        'node_id_route': ['1_transit_a', '2_transit_a',
                          '3_transit_a', '4_transit_a'],
        'x': [-122.265417, -122.266910, -122.269741, -122.238638],
        'y': [37.806372, 37.802687, 37.799480, 37.797234]
    }
    transit_nodes = pd.DataFrame(data).set_index('node_id_route')

    data = {'node_id_route': ['1_transit_a', '2_transit_a',
                              '3_transit_a', '4_transit_a'],
            'nearest_osm_node': [2, 1, 1, 3]}
    index = range(4)
    expected_transit_nodes = pd.concat(
        [transit_nodes, pd.DataFrame(data, index).set_index('node_id_route')],
        axis=1)
    return osm_nodes, transit_nodes, expected_transit_nodes


def test_nearest_neighbor(nearest_neighbor_dfs):
    osm_nodes, transit_nodes, expected_transit_nodes = nearest_neighbor_dfs
    transit_nodes['nearest_osm_node'] = network._nearest_neighbor(
        osm_nodes[['x', 'y']],
        transit_nodes[['x', 'y']])

    assert expected_transit_nodes.equals(transit_nodes)
