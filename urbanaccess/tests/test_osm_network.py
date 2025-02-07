import pytest
import pandas as pd

from urbanaccess.osm import network


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
            '9 Berkley & Harrison']}
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
        'distance': [0.3, 0.3, 0.5, 0.5, 0.6, 0.6, 1, 0.8, 0.8, 0.8, 0.4, 0.4],
        'oneway': ['yes', 'yes', 'no', 'no', 'no', 'no', 'yes', 'yes', 'no',
                   'no', 'yes', 'yes']
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
def expected_walk_edges_nodes(walk_edges, walk_nodes):
    # build expected data
    network_type = 'walk'
    walk_edges['net_type'] = network_type
    walk_nodes['net_type'] = network_type
    walk_edges['weight'] = (walk_edges['distance'] / 1609.34) / 3 * 60
    return walk_nodes, walk_edges


def test_create_osm_net(walk_nodes, walk_edges, expected_walk_edges_nodes):
    expected_walk_nodes, expected_walk_edges = expected_walk_edges_nodes
    ua_network = network.create_osm_net(
        osm_edges=walk_edges, osm_nodes=walk_nodes,
        travel_speed_mph=3, network_type='walk')
    assert ua_network.osm_edges.empty is False
    assert ua_network.osm_edges.equals(expected_walk_edges)
    assert ua_network.osm_nodes.empty is False
    assert ua_network.osm_nodes.equals(expected_walk_nodes)


def test_create_osm_net_invalid(walk_nodes, walk_edges):
    with pytest.raises(ValueError) as excinfo:
        ua_network = network.create_osm_net(
            osm_edges=walk_edges, osm_nodes=walk_nodes,
            travel_speed_mph=3, network_type=None)
    expected_error = (
        "'None' network_type passed is either not a string or is None.")
    assert expected_error in str(excinfo.value)
