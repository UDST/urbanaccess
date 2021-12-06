import pytest

from urbanaccess.osm import load


def _check_osm_node_edge_format(nodes, edges):
    # we cannot full know the expected results from OSM to test by given
    # data can change at any time so tests must be broad on the resulting data
    req_col_list = ['x', 'y', 'id']
    for col in req_col_list:
        assert col in nodes.columns
        assert nodes[col].isnull().values.any() == False  # noqa

    req_col_list = ['distance', 'from', 'to']
    for col in req_col_list:
        assert col in edges.columns
        assert edges[col].isnull().values.any() == False  # noqa


@pytest.fixture
def bbox1():
    # bbox where we do not expect low connectivity nodes
    return (-122.2762870789, 37.8211879615, -122.2701716423, 37.8241329692)


@pytest.fixture
def bbox2():
    # bbox where we expect low connectivity nodes
    return (-122.4261214345, 37.8073837621, -122.4198987096, 37.8286680163)


def test_ua_network_from_bbox_remove_lcn_False(bbox1):
    nodes, edges = load.ua_network_from_bbox(
        bbox=bbox1, network_type='walk',
        timeout=180, memory=None,
        max_query_area_size=50 * 1000 * 50 * 1000,
        remove_lcn=False)
    _check_osm_node_edge_format(nodes, edges)


def test_ua_network_from_bbox_remove_lcn_True_no_lcn(bbox1):
    # we dont expect any low connectivity nodes to be removed
    nodes, edges = load.ua_network_from_bbox(
        bbox=bbox1, network_type='walk',
        timeout=180, memory=None,
        max_query_area_size=50 * 1000 * 50 * 1000,
        remove_lcn=True)
    _check_osm_node_edge_format(nodes, edges)


def test_ua_network_from_bbox_remove_lcn_True_yes_lcn(bbox2):
    # expect low connectivity nodes to be removed
    nodes, edges = load.ua_network_from_bbox(
        bbox=bbox2, network_type='walk',
        timeout=180, memory=None,
        max_query_area_size=50 * 1000 * 50 * 1000,
        remove_lcn=True)
    _check_osm_node_edge_format(nodes, edges)
