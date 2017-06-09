import pytest

from urbanaccess.osm.load import ua_network_from_bbox


@pytest.fixture
def bbox1():
    return (-122.2762870789, 37.8211879615, -122.2701716423, 37.8241329692)


def test_column_names(bbox1):
    nodes, edges = ua_network_from_bbox(bbox=bbox1, network_type='walk',
                                        timeout=180, memory=None,
                                        max_query_area_size=50 * 1000 * 50 *
                                                            1000,
                                        remove_lcn=False)
    col_list = ['x', 'y', 'id']
    for col in col_list:
        assert col in nodes.columns

    col_list = ['distance', 'from', 'to']
    for col in col_list:
        assert col in edges.columns
