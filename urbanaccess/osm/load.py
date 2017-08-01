from __future__ import division
import time
from osmnet.load import network_from_bbox

from pandana.network import reserve_num_graphs
from pandana import Network

from urbanaccess.utils import log

# set the number of Pandana Networks in memory to arbitrary 40 for
# removing low connectivity nodes
reserve_num_graphs(40)


def ua_network_from_bbox(lat_min=None, lng_min=None, lat_max=None,
                         lng_max=None, bbox=None, network_type='walk',
                         timeout=180, memory=None,
                         max_query_area_size=50 * 1000 * 50 * 1000,
                         remove_lcn=True):
    """
    Make a graph network (nodes and edges) from a bounding lat/lon box that
    is compatible with the network analysis tool Pandana

    Parameters
    ----------
    lat_min : float
        southern latitude of bounding box
    lng_min : float
        eastern longitude of bounding box
    lat_max : float
        northern latitude of bounding box
    lng_max : float
        western longitude of bounding box
    bbox : tuple
        Bounding box formatted as a 4 element tuple:
        (lng_max, lat_min, lng_min, lat_max)
        example: (-122.304611,37.798933,-122.263412,37.822802)
        a bbox can be extracted for an area using: the CSV format
        bbox from http://boundingbox.klokantech.com/
    network_type : {'walk', 'drive'}, optional
        Specify the network type where value of 'walk' includes roadways
        where pedestrians are allowed and pedestrian
        pathways and 'drive' includes driveable roadways. Default is walk.
    timeout : int, optional
        the timeout interval for requests and to pass to Overpass API
    memory : int, optional
        server memory allocation size for the query, in bytes. If none,
        server will use its default allocation size
    max_query_area_size : float, optional
        max area for any part of the geometry, in the units the geometry
        is in: any polygon bigger will get divided up
        for multiple queries to Overpass API
        (default is 50,000 * 50,000 units
        (ie, 50km x 50km in area, if units are meters))
    remove_lcn : bool, optional
        remove low connectivity nodes from the resulting network.
        this ensures the resulting network does
        not have nodes that are unconnected
        from the rest of the larger network

    Returns
    -------
    nodesfinal, edgesfinal : pandas.DataFrame

    """

    start_time = time.time()

    # returned osm data allows travel in both directions
    # so that all edges in integrated network are all one way edges
    two_way = False

    nodes, edges = network_from_bbox(lat_min=lat_min, lng_min=lng_min,
                                     lat_max=lat_max, lng_max=lng_max,
                                     bbox=bbox, network_type=network_type,
                                     two_way=two_way, timeout=timeout,
                                     memory=memory,
                                     max_query_area_size=max_query_area_size)

    # remove low connectivity nodes and return cleaned nodes and edges
    if remove_lcn:
        log('checking for low connectivity nodes...')
        pandana_net = Network(nodes['x'], nodes['y'],
                              edges['from'], edges['to'], edges[['distance']])
        lcn = pandana_net.low_connectivity_nodes(impedance=10000, count=10,
                                                 imp_name='distance')
        log(
            '{:,} out of {:,} nodes ({:.2f} percent of total) were '
            'identified as having low connectivity and have '
            'been removed.'.format(len(lcn), len(nodes),
                                   (len(lcn) / len(nodes)) * 100))

        rm_nodes = set(lcn)

        nodes_to_keep = ~nodes.index.isin(rm_nodes)
        edges_to_keep = ~(
            edges['from'].isin(rm_nodes) | edges['to'].isin(rm_nodes))

        nodes = nodes.loc[nodes_to_keep]
        edges = edges.loc[edges_to_keep]

        log('Completed OSM data download and graph node and edge table '
            'creation in {:,.2f} seconds'.format(time.time() - start_time))

        return nodes, edges

    else:

        log('Completed OSM data download and graph node and edge table '
            'creation in {:,.2f} seconds'.format(time.time() - start_time))

        return nodes, edges
