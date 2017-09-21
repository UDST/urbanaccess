from __future__ import division
import time
from osmnet.load import network_from_bbox

from pandana import Network

from urbanaccess.utils import log


def ua_network_from_bbox(lat_min=None, lng_min=None, lat_max=None,
                         lng_max=None, bbox=None, network_type='walk',
                         timeout=180, memory=None,
                         max_query_area_size=50*1000*50*1000, remove_lcn=True):
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

    # returned osm data allows travel in both directions so that
    # all edges in integrated network are considered one way edges
    two_way = False

    # this function a wrapper around the below OSMnet function
    nodes, edges = network_from_bbox(lat_min=lat_min,
                                     lng_min=lng_min,
                                     lat_max=lat_max,
                                     lng_max=lng_max,
                                     bbox=bbox,
                                     network_type=network_type,
                                     two_way=two_way,
                                     timeout=timeout,
                                     memory=memory,
                                     max_query_area_size=max_query_area_size)

    # only perform operations if requested by user
    if remove_lcn:
        log('checking for low connectivity nodes...')

        # create a Pandana network...
        pandana_net = Network(nodes['x'],
                              nodes['y'],
                              edges['from'],
                              edges['to'],
                              edges[['distance']])
        
        # ...so we can remove low connectivity nodes
        lcn = pandana_net.low_connectivity_nodes(impedance=10000,
                                                 count=10,
                                                 imp_name='distance')
        
        # report how many nodes will be dropped
        lcn_to_node_ratio = ((len(lcn)/len(nodes)) * 100)
        removed_msg = ('{:,} out of {:,} nodes ({:.2f} percent '
                       'of total) were identified as having low '
                       'connectivity and have been '
                       'removed.').format(len(lcn),
                                          len(nodes),
                                          lcn_to_node_ratio)
        log(removed_msg)

        # get unique node ids to drop
        rm_nodes = set(lcn)

        # for nodes, only keep those whose index is not in the
        # low connectivity nodes set list
        nodes_in_rm_nodes = nodes.index.isin(rm_nodes)
        nodes = nodes.loc[~nodes_in_rm_nodes]

        # similarly, we must remove all edges connected to these nodes
        e_fr = edges['from']
        e_to = edges['to']
        edges_attached_to_rm_nodes = e_fr.isin(rm_nodes) | e_to.isin(rm_nodes)
        edges = edges.loc[~edges_attached_to_rm_nodes]

        # report back performance time
        removal_time_diff = time.time() - start_time
        completed_removal = ('Completed OSM data download and graph '
                             'node and edge table creation in {:,.2f} '
                             'seconds').format(removal_time_diff)
        log(completed_removal)

    else:
        # let user know step being skipped, report back performance time
        no_removal_time_diff = time.time() - start_time
        no_removal_msg = ('Completed OSM data download and graph node '
                          'and edge table creation in {:,.2f} '
                          'seconds').format(no_removal_time_diff)
        log(no_removal_msg)

    return nodes, edges
