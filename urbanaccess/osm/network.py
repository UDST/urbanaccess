import time

from urbanaccess.utils import log
from urbanaccess.network import ua_network


def create_osm_net(osm_edges, osm_nodes,
                   travel_speed_mph=3, network_type='walk'):
    """
    Create a travel time weight network graph in units of minutes from
    openstreetmap nodes and edges

    Parameters
    ----------
    osm_edges : pandas.DataFrame
        osm edge dataframe
    osm_nodes : pandas.DataFrame
        osm node dataframe
    travel_speed_mph : int, optional
        travel speed to use to calculate travel time across a
        distance on a edge. units are in miles per hour (MPH)
        for pedestrian travel this is assumed to be 3 MPH
    network_type : str, optional
        default is 'walk' for the osm pedestrian network.
        this string is used to label the osm network once it is
        integrated with the transit network

    Returns
    -------
    ua_network : object
        urbanaccess_network object with osm_edges and osm_nodes dataframes
    ua_network.osm_edges : pandas.DataFrame
    ua_network.osm_nodes : pandas.DataFrame

    """
    start_time = time.time()

    if not isinstance(network_type, str) or network_type is None:
        raise ValueError('{!s} network_type passed is either not a '
                         'string or is None'.format(network_type))

    # assign impedance to OSM edges
    osm_edges['weight'] = (osm_edges[
                               'distance'] / 1609.34) / travel_speed_mph * 60

    # assign node and edge net type
    osm_edges['net_type'] = network_type
    osm_nodes['net_type'] = network_type

    ua_network.osm_nodes = osm_nodes
    ua_network.osm_edges = osm_edges

    log(
        'Created OSM network with travel time impedance using a travel speed '
        'of {} MPH. Took {:,.2f} seconds'.format(
            travel_speed_mph, time.time() - start_time))

    return ua_network
