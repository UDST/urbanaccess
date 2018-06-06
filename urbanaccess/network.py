from future.utils import raise_with_traceback
from geopy.distance import vincenty
import pandas as pd
import time

from urbanaccess.utils import log, df_to_hdf5, hdf5_to_df
from urbanaccess import config

from .synthesize import (integrate_walk_and_transit_networks,
                         make_pdna_compatible)

# TODO: This should not be a class unless it needs to have some
#       methods associated with it, otherwise this should just be a dict
class urbanaccess_network(object):
    """
    A urbanaccess object of dataframes representing
    the components of a graph network

    Parameters
    ----------
    transit_nodes : pandas.DataFrame
    transit_edges : pandas.DataFrame
    net_connector_edges : pandas.DataFrame
    osm_nodes : pandas.DataFrame
    osm_edges : pandas.DataFrame
    net_nodes : pandas.DataFrame
    net_edges : pandas.DataFrame
    """

    def __init__(self,
                 transit_nodes=pd.DataFrame(),
                 transit_edges=pd.DataFrame(),
                 net_connector_edges = pd.DataFrame(),
                 osm_nodes=pd.DataFrame(),
                 osm_edges=pd.DataFrame(),
                 net_nodes=pd.DataFrame(),
                 net_edges=pd.DataFrame()):

        self.transit_nodes = transit_nodes
        self.transit_edges = transit_edges
        self.net_connector_edges = net_connector_edges
        self.osm_nodes = osm_nodes
        self.osm_edges = osm_edges
        self.net_nodes = net_nodes
        self.net_edges = net_edges

# instantiate the UrbanAccess network object
ua_network = urbanaccess_network()


def integrate_network(urbanaccess_network=None, headways=False,
                      urbanaccess_gtfsfeeds_df=None, headway_statistic='mean'):
    """
    Create an integrated network comprised of transit and OSM nodes and edges
    by connecting the transit network with the osm network.
    travel time is in units of minutes

    Parameters
    ----------
    urbanaccess_network : object
        ua_network object with transit_edges, transit_nodes,
        osm_edges, osm_nodes
    headways : bool, optional
        if true, route stop level headways calculated in a previous step
        will be applied to the osm to transit connector
        edge travel time weights as an approximate measure
        of average passenger transit stop waiting time.
    urbanaccess_gtfsfeeds_df : object, optional
        required if headways is true; the gtfsfeeds_dfs object that holds
        the corresponding headways and stops dataframes
    headway_statistic : {'mean', 'std', 'min', 'max'}, optional
        required if headways is true; route stop headway
        statistic to apply to the osm to transit connector edges:
        mean, std, min, max. Default is mean.

    Returns
    -------
    urbanaccess_network : object
    urbanaccess_network.transit_edges : pandas.DataFrame
    urbanaccess_network.transit_nodes : pandas.DataFrame
    urbanaccess_network.osm_edges : pandas.DataFrame
    urbanaccess_network.osm_nodes : pandas.DataFrame
    urbanaccess_network.net_connector_edges : pandas.DataFrame
    urbanaccess_network.net_edges : pandas.DataFrame
    urbanaccess_network.net_nodes : pandas.DataFrame
    """

    # check argument parameters
    assert isinstance(headways, bool)

    # loop through the urbanaccess_network and make sure all
    # required components are populated dataframes
    check_if_empty_list = [urbanaccess_network.transit_edges,
                           urbanaccess_network.transit_nodes,
                           urbanaccess_network.osm_edges,
                           urbanaccess_network.osm_nodes]
    something_is_empty = False
    for item in check_if_empty_list:
        if item.empty:
            something_is_empty = True
    if something_is_empty:
        err_text = ('One of the network objects: transit_edges, '
                    'transit_nodes, osm_edges, or osm_nodes were '
                    'found to be empty.')
        raise_with_traceback(ValueError(err_text))

    # update the user that we are proceeding forward
    status = ('Loaded UrbanAccess network components comprised of: {:,} '
              'transit nodes and {:,} edges; {:,} OSM nodes and {:,} '
              'edges.').format(len(urbanaccess_network.transit_nodes),
                               len(urbanaccess_network.transit_edges),
                               len(urbanaccess_network.osm_nodes),
                               len(urbanaccess_network.osm_edges))
    log(status)

    # Note: Deprecating the following args:
    #       headways (basically proceed as though it were true)
    
    # need to convert everything out of the class attribute format for
    # these new functions as they've been rewritten to accept dicts instead
    transit_nw = {
        'nodes': urbanaccess_network.transit_nodes,
        'edges': urbanaccess_network.transit_edges
    }
    walk_nw = {
        'nodes': urbanaccess_network.osm_nodes,
        'edges': urbanaccess_network.osm_edges
    }
    gtfsfeeds_df = {
        'stops': urbanaccess_gtfsfeeds_df.stops,
        'routes': urbanaccess_gtfsfeeds_df.routes,
        'trips': urbanaccess_gtfsfeeds_df.trips,
        'stop_times': urbanaccess_gtfsfeeds_df.stop_times,
        'calendar': urbanaccess_gtfsfeeds_df.calendar,
        'calendar_dates': urbanaccess_gtfsfeeds_df.calendar_dates,
        'stop_times_int': urbanaccess_gtfsfeeds_df.stop_times_int,
        'headways': urbanaccess_gtfsfeeds_df.headways
    }

    # this function (integrate_network) has now become a wrapper
    # around the new integrate_walk_and_transit_networks method 
    integr_nw = integrate_walk_and_transit_networks(transit_nw,
                                                    walk_nw,
                                                    gtfsfeeds_df,
                                                    headway_statistic)

    # now convert back into the original method
    urbanaccess_network.net_connector_edges = integr_nw['connector_edges']
    urbanaccess_network.net_nodes = integr_nw['nodes']
    urbanaccess_network.net_edges = integr_nw['edges']

    node_df = urbanaccess_network.net_nodes
    # handle the case when an id col may not be present
    if 'id' not in node_df.columns:
        node_df['id'] = node_df.index.values
    
    # update edges and nodes, and make pdna compatible
    edge_df = urbanaccess_network.net_edges
    ua_net = make_pdna_compatible(edge_df, node_df)
    urbanaccess_network.net_edges = ua_net['edges']
    urbanaccess_network.net_nodes = ua_net['nodes']

    completed_msg = ('Network edge and node network integration '
                     'completed successfully resulting in a total '
                     'of {:,} nodes and {:,} edges: Transit: {:,} '
                     'nodes {:,} edges; OSM {:,} nodes {:,} edges; '
                     'and {:,} connector edges.').format(
                                len(urbanaccess_network.net_nodes),
                                len(urbanaccess_network.net_edges),
                                len(urbanaccess_network.transit_nodes),
                                len(urbanaccess_network.transit_edges),
                                len(urbanaccess_network.osm_nodes),
                                len(urbanaccess_network.osm_edges),
                                len(urbanaccess_network.net_connector_edges))
    log(completed_msg)

    return urbanaccess_network
