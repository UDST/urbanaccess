import time
import geopy
from geopy import distance

from sklearn.neighbors import KDTree
import pandas as pd

from urbanaccess.utils import log


if int(geopy.__version__[0]) < 2:
    dist_calc = distance.vincenty
else:
    dist_calc = distance.geodesic


def nearest_neighbor(df1, df2):
    """
    For a DataFrame of xy coordinates find the nearest xy
    coordinates in a subsequent DataFrame

    Parameters
    ----------
    df1 : pandas.DataFrame
        DataFrame of records to return as the nearest record to records in df2
    df2 : pandas.DataFrame
        DataFrame of records with xy coordinates for which to find the
        nearest record in df1 for
    Returns
    -------
    df1.index.values[indexes] : pandas.Series
        index of records in df1 that are nearest to the coordinates in df2
    """
    try:
        df1_matrix = df1.to_numpy()
        df2_matrix = df2.to_numpy()
    except AttributeError:
        df1_matrix = df1.values
        df2_matrix = df2.values
    kdt = KDTree(df1_matrix)
    indexes = kdt.query(df2_matrix, k=1, return_distance=False)
    return df1.index.values[indexes]


def connector_edges(osm_nodes, transit_nodes, travel_speed_mph=3):
    """
    Generate the connector edges between the OSM and transit edges and
    weight by travel time

    Parameters
    ----------
    osm_nodes : pandas.DataFrame
        OSM nodes DataFrame
    transit_nodes : pandas.DataFrame
        transit nodes DataFrame
    travel_speed_mph : int, optional
        travel speed to use to calculate travel time across a
        distance on an edge. units are in miles per hour (MPH)
        for pedestrian travel this is assumed to be 3 MPH

    Returns
    -------
    net_connector_edges : pandas.DataFrame

    """
    start_time = time.time()

    transit_nodes['nearest_osm_node'] = nearest_neighbor(
        osm_nodes[['x', 'y']],
        transit_nodes[['x', 'y']])

    net_connector_edges = []
    for transit_node_id, row in transit_nodes.iterrows():
        # create new edge between the node in df2 (transit)
        # and the node in OpenStreetMap (pedestrian)
        osm_node_id = int(row['nearest_osm_node'])
        osm_row = osm_nodes.loc[osm_node_id]

        distance_mi = dist_calc(
            (row['y'], row['x']), (osm_row['y'], osm_row['x'])).miles
        time_ped_to_transit = distance_mi / travel_speed_mph * 60
        time_transit_to_ped = distance_mi / travel_speed_mph * 60

        # save the edge
        net_type = 'transit to osm'
        net_connector_edges.append((transit_node_id, osm_node_id,
                                    time_transit_to_ped, net_type))
        # make the edge bi-directional
        net_type = 'osm to transit'
        net_connector_edges.append((osm_node_id, transit_node_id,
                                    time_ped_to_transit, net_type))

    net_connector_edges = pd.DataFrame(
        net_connector_edges, columns=["from", "to", "weight", "net_type"])

    log('Connector edges between the OSM and transit network nodes '
        'successfully completed. Took {:,.2f} seconds'.format(
            time.time() - start_time))

    return net_connector_edges
